# scripts/vault_csv_by_pattern.py
# -------------------------------------------------------------------
# 任意ディレクトリ内のCSVファイルを「そのままのバイト列」で
# SQLite Vault（object_store/file_index）へ格納します。
#
# ● 共通機能
#   - --all で全件 / もしくは --start/--end(YYYYMMDD) で期間抽出
#   - --glob で探索対象を絞り込み（既定: *.csv）
#   - --regex でファイル名から日付(YYYYMMDD)を抽出（(?P<ymd>...) 必須推奨）
#     → 例:  raw      : '^(?P<ymd>\d{8})_raw\.csv$'
#            raceinfo : '^raceinfo_(?P<ymd>\d{8})\.csv$'
#     ※ 指定がない場合は「最初に現れる8桁数字」を日付として推定
#   - gzip圧縮 (--gzip)、sha256による重複排除、WAL 最適化、分割COMMIT
#   - 進捗は tqdm（環境変数 TQDM_DISABLE=1 か --no-progress で抑止）
#
# ● 出力スキーマ（共通化）
#   object_store(sha256,size,is_gzip,bytes)
#   file_index(id,rel_path,mtime,size,sha256,date_ymd)
#
from __future__ import annotations
import argparse, gzip, hashlib, os, re, sqlite3, time
from pathlib import Path
from datetime import datetime, date
from typing import Iterable, Tuple, Optional

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k): return x  # tqdm無しでもOK

SCHEMA_SQL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS object_store(
  sha256   TEXT PRIMARY KEY,
  size     INTEGER NOT NULL,   -- original bytes (uncompressed)
  is_gzip  INTEGER NOT NULL,   -- 0/1
  bytes    BLOB NOT NULL       -- payload (gzip if is_gzip=1)
);

CREATE TABLE IF NOT EXISTS file_index(
  id        INTEGER PRIMARY KEY,
  rel_path  TEXT UNIQUE,       -- relative path from input dir (posix)
  mtime     REAL,
  size      INTEGER,
  sha256    TEXT NOT NULL REFERENCES object_store(sha256),
  date_ymd  TEXT               -- 'YYYY-MM-DD' (from filename)
);

CREATE INDEX IF NOT EXISTS ix_file_sha  ON file_index(sha256);
CREATE INDEX IF NOT EXISTS ix_file_date ON file_index(date_ymd);
"""

PRAGMAS_BULK = [
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("temp_store", "MEMORY"),
    ("wal_autocheckpoint", "1000"),
    ("mmap_size", str(128 * 1024 * 1024)),
    ("cache_size", str(-512 * 1024)),  # 512MB equiv
]

DEFAULT_YMD_FALLBACK = re.compile(r"(?P<ymd>\d{8})")  # 先頭8桁数字を拾う保険

def set_pragmas(con: sqlite3.Connection, pairs: Iterable[Tuple[str, str]]) -> None:
    cur = con.cursor()
    for k, v in pairs:
        try: cur.execute(f"PRAGMA {k}={v};")
        except sqlite3.DatabaseError: pass

def init_db(db_path: Path) -> sqlite3.Connection:
    new_db = not db_path.exists()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys=ON;")
    if new_db:
        try: con.execute("PRAGMA page_size=32768;")
        except sqlite3.DatabaseError: pass
    set_pragmas(con, PRAGMAS_BULK)
    con.executescript(SCHEMA_SQL)
    con.commit()
    return con

def to_rel_path(p: Path, base: Path) -> str:
    return p.relative_to(base).as_posix()

def sha256_and_bytes(p: Path) -> tuple[str, int, bytes]:
    b = p.read_bytes()
    return hashlib.sha256(b).hexdigest(), len(b), b

def parse_ymd_from_name(name: str, rx: Optional[re.Pattern]) -> Optional[date]:
    m = rx.search(name) if rx else DEFAULT_YMD_FALLBACK.search(name)
    if not m: return None
    ymd = m.group("ymd")
    try:
        return datetime.strptime(ymd, "%Y%m%d").date()
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser(description="Vault CSV files (exact bytes) into SQLite by filename pattern/date.")
    ap.add_argument("--input-dir", default="data/raw", help="root dir to search files")
    ap.add_argument("--db", required=True, help="output sqlite path (e.g., data/sqlite/vault.sqlite)")
    ap.add_argument("--glob", default="*.csv", help="filename glob under input-dir (default: *.csv)")
    ap.add_argument("--regex", default=None, help="Python regex to extract (?P<ymd>YYYYMMDD) from filename")
    ap.add_argument("--all", action="store_true", help="include ALL matched files (ignore dates)")
    ap.add_argument("--start", help="start date YYYYMMDD (inclusive)")
    ap.add_argument("--end",   help="end date YYYYMMDD (inclusive)")
    ap.add_argument("--gzip", action="store_true", help="gzip-compress payload")
    ap.add_argument("--commit-every", type=int, default=5000, help="commit every N files")
    ap.add_argument("--max-files", type=int, default=0, help="limit for testing")
    ap.add_argument("--no-progress", action="store_true", help="disable tqdm progress")
    args = ap.parse_args()

    input_dir = Path(args.input_dir).resolve()
    db_path   = Path(args.db).resolve()
    if not input_dir.exists():
        print(f"[ERR] input_dir not found: {input_dir}")
        return

    rx = re.compile(args.regex, re.IGNORECASE) if args.regex else None

    if not args.all:
        if not (args.start and args.end):
            raise SystemExit("[ERR] provide --start and --end, or --all")
        start_date = datetime.strptime(args.start, "%Y%m%d").date()
        end_date   = datetime.strptime(args.end, "%Y%m%d").date()

    # 列挙
    # Path.rglob はルートからの相対グロブ。サブフォルダも探索。
    files = sorted(input_dir.rglob(args.glob), key=lambda p: p.name.lower())
    candidates: list[tuple[Path, Optional[date]]] = []
    for p in files:
        ymd = parse_ymd_from_name(p.name, rx)
        if args.all:
            candidates.append((p, ymd))
        else:
            # 期間指定時は ymd が取れないものは除外（安全）
            if ymd and (start_date <= ymd <= end_date):
                candidates.append((p, ymd))

    # 並び替え：日付→ファイル名
    candidates.sort(key=lambda t: ((t[1] or date.min), t[0].name))
    if args.max_files:
        candidates = candidates[: args.max_files]

    # 進捗の有効/無効
    disable_env = os.environ.get("TQDM_DISABLE", "").lower() in ("1", "true", "yes")
    use_tqdm = (not args.no_progress) and (not disable_env)
    def iter_with_progress(it):
        return tqdm(it, desc="Vaulting") if use_tqdm else it

    con = init_db(db_path)
    cur = con.cursor()

    info_range = "ALL" if args.all else f"{args.start}..{args.end}"
    print(f"[INFO] root: {input_dir}")
    print(f"[INFO] glob: {args.glob}")
    print(f"[INFO] regex: {args.regex or DEFAULT_YMD_FALLBACK.pattern}  (must contain ?P<ymd> when you need date filters)")
    print(f"[INFO] range: {info_range} / matched files: {len(candidates)}")

    n_new = n_dup = 0
    total_bytes = 0
    t0 = time.time()

    con.execute("BEGIN")
    try:
        for i, (p, ymd) in enumerate(iter_with_progress(candidates)):
            try:
                sha, size, data = sha256_and_bytes(p)
            except Exception as e:
                print(f"[WARN] read failed: {p} ({e})")
                continue

            exists = cur.execute("SELECT 1 FROM object_store WHERE sha256=?", (sha,)).fetchone()
            if exists is None:
                payload = gzip.compress(data) if args.gzip else data
                cur.execute(
                    "INSERT INTO object_store(sha256,size,is_gzip,bytes) VALUES (?,?,?,?)",
                    (sha, size, 1 if args.gzip else 0, payload),
                )
                n_new += 1
            else:
                n_dup += 1

            relp = to_rel_path(p, input_dir)
            mtime = p.stat().st_mtime
            cur.execute(
                "INSERT OR REPLACE INTO file_index(rel_path,mtime,size,sha256,date_ymd) VALUES (?,?,?,?,?)",
                (relp, mtime, size, sha, (ymd.strftime("%Y-%m-%d") if ymd else None)),
            )

            total_bytes += size
            if args.commit_every and (i + 1) % args.commit_every == 0:
                con.commit()
                con.execute("BEGIN")
        con.commit()
    finally:
        con.close()

    dt = time.time() - t0
    mb = total_bytes / (1024 * 1024)
    rate = (len(candidates) / dt) if dt > 0 else float("inf")
    print(f"[DONE] new:{n_new} dup:{n_dup} files:{len(candidates)} bytes:{mb:.1f}MB elapsed:{dt:.1f}s ({rate:.1f} files/s)")
    print(f"[TIP ] optimize copy: sqlite3 \"{db_path}\" \"PRAGMA wal_checkpoint(FULL); VACUUM INTO '{db_path.with_suffix('._compact.sqlite')}'\"")

if __name__ == "__main__":
    main()
