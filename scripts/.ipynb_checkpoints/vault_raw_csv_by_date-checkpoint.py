# scripts/vault_raw_csv_by_date.py
# ---------------------------------------------
# data/raw/ 以下の "YYYYMMDD_raw.csv" を、日付範囲で選別して
# そのままのバイト列で SQLite にBLOB保存（Vault化）します。
# ・内容アドレス化（sha256）で重複排除
# ・任意でgzip圧縮 (--gzip)
# ・WAL + 大きめキャッシュ + 分割COMMITで高速
from __future__ import annotations
import argparse, hashlib, gzip, sqlite3, time, re
from pathlib import Path
from datetime import datetime
from typing import Iterable, Tuple

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k): return x

SCHEMA_SQL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS object_store(
  sha256   TEXT PRIMARY KEY,
  size     INTEGER NOT NULL,   -- 非圧縮サイズ（bytes）
  is_gzip  INTEGER NOT NULL,   -- 0/1
  bytes    BLOB NOT NULL       -- (is_gzip==1)ならgzip圧縮データ
);

CREATE TABLE IF NOT EXISTS file_index(
  id        INTEGER PRIMARY KEY,
  rel_path  TEXT UNIQUE,       -- 入力ディレクトリからの相対パス（/区切り）
  mtime     REAL,
  size      INTEGER,
  sha256    TEXT NOT NULL REFERENCES object_store(sha256),
  date_ymd  TEXT               -- 'YYYY-MM-DD'（ファイル名先頭8桁から）
);

CREATE INDEX IF NOT EXISTS ix_file_sha ON file_index(sha256);
CREATE INDEX IF NOT EXISTS ix_file_date ON file_index(date_ymd);
"""

PRAGMAS_BULK = [
    ("journal_mode", "WAL"),
    ("synchronous", "NORMAL"),
    ("temp_store", "MEMORY"),
    ("wal_autocheckpoint", "1000"),
    ("mmap_size", str(128 * 1024 * 1024)),
    ("cache_size", str(-512 * 1024)),  # 512MB相当
]

# ★ ここを _raw.csv のみ許可に変更
NAME_RE = re.compile(r"^(?P<ymd>\d{8})_raw\.csv$", re.IGNORECASE)

def set_pragmas(con: sqlite3.Connection, pairs: Iterable[Tuple[str, str]]) -> None:
    cur = con.cursor()
    for k, v in pairs:
        try:
            cur.execute(f"PRAGMA {k}={v};")
        except sqlite3.DatabaseError:
            pass

def init_db(db_path: Path) -> sqlite3.Connection:
    new_db = not db_path.exists()
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys=ON;")
    if new_db:
        try:
            con.execute("PRAGMA page_size=32768;")
        except sqlite3.DatabaseError:
            pass
    set_pragmas(con, PRAGMAS_BULK)
    con.executescript(SCHEMA_SQL)
    con.commit()
    return con

def to_rel_path(p: Path, base: Path) -> str:
    return p.relative_to(base).as_posix()

def sha256_and_bytes(p: Path) -> tuple[str, int, bytes]:
    b = p.read_bytes()
    return hashlib.sha256(b).hexdigest(), len(b), b

def main():
    ap = argparse.ArgumentParser(description="Vault YYYYMMDD_raw.csv by date range (store exact bytes as BLOB).")
    ap.add_argument("--input-dir", default="data/raw", help="root dir that holds YYYYMMDD_raw.csv")
    ap.add_argument("--db", required=True, help="output sqlite path (e.g., data/sqlite/csv_vault_2025Q3.sqlite)")
    ap.add_argument("--start", required=True, help="start date YYYYMMDD (inclusive)")
    ap.add_argument("--end",   required=True, help="end date YYYYMMDD (inclusive)")
    ap.add_argument("--gzip", action="store_true", help="compress payload with gzip")
    ap.add_argument("--commit-every", type=int, default=5000, help="commit every N files")
    ap.add_argument("--max-files", type=int, default=0, help="limit for testing")
    args = ap.parse_args()

    input_dir = Path(args.input_dir).resolve()
    db_path   = Path(args.db).resolve()
    if not input_dir.exists():
        print(f"[ERR] input_dir not found: {input_dir}")
        return

    start_date = datetime.strptime(args.start, "%Y%m%d").date()
    end_date   = datetime.strptime(args.end, "%Y%m%d").date()

    # 候補ファイル列挙（_raw.csv のみ）
    candidates = []
    for p in input_dir.rglob("*.csv"):
        m = NAME_RE.match(p.name)
        if not m:
            continue
        ymd = datetime.strptime(m.group("ymd"), "%Y%m%d").date()
        if start_date <= ymd <= end_date:
            candidates.append((p, ymd))
    candidates.sort(key=lambda x: (x[1], x[0].name))
    if args.max_files:
        candidates = candidates[: args.max_files]

    con = init_db(db_path)
    cur = con.cursor()

    print(f"[INFO] range: {args.start}..{args.end} / matched files: {len(candidates)}")
    n_new, n_dup, total_bytes = 0, 0, 0
    t0 = time.time()

    con.execute("BEGIN")
    try:
        for i, (p, ymd) in enumerate(tqdm(candidates, desc="Vaulting")):
            try:
                sha, size, data = sha256_and_bytes(p)
            except Exception as e:
                print(f"[WARN] read failed: {p} ({e})")
                continue

            # object_store: 重複排除
            exists = cur.execute("SELECT 1 FROM object_store WHERE sha256=?", (sha,)).fetchone()
            if exists is None:
                blob = gzip.compress(data) if args.gzip else data
                cur.execute(
                    "INSERT INTO object_store(sha256,size,is_gzip,bytes) VALUES (?,?,?,?)",
                    (sha, size, 1 if args.gzip else 0, blob),
                )
                n_new += 1
            else:
                n_dup += 1

            relp = to_rel_path(p, input_dir)
            mtime = p.stat().st_mtime
            cur.execute(
                "INSERT OR REPLACE INTO file_index(rel_path,mtime,size,sha256,date_ymd) VALUES (?,?,?,?,?)",
                (relp, mtime, size, sha, ymd.strftime("%Y-%m-%d")),
            )

            total_bytes += size
            if args.commit_every and (i + 1) % args.commit_every == 0:
                con.commit()
                con.execute("BEGIN")
        con.commit()
    finally:
        con.close()

    dt_s = time.time() - t0
    mb = total_bytes / (1024 * 1024)
    rate = (len(candidates) / dt_s) if dt_s > 0 else float("inf")
    print(f"[DONE] new:{n_new} dup:{n_dup} files:{len(candidates)} bytes:{mb:.1f}MB elapsed:{dt_s:.1f}s ({rate:.1f} files/s)")
    print(f"[TIP ] optimize copy: sqlite3 \"{db_path}\" \"PRAGMA wal_checkpoint(FULL); VACUUM INTO '{db_path.with_suffix('._compact.sqlite')}'\"")

if __name__ == "__main__":
    main()
