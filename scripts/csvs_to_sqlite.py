# scripts/csvs_to_sqlite.py
from __future__ import annotations
import argparse, sqlite3, sys, re, os
from pathlib import Path
from typing import List
import pandas as pd

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k): return x  # tqdmが無くても動く

LIKELY_DATE_COLS = {
    "date","race_date","visit_at","datetime","dt","created_at","updated_at","fetched_at","time_at"
}
LIKELY_INDEX_COL_SUFFIXES = ("_id",)
LIKELY_INDEX_COLS = {"id","race_id","player_id","date","datetime","created_at","updated_at"}

PRAGMAS = [
    ("journal_mode","WAL"),
    ("synchronous","NORMAL"),
    ("temp_store","MEMORY"),
    ("wal_autocheckpoint","1000"),
    ("cache_size", str(-256*1024)),  # 256MB相当
]

def set_pragmas(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON;")
    for k, v in PRAGMAS:
        try:
            cur.execute(f"PRAGMA {k}={v};")
        except sqlite3.DatabaseError:
            pass

def sanitize_table_name(name: str) -> str:
    base = re.sub(r"[^0-9A-Za-z_]", "_", name)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "table"
    if base[0].isdigit():
        base = "t_" + base
    return base.lower()

def auto_convert_types(df: pd.DataFrame) -> pd.DataFrame:
    # 数値にできる列は数値へ
    for c in df.columns:
        # まず数値
        df[c] = pd.to_numeric(df[c], errors="ignore")
    # 日付っぽい列名は日付へ
    for c in df.columns:
        cl = c.lower()
        if cl in LIKELY_DATE_COLS or cl.endswith("_at"):
            try:
                df[c] = pd.to_datetime(df[c], errors="coerce")
            except Exception:
                pass
    return df

def create_indexes(con: sqlite3.Connection, table: str, cols: List[str]):
    cur = con.cursor()
    for c in cols:
        cl = c.lower()
        if cl in LIKELY_INDEX_COLS or cl.endswith(LIKELY_INDEX_COL_SUFFIXES):
            idx = f"ix_{table}_{cl}"
            try:
                cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx}" ON "{table}"("{c}");')
            except sqlite3.DatabaseError:
                pass
    con.commit()

def read_csv_chunks(path: Path, chunksize: int, sep: str):
    # 文字コードは UTF-8(BOM含む) → ダメなら cp932（日本語Windows想定）
    encodings = ["utf-8-sig", "cp932"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, chunksize=chunksize, low_memory=False, encoding=enc, sep=sep)
        except Exception as e:
            last_err = e
    raise last_err

def main():
    ap = argparse.ArgumentParser(description="Pack CSVs into a single SQLite DB.")
    ap.add_argument("--input-dir", default="data/raw", help="CSV root directory")
    ap.add_argument("--db", default="data/sqlite/raw.sqlite", help="Output SQLite file")
    ap.add_argument("--pattern", default="*.csv", help="Glob pattern")
    ap.add_argument("--sep", default=",", help="CSV delimiter (default ,)")
    ap.add_argument("--chunksize", type=int, default=50000, help="Rows per chunk")
    ap.add_argument("--replace", action="store_true", help="Replace table if exists")
    args = ap.parse_args()

    in_dir = Path(args.input_dir)
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.rglob(args.pattern))
    if not files:
        print(f"[ERR] no CSV matched in {in_dir} (pattern={args.pattern})")
        sys.exit(1)

    con = sqlite3.connect(db_path)
    set_pragmas(con)

    print(f"[INFO] Importing {len(files)} file(s) into {db_path}")
    for f in tqdm(files, desc="CSV"):
        table = sanitize_table_name(f.stem)
        if_exists = "replace" if args.replace else "append"
        first_chunk = True

        try:
            chunks = read_csv_chunks(f, chunksize=args.chunksize, sep=args.sep)
        except Exception as e:
            print(f"[WARN] read failed: {f} ({e})")
            continue

        for chunk in chunks:
            chunk = auto_convert_types(chunk)
            # pandasのTimestampはto_sqlで自動的に文字列保存される（SQLiteはTEXT）※それでOK
            chunk.to_sql(table, con, if_exists=("replace" if (if_exists=="replace" and first_chunk) else "append"),
                         index=False, method=None)
            if first_chunk:
                create_indexes(con, table, list(chunk.columns))
                first_chunk = False

    con.commit()
    con.close()
    print(f"[DONE] -> {db_path}")

if __name__ == "__main__":
    main()
 
