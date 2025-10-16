# scripts/export_vault.py
# Vault(DB) → 元ファイルをそのまま復元（gzipは自動解凍）
from __future__ import annotations
import argparse, gzip, sqlite3
from pathlib import Path
try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k): return x

def main():
    ap = argparse.ArgumentParser(description="Export files from a Vault SQLite DB (object_store/file_index).")
    ap.add_argument("--db", required=True, help="input sqlite (e.g., data/sqlite/csv_vault_2025Q3_compact.sqlite)")
    ap.add_argument("--dest", required=True, help="output directory (created if needed)")
    ap.add_argument("--pattern", default=None, help="LIKE pattern for rel_path (e.g., %.csv)")
    ap.add_argument("--limit", type=int, default=0, help="export at most N files")
    args = ap.parse_args()

    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row

    where = "WHERE 1=1"
    params = []
    if args.pattern:
        where += " AND rel_path LIKE ?"
        params.append(args.pattern)

    limit_sql = f" LIMIT {int(args.limit)}" if args.limit else ""
    sql = f"""SELECT rel_path, is_gzip, bytes
              FROM file_index JOIN object_store USING(sha256)
              {where}
              ORDER BY rel_path{limit_sql};"""
    rows = con.execute(sql, params).fetchall()

    for r in tqdm(rows, desc="Exporting"):
        out_path = dest / Path(r["rel_path"])
        out_path.parent.mkdir(parents=True, exist_ok=True)
        data = r["bytes"]
        if r["is_gzip"]:
            data = gzip.decompress(data)
        out_path.write_bytes(data)

    con.close()
    print(f"[DONE] export {len(rows)} file(s) to {dest}")

if __name__ == "__main__":
    main()
