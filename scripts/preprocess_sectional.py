# scripts/preprocess_sectional.py
# -----------------------------------------------------------------------------
# master（または live）CSV に「節間（raceinfo）由来の列」を“上書き付与”するスクリプト
# 役割:
#   - (race_id, player_id) をキーに many-to-one LEFT JOIN
#   - 既存列は壊さず、新規列のみを追加（同名は master 優先で raceinfo 側を捨てる）
#   - 結果列（*_flag_cur 等のリーク列）は結合前に強制除外
#   - 主キーは最初から文字列で読み込み、DtypeWarning を回避
#
# 使い方（学習・期間指定）:
#   python scripts/preprocess_sectional.py \
#     --master data/processed/master.csv \
#     --raceinfo-dir data/processed/raceinfo \
#     --start-date 2024-12-01 --end-date 2025-09-30 \
#     --out data/processed/master.csv
#
# 使い方（推論・単日）:
#   python scripts/preprocess_sectional.py \
#     --master data/live/raw_YYYYMMDD_JCD_R.csv \
#     --raceinfo-dir data/processed/raceinfo \
#     --date 2025-10-20 \
#     --out data/live/raw_YYYYMMDD_JCD_R.csv
# -----------------------------------------------------------------------------

import argparse
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
import pandas as pd

# ====== CLI ===================================================================
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="data/processed/master.csv")
    ap.add_argument("--raceinfo-dir", default="data/processed/raceinfo")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="YYYY-MM-DD")
    g.add_argument("--start-date", help="YYYY-MM-DD")
    ap.add_argument("--end-date", help="YYYY-MM-DD")
    ap.add_argument("--out", default="data/processed/master.csv")
    return ap.parse_args()

# ====== Helpers ================================================================
def _to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def _date_range(date: Optional[str], start: Optional[str], end: Optional[str]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    if date:
        d = pd.to_datetime(date)
        return d.normalize(), d.normalize()
    if start is None or end is None:
        raise ValueError("When --date is not given, both --start-date and --end-date are required.")
    return pd.to_datetime(start).normalize(), pd.to_datetime(end).normalize()

def _list_csvs(root: Path) -> List[Path]:
    return sorted([p for p in root.rglob("*.csv") if p.is_file()])

def _safe_read_csv(path: Path, key_dtypes=True) -> Optional[pd.DataFrame]:
    try:
        if key_dtypes:
            df = pd.read_csv(path, low_memory=False, dtype={"race_id": "string", "player_id": "string"})
        else:
            df = pd.read_csv(path, low_memory=False)
        return df
    except Exception as e:
        print(f"[WARN] skip read: {path} err={e}")
        return None

# ====== Core ==================================================================
def load_raceinfo(dir_path: Path, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    """
    raceinfo ディレクトリ配下の CSV を読み、date 列（or yyyymmdd 列）が範囲に入るものだけに絞る。
    date 列が無い場合はファイル名から YYYYMMDD を推測（best-effort）。
    """
    paths = _list_csvs(dir_path)
    if not paths:
        print(f"[INFO] no raceinfo csv under: {dir_path}")
        return pd.DataFrame(columns=["race_id","player_id"])

    out_frames: List[pd.DataFrame] = []
    for p in paths:
        df = _safe_read_csv(p, key_dtypes=True)
        if df is None or df.empty:
            continue

        # 日付列の解釈（date or yyyymmdd）
        date_col = None
        for cand in ("date", "yyyymmdd", "race_date"):
            if cand in df.columns:
                date_col = cand
                break

        # ファイル単位の粗フィルタ（date 列が無い場合はファイル名から推測）
        keep_df = True
        if date_col:
            dt = _to_datetime(df[date_col])
            mask = (dt >= start_ts) & (dt <= end_ts)
            df = df.loc[mask].copy()
            keep_df = not df.empty
        else:
            # ファイル名から YYYYMMDD を拾う（見つからなければ採用）
            name = p.stem
            ymd = None
            for token in name.replace("-", "_").split("_"):
                if token.isdigit() and len(token) == 8:
                    try:
                        ymd = pd.to_datetime(token)
                        break
                    except Exception:
                        pass
            if ymd is not None:
                keep_df = (ymd >= start_ts) and (ymd <= end_ts)

        if keep_df and not df.empty:
            out_frames.append(df)

    if not out_frames:
        print(f"[INFO] raceinfo matched 0 rows in {dir_path} by date range {start_ts.date()}..{end_ts.date()}")
        return pd.DataFrame(columns=["race_id","player_id"])

    ri = pd.concat(out_frames, axis=0, ignore_index=True)

    # 主キーの整備（文字列・strip）
    for k in ("race_id","player_id"):
        if k in ri.columns:
            ri[k] = ri[k].astype("string").str.strip()

    # リーク列（結果）を強制 drop
    LEAK_DROP = ["finish1_flag_cur","finish2_flag_cur","finish3_flag_cur"]
    ri = ri.drop(columns=[c for c in LEAK_DROP if c in ri.columns], errors="ignore")

    # many-to-one 化（重複キーは最後勝ち）
    if "race_id" in ri.columns and "player_id" in ri.columns:
        ri = ri.drop_duplicates(subset=["race_id","player_id"], keep="last")

    return ri

def main():
    args = parse_args()
    master_path = Path(args.master)
    out_path    = Path(args.out)
    ri_root     = Path(args.raceinfo_dir)

    start_ts, end_ts = _date_range(args.date, args.start_date, args.end_date)
    print(f"[INFO] master : {master_path}")
    print(f"[INFO] raceinfo: {ri_root}")
    print(f"[INFO] period  : {start_ts.date()} .. {end_ts.date()} (inclusive)")

    # master を“最初から文字列 dtype”で読み、警告を抑止
    master = pd.read_csv(
        master_path,
        low_memory=False,
        dtype={"race_id": "string", "player_id": "string"}
    )
    before_rows, before_cols = master.shape
    print(f"[INFO] master shape: {master.shape}")

    # キーの整備
    for k in ("race_id","player_id"):
        if k not in master.columns:
            raise RuntimeError(f"missing key column in master: {k}")
        master[k] = master[k].astype("string").str.strip()

    # raceinfo 読み込み
    ri = load_raceinfo(ri_root, start_ts, end_ts)
    print(f"[INFO] raceinfo shape: {ri.shape}")

    if ri.empty:
        # 追加なしでそのまま出力
        out_path.parent.mkdir(parents=True, exist_ok=True)
        master.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] wrote: {out_path} rows={master.shape[0]} cols={master.shape[1]} (no raceinfo to attach)")
        return

    # 結合キー確認
    if not {"race_id","player_id"}.issubset(ri.columns):
        print("[WARN] raceinfo has no keys (race_id, player_id). nothing to attach.")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        master.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] wrote: {out_path} rows={master.shape[0]} cols={master.shape[1]} (unchanged)")
        return

    # 既存列と衝突する列は raceinfo 側を落として“新規列のみ”採用
    key_cols = {"race_id","player_id"}
    ri_add_cols = [c for c in ri.columns if c not in key_cols and c not in master.columns]
    ri_small = ri[list(key_cols) + ri_add_cols].copy()

    # LEFT JOIN（many_to_one を期待）
    before_set = set(master.columns)
    merged = master.merge(ri_small, on=["race_id","player_id"], how="left", validate="many_to_one")
    added_cols = sorted(list(set(merged.columns) - before_set))

    # 保存
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote: {out_path} rows={merged.shape[0]} cols={merged.shape[1]}")
    print(f"[OK] added_cols: {len(added_cols)} -> {', '.join(added_cols[:20])}{' ...' if len(added_cols)>20 else ''}")

if __name__ == "__main__":
    main()
