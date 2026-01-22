# -*- coding: utf-8 -*-
"""
scripts/build_motor_section_base.py

FULL joined（raw_with_motor__all）から、
motor_id × section_id の「節確定・不変」モーター特徴量の正本を作成する。

【設計方針】
- 1行 = motor_id × section_id（節単位）
- prev_* は作らない（派生は別スクリプト）
- dns（is_start==False）は分母から除外
- void（rank_class=="void"）は安全弁として除外
- entry と wakuban は混同しない（進入は entry を使用）
- 正本は全期間一塊（motor_section_base__all.csv）

【入力に必須の列（raw_with_motor__all.csv）】
- motor_id
- section_id
- date_dt
- entry
- rank_num
- is_start
- rank_class

【出力】
- motor_section_base__all.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


# ==================================================
# 点数定義（player_id 特徴量と同型）
# ==================================================
SCORE_MAP = {1: 10, 2: 8, 3: 6, 4: 4, 5: 2, 6: 1}

RANKING_POINT_MAP = {
    1: {1: 0, 2: -1, 3: -2, 4: -3, 5: -4, 6: -5},
    2: {1: 1, 2: 0, 3: -1, 4: -2, 5: -3, 6: -4},
    3: {1: 2, 2: 1, 3: 0, 4: -1, 5: -2, 6: -3},
    4: {1: 3, 2: 2, 3: 1, 4: 0, 5: -1, 6: -2},
    5: {1: 4, 2: 3, 3: 2, 4: 1, 5: 0, 6: -1},
    6: {1: 5, 2: 4, 3: 3, 4: 2, 5: 1, 6: 0},
}

CONDITION_POINT_MAP = {
    1: {1: 2, 2: -1, 3: -2, 4: -3, 5: -4, 6: -5},
    2: {1: 1, 2: 1, 3: -1, 4: -2, 5: -3, 6: -4},
    3: {1: 2, 2: 1, 3: 1, 4: -1, 5: -2, 6: -3},
    4: {1: 3, 2: 2, 3: 1, 4: 0, 5: -1, 6: -2},
    5: {1: 4, 2: 3, 3: 2, 4: 1, 5: 0, 6: -1},
    6: {1: 5, 2: 4, 3: 3, 4: 2, 5: 1, 6: -1},
}


# ==================================================
# utility
# ==================================================
def to_int_1to6(series: pd.Series) -> pd.Series:
    """
    entry / rank_num を 1..6 の Int64 に正規化
    """
    s = pd.to_numeric(series, errors="coerce")
    s = s.round().astype("Int64")
    return s.where(s.between(1, 6), pd.NA)


def parse_bool(series: pd.Series) -> pd.Series:
    """
    is_start を bool に正規化
    """
    if pd.api.types.is_bool_dtype(series):
        return series
    s = series.astype("string").str.lower()
    return s.isin(["true", "1", "t", "yes", "y"])


def build_point_table() -> pd.DataFrame:
    rows = []
    for e in range(1, 7):
        for r in range(1, 7):
            rows.append((e, r,
                         RANKING_POINT_MAP[e][r],
                         CONDITION_POINT_MAP[e][r]))
    return pd.DataFrame(
        rows,
        columns=["entry", "rank_num", "ranking_point", "condition_point"]
    )


# ==================================================
# main
# ==================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True,
                    help="raw_with_motor__all.csv / parquet")
    ap.add_argument("--out_csv", required=True,
                    help="motor_section_base__all.csv")

    # 列名（明示固定）
    ap.add_argument("--motor_id_col", default="motor_id")
    ap.add_argument("--section_id_col", default="section_id")
    ap.add_argument("--date_col", default="date_dt")
    ap.add_argument("--entry_col", default="entry")
    ap.add_argument("--rank_num_col", default="rank_num")
    ap.add_argument("--is_start_col", default="is_start")
    ap.add_argument("--rank_class_col", default="rank_class")

    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.out_csv)

    if not in_path.exists():
        print(f"[ERROR] input not found: {in_path}")
        return 1

    print("[START] build_motor_section_base.py")
    print(f"[INPUT] {in_path}")
    print(f"[OUTPUT] {out_path}")

    # ---- load
    if in_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(in_path)
    else:
        df = pd.read_csv(in_path, low_memory=False)

    print(f"[INFO] rows={len(df):,} cols={len(df.columns)}")

    # ---- required columns
    required = [
        args.motor_id_col,
        args.section_id_col,
        args.date_col,
        args.entry_col,
        args.rank_num_col,
        args.is_start_col,
        args.rank_class_col,
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"[ERROR] missing columns: {missing}")
        return 1

    # ---- normalize
    df = df.copy()
    df[args.date_col] = pd.to_datetime(df[args.date_col], errors="coerce")

    before = len(df)
    df = df[df[args.motor_id_col].notna() &
            df[args.section_id_col].notna()]
    print(f"[INFO] drop NA motor/section: {before:,} -> {len(df):,}")

    # void safeguard
    before = len(df)
    df = df[df[args.rank_class_col] != "void"]
    print(f"[INFO] drop void rows: {before:,} -> {len(df):,}")

    # is_start
    df["_is_start"] = parse_bool(df[args.is_start_col])

    # entry / rank
    df["_entry"] = to_int_1to6(df[args.entry_col])
    df["_rank_num"] = to_int_1to6(df[args.rank_num_col])

    denom = df[df["_is_start"]].copy()
    print(f"[INFO] denom rows: {len(denom):,}")

    # ---- score
    denom["_score"] = denom["_rank_num"].map(SCORE_MAP).fillna(0).astype("int64")

    # ---- ranking / condition
    pt = build_point_table()
    tmp = denom.merge(
        pt,
        how="left",
        left_on=["_entry", "_rank_num"],
        right_on=["entry", "rank_num"],
    )
    tmp["ranking_point"] = tmp["ranking_point"].fillna(0).astype("int64")
    tmp["condition_point"] = tmp["condition_point"].fillna(0).astype("int64")

    # ---- aggregate (正本)
    gcols = [args.motor_id_col, args.section_id_col]
    agg = tmp.groupby(gcols).agg(
        motor_race_ct=("_score", "size"),
        motor_score_sum=("_score", "sum"),
        motor_ranking_point_sum=("ranking_point", "sum"),
        motor_condition_point_sum=("condition_point", "sum"),
        section_start_dt=(args.date_col, "min"),
        section_end_dt=(args.date_col, "max"),
    ).reset_index()

    agg["motor_score_rate"] = agg["motor_score_sum"] / agg["motor_race_ct"]
    agg["motor_ranking_point_rate"] = (
        agg["motor_ranking_point_sum"] / agg["motor_race_ct"]
    )
    agg["motor_condition_point_rate"] = (
        agg["motor_condition_point_sum"] / agg["motor_race_ct"]
    )

    print(f"[INFO] sections aggregated: {len(agg):,}")

    # ---- date format
    for c in ["section_start_dt", "section_end_dt"]:
        agg[c] = pd.to_datetime(agg[c]).dt.strftime("%Y-%m-%d")

    # ---- output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[OK] wrote: {out_path}")
    print("[DONE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
