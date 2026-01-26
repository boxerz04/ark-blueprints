# -*- coding: utf-8 -*-
"""
preprocess_motor_id.py  (date-based, map direct) - STRING SAFE FIX

master / live CSV に対して、
motor_id_map__all.csv を参照し、
(code, motor_number, date) + effective_from/to により motor_id を付与する。

重要:
- merge 後に元行が崩れるため、_row_id で元行単位に集約
- effective_to が空の場合は「現在まで有効」として扱う（∞）
- 欠損・複数候補は即エラー（推測・補完なし）
- motor_id は **常に string として保持する（今回の修正点）**
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import re


def require_cols(df: pd.DataFrame, cols: list[str], name: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"missing required columns in {name}: {missing}")


def clean_code(s: pd.Series) -> pd.Series:
    s = s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    return s.map(lambda x: x.zfill(2) if re.fullmatch(r"\d{1,2}", x) else x)


def clean_motor_number(s: pd.Series) -> pd.Series:
    return s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def clean_motor_id(s: pd.Series) -> pd.Series:
    """
    motor_id を ID として正規化
    - string 化
    - '.0' 除去
    - 数字のみなら 6 桁ゼロ埋め
    """
    s = s.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    s = s.where(s.isna() | ~s.str.fullmatch(r"\d+"), s.str.zfill(6))
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument("--map_csv", default=r"data\processed\motor\motor_id_map__all.csv")
    ap.add_argument("--out_csv", default="")
    ap.add_argument("--date_col", default="date")
    ap.add_argument("--code_col", default="code")
    ap.add_argument("--motor_number_col", default="motor_number")
    ap.add_argument("--max_miss_rate", type=float, default=0.0)

    args = ap.parse_args()

    in_path = Path(args.in_csv)
    map_path = Path(args.map_csv)
    out_path = Path(args.out_csv) if args.out_csv else in_path

    print("[START] preprocess_motor_id.py (date based, STRING SAFE)")
    print(f"[IN ] {in_path}")
    print(f"[MAP] {map_path}")
    print(f"[OUT] {out_path}")

    if not in_path.exists():
        raise FileNotFoundError(in_path)
    if not map_path.exists():
        raise FileNotFoundError(map_path)

    # =========
    # 入力CSV
    # =========
    df = pd.read_csv(in_path, low_memory=False)
    print(f"[INFO] input rows={len(df):,}")

    require_cols(df, [args.date_col, args.code_col, args.motor_number_col], "input")

    df = df.copy()
    df["_row_id"] = df.index

    df["_date"] = pd.to_datetime(df[args.date_col], errors="coerce")
    if df["_date"].isna().any():
        bad = df.loc[df["_date"].isna(), [args.date_col]].head(20)
        raise ValueError(f"date parse failed in input. sample:\n{bad}")

    df["_code"] = clean_code(df[args.code_col])
    df["_motor_number"] = clean_motor_number(df[args.motor_number_col])

    # =========
    # motor_id_map（★ここが重要）
    # =========
    mp = pd.read_csv(
        map_path,
        low_memory=False,
        dtype={
            "code": "string",
            "motor_number": "string",
            "motor_id": "string",          # ★ FIX: 明示的に string
            "effective_from": "string",
            "effective_to": "string",
        },
    )

    require_cols(mp, ["code", "motor_number", "motor_id", "effective_from", "effective_to"], "motor_id_map")

    mp = mp.copy()
    mp["_code"] = clean_code(mp["code"])
    mp["_motor_number"] = clean_motor_number(mp["motor_number"])
    mp["motor_id"] = clean_motor_id(mp["motor_id"])   # ★ FIX: 正規化

    mp["_from"] = pd.to_datetime(mp["effective_from"], errors="coerce")
    if mp["_from"].isna().any():
        bad = mp.loc[mp["_from"].isna(), ["effective_from", "code", "motor_number"]].head(20)
        raise ValueError(f"effective_from parse failed in motor_id_map. sample:\n{bad}")

    mp["_to"] = pd.to_datetime(mp["effective_to"], errors="coerce")
    mp["_to"] = mp["_to"].fillna(pd.Timestamp.max)

    # =========
    # merge
    # =========
    merged = df.merge(
        mp[["_code", "_motor_number", "_from", "_to", "motor_id"]],
        on=["_code", "_motor_number"],
        how="left",
        validate="m:m",
    )

    in_period = (merged["_from"] <= merged["_date"]) & (merged["_date"] <= merged["_to"])
    merged.loc[~in_period, "motor_id"] = pd.NA

    cand_nunique = merged.groupby("_row_id")["motor_id"].nunique(dropna=True)
    if (cand_nunique > 1).any():
        ex_ids = cand_nunique[cand_nunique > 1].head(10).index.tolist()
        ex = merged.loc[
            merged["_row_id"].isin(ex_ids),
            [args.date_col, args.code_col, args.motor_number_col, "motor_id", "_from", "_to"],
        ]
        raise ValueError(f"multiple motor_id candidates found. sample:\n{ex.head(50)}")

    motor_id_by_row = merged.groupby("_row_id")["motor_id"].first()

    df["motor_id"] = clean_motor_id(df["_row_id"].map(motor_id_by_row))  # ★ FIX: 最後に再度正規化

    miss_rate = df["motor_id"].isna().mean() * 100
    print(f"[QC] motor_id miss rate: {miss_rate:.3f}%")

    if miss_rate > args.max_miss_rate:
        bad = df.loc[df["motor_id"].isna(), [args.date_col, args.code_col, args.motor_number_col]].head(30)
        raise ValueError(f"motor_id missing rows:\n{bad}")

    out = df.drop(columns=["_row_id", "_date", "_code", "_motor_number"])
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[OK] wrote: {out_path}")
    print("[DONE]")


if __name__ == "__main__":
    main()
