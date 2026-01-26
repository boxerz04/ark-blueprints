# -*- coding: utf-8 -*-
"""
preprocess_motor_section.py

master / live CSV に対して、
motor_id × section_id の参照テーブル
(motor_section_features_n__all.csv) を LEFT JOIN し、
motor 系特徴量列を付与する。

重要な設計方針:
- section_id は upstream で厳密に生成されている前提
- 本スクリプトでは section_id を「生成しない」「推測しない」
- section_id が存在しない場合は即エラーとする
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import pandas as pd


def _clean_motor_id(series: pd.Series) -> pd.Series:
    """motor_id を JOIN 安定化のため文字列へ正規化"""
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})
    return s


def _normalize_section_id(series: pd.Series) -> pd.Series:
    """
    section_id を 'YYYYMMDD_CC' 形式に正規化
    ※ 生成はしない。形式補正（zfill）のみ。
    """
    s = series.astype(str).str.strip()
    s = s.replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})

    def fix_one(x: str):
        if x is pd.NA:
            return x
        if "_" not in x:
            return x
        head, tail = x.split("_", 1)
        if re.fullmatch(r"\d{1,2}", tail):
            return f"{head}_{tail.zfill(2)}"
        return x

    return s.map(fix_one)


def _select_feature_columns(df: pd.DataFrame, key_cols: tuple[str, str]) -> pd.DataFrame:
    """motor / prev / delta 系のみを付与対象にする"""
    k1, k2 = key_cols
    cols = [k1, k2]

    for c in df.columns:
        if c in cols:
            continue
        if c.startswith("motor_") or c.startswith("prev") or c.startswith("delta_"):
            cols.append(c)

    if len(cols) <= 2:
        raise ValueError("no motor feature columns detected in reference table")

    return df[cols].copy()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in_csv", required=True)
    ap.add_argument(
        "--motor_features_csv",
        default=r"data\processed\motor\motor_section_features_n__all.csv",
    )
    ap.add_argument("--out_csv", default="")
    ap.add_argument("--motor_id_col", default="motor_id")
    ap.add_argument("--section_id_col", default="section_id")
    ap.add_argument(
        "--keep_existing",
        action="store_true",
        help="既存の motor 特徴量列を上書きしない",
    )
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    ref_path = Path(args.motor_features_csv)
    out_path = Path(args.out_csv) if args.out_csv else in_path

    print("[START] preprocess_motor_section.py")
    print(f"[IN ] {in_path}")
    print(f"[REF] {ref_path}")
    print(f"[OUT] {out_path}")

    if not in_path.exists():
        raise FileNotFoundError(f"input not found: {in_path}")
    if not ref_path.exists():
        raise FileNotFoundError(f"reference not found: {ref_path}")

    df = pd.read_csv(in_path, low_memory=False)
    print(f"[INFO] input rows={len(df):,} cols={len(df.columns)}")

    # 必須列チェック（妥協しない）
    for c in (args.motor_id_col, args.section_id_col):
        if c not in df.columns:
            raise ValueError(f"missing required column in input: {c}")

    ref = pd.read_csv(ref_path, low_memory=False)
    print(f"[INFO] ref rows={len(ref):,} cols={len(ref.columns)}")

    for c in (args.motor_id_col, args.section_id_col):
        if c not in ref.columns:
            raise ValueError(f"missing required column in reference: {c}")

    # 正規化（生成はしない）
    df[args.motor_id_col] = _clean_motor_id(df[args.motor_id_col])
    df[args.section_id_col] = _normalize_section_id(df[args.section_id_col])
    ref[args.motor_id_col] = _clean_motor_id(ref[args.motor_id_col])
    ref[args.section_id_col] = _normalize_section_id(ref[args.section_id_col])

    ref = _select_feature_columns(ref, (args.motor_id_col, args.section_id_col))
    feature_cols = [c for c in ref.columns if c not in (args.motor_id_col, args.section_id_col)]
    print(f"[INFO] attach feature cols: {len(feature_cols)}")

    if args.keep_existing:
        feature_cols = [c for c in feature_cols if c not in df.columns]
        ref = ref[[args.motor_id_col, args.section_id_col] + feature_cols]
        print(f"[INFO] keep_existing=ON -> attach cols reduced to {len(feature_cols)}")

    before_cols = set(df.columns)
    out = df.merge(
        ref,
        on=[args.motor_id_col, args.section_id_col],
        how="left",
        validate="m:1",
    )

    if feature_cols:
        miss_rate = out[feature_cols[0]].isna().mean() * 100
        print(f"[QC] join miss rate: {miss_rate:.2f}%")

    print(f"[INFO] output cols={len(out.columns)} (+{len(out.columns - before_cols)})")
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote: {out_path}")
    print("[DONE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
