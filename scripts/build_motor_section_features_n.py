# -*- coding: utf-8 -*-
"""
scripts/build_motor_section_features_n.py

motor_section_base__all.csv から、節単位の派生特徴量（prev / mean / sum / delta）を生成する。

【フェーズ2（本スクリプトのデフォルト）】
- prev1（直前節）
- prev3_sum / prev3_mean（直近3節）
- prev5_sum / prev5_mean（直近5節）
- delta_1_3 / delta_1_5（方向性：prev1 - mean）

【重要】
- すべて shift(1) した過去節のみで計算（当該節情報は混ぜない = リーク回避）
- 平均は「単純平均」（節を同列扱い）で、選手依存ノイズをならす意図
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List

import pandas as pd


# --------------------------------------------------
# utility
# --------------------------------------------------
def parse_int_list(s: str) -> List[int]:
    xs = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        xs.append(int(part))
    if not xs:
        raise ValueError(f"invalid mean_ns: {s}")
    return xs


def ensure_cols(df: pd.DataFrame, cols: List[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")


# ★ 追加（最小）：id 正規化ユーティリティ
def _normalize_motor_id(s: pd.Series) -> pd.Series:
    # 11101 / 11101.0 / " 11101 " → "011101"
    x = s.astype("string").str.strip()
    x = x.str.replace(r"\.0$", "", regex=True)
    x = x.str.replace(r"\D", "", regex=True)
    x = x.replace("", pd.NA)
    x = x.where(x.isna(), x.str.zfill(6))
    return x


def _normalize_section_id(s: pd.Series) -> pd.Series:
    # 20240928_3 / 20240928-3 → 20240928_03
    x = s.astype("string").str.strip()
    x = x.str.replace("-", "_", regex=False)

    pat = re.compile(r"^(\d{8})_(\d{1,2})$")

    def _fix(v):
        if v is None or v is pd.NA:
            return pd.NA
        v = str(v)
        m = pat.match(v)
        if not m:
            return v
        d, n = m.group(1), m.group(2)
        return f"{d}_{int(n):02d}"

    x = x.replace("", pd.NA).map(_fix)
    return x


# --------------------------------------------------
# main
# --------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        required=True,
        help="motor_section_base__all.csv",
    )
    ap.add_argument(
        "--out_csv",
        required=True,
        help="output features csv",
    )

    # キー列
    ap.add_argument("--motor_id_col", default="motor_id")
    ap.add_argument("--section_id_col", default="section_id")

    # 節の並び順に使う列（両方ある前提）
    ap.add_argument("--start_dt_col", default="section_start_dt")
    ap.add_argument("--end_dt_col", default="section_end_dt")

    # フェーズ2：mean対象の節数（複数指定可能）
    ap.add_argument("--mean_ns", default="3,5", help="e.g. 3,5")

    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.out_csv)

    if not in_path.exists():
        print(f"[ERROR] input not found: {in_path}")
        return 1

    mean_ns = parse_int_list(args.mean_ns)
    mean_ns = sorted(set(mean_ns))

    print("[START] build_motor_section_features_n.py")
    print(f"[DEBUG] script_path={Path(__file__).resolve()}")

    print(f"[INPUT] {in_path}")
    print(f"[OUTPUT] {out_path}")
    print(f"[CONF] mean_ns={mean_ns}")

    # ★ 変更なし：読み込み
    df = pd.read_csv(in_path, low_memory=False)
    print(f"[INFO] rows={len(df):,} cols={len(df.columns)}")

    # 必須列
    base_required = [
        args.motor_id_col,
        args.section_id_col,
        args.start_dt_col,
        args.end_dt_col,
    ]
    ensure_cols(df, base_required)

    # 対象列（ここを増やすのは簡単）
    value_cols = [
        "motor_race_ct",
        "motor_score_sum",
        "motor_ranking_point_sum",
        "motor_condition_point_sum",
        "motor_score_rate",
        "motor_ranking_point_rate",
        "motor_condition_point_rate",
    ]
    ensure_cols(df, value_cols)

    print("[INFO] target value columns:")
    for c in value_cols:
        print(f"  - {c}")

    # ★ 追加（最小）：ここでだけ id 正規化
    df = df.copy()
    df[args.motor_id_col] = _normalize_motor_id(df[args.motor_id_col])
    df[args.section_id_col] = _normalize_section_id(df[args.section_id_col])
    print("[DEBUG] motor_id head:", df[args.motor_id_col].head(5).tolist())


    # 日付型へ（並び順とQCで使う）
    df[args.start_dt_col] = pd.to_datetime(df[args.start_dt_col], errors="coerce")
    df[args.end_dt_col] = pd.to_datetime(df[args.end_dt_col], errors="coerce")

    # 数値型（安全のため）
    for c in value_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # 並び順：motorごとに節開始→節終了→section_id
    df = df.sort_values(
        [args.motor_id_col, args.start_dt_col, args.end_dt_col, args.section_id_col],
        kind="mergesort",
    ).reset_index(drop=True)

    g = df.groupby(args.motor_id_col, sort=False)

    # --------------------------------------------------
    # prev1（直前節）
    # --------------------------------------------------
    print("[STEP] build prev1")
    for c in value_cols:
        df[f"prev1_{c}"] = g[c].shift(1)

    # --------------------------------------------------
    # rolling（mean/sum）: 必ず shift(1) の後に rolling
    # --------------------------------------------------
    sum_cols = [
        "motor_race_ct",
        "motor_score_sum",
        "motor_ranking_point_sum",
        "motor_condition_point_sum",
    ]

    mean_cols = [
        "motor_score_rate",
        "motor_ranking_point_rate",
        "motor_condition_point_rate",
    ]

    for n in mean_ns:
        print(f"[STEP] build prev{n}_sum / prev{n}_mean")
        for c in sum_cols:
            s = g[c].shift(1)
            df[f"prev{n}_sum_{c}"] = s.groupby(df[args.motor_id_col], sort=False).rolling(
                window=n, min_periods=n
            ).sum().reset_index(level=0, drop=True)

        for c in mean_cols:
            s = g[c].shift(1)
            df[f"prev{n}_mean_{c}"] = s.groupby(df[args.motor_id_col], sort=False).rolling(
                window=n, min_periods=n
            ).mean().reset_index(level=0, drop=True)

    # --------------------------------------------------
    # delta（方向性）
    # --------------------------------------------------
    def has(col: str) -> bool:
        return col in df.columns

    print("[STEP] build delta (prev1 - mean)")
    for n in mean_ns:
        for c in mean_cols:
            mcol = f"prev{n}_mean_{c}"
            pcol = f"prev1_{c}"
            dcol = f"delta_1_{n}_{c}"
            if has(mcol) and has(pcol):
                df[dcol] = df[pcol] - df[mcol]

    # --------------------------------------------------
    # 出力列の整理
    # --------------------------------------------------
    out_cols = [
        args.motor_id_col,
        args.section_id_col,
        args.start_dt_col,
        args.end_dt_col,
    ] + value_cols

    out_cols += [f"prev1_{c}" for c in value_cols]

    for n in mean_ns:
        out_cols += [f"prev{n}_sum_{c}" for c in sum_cols]
        out_cols += [f"prev{n}_mean_{c}" for c in mean_cols]

    for n in mean_ns:
        out_cols += [f"delta_1_{n}_{c}" for c in mean_cols if f"delta_1_{n}_{c}" in df.columns]

    df[args.start_dt_col] = df[args.start_dt_col].dt.strftime("%Y-%m-%d")
    df[args.end_dt_col] = df[args.end_dt_col].dt.strftime("%Y-%m-%d")

    out = df[out_cols].copy()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"[OK] wrote: {out_path}")
    print("[DONE]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
