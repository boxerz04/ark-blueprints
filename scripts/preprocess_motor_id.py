#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step2.75: master.csv に motor_id を付与する（区間結合 / groupごと merge_asof）

結合条件（論理）:
- code
- motor_number
- effective_from <= date <= effective_to
  ※ effective_to が NaT の場合は「以後ずっと有効（上限なし）」として扱う

入力:
- master.csv（data/processed/master.csv）
- motor_id_map.csv（data/motor/map/motor_id_map__YYYYMMDD_YYYYMMDD.csv）

出力:
- デフォルト: master__with_motor_id.csv（masterは上書きしない）
- --inplace: master.csv を上書き（テスト後に使用推奨）

今回の修正点（重要）:
- motor_id_map の motor_id を read_csv 時点で文字列として読む（float化を防ぐ）
- merge_asof 後に motor_id を master へ代入する“直前”に
  「.0除去 + 6桁0埋め + 文字列化」を必ず行い、以後 motor_id が壊れないようにする
- suffix列（code_x / motor_number_x など）を復元して列ダブりを防止
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import List, Optional

import pandas as pd


REQ_MASTER = ["date", "code", "motor_number"]
REQ_MAP = ["code", "motor_number", "effective_from", "effective_to", "motor_id"]


def _require_cols(df: pd.DataFrame, cols: List[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"[ERROR] {name} is missing required columns: {missing}")


def _to_datetime_allow_nat(s: pd.Series, colname: str, allow_nat: bool) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    if (not allow_nat) and dt.isna().any():
        na = int(dt.isna().sum())
        raise ValueError(f"[ERROR] {colname}: failed to parse datetime for {na} rows.")
    return dt


def _ensure_int64_allow_na(s: pd.Series) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce")
    return out.astype("Int64")


def normalize_motor_id_series(s: pd.Series) -> pd.Series:
    """
    motor_id の表記揺れを正規化する（必ず6桁文字列にする）。
    - NaN/NA -> <NA>
    - 31002.0 -> 31002
    - 前後空白除去
    - 数字のみなら zfill(6)
    """
    # まず文字列化（欠損は 'nan' になり得るので後で落とす）
    x = s.astype("string")

    # 前後空白除去
    x = x.str.strip()

    # ".0" 末尾を除去（"31002.0" -> "31002"）
    x = x.str.replace(r"\.0$", "", regex=True)

    # "nan" / "" を欠損扱いへ
    x = x.where(~x.isin(["", "nan", "NaN", "<NA>"]), pd.NA)

    # 数字だけのものだけ zfill(6)
    # pandas string のままだと apply が最も分かりやすく安全
    def _zfill_if_digits(v: Optional[str]) -> Optional[str]:
        if v is None or v is pd.NA:
            return pd.NA
        if isinstance(v, str) and v.isdigit():
            return v.zfill(6)
        return v

    x = x.apply(_zfill_if_digits).astype("string")
    return x


def attach_motor_id_group_asof(
    master: pd.DataFrame,
    motor_map: pd.DataFrame,
    overwrite: bool,
) -> pd.DataFrame:
    _require_cols(master, REQ_MASTER, "master")
    _require_cols(motor_map, REQ_MAP, "motor_id_map")

    if (not overwrite) and ("motor_id" in master.columns):
        raise ValueError(
            "[ERROR] master already has 'motor_id'. Use --overwrite to replace it, "
            "or drop the column before running."
        )

    master = master.copy()
    motor_map = motor_map.copy()

    # ---- 型揃え（master）----
    master["date"] = _to_datetime_allow_nat(master["date"], "master.date", allow_nat=False)
    master["code"] = _ensure_int64_allow_na(master["code"])
    master["motor_number"] = _ensure_int64_allow_na(master["motor_number"])

    # ---- 型揃え（map）----
    motor_map["effective_from"] = _to_datetime_allow_nat(motor_map["effective_from"], "motor_id_map.effective_from", allow_nat=False)
    motor_map["effective_to"] = _to_datetime_allow_nat(motor_map["effective_to"], "motor_id_map.effective_to", allow_nat=True)
    motor_map["code"] = _ensure_int64_allow_na(motor_map["code"])
    motor_map["motor_number"] = _ensure_int64_allow_na(motor_map["motor_number"])

    # motor_id はここで必ず正規化（read_csvで文字列指定していても保険）
    motor_map["motor_id"] = normalize_motor_id_series(motor_map["motor_id"])

    # motor_id 結果列
    if overwrite and "motor_id" in master.columns:
        master["motor_id"] = pd.NA
    elif "motor_id" not in master.columns:
        master["motor_id"] = pd.NA

    # キー欠損行は motor_id 欠損のまま残す
    key_ok = master["code"].notna() & master["motor_number"].notna() & master["date"].notna()
    master_ok = master.loc[key_ok].copy()
    master_ng = master.loc[~key_ok].copy()

    # ソート（merge_asof 前提）
    master_ok.sort_values(["code", "motor_number", "date"], inplace=True, kind="mergesort")

    motor_map = motor_map[["code", "motor_number", "effective_from", "effective_to", "motor_id"]].copy()
    motor_map.sort_values(["code", "motor_number", "effective_from"], inplace=True, kind="mergesort")

    out_of_range_count = 0
    asof_matched_count = 0

    merged_parts: List[pd.DataFrame] = []

    # groupごと asof
    for (code, mnum), g in master_ok.groupby(["code", "motor_number"], sort=False):
        mg = motor_map[(motor_map["code"] == code) & (motor_map["motor_number"] == mnum)]
        if mg.empty:
            merged_parts.append(g)
            continue

        g = g.sort_values("date", kind="mergesort")
        mg = mg.sort_values("effective_from", kind="mergesort")

        tmp = pd.merge_asof(
            g,
            mg,
            left_on="date",
            right_on="effective_from",
            direction="backward",
            allow_exact_matches=True,
        )

        # 範囲判定
        ok_range = tmp["effective_to"].isna() | (tmp["date"] <= tmp["effective_to"])

        # asof一致/区間外のカウント（motor_id_y が欠損でないものをasof一致とする）
        asof_matched = tmp["motor_id_y"].notna()
        out_of_range = asof_matched & (~ok_range)
        asof_matched_count += int(asof_matched.sum())
        out_of_range_count += int(out_of_range.sum())

        # 区間外は無効化
        tmp.loc[~ok_range, "motor_id_y"] = pd.NA

        # ★ここが最重要：masterへ代入する前に motor_id を必ず正規化★
        tmp["motor_id"] = normalize_motor_id_series(tmp["motor_id_y"])

        # suffix列の復元と削除（列ダブり防止）
        for col in ["code", "motor_number"]:
            if f"{col}_x" in tmp.columns:
                tmp[col] = tmp[f"{col}_x"]

        drop_cols = [c for c in tmp.columns if c.endswith("_x") or c.endswith("_y")]
        drop_cols += [c for c in ["effective_from", "effective_to"] if c in tmp.columns]
        tmp.drop(columns=drop_cols, inplace=True)

        merged_parts.append(tmp)

    # concat: master_ng が空なら足さない（FutureWarning回避）
    parts = merged_parts + ([master_ng] if len(master_ng) else [])
    out_df = pd.concat(parts, ignore_index=True)

    # 並び（確認しやすさ優先）
    out_df.sort_values(["date", "code", "motor_number"], inplace=True, kind="mergesort", na_position="last")
    out_df.reset_index(drop=True, inplace=True)

    # 最終保証：motor_id を必ず string に統一（欠損<NA>も扱える）
    out_df["motor_id"] = normalize_motor_id_series(out_df["motor_id"])

    # レポート用（全行同一値を入れて main で拾う）
    out_df["_asof_matched_count"] = asof_matched_count
    out_df["_out_of_range_count"] = out_of_range_count

    return out_df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=r"data\processed\master.csv", help="Input master.csv")
    ap.add_argument("--motor_id_map", required=True, help="Input motor_id_map.csv")
    ap.add_argument("--out", default=None, help="Output path (default: <master>__with_motor_id.csv)")
    ap.add_argument("--inplace", action="store_true", help="Overwrite master.csv")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing motor_id column if present")
    ap.add_argument("--report_top_n_missing", type=int, default=20, help="Report top N missing groups")
    ap.add_argument("--low_memory_false", action="store_true", help="Read master.csv with low_memory=False (suppresses dtype warning)")
    args = ap.parse_args()

    master_path = Path(args.master)
    map_path = Path(args.motor_id_map)

    if not master_path.exists():
        print(f"[ERROR] master not found: {master_path}", file=sys.stderr)
        return 1
    if not map_path.exists():
        print(f"[ERROR] motor_id_map not found: {map_path}", file=sys.stderr)
        return 1

    if args.inplace:
        out_path = master_path
    else:
        out_path = Path(args.out) if args.out else master_path.with_name(master_path.stem + "__with_motor_id" + master_path.suffix)

    print(f"[INFO] master: {master_path}")
    print(f"[INFO] motor_id_map: {map_path}")
    print(f"[INFO] out: {out_path}")
    print(f"[INFO] inplace={args.inplace}, overwrite={args.overwrite}")

    # master読み込み
    if args.low_memory_false:
        master = pd.read_csv(master_path, low_memory=False)
    else:
        master = pd.read_csv(master_path)

    # motor_id_map読み込み（ここで motor_id を string に固定して float化を防ぐ）
    motor_map = pd.read_csv(map_path, dtype={"motor_id": "string"})

    n_before = len(master)
    out_df = attach_motor_id_group_asof(master, motor_map, overwrite=args.overwrite)

    if len(out_df) != n_before:
        print("[ERROR] row count changed unexpectedly.", file=sys.stderr)
        return 1

    # レポート
    missing = out_df["motor_id"].isna()
    miss_rate = float(missing.mean()) * 100.0

    asof_matched_count = int(out_df["_asof_matched_count"].iloc[0])
    out_of_range_count = int(out_df["_out_of_range_count"].iloc[0])

    print(f"[REPORT] motor_id missing: {missing.sum()} / {len(out_df)} ({miss_rate:.6f}%)")
    print(f"[REPORT] asof matched (pre-range-check): {asof_matched_count} / {len(out_df)}")
    print(f"[REPORT] out_of_range (fell outside effective_to): {out_of_range_count} / {len(out_df)}")

    if missing.any():
        tmp = out_df.loc[missing, ["code", "motor_number"]].value_counts().reset_index()
        tmp.columns = ["code", "motor_number", "n_missing_rows"]
        print(f"[REPORT] top missing (code,motor_number) groups (top {args.report_top_n_missing}):")
        print(tmp.head(args.report_top_n_missing).to_string(index=False))

        tmp2 = out_df.loc[missing, ["code"]].value_counts().reset_index()
        tmp2.columns = ["code", "n_missing_rows"]
        print("[REPORT] missing by code (top 24):")
        print(tmp2.head(24).to_string(index=False))

    # レポート用列を落として保存
    out_df.drop(columns=["_asof_matched_count", "_out_of_range_count"], inplace=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False, encoding="utf_8_sig")
    print("[DONE] saved:", out_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
