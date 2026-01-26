# -*- coding: utf-8 -*-
"""
preprocess_motor_section.py (FINAL)

目的
- motor_section_features_n__all.csv を master に安全に結合する（リーク防止を最優先）。
- motor_section_features 側は「同一 section_id 行に prev* / delta* が既に shift 済み」である前提。
  → master 側で prev_section_id を作ってズラす処理は行わない（2節前参照のリスクになるため）。

結合仕様（確定）
- JOINキー: (motor_id, section_id)
- master に結合する列: prev1_*, prev3_*, prev5_*, delta_* のみ
- 結合しない列（除外）:
  - 当該節集計（リーク源）: motor_*（motor_race_ct / motor_score_rate 等）
  - メタ: section_start_dt, section_end_dt

合意ルール
- master.section_id は絶対に補完しない
- master.motor_id / master.section_id の欠損は即エラー
- features 側 key 欠損・key 重複は即エラー
- JOIN後、prev/delta列が NaN になるのは「過去節不足」による仕様として許容
  （ただし、features側欠落によるNaNを検知するため QC も出力）

使い方例
(base) > python scripts\\preprocess_motor_section.py ^
  --master_csv data\\processed\\master\\master.csv ^
  --motor_section_csv data\\processed\\motor\\motor_section_features_n__all.csv ^
  --out_master_csv data\\processed\\master\\master__with_motor_section.csv

オプション
- --prefix : 結合列に付ける prefix（衝突回避）。デフォルト "motor_"
- --strict_key_match : masterの( motor_id, section_id )がfeaturesに存在しない場合に即エラー（デフォルトOFF）
  ※ 初回節は features 行が存在しても prev が NaN になり得るので「存在有無」だけで落とすのは通常不要。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

import pandas as pd


def _info(msg: str) -> None:
    print(f"[INFO] {msg}")


def _error(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)


def _require_columns(df: pd.DataFrame, cols: List[str], name: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} に必要な列がありません: missing={missing}")


def _assert_no_na(df: pd.DataFrame, col: str, name: str) -> None:
    na = df[col].isna()
    if na.any():
        sample = df.loc[na, [col]].head(10)
        raise ValueError(
            f"{name}.{col} に欠損があります（合意仕様により即エラー）: "
            f"na_count={int(na.sum())}, sample=\n{sample}"
        )


def _astype_string(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df


def _select_feature_cols(features: pd.DataFrame) -> List[str]:
    """
    結合対象列（prev/delta）だけを抽出する。
    """
    keep = []
    for c in features.columns:
        if c.startswith(("prev1_", "prev3_", "prev5_", "delta_")):
            keep.append(c)
    return keep


def _qc_features(features: pd.DataFrame) -> None:
    _require_columns(features, ["motor_id", "section_id"], "motor_section_features")
    _assert_no_na(features, "motor_id", "motor_section_features")
    _assert_no_na(features, "section_id", "motor_section_features")

    dup = features.duplicated(subset=["motor_id", "section_id"])
    if dup.any():
        sample = features.loc[dup, ["motor_id", "section_id"]].head(30)
        raise ValueError(
            "motor_section_features の (motor_id, section_id) が一意ではありません（即エラー）: "
            f"dup_count={int(dup.sum())}\nsample=\n{sample}"
        )


def _qc_master(master: pd.DataFrame) -> None:
    _require_columns(master, ["motor_id", "section_id"], "master")
    _assert_no_na(master, "motor_id", "master")
    _assert_no_na(master, "section_id", "master")  # 合意仕様


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master_csv", required=True, type=str, help="入力 master.csv")
    parser.add_argument("--motor_section_csv", required=True, type=str, help="入力 motor_section_features_n__all.csv")
    parser.add_argument("--out_master_csv", required=True, type=str, help="出力 master（motor_section prev/delta付与）")
    parser.add_argument("--prefix", default="motor_", type=str, help="結合する特徴量列に付けるprefix（衝突回避）")
    parser.add_argument("--encoding", default="utf-8-sig", type=str, help="CSV出力エンコーディング")
    parser.add_argument(
        "--strict_key_match",
        action="store_true",
        help="masterのキーがfeaturesに存在しない場合に即エラー（通常はOFF推奨）",
    )
    parser.add_argument("--qc_report_csv", default="", type=str, help="任意: QCレポート出力先CSV（空なら出力しない）")
    args = parser.parse_args()

    master_path = Path(args.master_csv)
    feat_path = Path(args.motor_section_csv)
    out_path = Path(args.out_master_csv)

    _info(f"master_csv: {master_path}")
    _info(f"motor_section_csv: {feat_path}")
    _info(f"out_master_csv: {out_path}")
    _info(f"prefix: '{args.prefix}'")
    _info(f"strict_key_match: {args.strict_key_match}")

    # --- Load ---
    master = pd.read_csv(master_path)
    features = pd.read_csv(feat_path)

    # 型統一（motor_id / section_id は string 厳守）
    master = _astype_string(master, ["motor_id", "section_id"])
    features = _astype_string(features, ["motor_id", "section_id"])

    # --- QC: inputs ---
    _qc_master(master)
    _qc_features(features)

    _info(f"master rows: {len(master):,} / cols: {len(master.columns):,}")
    _info(f"features rows: {len(features):,} / cols: {len(features.columns):,}")

    # --- Select columns to join (prev/delta only) ---
    join_cols = _select_feature_cols(features)
    if not join_cols:
        raise ValueError("features から prev/delta 列が1つも見つかりません。列名プレフィクスを確認してください。")

    # 排除対象（リーク源やメタ）を明示的に無視する（ここでは読み込むが結合しない）
    excluded = [c for c in features.columns if c not in (["motor_id", "section_id"] + join_cols)]
    _info(f"join feature cols: {len(join_cols):,}")
    _info(f"excluded cols (not joined): {len(excluded):,}")

    # --- Prepare features: keep only keys + join_cols, and prefix feature columns ---
    feat_small = features[["motor_id", "section_id"] + join_cols].copy()

    # master列との衝突回避（prefix付与）
    rename_map = {c: f"{args.prefix}{c}" for c in join_cols}
    feat_small = feat_small.rename(columns=rename_map)
    joined_cols_pref = [rename_map[c] for c in join_cols]

    # --- Optional: strict key coverage QC ---
    if args.strict_key_match:
        # masterキーがfeaturesに存在するか（値がNaNかどうかではなく、行の存在）
        mk = master[["motor_id", "section_id"]].drop_duplicates()
        fk = feat_small[["motor_id", "section_id"]].drop_duplicates()
        missing_keys = mk.merge(fk, on=["motor_id", "section_id"], how="left", indicator=True)
        missing_keys = missing_keys[missing_keys["_merge"] == "left_only"].drop(columns=["_merge"])
        if len(missing_keys) > 0:
            sample = missing_keys.head(30)
            raise ValueError(
                "master の (motor_id, section_id) が features に存在しないキーがあります（strict_key_match=ON）: "
                f"missing_key_count={len(missing_keys):,}\nsample=\n{sample}"
            )

    # --- JOIN ---
    merged = master.merge(
        feat_small,
        on=["motor_id", "section_id"],
        how="left",
        validate="many_to_one",  # master:多行、features:節単位で一意の想定
        indicator=True,
    )

    # merge状況（行の存在）
    both = int((merged["_merge"] == "both").sum())
    left_only = int((merged["_merge"] == "left_only").sum())
    _info(f"merge status: both={both:,}, left_only={left_only:,}")

    # --- QC: NaN rates on joined columns ---
    # ここでのNaNは「過去節不足」由来で許容されうる。
    # ただし、列ごとの欠損率を可視化して異常（急増）に気づけるようにする。
    na_rates = {}
    for c in joined_cols_pref:
        na_rates[c] = float(merged[c].isna().mean())

    # 上位10列（欠損率が高い順）
    top10 = sorted(na_rates.items(), key=lambda kv: kv[1], reverse=True)[:10]
    _info("NA rate (joined cols) top10: " + ", ".join([f"{k}={v:.6f}" for k, v in top10]))

    # --- Optional QC report ---
    if args.qc_report_csv:
        report_path = Path(args.qc_report_csv)
        rep_rows = [
            ("master_rows", len(master)),
            ("features_rows", len(features)),
            ("merge_both_rows", both),
            ("merge_left_only_rows", left_only),
            ("joined_feature_cols", len(joined_cols_pref)),
        ]
        # 欠損率も出す（全列は多いのでtop10＋平均を保存）
        rep_rows.append(("joined_na_rate_mean", sum(na_rates.values()) / len(na_rates)))
        for k, v in top10:
            rep_rows.append((f"na_rate__{k}", v))

        rep = pd.DataFrame(rep_rows, columns=["metric", "value"])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        rep.to_csv(report_path, index=False, encoding=args.encoding)
        _info(f"qc_report_csv written: {report_path}")

    # _merge は不要
    merged = merged.drop(columns=["_merge"])

    # --- Write ---
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False, encoding=args.encoding)
    _info("DONE")


if __name__ == "__main__":
    main()
