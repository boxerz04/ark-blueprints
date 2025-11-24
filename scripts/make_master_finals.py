#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
make_master_finals.py

共通の master.csv から、
「優勝戦 / 準優勝戦 / 準優進出戦」を対象とするレースだけを抽出し、
master_finals.csv を出力するスクリプト。

想定フロー:
  1) batch/build_master_range.ps1 で共通 master.csv を作成
  2) 本スクリプトで master_finals.csv を作成
  3) train_finals_from_master.ps1 などから master_finals.csv を学習に利用

デフォルト:
  入力: data/processed/master.csv
  出力: data/processed/master_finals.csv

必要に応じて --master-in / --master-out でパスを変更してください。
"""

import argparse
import os
import sys
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="master.csv を優勝戦/準優勝戦/準優進出戦にフィルタして master_finals.csv を作成するスクリプト"
    )
    ap.add_argument(
        "--master-in",
        type=str,
        default="data/processed/master.csv",
        help="入力 master.csv のパス（デフォルト: data/processed/master.csv）",
    )
    ap.add_argument(
        "--master-out",
        type=str,
        default="data/processed/master_finals.csv",
        help="出力 master_finals.csv のパス（デフォルト: data/processed/master_finals.csv）",
    )
    # 将来、他のステージを増やしたくなったときのために柔軟性を残しておく
    ap.add_argument(
        "--stage-filter",
        type=str,
        default="finals,semi,semi-entry",
        help=(
            "使用するステージの指定。"
            "finals=優勝戦, semi=準優勝戦, semi-entry=準優進出戦/準優進出 をカンマ区切りで指定。"
            'デフォルト: "finals,semi,semi-entry"'
        ),
    )
    return ap.parse_args()


def build_stage_pattern(stage_filter: str) -> str:
    """
    "finals,semi,semi-entry" のような stage_filter 文字列から
    race_name に対して使う正規表現パターンを作成する。
    """
    if not stage_filter:
        # 空なら何もフィルタしない、という扱いにしたいが、
        # 今回の用途では stage_filter を省略しない前提。
        return ""

    stages = {s.strip().lower() for s in stage_filter.split(",") if s.strip()}
    pats = []

    # finals: 優勝戦
    if "finals" in stages:
        pats.append(r"優勝戦")

    # semi: 準優勝戦
    if "semi" in stages:
        pats.append(r"準優勝戦")

    # semi-entry: 準優進出戦 / 準優進出
    if "semi-entry" in stages or "semi_entry" in stages:
        pats.append(r"準優進出戦|準優進出")

    if not pats:
        return ""

    # 任意の1つにマッチすれば良いので OR でつなぐ
    return "(" + "|".join(pats) + ")"


def main() -> None:
    args = parse_args()

    master_in = args.master_in
    master_out = args.master_out

    print(f"[INFO] master_in  : {master_in}")
    print(f"[INFO] master_out : {master_out}")
    print(f"[INFO] stage_filter: {args.stage_filter}")

    if not os.path.exists(master_in):
        print(f"[ERROR] master_in が見つかりません: {master_in}", file=sys.stderr)
        sys.exit(1)

    # master.csv 読み込み
    # 他のスクリプトとあわせて encoding="utf-8-sig", parse_dates=["date"] を採用
    print("[INFO] loading master...")
    df = pd.read_csv(master_in, encoding="utf-8-sig", parse_dates=["date"])
    n_before = len(df)
    print(f"[INFO] master shape: {df.shape}")

    # race_name が存在するか確認
    if "race_name" not in df.columns:
        print("[ERROR] 'race_name' 列が master に存在しません。フィルタできません。", file=sys.stderr)
        sys.exit(1)

    # ステージに応じた正規表現パターンを構築
    pat = build_stage_pattern(args.stage_filter)
    if not pat:
        print("[WARN] stage_filter から有効なパターンが作れませんでした。入力をそのままコピーします。")
        df_filtered = df.copy()
    else:
        # race_name に対して部分一致検索（NaN は空文字扱い）
        rn = df["race_name"].fillna("").astype(str)
        mask = rn.str.contains(pat, regex=True)
        df_filtered = df.loc[mask].copy()
        n_after = len(df_filtered)
        print(f"[INFO] stage-filter '{args.stage_filter}' → {n_after}/{n_before} rows kept")

    # 出力ディレクトリを作成
    out_dir = os.path.dirname(master_out)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # 出力
    df_filtered.to_csv(master_out, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote finals master: {master_out}  shape={df_filtered.shape}")


if __name__ == "__main__":
    main()
