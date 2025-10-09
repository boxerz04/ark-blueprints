# scripts/preprocess_sectional.py
# -*- coding: utf-8 -*-
"""
master と raceinfo を結合して「節内（sectional）モデル」用の master_sectional.csv を生成するスクリプト。

■ 使い方
# 単日
python scripts/preprocess_sectional.py --date 2025-08-24
# 期間
python scripts/preprocess_sectional.py --start-date 2025-08-01 --end-date 2025-08-31
# 全日（raceinfo_*.csv を全部）
python scripts/preprocess_sectional.py
# 出力先を変える
python scripts/preprocess_sectional.py --out data/processed/sectional/master_sectional.csv
"""

from __future__ import annotations

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import glob
import re
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import pandas as pd

# ====== デフォルトパス ===============================================================
MASTER_PATH_DEFAULT   = "data/processed/master.csv"
RACEINFO_DIR_DEFAULT  = "data/processed/raceinfo"
OUT_PATH_DEFAULT      = "data/processed/sectional/master_sectional.csv"

# ====== sectional で “即使う” 列（raceinfo 側） ======================================
USE_COLS_RACEINFO = [
    "player_id", "race_id",
    "ST_mean_current", "ST_rank_current", "ST_previous_time",
    "score", "score_rate",
    "ranking_point_sum", "ranking_point_rate",
    "condition_point_sum", "condition_point_rate",
    "race_ct_current",  # ← 追加採用
]

# ====== ユーティリティ ================================================================
def yyyymmdd(s: str) -> str:
    """YYYYMMDD or YYYY-MM-DD を YYYYMMDD に正規化。"""
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s.replace("-", "")
    raise argparse.ArgumentTypeError("日付は YYYYMMDD か YYYY-MM-DD で指定してください")

def iter_dates(start: str, end: str) -> Iterable[str]:
    """YYYYMMDD の範囲を両端含めて日次で列挙。"""
    s = datetime.strptime(start, "%Y%m%d"); e = datetime.strptime(end, "%Y%m%d")
    if s > e:
        raise SystemExit("[ERROR] --start-date は --end-date 以前である必要があります")
    cur = s
    while cur <= e:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)

def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def _load_raceinfo_for_range(dirpath: str, date: Optional[str], start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    """
    raceinfo_YYYYMMDD*.csv を単日/期間/全日で集めて縦結合し、必要列だけに絞って返す。
    同一 (player_id, race_id) は後勝ちで1件化。
    """
    # パターン作成
    patterns: List[str] = []
    if date:
        d = yyyymmdd(date); patterns = [os.path.join(dirpath, f"raceinfo_{d}*.csv")]
    elif start and end:
        s = yyyymmdd(start); e = yyyymmdd(end)
        for d in iter_dates(s, e):
            patterns.append(os.path.join(dirpath, f"raceinfo_{d}*.csv"))
    else:
        patterns = [os.path.join(dirpath, "raceinfo_*.csv")]

    # 実在ファイル列挙
    paths: List[str] = []
    for pat in patterns:
        paths.extend(sorted(glob.glob(pat)))
    if not paths:
        print("[WARN] raceinfo CSV が見つかりませんでした:", patterns)
        return pd.DataFrame(columns=USE_COLS_RACEINFO)

    # 読み込み＋必要列抽出
    frames: List[pd.DataFrame] = []
    for p in paths:
        try:
            df = pd.read_csv(p)
        except Exception as e:
            print(f"[WARN] 読み込み失敗: {p} ({e})")
            continue
        keep = [c for c in USE_COLS_RACEINFO if c in df.columns]
        if not keep:
            print(f"[WARN] 必要列が見当たりません: {p}")
            continue
        frames.append(df[keep])

    if not frames:
        return pd.DataFrame(columns=USE_COLS_RACEINFO)

    rf = pd.concat(frames, ignore_index=True)

    # join 安定化（キーを文字列に）
    for k in ("player_id", "race_id"):
        if k in rf.columns:
            rf[k] = rf[k].astype(str)

    # 後勝ち重複排除
    rf = rf.drop_duplicates(subset=["player_id", "race_id"], keep="last")
    return rf

# ====== メイン =======================================================================
def main():
    ap = argparse.ArgumentParser(description="Preprocess for SECTIONAL model (merge master with raceinfo)")
    ap.add_argument("--master", default=MASTER_PATH_DEFAULT, help="master.csv のパス")
    ap.add_argument("--raceinfo-dir", default=RACEINFO_DIR_DEFAULT, help="raceinfo 日次CSVのディレクトリ")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--date", type=yyyymmdd, help="単日 (YYYYMMDD or YYYY-MM-DD)")
    g.add_argument("--start-date", type=yyyymmdd, help="期間開始 (YYYYMMDD or YYYY-MM-DD)")
    ap.add_argument("--end-date", type=yyyymmdd, help="期間終了 (YYYYMMDD or YYYY-MM-DD)")
    ap.add_argument("--out", default=OUT_PATH_DEFAULT, help="出力 CSV パス")
    args = ap.parse_args()

    _ensure_parent_dir(args.out)

    # 1) master 読み込み
    if not os.path.exists(args.master):
        raise SystemExit(f"[ERROR] master が見つかりません: {args.master}")
    master = pd.read_csv(args.master)

    # 必要キー確認
    for k in ["player_id", "race_id"]:
        if k not in master.columns:
            raise SystemExit(f"[ERROR] master に {k} がありません: {args.master}")

    # 2) raceinfo 読み込み（単日/期間/全日）
    raceinfo = _load_raceinfo_for_range(args.raceinfo_dir, args.date, args.start_date, args.end_date)

    # 3) 型整形（join 安定化）
    master["player_id"] = master["player_id"].astype(str)
    master["race_id"]   = master["race_id"].astype(str)
    if not raceinfo.empty:
        raceinfo["player_id"] = raceinfo["player_id"].astype(str)
        raceinfo["race_id"]   = raceinfo["race_id"].astype(str)

    # 4) LEFT JOIN
    merged = master.merge(raceinfo, on=["player_id", "race_id"], how="left")

    # 5) 数値カラムの型を数値化（存在する列のみ）
    numeric_cols = [c for c in USE_COLS_RACEINFO
                    if c not in ("player_id", "race_id", "ST_previous_time")]
    for c in numeric_cols:
        if c in merged.columns:
            merged[c] = pd.to_numeric(merged[c], errors="coerce")

    # 6) 保存
    merged.to_csv(args.out, index=False, encoding="utf_8_sig")
    print(f"[OK] wrote: {args.out} rows={len(merged)} cols={len(merged.columns)}")

if __name__ == "__main__":
    main()
