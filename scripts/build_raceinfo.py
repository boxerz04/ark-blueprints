# scripts/build_raceinfo.py
# -*- coding: utf-8 -*-
"""
racelist(出走表) .bin を処理して「今節スナップショット」CSVを日次で出力する最小スクリプト。
- 単一日付 (--date) / 日付レンジ (--start-date --end-date) / ディレクトリから自動抽出 (--all-available) に対応。
- HTMLのパースと特徴量抽出は src/raceinfo_features.py の関数を呼び出すだけに留める。
- レイアウト依存の改善は src 側の責務。ここではフローの配線に徹する。
"""

from __future__ import annotations

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import os
import re
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import pandas as pd

# ここで src の関数を利用
from src.raceinfo_features import (
    process_racelist_content,
    calculate_raceinfo_points,
    load_html,
    ranking_point_map,
    condition_point_map,
)


def yyyymmdd(s: str) -> str:
    """YYYYMMDD or YYYY-MM-DD を YYYYMMDD に正規化。"""
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s.replace("-", "")
    raise argparse.ArgumentTypeError("日付は YYYYMMDD か YYYY-MM-DD で指定してください")


def iter_dates_from_range(start: str, end: str) -> Iterable[str]:
    """YYYYMMDD の範囲（両端含む）を日次で列挙。"""
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    if s > e:
        raise ValueError("--start-date は --end-date 以前である必要があります")
    cur = s
    while cur <= e:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)


def extract_dates_from_filenames(dirpath: str) -> List[str]:
    """
    ディレクトリ内の .bin ファイル名から「8桁の連続数字（YYYYMMDD）」を抽出して一意な日付リストに。
    例: .../20240914_racelist_12R.bin -> '20240914'
    """
    dates: set[str] = set()
    for fn in os.listdir(dirpath):
        if not fn.endswith(".bin"):
            continue
        m = re.search(r"(\d{8})", fn)
        if m:
            dates.add(m.group(1))
    return sorted(dates)


def find_bin_files_for_date(dirpath: str, ymd: str) -> List[str]:
    """指定日付(YYYYMMDD)をファイル名に含む .bin を列挙。見つからない場合は空リスト。"""
    return sorted([os.path.join(dirpath, f)
                   for f in os.listdir(dirpath)
                   if f.endswith(".bin") and ymd in f])


def extract_race_id_from_filename(filepath: str) -> str:
    """ファイル名中の連続数字を race_id として抽出。なければ拡張子を除いたファイル名。"""
    name = os.path.basename(filepath)
    m = re.search(r"(\d{6,})", name)
    return m.group(1) if m else os.path.splitext(name)[0]


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def process_one_day(html_dir: str, out_dir: str, ymd: str) -> Optional[str]:
    """
    1日分の .bin を処理して CSV を出力。
    .bin が見つからなければ None を返す。出力パスを返す。
    """
    files = find_bin_files_for_date(html_dir, ymd)
    if not files:
        return None

    rows: list[pd.DataFrame] = []
    for fp in files:
        content = load_html(fp)
        df = process_racelist_content(content)
        race_id = extract_race_id_from_filename(fp)
        df = calculate_raceinfo_points(
            df, ranking_map=ranking_point_map, condition_map=condition_point_map, race_id=race_id
        )
        rows.append(df)

    out_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f"raceinfo_{ymd}.csv")
    out_df.to_csv(out_path, index=False, encoding="utf_8_sig")
    print(f"[OK] wrote: {out_path}  rows={len(out_df)}  files={len(files)}")
    return out_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate raceinfo daily CSVs from racelist .bin files")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=yyyymmdd, help="対象日 (YYYYMMDD or YYYY-MM-DD)")
    g.add_argument("--start-date", type=yyyymmdd, help="開始日 (YYYYMMDD or YYYY-MM-DD)")
    p.add_argument("--end-date", type=yyyymmdd, help="終了日 (YYYYMMDD or YYYY-MM-DD) ※ --start-date と併用")
    g.add_argument("--all-available", action="store_true", help="ディレクトリ内の .bin から抽出できる全日付を処理")

    p.add_argument("--html-dir", default="data/html/racelist", help="入力 .bin のディレクトリ")
    p.add_argument("--out-dir", default="data/processed/raceinfo", help="出力ディレクトリ")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # 日付の決定
    if args.date:
        dates = [args.date]
    elif args.start_date:
        if not args.end_date:
            raise SystemExit("--start-date を使う場合は --end-date も指定してください")
        dates = list(iter_dates_from_range(args.start_date, args.end_date))
    else:
        # --all-available
        dates = extract_dates_from_filenames(args.html_dir)
        if not dates:
            print("[WARN] ファイル名から日付(YYYYMMDD)が抽出できませんでした。全 .bin を1日として処理することも検討してください。")
            return

    # 実行
    total_rows = 0
    for ymd in dates:
        out = process_one_day(args.html_dir, args.out_dir, ymd)
        if out:
            try:
                total_rows += sum(1 for _ in open(out, "r", encoding="utf_8_sig")) - 1
            except Exception:
                pass

    print(f"[DONE] processed days={len(dates)} total_rows~={total_rows}")


if __name__ == "__main__":
    main()
