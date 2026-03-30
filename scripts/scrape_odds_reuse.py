# -*- coding: utf-8 -*-
"""
scrape_odds_reuse.py
再確認時に事前取得したodds3t HTMLを再利用して保存し、odds2tfを追加取得する。
"""

import argparse
import os
import sys
import urllib.request
from dataclasses import dataclass

from odds_profile_paths import resolve_odds_profile_paths

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}


@dataclass
class SaveResult:
    ok: bool
    message: str


def save_prefetched_odds3t(date: str, jcd: str, rno: str, prefetched_odds3t_path: str, profile: str, mins_before: int) -> SaveResult:
    jcd2 = str(jcd).zfill(2)
    rno2 = str(rno).zfill(2)
    paths = resolve_odds_profile_paths(ROOT_DIR, profile, mins_before)
    save_path = os.path.join(
        paths.odds3t_date_dir(date),
        f"odds3t{date}{jcd2}{rno2}.html",
    )

    try:
        if not os.path.exists(prefetched_odds3t_path):
            msg = f"prefetched odds3t path not found: {prefetched_odds3t_path}"
            print(f"[ERROR] save_prefetched_odds3t: {msg}")
            return SaveResult(ok=False, message=msg)

        with open(prefetched_odds3t_path, "rb") as rf:
            content = rf.read()
        if len(content) == 0 or len(content.strip()) == 0:
            msg = "empty prefetched odds3t content"
            print(f"[ERROR] save_prefetched_odds3t: {msg}")
            return SaveResult(ok=False, message=msg)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as wf:
            wf.write(content)
        print(f"[SAVED] {save_path} (prefetched_odds3t)")
        return SaveResult(ok=True, message=f"saved:{save_path}")
    except Exception as e:
        print(f"[ERROR] save_prefetched_odds3t: {e}")
        return SaveResult(ok=False, message=f"error={e}")


def fetch_and_save_odds2tf(date: str, jcd: str, rno: str, profile: str, mins_before: int, timeout: int = 15) -> SaveResult:
    jcd2 = str(jcd).zfill(2)
    rno2 = str(rno).zfill(2)
    paths = resolve_odds_profile_paths(ROOT_DIR, profile, mins_before)
    url = f"https://www.boatrace.jp/owpc/pc/race/odds2tf?rno={rno}&jcd={jcd2}&hd={date}"
    save_path = os.path.join(
        paths.odds2tf_date_dir(date),
        f"odds2tf{date}{jcd2}{rno2}.html",
    )

    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", 200)
            if status != 200:
                return SaveResult(ok=False, message=f"HTTP {status}")
            content = resp.read()

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as wf:
            wf.write(content)

        print(f"[SAVED] {save_path} (odds2tf)")
        return SaveResult(ok=True, message=f"saved:{save_path}")
    except Exception as e:
        print(f"[ERROR] fetch_and_save_odds2tf: {e}")
        return SaveResult(ok=False, message=f"error={e}")


def classify_result(saved_odds3t: int, saved_odds2tf: int) -> tuple[str, int]:
    if saved_odds3t == 1 and saved_odds2tf == 1:
        return "success", 0
    if saved_odds3t == 1 or saved_odds2tf == 1:
        return "partial", 2
    return "failed", 1


def parse_args():
    p = argparse.ArgumentParser(description="prefetched odds3t再利用保存 + odds2tf追加取得")
    p.add_argument("--date", required=True, help="日付 (YYYYMMDD)")
    p.add_argument("--jcd", required=True, help="場コード")
    p.add_argument("--rno", required=True, help="レース番号")
    p.add_argument("--prefetched_odds3t_path", required=True, help="事前取得odds3t HTMLパス")
    p.add_argument("--title", default="", help="タイトル（任意）")
    p.add_argument("--latest_deadline_dt", default="", help="再確認時締切（任意）")
    p.add_argument("--profile", required=True, choices=["5m", "2m"], help="出力profile")
    p.add_argument("--mins_before", type=int, required=True, choices=[5, 2], help="締切何分前")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    res3 = save_prefetched_odds3t(
        args.date,
        args.jcd,
        args.rno,
        args.prefetched_odds3t_path,
        args.profile,
        args.mins_before,
    )
    saved_odds3t = 1 if res3.ok else 0

    res2 = fetch_and_save_odds2tf(args.date, args.jcd, args.rno, args.profile, args.mins_before)
    saved_odds2tf = 1 if res2.ok else 0

    status, rc = classify_result(saved_odds3t, saved_odds2tf)
    jcd2 = str(args.jcd).zfill(2)
    rno2 = str(args.rno).zfill(2)
    print(
        "RESULT "
        f"status={status} "
        f"saved_odds3t={saved_odds3t} "
        f"saved_odds2tf={saved_odds2tf} "
        f"jcd={jcd2} rno={rno2} date={args.date}"
    )
    return rc


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        print(f"[FATAL] {e}")
        print("RESULT status=failed saved_odds3t=0 saved_odds2tf=0 jcd=00 rno=00 date=00000000")
        raise SystemExit(1)
