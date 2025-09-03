# -*- coding: utf-8 -*-
"""
scripts/scrape_one_race.py
1レース分のHTMLを非同期で取得して **data/live/html/* に保存** する。
!!! raceresult は保存しません !!!

取得対象:
- pay（開催一覧）/ index（開催タイトル等） … 日付単位
- racelist / pcexpect / beforeinfo … レース単位
- raceindex / rankingmotor … 場・日単位
※ ark-blueprints の従来命名に合わせて .bin で保存

使い方:
  python scripts/scrape_one_race.py --date 20250903 --jcd 03 --race 11
  # or ハイフン区切り
  python scripts/scrape_one_race.py --date 2025-09-03 --jcd 3 --race 11
"""

import os
import re
import argparse
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from tqdm import tqdm
import traceback
from datetime import datetime
import random

# ----------------------------
# 設定・ユーティリティ
# ----------------------------
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
}
semaphore = asyncio.Semaphore(5)

root_dir = os.path.dirname(os.path.dirname(__file__))

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def normalize_yyyymmdd(s: str) -> str:
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return datetime.strptime(s, "%Y-%m-%d").strftime("%Y%m%d")
    raise ValueError("Invalid --date format. Use YYYY-MM-DD or YYYYMMDD.")

def parse_args():
    p = argparse.ArgumentParser(description="Boatrace HTML scraper (ONE RACE, no raceresult)")
    p.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    p.add_argument("--jcd",  required=True, help="場コード（2桁推奨、数値でも可）例: 03")
    p.add_argument("--race", required=True, type=int, help="レース番号（1-12）")
    return p.parse_args()

# ----------------------------
# 保存パス（live/html 配下）
# ----------------------------
def live_path(kind: str, date: str, jcd: str = "", rno: int | None = None) -> str:
    """
    kind: pay | index | racelist | pcexpect | beforeinfo | raceindex | rankingmotor
    """
    base_dir = os.path.join(root_dir, "data", "live", "html", kind)
    if kind in ("pay", "index"):
        fname = f"{kind}{date}.bin"
    elif kind in ("raceindex", "rankingmotor"):
        fname = f"{kind}{date}{jcd}.bin"
    elif kind in ("racelist", "pcexpect", "beforeinfo"):
        if rno is None:
            raise ValueError("rno is required for race-level pages")
        race_id = f"{date}{jcd}{str(rno).zfill(2)}"
        fname = f"{kind}{race_id}.bin"
    else:
        raise ValueError(f"unknown kind: {kind}")
    return os.path.join(base_dir, fname)

# ----------------------------
# 取得系コルーチン
# ----------------------------
async def fetch(session: aiohttp.ClientSession, url: str, save_path: str):
    try:
        async with semaphore:
            async with session.get(url, headers=headers) as resp:
                content = await resp.read()
                ensure_parent_dir(save_path)
                with open(save_path, "wb") as f:
                    f.write(content)
            # 礼儀として低頻度スリープ
            await asyncio.sleep(random.uniform(0.8, 2.0))
    except Exception:
        print(f"[ERROR] fetching URL: {url}")
        ensure_parent_dir(f"{save_path}.error.log")
        with open(f"{save_path}.error.log", "a", encoding="utf-8") as f:
            traceback.print_exc(file=f)

async def scrape_one_race(session: aiohttp.ClientSession, yyyymmdd: str, code2: str, rno: int):
    race_id = f"{yyyymmdd}{code2}{str(rno).zfill(2)}"

    # 日付単位（pay / index）
    pay_url   = f"https://www.boatrace.jp/owpc/pc/race/pay?hd={yyyymmdd}"
    index_url = f"https://www.boatrace.jp/owpc/pc/race/index?hd={yyyymmdd}"

    # レース単位
    racelist_url   = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={rno}&jcd={code2}&hd={yyyymmdd}"
    pcexpect_url   = f"https://www.boatrace.jp/owpc/pc/race/pcexpect?rno={rno}&jcd={code2}&hd={yyyymmdd}"
    beforeinfo_url = f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno={rno}&jcd={code2}&hd={yyyymmdd}"
    # raceresult_url = ...  # ← 作りません（要求により除外）

    # 場・日単位
    raceindex_url    = f"https://www.boatrace.jp/owpc/pc/race/raceindex?jcd={code2}&hd={yyyymmdd}"
    rankingmotor_url = f"https://www.boatrace.jp/owpc/pc/race/rankingmotor?jcd={code2}&hd={yyyymmdd}"

    tasks = [
        # day
        fetch(session, pay_url,         live_path("pay", yyyymmdd)),
        fetch(session, index_url,       live_path("index", yyyymmdd)),
        # race
        fetch(session, racelist_url,    live_path("racelist", yyyymmdd, code2, rno)),
        fetch(session, pcexpect_url,    live_path("pcexpect", yyyymmdd, code2, rno)),
        fetch(session, beforeinfo_url,  live_path("beforeinfo", yyyymmdd, code2, rno)),
        # venue
        fetch(session, raceindex_url,   live_path("raceindex", yyyymmdd, code2)),
        fetch(session, rankingmotor_url,live_path("rankingmotor", yyyymmdd, code2)),
    ]
    for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc=f"Scraping {race_id}"):
        await task

# ----------------------------
# メイン
# ----------------------------
async def main():
    args = parse_args()
    yyyymmdd = normalize_yyyymmdd(args.date)
    code2 = str(args.jcd).zfill(2)
    rno = int(args.race)
    if not (1 <= rno <= 12):
        raise ValueError("--race は 1〜12 の整数で指定してください")

    async with aiohttp.ClientSession() as session:
        await scrape_one_race(session, yyyymmdd, code2, rno)

    print("完了: 1レース分のHTMLを data/live/html/* に保存しました（raceresultは保存していません）")

if __name__ == "__main__":
    asyncio.run(main())
