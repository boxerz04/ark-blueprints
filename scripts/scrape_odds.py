# -*- coding: utf-8 -*-
"""
scrape_odds.py
準優進出戦・準優勝戦・優勝戦の3連単オッズと2連単/2連複オッズを取得
"""

import argparse
import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

semaphore = asyncio.Semaphore(5)

# プロジェクトルートを基準にする
root_dir = os.path.dirname(os.path.dirname(__file__))


async def fetch_and_save(session, url, save_path, title_keywords=("準優勝戦", "準優進出戦", "優勝戦")):
    """URLを取得して条件に一致すればHTML保存"""
    try:
        async with semaphore:
            async with session.get(url) as response:
                content = await response.read()

        soup = BeautifulSoup(content, "lxml")
        title_elem = soup.find("h3", class_="title16_titleDetail__add2020")
        title_text = title_elem.get_text(strip=True) if title_elem else ""

        if any(k in title_text for k in title_keywords):
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, "wb") as f:
                f.write(content)
            print(f"[SAVED] {save_path} ({title_text})")
        else:
            print(f"[SKIP] {url} → {title_text}")

    except Exception as e:
        print(f"[ERROR] {url}: {e}")


async def main(date, jcd, rno):
    async with aiohttp.ClientSession(headers=headers) as session:
        # 3連単
        url_3t = f"https://www.boatrace.jp/owpc/pc/race/odds3t?rno={rno}&jcd={jcd.zfill(2)}&hd={date}"
        save_path_3t = os.path.join(root_dir, "data", "html", "odds3t", date, f"odds3t{date}{jcd.zfill(2)}{str(rno).zfill(2)}.html")
        await fetch_and_save(session, url_3t, save_path_3t)

        # 2連単・2連複
        url_2tf = f"https://www.boatrace.jp/owpc/pc/race/odds2tf?rno={rno}&jcd={jcd.zfill(2)}&hd={date}"
        save_path_2tf = os.path.join(root_dir, "data", "html", "odds2tf", date, f"odds2tf{date}{jcd.zfill(2)}{str(rno).zfill(2)}.html")
        await fetch_and_save(session, url_2tf, save_path_2tf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="日付 (例: 20250831)")
    parser.add_argument("--jcd", required=True, help="場コード (例: 20)")
    parser.add_argument("--rno", required=True, help="レース番号 (例: 12)")
    args = parser.parse_args()

    asyncio.run(main(args.date, args.jcd, args.rno))
