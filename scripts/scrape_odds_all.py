# -*- coding: utf-8 -*-
"""
scrape_odds_all.py
全レース対象で3連単/2連単・2連複オッズHTMLを取得・保存する検証版。
既存の保存先・命名規約を踏襲しつつ、1レースごとに機械可読な RESULT 行を出力する。
"""

import argparse
import asyncio
import os
from dataclasses import dataclass

import aiohttp
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/114.0.0.0 Safari/537.36"
    )
}

SEMAPHORE = asyncio.Semaphore(5)
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))


@dataclass
class SaveResult:
    ok: bool
    title: str
    message: str


async def fetch_and_save(session: aiohttp.ClientSession, url: str, save_path: str) -> SaveResult:
    """URLを取得してHTML保存。タイトルも可能な範囲で抽出して返す。"""
    title_text = ""
    try:
        async with SEMAPHORE:
            async with session.get(url) as response:
                if response.status != 200:
                    return SaveResult(False, "", f"HTTP {response.status}")
                content = await response.read()

        soup = BeautifulSoup(content, "lxml")
        title_elem = soup.find("h3", class_="title16_titleDetail__add2020")
        title_text = title_elem.get_text(strip=True) if title_elem else ""

        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(content)

        print(f"[SAVED] {save_path} ({title_text})")
        return SaveResult(True, title_text, "saved")
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return SaveResult(False, title_text, f"error={e}")


async def main(date: str, jcd: str, rno: str) -> int:
    jcd2 = str(jcd).zfill(2)
    rno2 = str(rno).zfill(2)

    url_3t = f"https://www.boatrace.jp/owpc/pc/race/odds3t?rno={rno}&jcd={jcd2}&hd={date}"
    save_path_3t = os.path.join(
        ROOT_DIR,
        "data",
        "html",
        "odds3t",
        date,
        f"odds3t{date}{jcd2}{rno2}.html",
    )

    url_2tf = f"https://www.boatrace.jp/owpc/pc/race/odds2tf?rno={rno}&jcd={jcd2}&hd={date}"
    save_path_2tf = os.path.join(
        ROOT_DIR,
        "data",
        "html",
        "odds2tf",
        date,
        f"odds2tf{date}{jcd2}{rno2}.html",
    )

    saved_odds3t = 0
    saved_odds2tf = 0

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        # 片方失敗でももう片方を試行
        res3 = await fetch_and_save(session, url_3t, save_path_3t)
        saved_odds3t = 1 if res3.ok else 0

        res2 = await fetch_and_save(session, url_2tf, save_path_2tf)
        saved_odds2tf = 1 if res2.ok else 0

    if saved_odds3t == 1 and saved_odds2tf == 1:
        status = "success"
        rc = 0
    elif saved_odds3t == 1 or saved_odds2tf == 1:
        status = "partial"
        rc = 2
    else:
        status = "failed"
        rc = 1

    print(
        "RESULT "
        f"status={status} "
        f"saved_odds3t={saved_odds3t} "
        f"saved_odds2tf={saved_odds2tf} "
        f"jcd={jcd2} rno={rno2} date={date}"
    )
    return rc


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="日付 (例: 20260326)")
    parser.add_argument("--jcd", required=True, help="場コード (例: 20)")
    parser.add_argument("--rno", required=True, help="レース番号 (例: 12)")
    args = parser.parse_args()

    try:
        exit_code = asyncio.run(main(args.date, args.jcd, args.rno))
    except Exception as e:
        # 致命的エラー時も可能な範囲で RESULT を出す
        jcd2 = str(args.jcd).zfill(2)
        rno2 = str(args.rno).zfill(2)
        print(f"[FATAL] {e}")
        print(
            "RESULT "
            "status=failed "
            "saved_odds3t=0 "
            "saved_odds2tf=0 "
            f"jcd={jcd2} rno={rno2} date={args.date}"
        )
        exit_code = 1

    raise SystemExit(exit_code)
