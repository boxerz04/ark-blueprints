# scripts/scrape.py
import os
import re
import argparse
from typing import Optional
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd  # 現状未使用でも将来の拡張で使うため残す
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

# 同時リクエスト数の制限
semaphore = asyncio.Semaphore(5)

def ensure_parent_dir(path: str) -> None:
    """保存先フォルダが無ければ作成する"""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def parse_args():
    p = argparse.ArgumentParser(description="Boatrace HTML scraper")
    p.add_argument(
        "--date",
        help="取得対象日（YYYY-MM-DD または YYYYMMDD）。省略時は今日。",
        default=None,
    )
    return p.parse_args()

def normalize_yyyymmdd(s: Optional[str]) -> str:
    """
    文字列日付を YYYYMMDD に正規化。
    - None のときは今日（ローカル）を返す
    - 'YYYY-MM-DD' または 'YYYYMMDD' を受け付ける
    """
    if s is None:
        return datetime.now().strftime("%Y%m%d")
    s = s.strip()
    if re.fullmatch(r"\d{8}", s):
        return s
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y%m%d")
    raise ValueError("Invalid --date format. Use YYYY-MM-DD or YYYYMMDD.")

# ----------------------------
# 取得系コルーチン
# ----------------------------

async def fetch(session, url, save_path):
    try:
        async with semaphore:
            async with session.get(url, headers=headers) as response:
                content = await response.read()
                ensure_parent_dir(save_path)
                with open(save_path, "wb") as f:
                    f.write(content)
            # 連続アクセスの礼儀としてランダムスリープ
            await asyncio.sleep(random.uniform(1, 5))
    except Exception:
        print(f"[ERROR] fetching URL: {url}")
        ensure_parent_dir(f"{save_path}_error.log")
        with open(f"{save_path}_error.log", "a", encoding="utf-8") as f:
            traceback.print_exc(file=f)

async def process_race(session, yyyymmdd, code, rno):
    race_id = f"{yyyymmdd}{code}{str(rno).zfill(2)}"
    urls = {
        "racelist":     f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={rno}&jcd={code}&hd={yyyymmdd}",
        "pcexpect":     f"https://www.boatrace.jp/owpc/pc/race/pcexpect?rno={rno}&jcd={code}&hd={yyyymmdd}",
        "beforeinfo":   f"https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno={rno}&jcd={code}&hd={yyyymmdd}",
        "raceresult":   f"https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno}&jcd={code}&hd={yyyymmdd}",
        "raceindex":    f"https://www.boatrace.jp/owpc/pc/race/raceindex?jcd={code}&hd={yyyymmdd}",
        "rankingmotor": f"https://www.boatrace.jp/owpc/pc/race/rankingmotor?jcd={code}&hd={yyyymmdd}",
    }

    tasks = [
        fetch(session, urls["racelist"],     f"data/html/racelist/racelist{race_id}.bin"),
        fetch(session, urls["pcexpect"],     f"data/html/pcexpect/pcexpect{race_id}.bin"),
        fetch(session, urls["beforeinfo"],   f"data/html/beforeinfo/beforeinfo{race_id}.bin"),
        fetch(session, urls["raceresult"],   f"data/html/raceresult/raceresult{race_id}.bin"),
        fetch(session, urls["raceindex"],    f"data/html/raceindex/raceindex{yyyymmdd}{code}.bin"),
        fetch(session, urls["rankingmotor"], f"data/html/rankingmotor/rankingmotor{yyyymmdd}{code}.bin"),
    ]
    await asyncio.gather(*tasks)

# ----------------------------
# メイン処理
# ----------------------------

async def main():
    args = parse_args()
    yyyymmdd = normalize_yyyymmdd(args.date)

    print(f"処理する日付: {yyyymmdd}")

    async with aiohttp.ClientSession() as session:
        # 開催日単位ページ：pay
        pay_url = f"https://www.boatrace.jp/owpc/pc/race/pay?hd={yyyymmdd}"
        await fetch(session, pay_url, f"data/html/pay/pay{yyyymmdd}.bin")

        # 開催場コード（jcd）を pay ページから抽出
        try:
            resp = await session.get(pay_url, headers=headers)
            content = await resp.read()
            soup = BeautifulSoup(content, "html.parser")

            code_list = []
            for img in soup.find_all("img", alt=True):
                alt_text = img["alt"]
                # 日本語を含むALTを目印にし、画像ファイル名から場コードを抽出
                if re.search(r"[\u3000-\u303F\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]", alt_text):
                    src = img.get("src", "")
                    m = re.search(r"text_place1_(\d+)\.png", src)
                    if m:
                        code_list.append(m.group(1))

            print(f"【{yyyymmdd}】の開催は【{len(code_list)}】場です")

            # index も保存
            index_url = pay_url.replace("pay", "index")
            await fetch(session, index_url, f"data/html/index/index{yyyymmdd}.bin")

            # 各場×各Rを取得
            tasks = []
            for code in code_list:
                for rno in range(1, 13):
                    tasks.append(process_race(session, yyyymmdd, code, rno))

            for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing races"):
                await task

        except Exception:
            print(f"[ERROR] processing date {yyyymmdd}")
            ensure_parent_dir(f"data/html/{yyyymmdd}_html_error.log")
            with open(f"data/html/{yyyymmdd}_html_error.log", "a", encoding="utf-8") as f:
                traceback.print_exc(file=f)

    print("処理が終了しました")

if __name__ == "__main__":
    asyncio.run(main())
