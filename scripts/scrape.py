import os
import re
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from tqdm import tqdm  # 修正: Jupyter専用の `notebook` モードを削除
import pandas as pd
import traceback
from datetime import datetime, timedelta  # 修正: `datetime.now()` を適用
import random

# ユーザーエージェントを設定
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

# 日付を設定
dt_now = datetime.now()  # 修正: `datetime.now()` を明示
year = '2025'
date_list = [dt_now.strftime('%m%d')]  # 月日だけを取得
#date_list = ['0526', '0527', '0528', '0529', '0530', '0531']

print(f"処理する日付: {date_list}")

# 同時リクエスト数の制限
semaphore = asyncio.Semaphore(5)

# 非同期でHTMLを取得
async def fetch(session, url, save_path):
    try:
        async with semaphore:
            async with session.get(url, headers=headers) as response:
                content = await response.read()
                with open(save_path, 'wb') as file:
                    file.write(content)
            await asyncio.sleep(random.uniform(1, 5))  # 1〜5秒のランダム待機
    except Exception as e:
        print(f'Error fetching URL {url}')
        with open(f'{save_path}_error.log', 'a') as f:
            traceback.print_exc(file=f)

# レース情報を取得
async def process_race(session, date, code, rno):
    race_id = f"{date}{code}{str(rno).zfill(2)}"
    urls = {
        'racelist': f'https://www.boatrace.jp/owpc/pc/race/racelist?rno={rno}&jcd={code}&hd={date}',
        'pcexpect': f'https://www.boatrace.jp/owpc/pc/race/pcexpect?rno={rno}&jcd={code}&hd={date}',
        'beforeinfo': f'https://www.boatrace.jp/owpc/pc/race/beforeinfo?rno={rno}&jcd={code}&hd={date}',
        'raceresult': f'https://www.boatrace.jp/owpc/pc/race/raceresult?rno={rno}&jcd={code}&hd={date}',
        'raceindex': f'https://www.boatrace.jp/owpc/pc/race/raceindex?jcd={code}&hd={date}',
        'rankingmotor': f'https://www.boatrace.jp/owpc/pc/race/rankingmotor?jcd={code}&hd={date}'
    }

    tasks = [
        fetch(session, urls['racelist'], f'data/html/racelist/racelist{race_id}.bin'),
        fetch(session, urls['pcexpect'], f'data/html/pcexpect/pcexpect{race_id}.bin'),
        fetch(session, urls['beforeinfo'], f'data/html/beforeinfo/beforeinfo{race_id}.bin'),
        fetch(session, urls['raceresult'], f'data/html/raceresult/raceresult{race_id}.bin'),
        fetch(session, urls['raceindex'], f'data/html/raceindex/raceindex{date}{code}.bin'),
        fetch(session, urls['rankingmotor'], f'data/html/rankingmotor/rankingmotor{date}{code}.bin')
    ]

    await asyncio.gather(*tasks)

# メイン処理
async def main():
    async with aiohttp.ClientSession() as session:
        for d in date_list:
            date = year + d

            # payスクレイピング
            first_url = f"https://www.boatrace.jp/owpc/pc/race/pay?hd={date}"
            await fetch(session, first_url, f'data/html/pay/pay{date}.bin')

            # code_listを取得
            try:
                response = await session.get(first_url)
                content = await response.read()
                soup = BeautifulSoup(content, 'html.parser')
                
                code_list = []
                image_tags = soup.find_all('img', alt=True)
                for img in image_tags:
                    alt_text = img['alt']
                    if re.search(r'[\u3000-\u303F\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', alt_text):
                        src = img['src']
                        track_code_match = re.search(r'text_place1_(\d+).png', src)
                        if track_code_match:
                            track_code = track_code_match.group(1)
                            code_list.append(track_code)

                print(f'【{date}】の開催は【{len(code_list)}】場です')

                # indexへ遷移、スクレイピング
                second_url = first_url.replace('pay', 'index')
                await fetch(session, second_url, f'data/html/index/index{date}.bin')

                tasks = []
                for code in code_list:
                    for rno in range(1, 13):
                        tasks.append(process_race(session, date, code, rno))

                for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing races"):
                    await task

            except Exception as e:
                print(f'Error processing date {date}')
                with open(f'{date}_html_error.log', 'a') as f:
                    traceback.print_exc(file=f)

        print('処理が終了しました')

# スクリプトとして実行
if __name__ == "__main__":
    asyncio.run(main())  # 修正: スクリプト実行用に `asyncio.run()` を追加
