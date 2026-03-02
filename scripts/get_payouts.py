import os
import time
import argparse
import random
import asyncio
from datetime import datetime, timedelta
import aiohttp

# 一般的なWebブラウザのUser-Agentを偽装
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

async def fetch_page(session, url, date_str, output_dir, semaphore, overwrite=False, max_retries=3):
    file_path = os.path.join(output_dir, f"payouts{date_str}.html")
    
    if os.path.exists(file_path) and not overwrite:
        print(f"[{date_str}] Skipping, file already exists: {file_path}")
        return True

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    
    async with semaphore:
        # 取得ごとにランダム待機 (1〜4秒)し、人間らしいアクセスパターンをエミュレート
        sleep_time = random.uniform(1.0, 4.0)
        await asyncio.sleep(sleep_time)
        
        for attempt in range(max_retries):
            try:
                print(f"[{date_str}] Fetching data... (Attempt {attempt + 1}/{max_retries})")
                
                # aiohttpでのリクエスト (やや長めのタイムアウト設定)
                timeout = aiohttp.ClientTimeout(total=15)
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    response.raise_for_status() # HTTPエラーなら例外を投げる
                    html_content = await response.text()
                    
                    # 結果を保存
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(html_content)
                        
                    print(f"[{date_str}] Successfully saved to {file_path}")
                    return True # 成功したら関数を抜ける
                    
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"[{date_str}] Error on attempt {attempt + 1}: {e}")
                
                if attempt < max_retries - 1:
                    # 指数バックオフ: 2^0, 2^1, ... にランダムなジッター(揺らぎ)を加える
                    backoff_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                    print(f"[{date_str}] Retrying in {backoff_time:.2f} seconds...")
                    await asyncio.sleep(backoff_time)
                else:
                    print(f"[{date_str}] Failed after {max_retries} attempts.")
                    return False

def resolve_dir_path(path):
    if os.path.isabs(path):
        return path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, path)


async def main(start_date_str, end_date_str, output_dir, overwrite=False):
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")

    os.makedirs(output_dir, exist_ok=True)
    
    base_url = "https://www.boatrace.jp/owpc/pc/race/pay?hd={}"
    
    # 同時接続リクエスト数を 3〜5 程度に制限 (ここでは 3 に設定)
    semaphore = asyncio.Semaphore(3)
    
    # 対象日付のリストを作成
    dates_to_fetch = []
    current_date = start_date
    while current_date <= end_date:
        dates_to_fetch.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
        
    print(f"Target dates: {dates_to_fetch}")
    
    # クライアントセッションの作成
    async with aiohttp.ClientSession() as session:
        tasks = []
        for date_str in dates_to_fetch:
            url = base_url.format(date_str)
            # 各日付ごとの非同期タスクを生成
            task = asyncio.create_task(fetch_page(session, url, date_str, output_dir, semaphore, overwrite=overwrite))
            tasks.append(task)
            
        # 全てのタスクを並行して実行 (完了待ち)
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ボートレースの払戻金ページ(HTML)を非同期で取得します。")
    parser.add_argument("--start", type=str, required=True, help="開始日 (YYYYMMDD)")
    parser.add_argument("--end", type=str, required=True, help="終了日 (YYYYMMDD)")
    parser.add_argument("--out-dir", type=str, default="data/html/payouts", help="取得HTML保存先ディレクトリ")
    parser.add_argument("--overwrite", action="store_true", help="既存HTMLがある場合に上書きする")
    
    args = parser.parse_args()
    
    # Windows の aiohttp プロキシ・ソケット終了時エラーを防ぐ定番の Workaround
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    output_dir = resolve_dir_path(args.out_dir)
    asyncio.run(main(args.start, args.end, output_dir, overwrite=args.overwrite))
