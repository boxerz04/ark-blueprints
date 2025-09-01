# scripts/build_timeline_live.py
# -*- coding: utf-8 -*-
import asyncio
import aiohttp
import pandas as pd
import os
import logging
import argparse
from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm
import warnings
import re

warnings.simplefilter("ignore", category=FutureWarning)

# ===== パス（プロジェクト直下固定）=====
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))  # ark-blueprints/
LOG_DIR = os.path.join(ROOT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "build_timeline_live.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)

BASE_URL = "https://www.boatrace.jp/owpc/pc/race/raceresult"
JCD_LIST = [str(i).zfill(2) for i in range(1, 25)]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/125.0.0.0 Safari/537.36"
}
TIMEOUT = aiohttp.ClientTimeout(total=20)

SEM = asyncio.Semaphore(12)  # 併走数

# ---------- HTTP ----------
async def fetch(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with SEM:
            async with session.get(url) as resp:
                if resp.status != 200:
                    logging.warning(f"HTTP {resp.status} for {url}")
                    return None
                return await resp.text()
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

# ---------- ページ状態判定 ----------
def is_day_canceled(html: str) -> bool:
    """その日の開催自体が中止/順延なら True（タイムラインから除外）"""
    try:
        soup = BeautifulSoup(html, "html.parser")
        h3 = soup.find("h3", class_="title12_title")
        if h3:
            txt = h3.get_text(strip=True)
            if "レース中止" in txt or "順延" in txt:
                return True
    except Exception:
        pass
    return False

# ---------- 時刻抽出（BSで行単位に抜く） ----------
def parse_deadline_times_from_raceresult(html: str) -> dict[str, str] | None:
    """
    raceresultページの「締切予定時刻」行から HH:MM を12個抽出して 1..12R に割り当てる。
    pandas.read_htmlは使わず、BeautifulSoupで tr を正確に掴む。
    """
    soup = BeautifulSoup(html, "html.parser")
    # 「table1」ブロックのうち、「締切予定時刻」を含む tr を探す
    for div in soup.select("div.table1"):
        table = div.find("table")
        if not table:
            continue
        tr_list = table.find_all("tr")
        for tr in tr_list:
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            label = cells[0].get_text(strip=True)
            # 先頭セルが「レース」ではなく、行のどこかに「締切予定時刻」が来る形もあるので柔軟に
            row_text = " ".join(td.get_text(" ", strip=True) for td in cells)
            if "締切予定時刻" in row_text:
                # 時刻っぽいものを左→右に順に抽出
                times = []
                for td in cells:
                    t = td.get_text(strip=True)
                    if re.fullmatch(r"\d{1,2}:\d{2}", t):
                        times.append(t)
                # 12個揃っているのが通常
                if len(times) >= 12:
                    times = times[:12]  # 12を超えても先頭12を採用（通常は12ぴったり）
                    return {str(i + 1): times[i] for i in range(12)}
                # セル構造の差異で1つのセルに複数混在している場合に備え、行全体から拾うフォールバック
                all_text = tr.get_text(" ", strip=True)
                times2 = re.findall(r"\b\d{1,2}:\d{2}\b", all_text)
                if len(times2) >= 12:
                    times2 = times2[:12]
                    return {str(i + 1): times2[i] for i in range(12)}
                logging.error(f"Found only {len(times)} times in row; fallback found {len(times2)}")
                return None
    # 見つからなかった
    logging.error("締切予定時刻 行が見つかりませんでした")
    return None

# ---------- タイトル抽出 ----------
def parse_title_from_html(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
        h3 = soup.find("h3", class_="title16_titleDetail__add2020")
        if not h3:
            return ""
        txt = h3.get_text(separator=" ", strip=True)
        txt = re.sub(r"\s+", " ", txt).replace("\u3000", " ").strip()
        return txt
    except Exception as e:
        logging.error(f"Error parsing title: {e}")
        return ""

# ---------- 1会場処理 ----------
async def process_jcd(session: aiohttp.ClientSession, jcd: str, yyyymmdd: str, seq_start: int):
    """
    1) 任意R（12R）で当日ページを取得（開催有無判定 & 締切行抽出）
    2) 各Rごとに当該ページからタイトル抽出
    """
    url_any = f"{BASE_URL}?rno=12&jcd={jcd}&hd={yyyymmdd}"
    html_any = await fetch(session, url_any)
    if not html_any:
        return [], seq_start

    if is_day_canceled(html_any):
        # その日は中止/順延
        return [], seq_start

    times = parse_deadline_times_from_raceresult(html_any)
    if not times:
        # この会場はスキップ
        return [], seq_start

    # タイトルは各Rページから
    urls = [(rno, f"{BASE_URL}?rno={rno}&jcd={jcd}&hd={yyyymmdd}") for rno in range(1, 13)]
    tasks = [fetch(session, u) for _, u in urls]
    htmls = await asyncio.gather(*tasks, return_exceptions=True)

    rows = []
    seq = seq_start
    for (rno, _), html in zip(urls, htmls):
        title = ""
        if not isinstance(html, Exception) and html:
            # 「※ データはありません。」はレース未確定の通常状態なので、タイトルは読む
            title = parse_title_from_html(html) or ""

        deadline = times.get(str(rno))
        if not deadline:  # 念のため
            continue

        dt_str = f"{yyyymmdd} {deadline}"
        try:
            dt_obj = datetime.strptime(dt_str, "%Y%m%d %H:%M")
            deadline_dt = dt_obj.strftime("%Y-%m-%d %H:%M")
        except Exception:
            deadline_dt = ""

        race_id = f"{yyyymmdd}{jcd}{str(rno).zfill(2)}"
        rows.append({
            "seq": seq,  # 後で振り直す
            "race_id": race_id,
            "jcd": str(int(jcd)),  # 先頭ゼロ落としで出力（例: "05" -> "5"）
            "rno": str(rno),
            "title": title,
            "deadline": deadline,
            "deadline_dt": deadline_dt,
        })
        seq += 1

    return rows, seq

# ---------- 全場 ----------
async def build_timeline(yyyymmdd: str) -> pd.DataFrame:
    connector = aiohttp.TCPConnector(limit=0, ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, timeout=TIMEOUT, connector=connector) as session:
        all_rows = []
        seq = 1
        for jcd in tqdm(JCD_LIST, desc="会場処理中"):
            try:
                rows, seq = await process_jcd(session, jcd, yyyymmdd, seq)
                all_rows.extend(rows)
            except Exception as e:
                logging.error(f"process_jcd failed jcd={jcd}: {e}")

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df["deadline_dt"] = pd.to_datetime(df["deadline_dt"], errors="coerce")
        df = df.dropna(subset=["deadline_dt"]).sort_values("deadline_dt").reset_index(drop=True)
        df["seq"] = df.index + 1
        df["deadline_dt"] = df["deadline_dt"].dt.strftime("%Y-%m-%d %H:%M")

    out_dir = os.path.join(ROOT_DIR, "data", "timeline")
    os.makedirs(out_dir, exist_ok=True)
    save_path = os.path.join(out_dir, f"{yyyymmdd}_timeline_live.csv")
    df.to_csv(save_path, index=False, encoding="utf-8-sig")
    print(f"タイムラインを保存しました: {save_path}")
    logging.info(f"タイムラインを保存しました: {save_path}")

    return df

# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(description="当日タイムライン構築スクリプト")
    parser.add_argument("--date", help="対象日 (YYYYMMDD)。省略時は本日。")
    args = parser.parse_args()

    yyyymmdd = args.date if args.date else datetime.now().strftime("%Y%m%d")
    print(f"対象日: {yyyymmdd}")
    logging.info(f"build_timeline_live started for {yyyymmdd}")

    df = asyncio.run(build_timeline(yyyymmdd))
    if df.empty:
        print("未確定レースが見つかりませんでした。")
        logging.warning("No upcoming races found.")

if __name__ == "__main__":
    main()
