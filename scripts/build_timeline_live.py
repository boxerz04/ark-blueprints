# scripts/build_timeline_live.py
import os
import re
import pandas as pd
import warnings
from bs4 import BeautifulSoup
import asyncio
from datetime import datetime
from tqdm import tqdm

warnings.filterwarnings("ignore", category=FutureWarning)

DATA_DIR = "data/html"
TIMELINE_DIR = "data/timeline"
os.makedirs(TIMELINE_DIR, exist_ok=True)

def load_html(path):
    with open(path, "rb") as f:
        return f.read().decode("utf-8", errors="ignore")

def parse_deadline_times_from_raceresult_pd(html):
    """raceresultのテーブルから締切予定時刻を抽出"""
    from io import StringIO
    dfs = pd.read_html(StringIO(html))
    # 先頭テーブルの2行目以降が締切時刻
    row = dfs[0].iloc[0]
    times = {}
    for rno in range(1, 13):
        try:
            v = str(row.iloc[int(rno)]).strip()
            if re.match(r"\d{1,2}:\d{2}", v):
                times[rno] = v
        except Exception:
            continue
    return times

def parse_title_from_racelist(html):
    """racelistページからタイトルを抽出"""
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("div", class_="title16__add2020")
    if not title_tag:
        return ""
    h3 = title_tag.find("h3")
    return h3.get_text(strip=True) if h3 else ""

def is_cancelled(html):
    """レース中止判定"""
    return "レース中止" in html

async def build_timeline(yyyymmdd):
    timeline_rows = []
    seq = 1

    # raceresult ディレクトリから対象日ファイルを探す
    rr_dir = os.path.join(DATA_DIR, "raceresult", yyyymmdd)
    if not os.path.exists(rr_dir):
        print(f"[ERROR] raceresult ディレクトリが見つかりません: {rr_dir}")
        return pd.DataFrame()

    rr_files = [f for f in os.listdir(rr_dir) if f.endswith(".html")]
    for rr_file in tqdm(rr_files, desc="会場処理中"):
        m = re.match(r"raceresult(\d{8})(\d{2})(\d{2})\.html", rr_file)
        if not m:
            continue
        date, jcd, rno = m.groups()
        rr_path = os.path.join(rr_dir, rr_file)
        rr_html = load_html(rr_path)

        # レース中止ならスキップ
        if is_cancelled(rr_html):
            continue

        # 締切時刻取得
        times = parse_deadline_times_from_raceresult_pd(rr_html)
        deadline = times.get(int(rno))
        if not deadline:
            continue

        # racelist からタイトルを取る
        rl_path = os.path.join(DATA_DIR, "racelist", yyyymmdd, f"racelist{date}{jcd}{rno}.html")
        title = ""
        if os.path.exists(rl_path):
            rl_html = load_html(rl_path)
            title = parse_title_from_racelist(rl_html)

        # datetime に変換
        deadline_dt = None
        try:
            deadline_dt = datetime.strptime(f"{date} {deadline}", "%Y%m%d %H:%M")
        except Exception:
            pass

        race_id = f"{date}{jcd}{rno.zfill(2)}"
        timeline_rows.append({
            "seq": seq,
            "race_id": race_id,
            "jcd": jcd,
            "rno": rno,
            "title": title,
            "deadline": deadline,
            "deadline_dt": deadline_dt,
        })
        seq += 1

    # 締切時刻でソート
    df = pd.DataFrame(timeline_rows)
    if not df.empty:
        df = df.sort_values("deadline_dt").reset_index(drop=True)
        df["seq"] = range(1, len(df) + 1)

    return df

def main():
    today = datetime.now().strftime("%Y%m%d")
    print(f"対象日: {today}")

    df = asyncio.run(build_timeline(today))
    if df.empty:
        print("未確定レースが見つかりませんでした。")
        return

    out_path = os.path.join(TIMELINE_DIR, f"{today}_timeline_live.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[SAVED] {out_path}")
    print(df)

if __name__ == "__main__":
    main()
