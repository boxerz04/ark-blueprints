# scripts/run_odds_scheduler.py
import argparse
import pandas as pd
import subprocess
import time
from datetime import datetime, timedelta
import schedule
import os
import sys

def parse_args():
    p = argparse.ArgumentParser(description="直前オッズ収集スケジューラ")
    p.add_argument(
        "--timeline",
        help="タイムラインCSVのパス",
        required=True,
    )
    p.add_argument(
        "--mins_before",
        type=int,
        default=5,
        help="締切何分前にジョブを実行するか（デフォルト: 5）",
    )
    p.add_argument(
        "--python",
        help="利用するPython実行ファイルのパス（省略時は sys.executable）",
        default=sys.executable,
    )
    return p.parse_args()

def run_scraper(jcd, rno, date, python_exec):
    """指定の会場・R・日付でオッズスクレイパーを実行"""
    script_path = os.path.join("scripts", "scrape_odds.py")
    cmd = [python_exec, script_path, "--date", date, "--jcd", str(jcd), "--rno", str(rno)]
    print(f"[INFO] 実行: {' '.join(cmd)}")
    try:
        subprocess.Popen(cmd)
    except Exception as e:
        print(f"[ERROR] スクレイパー実行失敗: {e}")

def main():
    args = parse_args()
    timeline_csv = args.timeline
    mins_before = args.mins_before

    if not os.path.exists(timeline_csv):
        print(f"[ERROR] タイムラインCSVが見つかりません: {timeline_csv}")
        return

    df = pd.read_csv(timeline_csv)
    if "deadline_dt" not in df.columns:
        print("[ERROR] timeline CSV に deadline_dt がありません")
        return

    jobs = []
    now = datetime.now()

    for _, row in df.iterrows():
        try:
            dt = pd.to_datetime(row["deadline_dt"])
        except Exception:
            print(f"[WARN] deadline_dt を解釈できません: {row['deadline_dt']}")
            continue

        # 実行予定時刻 = 締切時刻 - mins_before 分
        run_at = dt - timedelta(minutes=mins_before)

        if run_at < now:
            continue  # すでに過ぎたジョブはスキップ

        jcd = row.get("jcd")
        rno = row.get("rno")
        date = str(dt.date()).replace("-", "")

        schedule.every().day.at(run_at.strftime("%H:%M")).do(
            run_scraper, jcd=jcd, rno=rno, date=date, python_exec=args.python
        )

        jobs.append((row.get("seq"), jcd, rno, run_at.strftime("%Y-%m-%d %H:%M")))
        print(f"[SCHEDULED] seq={row.get('seq')} jcd={jcd} R={rno} 実行予定={run_at}")

    if not jobs:
        print("実行すべきジョブはありません（対象タイトルなし、あるいは時間切れ）。")
        return

    print(f"{len(jobs)} 件のジョブを登録しました。待機中...")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
