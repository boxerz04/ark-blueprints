# scripts/run_odds_scheduler.py
# -*- coding: utf-8 -*-
import argparse
import pandas as pd
import subprocess
import time
from datetime import datetime, timedelta
import schedule
import os
import sys
import logging

# -----------------------------
# パス系ユーティリティ
# -----------------------------
def project_root() -> str:
    # このファイルの親(=scripts)の親がプロジェクトルート
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# -----------------------------
# ロガー
# -----------------------------
def setup_logger(log_file: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger("run_odds_scheduler")
    logger.setLevel(logging.INFO)
    # 既存ハンドラがあればクリア（タスクスケジューラの多重起動対策）
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # コンソールにも出す（デバッグしやすく）
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger

# -----------------------------
# 引数
# -----------------------------
def parse_args():
    p = argparse.ArgumentParser(description="直前オッズ収集スケジューラ")
    p.add_argument(
        "--timeline",
        help="タイムラインCSVのパス（未指定なら data/timeline から最新 *_timeline_live.csv を自動検出）",
        default=None,
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
    p.add_argument(
        "--log_file",
        help="ログ出力先（省略時は <project_root>/logs/run_odds_scheduler.log）",
        default=None,
    )
    return p.parse_args()

# -----------------------------
# タイムライン自動検出
# -----------------------------
def guess_latest_timeline(root: str) -> str | None:
    tl_dir = os.path.join(root, "data", "timeline")
    if not os.path.exists(tl_dir):
        return None
    # *_timeline_live.csv のみを対象
    cands = [f for f in os.listdir(tl_dir) if f.endswith("_timeline_live.csv")]
    if not cands:
        return None
    # 先頭の YYYYMMDD 部分でソートして最新を選ぶ
    def key_of(fname: str) -> str:
        # 期待形式: 20250901_timeline_live.csv
        return fname.split("_")[0]
    latest = max(cands, key=key_of)
    return os.path.join(tl_dir, latest)

# -----------------------------
# スクレイパー実行
# -----------------------------
def run_scraper(jcd, rno, date, python_exec, job_id, jobs_left, logger: logging.Logger):
    """指定の会場・R・日付でオッズスクレイパーを実行"""
    # 実行するスクリプト（タイトル判定は scrape_odds.py 側で実施）
    script_path = os.path.join(project_root(), "scripts", "scrape_odds.py")
    cmd = [python_exec, script_path, "--date", date, "--jcd", str(jcd), "--rno", str(rno)]
    logger.info(f"[INFO] 実行: {' '.join(cmd)}")
    try:
        # 非同期に起動（待たない）
        subprocess.Popen(cmd)
    except Exception as e:
        logger.error(f"[ERROR] スクレイパー実行失敗: {e}")

    # 実行済みジョブを削除し、残がゼロなら終了
    jobs_left.discard(job_id)
    if not jobs_left:
        logger.info("[INFO] 全ジョブが終了しました。スケジューラを停止します。")
        # schedule のループを抜けるために強制終了
        sys.exit(0)

# -----------------------------
# メイン
# -----------------------------
def main():
    args = parse_args()
    root = project_root()

    log_file = args.log_file or os.path.join(root, "logs", "run_odds_scheduler.log")
    logger = setup_logger(log_file)
    logger.info("run_odds_scheduler 起動")

    # timeline 未指定なら自動検出
    if args.timeline:
        timeline_csv = args.timeline
        logger.info(f"timeline 指定: {timeline_csv}")
    else:
        timeline_csv = guess_latest_timeline(root)
        if timeline_csv:
            logger.info(f"timeline 未指定のため自動選択: {timeline_csv}")
        else:
            logger.error("timeline CSV が指定されず、自動検出もできませんでした。終了します。")
            sys.exit(1)

    if not os.path.exists(timeline_csv):
        logger.error(f"[ERROR] タイムラインCSVが見つかりません: {timeline_csv}")
        sys.exit(1)

    try:
        df = pd.read_csv(timeline_csv)
    except Exception as e:
        logger.error(f"[ERROR] タイムラインCSVの読み込みに失敗しました: {e}")
        sys.exit(1)

    if "deadline_dt" not in df.columns:
        logger.error("[ERROR] timeline CSV に deadline_dt カラムがありません")
        sys.exit(1)

    now = datetime.now()
    jobs_left = set()
    jobs_count = 0

    # 締切時刻順に並べて登録（冪等＆表示用）
    try:
        df["deadline_dt_parsed"] = pd.to_datetime(df["deadline_dt"], errors="coerce")
    except Exception:
        df["deadline_dt_parsed"] = pd.NaT
    df = df.sort_values("deadline_dt_parsed").reset_index(drop=True)

    for _, row in df.iterrows():
        dt = row["deadline_dt_parsed"]
        if pd.isna(dt):
            logger.warning(f"[WARN] deadline_dt を解釈できません: {row.get('deadline_dt')}")
            continue

        run_at = dt - timedelta(minutes=args.mins_before)
        if run_at < now:
            # すでに過ぎたジョブはスキップ（過去レース）
            continue

        jcd = row.get("jcd")
        rno = row.get("rno")
        seq = row.get("seq")
        # run_odds_schedulerは当日日付で動く想定。scrapeはYYYYMMDDを要求
        date = str(dt.date()).replace("-", "")

        # ジョブID（重複防止・完了管理）
        job_id = f"{seq}-{jcd}-{rno}"
        if job_id in jobs_left:
            continue
        jobs_left.add(job_id)

        # schedule は「HH:MM」ベースなので今日の時刻として登録
        hhmm = run_at.strftime("%H:%M")

        # schedule に関数を登録
        schedule.every().day.at(hhmm).do(
            run_scraper,
            jcd=jcd,
            rno=rno,
            date=date,
            python_exec=args.python,
            job_id=job_id,
            jobs_left=jobs_left,
            logger=logger
        )

        jobs_count += 1
        logger.info(f"[SCHEDULED] seq={seq} jcd={jcd} R={rno} 実行予定={run_at.strftime('%Y-%m-%d %H:%M:%S')}")

    if jobs_count == 0:
        logger.info("実行すべきジョブはありません（対象タイトルなし、あるいは時間切れ）。")
        sys.exit(0)

    logger.info(f"{jobs_count} 件のジョブを登録しました。待機中...")

    # ループ：全ジョブ消化で sys.exit(0)
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except SystemExit:
        # 正常終了
        pass
    except Exception as e:
        logger.error(f"[ERROR] run loop で例外: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
