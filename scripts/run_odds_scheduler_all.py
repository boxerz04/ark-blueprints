# scripts/run_odds_scheduler_all.py
# -*- coding: utf-8 -*-
import argparse
import csv
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import schedule


def project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def setup_logger(log_file: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger("run_odds_scheduler_all")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def parse_args():
    p = argparse.ArgumentParser(description="全レース直前オッズ収集スケジューラ（検証版）")
    p.add_argument(
        "--timeline",
        default=None,
        help="タイムラインCSVのパス（未指定なら data/timeline から最新 *_timeline_live.csv を自動検出）",
    )
    p.add_argument(
        "--mins_before",
        type=int,
        default=5,
        help="締切何分前にジョブを実行するか（デフォルト: 5）",
    )
    p.add_argument(
        "--python",
        default=sys.executable,
        help="利用するPython実行ファイルのパス（省略時は sys.executable）",
    )
    p.add_argument(
        "--log_file",
        default=None,
        help="ログ出力先（省略時は <project_root>/logs/run_odds_scheduler_all.log）",
    )
    p.add_argument(
        "--status_csv",
        default=None,
        help="ステータスCSV出力先（省略時は data/odds_status/YYYYMMDD_odds_status.csv）",
    )
    return p.parse_args()


def guess_latest_timeline(root: str) -> str | None:
    tl_dir = os.path.join(root, "data", "timeline")
    if not os.path.exists(tl_dir):
        return None
    cands = [f for f in os.listdir(tl_dir) if f.endswith("_timeline_live.csv")]
    if not cands:
        return None

    def key_of(fname: str) -> str:
        return fname.split("_")[0]

    latest = max(cands, key=key_of)
    return os.path.join(tl_dir, latest)


def parse_result_line(stdout_text: str) -> dict:
    result = {"status": None, "saved_odds3t": None, "saved_odds2tf": None}
    if not stdout_text:
        return result

    lines = [ln.strip() for ln in stdout_text.splitlines() if ln.strip()]
    result_lines = [ln for ln in lines if ln.startswith("RESULT ")]
    if not result_lines:
        return result

    last = result_lines[-1]
    m_status = re.search(r"status=(\w+)", last)
    m_3t = re.search(r"saved_odds3t=(\d+)", last)
    m_2tf = re.search(r"saved_odds2tf=(\d+)", last)

    if m_status:
        result["status"] = m_status.group(1)
    if m_3t:
        result["saved_odds3t"] = int(m_3t.group(1))
    if m_2tf:
        result["saved_odds2tf"] = int(m_2tf.group(1))
    return result


def tail_text(text: str | None, max_len: int = 300) -> str:
    if not text:
        return ""
    one_line = " | ".join([ln.strip() for ln in text.splitlines() if ln.strip()])
    if len(one_line) <= max_len:
        return one_line
    return one_line[-max_len:]


def infer_status(returncode: int, parsed: dict) -> tuple[str, int, int]:
    status = parsed.get("status")
    saved_3t = parsed.get("saved_odds3t")
    saved_2tf = parsed.get("saved_odds2tf")

    if status in {"success", "partial", "failed"} and saved_3t is not None and saved_2tf is not None:
        return status, int(saved_3t), int(saved_2tf)

    if returncode == 0:
        return "success", 1 if saved_3t is None else int(saved_3t), 1 if saved_2tf is None else int(saved_2tf)
    if returncode == 2:
        return "partial", 0 if saved_3t is None else int(saved_3t), 0 if saved_2tf is None else int(saved_2tf)
    return "failed", 0 if saved_3t is None else int(saved_3t), 0 if saved_2tf is None else int(saved_2tf)


def ensure_status_csv(status_csv: str):
    os.makedirs(os.path.dirname(status_csv), exist_ok=True)
    if os.path.exists(status_csv):
        return
    header = [
        "date",
        "race_id",
        "seq",
        "jcd",
        "rno",
        "title",
        "deadline_dt",
        "scheduled_at",
        "started_at",
        "finished_at",
        "returncode",
        "status",
        "saved_odds3t",
        "saved_odds2tf",
        "stdout_last",
        "stderr_last",
    ]
    with open(status_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()


def append_status_row(status_csv: str, row: dict):
    with open(status_csv, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writerow(row)


def run_scraper_job(job: dict, python_exec: str, status_csv: str, stats: dict, logger: logging.Logger):
    started_at = datetime.now()
    logger.info(
        "[START] seq=%s race_id=%s jcd=%s rno=%s title=%s",
        job["seq"],
        job["race_id"],
        job["jcd"],
        job["rno"],
        job["title"],
    )

    script_path = os.path.join(project_root(), "scripts", "scrape_odds_all.py")
    cmd = [
        python_exec,
        script_path,
        "--date",
        job["date"],
        "--jcd",
        str(job["jcd"]),
        "--rno",
        str(job["rno"]),
    ]

    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        returncode = int(cp.returncode)
        stdout_text = cp.stdout or ""
        stderr_text = cp.stderr or ""
    except Exception as e:
        returncode = 1
        stdout_text = ""
        stderr_text = f"subprocess error: {e}"

    parsed = parse_result_line(stdout_text)
    status, saved_3t, saved_2tf = infer_status(returncode, parsed)
    finished_at = datetime.now()

    stdout_last = tail_text(stdout_text)
    stderr_last = tail_text(stderr_text)

    row = {
        "date": job["date"],
        "race_id": job["race_id"],
        "seq": job["seq"],
        "jcd": job["jcd"],
        "rno": job["rno"],
        "title": job["title"],
        "deadline_dt": job["deadline_dt"],
        "scheduled_at": job["scheduled_at"],
        "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at": finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        "returncode": returncode,
        "status": status,
        "saved_odds3t": saved_3t,
        "saved_odds2tf": saved_2tf,
        "stdout_last": stdout_last,
        "stderr_last": stderr_last,
    }
    append_status_row(status_csv, row)

    stats["done"] += 1
    stats[status] += 1

    logger.info(
        "[END] seq=%s race_id=%s returncode=%s status=%s saved_odds3t=%s saved_odds2tf=%s",
        job["seq"],
        job["race_id"],
        returncode,
        status,
        saved_3t,
        saved_2tf,
    )
    if stdout_last:
        logger.info("[STDOUT] seq=%s %s", job["seq"], stdout_last)
    if stderr_last:
        logger.info("[STDERR] seq=%s %s", job["seq"], stderr_last)


def main():
    args = parse_args()
    root = project_root()

    log_file = args.log_file or os.path.join(root, "logs", "run_odds_scheduler_all.log")
    logger = setup_logger(log_file)
    logger.info("run_odds_scheduler_all 起動")

    if args.timeline:
        timeline_csv = args.timeline
        logger.info("timeline 指定: %s", timeline_csv)
    else:
        timeline_csv = guess_latest_timeline(root)
        if timeline_csv:
            logger.info("timeline 未指定のため自動選択: %s", timeline_csv)
        else:
            logger.error("timeline CSV が指定されず、自動検出もできませんでした。終了します。")
            sys.exit(1)

    if not os.path.exists(timeline_csv):
        logger.error("タイムラインCSVが見つかりません: %s", timeline_csv)
        sys.exit(1)

    try:
        df = pd.read_csv(timeline_csv)
    except Exception as e:
        logger.error("タイムラインCSVの読み込みに失敗しました: %s", e)
        sys.exit(1)

    if "deadline_dt" not in df.columns:
        logger.error("timeline CSV に deadline_dt カラムがありません")
        sys.exit(1)

    df["deadline_dt_parsed"] = pd.to_datetime(df["deadline_dt"], errors="coerce")
    df = df.dropna(subset=["deadline_dt_parsed"]).sort_values("deadline_dt_parsed").reset_index(drop=True)

    if df.empty:
        logger.info("実行可能な行がありません（deadline_dt不正または空）。")
        sys.exit(0)

    today = datetime.now().strftime("%Y%m%d")
    status_csv = args.status_csv or os.path.join(root, "data", "odds_status", f"{today}_odds_status.csv")
    ensure_status_csv(status_csv)
    logger.info("status CSV: %s", status_csv)

    now = datetime.now()
    jobs = []
    for _, row in df.iterrows():
        dt = row["deadline_dt_parsed"]
        run_at = dt - timedelta(minutes=args.mins_before)
        if run_at < now:
            logger.info(
                "[SKIP_PAST] seq=%s race_id=%s run_at=%s",
                row.get("seq", ""),
                row.get("race_id", ""),
                run_at.strftime("%Y-%m-%d %H:%M:%S"),
            )
            continue

        jcd = str(row.get("jcd", "")).strip()
        rno = str(row.get("rno", "")).strip()
        if not jcd or not rno:
            logger.warning("[SKIP] jcd/rno不足 seq=%s", row.get("seq", ""))
            continue

        date = dt.strftime("%Y%m%d")
        jobs.append(
            {
                "date": date,
                "race_id": str(row.get("race_id", "")),
                "seq": row.get("seq", ""),
                "jcd": jcd,
                "rno": rno,
                "title": str(row.get("title", "")),
                "deadline_dt": dt.strftime("%Y-%m-%d %H:%M"),
                "scheduled_at": run_at.strftime("%Y-%m-%d %H:%M:%S"),
                "run_at": run_at,
            }
        )

    if not jobs:
        logger.info("実行すべきジョブはありません（全件過去時刻、または不正行のみ）。")
        sys.exit(0)

    jobs.sort(key=lambda x: x["run_at"])

    stats = {"total": len(jobs), "done": 0, "success": 0, "partial": 0, "failed": 0}

    for job in jobs:
        hhmm = job["run_at"].strftime("%H:%M")
        logger.info(
            "[SCHEDULED] seq=%s race_id=%s jcd=%s rno=%s 実行予定=%s",
            job["seq"],
            job["race_id"],
            job["jcd"],
            job["rno"],
            job["scheduled_at"],
        )
        schedule.every().day.at(hhmm).do(
            run_scraper_job,
            job=job,
            python_exec=args.python,
            status_csv=status_csv,
            stats=stats,
            logger=logger,
        )

    logger.info("%s 件のジョブを登録しました。待機中...", len(jobs))

    try:
        while stats["done"] < stats["total"]:
            schedule.run_pending()
            time.sleep(1)
    except Exception as e:
        logger.error("run loop で例外: %s", e)
        sys.exit(1)

    logger.info(
        "[SUMMARY] total=%s success=%s partial=%s failed=%s",
        stats["total"],
        stats["success"],
        stats["partial"],
        stats["failed"],
    )


if __name__ == "__main__":
    main()
