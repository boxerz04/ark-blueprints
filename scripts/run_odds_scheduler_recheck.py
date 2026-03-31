# scripts/run_odds_scheduler_recheck.py
# -*- coding: utf-8 -*-
import argparse
import csv
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta

import pandas as pd
import schedule
from bs4 import BeautifulSoup

from odds_profile_paths import resolve_odds_profile_paths

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )
}


def project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def setup_logger(log_file: str) -> logging.Logger:
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger("run_odds_scheduler_recheck")
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
    p = argparse.ArgumentParser(description="odds3t再利用型 締切再確認つき 全レーススケジューラ")
    p.add_argument("--timeline", default=None, help="timeline CSV path")
    p.add_argument("--profile", required=True, choices=["5m", "2m"], help="出力profile")
    p.add_argument("--mins_before", type=int, default=5, help="何分前実行か")
    p.add_argument("--python", default=sys.executable, help="python executable")
    p.add_argument("--log_file", default=None, help="ログ出力先")
    p.add_argument("--status_csv", default=None, help="status CSV path")
    p.add_argument(
        "--debug_force_deadline_delay_minutes",
        type=int,
        default=0,
        help="recheckで得た最新締切に疑似的にN分加算する（開発用）",
    )
    return p.parse_args()


def guess_latest_timeline(root: str) -> str | None:
    tl_dir = os.path.join(root, "data", "timeline")
    if not os.path.exists(tl_dir):
        return None
    cands = [f for f in os.listdir(tl_dir) if f.endswith("_timeline_live.csv")]
    if not cands:
        return None
    return os.path.join(tl_dir, max(cands, key=lambda x: x.split("_")[0]))


def fetch_html_text(url: str, timeout: int = 15) -> str:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        status = getattr(resp, "status", 200)
        if status != 200:
            raise RuntimeError(f"HTTP {status}")
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def parse_deadline_times_from_odds3t(html: str) -> dict[str, str]:
    result: dict[str, str] = {}
    soup = BeautifulSoup(html, "html.parser")

    for tr in soup.find_all("tr"):
        row_text = tr.get_text(" ", strip=True)
        if "締切予定時刻" not in row_text:
            continue

        times = []
        cells = tr.find_all(["th", "td"])
        for cell in cells:
            text = cell.get_text(" ", strip=True)
            times.extend(re.findall(r"\b\d{1,2}:\d{2}\b", text))

        if len(times) < 12:
            times = re.findall(r"\b\d{1,2}:\d{2}\b", row_text)

        if len(times) >= 12:
            times = times[:12]
            result = {str(i + 1): times[i] for i in range(12)}
            break

    return result


def fetch_prefetched_odds3t(job: dict) -> tuple[str, datetime | None, bool]:
    jcd2 = str(job["jcd"]).zfill(2)
    url = f"https://www.boatrace.jp/owpc/pc/race/odds3t?rno={job['rno']}&jcd={jcd2}&hd={job['date']}"
    html = fetch_html_text(url, timeout=15)

    times = parse_deadline_times_from_odds3t(html)
    hhmm = times.get(str(int(job["rno"]))) if times else None
    if not hhmm:
        return html, None, False

    latest_dt = datetime.strptime(f"{job['date']} {hhmm}", "%Y%m%d %H:%M")
    return html, latest_dt, True


def parse_result_line(stdout_text: str) -> dict:
    result = {"status": None, "saved_odds3t": None, "saved_odds2tf": None, "saved_odds3f": None}
    lines = [ln.strip() for ln in (stdout_text or "").splitlines() if ln.strip()]
    result_lines = [ln for ln in lines if ln.startswith("RESULT ")]
    if not result_lines:
        return result

    last = result_lines[-1]
    m_status = re.search(r"status=(\w+)", last)
    m_3t = re.search(r"saved_odds3t=(\d+)", last)
    m_2tf = re.search(r"saved_odds2tf=(\d+)", last)
    m_3f = re.search(r"saved_odds3f=(\d+)", last)
    if m_status:
        result["status"] = m_status.group(1)
    if m_3t:
        result["saved_odds3t"] = int(m_3t.group(1))
    if m_2tf:
        result["saved_odds2tf"] = int(m_2tf.group(1))
    if m_3f:
        result["saved_odds3f"] = int(m_3f.group(1))
    return result


def infer_status(returncode: int, parsed: dict) -> tuple[str, int, int, int]:
    status = parsed.get("status")
    saved_3t = parsed.get("saved_odds3t")
    saved_2tf = parsed.get("saved_odds2tf")
    saved_3f = parsed.get("saved_odds3f")
    if status in {"success", "partial", "failed"} and saved_3t is not None and saved_2tf is not None and saved_3f is not None:
        return status, int(saved_3t), int(saved_2tf), int(saved_3f)
    if returncode == 0:
        return (
            "success",
            1 if saved_3t is None else int(saved_3t),
            1 if saved_2tf is None else int(saved_2tf),
            1 if saved_3f is None else int(saved_3f),
        )
    if returncode == 2:
        return (
            "partial",
            0 if saved_3t is None else int(saved_3t),
            0 if saved_2tf is None else int(saved_2tf),
            0 if saved_3f is None else int(saved_3f),
        )
    return (
        "failed",
        0 if saved_3t is None else int(saved_3t),
        0 if saved_2tf is None else int(saved_2tf),
        0 if saved_3f is None else int(saved_3f),
    )


def tail_text(text: str | None, max_len: int = 300) -> str:
    if not text:
        return ""
    one_line = " | ".join([ln.strip() for ln in text.splitlines() if ln.strip()])
    return one_line if len(one_line) <= max_len else one_line[-max_len:]


def ensure_status_csv(status_csv: str):
    os.makedirs(os.path.dirname(status_csv), exist_ok=True)
    if os.path.exists(status_csv):
        return
    header = [
        "date",
        "profile",
        "mins_before",
        "race_id",
        "seq",
        "jcd",
        "rno",
        "title",
        "initial_deadline_dt",
        "latest_deadline_dt",
        "deadline_dt",
        "scheduled_at",
        "started_at",
        "finished_at",
        "returncode",
        "status",
        "saved_odds3t",
        "saved_odds2tf",
        "saved_odds3f",
        "recheck_ok",
        "reschedule_count",
        "stdout_last",
        "stderr_last",
    ]
    with open(status_csv, "w", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=header).writeheader()


def append_status_row(status_csv: str, row: dict):
    with open(status_csv, "a", newline="", encoding="utf-8-sig") as f:
        csv.DictWriter(f, fieldnames=list(row.keys())).writerow(row)


def run_scraper_job_with_prefetched_odds3t(
    job: dict,
    prefetched_html: str,
    python_exec: str,
    status_csv: str,
    stats: dict,
    logger: logging.Logger,
    recheck_ok: bool,
    latest_deadline_dt: datetime | None,
    profile: str,
    mins_before: int,
):
    started_at = datetime.now()
    logger.info("[START] seq=%s race_id=%s jcd=%s rno=%s title=%s", job["seq"], job["race_id"], job["jcd"], job["rno"], job["title"])

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix="_odds3t.html", delete=False) as tf:
            tf.write(prefetched_html)
            tmp_path = tf.name

        script_path = os.path.join(project_root(), "scripts", "scrape_odds_reuse.py")
        cmd = [
            python_exec,
            script_path,
            "--date",
            job["date"],
            "--jcd",
            str(job["jcd"]),
            "--rno",
            str(job["rno"]),
            "--prefetched_odds3t_path",
            tmp_path,
            "--title",
            str(job.get("title", "")),
            "--latest_deadline_dt",
            latest_deadline_dt.strftime("%Y-%m-%d %H:%M") if latest_deadline_dt else "",
            "--profile",
            profile,
            "--mins_before",
            str(mins_before),
        ]
        cp = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        returncode, stdout_text, stderr_text = int(cp.returncode), cp.stdout or "", cp.stderr or ""
    except Exception as e:
        returncode, stdout_text, stderr_text = 1, "", f"subprocess error: {e}"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception as e:
                logger.warning("temp file cleanup failed: %s", e)

    parsed = parse_result_line(stdout_text)
    status, saved_3t, saved_2tf, saved_3f = infer_status(returncode, parsed)
    finished_at = datetime.now()

    row = {
        "date": job["date"],
        "profile": profile,
        "mins_before": mins_before,
        "race_id": job["race_id"],
        "seq": job["seq"],
        "jcd": job["jcd"],
        "rno": job["rno"],
        "title": job["title"],
        "initial_deadline_dt": job["initial_deadline_dt"],
        "latest_deadline_dt": latest_deadline_dt.strftime("%Y-%m-%d %H:%M") if latest_deadline_dt else "",
        "deadline_dt": job["deadline_dt"],
        "scheduled_at": job["scheduled_at"],
        "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at": finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        "returncode": returncode,
        "status": status,
        "saved_odds3t": saved_3t,
        "saved_odds2tf": saved_2tf,
        "saved_odds3f": saved_3f,
        "recheck_ok": 1 if recheck_ok else 0,
        "reschedule_count": job["reschedule_count"],
        "stdout_last": tail_text(stdout_text),
        "stderr_last": tail_text(stderr_text),
    }
    append_status_row(status_csv, row)

    stats["done"] += 1
    stats[status] += 1

    logger.info(
        "[END] seq=%s race_id=%s returncode=%s status=%s saved_odds3t=%s saved_odds2tf=%s saved_odds3f=%s recheck_ok=%s reschedule_count=%s",
        job["seq"],
        job["race_id"],
        returncode,
        status,
        saved_3t,
        saved_2tf,
        saved_3f,
        1 if recheck_ok else 0,
        job["reschedule_count"],
    )


def run_fallback_scraper_job(
    job: dict,
    python_exec: str,
    status_csv: str,
    stats: dict,
    logger: logging.Logger,
    recheck_ok: bool,
    latest_deadline_dt: datetime | None,
    profile: str,
    mins_before: int,
):
    started_at = datetime.now()
    logger.info(
        "[START] seq=%s race_id=%s jcd=%s rno=%s title=%s (fallback=scrape_odds_all.py)",
        job["seq"],
        job["race_id"],
        job["jcd"],
        job["rno"],
        job["title"],
    )

    try:
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
            "--profile",
            profile,
            "--mins_before",
            str(mins_before),
        ]
        cp = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        returncode, stdout_text, stderr_text = int(cp.returncode), cp.stdout or "", cp.stderr or ""
    except Exception as e:
        returncode, stdout_text, stderr_text = 1, "", f"subprocess error: {e}"

    parsed = parse_result_line(stdout_text)
    status, saved_3t, saved_2tf, saved_3f = infer_status(returncode, parsed)
    finished_at = datetime.now()

    row = {
        "date": job["date"],
        "profile": profile,
        "mins_before": mins_before,
        "race_id": job["race_id"],
        "seq": job["seq"],
        "jcd": job["jcd"],
        "rno": job["rno"],
        "title": job["title"],
        "initial_deadline_dt": job["initial_deadline_dt"],
        "latest_deadline_dt": latest_deadline_dt.strftime("%Y-%m-%d %H:%M") if latest_deadline_dt else "",
        "deadline_dt": job["deadline_dt"],
        "scheduled_at": job["scheduled_at"],
        "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at": finished_at.strftime("%Y-%m-%d %H:%M:%S"),
        "returncode": returncode,
        "status": status,
        "saved_odds3t": saved_3t,
        "saved_odds2tf": saved_2tf,
        "saved_odds3f": saved_3f,
        "recheck_ok": 1 if recheck_ok else 0,
        "reschedule_count": job["reschedule_count"],
        "stdout_last": tail_text(stdout_text),
        "stderr_last": tail_text(stderr_text),
    }
    append_status_row(status_csv, row)

    stats["done"] += 1
    stats[status] += 1

    logger.info(
        "[END] seq=%s race_id=%s returncode=%s status=%s saved_odds3t=%s saved_odds2tf=%s saved_odds3f=%s recheck_ok=%s reschedule_count=%s (fallback=scrape_odds_all.py)",
        job["seq"],
        job["race_id"],
        returncode,
        status,
        saved_3t,
        saved_2tf,
        saved_3f,
        1 if recheck_ok else 0,
        job["reschedule_count"],
    )


def handle_job_with_recheck(
    job: dict,
    mins_before: int,
    python_exec: str,
    status_csv: str,
    stats: dict,
    logger: logging.Logger,
    debug_force_deadline_delay_minutes: int = 0,
    profile: str = "5m",
):
    now = datetime.now()
    latest_deadline_dt = None
    recheck_ok = False
    prefetched_html = ""
    prefetched_html_available = False

    try:
        prefetched_html, latest_deadline_dt, recheck_ok = fetch_prefetched_odds3t(job)
        prefetched_html_available = bool(prefetched_html and prefetched_html.strip())
        if not prefetched_html_available:
            logger.warning(
                "[RECHECK_FAIL] seq=%s race_id=%s reason=odds3t fetch failed (empty html)",
                job["seq"],
                job["race_id"],
            )
            logger.warning(
                "[FALLBACK] seq=%s race_id=%s scrape_odds_all.py にフォールバックします",
                job["seq"],
                job["race_id"],
            )
            run_fallback_scraper_job(
                job=job,
                python_exec=python_exec,
                status_csv=status_csv,
                stats=stats,
                logger=logger,
                recheck_ok=False,
                latest_deadline_dt=latest_deadline_dt,
                profile=profile,
                mins_before=mins_before,
            )
            return schedule.CancelJob

        if latest_deadline_dt and debug_force_deadline_delay_minutes:
            latest_deadline_dt = latest_deadline_dt + timedelta(minutes=debug_force_deadline_delay_minutes)

        logger.info(
            "[RECHECK] seq=%s race_id=%s initial_deadline=%s latest_deadline=%s",
            job["seq"],
            job["race_id"],
            job["initial_deadline_dt"],
            latest_deadline_dt.strftime("%Y-%m-%d %H:%M") if latest_deadline_dt else "",
        )

        if latest_deadline_dt and latest_deadline_dt > now + timedelta(minutes=mins_before):
            old_run_at = job["run_at"]
            new_run_at = latest_deadline_dt - timedelta(minutes=mins_before)
            job["deadline_dt"] = latest_deadline_dt.strftime("%Y-%m-%d %H:%M")
            job["scheduled_at"] = new_run_at.strftime("%Y-%m-%d %H:%M:%S")
            job["run_at"] = new_run_at
            job["reschedule_count"] += 1

            schedule.every().day.at(new_run_at.strftime("%H:%M")).do(
                handle_job_with_recheck,
                job=job,
                mins_before=mins_before,
                python_exec=python_exec,
                status_csv=status_csv,
                stats=stats,
                logger=logger,
                debug_force_deadline_delay_minutes=debug_force_deadline_delay_minutes,
                profile=profile,
            )
            logger.info(
                "[RESCHEDULE] seq=%s race_id=%s old_run_at=%s new_run_at=%s latest_deadline=%s count=%s",
                job["seq"],
                job["race_id"],
                old_run_at.strftime("%Y-%m-%d %H:%M:%S"),
                new_run_at.strftime("%Y-%m-%d %H:%M:%S"),
                latest_deadline_dt.strftime("%Y-%m-%d %H:%M"),
                job["reschedule_count"],
            )
            return schedule.CancelJob

    except Exception as e:
        logger.warning(
            "[RECHECK_FAIL] seq=%s race_id=%s reason=odds3t fetch failed: %s",
            job["seq"],
            job["race_id"],
            e,
        )
        logger.warning(
            "[FALLBACK] seq=%s race_id=%s scrape_odds_all.py にフォールバックします",
            job["seq"],
            job["race_id"],
        )
        run_fallback_scraper_job(
            job=job,
            python_exec=python_exec,
            status_csv=status_csv,
            stats=stats,
            logger=logger,
            recheck_ok=False,
            latest_deadline_dt=latest_deadline_dt,
            profile=profile,
            mins_before=mins_before,
        )
        return schedule.CancelJob

    if not recheck_ok:
        logger.warning(
            "[RECHECK_FAIL] seq=%s race_id=%s recheck失敗のため取得済みodds3tで保存処理へ進みます",
            job["seq"],
            job["race_id"],
        )

    run_scraper_job_with_prefetched_odds3t(
        job=job,
        prefetched_html=prefetched_html,
        python_exec=python_exec,
        status_csv=status_csv,
        stats=stats,
        logger=logger,
        recheck_ok=recheck_ok,
        latest_deadline_dt=latest_deadline_dt,
        profile=profile,
        mins_before=mins_before,
    )
    return schedule.CancelJob


def main():
    args = parse_args()
    root = project_root()
    profile_paths = resolve_odds_profile_paths(root, args.profile, args.mins_before)

    log_file = args.log_file or profile_paths.scheduler_log_path()
    logger = setup_logger(log_file)
    logger.info("run_odds_scheduler_recheck 起動 profile=%s mins_before=%s", args.profile, args.mins_before)

    timeline_csv = args.timeline or guess_latest_timeline(root)
    if not timeline_csv:
        logger.error("timeline CSV が指定されず、自動検出もできませんでした。終了します。")
        sys.exit(1)
    logger.info("timeline 読込: %s", timeline_csv)

    if not os.path.exists(timeline_csv):
        logger.error("タイムラインCSVが見つかりません: %s", timeline_csv)
        sys.exit(1)

    df = pd.read_csv(timeline_csv)
    if "deadline_dt" not in df.columns:
        logger.error("timeline CSV に deadline_dt カラムがありません")
        sys.exit(1)

    df["deadline_dt_parsed"] = pd.to_datetime(df["deadline_dt"], errors="coerce")
    df = df.dropna(subset=["deadline_dt_parsed"]).sort_values("deadline_dt_parsed").reset_index(drop=True)

    if df.empty:
        logger.info("実行可能な行がありません。")
        sys.exit(0)

    today = datetime.now().strftime("%Y%m%d")
    status_csv = args.status_csv or profile_paths.status_csv_path(today)
    ensure_status_csv(status_csv)
    logger.info("status CSV: %s", status_csv)

    now = datetime.now()
    jobs = []
    for _, row in df.iterrows():
        dt = row["deadline_dt_parsed"]
        run_at = dt - timedelta(minutes=args.mins_before)
        if run_at < now:
            continue

        jcd = str(row.get("jcd", "")).strip()
        rno = str(row.get("rno", "")).strip()
        if not jcd or not rno:
            continue

        jobs.append(
            {
                "date": dt.strftime("%Y%m%d"),
                "race_id": str(row.get("race_id", "")),
                "seq": row.get("seq", ""),
                "jcd": jcd,
                "rno": rno,
                "title": str(row.get("title", "")),
                "initial_deadline_dt": dt.strftime("%Y-%m-%d %H:%M"),
                "deadline_dt": dt.strftime("%Y-%m-%d %H:%M"),
                "scheduled_at": run_at.strftime("%Y-%m-%d %H:%M:%S"),
                "run_at": run_at,
                "reschedule_count": 0,
            }
        )

    if not jobs:
        logger.info("実行すべきジョブはありません。")
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
            handle_job_with_recheck,
            job=job,
            mins_before=args.mins_before,
            python_exec=args.python,
            status_csv=status_csv,
            stats=stats,
            logger=logger,
            debug_force_deadline_delay_minutes=args.debug_force_deadline_delay_minutes,
            profile=args.profile,
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
