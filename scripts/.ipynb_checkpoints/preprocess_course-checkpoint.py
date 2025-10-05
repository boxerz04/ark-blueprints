# scripts/preprocess_course.py
# -*- coding: utf-8 -*-
"""
除外後 master（学習母集団）に、除外“前”raw（日次CSV群）を用いた
「選手×entry 別の直前N走 ちょうど1/2/3着率 & ST統計」を LEFT JOIN して出力する。

インターフェースは preprocess.py と揃える：
  --start-date / --end-date  … 学習対象期間（inclusive）。未指定なら master の min/max を使用
  --raw-dir                  … raw 日次CSVディレクトリ（preprocess.py と同じ）
  --warmup-days              … raw を start-date より過去にさかのぼって読む日数（助走）
  --n-last                   … 直前N走の窓長（デフォルト10）
  --reports-dir              … 例外・実行ログ等の出力先

出力は --out で指定された CSV（既定は data/processed/course/master_course.csv）
"""

from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import re
import traceback
import numpy as np
import pandas as pd

# =========================
# Config / Utils
# =========================
ZEN2HAN = str.maketrans("０１２３４５６７８９．－", "0123456789.-")

def normalize_zenkaku_digits(s: pd.Series) -> pd.Series:
    if s.dtype != object:
        s = s.astype(str)
    return s.str.translate(ZEN2HAN)

def parse_st(val) -> float:
    """ 'F.01' -> -0.01, '0.07' -> +0.07, 'L.03' -> +0.03 / その他は NaN """
    if val is None:
        return np.nan
    t = str(val).strip()
    if t == "" or t in {"-", "—", "ー", "―"}:
        return np.nan
    t = t.replace("Ｆ", "F").replace("Ｌ", "L")
    m = re.match(r"^\d+\s*([FL](?:\.\d+)?)$", t, flags=re.I)  # '3  L' / '3F.01'
    if m:
        t = m.group(1)
    sign = 1.0
    if t[:1].lower() == "f":
        sign, t = -1.0, t[1:].strip()
    elif t[:1].lower() == "l":
        sign, t = 1.0, t[1:].strip()
    if re.fullmatch(r"\d{2}", t):
        t = "0." + t
    if t.startswith("."):
        t = "0" + t
    if t == "" or not re.fullmatch(r"\d+(\.\d+)?", t):
        return np.nan
    try:
        return sign * float(t)
    except ValueError:
        return np.nan

def write_crash(reports_dir: Path, stage: str, err: Exception,
                df_like: pd.DataFrame | None, cols_hint: list[str] | None = None):
    """ 例外状況を reports_dir に保存（簡易版） """
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    rpt_txt = reports_dir / f"crash_report_{ts}.txt"
    rpt_csv = reports_dir / f"crash_rows_{ts}.csv"

    with open(rpt_txt, "w", encoding="utf-8") as f:
        f.write(f"[STAGE] {stage}\n")
        f.write(f"[ERROR] {repr(err)}\n\n")
        f.write("[TRACEBACK]\n")
        f.write("".join(traceback.format_exception(type(err), err, err.__traceback__)))
        f.write("\n")
        if isinstance(df_like, pd.DataFrame) and len(df_like):
            srcs = (df_like["__source_file"].dropna().unique().tolist()[:10]
                    if "__source_file" in df_like.columns else [])
            rids = (df_like["race_id"].dropna().astype(str).unique().tolist()[:20]
                    if "race_id" in df_like.columns else [])
            f.write(f"[HINT] __source_file (top): {srcs}\n")
            f.write(f"[HINT] race_id      (top): {rids}\n")

    if isinstance(df_like, pd.DataFrame) and len(df_like):
        keep_cols = ["race_id", "__source_file"]
        if cols_hint:
            for c in cols_hint:
                if c in df_like.columns and c not in keep_cols:
                    keep_cols.append(c)
        for c in df_like.columns:
            if len(keep_cols) >= 20:
                break
            if c not in keep_cols:
                keep_cols.append(c)
        df_like[keep_cols].head(100).to_csv(rpt_csv, index=False, encoding="utf-8-sig")

def write_run_log(reports_dir: Path, out_path: Path,
                  start_dt, end_dt, warmup_days: int, n_last: int,
                  master_rows: int, hist_rows: int, joined_rows: int,
                  df_joined: pd.DataFrame,
                  effective_raw_min_date: pd.Timestamp | None,
                  effective_raw_max_date: pd.Timestamp | None):
    """ 実行サマリ（テキスト） """
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = reports_dir / f"course_run_{ts}.txt"

    min_d = max_d = None
    if "date" in df_joined.columns and len(df_joined):
        try:
            _d = pd.to_datetime(df_joined["date"])
            min_d, max_d = _d.min(), _d.max()
        except Exception:
            pass

    lines = []
    lines.append(f"[RUN_ID] {ts}")
    lines.append(f"[OUT]    {out_path}")
    lines.append(f"[PERIOD] start={None if start_dt is None else start_dt.date()}  "
                 f"end={None if end_dt is None else end_dt.date()}  (inclusive)")
    lines.append(f"[WARMUP] {warmup_days} days")
    lines.append(f"[N_LAST] {n_last}")
    lines.append(f"[ROWS]   master={master_rows}  hist={hist_rows}  joined={joined_rows}")
    lines.append(f"[RAW_USED] date_min={effective_raw_min_date}  date_max={effective_raw_max_date}")
    lines.append(f"[JOINED ] date_min={min_d}  date_max={max_d}")
    lines.append("")
    lines.append("note: PERIOD 未指定時は master の min/max を自動採用。RAW_USED は warmup 適用後の実効範囲。")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[OK] run log          : {log_path}")

# =========================
# Raw loader（preprocess.py と同等の挙動）
# =========================
def load_raw_dir(raw_dir: Path) -> pd.DataFrame:
    files = sorted(raw_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSVs in {raw_dir}")
    frames = []
    for p in files:
        dfi = pd.read_csv(p, encoding="utf-8-sig", dtype=str, keep_default_na=False, engine="python")
        dfi["__source_file"] = p.name
        frames.append(dfi)
    raw = pd.concat(frames, ignore_index=True, sort=False)
    return raw

# =========================
# Core
# =========================
def is_started_from_rank(rank_raw) -> bool:
    """ 数値→出走、F/L/S/BFD 等→出走、欠場/取消/DNS→不出走 """
    if pd.isna(rank_raw):
        return False
    t = str(rank_raw).strip()
    t = normalize_zenkaku_digits(pd.Series([t]))[0]
    if t in {"欠場", "不出走", "取消", "出走取消", "Ｆ欠場", "L欠場", "DNS"}:
        return False
    try:
        float(t)
        return True
    except ValueError:
        pass
    return bool(re.search(r"(F|L|S|BFD)", t, flags=re.I))

# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser(
        description="Attach course-wise last-N results & ST stats to master via left join (period-aware, with warmup)."
    )
    ap.add_argument("--master", type=str, default="data/processed/master.csv",
                    help="除外後 master.csv のパス")
    ap.add_argument("--raw-dir", type=str, default="data/raw",
                    help="除外前 raw の日次CSVディレクトリ（preprocess.py と同じ）")
    ap.add_argument("--out", type=str, default="data/processed/course/master_course.csv",
                    help="出力 CSV パス")
    ap.add_argument("--reports-dir", type=str, default="data/processed/course_meta",
                    help="ログ・例外・実行サマリの出力先フォルダ")
    # 期間指定（inclusive）
    ap.add_argument("--start-date", type=str, default=None,
                    help="(optional) inclusive start date in YYYY-MM-DD")
    ap.add_argument("--end-date", type=str, default=None,
                    help="(optional) inclusive end date in YYYY-MM-DD")
    # コース特徴量固有
    ap.add_argument("--warmup-days", type=int, default=180,
                    help="直前N走の助走として start-date より過去にさかのぼって読む日数")
    ap.add_argument("--n-last", type=int, default=10,
                    help="直前N走の窓長")
    args = ap.parse_args()

    master_path = Path(args.master)
    raw_dir = Path(args.raw_dir)
    out_path = Path(args.out)
    reports_dir = Path(args.reports_dir)

    # ---- 期間オプションのパース（未指定なら None。後で master の min/max を採用）
    start_dt = None
    end_dt = None
    if args.start_date:
        start_dt = pd.to_datetime(args.start_date, format="%Y-%m-%d", errors="raise")
    if args.end_date:
        end_dt = pd.to_datetime(args.end_date,   format="%Y-%m-%d", errors="raise")
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        raise ValueError(f"--end-date ({end_dt.date()}) must be >= --start-date ({start_dt.date()})")

    # ---- load master
    try:
        print(f"[INFO] load master : {master_path}")
        master = pd.read_csv(master_path, dtype=str, encoding="utf-8-sig", keep_default_na=False, engine="python")
        if "date" in master.columns:
            try:
                master["date"] = pd.to_datetime(master["date"])
            except Exception:
                pass
        print(f"[INFO] master shape: {master.shape}")
    except Exception as e:
        write_crash(reports_dir, stage="load_master", err=e, df_like=None)
        raise

    # ---- load raw directory（preprocess.py と同様）
    try:
        print(f"[INFO] load raw dir: {raw_dir}")
        raw = load_raw_dir(raw_dir)
        # raw.date を datetime 化（format ばらつき対応）
        if "date" in raw.columns:
            try:
                raw["date"] = pd.to_datetime(raw["date"], format="%Y%m%d", errors="coerce")
            except Exception:
                raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
        print(f"[INFO] raw shape   : {raw.shape}")
    except Exception as e:
        write_crash(reports_dir, stage="load_raw_dir", err=e, df_like=None)
        raise

    # ---- 期間決定（未指定なら master の min/max を採用）
    if start_dt is None or end_dt is None:
        if "date" not in master.columns or master["date"].isna().all():
            raise ValueError("master に date 列が無い/解釈不可のため、--start-date/--end-date を指定してください。")
        mmin, mmax = master["date"].min(), master["date"].max()
        if start_dt is None:
            start_dt = mmin
        if end_dt is None:
            end_dt = mmax
    print(f"[INFO] target period: {start_dt.date()} .. {end_dt.date()} (inclusive)")

    # ---- raw の期間フィルタ（助走付き、未来遮断）
    warmup_days = int(args.warmup_days)
    n_last = int(args.n_last)
    raw_min = start_dt - timedelta(days=warmup_days)
    raw_max = end_dt
    before = len(raw)
    raw = raw[(raw["date"] >= raw_min) & (raw["date"] <= raw_max)].copy()
    after = len(raw)
    print(f"[INFO] raw period filter applied: {raw_min.date()} .. {raw_max.date()} rows {before} -> {after}")
    effective_raw_min_date = pd.to_datetime(raw["date"]).min() if len(raw) else None
    effective_raw_max_date = pd.to_datetime(raw["date"]).max() if len(raw) else None

    # ---- 必須列チェック（raw）
    need_raw = {"race_id","player_id","entry","rank","ST","date"}
    missing = [c for c in need_raw if c not in raw.columns]
    if missing:
        raise ValueError(f"raw に必須列が不足: {missing}")

    # ---- 正規化（rank数値化、STパース、entryはIntに寄せる）
    raw["rank"] = normalize_zenkaku_digits(raw["rank"].astype(str))
    raw["rank_num"] = pd.to_numeric(raw["rank"], errors="coerce")  # 数字以外→NaN（分子に入らない）
    raw["ST"] = raw["ST"].apply(parse_st).astype(float)
    raw["entry"] = pd.to_numeric(raw["entry"], errors="coerce").astype("Int64")

    # ---- マスク（分母の定義）
    raw["scheduled_mask"] = True
    raw["started_mask"] = raw["rank"].apply(is_started_from_rank)

    # ---- 順位フラグ（“以内”ではなく ちょうど）
    raw["exact1_flag"] = (raw["rank_num"] == 1).astype(int)
    raw["exact2_flag"] = (raw["rank_num"] == 2).astype(int)
    raw["exact3_flag"] = (raw["rank_num"] == 3).astype(int)

    # ---- ソート＆グルーピング
    raw["player_id"] = raw["player_id"].astype(str)
    raw["race_id"] = raw["race_id"].astype(str)
    master["player_id"] = master["player_id"].astype(str)
    master["race_id"] = master["race_id"].astype(str)

    raw = raw.sort_values(["date", "race_id"]).reset_index(drop=True)
    g = raw.groupby(["player_id", "entry"], sort=False)

    def lastn_rate(flag_col: str, mask_col: str, n: int):
        prev = g[flag_col].shift(1)                 # リーク防止：当該行を除外
        prev = prev.where(g[mask_col].shift(1))     # 分母に入れる行だけ残す
        win  = prev.rolling(n, min_periods=1).sum()
        cnt  = prev.rolling(n, min_periods=1).count()
        return (win / cnt), cnt

    # ---- Started ベース（走力寄り）
    r1s, c1s = lastn_rate("exact1_flag", "started_mask", n_last)
    r2s, c2s = lastn_rate("exact2_flag", "started_mask", n_last)
    r3s, c3s = lastn_rate("exact3_flag", "started_mask", n_last)

    st_prev = g["ST"].shift(1).where(g["started_mask"].shift(1))
    st_mean = st_prev.rolling(n_last, min_periods=1).mean()
    st_std  = st_prev.rolling(n_last, min_periods=2).std()

    def attach(name: str, ser: pd.Series):
        raw[name] = ser.reset_index(level=[0, 1], drop=True)

    attach(f"exact1_rate_last{n_last}_start", r1s)
    attach(f"exact1_cnt_last{n_last}_start",  c1s)
    attach(f"exact2_rate_last{n_last}_start", r2s)
    attach(f"exact2_cnt_last{n_last}_start",  c2s)
    attach(f"exact3_rate_last{n_last}_start", r3s)
    attach(f"exact3_cnt_last{n_last}_start",  c3s)
    attach(f"ST_mean_last{n_last}_start",     st_mean)
    attach(f"ST_std_last{n_last}_start",      st_std)

    # ---- Scheduled ベース（任意）
    r1c, c1c = lastn_rate("exact1_flag", "scheduled_mask", n_last)
    r2c, c2c = lastn_rate("exact2_flag", "scheduled_mask", n_last)
    r3c, c3c = lastn_rate("exact3_flag", "scheduled_mask", n_last)
    attach(f"exact1_rate_last{n_last}_sched", r1c)
    attach(f"exact1_cnt_last{n_last}_sched",  c1c)
    attach(f"exact2_rate_last{n_last}_sched", r2c)
    attach(f"exact2_cnt_last{n_last}_sched",  c2c)
    attach(f"exact3_rate_last{n_last}_sched", r3c)
    attach(f"exact3_cnt_last{n_last}_sched",  c3c)
    raw[f"start_rate_last{n_last}"] = (
        raw[f"exact1_cnt_last{n_last}_start"] / raw[f"exact1_cnt_last{n_last}_sched"]
    ).astype(float)

    # ---- master に LEFT JOIN（キー：race_id, player_id, entry）
    keep_cols = [
        "race_id", "player_id", "entry",
        f"exact1_rate_last{n_last}_start", f"exact1_cnt_last{n_last}_start",
        f"exact2_rate_last{n_last}_start", f"exact2_cnt_last{n_last}_start",
        f"exact3_rate_last{n_last}_start", f"exact3_cnt_last{n_last}_start",
        f"ST_mean_last{n_last}_start",     f"ST_std_last{n_last}_start",
        f"exact1_rate_last{n_last}_sched", f"exact1_cnt_last{n_last}_sched",
        f"exact2_rate_last{n_last}_sched", f"exact2_cnt_last{n_last}_sched",
        f"exact3_rate_last{n_last}_sched", f"exact3_cnt_last{n_last}_sched",
        f"start_rate_last{n_last}",
    ]
    hist = raw[keep_cols].copy()

    try:
        merged = master.merge(hist, on=["race_id", "player_id", "entry"], how="left")
    except Exception as e:
        write_crash(reports_dir, stage="left_join", err=e, df_like=master, cols_hint=["race_id","player_id","entry"])
        raise

    # ---- 軽い品質チェック
    try:
        cnt_start = merged[f"exact1_cnt_last{n_last}_start"]
        cnt_sched = merged[f"exact1_cnt_last{n_last}_sched"]
        if cnt_start.notna().any() and cnt_sched.notna().any():
            bad_cnt = ((cnt_start > cnt_sched) | (cnt_sched > n_last))
            if bad_cnt.any():
                print(f"[WARN] cnt bounds violation rows={int(bad_cnt.sum())} (should satisfy cnt_start ≤ cnt_sched ≤ {n_last})")
        for k in ["1","2","3"]:
            r = merged[f"exact{k}_rate_last{n_last}_start"]
            if r.notna().any():
                bad = (r < 0) | (r > 1)
                if bad.any():
                    print(f"[WARN] exact{k}_rate_last{n_last}_start out of [0,1]: rows={int(bad.sum())}")
    except Exception as e:
        write_crash(reports_dir, stage="quality_check", err=e, df_like=merged, cols_hint=["race_id"])

    # ---- 保存
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] wrote course csv : {out_path}  {merged.shape}")
    except Exception as e:
        write_crash(reports_dir, stage="save", err=e, df_like=merged, cols_hint=["race_id","player_id","entry"])
        raise

    # ---- 実行ログ（テキスト）
    try:
        write_run_log(
            reports_dir=reports_dir,
            out_path=out_path,
            start_dt=start_dt,
            end_dt=end_dt,
            warmup_days=warmup_days,
            n_last=n_last,
            master_rows=len(master),
            hist_rows=len(hist),
            joined_rows=len(merged),
            df_joined=merged,
            effective_raw_min_date=effective_raw_min_date,
            effective_raw_max_date=effective_raw_max_date,
        )
    except Exception as e:
        write_crash(reports_dir, stage="write_run_log", err=e, df_like=merged, cols_hint=["race_id"])

if __name__ == "__main__":
    main()
