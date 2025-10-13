# scripts/preprocess_course.py
# -*- coding: utf-8 -*-
"""
除外“前”の raw（日次CSV群）から、選手×entry（進入後コース）／選手×wakuban（枠番）それぞれの
直前N走の各着率（1/2/3）と ST 統計を【リーク無し】で集計し、master（除外後）に結合して保存します。

・分母ルール：数値着は出走。非数値は「欠」だけ非出走（分母から除外）。F/L/転/落/妨/不/エ/沈 等は出走。
・旧PEロジック踏襲：groupby(player, course_col) → shift(1) → rolling(N) → reset_index(drop=True)
・entry軸とwakuban軸の両方を同時に出力（列サフィックス：_entry / _waku）
"""

from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import re
import traceback
import numpy as np
import pandas as pd

# ---- 固定カラム名
PLAYER_COL = "player_id"
ENTRY_COL  = "entry"       # 進入後コース
WAKU_COL   = "wakuban"     # 枠番
RANK_COL   = "rank"        # 着順（数値 or 記号）
ST_COL     = "ST"          # スタートタイミング（例: 'F.01','0.07','L.03'）
DATE_COL   = "date"
RACE_COL   = "race_id"

ZEN2HAN = str.maketrans("０１２３４５６７８９ＦＬ．－", "0123456789FL.-")

# =========================
# Helpers
# =========================
def normalize_zenkaku_digits(s: pd.Series) -> pd.Series:
    if s.dtype != object:
        s = s.astype(str)
    return s.str.translate(ZEN2HAN)

def parse_st(val) -> float:
    """
    'F.01' -> -0.01, '0.07' -> +0.07, 'L.03' -> +0.03 / その他は NaN
    """
    if val is None:
        return np.nan
    t = str(val).strip().translate(ZEN2HAN)
    if t == "" or t in {"-", "—", "ー", "―"}:
        return np.nan
    # '3  L' / '3F.01' のような混在に対応
    m = re.match(r"^\d+\s*([FL](?:\.\d+)?)$", t, flags=re.I)
    if m:
        t = m.group(1)
    sign = 1.0
    if t[:1].upper() == "F":
        sign = -1.0
        t = t[1:].strip()
    elif t[:1].upper() == "L":
        # L のときは +（遅れ）
        t = t[1:].strip()
    if re.fullmatch(r"\d{2}", t):
        t = "0." + t
    if t.startswith("."):
        t = "0" + t
    if not re.fullmatch(r"\d+(\.\d+)?", t):
        return np.nan
    try:
        return sign * float(t)
    except ValueError:
        return np.nan

# 分母ルール：数値は出走。非数値は「欠」だけ非出走、他は出走扱い
START_EXCLUDE = {"欠"}

def is_started_from_rank(rank_raw) -> bool:
    if rank_raw is None:
        return False
    t = str(rank_raw).strip().translate(ZEN2HAN)
    if t == "" or t in {"-", "—", "ー", "―"}:
        return False
    if re.fullmatch(r"\d+", t):  # 数値着
        return True
    m = re.match(r"^([FL])(?:\.\d+)?$", t, flags=re.I)  # 'F.01' / 'L.03'
    if m:
        t = m.group(1).upper()
    first = t[0]
    return first not in START_EXCLUDE

def write_crash(reports_dir: Path, stage: str, err: Exception,
                df_like: pd.DataFrame | None, cols_hint: list[str] | None = None):
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    rpt_txt = reports_dir / f"crash_report_{ts}.txt"
    rpt_csv = reports_dir / f"crash_rows_{ts}.csv"
    with open(rpt_txt, "w", encoding="utf-8") as f:
        f.write(f"[STAGE] {stage}\n[ERROR] {repr(err)}\n\n[TRACEBACK]\n")
        f.write("".join(traceback.format_exception(type(err), err, err.__traceback__)))
    if isinstance(df_like, pd.DataFrame) and len(df_like):
        keep = [c for c in [RACE_COL, PLAYER_COL, ENTRY_COL, WAKU_COL, "__source_file"] if c in df_like.columns]
        if cols_hint:
            keep += [c for c in cols_hint if c in df_like.columns and c not in keep]
        df_like[keep].head(120).to_csv(rpt_csv, index=False, encoding="utf-8-sig")

def write_run_log(reports_dir: Path, out_path: Path,
                  start_dt, end_dt, warmup_days: int, n_last: int,
                  rows_master: int, rows_joined: int,
                  raw_used_min, raw_used_max, joined_min, joined_max):
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = reports_dir / f"course_run_{ts}.txt"
    lines = [
        f"[RUN_ID]  {ts}",
        f"[OUT]     {out_path}",
        f"[PERIOD]  start={None if start_dt is None else start_dt.date()}  end={None if end_dt is None else end_dt.date()} (inclusive)",
        f"[WARMUP]  {warmup_days} days",
        f"[N_LAST]  {n_last}",
        f"[ROWS]    master={rows_master}  joined={rows_joined}",
        f"[RAW_USED]date_min={raw_used_min}  date_max={raw_used_max}",
        f"[JOINED ]date_min={joined_min}  date_max={joined_max}",
        "",
        "notes:",
        "- denominator = starts only (数値着 & F/L/転/落/妨/不/エ/沈 等). '欠' は除外。",
        "- leak-free via shift(1) within group (player × entry/wakuban).",
        "- features emitted for BOTH axes: *_entry and *_waku.",
    ]
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[OK] run log          : {log_path}")

def load_raw_dir(raw_dir: Path) -> pd.DataFrame:
    files = sorted(raw_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSVs in {raw_dir}")
    frames = []
    for p in files:
        dfi = pd.read_csv(p, dtype=str, encoding="utf-8-sig", keep_default_na=False, engine="python")
        dfi["__source_file"] = p.name
        frames.append(dfi)
    return pd.concat(frames, ignore_index=True, sort=False)

# =========================
# History features (core)
# =========================
def compute_history_features(raw: pd.DataFrame, course_col: str, n_last: int, suffix: str) -> pd.DataFrame:
    """
    raw（除外前・正規化済）から、(player_id × course_col) 単位で
    直前N走の各着率/分母と ST mean/std を作る。
    suffix: "_entry" または "_waku"
    returns: [race_id, player_id, course_col, finish{1..3}_rate/cnt_lastN{suffix}, st_mean/std_lastN{suffix}]
    """
    assert course_col in raw.columns, f"{course_col} not in raw"
    g = raw.groupby([PLAYER_COL, course_col], sort=False)

    # 直前のみ参照（リーク防止）
    raw["_f1_prev"] = g["exact1_flag"].shift(1)
    raw["_f2_prev"] = g["exact2_flag"].shift(1)
    raw["_f3_prev"] = g["exact3_flag"].shift(1)
    raw["_st_prev"] = g["ST_parsed"].shift(1)
    raw["_stt_prev"] = g["started_mask"].shift(1)

    # 分母に入れるのは「直前が出走」のみ
    raw["_f1_prev_v"] = raw["_f1_prev"].where(raw["_stt_prev"] == True, np.nan)
    raw["_f2_prev_v"] = raw["_f2_prev"].where(raw["_stt_prev"] == True, np.nan)
    raw["_f3_prev_v"] = raw["_f3_prev"].where(raw["_stt_prev"] == True, np.nan)
    raw["_st_prev_v"] = raw["_st_prev"].where(raw["_stt_prev"] == True, np.nan)

    def add_rate(prev_valid_col: str, out_key: str, n: int):
        wins = g[prev_valid_col].rolling(n, min_periods=1).sum()
        cnt  = g[prev_valid_col].rolling(n, min_periods=1).count()
        raw[f"{out_key}_rate_last{n}{suffix}"] = (wins / cnt).reset_index(level=[0,1], drop=True)
        raw[f"{out_key}_cnt_last{n}{suffix}"]  = cnt.reset_index(level=[0,1], drop=True)

    add_rate("_f1_prev_v", "finish1", n_last)
    add_rate("_f2_prev_v", "finish2", n_last)
    add_rate("_f3_prev_v", "finish3", n_last)

    st_mean = g["_st_prev_v"].rolling(n_last, min_periods=1).mean()
    st_std  = g["_st_prev_v"].rolling(n_last, min_periods=2).std()
    raw[f"st_mean_last{n_last}{suffix}"] = st_mean.reset_index(level=[0,1], drop=True)
    raw[f"st_std_last{n_last}{suffix}"]  = st_std.reset_index(level=[0,1], drop=True)

    keep_cols = [
        RACE_COL, PLAYER_COL, course_col,
        f"finish1_rate_last{n_last}{suffix}", f"finish1_cnt_last{n_last}{suffix}",
        f"finish2_rate_last{n_last}{suffix}", f"finish2_cnt_last{n_last}{suffix}",
        f"finish3_rate_last{n_last}{suffix}", f"finish3_cnt_last{n_last}{suffix}",
        f"st_mean_last{n_last}{suffix}",      f"st_std_last{n_last}{suffix}",
    ]
    hist = raw[keep_cols].copy()

    # 一時列の掃除
    raw.drop(columns=[
        "_f1_prev","_f2_prev","_f3_prev","_st_prev","_stt_prev",
        "_f1_prev_v","_f2_prev_v","_f3_prev_v","_st_prev_v"
    ], inplace=True)

    return hist

# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser(description="Attach BOTH entry-based and wakuban-based course history features to master (leak-free).")
    ap.add_argument("--master",      type=str, default="data/processed/master.csv")
    ap.add_argument("--raw-dir",     type=str, default="data/raw")
    ap.add_argument("--out",         type=str, default="data/processed/course/master_course.csv")
    ap.add_argument("--reports-dir", type=str, default="data/processed/course_meta")
    ap.add_argument("--start-date",  type=str, default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end-date",    type=str, default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--warmup-days", type=int, default=180)
    ap.add_argument("--n-last",      type=int, default=10)
    args = ap.parse_args()

    master_path   = Path(args.master)
    raw_dir       = Path(args.raw_dir)
    out_path      = Path(args.out)
    reports_dir   = Path(args.reports_dir)
    warmup_days   = int(args.warmup_days)
    n_last        = int(args.n_last)

    # ---- load master
    try:
        print(f"[INFO] load master : {master_path}")
        master = pd.read_csv(master_path, dtype=str, encoding="utf-8-sig", keep_default_na=False, engine="python")
        # types
        master[PLAYER_COL] = master[PLAYER_COL].astype(str)
        master[RACE_COL]   = master[RACE_COL].astype(str)
        # numeric Int64
        master[ENTRY_COL]  = pd.to_numeric(master.get(ENTRY_COL, np.nan), errors="coerce").astype("Int64")
        master[WAKU_COL]   = pd.to_numeric(master.get(WAKU_COL,  np.nan), errors="coerce").astype("Int64")
        # date
        try:
            master[DATE_COL] = pd.to_datetime(master[DATE_COL])
        except Exception:
            master[DATE_COL] = pd.to_datetime(master[DATE_COL], errors="coerce")
        print(f"[INFO] master shape: {master.shape}")
    except Exception as e:
        write_crash(reports_dir, "load_master", e, None)
        raise

    # ---- decide period
    start_dt = pd.to_datetime(args.start_date, format="%Y-%m-%d", errors="raise") if args.start_date else None
    end_dt   = pd.to_datetime(args.end_date,   format="%Y-%m-%d", errors="raise") if args.end_date   else None
    if (start_dt is None or end_dt is None):
        if DATE_COL not in master.columns or master[DATE_COL].isna().all():
            raise ValueError("master の date が解釈できません。--start-date/--end-date を指定してください。")
        if start_dt is None: start_dt = master[DATE_COL].min()
        if end_dt   is None: end_dt   = master[DATE_COL].max()
    if end_dt < start_dt:
        raise ValueError("end-date must be >= start-date")
    print(f"[INFO] target period: {start_dt.date()} .. {end_dt.date()} (inclusive)")

    # ---- load raw
    try:
        print(f"[INFO] load raw dir: {raw_dir}")
        raw = load_raw_dir(raw_dir)
        # date normalize
        try:
            raw[DATE_COL] = pd.to_datetime(raw[DATE_COL], format="%Y%m%d", errors="coerce")
        except Exception:
            raw[DATE_COL] = pd.to_datetime(raw[DATE_COL], errors="coerce")
        print(f"[INFO] raw shape   : {raw.shape}")
    except Exception as e:
        write_crash(reports_dir, "load_raw_dir", e, None)
        raise

    # ---- limit raw period (warmup applied; no future)
    raw_min = start_dt - timedelta(days=warmup_days)
    raw_max = end_dt
    before = len(raw)
    raw = raw[(raw[DATE_COL] >= raw_min) & (raw[DATE_COL] <= raw_max)].copy()
    after = len(raw)
    print(f"[INFO] raw period filter applied: {raw_min.date()} .. {raw_max.date()} rows {before} -> {after}")

    # ---- normalize (no exclusion here)
    raw[PLAYER_COL] = raw[PLAYER_COL].astype(str)
    raw[RACE_COL]   = raw[RACE_COL].astype(str)
    raw[ENTRY_COL]  = pd.to_numeric(raw.get(ENTRY_COL, np.nan), errors="coerce").astype("Int64")
    raw[WAKU_COL]   = pd.to_numeric(raw.get(WAKU_COL,  np.nan), errors="coerce").astype("Int64")

    raw[RANK_COL]   = normalize_zenkaku_digits(raw.get(RANK_COL, "").astype(str))
    raw["rank_num"] = pd.to_numeric(raw[RANK_COL], errors="coerce")
    raw["started_mask"] = raw[RANK_COL].apply(is_started_from_rank)

    # 着別フラグ（数値着のみ1）… floatにしてNaNと0を区別しやすく
    raw["exact1_flag"] = (raw["rank_num"] == 1).astype(float)
    raw["exact2_flag"] = (raw["rank_num"] == 2).astype(float)
    raw["exact3_flag"] = (raw["rank_num"] == 3).astype(float)

    # ST パース
    raw["ST_parsed"] = raw.get(ST_COL, np.nan)
    if ST_COL in raw.columns:
        raw["ST_parsed"] = raw[ST_COL].apply(parse_st).astype(float)
    else:
        raw["ST_parsed"] = np.nan

    # 時系列ソート
    raw = raw.sort_values([DATE_COL, RACE_COL]).reset_index(drop=True)

    # ---- entry軸の履歴
    try:
        hist_entry = compute_history_features(raw.copy(), ENTRY_COL, n_last, suffix="_entry")
    except Exception as e:
        write_crash(reports_dir, "compute_history_entry", e, raw, cols_hint=[PLAYER_COL, ENTRY_COL, DATE_COL, RACE_COL])
        raise

    # ---- wakuban軸の履歴
    try:
        hist_waku  = compute_history_features(raw.copy(), WAKU_COL,  n_last, suffix="_waku")
    except Exception as e:
        write_crash(reports_dir, "compute_history_waku", e, raw, cols_hint=[PLAYER_COL, WAKU_COL, DATE_COL, RACE_COL])
        raise

    # ---- master 側：当該結果フラグ（検証用）
    m = master.copy()
    m[RANK_COL] = pd.to_numeric(m[RANK_COL], errors="coerce").astype("Int64")
    m["finish1_flag_cur"] = (m[RANK_COL] == 1).astype(int)
    m["finish2_flag_cur"] = (m[RANK_COL] == 2).astype(int)
    m["finish3_flag_cur"] = (m[RANK_COL] == 3).astype(int)

    # ---- 結合（LEFT JOIN）
    try:
        merged = m.merge(
            hist_entry,
            on=[RACE_COL, PLAYER_COL, ENTRY_COL],
            how="left",
            validate="many_to_one"
        )
    except Exception as e:
        write_crash(reports_dir, "merge_entry", e, m, cols_hint=[RACE_COL, PLAYER_COL, ENTRY_COL])
        raise

    try:
        merged = merged.merge(
            hist_waku,
            on=[RACE_COL, PLAYER_COL, WAKU_COL],
            how="left",
            validate="many_to_one"
        )
    except Exception as e:
        write_crash(reports_dir, "merge_waku", e, merged, cols_hint=[RACE_COL, PLAYER_COL, WAKU_COL])
        raise

    # ---- 保存
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote course csv : {out_path}  {merged.shape}")

    # ---- 実行ログ
    raw_used_min = pd.to_datetime(raw[DATE_COL]).min() if len(raw) else None
    raw_used_max = pd.to_datetime(raw[DATE_COL]).max() if len(raw) else None
    joined_min   = pd.to_datetime(merged[DATE_COL]).min() if DATE_COL in merged.columns else None
    joined_max   = pd.to_datetime(merged[DATE_COL]).max() if DATE_COL in merged.columns else None
    write_run_log(
        reports_dir, out_path, start_dt, end_dt, warmup_days, n_last,
        rows_master=len(master), rows_joined=len(merged),
        raw_used_min=raw_used_min, raw_used_max=raw_used_max,
        joined_min=joined_min, joined_max=joined_max
    )

if __name__ == "__main__":
    main()
