# scripts/preprocess_course.py
# -*- coding: utf-8 -*-
"""
除外後 master.csv に、除外“前” raw（日次CSV群）から計算した
選手×entry 別の直近n走「ちょうど1/2/3着率」と ST 統計（PEロジック）をリーク無しで付与する。

・集計ロジックは旧コードを踏襲：groupby(player_id, entry) → shift(1) → rolling(n) → reset_index(level=[0,1], drop=True)
・raw は正規化/型変換のみ（preprocess.py と同等）※除外はしない
・分母は“出走のみ”（DNS/取消は除外、F/L/S/BFDや数値着は出走扱い）
・master の期間に合わせて、raw は start-date から warmup-days だけ過去に遡って読み込み（未来は遮断）
・出力列サフィックスは “_pe” （旧コードの列名と互換）
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
ENTRY_COL  = "entry"        # 進入後コース（rawにもこの列がある前提）
RANK_COL   = "rank"         # 着順（rawは文字入り可）
ST_COL     = "ST"           # スタートタイミング（例: 'F.01','0.07'）
DATE_COL   = "date"
RACE_COL   = "race_id"

ZEN2HAN = str.maketrans("０１２３４５６７８９．－", "0123456789.-")

# =========================
# Helpers
# =========================
def normalize_zenkaku_digits(s: pd.Series) -> pd.Series:
    if s.dtype != object:
        s = s.astype(str)
    return s.str.translate(ZEN2HAN)

def parse_st(val) -> float:
    """ 'F.01'->-0.01, '0.07'->+0.07, 'L.03'->+0.03 / その他は NaN """
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

# rank 文字列 → 出走フラグ
# ルール: 数値着 = 出走, '欠' だけ非出走, それ以外(F/L/転/落/妨/不/エ/沈)は出走
START_EXCLUDE = {"欠"}  # 分母から外すのはここだけ

def is_started_from_rank(rank_raw) -> bool:
    if rank_raw is None:
        return False
    t = str(rank_raw).strip()
    # 全角→半角統一
    t = t.translate(str.maketrans("０１２３４５６７８９ＦＬ．－", "0123456789FL.-"))
    if t == "" or t in {"-", "—", "ー", "―"}:
        return False

    # 数値なら出走（着順あり）
    if re.fullmatch(r"\d+", t):
        return True

    # F/L の派生（F.01 等）は F/L に潰す
    m = re.match(r"^([FL])(?:\.\d+)?$", t, flags=re.I)
    if m:
        t = m.group(1).upper()

    # 先頭1文字で判定（転/落/妨/不/エ/沈 など）
    first = t[0]
    if first in START_EXCLUDE:
        return False  # 欠のみ非出走
    return True       # それ以外は出走扱い


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
        keep = [c for c in [RACE_COL, PLAYER_COL, ENTRY_COL, "__source_file"] if c in df_like.columns]
        if cols_hint:
            keep += [c for c in cols_hint if c in df_like.columns and c not in keep]
        df_like[keep].head(100).to_csv(rpt_csv, index=False, encoding="utf-8-sig")

def write_run_log(reports_dir: Path, out_path: Path,
                  start_dt, end_dt, warmup_days: int, n_last: int,
                  master_rows: int, hist_rows: int, joined_rows: int,
                  raw_used_min, raw_used_max, joined_min, joined_max):
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = reports_dir / f"course_run_{ts}.txt"
    lines = [
        f"[RUN_ID] {ts}",
        f"[OUT]    {out_path}",
        f"[PERIOD] start={None if start_dt is None else start_dt.date()}  end={None if end_dt is None else end_dt.date()} (inclusive)",
        f"[WARMUP] {warmup_days} days",
        f"[N_LAST] {n_last}",
        f"[ROWS]   master={master_rows}  hist={hist_rows}  joined={joined_rows}",
        f"[RAW_USED] date_min={raw_used_min}  date_max={raw_used_max}",
        f"[JOINED ] date_min={joined_min}  date_max={joined_max}",
        "",
        "note: 分母は“出走のみ”。DNS/取消は除外。flagは“ちょうど”着（以内ではない）。",
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
        dfi = pd.read_csv(p, encoding="utf-8-sig", dtype=str, keep_default_na=False, engine="python")
        dfi["__source_file"] = p.name
        frames.append(dfi)
    return pd.concat(frames, ignore_index=True, sort=False)

# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser(description="Attach course features (PE logic on raw) to master.")
    ap.add_argument("--master",      type=str, default="data/processed/master.csv")
    ap.add_argument("--raw-dir",     type=str, default="data/raw")
    ap.add_argument("--out",         type=str, default="data/processed/course/master_course.csv")
    ap.add_argument("--reports-dir", type=str, default="data/processed/course_meta")
    ap.add_argument("--start-date",  type=str, default=None, help="YYYY-MM-DD inclusive (optional)")
    ap.add_argument("--end-date",    type=str, default=None, help="YYYY-MM-DD inclusive (optional)")
    ap.add_argument("--warmup-days", type=int, default=180,  help="how many days to look back before start-date")
    ap.add_argument("--n-last",      type=int, default=10,   help="window size for last-N stats")
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
        if DATE_COL in master.columns:
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

    # ---- limit raw period (with warmup; future blocked)
    raw_min = start_dt - timedelta(days=warmup_days)
    raw_max = end_dt
    before = len(raw)
    raw = raw[(raw[DATE_COL] >= raw_min) & (raw[DATE_COL] <= raw_max)].copy()
    after = len(raw)
    print(f"[INFO] raw period filter applied: {raw_min.date()} .. {raw_max.date()} rows {before} -> {after}")

    # ---- normalize & types（preprocess.py の正規化/型変換相当。除外はしない）
    # keys
    raw[PLAYER_COL] = raw[PLAYER_COL].astype(str)
    raw[RACE_COL]   = raw[RACE_COL].astype(str)
    master[PLAYER_COL] = master[PLAYER_COL].astype(str)
    master[RACE_COL]   = master[RACE_COL].astype(str)
    # entry は両側とも Int64 に統一
    raw[ENTRY_COL]    = pd.to_numeric(raw[ENTRY_COL], errors="coerce").astype("Int64")
    master[ENTRY_COL] = pd.to_numeric(master[ENTRY_COL], errors="coerce").astype("Int64")
    # rank / ST の数値化
    raw[RANK_COL]   = normalize_zenkaku_digits(raw[RANK_COL].astype(str))
    raw["rank_num"] = pd.to_numeric(raw[RANK_COL], errors="coerce")      # float
    raw[ST_COL]     = raw[ST_COL].apply(parse_st).astype(float)

    # ---- flags & started mask（分母は出走のみ）
    raw["started_mask"] = raw[RANK_COL].apply(is_started_from_rank)
    raw["exact1_flag"] = (raw["rank_num"] == 1).astype(float)  # NaN耐性のためfloat
    raw["exact2_flag"] = (raw["rank_num"] == 2).astype(float)
    raw["exact3_flag"] = (raw["rank_num"] == 3).astype(float)

    # ---- 時系列ソート & グループ
    raw = raw.sort_values([DATE_COL, RACE_COL]).reset_index(drop=True)
    g = raw.groupby([PLAYER_COL, ENTRY_COL], sort=False)

    # ==== 旧コードのPEロジックをそのまま raw に移植 ====
    # 1) 当該を含めないため直前に shift(1)
    raw["_exact1_prev"] = g["exact1_flag"].shift(1)
    raw["_exact2_prev"] = g["exact2_flag"].shift(1)
    raw["_exact3_prev"] = g["exact3_flag"].shift(1)
    raw["_ST_prev"]     = g[ST_COL].shift(1)
    raw["_started_prev"] = g["started_mask"].shift(1)

    # 2) 分母は“直前が出走の行のみ”に限定（出走でない行は NaN 落とし）
    raw["_exact1_prev_v"] = raw["_exact1_prev"].where(raw["_started_prev"] == True, np.nan)
    raw["_exact2_prev_v"] = raw["_exact2_prev"].where(raw["_started_prev"] == True, np.nan)
    raw["_exact3_prev_v"] = raw["_exact3_prev"].where(raw["_started_prev"] == True, np.nan)
    raw["_ST_prev_v"]     = raw["_ST_prev"].where(raw["_started_prev"] == True, np.nan)

    # 3) (player×entry)列として rolling → reset_index(level=[0,1]) で元行に揃えて代入
    def add_lastn_rate_from_valid(prev_valid_col: str, out_key: str, n: int):
        wins = g[prev_valid_col].rolling(n, min_periods=1).sum()
        cnt  = g[prev_valid_col].rolling(n, min_periods=1).count()
        raw[f"{out_key}_rate_last{n}_entry"] = (wins / cnt).reset_index(level=[0,1], drop=True)
        raw[f"{out_key}_cnt_last{n}_entry"]  = cnt.reset_index(level=[0,1], drop=True)

    add_lastn_rate_from_valid("_exact1_prev_v", "finish1", n_last)
    add_lastn_rate_from_valid("_exact2_prev_v", "finish2", n_last)
    add_lastn_rate_from_valid("_exact3_prev_v", "finish3", n_last)

    st_mean = g["_ST_prev_v"].rolling(n_last, min_periods=1).mean()
    st_std  = g["_ST_prev_v"].rolling(n_last, min_periods=2).std()
    raw[f"st_mean_last{n_last}_entry"] = st_mean.reset_index(level=[0,1], drop=True)
    raw[f"st_std_last{n_last}_entry"]  = st_std.reset_index(level=[0,1], drop=True)

    # 4) 一時列の掃除（同じ）
    raw.drop(columns=[
        "_exact1_prev","_exact2_prev","_exact3_prev","_ST_prev","_started_prev",
        "_exact1_prev_v","_exact2_prev_v","_exact3_prev_v","_ST_prev_v"
    ], inplace=True)

    # ---- master に LEFT JOIN（キー：race_id, player_id, entry）
    keep_cols = [
        RACE_COL, PLAYER_COL, ENTRY_COL,
        f"finish1_rate_last{n_last}_entry", f"finish1_cnt_last{n_last}_entry",
        f"finish2_rate_last{n_last}_entry", f"finish2_cnt_last{n_last}_entry",
        f"finish3_rate_last{n_last}_entry", f"finish3_cnt_last{n_last}_entry",
        f"st_mean_last{n_last}_entry",      f"st_std_last{n_last}_entry",
    ]
    hist = raw[keep_cols].copy()

    # master 側の当該結果フラグ（検証・集計用）
    mtmp = master.copy()
    mtmp[RANK_COL] = pd.to_numeric(mtmp[RANK_COL], errors="coerce").astype("Int64")
    mtmp["finish1_flag_cur"] = (mtmp[RANK_COL] == 1).astype(int)
    mtmp["finish2_flag_cur"] = (mtmp[RANK_COL] == 2).astype(int)
    mtmp["finish3_flag_cur"] = (mtmp[RANK_COL] == 3).astype(int)

    try:
        merged = mtmp.merge(hist, on=[RACE_COL, PLAYER_COL, ENTRY_COL], how="left")
    except Exception as e:
        write_crash(reports_dir, "left_join", e, mtmp, cols_hint=[RACE_COL, PLAYER_COL, ENTRY_COL])
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
        master_rows=len(master), hist_rows=len(hist), joined_rows=len(merged),
        raw_used_min=raw_used_min, raw_used_max=raw_used_max,
        joined_min=joined_min, joined_max=joined_max
    )

if __name__ == "__main__":
    main()
