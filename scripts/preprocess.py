# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime
import re
import traceback
import numpy as np
import pandas as pd

# =========================
# Config / Columns
# =========================
RATE_COLS = [
    "N_winning_rate","N_2rentai_rate","N_3rentai_rate",
    "LC_winning_rate","LC_2rentai_rate","LC_3rentai_rate",
    "motor_2rentai_rate","motor_3rentai_rate",
    "boat_2rentai_rate","boat_3rentai_rate",
]
WX_COLS = ["temperature","wind_speed","water_temperature","wave_height","weather","wind_direction"]
INT_COLS = [
    "player_id","age","run_once","F","L",
    "motor_number","boat_number","pred_mark","code","R",
    "entry_tenji","wakuban","entry","ST_rank",
    "day","section","is_wakunari",
]
FLOAT_COLS = [
    "weight","ST_mean",
    "temperature","wind_speed","water_temperature","wave_height",
    "time_tenji","Tilt",
]
LEAK_COLS = ["entry","is_wakunari","rank","winning_trick","remarks","henkan_ticket","ST","ST_rank","__source_file"]
ID_COLS = ["race_id","player","player_id","motor_number","boat_number","section_id"]

ZEN2HAN = str.maketrans("０１２３４５６７８９．－", "0123456789.-")

# =========================
# Helpers
# =========================
def season_q_from_month(m: int) -> str:
    if 3 <= m <= 5:   return "spring"
    if 6 <= m <= 8:   return "summer"
    if 9 <= m <= 11:  return "autumn"
    return "winter"   # 12,1,2

def normalize_zenkaku_digits(s: pd.Series) -> pd.Series:
    if s.dtype != object:
        s = s.astype(str)
    return s.str.translate(ZEN2HAN)

def parse_st(val) -> float:
    """
    'F.01' -> -0.01, '0.07' -> +0.07, 'L.03' -> +0.03
    '3  L'/'3F.01' のような混入は 'L'/'F.01' に正規化。その他は NaN。
    """
    if val is None:
        return np.nan
    t = str(val).strip()
    if t == "" or t in {"-", "—", "ー", "―"}:
        return np.nan
    t = t.replace("Ｆ", "F").replace("Ｌ", "L")
    m = re.match(r"^\d+\s*([FL](?:\.\d+)?)$", t, flags=re.I)
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

# =========================
# IO
# =========================
def load_raw(raw_dir: Path) -> pd.DataFrame:
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
# Anomaly Scanner (freq report)
# =========================
def scan_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    def collect_bad_values(series: pd.Series, mask: pd.Series, colname: str, topn: int = 20):
        vc = series[mask].fillna("(NaN)").astype(str).value_counts().head(topn)
        for val, cnt in vc.items():
            rows.append({"column": colname, "bad_value": val, "count": int(cnt)})

    if "rank" in df.columns:
        s = normalize_zenkaku_digits(df["rank"].astype(str))
        mask = pd.to_numeric(s, errors="coerce").isna() & s.ne("")
        collect_bad_values(s, mask, "rank")

    for c in ["ST", "ST_tenji"]:
        if c in df.columns:
            s = df[c].astype(str)
            bad_mask = s.apply(lambda x: pd.isna(parse_st(x)))
            collect_bad_values(s, bad_mask, c)

    for c in RATE_COLS:
        if c in df.columns:
            s = df[c].astype(str)
            mask = pd.to_numeric(s, errors="coerce").isna() & s.ne("")
            collect_bad_values(s, mask, c)

    for c in ["temperature","wind_speed","water_temperature","wave_height"]:
        if c in df.columns:
            s = df[c].astype(str)
            mask = pd.to_numeric(s, errors="coerce").isna() & s.ne("")
            collect_bad_values(s, mask, c)

    for c in ["weather","wind_direction"]:
        if c in df.columns:
            s = df[c].astype(str)
            mask = s.eq("") | s.isna() | s.isin(["-", "—", "ー", "―"])
            collect_bad_values(s, mask, c)

    return pd.DataFrame(rows)

# =========================
# Core transforms
# =========================
def cast_and_clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    report = {}
    out = df.copy()

    # date
    if "date" in out.columns:
        try:
            out["date"] = pd.to_datetime(out["date"], format="%Y%m%d", errors="coerce")
        except Exception:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")

    # rank（全角→半角→Int）
    if "rank" in out.columns:
        out["rank"] = normalize_zenkaku_digits(out["rank"].astype(str))
        rnum = pd.to_numeric(out["rank"], errors="coerce").astype("Int64")
        report["rank"] = {"target":"int","n":len(rnum),"n_nan":int(rnum.isna().sum())}
        out["rank"] = rnum

    # 全角→半角（よく紛れる列）
    maybe_zen = [c for c in ["wakuban","R","entry","entry_tenji","ST_rank","day","section","code"] if c in out.columns]
    for c in maybe_zen:
        out[c] = normalize_zenkaku_digits(out[c].astype(str))

    # INT/FLOAT 基本変換
    for c in INT_COLS:
        if c in out.columns:
            ser = pd.to_numeric(out[c], errors="coerce").astype("Int64")
            report[c] = {"target":"int","n":len(ser),"n_nan":int(ser.isna().sum())}
            out[c] = ser

    for c in FLOAT_COLS:
        if c in out.columns:
            ser = pd.to_numeric(out[c], errors="coerce")
            report[c] = {"target":"float","n":len(ser),"n_nan":int(ser.isna().sum())}
            out[c] = ser

    # rate: float化→NaNは0.0
    present = [c for c in RATE_COLS if c in out.columns]
    if present:
        out[present] = out[present].apply(pd.to_numeric, errors="coerce")
        na_cells = int(out[present].isna().sum().sum())
        if na_cells > 0:
            out[present] = out[present].fillna(0.0)
        report["rate_nan_to_zero_cells"] = na_cells

    # ST/ST_tenji 数値化（符号付き秒）
    if "ST_tenji" in out.columns:
        out["ST_tenji"] = out["ST_tenji"].apply(parse_st).astype(float)
    if "ST" in out.columns:
        out["ST"] = out["ST"].apply(parse_st).astype(float)

    # 展示STレース内順位
    if {"race_id","ST_tenji"}.issubset(out.columns):
        out["ST_tenji_rank"] = (
            out.groupby("race_id")["ST_tenji"].rank(method="min", ascending=True).astype("Int64")
        )

    return out, report

def drop_bad_races(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    info = {}
    if "race_id" not in df.columns:
        raise ValueError("race_id 列が必要です。")

    bad_rank = sorted(df.loc[df["rank"].isna(), "race_id"].dropna().unique().tolist())

    if any(c in df.columns for c in WX_COLS):
        subcols = [c for c in WX_COLS if c in df.columns]
        mask_any_na = df[subcols].isna().any(axis=1)
        bad_wx = sorted(df.loc[mask_any_na, "race_id"].dropna().unique().tolist())
    else:
        bad_wx = []

    mask_st_bad = df["ST_tenji"].isna() if "ST_tenji" in df.columns else pd.Series(False, index=df.index)
    bad_st = sorted(df.loc[mask_st_bad, "race_id"].dropna().unique().tolist())

    before_rows, before_races = len(df), df["race_id"].nunique()
    keep = ~df["race_id"].isin(set(bad_rank) | set(bad_wx) | set(bad_st))
    out = df[keep].copy()
    after_rows, after_races = len(out), out["race_id"].nunique()

    info.update({
        "bad_races_rank": bad_rank,
        "bad_races_wx": bad_wx,
        "bad_races_sttenji": bad_st,
        "rows_before": before_rows, "rows_after": after_rows,
        "races_before": before_races,"races_after": after_races
    })
    return out, info

def quick_checks(df: pd.DataFrame):
    assert len(df) % 6 == 0, "行数が6の倍数ではない"
    g = df.groupby("race_id").size()
    assert g.min()==6 and g.max()==6, "各レースが6行になっていない"
    u = df.groupby("race_id")["wakuban"].nunique()
    assert u.min()==6, "各レースの枠番がユニークでない"

    assert "is_top2" in df.columns, "is_top2 が見当たりません（rank から作成してください）"
    assert df["is_top2"].isna().sum()==0, "is_top2にNaN"
    if "ST_tenji" in df.columns:
        assert df["ST_tenji"].isna().sum()==0, "ST_tenjiにNaN"

    present = [c for c in RATE_COLS if c in df.columns]
    if present:
        assert df[present].isna().sum().sum()==0, "rate系にNaNが残っています"

def write_exclusion_reports(reports_dir: Path, drop_info: dict):
    reports_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    recs = []
    recs += [("rank_nonnumeric", rid, "着順が数字でない（失格・転覆等）") for rid in drop_info["bad_races_rank"]]
    recs += [("weather_missing_any", rid, "気象データに欠損あり") for rid in drop_info["bad_races_wx"]]
    recs += [("sttenji_nonnumeric", rid, "展示STに数値化できない表記あり（L等）") for rid in drop_info["bad_races_sttenji"]]
    df_new = pd.DataFrame(recs, columns=["rule_key","race_id","reason"])

    if len(df_new):
        snap_path = reports_dir / f"excluded_races_{run_id}.csv"
        df_new.to_csv(snap_path, index=False, encoding="utf-8-sig")

        agg_path = reports_dir / "excluded_races.csv"
        if agg_path.exists():
            df_hist = pd.read_csv(agg_path, dtype={"race_id": str}, encoding="utf-8-sig")[["race_id","reason"]]
        else:
            df_hist = pd.DataFrame(columns=["race_id","reason"])
        df_all = pd.concat([df_hist, df_new[["race_id","reason"]]], ignore_index=True)
        df_agg = (
            df_all.groupby("race_id")["reason"]
                  .apply(lambda s: " / ".join(sorted(set(s.dropna()))))
                  .reset_index()
                  .sort_values("race_id")
        )
        df_agg.to_csv(agg_path, index=False, encoding="utf-8-sig")

def make_is_top2(df: pd.DataFrame) -> pd.DataFrame:
    if "rank" in df.columns:
        y = (pd.to_numeric(df["rank"], errors="coerce") <= 2).astype("Int64")
        df = df.copy()
        df["is_top2"] = y
    return df

# =========================
# Crash writer
# =========================
def write_crash(reports_dir: Path, stage: str, err: Exception, df_like: pd.DataFrame | None,
                cols_hint: list[str] | None = None, anomalies_df: pd.DataFrame | None = None):
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

    if isinstance(anomalies_df, pd.DataFrame) and len(anomalies_df):
        anomalies_path = reports_dir / f"anomalies_report_{ts}.csv"
        anomalies_df.sort_values(["column","count"], ascending=[True, False]).to_csv(
            anomalies_path, index=False, encoding="utf-8-sig"
        )

# =========================
# Run log writer
# =========================
def write_run_log(reports_dir: Path, out_path: Path,
                  start_dt, end_dt, df_final: pd.DataFrame):
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = reports_dir / f"master_run_{ts}.txt"

    min_d = max_d = None
    if "date" in df_final.columns and len(df_final):
        try:
            min_d = pd.to_datetime(df_final["date"]).min()
            max_d = pd.to_datetime(df_final["date"]).max()
        except Exception:
            pass

    lines = []
    lines.append(f"[RUN_ID] {ts}")
    lines.append(f"[OUT]    {out_path}")
    lines.append(f"[PERIOD] start={None if start_dt is None else start_dt.date()}  "
                 f"end={None if end_dt is None else end_dt.date()}  (inclusive)")
    lines.append(f"[ROWS]   {len(df_final)}")
    races = int(df_final["race_id"].nunique()) if "race_id" in df_final.columns else None
    lines.append(f"[RACES]  {races}")
    lines.append(f"[DATA]   date_min={min_d}  date_max={max_d}")
    lines.append("")
    lines.append("note: PERIOD は指定がなければ None（従来どおり全期間）")

    with open(log_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[OK] run log          : {log_path}")

# =========================
# Priors loader & merge
# =========================
def _read_prior_latest(root: Path, subdir: str) -> pd.DataFrame:
    p = root / subdir / "latest.csv"
    if not p.exists():
        raise FileNotFoundError(f"prior not found: {p}")
    df = pd.read_csv(p, encoding="utf-8-sig")
    return df

def _assert_unique(df: pd.DataFrame, keys: list[str], name: str):
    dup = df.duplicated(keys, keep=False)
    if dup.any():
        sample = df.loc[dup, keys].head(10)
        raise ValueError(f"[{name}] right keys not unique on {keys}\n{sample}")

def add_season_q(df: pd.DataFrame) -> pd.DataFrame:
    if "date" not in df.columns:
        raise KeyError("date 列が必要です（season_q算出）")
    out = df.copy()
    out["season_q"] = out["date"].dt.month.map(season_q_from_month).astype("object")
    return out

# =========================
# Main (CLI)
# =========================
def main():
    ap = argparse.ArgumentParser(
        description="Build master.csv from daily raw CSVs with consistent rules (+ priors join)."
    )
    ap.add_argument("--raw-dir", type=str, default="data/raw")
    ap.add_argument("--out", type=str, default="data/processed/master.csv")
    ap.add_argument("--reports-dir", type=str, default="data/processed/reports")
    ap.add_argument("--priors-root", type=str, default="data/priors")
    ap.add_argument("--tenji-sd-floor", type=float, default=0.02, help="tenji_z 計算の SD 下限")
    ap.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end-date",   type=str, default=None, help="YYYY-MM-DD inclusive")
    # 個別ON/OFF（検証用）
    ap.add_argument("--no-join-tenji", action="store_true")
    ap.add_argument("--no-join-season-course", action="store_true")
    ap.add_argument("--no-join-winning-trick", action="store_true")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir)
    out_path = Path(args.out)
    reports_dir = Path(args.reports_dir)
    priors_root = Path(args.priors_root)

    # 期間オプションのパース
    start_dt = pd.to_datetime(args.start_date, format="%Y-%m-%d", errors="raise") if args.start_date else None
    end_dt   = pd.to_datetime(args.end_date,   format="%Y-%m-%d", errors="raise") if args.end_date else None
    if start_dt is not None and end_dt is not None and end_dt < start_dt:
        raise ValueError(f"--end-date ({end_dt.date()}) must be >= --start-date ({start_dt.date()})")

    # --- load
    try:
        print(f"[INFO] load raw from : {raw_dir}")
        df_raw = load_raw(raw_dir)
        print(f"[INFO] raw shape     : {df_raw.shape}")
    except Exception as e:
        write_crash(reports_dir, stage="load_raw", err=e, df_like=None)
        raise

    # anomalies
    anomalies_df = pd.DataFrame()
    try:
        anomalies_df = scan_anomalies(df_raw)
        if len(anomalies_df):
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            anomalies_path = reports_dir / f"anomalies_report_{ts}.csv"
            anomalies_df.sort_values(["column","count"], ascending=[True, False]).to_csv(
                anomalies_path, index=False, encoding="utf-8-sig"
            )
            print(f"[INFO] anomalies report saved: {anomalies_path}")
        else:
            print("[INFO] anomalies: none (no obvious bad tokens)")
    except Exception as e:
        write_crash(reports_dir, stage="scan_anomalies", err=e, df_like=df_raw)

    # --- cast & clean
    try:
        df_cast, conv_report = cast_and_clean(df_raw)
        df_cast = make_is_top2(df_cast)
    except Exception as e:
        cols_hint = ["rank","ST","ST_tenji"] + RATE_COLS + WX_COLS
        write_crash(reports_dir, stage="cast_and_clean", err=e, df_like=df_raw, cols_hint=cols_hint, anomalies_df=anomalies_df)
        raise

    # 期間フィルタ（指定時のみ）
    if start_dt is not None or end_dt is not None:
        before_rows = len(df_cast)
        mask = pd.Series(True, index=df_cast.index)
        if start_dt is not None:
            mask &= (df_cast["date"] >= start_dt)
        if end_dt is not None:
            mask &= (df_cast["date"] <= end_dt)
        df_cast = df_cast.loc[mask].copy()
        after_rows = len(df_cast)
        print(f"[INFO] date filter applied : start={start_dt} end={end_dt}  rows {before_rows} -> {after_rows}")

    # --- drop bad races
    try:
        df_kept, drop_info = drop_bad_races(df_cast)
    except Exception as e:
        write_crash(reports_dir, stage="drop_bad_races", err=e, df_like=df_cast, cols_hint=["race_id"], anomalies_df=anomalies_df)
        raise

    print(f"[DROP] rank   : {len(drop_info['bad_races_rank'])}")
    print(f"[DROP] weather: {len(drop_info['bad_races_wx'])}")
    print(f"[DROP] STtenji: {len(drop_info['bad_races_sttenji'])}")
    print(f"[INFO] rows {drop_info['rows_before']} -> {drop_info['rows_after']}")
    print(f"[INFO] races {drop_info['races_before']} -> {drop_info['races_after']}")

    # --- checks
    try:
        quick_checks(df_kept)
    except AssertionError as e:
        write_crash(reports_dir, stage="quick_checks", err=e, df_like=df_kept, cols_hint=["race_id","wakuban","is_top2","ST_tenji"])
        raise
    except Exception as e:
        write_crash(reports_dir, stage="quick_checks", err=e, df_like=df_kept, cols_hint=["race_id"])
        raise

    # ========= ここから prior 結合 =========
    df = df_kept.copy()

    # 1) season_q 付与（場名は raw のまま。地の列名 place をそのまま使う）
    df["season_q"] = df["date"].dt.month.map(season_q_from_month).astype("object")
    # 場名（文字列）で join するため strip のみ（マッピングはしない）
    if "place" in df.columns:
        df["place"] = df["place"].astype(str).str.strip()

    # 2) tenji prior
    if not args.no_join_tenji:
        tenji = _read_prior_latest(priors_root, "tenji")
        # 必要列チェック
        need = {"place","wakuban","season_q","tenji_mu","tenji_sd","n_tenji"}
        miss = need - set(tenji.columns)
        if miss:
            raise KeyError(f"[tenji prior] missing columns: {sorted(miss)}")
        tenji["place"] = tenji["place"].astype(str).str.strip()
        _assert_unique(tenji, ["place","wakuban","season_q"], "tenji")
        before = len(df)
        df = df.merge(
            tenji[["place","wakuban","season_q","tenji_mu","tenji_sd","n_tenji"]],
            on=["place","wakuban","season_q"], how="left", validate="m:1"
        )
        hit = df["tenji_mu"].notna().mean() * 100
        print(f"[INFO] joined tenji prior        : hit={hit:.1f}% rows={before}->{len(df)}")
        # tenji_resid / tenji_z
        if "time_tenji" in df.columns:
            sd_floor = float(args.tenji_sd_floor)
            mu = pd.to_numeric(df["tenji_mu"], errors="coerce")
            sd = pd.to_numeric(df["tenji_sd"], errors="coerce")
            tt = pd.to_numeric(df["time_tenji"], errors="coerce")
            resid = tt - mu
            z = resid / np.maximum(sd, sd_floor)
            df["tenji_resid"] = resid
            df["tenji_z"] = z

    # 3) season_course prior（entry入着率）
    if not args.no_join_season_course:
        sc = _read_prior_latest(priors_root, "season_course")
        need = {"place","entry","season_q","n_finished"} \
               | {f"p{k}" for k in range(1,7)} \
               | {f"base_p{k}" for k in range(1,7)} \
               | {f"adv_p{k}" for k in range(1,7)} \
               | {f"lr_p{k}" for k in range(1,7)}
        miss = need - set(sc.columns)
        if miss:
            raise KeyError(f"[season_course prior] missing columns: {sorted(miss)}")
        sc["place"] = sc["place"].astype(str).str.strip()
        _assert_unique(sc, ["place","entry","season_q"], "season_course")
        keep_sc = ["place","entry","season_q","n_finished"] \
                  + [f"p{k}" for k in range(1,7)] \
                  + [f"base_p{k}" for k in range(1,7)] \
                  + [f"adv_p{k}" for k in range(1,7)] \
                  + [f"lr_p{k}" for k in range(1,7)]
        before = len(df)
        df = df.merge(sc[keep_sc], on=["place","entry","season_q"], how="left", validate="m:1")
        hit = df["n_finished"].notna().mean() * 100
        print(f"[INFO] joined season_course prior: hit={hit:.1f}% rows={before}->{len(df)}")

    # 4) winning_trick prior（決まり手）
    if not args.no_join_winning_trick:
        wt = _read_prior_latest(priors_root, "winning_trick")
        need = {"place","entry","season_q","n_win",
                "p_nige","p_sashi","p_makuri","p_makurizashi","p_nuki","p_megumare",
                "base_p_nige","base_p_sashi","base_p_makuri","base_p_makurizashi","base_p_nuki","base_p_megumare",
                "adv_p_nige","adv_p_sashi","adv_p_makuri","adv_p_makurizashi","adv_p_nuki","adv_p_megumare",
                "lr_p_nige","lr_p_sashi","lr_p_makuri","lr_p_makurizashi","lr_p_nuki","lr_p_megumare"}
        miss = need - set(wt.columns)
        if miss:
            raise KeyError(f"[winning_trick prior] missing columns: {sorted(miss)}")
        wt["place"] = wt["place"].astype(str).str.strip()
        _assert_unique(wt, ["place","entry","season_q"], "winning_trick")
        keep_wt = ["place","entry","season_q","n_win",
                   "p_nige","p_sashi","p_makuri","p_makurizashi","p_nuki","p_megumare",
                   "base_p_nige","base_p_sashi","base_p_makuri","base_p_makurizashi","base_p_nuki","base_p_megumare",
                   "adv_p_nige","adv_p_sashi","adv_p_makuri","adv_p_makurizashi","adv_p_nuki","adv_p_megumare",
                   "lr_p_nige","lr_p_sashi","lr_p_makuri","lr_p_makurizashi","lr_p_nuki","lr_p_megumare"]
        before = len(df)
        df = df.merge(wt[keep_wt], on=["place","entry","season_q"], how="left", validate="m:1")
        hit = df["n_win"].notna().mean() * 100
        print(f"[INFO] joined winning_trick prior: hit={hit:.1f}% rows={before}->{len(df)}")

    # ========= ここまで prior 結合 =========

    # --- save & reports
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] wrote master.csv : {out_path}  {df.shape}")
        write_run_log(reports_dir, out_path, start_dt, end_dt, df)
        write_exclusion_reports(reports_dir, drop_info)
        print(f"[OK] reports dir      : {reports_dir}")
    except Exception as e:
        write_crash(reports_dir, stage="save", err=e, df_like=df, cols_hint=["race_id","__source_file"])
        raise

if __name__ == "__main__":
    main()
