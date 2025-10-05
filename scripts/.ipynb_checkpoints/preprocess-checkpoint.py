# preprocess.py
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
    """
    想定外トークンや非数値の頻出値を集計する簡易スキャナ。
    - rank: 数値以外
    - ST/ST_tenji: parse_st 不能の値
    - rate系: 数値化不能の値
    - WX: 数値列の数値化不能・欠損相当文字（空文字, '-', 等）
    """
    rows = []

    # helper: collect frequencies for a boolean mask of "bad" values
    def collect_bad_values(series: pd.Series, mask: pd.Series, colname: str, topn: int = 20):
        vc = series[mask].fillna("(NaN)").astype(str).value_counts().head(topn)
        for val, cnt in vc.items():
            rows.append({"column": colname, "bad_value": val, "count": int(cnt)})

    # rank (非数値)
    if "rank" in df.columns:
        s = normalize_zenkaku_digits(df["rank"].astype(str))
        mask = pd.to_numeric(s, errors="coerce").isna() & s.ne("")
        collect_bad_values(s, mask, "rank")

    # ST / ST_tenji (parse不能)
    for c in ["ST", "ST_tenji"]:
        if c in df.columns:
            s = df[c].astype(str)
            bad_mask = s.apply(lambda x: pd.isna(parse_st(x)))
            # 数値化可能なものは除外（空も含むNaNはbad扱いに入る想定）
            collect_bad_values(s, bad_mask, c)

    # rate 系（非数値）
    for c in RATE_COLS:
        if c in df.columns:
            s = df[c].astype(str)
            mask = pd.to_numeric(s, errors="coerce").isna() & s.ne("")
            collect_bad_values(s, mask, c)

    # WX 数値列（temperature, wind_speed, water_temperature, wave_height）
    for c in ["temperature","wind_speed","water_temperature","wave_height"]:
        if c in df.columns:
            s = df[c].astype(str)
            mask = pd.to_numeric(s, errors="coerce").isna() & s.ne("")
            collect_bad_values(s, mask, c)

    # weather / wind_direction はカテゴリだが、空や記号の頻出を出す
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

    # 1) rank 非数値
    bad_rank = sorted(df.loc[df["rank"].isna(), "race_id"].dropna().unique().tolist())

    # 2) 気象欠損
    if any(c in df.columns for c in WX_COLS):
        subcols = [c for c in WX_COLS if c in df.columns]
        mask_any_na = df[subcols].isna().any(axis=1)
        bad_wx = sorted(df.loc[mask_any_na, "race_id"].dropna().unique().tolist())
    else:
        bad_wx = []

    # 3) 展示ST 非数値
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
    # 6艇×レース、枠ユニーク
    assert len(df) % 6 == 0, "行数が6の倍数ではない"
    g = df.groupby("race_id").size()
    assert g.min()==6 and g.max()==6, "各レースが6行になっていない"
    u = df.groupby("race_id")["wakuban"].nunique()
    assert u.min()==6, "各レースの枠番がユニークでない"

    # 目的変数（is_top2）と展示STの欠損禁止
    assert "is_top2" in df.columns, "is_top2 が見当たりません（rank から作成してください）"
    assert df["is_top2"].isna().sum()==0, "is_top2にNaN"
    if "ST_tenji" in df.columns:
        assert df["ST_tenji"].isna().sum()==0, "ST_tenjiにNaN"

    # rate系 NaN（0.00置換後に残っていないか）
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

        # aggregate
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
    """
    例外の発生状況を reports/ に書き出す。
    - stage: どの処理段階で失敗したか
    - err  : 例外オブジェクト
    - df_like: 直近のデータ（あれば）→ race_id / __source_file など
    - cols_hint: サンプルCSVに含めたい列
    - anomalies_df: 直前スキャンの結果（頻出バッド値）も一緒に保存
    """
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
# Run log writer (plain text)
# =========================
def write_run_log(reports_dir: Path, out_path: Path,
                  start_dt, end_dt, df_final: pd.DataFrame):
    """
    実行の要約をテキストで残す。期間指定の有無・出力先・行数/レース数・
    実データの最小/最大日（フィルタ後）を記録。
    """
    reports_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = reports_dir / f"master_run_{ts}.txt"

    # date列が無い/NaT混在でも壊れないように安全に取得
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
# Main (CLI)
# =========================
def main():
    ap = argparse.ArgumentParser(
        description="Build master.csv from daily raw CSVs with consistent rules (with crash & anomaly reports)."
    )
    ap.add_argument("--raw-dir", type=str, default="data/raw")
    ap.add_argument("--out", type=str, default="data/processed/master.csv")
    ap.add_argument("--reports-dir", type=str, default="data/processed/reports")
    # --- New: 期間指定（任意）。未指定ならフィルタは一切かからず、従来と同じ“全期間”処理になります。
    #     日付は inclusive（start <= date <= end）。format 不一致は明示エラーにして潜在バグを防止。
    ap.add_argument("--start-date", type=str, default=None,
                    help="(optional) inclusive start date in YYYY-MM-DD (e.g., 2024-01-01)")
    ap.add_argument("--end-date", type=str, default=None,
                    help="(optional) inclusive end date   in YYYY-MM-DD (e.g., 2024-12-31)")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir)
    out_path = Path(args.out)
    reports_dir = Path(args.reports_dir)

    # --- New: 期間オプションのパースだけ先に行う（この時点ではフィルタはかけない）。
    #     実際の絞り込みは cast_and_clean により date が datetime 化された後に行う。
    start_dt = None
    end_dt = None
    if args.start_date:
        start_dt = pd.to_datetime(args.start_date, format="%Y-%m-%d", errors="raise")
    if args.end_date:
        end_dt = pd.to_datetime(args.end_date,   format="%Y-%m-%d", errors="raise")
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

    # 事前に“未知パターン頻出値”をスキャン（成功時も失敗時も参考になる）
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
        # スキャン自体の失敗は致命ではないが、手掛かりを残す
        write_crash(reports_dir, stage="scan_anomalies", err=e, df_like=df_raw)

    # --- cast & clean
    try:
        df_cast, conv_report = cast_and_clean(df_raw)
        df_cast = make_is_top2(df_cast)
    except Exception as e:
        cols_hint = ["rank","ST","ST_tenji"] + RATE_COLS + WX_COLS
        write_crash(reports_dir, stage="cast_and_clean", err=e, df_like=df_raw, cols_hint=cols_hint, anomalies_df=anomalies_df)
        raise

    # --- New: optional period filter（指定がある場合のみ適用。未指定なら従来どおり“全期間”）
    #     ・cast_and_clean により date は datetime 化済み（NaT あり得る）
    #     ・inclusive にするため <= を使用。NaT は比較で False になり自動的に落ちる（既存設計と矛盾しない）。
    if start_dt is not None or end_dt is not None:
        before_rows = len(df_cast)
        mask = pd.Series(True, index=df_cast.index)
        if start_dt is not None:
            mask &= (df_cast["date"] >= start_dt)
        if end_dt is not None:
            mask &= (df_cast["date"] <= end_dt)
        # copy() で後段の SettingWithCopyWarning を避ける
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

    # --- save & reports
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_kept.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[OK] wrote master.csv : {out_path}  {df_kept.shape}")
	write_run_log(reports_dir, out_path, start_dt, end_dt, df_kept)
        write_exclusion_reports(reports_dir, drop_info)
        print(f"[OK] reports dir      : {reports_dir}")
    except Exception as e:
        write_crash(reports_dir, stage="save", err=e, df_like=df_kept, cols_hint=["race_id","__source_file"])
        raise


if __name__ == "__main__":
    main()
