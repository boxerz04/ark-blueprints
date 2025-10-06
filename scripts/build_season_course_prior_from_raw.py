# -*- coding: utf-8 -*-
"""
build_season_course_prior_from_raw.py

- data/raw/<*.csv> を preprocess.load_raw で結合
- 指定期間 [--from, --to] を抽出して
  「季節×場×entry(進入コース) の入着率 prior」を作成
- キー: (place, entry, season_q)  ※四季: spring/summer/autumn/winter

分母: 完走のみ（1〜6位の数値着だけを分母・分子に計上）
      DNS/取消(欠) や F/L/転/落/妨/不/エ/沈 などは分母・分子とも除外

出力:
  - 件数: n_finished, c1..c6
  - 絶対率: p1..p6（0〜1、m=0なら平滑化なし）
  - 同 season_q × entry の全場平均: base_p1..base_p6
  - 差分（相対値）: adv_p1..adv_p6 = p* - base_p*
  - 対数比: lr_p1..lr_p6 = log((p*+eps)/(base_p*+eps)), eps=1/(n_finished + m_strength + 1)

既定: 平滑化 m=0（なし）。--m-strength で Dirichlet 等配平滑化可。
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd

from preprocess import load_raw, cast_and_clean  # 既存ユーティリティ

# =========================
# helpers
# =========================
ZEN2HAN_NUM = str.maketrans("０１２３４５６７８９", "0123456789")

def season_q_from_month(m: int) -> str:
    if 3 <= m <= 5:   return "spring"
    if 6 <= m <= 8:   return "summer"
    if 9 <= m <= 11:  return "autumn"
    return "winter"   # 12,1,2

def normalize_numeric_str(s: pd.Series) -> pd.Series:
    if s.dtype != object:
        s = s.astype(str)
    return s.str.translate(ZEN2HAN_NUM).str.strip()

def autodetect_finish_col(df: pd.DataFrame, user_col: str | None) -> str:
    if user_col and user_col in df.columns: return user_col
    for c in ["rank", "arrival", "chakujun", "chaku", "finish", "finish_order"]:
        if c in df.columns: return c
    raise KeyError("着順列が見つかりません。--finish-col を指定してください。")

def autodetect_entry_col(df: pd.DataFrame, user_col: str | None) -> str:
    if user_col and user_col in df.columns: return user_col
    for c in ["entry", "course"]:
        if c in df.columns: return c
    raise KeyError("entry(進入コース) 列が見つかりません。--entry-col を指定してください。")


# =========================
# main
# =========================
def parse_args():
    ap = argparse.ArgumentParser(description="Build season×place×entry finish-distribution prior with relativization.")
    ap.add_argument("--raw-dir", type=str, default="data/raw", help="日次CSVが並ぶディレクトリ")
    ap.add_argument("--from", dest="from_date", type=str, required=True, help="開始日 YYYYMMDD")
    ap.add_argument("--to", dest="to_date", type=str, required=True, help="終了日 YYYYMMDD")
    ap.add_argument("--finish-col", type=str, default=None, help="着順列名（未指定なら自動検出）")
    ap.add_argument("--entry-col",  type=str, default=None, help="進入コース列名（未指定なら自動検出: entry/course）")
    ap.add_argument("--m-strength", type=int, default=0, help="Dirichlet 等配の疑似件数 m（既定0=平滑化なし）")
    ap.add_argument("--out", type=str, required=True, help="出力CSVパス（data/priors/season_course/... を推奨）")
    ap.add_argument("--link-latest", action="store_true", help="同フォルダに latest.csv を作成/上書き")
    return ap.parse_args()


def main():
    args = parse_args()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) raw 読み込み → 型正規化
    print(f"[INFO] load_raw: {args.raw_dir}")
    df_raw = load_raw(Path(args.raw_dir))
    df, _ = cast_and_clean(df_raw)

    # 2) 期間抽出（inclusive）
    s = pd.to_datetime(args.from_date, format="%Y%m%d")
    e = pd.to_datetime(args.to_date,   format="%Y%m%d")
    df = df.loc[(df["date"] >= s) & (df["date"] <= e)].copy()
    print(f"[INFO] period: {args.from_date}..{args.to_date} -> rows={len(df)}")
    if len(df) == 0:
        if "date" in df_raw.columns and len(df_raw) > 0:
            dmin = pd.to_datetime(df_raw["date"]).min()
            dmax = pd.to_datetime(df_raw["date"]).max()
            raise ValueError(f"指定期間に該当行がありません。利用可能: {dmin:%Y-%m-%d}〜{dmax:%Y-%m-%d}")
        raise ValueError("raw が空、または date 列が見つかりません。")

    # 3) 列の決定
    if "place" not in df.columns:
        raise KeyError("必要列がありません: place")
    rank_col  = autodetect_finish_col(df, args.finish_col)
    entry_col = autodetect_entry_col(df, args.entry_col)
    print(f"[INFO] finish_col = {rank_col}, entry_col = {entry_col}")

    # 4) 四季の付与
    df["season_q"] = df["date"].dt.month.map(season_q_from_month)

    # 5) 完走のみ抽出（1..6 の数値着だけを扱う）
    rnorm = normalize_numeric_str(df[rank_col])
    df["__rank_num"] = pd.to_numeric(rnorm, errors="coerce").astype("Int64")
    finished = df.loc[df["__rank_num"].between(1, 6, inclusive="both")].copy()

    # entry 1..6 のみ
    en = normalize_numeric_str(finished[entry_col])
    finished["__entry"] = pd.to_numeric(en, errors="coerce").astype("Int64")
    finished = finished.loc[finished["__entry"].between(1, 6, inclusive="both")].copy()

    # 6) 分母（完走数）と 1..6着カウント
    keys = ["place", "__entry", "season_q"]
    denom = finished.groupby(keys).size().rename("n_finished").reset_index()
    cnt   = finished.groupby(keys + ["__rank_num"]).size().rename("cnt").reset_index()

    # 7) ピボット c1..c6
    pivot = cnt.pivot_table(index=keys, columns="__rank_num", values="cnt",
                            fill_value=0, aggfunc="sum")
    for k in range(1, 7):
        if k not in pivot.columns:
            pivot[k] = 0
    pivot = pivot[[1, 2, 3, 4, 5, 6]].reset_index().rename(columns={"__entry": "entry", 1:"c1",2:"c2",3:"c3",4:"c4",5:"c5",6:"c6"})

    # 8) マージ（完走はあるが特定着が0のセルも残す）
    tbl = denom.merge(pivot, left_on=keys, right_on=["place","entry","season_q"], how="left")
    for k in range(1, 7):
        ck = f"c{k}"
        if ck not in tbl.columns:
            tbl[ck] = 0
    tbl[["c1","c2","c3","c4","c5","c6"]] = tbl[["c1","c2","c3","c4","c5","c6"]].fillna(0).astype(int)

    # 9) 絶対率 p1..p6
    m = int(args.m_strength)
    if m <= 0:
        denom_all = tbl["n_finished"].replace(0, np.nan)
        for k in range(1, 7):
            tbl[f"p{k}"] = tbl[f"c{k}"] / denom_all
    else:
        alpha = m / 6.0
        denom_all = tbl["n_finished"].astype(float) + m
        for k in range(1, 7):
            tbl[f"p{k}"] = (tbl[f"c{k}"] + alpha) / denom_all

    # 10) 基準（同 season_q × entry の全場平均）と相対化
    base = (
        tbl.groupby(["season_q", "entry"])[[f"p{k}" for k in range(1,7)]]
        .mean()
        .rename(columns={f"p{k}": f"base_p{k}" for k in range(1,7)})
        .reset_index()
    )
    tbl = tbl.merge(base, on=["season_q", "entry"], how="left", validate="many_to_one")

    # 差分: adv_p* = p* - base_p*
    for k in range(1, 7):
        tbl[f"adv_p{k}"] = tbl[f"p{k}"] - tbl[f"base_p{k}"]

    # 対数比: lr_p* = log((p* + eps) / (base_p* + eps))
    eps = 1.0 / (tbl["n_finished"].fillna(0).astype(float) + float(m) + 1.0)
    for k in range(1, 7):
        num = tbl[f"p{k}"].astype(float) + eps
        den = tbl[f"base_p{k}"].astype(float) + eps
        tbl[f"lr_p{k}"] = np.log(num / den)

    # 11) メタ
    tbl["built_from"] = args.from_date
    tbl["built_to"]   = args.to_date
    tbl["m_strength"] = m
    tbl["keys"]       = "place-entry-seasonq"
    tbl["version"]    = 3  # 相対化列を追加

    # 12) 保存
    out_cols = (
        ["place", "entry", "season_q", "n_finished"] +
        [f"c{k}" for k in range(1,7)] +
        [f"p{k}" for k in range(1,7)] +
        [f"base_p{k}" for k in range(1,7)] +
        [f"adv_p{k}" for k in range(1,7)] +
        [f"lr_p{k}" for k in range(1,7)] +
        ["built_from", "built_to", "m_strength", "keys", "version"]
    )
    tbl = tbl.sort_values(["place", "entry", "season_q"]).reset_index(drop=True)
    tbl[out_cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote prior: {out_path} (rows={len(tbl)})")

    # latest.csv の更新
    if args.link_latest:
        latest = out_path.parent / "latest.csv"
        tbl[out_cols].to_csv(latest, index=False, encoding="utf-8-sig")
        print(f"[OK] updated latest: {latest}")


if __name__ == "__main__":
    main()
