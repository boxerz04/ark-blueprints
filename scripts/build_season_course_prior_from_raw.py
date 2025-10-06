# -*- coding: utf-8 -*-
"""
build_season_course_prior_from_raw.py
- data/raw/<*.csv> を preprocess.load_raw で結合
- 指定期間 [--from, --to] を抽出して「季節×場×コースの入着率 prior」を作成
- キー: (place, wakuban, season_q)  ※四季: spring/summer/autumn/winter
- 分母: 完走のみ（1〜6位の数値着だけを分母・分子に計上）
        DNS/取消(欠) や F/L/転/落/妨/不/エ/沈 などは **分母からも分子からも除外**
- 出力: c1..c6, p1..p6（0〜1）, perc1..perc6（%・小数1位）
- 平滑化: 既定 m=0（なし）。必要なら --m-strength で Dirichlet 等配平滑化を指定可。
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd
import re

from preprocess import load_raw, cast_and_clean  # 既存ユーティリティを再利用


# =========================
# ヘルパ
# =========================
ZEN2HAN = str.maketrans("０１２３４５６７８９ＦＬ．－", "0123456789FL.-")

def season_q_from_month(m: int) -> str:
    if 3 <= m <= 5:   return "spring"
    if 6 <= m <= 8:   return "summer"
    if 9 <= m <= 11:  return "autumn"
    return "winter"   # 12,1,2

def autodetect_finish_col(df: pd.DataFrame, user_col: str | None) -> str:
    """着順列を自動検出（指定があれば優先）"""
    if user_col and user_col in df.columns:
        return user_col
    for c in ["rank", "arrival", "chakujun", "chaku", "finish", "finish_order"]:
        if c in df.columns:
            return c
    raise KeyError("着順列が見つかりません。--finish-col で列名を指定してください。")

def normalize_rank_str(s: pd.Series) -> pd.Series:
    """全角→半角・トリム"""
    if s.dtype != object:
        s = s.astype(str)
    return s.str.translate(ZEN2HAN).str.strip()


# =========================
# メイン
# =========================
def parse_args():
    ap = argparse.ArgumentParser(description="Build season×place×course finish-distribution prior (1〜6着).")
    ap.add_argument("--raw-dir", type=str, default="data/raw", help="日次CSVが並ぶディレクトリ")
    ap.add_argument("--from", dest="from_date", type=str, required=True, help="開始日 YYYYMMDD")
    ap.add_argument("--to", dest="to_date", type=str, required=True, help="終了日 YYYYMMDD")
    ap.add_argument("--finish-col", type=str, default=None, help="着順列名（未指定なら自動検出）")
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
    df, _report = cast_and_clean(df_raw)

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
        raise ValueError("raw データが空、または date 列が見つかりません。")

    # 3) 必須列 & 着順列
    for c in ["place", "wakuban", "date"]:
        if c not in df.columns:
            raise KeyError(f"必要列がありません: {c}")
    rank_col = autodetect_finish_col(df, args.finish_col)
    print(f"[INFO] finish_col = {rank_col}")

    # 4) 四季を付与
    df["season_q"] = df["date"].dt.month.map(season_q_from_month)

    # 5) 完走のみ抽出（1..6 の数値着だけを扱う）
    rnorm = normalize_rank_str(df[rank_col])
    df["__rank_num"] = pd.to_numeric(rnorm, errors="coerce").astype("Int64")
    finished = df.loc[df["__rank_num"].between(1, 6, inclusive="both")].copy()

    # 6) 分母（完走数）と 1..6着カウント
    keys = ["place", "wakuban", "season_q"]
    denom = finished.groupby(keys).size().rename("n_finished").reset_index()
    cnt   = finished.groupby(keys + ["__rank_num"]).size().rename("cnt").reset_index()

    # 7) ピボット c1..c6
    pivot = cnt.pivot_table(index=keys, columns="__rank_num", values="cnt",
                            fill_value=0, aggfunc="sum")
    for k in range(1, 7):
        if k not in pivot.columns:
            pivot[k] = 0
    pivot = pivot[[1, 2, 3, 4, 5, 6]].rename(columns={i: f"c{i}" for i in range(1, 7)}).reset_index()

    # 8) マージ（完走はあるが特定着が0のセルも残す）
    tbl = denom.merge(pivot, on=keys, how="left")
    for k in range(1, 7):
        ck = f"c{k}"
        tbl[ck] = tbl[ck].fillna(0).astype(int)

    # 9) 比率 p1..p6（m=0なら平滑化なし）
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

    # 11) メタ列
    tbl["built_from"] = args.from_date
    tbl["built_to"]   = args.to_date
    tbl["m_strength"] = m
    tbl["keys"]       = "place-wakuban-seasonq"
    tbl["version"]    = 1

    # 12) 保存
    out_cols = [
        "place", "wakuban", "season_q",
        "n_finished",
        "c1", "c2", "c3", "c4", "c5", "c6",
        "p1", "p2", "p3", "p4", "p5", "p6",
        "built_from", "built_to", "m_strength", "keys", "version",
    ]
    tbl = tbl.sort_values(keys).reset_index(drop=True)
    tbl[out_cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote prior: {out_path} (rows={len(tbl)})")

    # latest.csv の更新
    if args.link_latest:
        latest = out_path.parent / "latest.csv"
        tbl[out_cols].to_csv(latest, index=False, encoding="utf-8-sig")
        print(f"[OK] updated latest: {latest}")


if __name__ == "__main__":
    main()
