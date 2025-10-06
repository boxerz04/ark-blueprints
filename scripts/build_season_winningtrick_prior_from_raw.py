# -*- coding: utf-8 -*-
"""
build_season_winningtrick_prior_from_raw.py

- data/raw/<*.csv> を preprocess.load_raw で結合
- 指定期間 [--from, --to] を抽出して
  「季節×場×entry(進入コース) の決まり手 prior」を作成
- キー: (place, entry, season_q)  ※四季: spring/summer/autumn/winter
- 分母: rank==1 の件数で、かつ決まり手が6種のいずれかで取得できた件数（n_win）
- 決まり手6種（厳密一致のみ採用）:
  逃げ, 差し, まくり, まくり差し, 抜き, 恵まれ
  → 出力ラベル: nige, sashi, makuri, makurizashi, nuki, megumare
- 出力:
  * 件数: c_*
  * 絶対率: p_*（0〜1、m=0なら平滑化なし）
  * 同season_q×entryの全場平均: base_p_*
  * 差分（相対値）: adv_p_* = p_* - base_p_*
  * 対数比: lr_p_* = log((p_*+eps)/(base_p_*+eps))  ※ eps は行ごとに 1/(n_win + m_strength + 1)
- 既定: 平滑化 m=0（なし）。--m-strength で Dirichlet 等配平滑化可。
- 備考: raw はレース行全艇に決まり手が付くが、実際のカウントは rank==1 の行のみ採用。
"""

from pathlib import Path
import argparse
import numpy as np
import pandas as pd

from preprocess import load_raw, cast_and_clean  # 既存ユーティリティ

# =========================
# ヘルパ
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

def autodetect_trick_col(df: pd.DataFrame, user_col: str | None) -> str:
    if user_col and user_col in df.columns: return user_col
    for c in ["winning_trick", "kimarite", "decision", "decisive", "trick"]:
        if c in df.columns: return c
    raise KeyError("決まり手列が見つかりません。--trick-col を指定してください。")

# 厳密一致で採用する6カテゴリ（→ 出力ラベルはローマ字英小文字）
TRICK_CANON = {
    "逃げ": "nige",
    "差し": "sashi",
    "まくり": "makuri",
    "まくり差し": "makurizashi",
    "抜き": "nuki",
    "恵まれ": "megumare",
}
TRICK_LABELS = ["nige","sashi","makuri","makurizashi","nuki","megumare"]


# =========================
# メイン
# =========================
def parse_args():
    ap = argparse.ArgumentParser(description="Build season×place×entry winning-trick prior (rank==1 only, exact match).")
    ap.add_argument("--raw-dir", type=str, default="data/raw", help="日次CSVが並ぶディレクトリ")
    ap.add_argument("--from", dest="from_date", type=str, required=True, help="開始日 YYYYMMDD")
    ap.add_argument("--to", dest="to_date", type=str, required=True, help="終了日 YYYYMMDD")
    ap.add_argument("--finish-col", type=str, default=None, help="着順列名（未指定なら自動検出）")
    ap.add_argument("--entry-col",  type=str, default=None, help="進入コース列名（未指定なら自動検出: entry/course）")
    ap.add_argument("--trick-col",  type=str, default=None, help="決まり手列名（未指定なら自動検出）")
    ap.add_argument("--m-strength", type=int, default=0, help="Dirichlet 等配の疑似件数 m（既定0=平滑化なし）")
    ap.add_argument("--out", type=str, required=True, help="出力CSVパス（data/priors/winning_trick/... を推奨）")
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
        raise ValueError("指定期間に該当行がありません。")

    # 3) 列の決定
    if "place" not in df.columns:
        raise KeyError("必要列がありません: place")
    rank_col  = autodetect_finish_col(df, args.finish_col)
    entry_col = autodetect_entry_col(df, args.entry_col)
    trick_col = autodetect_trick_col(df, args.trick_col)
    print(f"[INFO] finish_col={rank_col}, entry_col={entry_col}, trick_col={trick_col}")

    # 4) 四季
    df["season_q"] = df["date"].dt.month.map(season_q_from_month)

    # 5) 1着のみ採用
    rnorm = normalize_numeric_str(df[rank_col])
    df["__rank_num"] = pd.to_numeric(rnorm, errors="coerce").astype("Int64")
    win = df.loc[df["__rank_num"] == 1].copy()

    # entry 1..6 のみ
    en = normalize_numeric_str(win[entry_col])
    win["__entry"] = pd.to_numeric(en, errors="coerce").astype("Int64")
    win = win.loc[win["__entry"].between(1, 6, inclusive="both")].copy()

    # 決まり手: 厳密一致のみ採用
    win["__trick"] = win[trick_col].astype(str).str.strip().map(TRICK_CANON).astype("object")
    win = win.dropna(subset=["__trick"]).copy()

    # 6) 集計（place×entry×season_q × trick）
    keys = ["place", "__entry", "season_q"]
    cnt = win.groupby(keys + ["__trick"]).size().rename("cnt").reset_index()
    denom = win.groupby(keys).size().rename("n_win").reset_index()

    # 7) ピボット c_*（6カテゴリ）
    pivot = cnt.pivot_table(index=keys, columns="__trick", values="cnt",
                            fill_value=0, aggfunc="sum")
    for lab in TRICK_LABELS:
        if lab not in pivot.columns:
            pivot[lab] = 0
    pivot = pivot[TRICK_LABELS].reset_index().rename(columns={"__entry": "entry"})

    # 8) マージ & 絶対率 p_*
    tbl = denom.merge(pivot, left_on=keys, right_on=["place","entry","season_q"], how="left")
    for lab in TRICK_LABELS:
        tbl[lab] = tbl[lab].fillna(0).astype(int)

    m = int(args.m_strength)
    if m <= 0:
        denom_all = tbl["n_win"].replace(0, np.nan)
        for lab in TRICK_LABELS:
            tbl[f"p_{lab}"] = tbl[lab] / denom_all
    else:
        alpha = m / float(len(TRICK_LABELS))  # 等配
        denom_all = tbl["n_win"].astype(float) + m
        for lab in TRICK_LABELS:
            tbl[f"p_{lab}"] = (tbl[lab] + alpha) / denom_all

    # 9) 基準（同 season_q × entry の全場平均）と相対化
    base = (
        tbl.groupby(["season_q", "entry"])[[f"p_{lab}" for lab in TRICK_LABELS]]
        .mean()
        .rename(columns={f"p_{lab}": f"base_p_{lab}" for lab in TRICK_LABELS})
        .reset_index()
    )
    tbl = tbl.merge(base, on=["season_q", "entry"], how="left", validate="many_to_one")

    # 差分: adv_p_* = p_* - base_p_*
    for lab in TRICK_LABELS:
        tbl[f"adv_p_{lab}"] = tbl[f"p_{lab}"] - tbl[f"base_p_{lab}"]

    # 対数比: lr_p_* = log((p_* + eps) / (base_p_* + eps))
    # eps は行ごとに 1/(n_win + m_strength + 1)
    eps = 1.0 / (tbl["n_win"].fillna(0).astype(float) + float(m) + 1.0)
    for lab in TRICK_LABELS:
        num = tbl[f"p_{lab}"].astype(float) + eps
        den = tbl[f"base_p_{lab}"].astype(float) + eps
        tbl[f"lr_p_{lab}"] = np.log(num / den)

    # 10) 列名整形（c_* にリネーム）
    for lab in TRICK_LABELS:
        tbl.rename(columns={lab: f"c_{lab}"}, inplace=True)

    # 11) メタ
    tbl["built_from"] = args.from_date
    tbl["built_to"]   = args.to_date
    tbl["m_strength"] = m
    tbl["keys"]       = "place-entry-seasonq"
    tbl["version"]    = 3  # 相対化列を追加

    # 12) 保存
    out_cols = (
        ["place", "entry", "season_q", "n_win"] +
        [f"c_{lab}" for lab in TRICK_LABELS] +
        [f"p_{lab}" for lab in TRICK_LABELS] +
        [f"base_p_{lab}" for lab in TRICK_LABELS] +
        [f"adv_p_{lab}" for lab in TRICK_LABELS] +
        [f"lr_p_{lab}" for lab in TRICK_LABELS] +
        ["built_from", "built_to", "m_strength", "keys", "version"]
    )
    tbl = tbl.sort_values(["place","entry","season_q"]).reset_index(drop=True)
    tbl[out_cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] wrote prior: {out_path} (rows={len(tbl)})")

    # latest.csv の更新
    if args.link_latest:
        latest = out_path.parent / "latest.csv"
        tbl[out_cols].to_csv(latest, index=False, encoding="utf-8-sig")
        print(f"[OK] updated latest: {latest}")


if __name__ == "__main__":
    main()
