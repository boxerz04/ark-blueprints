# -*- coding: utf-8 -*-
"""
sectional モデル用の特徴量生成スクリプト（短期型 + ステージ絞り込み対応）
- 入力: data/processed/sectional/master_sectional.csv
- 出力:
  - data/processed/sectional/X(.npz or _dense.npz)
  - data/processed/sectional/y.csv            （列名: is_top2）
  - data/processed/sectional/ids.csv
  - models/sectional/latest/feature_pipeline.pkl

ポイント:
- 学習時のみ --stage-filter で race_name をフィルタ（優勝戦/準優勝戦/準優進出戦）
- 推論は adapter + feature_pipeline.pkl によって全レースで動作（本スクリプトの絞り込みは学習時のみ）
- SECTIONAL_COLS はそのまま（節間=短期の核）
- base 由来の長期/固定/ノイズ寄りは EXPLICIT_DROP で除外
- pred_mark は“加点”スコアとして数値特徴で採用（OneHotしない）
- code / R / wakuban / Tilt / time_tenji / age / weight は使う
- date は時系列ソートにのみ使用（学習からは外す）
"""

from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from scipy import sparse
from scipy.sparse import save_npz
import joblib

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


# ========================= 列セット定義 =========================

TARGET = "is_top2"

# 学習に入れない ID 系（code/R/wakuban は使うので含めない）
ID_COLS = [
    "race_id", "player_id", "player",
    "motor_number", "boat_number",
    "section_id",
    "date",  # 並べ替え専用（features からは除外）
]

# そのまま（変更しない）
LEAK_COLS = ["entry", "is_wakunari", "rank", "winning_trick", "remarks",
             "henkan_ticket", "ST", "ST_rank", "__source_file"]

# 節間の核（そのまま）
SECTIONAL_COLS = [
    "ST_mean_current","ST_rank_current","ST_previous_time",
    "score","score_rate",
    "ranking_point_sum","ranking_point_rate",
    "condition_point_sum","condition_point_rate",
    "race_ct_current",
]

# カテゴリは低カーデ前提の安全系のみ（pred_mark は数値で使うため入れない）
SAFE_CAT = [
    "place","wind_direction","sex",
    "race_grade","race_type","race_attribute",
]

# 明示的に除外する列（長期/固定/ノイズ寄り）
EXPLICIT_DROP = [
    "AB_class","weather",
    # 全国/当地の長期レート
    "N_winning_rate","N_2rentai_rate","N_3rentai_rate",
    "LC_winning_rate","LC_2rentai_rate","LC_3rentai_rate",
    # モーター/ボート通期
    "motor_2rentai_rate","motor_3rentai_rate",
    "boat_2rentai_rate","boat_3rentai_rate",
    # 固定プロフィール（age/weight は今回は使う）
    "team","origin",
    # 展示整備まわり（今回は使わない）
    "precondition_1","precondition_2","propeller","parts_exchange","counter_weight",
    # 情報ラベル類（今回は使わない）
    "race_name","title","schedule","timetable",
]

# 使用を明示する “守るべき列”
KEEP_FORCE = set([
    "code","R","wakuban","pred_mark","Tilt","time_tenji","age","weight"
] + SECTIONAL_COLS)


# ========================= 引数/補助 =========================

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Build features for SECTIONAL model (short-term)")
    ap.add_argument("--in", dest="in_path", default="data/processed/sectional/master_sectional.csv",
                    help="master_sectional.csv")
    ap.add_argument("--out-dir", default="data/processed/sectional",
                    help="出力ディレクトリ（X/y/ids）")
    ap.add_argument("--model-dir", default="models/sectional/latest",
                    help="feature_pipeline.pkl の出力先")
    ap.add_argument("--max-cat-card", type=int, default=50,
                    help="自動カテゴリに採用する最大カーディナリティ")
    # ★ 学習時のみステージ絞り込み（優勝戦/準優勝戦/準優進出戦など）
    ap.add_argument(
        "--stage-filter",
        default="",
        help="学習対象を race_name で絞る（正規表現プリセット）。例: finals,semi,semi-entry（カンマ区切り）"
    )
    return ap

def filter_by_race_name(df: pd.DataFrame, stage_filter: str) -> pd.DataFrame:
    """race_name に含まれる文言で絞り込み。stage_filter='finals,semi,semi-entry' など。"""
    if not stage_filter:
        return df
    stages = {s.strip().lower() for s in stage_filter.split(",") if s.strip()}
    pats = []
    if "finals" in stages:
        pats.append(r"優勝戦")
    if "semi" in stages:
        pats.append(r"準優勝戦")
    if "semi-entry" in stages or "semi_entry" in stages:
        pats.append(r"準優進出戦|準優進出")
    if not pats:
        return df
    pat = "(" + "|".join(pats) + ")"
    # race_name が無いケースも想定して安全に
    rn = df.get("race_name")
    if rn is None:
        print("[stage-filter] race_name 列が無いためフィルタをスキップします")
        return df
    mask = rn.fillna("").astype(str).str.contains(pat, regex=True)
    df2 = df.loc[mask].copy()
    print(f"[stage-filter] '{stage_filter}' → {df2.shape[0]}/{df.shape[0]} rows kept")
    return df2


# ========================= メイン =========================

def main():
    args = build_parser().parse_args()
    in_path    = Path(args.in_path)
    out_dir    = Path(args.out_dir)
    model_dir  = Path(args.model_dir)
    max_card   = args.max_cat_card

    out_dir.mkdir(parents=True, exist_ok=True)
    model_dir.mkdir(parents=True, exist_ok=True)

    print("[INFO] in_path   :", in_path.resolve())
    print("[INFO] out_dir   :", out_dir.resolve())
    print("[INFO] model_dir :", model_dir.resolve())

    if not in_path.exists():
        raise SystemExit(f"[ERROR] not found: {in_path}")

    # 1) 読み込み & 時系列整列
    df = pd.read_csv(in_path, encoding="utf-8-sig", parse_dates=["date"])
    print("[INFO] master_sectional shape:", df.shape)

    # race 単位の代表日付で並べ替え
    race_date = df.groupby("race_id")["date"].min()
    meta_cols = [c for c in ["code","R"] if c in df.columns]
    meta = df.groupby("race_id")[meta_cols].min() if meta_cols else pd.DataFrame(index=race_date.index)
    race_order = (race_date.to_frame("race_date")
                 .join(meta)
                 .sort_values(["race_date"] + meta_cols, na_position="last")
                 .index.to_numpy())

    df_sorted = (df.set_index("race_id").loc[race_order].reset_index()
                   .sort_values(["date","race_id","wakuban"])
                   .reset_index(drop=True))

    # ★ ここで学習対象ステージを絞る（推論には影響しない）
    df_sorted = filter_by_race_name(df_sorted, args.stage_filter)
    if df_sorted.empty:
        raise SystemExit("[ERROR] stage-filter の結果が空です。条件を見直してください。")

    # 2) y / ids / used
    if TARGET not in df_sorted.columns:
        raise SystemExit(f"[ERROR] TARGET '{TARGET}' not found")
    y   = df_sorted[TARGET].astype(int).copy()

    ids_keep = [c for c in ["race_id","player","player_id","motor_number","boat_number","section_id"] if c in df_sorted.columns]
    ids = df_sorted[ids_keep].astype(str, errors="ignore").copy() if ids_keep else pd.DataFrame()

    used = df_sorted.drop(columns=[c for c in (ID_COLS + LEAK_COLS + [TARGET]) if c in df_sorted.columns],
                          errors="ignore").copy()

    # 3) 長期/固定の明示ドロップ（ただし KEEP_FORCE は残す）
    drop_long = [c for c in used.columns if (c in EXPLICIT_DROP) and (c not in KEEP_FORCE)]
    if drop_long:
        used = used.drop(columns=drop_long, errors="ignore")
        print(f"[info] dropped(EXPLICIT): {len(drop_long)} -> {drop_long[:10]}")

    # 4) 重要列の型整備（pred_mark は数値特徴として採用）
    def _numify(col, lo=None, hi=None, fill=0.0):
        if col in used.columns:
            used[col] = pd.to_numeric(used[col], errors="coerce").fillna(fill)
            if lo is not None or hi is not None:
                used[col] = used[col].clip(lower=lo, upper=hi)

    for c in ["pred_mark","Tilt","time_tenji","code","R","wakuban","age","weight","ST_previous_time"]:
        _numify(c, fill=0.0)
    # 欠損・外れ値の軽い保険（必要に応じて調整）
    used["pred_mark"] = used.get("pred_mark", 0).clip(-10, 10)

    # 5) 簡易チェック
    assert "wakuban" in used.columns, "wakuban がありません"
    assert used["wakuban"].between(1, 6).all(), "wakuban に 1–6 以外があります"

    # 6) 数値 / カテゴリ の自動選抜
    NUM_COLS = [c for c in used.columns if is_numeric_dtype(used[c])]

    obj_cols        = used.select_dtypes(include="object").columns.tolist()
    safe_present    = [c for c in SAFE_CAT if c in obj_cols]
    auto_candidates = [c for c in obj_cols if c not in safe_present]
    auto_card       = used[auto_candidates].nunique(dropna=True).sort_values(ascending=False) if auto_candidates else pd.Series(dtype=int)
    auto_add        = auto_card[auto_card <= max_card].index.tolist()
    CAT_COLS        = sorted(set(safe_present + auto_add))

    print(f"[cols] NUM={len(NUM_COLS)}  CAT={len(CAT_COLS)}  (max_cat_card={max_card})")
    if CAT_COLS:
        cat_card = used[CAT_COLS].nunique(dropna=True).sort_values(ascending=False)
        print("[CAT cardinality]\n", cat_card.head(20).to_string())

    # ---- ちょいテスト（確認したら削除OK）----
    print("pred_mark in NUM_COLS?", "pred_mark" in NUM_COLS)
    print("pred_mark in CAT_COLS?", "pred_mark" in CAT_COLS)
    # ----------------------------------------

    # 7) 前処理パイプライン
    num_tf = Pipeline(steps=[("scaler", StandardScaler())])
    try:
        cat_tf = Pipeline(steps=[("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=True))])
    except TypeError:
        cat_tf = Pipeline(steps=[("ohe", OneHotEncoder(handle_unknown="ignore", sparse=True))])

    preprocessor = ColumnTransformer(
        transformers=[("num", num_tf, NUM_COLS), ("cat", cat_tf, CAT_COLS)],
        remainder="drop",
    )

    # 8) 変換・保存
    X = preprocessor.fit_transform(used)
    print("[INFO] X type/shape :", type(X), getattr(X, "shape", None))
    print("[INFO] y balance    :", y.value_counts().to_dict())

    # 保存
    if sparse.issparse(X):
        save_npz(out_dir / "X.npz", X)
        x_path = out_dir / "X.npz"
    else:
        np.savez_compressed(out_dir / "X_dense.npz", X=X)
        x_path = out_dir / "X_dense.npz"

    y.to_frame(TARGET).to_csv(out_dir / "y.csv", index=False, encoding="utf-8-sig")
    if not ids.empty:
        ids.to_csv(out_dir / "ids.csv", index=False, encoding="utf-8-sig")

    joblib.dump(preprocessor, model_dir / "feature_pipeline.pkl")

    print("[OK] 保存が完了しました")
    print(f" - X:        {x_path}")
    print(f" - y:        {out_dir / 'y.csv'}")
    if not ids.empty:
        print(f" - ids:      {out_dir / 'ids.csv'}")
    print(f" - pipeline: {model_dir / 'feature_pipeline.pkl'}")


if __name__ == "__main__":
    main()
