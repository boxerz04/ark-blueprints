# -*- coding: utf-8 -*-
"""
master.csv から base モデル学習用の特徴量・ターゲット・ID と
前処理パイプラインを生成するスクリプト。

出力（デフォルト）:
- data/processed/base/X.npz または X_dense.npz
- data/processed/base/y.csv      （列名: is_top2）
- data/processed/base/ids.csv    （race_id, player, player_id, motor_number, boat_number, section_id）
- models/base/latest/feature_pipeline.pkl

使い方例:
python scripts/preprocess_base_features.py ^
  --master data/processed/master.csv ^
  --out-dir data/processed/base ^
  --pipeline-dir models/base/latest

改修点:
- prior 列の取り込み（tenji / entry入着率 / 決まり手）を前提に数値列へ自動採用
- 相対化列（adv_*, lr_*）は NaN→0、それ以外の数値は NaN→中央値
- season_q をカテゴリ採用
- weather を不採用（ドロップ）
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from scipy import sparse
from scipy.sparse import save_npz
import joblib

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer


# ---------- プロジェクトルート自動検出 ----------
def find_project_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "data").exists() and (p / "models").exists():
            return p
    return start


# ---------- 引数 ----------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Preprocess features for BASE model")
    ap.add_argument("--master", default="data/processed/master.csv",
                    help="master.csv のパス（preprocess.py の出力）")
    ap.add_argument("--out-dir", default="data/processed/base",
                    help="特徴量/目的変数/ID の出力ディレクトリ")
    ap.add_argument("--pipeline-dir", default="models/base/latest",
                    help="feature_pipeline.pkl の出力先ディレクトリ")
    ap.add_argument("--target", default="is_top2", help="ターゲット列名（既定: is_top2）")
    return ap


# --- priorの相対化列を0埋め対象にするための接頭辞 ---
ADV_PREFIXES = ("adv_p", "lr_p", "adv_p_", "lr_p_")


# ---------- メイン ----------
def main():
    args = build_parser().parse_args()

    PR = find_project_root(Path.cwd())
    master_path   = (PR / args.master).resolve()
    out_dir       = (PR / args.out_dir).resolve()
    pipeline_dir  = (PR / args.pipeline_dir).resolve()

    out_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    print("[INFO] PROJECT_ROOT :", PR)
    print("[INFO] master       :", master_path)
    print("[INFO] out_dir      :", out_dir)
    print("[INFO] pipeline_dir :", pipeline_dir)

    if not master_path.exists():
        raise SystemExit(f"[ERROR] master が見つかりません: {master_path}")

    # --- 読み込み ---
    df = pd.read_csv(master_path, encoding="utf-8-sig", parse_dates=["date"])
    print("[INFO] master shape :", df.shape)

    TARGET   = args.target
    ID_COLS  = ["race_id", "player", "player_id", "motor_number", "boat_number", "section_id"]
    LEAK_COLS = [
        "entry", "is_wakunari", "rank", "winning_trick", "remarks",
        "henkan_ticket", "ST", "ST_rank", "__source_file",
        "finish1_flag_cur", "finish2_flag_cur", "finish3_flag_cur"
    ]

    # 必須チェック
    if TARGET not in df.columns:
        raise SystemExit(f"[ERROR] ターゲット列 {TARGET} が見つかりません（preprocess.py の設定を確認）")

    # y / ids / used（一次）
    y   = df[TARGET].astype(int)
    ids = df[ID_COLS].astype(str, errors="ignore") if set(ID_COLS).issubset(df.columns) else pd.DataFrame()

    used = df.drop(columns=[c for c in (ID_COLS + LEAK_COLS + [TARGET]) if c in df.columns]).copy()

    # 最小の整合性チェック
    assert "wakuban" in used.columns, "wakuban が見当たりません（preprocess.py の出力を確認）"
    assert used["wakuban"].between(1, 6).all(), "wakuban に 1–6 以外の値があります"
    assert "ST_tenji" in used.columns, "ST_tenji が見当たりません"

    # --- 時系列ソート（race_id → date） ---
    race_date = df.groupby("race_id")["date"].min()
    meta_cols = [c for c in ["code", "R"] if c in df.columns]
    meta = df.groupby("race_id")[meta_cols].min() if meta_cols else pd.DataFrame(index=race_date.index)

    race_order = (
        race_date.to_frame("race_date")
                 .join(meta)
                 .sort_values(["race_date"] + meta_cols, na_position="last")
                 .index.to_numpy()
    )

    df_sorted = (
        df.set_index("race_id")
          .loc[race_order]
          .reset_index()
          .sort_values(["date", "race_id", "wakuban"])
          .reset_index(drop=True)
    )

    # 並べ替え後に作り直し（重要）
    y   = df_sorted[TARGET].astype(int)
    ids = df_sorted[ID_COLS].astype(str, errors="ignore") if set(ID_COLS).issubset(df_sorted.columns) else pd.DataFrame()
    used = df_sorted.drop(columns=[c for c in (ID_COLS + LEAK_COLS + [TARGET]) if c in df_sorted.columns]).copy()

    # --- 列選択: 数値 / カテゴリ ---
    # 1) 数値は全部（prior列を含む）
    NUM_COLS: List[str] = [c for c in used.columns if is_numeric_dtype(used[c])]

    # 2) カテゴリ：season_q を採用、weather は不採用
    SAFE_CAT = [
        "AB_class", "wind_direction", "sex",   # weather は除外
        "race_grade", "race_type", "race_attribute",
        "season_q",  # ← 追加採用
    ]
    DROP_FEATS = [
        "origin","team","parts_exchange","title","schedule","timetable",
        "precondition_1","precondition_2","propeller",
        "weather", "place"  # ← 明示的に除外
    ]
    MAX_CAT_CARD = 50

    obj_cols = used.select_dtypes(include="object").columns.tolist()
    safe_present = [c for c in SAFE_CAT if c in obj_cols]
    auto_candidates = [c for c in obj_cols if c not in safe_present]
    if auto_candidates:
        auto_card = used[auto_candidates].nunique(dropna=True)
        auto_add  = auto_card[auto_card <= MAX_CAT_CARD].index.tolist()
    else:
        auto_add = []

    CAT_COLS = sorted(set(safe_present + auto_add))

    # 明示ドロップ
    NUM_COLS = [c for c in NUM_COLS if c not in DROP_FEATS]
    CAT_COLS = [c for c in CAT_COLS if c not in DROP_FEATS]

    print(f"[INFO] NUM_COLS ({len(NUM_COLS)}) -> head: {NUM_COLS[:10]}")
    print(f"[INFO] CAT_COLS ({len(CAT_COLS)}) -> head: {CAT_COLS[:10]}")

    # --- 数値列を 0埋め対象(adv/lr) と それ以外 に分割 ---
    adv_like = [c for c in NUM_COLS if any(c.startswith(p) for p in ADV_PREFIXES)]
    num_other = [c for c in NUM_COLS if c not in adv_like]
    print(f"[INFO] adv/lr zero-fill cols = {len(adv_like)}  (例: {adv_like[:6]})")

    # --- 前処理パイプライン（sklearn 1.2+ / 1.1- 両対応） ---
    num_zero_tf = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ("scaler", StandardScaler()),
    ])
    num_other_tf = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    try:
        cat_tf = Pipeline(steps=[("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=True))])
    except TypeError:
        cat_tf = Pipeline(steps=[("ohe", OneHotEncoder(handle_unknown="ignore", sparse=True))])

    transformers = []
    if adv_like:
        transformers.append(("num_zero", num_zero_tf, adv_like))
    if num_other:
        transformers.append(("num", num_other_tf, num_other))
    if CAT_COLS:
        transformers.append(("cat", cat_tf, CAT_COLS))

    preprocessor = ColumnTransformer(transformers=transformers, remainder="drop")
    print(f"[Pipeline] num_zero={len(adv_like)}  num={len(num_other)}  cat={len(CAT_COLS)}")

    # --- fit_transform & 保存 ---
    X = preprocessor.fit_transform(used)
    print("[INFO] X type/shape :", type(X), X.shape)
    print("[INFO] y balance    :", y.value_counts().to_dict())

    # 出力
    out_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    if sparse.issparse(X):
        save_npz(out_dir / "X.npz", X)
        x_path = out_dir / "X.npz"
    else:
        np.savez_compressed(out_dir / "X_dense.npz", X=X)
        x_path = out_dir / "X_dense.npz"

    # y/ids 保存
    y.to_frame("is_top2").to_csv(out_dir / "y.csv", index=False, encoding="utf-8-sig")
    if not ids.empty:
        ids.to_csv(out_dir / "ids.csv", index=False, encoding="utf-8-sig")

    # 前処理パイプライン
    joblib.dump(preprocessor, pipeline_dir / "feature_pipeline.pkl")

    print("[OK] 保存が完了しました")
    print(f" - X:        {x_path} (shape={X.shape}, sparse={sparse.issparse(X)})")
    print(f" - y:        {out_dir / 'y.csv'}")
    if not ids.empty:
        print(f" - ids:      {out_dir / 'ids.csv'}")
    print(f" - pipeline: {pipeline_dir / 'feature_pipeline.pkl'}")


if __name__ == "__main__":
    main()
