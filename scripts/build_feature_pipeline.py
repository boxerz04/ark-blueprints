# -*- coding: utf-8 -*-
"""
scripts/build_feature_pipeline.py (features.ipynb 準拠 + ST派生の自動補完)
- data/processed/master.csv を読み込み（学習時と同じ入力）
- 仕様:
  * used = df.drop(ID_COLS + LEAK_COLS + [TARGET])
  * used に ensure_st_features を適用（ST数値化＋ST_tenji_rank 生成（無い時のみ））
  * 数値 = すべて → StandardScaler()
  * カテゴリ = SAFE_CAT + 低カーディナリティ(<=50)自動追加 → OneHotEncoder(handle_unknown='ignore', sparse_output=True or sparse=True)
  * 明示ドロップ = DROP_FEATS
  * remainder='drop'
  * パイプライン先頭に ensure_st_features を FunctionTransformer で入れて保存
"""

import argparse
from pathlib import Path
import sys
import json
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_object_dtype, is_string_dtype
import joblib

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer
from sklearn.compose import ColumnTransformer

ROOT = Path(__file__).resolve().parents[1]

# scripts/ark_features.py を import できるようにパス追加
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
import ark_features  # noqa: E402

TARGET = "is_top2"
ID_COLS   = ["race_id", "player", "player_id", "motor_number", "boat_number", "section_id"]
LEAK_COLS = ["entry", "is_wakunari", "rank", "winning_trick", "remarks",
             "henkan_ticket", "ST", "ST_rank", "__source_file"]

SAFE_CAT = [
    "AB_class","place","weather","wind_direction","sex",
    "race_grade","race_type","race_attribute",
]
MAX_CAT_CARD = 50

DROP_FEATS = [
    "origin","team","parts_exchange","title","schedule","timetable",
    "precondition_1","precondition_2","propeller",
]

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed-dir", default=str(ROOT / "data" / "processed"))
    # ★ 出力デフォルトを models/base/latest に
    ap.add_argument("--out", default=str(ROOT / "models" / "base" / "latest" / "feature_pipeline.pkl"))
    return ap.parse_args()

def build_pipeline_from_master(master_csv: Path) -> Pipeline:
    df = pd.read_csv(master_csv, encoding="utf-8-sig", parse_dates=["date"])
    used = df.drop(columns=ID_COLS + LEAK_COLS + [TARGET], errors="ignore").copy()

    # ST 数値化 ＋ ST_tenji_rank 生成（無い場合のみ）— idempotent
    used = ark_features.ensure_st_features(used)

    # 数値列
    NUM_COLS = [c for c in used.columns if is_numeric_dtype(used[c])]

    # カテゴリ列（object/string）＋ SAFE 追加
    obj_cols        = [c for c in used.columns if is_object_dtype(used[c]) or is_string_dtype(used[c])]
    safe_present    = [c for c in SAFE_CAT if c in obj_cols]
    auto_candidates = [c for c in obj_cols if c not in safe_present]
    auto_add        = []
    if auto_candidates:
        card = used[auto_candidates].nunique(dropna=True).sort_values(ascending=False)
        auto_add = card[card <= MAX_CAT_CARD].index.tolist()
    CAT_COLS = sorted(set(safe_present + auto_add))

    # 明示ドロップを除外
    NUM_COLS = [c for c in NUM_COLS if c not in DROP_FEATS]
    CAT_COLS = [c for c in CAT_COLS if c not in DROP_FEATS]

    num_tf = Pipeline(steps=[("scaler", StandardScaler())])

    # ★ scikit-learn 1.2+ / 1.1- 互換
    try:
        cat_tf = Pipeline(steps=[
            ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=True))  # 1.2+
        ])
    except TypeError:
        cat_tf = Pipeline(steps=[
            ("ohe", OneHotEncoder(handle_unknown="ignore", sparse=True))         # <=1.1
        ])

    ct = ColumnTransformer(
        transformers=[
            ("num", num_tf, NUM_COLS),
            ("cat", cat_tf, CAT_COLS),
        ],
        remainder="drop"
    )

    pipe = Pipeline(steps=[
        ("st_features", FunctionTransformer(ark_features.ensure_st_features, validate=False)),
        ("ct", ct),
    ])

    # ログ
    print(f"[Pipeline] num={len(NUM_COLS)} cat={len(CAT_COLS)}")
    if CAT_COLS:
        cat_card = used[CAT_COLS].nunique(dropna=True).sort_values(ascending=False)
        print("\n[CAT_COLS cardinality]\n", cat_card.to_string())

    _ = pipe.fit(used)
    return pipe

def main():
    args = parse_args()
    processed_dir = Path(args.processed_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    master_csv = processed_dir / "master.csv"
    if not master_csv.exists():
        raise FileNotFoundError(f"master.csv not found: {master_csv}")

    print(f"[load] {master_csv}")
    pipe = build_pipeline_from_master(master_csv)
    joblib.dump(pipe, out_path)
    print(f"[OK] saved pipeline: {out_path}")

    # 可能なら特徴名も保存（整合チェック用のおまけ）
    try:
        feat_names = pipe.get_feature_names_out()
        feat_json = out_path.with_name("feature_names.json")
        with open(feat_json, "w", encoding="utf-8") as f:
            json.dump(list(map(str, feat_names)), f, ensure_ascii=False, indent=2)
        print(f"[OK] saved feature names: {feat_json}")
    except Exception:
        pass

if __name__ == "__main__":
    main()
