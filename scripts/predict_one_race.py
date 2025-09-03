# -*- coding: utf-8 -*-
"""
scripts/predict_one_race.py
- build_live_row.py が出力した live CSV（6行）を読み込み
- 保存済みの特徴量パイプライン（feature_pipeline.pkl）で transform
- 学習済みモデル（model.pkl）で推論し、CSV保存

※ 互換性シム:
  feature_pipeline.pkl を作成した際の FunctionTransformer が
  __main__.add_st_features を参照しているため、
  同一シグネチャの関数をこのスクリプト内に定義して pickle の参照を解決します。
"""

import argparse
import os
import sys
from pathlib import Path
import re
import numpy as np
import pandas as pd
import joblib

ROOT = Path(__file__).resolve().parents[1]

# ====== 互換性シム（build_feature_pipeline.py と一致させる） ======
def parse_st_value(x):
    if x is None:
        return np.nan
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return np.nan
    s = str(x).strip()
    if s == "" or s in {"-", "—", "–", "NaN", "nan"}:
        return np.nan
    m = re.match(r"^([FL])\.(\d+)$", s, flags=re.IGNORECASE)
    if m:
        sign = -1.0 if m.group(1).upper() == "F" else 1.0
        return sign * float("0." + m.group(2))
    if re.match(r"^\.\d+$", s):
        return float("0" + s)
    try:
        return float(s)
    except Exception:
        return np.nan

def add_st_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ST_tenji" in out.columns:
        out["ST_tenji"] = out["ST_tenji"].apply(parse_st_value).astype(float)
        rank = pd.to_numeric(out["ST_tenji"], errors="coerce").rank(method="min", ascending=True)
        out["ST_tenji_rank"] = rank.fillna(7).astype("Int64")
    return out
# ====== 互換性シムここまで ======

def parse_args():
    p = argparse.ArgumentParser(description="Predict one race using feature pipeline + model.")
    p.add_argument("--live-csv", required=True, help="build_live_row.py が出力した CSV（6行想定）")
    p.add_argument("--model-dir", default=str(ROOT / "models" / "latest"),
                   help="モデルと前処理器のディレクトリ（model.pkl / feature_pipeline.pkl を想定）")
    p.add_argument("--model", help="モデル pkl のパス（省略時は <model-dir>/model.pkl）")
    p.add_argument("--feature-pipeline", help="特徴量パイプライン pkl のパス（省略時は <model-dir>/feature_pipeline.pkl）")
    p.add_argument("--out", help="出力CSV（省略時は pred_*.csv を live-csv と同じ場所に作成）")
    p.add_argument("--id-cols", default="race_id,code,R,wakuban,player",
                   help="出力に含める識別系カラム（カンマ区切り）")
    return p.parse_args()

def infer_out_path(live_csv_path: Path) -> Path:
    name = live_csv_path.name
    if name.startswith("raw_"):
        return live_csv_path.with_name("pred_" + name[len("raw_"):])
    return live_csv_path.with_name("pred_" + name)

def load_pipeline(path: Path):
    if not path.exists():
        print(f"[ERROR] feature pipeline not found: {path}", file=sys.stderr)
        print("  -> build_feature_pipeline.py で feature_pipeline.pkl を作成してください。", file=sys.stderr)
        sys.exit(1)
    return joblib.load(path)

def load_model(path: Path):
    if not path.exists():
        print(f"[ERROR] model not found: {path}", file=sys.stderr)
        sys.exit(1)
    return joblib.load(path)

def main():
    args = parse_args()

    live_csv = Path(args.live_csv)
    if not live_csv.exists():
        print(f"[ERROR] live csv not found: {live_csv}", file=sys.stderr)
        sys.exit(1)

    model_dir = Path(args.model_dir)
    model_path = Path(args.model) if args.model else (model_dir / "model.pkl")
    pipe_path  = Path(args.feature_pipeline) if args.feature_pipeline else (model_dir / "feature_pipeline.pkl")

    out_path = Path(args.out) if args.out else infer_out_path(live_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 入力CSV
    df_live = pd.read_csv(live_csv, encoding="utf_8_sig")
    if len(df_live) != 6:
        print(f"[WARN] live csv rows != 6 (got {len(df_live)}). 続行します。", file=sys.stderr)

    # 前処理器 & モデル
    pipe = load_pipeline(pipe_path)
    model = load_model(model_path)

    # 前処理（パイプライン側に集約）
    try:
        X_live = pipe.transform(df_live)
    except Exception as e:
        print(f"[ERROR] feature pipeline transform failed: {type(e).__name__}: {e}", file=sys.stderr)
        print("  -> 学習時と同じ列名・dtype・前処理器かを確認してください。", file=sys.stderr)
        sys.exit(1)

    # 推論
    try:
        y_pred = model.predict(X_live)
    except Exception as e:
        print(f"[ERROR] model.predict failed: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

    # 確率が取れる分類器なら保存
    proba_df = None
    if hasattr(model, "predict_proba"):
        try:
            proba = model.predict_proba(X_live)
            if proba.ndim == 2 and proba.shape[1] == 2:
                proba_df = pd.DataFrame({"proba_1": proba[:, 1], "proba_0": proba[:, 0]})
            elif proba.ndim == 2:
                proba_df = pd.DataFrame(proba, columns=[f"proba_{i}" for i in range(proba.shape[1])])
        except Exception:
            proba_df = None

    # 出力テーブル
    id_cols = [c.strip() for c in args.id_cols.split(",") if c.strip() in df_live.columns]
    out = pd.DataFrame(index=df_live.index)
    for c in id_cols:
        out[c] = df_live[c]
    out["prediction"] = y_pred
    if proba_df is not None:
        out = pd.concat([out, proba_df], axis=1)

    # 念のため主要キーがなければ補完
    for k in ("race_id", "code", "R", "wakuban", "player"):
        if k not in out.columns and k in df_live.columns:
            out[k] = df_live[k]

    out.to_csv(out_path, index=False, encoding="utf_8_sig")
    print(f"[OK] saved: {out_path}  (rows={len(out)}, cols={len(out.columns)})")

    with pd.option_context("display.max_columns", None):
        print(out.to_string(index=False))

if __name__ == "__main__":
    main()
