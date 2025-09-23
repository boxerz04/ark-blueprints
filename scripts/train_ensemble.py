# scripts/train_ensemble.py
from __future__ import annotations
import argparse, json, os, shutil, time
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, log_loss
from joblib import dump
from pathlib import Path
from src.ensemble.meta_features import build_meta_features

def read_oof(path: str, p_col: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # 必須チェック
    for c in ["race_id", "player_id", "y", p_col]:
        if c not in df.columns:
            raise ValueError(f"{path} に {c} が必要です")
    # 型とソート（念のため）
    df["y"] = df["y"].astype(int)
    return df[["race_id","player_id","y",p_col,"stage","race_attribute"] if "stage" in df.columns or "race_attribute" in df.columns else ["race_id","player_id","y",p_col]]

def evaluate(y_true: np.ndarray, p: np.ndarray) -> dict:
    out = {}
    try:
        out["auc"] = float(roc_auc_score(y_true, p))
    except Exception:
        out["auc"] = None
    try:
        # epsilon to avoid -inf
        eps = 1e-7
        p_clip = np.clip(p, eps, 1 - eps)
        out["logloss"] = float(log_loss(y_true, p_clip))
    except Exception:
        out["logloss"] = None
    return out

def main():
    ap = argparse.ArgumentParser(description="Train meta model (stacking) from base/sectional OOF predictions")
    ap.add_argument("--base-oof", required=True, help="CSV with columns: race_id,player_id,y,p_base[,stage,race_attribute]")
    ap.add_argument("--sectional-oof", required=True, help="CSV with columns: race_id,player_id,y,p_sectional[,stage,race_attribute]")
    ap.add_argument("--outdir", default="models/ensemble/runs", help="Output base directory for runs")
    ap.add_argument("--tag", default=None, help="Optional run tag")
    ap.add_argument("--update-latest", action="store_true", help="Copy artifacts to models/ensemble/latest")
    args = ap.parse_args()

    base = read_oof(args.base_oof, "p_base")
    sec  = read_oof(args.sectional_oof, "p_sectional")

    # 外部結合 → base基準でマージ（player_id/race_idキー）
    df = pd.merge(base, sec[["race_id","player_id","p_sectional"] + ([c for c in ["stage","race_attribute"] if c in sec.columns])], 
                  on=["race_id","player_id"], how="left")
    # stage/race_attribute が base側にあれば保持
    for c in ["stage","race_attribute"]:
        if c in base.columns and c not in df.columns:
            df[c] = base[c]

    # メタ特徴
    X, used_cols = build_meta_features(df)
    y = df["y"].values.astype(int)

    # 学習（軽量・堅牢）
    clf = LogisticRegression(max_iter=1000, solver="lbfgs")
    clf.fit(X, y)
    p_oof = clf.predict_proba(X)[:,1]
    metrics = evaluate(y, p_oof)

    # 産物保存
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_tag = args.tag or ts
    run_dir = Path(args.outdir) / run_tag
    run_dir.mkdir(parents=True, exist_ok=True)

    dump(clf, run_dir / "meta_model.pkl")
    with open(run_dir / "meta_features.json", "w", encoding="utf-8") as f:
        json.dump({"used_cols": used_cols}, f, ensure_ascii=False, indent=2)
    with open(run_dir / "train_meta.json", "w", encoding="utf-8") as f:
        json.dump({
            "created_at": ts,
            "base_oof": os.path.abspath(args.base_oof),
            "sectional_oof": os.path.abspath(args.sectional_oof),
            "samples": int(len(df)),
            "metrics_oof": metrics,
            "notes": "stacking logistic regression on OOF",
        }, f, ensure_ascii=False, indent=2)

    # OOFの予測も残すと比較しやすい
    out_oof = df[["race_id","player_id","y"]].copy()
    out_oof["p_meta_oof"] = p_oof
    out_oof["p_base"] = X["p_base"].values
    out_oof["p_sectional"] = df["p_sectional"].fillna(np.nan).values
    out_oof.to_csv(run_dir / "oof_predictions.csv", index=False)

    print(f"[OK] saved run to: {run_dir}")

    if args.update_latest:
        latest = Path("models/ensemble/latest")
        if latest.exists():
            shutil.rmtree(latest)
        shutil.copytree(run_dir, latest)
        print(f"[OK] updated {latest}")

if __name__ == "__main__":
    main()

