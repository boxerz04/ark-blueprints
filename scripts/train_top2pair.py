# -*- coding: utf-8 -*-
"""
scripts/train_top2pair.py (train.py準拠の評価出力)
- 入力（build_top2pair_dataset.py の成果物 / data/processed 固定）:
    - data/processed/X_top2pair.npz  または  X_top2pair_dense.npz
    - data/processed/y_top2pair.csv  （列名: y）
    - data/processed/ids_top2pair.csv
- 振る舞い:
    - StratifiedKFold による CV 評価（foldごとに AUC/PR-AUC/LogLoss/Acc/MCC を表示）
    - OOF（全体）でも AUC/PR-AUC/LogLoss を表示・保存
    - 全データで最終学習 → アーティファクト保存（runs/<id> と latest/）
- 出力:
    models/top2pair/runs/<model_id>/
        ├─ model.pkl
        ├─ train_meta.json
        ├─ feature_importance.csv
        └─ cv_folds.csv
    models/top2pair/latest/ にも同じ内容をコピー
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from scipy.sparse import load_npz
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    log_loss,
    accuracy_score,
    matthews_corrcoef,
)

# --- プロジェクトルートを sys.path に追加 ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.model_utils import gen_model_id, save_artifacts


# ---------- ユーティリティ ----------
def load_X(DATA_DIR: Path, prefix="X_top2pair"):
    """疎行列 or 密行列をロード"""
    xnpz = DATA_DIR / f"{prefix}.npz"
    if xnpz.exists():
        return load_npz(xnpz)
    xdens = DATA_DIR / f"{prefix}_dense.npz"
    if xdens.exists():
        arr = np.load(xdens)
        return arr["X"]
    raise FileNotFoundError(f"{prefix}.npz / {prefix}_dense.npz が見つかりません")


def get_git_commit(repo_root: Path) -> str:
    try:
        res = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=True,
        )
        return res.stdout.strip()
    except Exception:
        return ""


# ---------- メイン ----------
def main(args):
    approach = "top2pair"

    PR = PROJECT_ROOT
    DATA_DIR = PR / "data" / "processed"

    print("[INFO] PROJECT_ROOT:", PR)
    print("[INFO] loading dataset ...")

    # 入力データ
    X = load_X(DATA_DIR)
    y = pd.read_csv(DATA_DIR / "y_top2pair.csv").iloc[:, 0].to_numpy(dtype=int)  # 列名は y を想定
    ids = pd.read_csv(DATA_DIR / "ids_top2pair.csv", dtype=str)

    if y.shape[0] != X.shape[0] or len(ids) != X.shape[0]:
        raise RuntimeError(f"X, y, ids の行数が不一致: X={X.shape[0]} / y={y.shape[0]} / ids={len(ids)}")

    # クロスバリデーション
    print("[INFO] CV training ...")
    cv = StratifiedKFold(n_splits=args.cv, shuffle=True, random_state=args.random_state)
    oof_proba = np.zeros_like(y, dtype=float)
    metrics = []
    feature_importance = []

    for fold, (tr_idx, va_idx) in enumerate(cv.split(X, y), 1):
        clf = LGBMClassifier(
            n_estimators=args.n_estimators,
            learning_rate=args.learning_rate,
            num_leaves=args.num_leaves,
            subsample=args.subsample,
            colsample_bytree=args.colsample_bytree,
            random_state=args.random_state,
            n_jobs=args.n_jobs,
        )
        clf.fit(X[tr_idx], y[tr_idx])

        proba_va = clf.predict_proba(X[va_idx])[:, 1]
        oof_proba[va_idx] = proba_va
        pred_va = (proba_va >= 0.5).astype(int)

        # foldメトリクス（ラベルが単一になっても落ちないよう防御）
        try:
            ll = float(log_loss(y[va_idx], proba_va, labels=[0, 1]))
        except ValueError:
            ll = None

        fold_metrics = {
            "fold": fold,
            "auc": float(roc_auc_score(y[va_idx], proba_va)),
            "pr_auc": float(average_precision_score(y[va_idx], proba_va)),
            "logloss": ll,
            "accuracy": float(accuracy_score(y[va_idx], pred_va)),
            "mcc": float(matthews_corrcoef(y[va_idx], pred_va)),
        }
        metrics.append(fold_metrics)

        # ログ出力（train.py 風）
        ll_txt = f"{fold_metrics['logloss']:.4f}" if fold_metrics["logloss"] is not None else "nan"
        print(
            f"[Fold {fold}] AUC={fold_metrics['auc']:.4f} "
            f"PR-AUC={fold_metrics['pr_auc']:.4f} "
            f"LogLoss={ll_txt} "
            f"Acc={fold_metrics['accuracy']:.4f} "
            f"MCC={fold_metrics['mcc']:.4f}"
        )

        # 特徴量重要度
        fi = pd.DataFrame({
            "feature": clf.booster_.feature_name(),
            f"importance_fold{fold}": clf.booster_.feature_importance(importance_type="gain"),
        })
        feature_importance.append(fi)

    # OOF全体のスコア
    oof_metrics = {
        "auc": float(roc_auc_score(y, oof_proba)),
        "pr_auc": float(average_precision_score(y, oof_proba)),
        "logloss": float(log_loss(y, oof_proba, labels=[0, 1])),
    }
    print(f"[OOF] AUC={oof_metrics['auc']:.4f}  PR-AUC={oof_metrics['pr_auc']:.4f}  LogLoss={oof_metrics['logloss']:.4f}")

    # 全データで最終学習
    print("[INFO] training on FULL data ...")
    final_clf = LGBMClassifier(
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        random_state=args.random_state,
        n_jobs=args.n_jobs,
    )
    final_clf.fit(X, y)

    # 保存メタ
    model_id = gen_model_id()
    meta = {
        "model_id": model_id,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version_tag": args.version_tag,
        "notes": args.notes,
        "git_commit": get_git_commit(PR),
        "n_rows": int(X.shape[0]),
        "n_features": int(X.shape[1]),
        "cv": metrics,      # 各foldの辞書
        "oof": oof_metrics, # 全体OOF指標
    }

    # 副産物を保存（CSV）。列重複に注意して統合
    fi_all = pd.concat(feature_importance, axis=1)
    fi_all = fi_all.loc[:, ~fi_all.columns.duplicated()]
    cv_df = pd.DataFrame(metrics)

    # save_artifacts に渡す（ファイルパス or DataFrame/obj の混在OK設計ならこのまま）
    artifacts = {
        "model.pkl": final_clf,
        "train_meta.json": meta,
        "feature_importance.csv": fi_all,
        "cv_folds.csv": cv_df,
    }
    save_artifacts(approach, model_id, artifacts)

    print(f"[OK] saved {approach} model artifacts: {model_id}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cv", type=int, default=5)
    ap.add_argument("--n-estimators", type=int, default=400)
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--num-leaves", type=int, default=63)
    ap.add_argument("--subsample", type=float, default=0.8)
    ap.add_argument("--colsample-bytree", type=float, default=0.8)
    ap.add_argument("--random-state", type=int, default=42)
    ap.add_argument("--n-jobs", type=int, default=-1)
    ap.add_argument("--version-tag", type=str, default="", help="モデルのバージョンタグ")
    ap.add_argument("--notes", type=str, default="", help="任意の説明文")
    args = ap.parse_args()
    main(args)
