# scripts/train.py
# --------------------------------------------
# 入力（features_* の成果物）:
#   - data/processed/{approach}/X.npz  (or X_dense.npz)
#   - data/processed/{approach}/y.csv  （列名 'y' / 'is_top2' どちらでもOK）
#   - data/processed/{approach}/ids.csv
#   - models/{approach}/latest/feature_pipeline.pkl
#
# 出力:
#   - models/{approach}/runs/<model_id>/{model.pkl, feature_pipeline.pkl, train_meta.json}
#   - models/{approach}/latest/{model.pkl, feature_pipeline.pkl, train_meta.json}
# --------------------------------------------

from __future__ import annotations
import argparse
import hashlib
import json
import subprocess
import sys

from datetime import datetime
from pathlib import Path

# プロジェクトルートを sys.path に追加
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from scipy.sparse import issparse, load_npz
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    log_loss,
    matthews_corrcoef,
    roc_auc_score,
)

from src.model_utils import gen_model_id, save_artifacts


# ---------- プロジェクトルート自動検出 ----------
def find_project_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "data").exists() and (p / "models").exists():
            return p
    return start.parent


# ---------- ユーティリティ ----------
def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_X(DATA_DIR: Path):
    """X.npz（疎） or X_dense.npz（密）を読み込む。"""
    xnpz = DATA_DIR / "X.npz"
    if xnpz.exists():
        return load_npz(xnpz)
    xdens = DATA_DIR / "X_dense.npz"
    if xdens.exists():
        arr = np.load(xdens)
        return arr["X"]
    raise FileNotFoundError(f"{DATA_DIR} に X.npz も X_dense.npz も見つかりません")


def load_y(y_path: Path) -> np.ndarray:
    """
    y.csv を読み込む。列名は 'y' / 'is_top2' / 先頭列のいずれでもOKにする。
    """
    dfy = pd.read_csv(y_path)
    if "y" in dfy.columns:
        col = "y"
    elif "is_top2" in dfy.columns:
        col = "is_top2"
    else:
        col = dfy.columns[0]
    return dfy[col].to_numpy(dtype=int)


def assert_feature_dim_matches(pipeline, X) -> None:
    try:
        feat_names = pipeline.get_feature_names_out()
        expected = int(len(feat_names))
    except Exception:
        expected = None
    n_cols = int(X.shape[1])
    if expected is not None and expected != n_cols:
        raise RuntimeError(f"特徴量次元の不一致: pipeline={expected} と X={n_cols}")


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


def _try_yaml():
    try:
        import yaml  # type: ignore

        return yaml
    except Exception:
        return None


def load_lgbm_params_yaml(params_yaml_path: Path) -> dict:
    y = _try_yaml()
    if y is None:
        raise RuntimeError(
            "PyYAML が見つかりません。--lgbm-params-yaml を使う場合は PyYAML をインストールしてください。"
        )
    with open(params_yaml_path, "r", encoding="utf-8") as f:
        data = y.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"LGBM params YAML must be a mapping: {params_yaml_path}")
    params = data.get("lgbm_params", data)
    if not isinstance(params, dict):
        raise ValueError(f"lgbm_params must be a mapping: {params_yaml_path}")
    return params


def time_split_indices(ids_df: pd.DataFrame, ratio: float = 0.8):
    """race_id の時間順（登場順）でホールドアウト分割。"""
    rid_order = ids_df["race_id"].astype(str).to_numpy()
    seen, uniq = set(), []
    for r in rid_order:
        if r not in seen:
            seen.add(r)
            uniq.append(r)
    cut = int(len(uniq) * ratio)
    train_rids = set(uniq[:cut])
    valid_rids = set(uniq[cut:])
    m_tr = np.array([r in train_rids for r in rid_order])
    m_va = ~m_tr
    return np.where(m_tr)[0], np.where(m_va)[0], rid_order[m_va]


def topk_hit_per_race(proba_va, y_va, rid_va, k=2) -> float:
    hits = []
    for rid in np.unique(rid_va):
        m = (rid_va == rid)
        if m.sum() < k:
            continue
        order = np.argsort(proba_va[m])[::-1]
        topk = y_va[m][order[:k]]
        hits.append(int(topk.sum() > 0))
    return float(np.mean(hits)) if hits else float("nan")


# ---------- メイン ----------
def main(args):
    approach = args.approach  # base / sectional / 他

    # プロジェクトルート
    PR = Path(args.project_root).resolve() if args.project_root else find_project_root(Path(__file__).resolve())
    print("[INFO] PROJECT_ROOT:", PR)

    # 参照パス（モデル別ディレクトリ）
    DATA_DIR = PR / "data" / "processed" / approach
    PIPE_SRC = PR / "models" / approach / "latest" / "feature_pipeline.pkl"

    # 入力チェック
    y_path = DATA_DIR / "y.csv"
    ids_path = DATA_DIR / "ids.csv"
    if not y_path.exists() or not ids_path.exists() or not PIPE_SRC.exists():
        raise FileNotFoundError(f"入力ファイルが不足しています: {DATA_DIR} / {PIPE_SRC}")

    # 読み込み
    X = load_X(DATA_DIR)
    y = load_y(y_path)
    ids = pd.read_csv(ids_path, dtype=str)
    pipeline = joblib.load(PIPE_SRC)

    if y.shape[0] != X.shape[0] or len(ids) != X.shape[0]:
        raise RuntimeError("X, y, ids の行数が不一致です")

    assert_feature_dim_matches(pipeline, X)

    lgbm_params = {
        "n_estimators": args.n_estimators,
        "learning_rate": args.learning_rate,
        "num_leaves": args.num_leaves,
        "subsample": args.subsample,
        "colsample_bytree": args.colsample_bytree,
        "random_state": args.random_state,
        "n_jobs": args.n_jobs,
    }
    lgbm_params_path = ""
    lgbm_params_hash = ""
    if args.lgbm_params_yaml:
        lgbm_params_file = Path(args.lgbm_params_yaml)
        if not lgbm_params_file.is_absolute():
            lgbm_params_file = PR / lgbm_params_file
        lgbm_params_file = lgbm_params_file.resolve()
        if not lgbm_params_file.exists():
            raise FileNotFoundError(f"LGBM params YAML not found: {lgbm_params_file}")
        external_params = load_lgbm_params_yaml(lgbm_params_file)
        lgbm_params.update(external_params)
        lgbm_params_path = str(lgbm_params_file)
        lgbm_params_hash = file_sha256(lgbm_params_file)
        print(f"[INFO] merged lgbm params from: {lgbm_params_file}")

    # ホールドアウト評価
    tr_idx, va_idx, rid_va = time_split_indices(ids, ratio=0.8)
    eval_clf = LGBMClassifier(**lgbm_params)
    eval_clf.fit(X[tr_idx], y[tr_idx])
    proba_va = eval_clf.predict_proba(X[va_idx])[:, 1]
    pred_va = (proba_va >= 0.5).astype(int)

    metrics_eval = {
        "auc": float(roc_auc_score(y[va_idx], proba_va)),
        "pr_auc": float(average_precision_score(y[va_idx], proba_va)),
        "logloss": float(log_loss(y[va_idx], proba_va, labels=[0, 1])),
        "accuracy": float(accuracy_score(y[va_idx], pred_va)),
        "mcc": float(matthews_corrcoef(y[va_idx], pred_va)),
        "top2_hit": topk_hit_per_race(proba_va, y[va_idx], rid_va, k=2),
    }
    print("[EVAL]", metrics_eval)

    # 全データで再学習
    clf = LGBMClassifier(**lgbm_params)
    clf.fit(X, y)

    # 保存
    model_id = gen_model_id()
    meta = {
        "model_id": model_id,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version_tag": args.version_tag,
        "notes": args.notes,
        "git_commit": get_git_commit(PR),
        "lgbm_params_yaml": lgbm_params_path,
        "lgbm_params_yaml_sha256": lgbm_params_hash,
        "lgbm_params": lgbm_params,
        "n_rows": int(X.shape[0]),
        "n_features": int(X.shape[1]),
        "sparse": bool(issparse(X)),
        "eval": metrics_eval,
    }
    artifacts = {
        "model.pkl": clf,
        "feature_pipeline.pkl": pipeline,
        "train_meta.json": meta,
    }
    save_artifacts(approach, model_id, artifacts)
    print(f"[OK] saved {approach} model artifacts: {model_id}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--approach", type=str, default="base", help="モデル名（base / sectional など）")
    ap.add_argument("--n-estimators", type=int, default=400)
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--num-leaves", type=int, default=63)
    ap.add_argument("--subsample", type=float, default=0.8)
    ap.add_argument("--colsample-bytree", type=float, default=0.8)
    ap.add_argument("--random-state", type=int, default=42)
    ap.add_argument("--n-jobs", type=int, default=-1)
    ap.add_argument("--version-tag", type=str, default="", help="モデルのバージョンタグ")
    ap.add_argument("--notes", type=str, default="", help="任意の説明文")
    ap.add_argument("--project-root", type=str, default="", help="リポジトリルート（未指定なら自動検出）")
    ap.add_argument("--lgbm-params-yaml", type=str, default="", help="LGBM パラメータ上書き用 YAML")
    args = ap.parse_args()
    main(args)
