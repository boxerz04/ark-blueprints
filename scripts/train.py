# train.py
# --------------------------------------------
# 入力（features.ipynb / build_feature_pipeline.py の成果物）:
#   - data/processed/X.npz  (or X_dense.npz)
#   - data/processed/y.csv
#   - data/processed/ids.csv
#   - models/latest/feature_pipeline.pkl
#
# 出力:
#   - models/runs/<model_id>/
#       ├─ model.pkl
#       ├─ feature_pipeline.pkl   (latest からコピーして固定)
#       └─ train_meta.json        (評価指標・環境情報・バージョンタグ等を含む)
#   - models/latest/              （最新版のペアで上書き）
#       ├─ model.pkl
#       ├─ feature_pipeline.pkl
#       └─ train_meta.json
#
# 評価:
#   - 時系列 80/20 ホールドアウトで一時モデルを学習し、
#     AUC / PR-AUC(AP) / LogLoss / Accuracy / MCC / Top-2Hit を算出
#   - 評価は保存用とは別の一時モデルで実施（本番モデルは全データ学習）
# --------------------------------------------

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path

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

# ---------- プロジェクトルート自動検出 ----------

def find_project_root(start: Path) -> Path:
    """
    現在ファイル位置から親方向に辿り、`data` と `models` を含むディレクトリをプロジェクトルートとみなす。
    見つからない場合は start.parents[1]（従来の scripts/ 前提）か、最後は start.parent を返す保険つき。
    """
    cur = start
    for p in [cur] + list(cur.parents):
        if (p / "data").exists() and (p / "models").exists():
            return p
    # 従来: scripts/train.py 前提（scripts の1つ上をルートとみなす）
    if len(start.parents) >= 2:
        return start.parents[1]
    return start.parent

# ---------- ユーティリティ ----------

def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_X(DATA_DIR: Path):
    xnpz = DATA_DIR / "X.npz"
    if xnpz.exists():
        return load_npz(xnpz)
    xdens = DATA_DIR / "X_dense.npz"
    if xdens.exists():
        arr = np.load(xdens)
        return arr["X"]
    raise FileNotFoundError("X.npz も X_dense.npz も見つかりません（features.ipynb の出力を確認）")

def assert_feature_dim_matches(pipeline, X) -> None:
    """
    前処理器の出力次元（= get_feature_names_out の長さ）と X の列数の一致をチェック。
    """
    try:
        feat_names = pipeline.get_feature_names_out()
        expected = int(len(feat_names))
    except Exception:
        expected = None  # 未対応実装のパイプラインの場合はスキップ
    n_cols = int(X.shape[1])
    if expected is not None and expected != n_cols:
        raise RuntimeError(
            f"特徴量次元の不一致: pipeline={expected} と X={n_cols} が一致しません。"
            " build_feature_pipeline.py と features.ipynb の出力整合を確認してください。"
        )

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

def time_split_indices(ids_df: pd.DataFrame, ratio: float = 0.8):
    """
    race_id の出現順を保持したまま時系列 80/20 分割。
    戻り値: (train_idx, valid_idx, rid_valid_order)
    """
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

def topk_hit_per_race(proba_va: np.ndarray, y_va: np.ndarray, rid_va: np.ndarray, k: int = 2) -> float:
    """
    各レースで確率Top-kに少なくとも1艇の正解(=1)が入っている割合。
    """
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
    # 1) プロジェクトルートを決定（--project-root が無ければ自動検出）
    if args.project_root:
        PR = Path(args.project_root).resolve()
    else:
        PR = find_project_root(Path(__file__).resolve())
    print("[INFO] PROJECT_ROOT:", PR)

    DATA_DIR   = PR / "data" / "processed"
    MODELS_DIR = PR / "models"
    LATEST_DIR = MODELS_DIR / "latest"
    RUNS_DIR   = MODELS_DIR / "runs"

    # ディレクトリ用意
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)

    # 入力チェック
    y_path   = DATA_DIR / "y.csv"
    ids_path = DATA_DIR / "ids.csv"
    pipe_src = LATEST_DIR / "feature_pipeline.pkl"
    if not y_path.exists():
        raise FileNotFoundError(f"{y_path} が見つかりません（features.ipynb の出力を確認）")
    if not ids_path.exists():
        raise FileNotFoundError(f"{ids_path} が見つかりません（features.ipynb の出力を確認）")
    if not pipe_src.exists():
        raise FileNotFoundError(f"{pipe_src} が見つかりません（build_feature_pipeline.py の実行を確認）")

    # データ読み込み
    print("[INFO] loading X / y / ids ...")
    X = load_X(DATA_DIR)
    y = pd.read_csv(y_path)["is_top2"].to_numpy(dtype=int)
    ids = pd.read_csv(ids_path, dtype=str)

    if y.shape[0] != X.shape[0] or len(ids) != X.shape[0]:
        raise RuntimeError(f"行数不一致: X={X.shape[0]} / y={y.shape[0]} / ids={len(ids)} の整合を確認してください")

    # 前処理器読み込み（latest）
    print("[INFO] loading latest feature_pipeline.pkl ...")
    pipeline = joblib.load(pipe_src)

    # 特徴量次元一致チェック
    assert_feature_dim_matches(pipeline, X)

    # ===== 評価（HO）: 一時モデルで Train/Valid 分割評価（保存はしない） =====
    print("[INFO] evaluating with time-based holdout ...")
    tr_idx, va_idx, rid_va = time_split_indices(ids, ratio=0.8)

    eval_clf = LGBMClassifier(
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        random_state=args.random_state,
        n_jobs=args.n_jobs,
    )
    Xtr = X[tr_idx]
    Xva = X[va_idx]
    ytr = y[tr_idx]
    yva = y[va_idx]

    eval_clf.fit(Xtr, ytr)
    proba_va = eval_clf.predict_proba(Xva)[:, 1]
    pred_va  = (proba_va >= 0.5).astype(int)

    metrics_eval = {
        "n_train_rows": int(len(tr_idx)),
        "n_valid_rows": int(len(va_idx)),
        "auc": float(roc_auc_score(yva, proba_va)),
        "pr_auc": float(average_precision_score(yva, proba_va)),  # AP をPR-AUCの代表として記録
        "logloss": float(log_loss(yva, proba_va, labels=[0, 1])),
        "accuracy": float(accuracy_score(yva, pred_va)),
        "mcc": float(matthews_corrcoef(yva, pred_va)),
        "top2_hit": topk_hit_per_race(proba_va, yva, rid_va, k=2),
    }
    print("[EVAL] ", metrics_eval)

    # ===== 本番モデルは「全データ」で学習 → 保存 =====
    print("[INFO] training LightGBM on FULL data ...")
    clf = LGBMClassifier(
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        random_state=args.random_state,
        n_jobs=args.n_jobs,
    )
    clf.fit(X, y)

    # model_id フォルダ生成
    now = datetime.now()
    model_id = now.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
    THIS_DIR = RUNS_DIR / model_id
    THIS_DIR.mkdir(parents=True, exist_ok=True)

    # 保存: model.pkl
    model_path = THIS_DIR / "model.pkl"
    joblib.dump(clf, model_path)

    # 保存: feature_pipeline.pkl（latest からコピーして固定）
    pipe_dst = THIS_DIR / "feature_pipeline.pkl"
    shutil.copy2(pipe_src, pipe_dst)

    # メタ情報作成
    meta = {
        "model_id": model_id,
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "project_root": str(PR),
        "version_tag": args.version_tag,
        "notes": args.notes,
        "git_commit": get_git_commit(PR),
        "data": {
            "X_source": "data/processed/X(.npz or X_dense.npz)",
            "y_source": str(y_path),
            "ids_source": str(ids_path),
            "n_rows": int(X.shape[0]),
            "n_features": int(X.shape[1]),
            "sparse": bool(issparse(X)),
        },
        "pipeline": {
            "path": "feature_pipeline.pkl",
            "latest_src": str(pipe_src),
            "sha256": file_sha256(pipe_dst),
        },
        "model": {
            "path": "model.pkl",
            "framework": "LightGBM",
            "params": {
                "n_estimators": args.n_estimators,
                "learning_rate": args.learning_rate,
                "num_leaves": args.num_leaves,
                "subsample": args.subsample,
                "colsample_bytree": args.colsample_bytree,
                "random_state": args.random_state,
                "n_jobs": args.n_jobs,
            },
        },
        "env": {
            "python": sys.version,
            "lightgbm": __import__("lightgbm").__version__,
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "scipy": __import__("scipy").__version__,
        },
        "eval": metrics_eval,
    }

    # 保存: train_meta.json
    with open(THIS_DIR / "train_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved run artifacts: {THIS_DIR}")

    # latest を同じペアで更新（上書き）
    print("[INFO] updating models/latest/ ...")
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(model_path, LATEST_DIR / "model.pkl")
    shutil.copy2(pipe_dst,   LATEST_DIR / "feature_pipeline.pkl")
    with open(LATEST_DIR / "train_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[OK] latest updated: {LATEST_DIR}")
    print(f"[OK] model_id: {model_id}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    # 学習ハイパラ
    ap.add_argument("--n-estimators", type=int, default=400)
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--num-leaves", type=int, default=63)
    ap.add_argument("--subsample", type=float, default=0.8)
    ap.add_argument("--colsample-bytree", type=float, default=0.8)
    ap.add_argument("--random-state", type=int, default=42)
    ap.add_argument("--n-jobs", type=int, default=-1)
    # バージョン管理メタ
    ap.add_argument("--version-tag", type=str, default="", help="モデルのバージョンタグ（例: v1.0.0）")
    ap.add_argument("--notes", type=str, default="", help="任意の説明文（例: '初版モデル：基本特徴量セット'）")
    # 任意: 明示的にルートを指定したい場合のみ使う（未指定なら自動検出）
    ap.add_argument("--project-root", type=str, default="", help="リポジトリのルート。未指定ならスクリプト位置から自動検出")
    args = ap.parse_args()
    main(args)
