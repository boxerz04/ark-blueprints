# scripts/tune_hyperparams.py
# ---------------------------------------------------------------------------
# ハイパーパラメータ探索（学習と同じ時系列ホールドアウト・同じ評価指標）
# 結果は models/<approach>/hpo_results.json に保存。確認後に train.py の引数や PS1 に反映可能。
#
# 探索指標（重要）:
#   - 既定は neg_log_loss（LogLoss 最小化）。確率の質を崩さず、PR-AUC・外さないAI に寄せる。
#   - 採用条件: LogLoss 最小 + PR-AUC が現行以上（上位10件をホールドアウト評価して選ぶ）。
#   - 当たり外れ軽減: 採用候補を seed 42/43/44 で再学習し、LogLoss/PR-AUC/Top2Hit の平均±標準偏差を保存。
# ---------------------------------------------------------------------------

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier
from scipy.sparse import load_npz
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    log_loss,
    matthews_corrcoef,
    roc_auc_score,
)
from sklearn.model_selection import PredefinedSplit, RandomizedSearchCV

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(PROJECT_ROOT))


def find_project_root(start: Path) -> Path:
    for p in [start] + list(start.parents):
        if (p / "data").exists() and (p / "models").exists():
            return p
    return start.parent


def load_X(data_dir: Path):
    xnpz = data_dir / "X.npz"
    if xnpz.exists():
        return load_npz(xnpz)
    xdens = data_dir / "X_dense.npz"
    if xdens.exists():
        arr = np.load(xdens)
        return arr["X"]
    raise FileNotFoundError(f"X not found under {data_dir}")


def load_y(y_path: Path) -> np.ndarray:
    dfy = pd.read_csv(y_path)
    if "y" in dfy.columns:
        col = "y"
    elif "is_top2" in dfy.columns:
        col = "is_top2"
    else:
        col = dfy.columns[0]
    return dfy[col].to_numpy(dtype=int)


def time_split_indices(ids_df: pd.DataFrame, ratio: float = 0.8):
    rid_order = ids_df["race_id"].astype(str).to_numpy()
    seen, uniq = set(), []
    for r in rid_order:
        if r not in seen:
            seen.add(r)
            uniq.append(r)
    cut = int(len(uniq) * ratio)
    train_rids = set(uniq[:cut])
    m_tr = np.array([r in train_rids for r in rid_order])
    m_va = ~m_tr
    return np.where(m_tr)[0], np.where(m_va)[0]


def topk_hit_per_race(proba_va, y_va, rid_va, k=2) -> float:
    hits = []
    for rid in np.unique(rid_va):
        m = rid_va == rid
        if m.sum() < k:
            continue
        order = np.argsort(proba_va[m])[::-1]
        topk = y_va[m][order[:k]]
        hits.append(int(topk.sum() > 0))
    return float(np.mean(hits)) if hits else float("nan")


def eval_holdout(clf, X_va, y_va, rid_va) -> dict:
    proba = clf.predict_proba(X_va)[:, 1]
    pred = (proba >= 0.5).astype(int)
    return {
        "logloss": float(log_loss(y_va, proba, labels=[0, 1])),
        "pr_auc": float(average_precision_score(y_va, proba)),
        "top2_hit": topk_hit_per_race(proba, y_va, rid_va, k=2),
        "auc": float(roc_auc_score(y_va, proba)),
        "accuracy": float(accuracy_score(y_va, pred)),
        "mcc": float(matthews_corrcoef(y_va, pred)),
    }


def main():
    ap = argparse.ArgumentParser(description="LightGBM ハイパーパラメータ探索（時系列ホールドアウト）")
    ap.add_argument("--approach", type=str, default="finals", help="approach (data/processed/<approach>)")
    ap.add_argument("--n-iter", type=int, default=80, help="RandomizedSearchCV の試行数")
    ap.add_argument(
        "--scoring",
        type=str,
        default="neg_log_loss",
        choices=("neg_log_loss", "roc_auc", "average_precision"),
        help="最適化する指標。既定は neg_log_loss。",
    )
    ap.add_argument("--out", type=str, default="", help="結果JSONの出力先")
    ap.add_argument("--project-root", type=str, default="", help="リポジトリルート")
    args = ap.parse_args()

    PR = Path(args.project_root).resolve() if args.project_root else find_project_root(Path(__file__).resolve())
    DATA_DIR = PR / "data" / "processed" / args.approach
    PIPE_SRC = PR / "models" / args.approach / "latest" / "feature_pipeline.pkl"
    META_PATH = PR / "models" / args.approach / "latest" / "train_meta.json"

    if not DATA_DIR.exists():
        raise FileNotFoundError(f"DATA_DIR not found: {DATA_DIR}")
    y_path = DATA_DIR / "y.csv"
    ids_path = DATA_DIR / "ids.csv"
    if not y_path.exists() or not ids_path.exists() or not PIPE_SRC.exists():
        raise FileNotFoundError(f"Missing inputs: {DATA_DIR} / {PIPE_SRC}")

    X = load_X(DATA_DIR)
    y = load_y(y_path)
    ids = pd.read_csv(ids_path, dtype=str)
    joblib.load(PIPE_SRC)

    tr_idx, va_idx = time_split_indices(ids, ratio=0.8)
    rid_va = ids["race_id"].astype(str).to_numpy()[va_idx]
    y_va = y[va_idx]
    X_tr, X_va = X[tr_idx], X[va_idx]
    y_tr = y[tr_idx]

    split_array = np.full(len(ids), -1)
    split_array[va_idx] = 0
    pds = PredefinedSplit(split_array)

    # 指示1: 正則化中心の探索空間。num_leaves は [15,31,63] のみ（127 は外す）
    param_dist = {
        "n_estimators": [200, 300, 400, 600],
        "learning_rate": [0.03, 0.05, 0.07, 0.1],
        "num_leaves": [15, 31, 63],
        "subsample": [0.7, 0.8, 0.9],
        "colsample_bytree": [0.7, 0.8, 0.9],
        "min_child_samples": [20, 30, 40, 60, 80],
        "reg_lambda": [0, 0.5, 1.0, 3.0, 10.0],
        "reg_alpha": [0, 0.1, 0.5, 1.0],
        "min_split_gain": [0, 0.01, 0.05, 0.1],
    }
    base = LGBMClassifier(random_state=42, n_jobs=-1, verbose=-1)
    search = RandomizedSearchCV(
        base,
        param_dist,
        n_iter=args.n_iter,
        cv=pds,
        scoring=args.scoring,
        n_jobs=1,
        verbose=1,
        random_state=42,
        refit=False,
    )
    print(f"[INFO] Fitting RandomizedSearchCV (scoring={args.scoring}, n_iter={args.n_iter}, time-based holdout)...")
    search.fit(X, y)

    # 現行モデルの指標（採用条件: PR-AUC が現行以上）
    current_metrics = None
    if META_PATH.exists():
        with open(META_PATH, encoding="utf-8") as f:
            meta = json.load(f)
        current_metrics = meta.get("eval") or {}
        print(f"[INFO] Current (latest) metrics: logloss={current_metrics.get('logloss')}, pr_auc={current_metrics.get('pr_auc')}, top2_hit={current_metrics.get('top2_hit')}")
    else:
        print("[WARN] train_meta.json not found; adoption filter (PR-AUC >= current) will be skipped.")

    # 指示3: 上位10件をホールドアウトで再評価し、LogLoss最小かつPR-AUC>=現行を採用
    cv_results = search.cv_results_
    # mean_test_score が大きい順（neg_log_loss なら大きいほど良い）
    rank = np.argsort(-np.array(cv_results["mean_test_score"]))
    top_k = 10
    candidates_holdout = []
    for i in range(min(top_k, len(rank))):
        idx = rank[i]
        params = cv_results["params"][idx]
        p = {**params, "random_state": 42, "n_jobs": -1, "verbose": -1}
        clf = LGBMClassifier(**p)
        clf.fit(X_tr, y_tr)
        met = eval_holdout(clf, X_va, y_va, rid_va)
        candidates_holdout.append({"params": params, "holdout": met})
    current_pr_auc = (current_metrics or {}).get("pr_auc") or 0.0
    # PR-AUC が現行以上のもののうち LogLoss 最小を採用
    admissible = [c for c in candidates_holdout if c["holdout"]["pr_auc"] >= current_pr_auc]
    if admissible:
        adopted = min(admissible, key=lambda c: c["holdout"]["logloss"])
        adopted_params = adopted["params"]
        adopted_metrics = adopted["holdout"]
    else:
        # 条件を満たすものがいなければ CV 最良をそのまま採用（フォールバック）
        adopted = candidates_holdout[0]
        adopted_params = adopted["params"]
        adopted_metrics = adopted["holdout"]
        print("[WARN] No candidate with PR-AUC >= current; adopted best CV candidate.")

    # 指示1: 上位5件（LogLoss順）を保存
    by_logloss = sorted(candidates_holdout, key=lambda c: c["holdout"]["logloss"])
    top5_candidates = []
    for c in by_logloss[:5]:
        top5_candidates.append({
            "params": c["params"],
            "logloss": c["holdout"]["logloss"],
            "pr_auc": c["holdout"]["pr_auc"],
            "top2_hit": c["holdout"]["top2_hit"],
            "auc": c["holdout"]["auc"],
        })

    # 指示2: 採用候補を seed 42,43,44 で再学習し、平均・標準偏差を保存
    seeds = [42, 43, 44]
    logloss_list, pr_auc_list, top2_list = [], [], []
    for seed in seeds:
        p = {**adopted_params, "random_state": seed, "n_jobs": -1, "verbose": -1}
        clf = LGBMClassifier(**p)
        clf.fit(X_tr, y_tr)
        met = eval_holdout(clf, X_va, y_va, rid_va)
        logloss_list.append(met["logloss"])
        pr_auc_list.append(met["pr_auc"])
        top2_list.append(met["top2_hit"])
    adopted_seed_stats = {
        "logloss": {"mean": float(np.mean(logloss_list)), "std": float(np.std(logloss_list))},
        "pr_auc": {"mean": float(np.mean(pr_auc_list)), "std": float(np.std(pr_auc_list))},
        "top2_hit": {"mean": float(np.mean(top2_list)), "std": float(np.std(top2_list))},
        "seeds": seeds,
    }

    out_path = Path(args.out) if args.out else PR / "models" / args.approach / "hpo_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "approach": args.approach,
        "n_iter": args.n_iter,
        "scoring": args.scoring,
        "best_params": search.best_params_,
        "best_cv_score": float(search.best_score_),
        "adopted_params": adopted_params,
        "adopted_metrics": adopted_metrics,
        "adopted_seed_stats": adopted_seed_stats,
        "top5_candidates": top5_candidates,
        "current_metrics": current_metrics,
        "n_train": int(tr_idx.size),
        "n_valid": int(va_idx.size),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("\n[OK] Best params (CV):", search.best_params_)
    print(f"[OK] Best CV score ({args.scoring}):", search.best_score_)
    print("[OK] Adopted params (LogLoss min + PR-AUC>=current):", adopted_params)
    print("[OK] Adopted holdout (seed=42):", adopted_metrics)
    print("[OK] Adopted seed stats (mean±std over seeds 42,43,44):", adopted_seed_stats)
    print("[OK] Top 5 by logloss:", [c["logloss"] for c in top5_candidates])
    print("[OK] Saved:", out_path)


if __name__ == "__main__":
    main()
