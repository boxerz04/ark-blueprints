# scripts/train_top2pair.py
# -*- coding: utf-8 -*-
"""
Top2ペア方式の学習スクリプト。
- 入力: features_cache/top2pair/タイムスタンプ配下の ids / X_dense.npz / y
- 学習: LightGBM (binary)
- 評価: レース内softmax後のlogloss（TimeSeriesSplit）
- 出力:
    models/runs/top2pair/<model_id>/
        ├─ model.pkl
        ├─ train_meta.json   # 評価指標・使用特徴・学習条件など
        ├─ feature_importance.csv
        └─ cv_folds.csv
    models/latest/top2pair/
        ├─ model.pkl
        └─ train_meta.json
"""
import argparse
import glob
import json
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss
from sklearn.model_selection import TimeSeriesSplit

# LightGBM が未インストールなら: pip install lightgbm
import lightgbm as lgb


# ---------------------------
# ユーティリティ
# ---------------------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def latest_cache_dir(base: str) -> str:
    paths = sorted(glob.glob(os.path.join(base, "*")))
    if not paths:
        raise SystemExit(f"[ERR] No cache dirs under: {base}")
    return paths[-1]


def load_cache(cache_dir: str):
    ids = pd.read_csv(os.path.join(cache_dir, "top2pair_ids.csv"))
    y = pd.read_csv(os.path.join(cache_dir, "top2pair_y.csv"))["y"].astype(int).to_numpy()
    X = np.load(os.path.join(cache_dir, "top2pair_X_dense.npz"))["X"].astype(np.float32)
    with open(os.path.join(cache_dir, "features.json"), "r", encoding="utf-8") as f:
        feat_names = json.load(f)["feature_names"]
    return ids, X, y, feat_names


def racewise_softmax_probs(ids: pd.DataFrame, raw_scores: np.ndarray) -> np.ndarray:
    """レース内(=同一race_idの15行)でsoftmax正規化"""
    probs = np.zeros_like(raw_scores, dtype=np.float64)
    tmp = pd.DataFrame({"race_id": ids["race_id"].values, "score": raw_scores})
    for _, idx in tmp.groupby("race_id").indices.items():
        s = tmp.iloc[idx]["score"].to_numpy()
        e = np.exp(s - s.max())
        probs[idx] = e / e.sum()
    return probs


def eval_racewise_logloss(ids, y_true, raw_scores):
    # y_true: {0,1} だが、レースごとに1つだけ1
    # raw_scores -> race内softmax
    p = racewise_softmax_probs(ids, raw_scores)
    df = ids[["race_id"]].copy()
    df["y"] = y_true
    df["p"] = p
    losses = []
    for rid, sub in df.groupby("race_id"):
        # 正解ペアの確率だけを見る
        p_true = sub.loc[sub["y"] == 1, "p"]
        if len(p_true) != 1:
            continue  # 不整合はスキップ
        losses.append(-np.log(p_true.iloc[0] + 1e-12))
    return float(np.mean(losses))


def make_timeseries_order(ids: pd.DataFrame) -> np.ndarray:
    """
    時系列順のインデックスを返す。
    優先: date -> code -> R -> race_id
    なければ落ちないようフォールバック。
    """
    cols = []
    if "date" in ids.columns:
        cols.append(pd.to_datetime(ids["date"], errors="coerce").fillna(pd.Timestamp(1970, 1, 1)).astype(np.int64))
    if "code" in ids.columns:
        cols.append(ids["code"].fillna(-1).to_numpy())
    if "R" in ids.columns:
        cols.append(ids["R"].fillna(-1).to_numpy())
    cols.append(ids["race_id"].astype("category").cat.codes.to_numpy())
    return np.lexsort(tuple(cols[::-1]))  # 最後の指定が最優先になるため逆順で渡す


def feature_importance_df(model: lgb.Booster, feat_names: list) -> pd.DataFrame:
    imp_gain = model.feature_importance(importance_type="gain")
    imp_split = model.feature_importance(importance_type="split")
    df = pd.DataFrame({
        "feature": feat_names,
        "importance_gain": imp_gain,
        "importance_split": imp_split
    }).sort_values("importance_gain", ascending=False)
    return df


# ---------------------------
# メイン
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache-base", default="data/processed/features_cache/top2pair",
                    help="features_cache/top2pair の親ディレクトリ")
    ap.add_argument("--cache-dir", default="",
                    help="特定のキャッシュフォルダ（未指定なら cache-base の最新）")
    ap.add_argument("--n-splits", type=int, default=5)
    ap.add_argument("--learning-rate", type=float, default=0.05)
    ap.add_argument("--num-leaves", type=int, default=31)
    ap.add_argument("--min-data-in-leaf", type=int, default=50)
    ap.add_argument("--feature-fraction", type=float, default=0.8)
    ap.add_argument("--bagging-fraction", type=float, default=0.8)
    ap.add_argument("--bagging-freq", type=int, default=1)
    ap.add_argument("--early-stopping-rounds", type=int, default=50)
    # 出力先（現行の流儀に合わせる）
    ap.add_argument("--runs-dir", default="models/runs/top2pair",
                    help="各実行の成果物保存ルート")
    ap.add_argument("--latest-dir", default="models/latest/top2pair",
                    help="最新版へのコピー先")
    args = ap.parse_args()

    cache_dir = args.cache_dir or latest_cache_dir(args.cache-base if hasattr(args, "cache-base") else args.cache_base)  # safety
    # ↑ argparse の属性名はハイフンを含められないため安全対策
    cache_dir = args.cache_dir or latest_cache_dir(args.cache_base)
    print("[INFO] cache_dir:", cache_dir)

    # 1) データ読み込み
    ids, X, y, feat_names = load_cache(cache_dir)

    # 2) 時系列順に並べ替え（リーク防止のためフォールドもこの順で切る）
    sort_idx = make_timeseries_order(ids)
    ids = ids.iloc[sort_idx].reset_index(drop=True)
    X = X[sort_idx]
    y = y[sort_idx]

    # 3) CV 学習
    params = dict(
        objective="binary",
        metric="binary_logloss",
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        min_data_in_leaf=args.min_data_in_leaf,
        feature_fraction=args.feature_fraction,
        bagging_fraction=args.bagging_fraction,
        bagging_freq=args.bagging_freq,
        verbose=-1,
    )
    tss = TimeSeriesSplit(n_splits=args.n_splits)
    cv_losses = []
    best_iters = []
    models = []

    for fold, (tr, va) in enumerate(tss.split(X), 1):
        dtr = lgb.Dataset(X[tr], label=y[tr])
        dva = lgb.Dataset(X[va], label=y[va], reference=dtr)
        model = lgb.train(
            params, dtr, valid_sets=[dva], num_boost_round=500,
            callbacks=[lgb.early_stopping(args.early_stopping_rounds, verbose=False)]
        )
        raw_va = model.predict(X[va], raw_score=True, num_iteration=model.best_iteration)
        loss = eval_racewise_logloss(ids.iloc[va], y[va], raw_va)
        cv_losses.append(loss)
        best_iters.append(int(model.best_iteration or 0))
        models.append(model)
        print(f"[CV] fold{fold}: racewise logloss = {loss:.6f} (best_iter={best_iters[-1]})")

    cv_mean = float(np.mean(cv_losses))
    cv_std = float(np.std(cv_losses))
    best_iter = int(np.median([bi for bi in best_iters if bi > 0]) if best_iters else 300)
    print(f"[CV] mean={cv_mean:.6f}  std={cv_std:.6f}  use_best_iter={best_iter}")

    # 4) 全データで再学習（best_iterで止める）
    dall = lgb.Dataset(X, label=y)
    final = lgb.train(params, dall, num_boost_round=best_iter or 300)

    # 5) ランID決定・保存先準備
    model_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(args.runs_dir, model_id)
    ensure_dir(run_dir)
    latest_dir = args.latest_dir
    ensure_dir(latest_dir)

    # 6) 保存（runs）
    model_path = os.path.join(run_dir, "model.pkl")
    joblib.dump(final, model_path)

    # feature importance
    fi = feature_importance_df(final, feat_names)
    fi.to_csv(os.path.join(run_dir, "feature_importance.csv"), index=False, encoding="utf-8")

    # folds
    pd.DataFrame({
        "fold": list(range(1, args.n_splits + 1)),
        "racewise_logloss": cv_losses,
        "best_iter": best_iters,
    }).to_csv(os.path.join(run_dir, "cv_folds.csv"), index=False, encoding="utf-8")

    # meta
    train_meta = dict(
        model_family="top2pair",
        model_id=model_id,
        cache_dir=os.path.abspath(cache_dir),
        features=feat_names,
        params=params,
        cv_logloss_mean=cv_mean,
        cv_logloss_std=cv_std,
        best_iter=best_iter,
        rows=int(len(X)),
        cols=int(X.shape[1]),
        trained_at= datetime.now().isoformat()
    )
    with open(os.path.join(run_dir, "train_meta.json"), "w", encoding="utf-8") as f:
        json.dump(train_meta, f, ensure_ascii=False, indent=2)

    print("[DONE] saved run:", run_dir)

    # 7) latest にコピー（model.pkl & train_meta.json）
    joblib.dump(final, os.path.join(latest_dir, "model.pkl"))
    with open(os.path.join(latest_dir, "train_meta.json"), "w", encoding="utf-8") as f:
        json.dump(train_meta, f, ensure_ascii=False, indent=2)

    print("[DONE] updated latest:", latest_dir)


if __name__ == "__main__":
    main()
