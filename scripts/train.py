# scripts/train.py
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from datetime import datetime
from lightgbm import LGBMClassifier
from scipy import sparse
from scipy.sparse import load_npz
import json

# --- パス設定 ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR  = PROJECT_ROOT / "data" / "processed"
MODEL_DIR = PROJECT_ROOT / "models" / "latest"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# --- 特徴量の読み込み（疎/密 どちらにも対応） ---
X_path_sparse = DATA_DIR / "X.npz"
X_path_dense  = DATA_DIR / "X_dense.npz"
if X_path_sparse.exists():
    X = load_npz(X_path_sparse)
    x_repr = "sparse"
elif X_path_dense.exists():
    X = np.load(X_path_dense)["X"]
    x_repr = "dense"
else:
    raise FileNotFoundError("X.npz も X_dense.npz も見つかりません。features.ipynb のセル6を実行してください。")

y = pd.read_csv(DATA_DIR / "y.csv")["is_top2"].to_numpy(dtype=int)

print(f"[load] X: {x_repr}, shape={X.shape}  y: shape={y.shape}")

# --- モデル定義（最初の実装そのまま） ---
clf = LGBMClassifier(
    n_estimators=400,
    learning_rate=0.05,
    num_leaves=63,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    n_jobs=-1
)

# --- 学習 ---
clf.fit(X, y)
print("[fit] done.")

# --- 保存 ---
model_path = MODEL_DIR / "model.pkl"
joblib.dump(clf, model_path)

# ついでに前処理器（features.ipynb のセル6で保存済み）も存在チェック
pipe_path = MODEL_DIR / "feature_pipeline.pkl"
if not pipe_path.exists():
    print("[warn] feature_pipeline.pkl が見つかりません。features.ipynb のセル6を確認してください。")

# メタ情報を JSON で保存（再現性のため）
meta = {
    "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "x_repr": x_repr,
    "x_shape": X.shape,
    "y_len": int(len(y)),
    "model": "LightGBM",
    "params": {
        "n_estimators": 400,
        "learning_rate": 0.05,
        "num_leaves": 63,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
        "n_jobs": -1
    }
}
with open(MODEL_DIR / "train_meta.json", "w", encoding="utf-8") as f:
    json.dump(meta, f, ensure_ascii=False, indent=2)

print(f"[OK] saved: {model_path}")
print(f"[OK] meta : {MODEL_DIR / 'train_meta.json'}")
