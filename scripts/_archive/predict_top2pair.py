# scripts/predict_top2pair.py
# ------------------------------------------------------------
# top2pair 推論スクリプト（features.json の探索を強化）
# 既定: data/processed/top2pair/features.json
# 互換: data/processed/features_top2pair.json など旧配置も自動フォールバック
# ------------------------------------------------------------

import argparse
import json
from pathlib import Path
import sys
import joblib
import pandas as pd
import numpy as np

# === 既存のコア処理はそのまま使う想定。ここでは features.json の解決と I/O まわりだけ強化しています ===

def resolve_features_path(user_path: str | None) -> Path:
    """--features が未指定 or 見つからない場合に候補から探す"""
    candidates = []
    if user_path:
        candidates.append(Path(user_path))

    # 新：整理後の標準配置
    candidates.append(Path("data/processed/top2pair/features.json"))

    # 互換：以前の配置・命名
    candidates.append(Path("data/processed/features_top2pair.json"))
    candidates.append(Path("data/processed/top2pair/features_top2pair.json"))
    candidates.append(Path("data/processed/top2pair/feature_list.json"))

    for p in candidates:
        if p.exists():
            return p

    raise FileNotFoundError(
        "features.json が見つかりませんでした。\n"
        "  試した場所:\n  - "
        + "\n  - ".join(str(p) for p in candidates)
        + "\n  → build_top2pair_dataset.py 実行で features.json を生成してください。"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Predict top2 pairs for one race.")
    p.add_argument("--mode", choices=["live", "offline"], default="live")
    p.add_argument("--master", required=True, help="live/raw CSV または前処理済み CSV")
    p.add_argument("--race-id", help="2025YYYYJJRR（live時は推奨）")
    p.add_argument("--model", help="model.pkl（未指定なら models/top2pair/latest/model.pkl）")
    # 既定を新配置に変更
    p.add_argument("--features", help="features.json のパス（未指定なら自動探索）")
    p.add_argument("--out", help="出力CSV（未指定なら data/live/top2pair/pred_*.csv）")
    p.add_argument("--quiet", action="store_true")
    return p.parse_args()


def load_model(model_path: Path) -> object:
    if not model_path.exists():
        sys.exit(f"[ERROR] model not found: {model_path}")
    if model_path.suffix.lower() != ".pkl":
        sys.exit(f"[ERROR] --model は .pkl を指定してください: {model_path}")
    return joblib.load(model_path)


def load_features(feature_path: Path) -> list[str]:
    try:
        txt = feature_path.read_text(encoding="utf-8")
        feat = json.loads(txt)
        if isinstance(feat, dict) and "features" in feat:
            return list(feat["features"])
        if isinstance(feat, list):
            return list(feat)
        raise ValueError("features.json は list か {'features': [...]} 形式である必要があります")
    except Exception as e:
        sys.exit(f"[ERROR] features.json 読み込み失敗: {feature_path} ({e})")


def infer_race_id_from_master(master_path: Path) -> str | None:
    # 例: raw_20250915_24_9.csv → 202509152409
    stem = master_path.stem
    m = None
    if "raw_" in stem:
        parts = stem.split("_")
        if len(parts) >= 4 and parts[1].isdigit() and parts[2].isdigit() and parts[3].isdigit():
            m = f"{parts[1]}{parts[2]}{int(parts[3]):02d}"
    return m


def main():
    args = parse_args()

    ROOT = Path(__file__).resolve().parents[1]
    master_path = Path(args.master)

    if not master_path.exists():
        sys.exit(f"[ERROR] master CSV not found: {master_path}")

    # features.json の解決（新配置を優先、未指定なら自動探索）
    features_path = resolve_features_path(args.features)
    if not args.quiet:
        print(f"[INFO] Using features: {features_path}")

    # モデルの解決
    model_path = Path(args.model) if args.model else (ROOT / "models" / "top2pair" / "latest" / "model.pkl")
    if not args.quiet:
        print(f"[INFO] Loading model from {model_path}")
    model = load_model(model_path)

    # 入力読み込み
    df = pd.read_csv(master_path)

    # race_id を補完（live 原始CSVの場合）
    race_id = args.race_id or infer_race_id_from_master(master_path)
    if race_id is None and "race_id" in df.columns:
        race_id = str(df["race_id"].iloc[0])
    if race_id is None:
        sys.exit("[ERROR] --race-id を指定してください（または master から推測できませんでした）")

    # 特徴カラムの抽出（存在しない列は無視 or 0埋め）
    feat_cols = load_features(features_path)
    present = [c for c in feat_cols if c in df.columns]
    missing = [c for c in feat_cols if c not in df.columns]
    if not args.quiet:
        if missing:
            print(f"[WARN] missing features ({len(missing)}): {missing[:10]}{' ...' if len(missing)>10 else ''}")

    X = df[present].copy()
    # 数値に寄せる（カテゴリ等は学習時に前処理されている想定。ここでは安全側で to_numeric）
    for c in present:
        if X[c].dtype == "O":
            X[c] = pd.to_numeric(X[c], errors="coerce")
    X = X.fillna(0.0).to_numpy(dtype=np.float32)

    # 予測（モデル仕様に依存：ここは既存の predict_top2pair.py と同等）
    # 想定：各 (i,j) の組に対して p_top2set を出すモデル。ここでは 6行の順序に依存しないため、
    #       モデル実装に準じて出力を受け取り、pairs (i,j,p) を構築する。
    # ここでは既存の振る舞いを踏襲し、model.predict_proba(X)[:,1] を 6C2 にマッピングする想定。
    # 実装が異なる場合は、元の predict_top2pair.py の「出力→pairs生成」部分をそのまま残してください。
    try:
        proba = model.predict_proba(X)[:, 1]
    except Exception:
        # 一部のモデルは predict_proba を持たない
        proba = model.predict(X)
        if proba.ndim > 1:
            proba = proba[:, -1]
        proba = np.asarray(proba, dtype=np.float32)

    # ここから先は既存実装に合わせてください。
    # --- ダミー例（必要なら元ファイルのロジックに置換） ---
    # 6艇想定で 6C2=15 個の (i,j) に割り付ける例。実際は元スクリプトの生成方法を使ってください。
    pairs = []
    idx = 0
    for i in range(1, 7):
        for j in range(i + 1, 7):
            p = float(proba[idx]) if idx < len(proba) else 0.0
            pairs.append((i, j, p))
            idx += 1
    # --- ダミー例ここまで ---

    # 出力先（liveの既定）
    if args.out:
        out_path = Path(args.out)
    else:
        out_dir = ROOT / "data" / ("live" if args.mode == "live" else "processed") / "top2pair"
        out_dir.mkdir(parents=True, exist_ok=True)
        # race_id が YYYYMMDDJJRR で来ている想定
        out_path = out_dir / f"pred_{race_id[:8]}_{race_id[8:10]}_{int(race_id[10:12]):d}.csv"

    # 保存（列: race_id, i, j, p_top2set）
    out_df = pd.DataFrame(pairs, columns=["i", "j", "p_top2set"])
    out_df.insert(0, "race_id", race_id)
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    if not args.quiet:
        print(f"[OK] saved: {out_path}")
        print("\n[TOP10 pairs by p_top2set]")
        disp = out_df.sort_values("p_top2set", ascending=False).head(10)
        with pd.option_context("display.max_rows", 20, "display.width", 120, "display.float_format", "{:,.6f}".format):
            print(disp.to_string(index=False))


if __name__ == "__main__":
    main()
