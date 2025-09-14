# -*- coding: utf-8 -*-
"""
scripts/predict_top2pair.py（旧ロジック準拠 + 表形式のコンソール出力）

- 入力（--mode live 前提）:
    --master : build_live_row.py の 6行raw CSV
    --race-id: レースID（YYYYMMDDJJRR 等）
- モデル:
    models/top2pair/latest/model.pkl（--model で上書き可）
    data/processed/features_top2pair.json（--features で上書き可）
- 出力:
    data/live/top2pair/pred_YYYYMMDD_JCD_RR.csv（UTF-8 SIG）
    列: race_id, wakuban_i, wakuban_j, player_i, player_j, score, softmax, p_top2set

- コンソール:
    [TOPK pairs by p_top2set] テーブル
    [Lane include probability (sum of pair probs containing lane)] テーブル
"""

import argparse
import sys
from pathlib import Path
import json
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from lightgbm import LGBMClassifier
import joblib

# --- プロジェクトルートを sys.path に追加 ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# === 学習時と揃える定数 ===
RACE_KEY = "race_id"
LEAK_COLS = {
    "is_top2", "rank", "winning_trick", "remarks",
    "henkan_ticket", "ST", "ST_rank", "__source_file",
    "entry", "is_wakunari",
}
ID_COLS_BASE = {
    "race_id", "section_id", "date", "code", "R", "place",
    "player", "player_id", "motor_number", "boat_number", "wakuban",
}
SHARED_NUMERIC_CANDS = ["temperature", "wind_speed", "water_temperature", "wave_height"]


def build_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """6艇→15ペア（i<j）"""
    if "wakuban" not in df.columns:
        raise KeyError("入力CSVに 'wakuban' がありません")
    df = df.copy()
    df["wakuban"] = pd.to_numeric(df["wakuban"], errors="coerce").astype("Int64")
    if df["wakuban"].isna().any():
        raise ValueError("wakuban に数値化できない値があります")

    L, R = df.add_suffix("_i"), df.add_suffix("_j")
    if RACE_KEY in df.columns:
        pairs = L.merge(R, left_on=f"{RACE_KEY}_i", right_on=f"{RACE_KEY}_j", how="inner")
    else:
        pairs = L.merge(R, how="cross")

    pairs = pairs[pairs["wakuban_i"] < pairs["wakuban_j"]].reset_index(drop=True)
    return pairs


def select_numeric_bases(pairs: pd.DataFrame) -> list[str]:
    """_i/_j 両方が数値で、リーク/ID以外のベース列を抽出"""
    bases = []
    for col in pairs.columns:
        if not col.endswith("_i"):
            continue
        base = col[:-2]
        if base in LEAK_COLS or base in ID_COLS_BASE:
            continue
        ci, cj = f"{base}_i", f"{base}_j"
        if cj not in pairs.columns:
            continue
        if is_numeric_dtype(pairs[ci]) and is_numeric_dtype(pairs[cj]):
            bases.append(base)
    return sorted(set(bases))


def build_features_live(pairs: pd.DataFrame, feature_list: list[str]) -> pd.DataFrame:
    """学習時 features に列順を合わせ、足りない列は0埋め、余剰列は捨てる"""
    feats = {}
    order = []

    # 共有数値（存在するもののみ）
    for s in SHARED_NUMERIC_CANDS:
        ci = f"{s}_i"
        if ci in pairs.columns and is_numeric_dtype(pairs[ci]):
            name = f"shared_{s}"
            feats[name] = pairs[ci].to_numpy(dtype="float32")
            order.append(name)

    # mean / diff / |diff|
    for base in select_numeric_bases(pairs):
        ai = pairs[f"{base}_i"].to_numpy(dtype="float32")
        aj = pairs[f"{base}_j"].to_numpy(dtype="float32")
        feats[f"{base}_mean"]  = (ai + aj) * 0.5
        feats[f"{base}_diff"]  = ai - aj
        feats[f"{base}_adiff"] = np.abs(ai - aj)
        order.extend([f"{base}_mean", f"{base}_diff", f"{base}_adiff"])

    Xdf = pd.DataFrame({k: feats[k] for k in order})

    # 列合わせ
    for col in feature_list:
        if col not in Xdf.columns:
            Xdf[col] = 0.0
    Xdf = Xdf[feature_list]
    return Xdf


def infer_out_path(master_path: Path, out_dir: Path, race_id: str) -> Path:
    """raw_YYYYMMDD_JCD_RR.csv → pred_YYYYMMDD_JCD_RR.csv（拡張子必須）"""
    stem = master_path.stem
    if stem.startswith("raw_"):
        name = "pred_" + stem[4:] + ".csv"
    else:
        name = f"pred_top2pair_{race_id}.csv"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / name


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["live"], default="live")
    ap.add_argument("--master", required=True, help="6行raw CSV（build_live_row.pyの出力）")
    ap.add_argument("--race-id", required=True, help="レースID（YYYYMMDDJJRR 等）")
    ap.add_argument("--model", default="", help="モデルパス（未指定: models/top2pair/latest/model.pkl）")
    ap.add_argument("--features", default=str(PROJECT_ROOT / "data" / "processed" / "features_top2pair.json"))
    ap.add_argument("--out-dir", default=str(PROJECT_ROOT / "data" / "live" / "top2pair"))
    ap.add_argument("--topk", type=int, default=10, help="コンソールに表示する上位ペア数")
    args = ap.parse_args()

    PR = PROJECT_ROOT
    master_path = Path(args.master)
    race_id = str(args.race_id)

    # 1) モデル
    model_path = Path(args.model) if args.model else (PR / "models" / "top2pair" / "latest" / "model.pkl")
    print("[INFO] Loading model from", model_path)
    model: LGBMClassifier = joblib.load(model_path)

    # 2) 学習時 features
    feature_list = json.loads(Path(args.features).read_text(encoding="utf-8"))
    if not isinstance(feature_list, list) or not feature_list:
        raise ValueError("features_top2pair.json が不正です")

    # 3) 入力CSV（6行）
    df = pd.read_csv(master_path, encoding="utf-8-sig")
    if RACE_KEY not in df.columns:
        df[RACE_KEY] = race_id

    # 4) ペア化 & 特徴
    pairs = build_pairs(df)
    Xdf = build_features_live(pairs, feature_list)
    X = Xdf.to_numpy(dtype=np.float32)

    # 5) 予測（確率）
    proba = model.predict_proba(X)[:, 1]
    exp = np.exp(proba - proba.max())
    softmax = exp / exp.sum()  # 15ペアで正規化（合計1）

    # 6) CSV保存（旧版互換の p_top2set を含める）
    out = pd.DataFrame({
        "race_id": pairs.get(f"{RACE_KEY}_i", pd.Series([race_id]*len(pairs))),
        "wakuban_i": pairs["wakuban_i"].astype(int),
        "wakuban_j": pairs["wakuban_j"].astype(int),
        "player_i": pairs.get("player_i", pd.Series([""]*len(pairs))),
        "player_j": pairs.get("player_j", pd.Series([""]*len(pairs))),
        "score": proba,
        "softmax": softmax,
        "p_top2set": softmax,
    })
    out_path = infer_out_path(master_path, Path(args.out_dir), race_id)
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print("[OK] saved:", out_path)

    # 7) コンソール出力：TopK 表
    K = max(1, args.topk)
    topk_df = (
        out.sort_values("p_top2set", ascending=False)
           .head(K)
           .loc[:, ["race_id", "wakuban_i", "wakuban_j", "p_top2set"]]
           .rename(columns={"wakuban_i": "i", "wakuban_j": "j"})
    )
    print("\n[TOP{} pairs by p_top2set]".format(K))
    # 小数6桁表示
    fmt = {"p_top2set": lambda v: f"{v:.6f}"}
    print(topk_df.to_string(index=False, formatters=fmt))

    # 8) コンソール出力：Lane include probability
    lane_probs = (
        pd.concat([
            out[["wakuban_i", "p_top2set"]].rename(columns={"wakuban_i": "wakuban"}),
            out[["wakuban_j", "p_top2set"]].rename(columns={"wakuban_j": "wakuban"}),
        ], ignore_index=True)
        .groupby("wakuban", as_index=False)["p_top2set"].sum()
        .rename(columns={"p_top2set": "contain_top2_prob"})
        .sort_values("contain_top2_prob", ascending=False)
    )
    # 表示整形
    lane_probs["wakuban"] = lane_probs["wakuban"].astype(int)
    fmt2 = {"contain_top2_prob": lambda v: f"{v:.6f}"}
    print("\n[Lane include probability (sum of pair probs containing lane)]")
    print(lane_probs.to_string(index=False, formatters=fmt2))


if __name__ == "__main__":
    main()
