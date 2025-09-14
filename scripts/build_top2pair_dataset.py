# -*- coding: utf-8 -*-
"""
scripts/build_top2pair_dataset.py
- master.csv（1行=1艇）から、Top2ペア学習用のデータセットを生成
- ラベルは master.csv の 'is_top2' を用いて、ペア両者が1なら1、それ以外は0
- 出力は data/processed/ に固定:
    - X_top2pair_dense.npz  （float32, dense）
    - y_top2pair.csv        （列名 'y'）
    - ids_top2pair.csv      （baseの ids.csv とは別に保存）
    - features_top2pair.json（使用特徴名の記録・任意）
"""

from pathlib import Path
import argparse
import json
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

ROOT = Path(__file__).resolve().parents[1]
DP   = ROOT / "data" / "processed"

# ----- 設定（必要ならここで微調整） -----
REQUIRE_EXACT_SIX = True  # レースは6艇揃いのみ採用（True推奨）
RACE_KEY = "race_id"      # グループキー（基本は race_id を推奨）

# 特徴量除外（リークやID系）
LEAK_COLS = {
    "is_top2", "rank", "winning_trick", "remarks",
    "henkan_ticket", "ST", "ST_rank", "__source_file",
    "entry", "is_wakunari",
}
ID_COLS_BASE = {
    "race_id", "section_id", "date", "code", "R", "place",
    "player", "player_id", "motor_number", "boat_number", "wakuban",
}

# 共有（レース共通）として残したい数値（存在すれば _i から拾う）
SHARED_NUMERIC_CANDS = [
    "temperature", "wind_speed", "water_temperature", "wave_height"
]

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default=str(DP / "master.csv"),
                    help="入力 master.csv（デフォルト: data/processed/master.csv）")
    ap.add_argument("--outdir", default=str(DP),
                    help="出力先（デフォルト: data/processed）")
    return ap.parse_args()

def load_master(master_csv: Path) -> pd.DataFrame:
    if not master_csv.exists():
        raise FileNotFoundError(f"master.csv not found: {master_csv}")
    df = pd.read_csv(master_csv, encoding="utf-8-sig", parse_dates=["date"], low_memory=False)
    req = {RACE_KEY, "wakuban", "is_top2"}
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise KeyError(f"master.csv 必須列が不足しています: {missing}")
    # wakuban を数値化（1..6 を期待）
    df["wakuban"] = pd.to_numeric(df["wakuban"], errors="coerce").astype("Int64")
    return df

def filter_complete_races(df: pd.DataFrame) -> pd.DataFrame:
    if not REQUIRE_EXACT_SIX:
        return df
    # 6艇揃いのレースのみ
    cnt = df.groupby(RACE_KEY)["wakuban"].count()
    ok_ids = cnt[cnt == 6].index
    out = df[df[RACE_KEY].isin(ok_ids)].copy()
    return out

def build_pair_table(df: pd.DataFrame) -> pd.DataFrame:
    """同一レース内で自己結合して i<j の15ペアを作成"""
    left  = df.add_suffix("_i")
    right = df.add_suffix("_j")
    pairs = left.merge(
        right,
        left_on=f"{RACE_KEY}_i",
        right_on=f"{RACE_KEY}_j",
        how="inner",
        suffixes=("_i", "_j")
    )
    # i<j のみ採用（wakubanでタイブレーク）
    if "wakuban_i" not in pairs or "wakuban_j" not in pairs:
        raise KeyError("wakuban 列が見当たりません")
    pairs = pairs[pairs["wakuban_i"] < pairs["wakuban_j"]].reset_index(drop=True)
    return pairs

def select_numeric_bases(pairs: pd.DataFrame) -> list[str]:
    """
    _i / _j 両方が数値列のベース名を抽出（ID/リークは除外）
    例: 'age_i' と 'age_j' の両方が数値 → ベース 'age' を採用
    """
    bases = []
    # _i で終わる列をベース候補とする
    for col in pairs.columns:
        if not col.endswith("_i"):
            continue
        base = col[:-2]
        if base in LEAK_COLS or base in ID_COLS_BASE:
            continue
        col_i = f"{base}_i"
        col_j = f"{base}_j"
        if col_j not in pairs.columns:
            continue
        if is_numeric_dtype(pairs[col_i]) and is_numeric_dtype(pairs[col_j]):
            bases.append(base)
    # 重複除去・安定ソート
    return sorted(set(bases))

def build_features_and_labels(pairs: pd.DataFrame):
    """
    特徴量:
      - 各数値ベースについて mean, diff(i-j), adiff(|i-j|)
      - 共有数値（存在すれば）: temperature, wind_speed, water_temperature, wave_height（_i 側を採用）
    ラベル:
      - y = (is_top2_i==1 & is_top2_j==1).astype(int)
    ID出力:
      - race_id, date, code, R, place, wakuban_i, wakuban_j, player_id_i/j, player_i/j
    """
    # ラベル
    for need in ("is_top2_i", "is_top2_j"):
        if need not in pairs.columns:
            raise KeyError(f"{need} が見当たりません（master.csv の is_top2 必須）")
    y = ((pairs["is_top2_i"] == 1) & (pairs["is_top2_j"] == 1)).astype("int8").to_numpy()

    # ベース選択
    bases = select_numeric_bases(pairs)

    # 共有数値（存在するものだけ拾う）
    shared_cols = []
    for s in SHARED_NUMERIC_CANDS:
        c = f"{s}_i"
        if c in pairs.columns and is_numeric_dtype(pairs[c]):
            shared_cols.append(s)

    feats = {}
    feat_names = []

    # 共有数値
    for s in shared_cols:
        arr = pairs[f"{s}_i"].to_numpy(dtype="float32")
        feats[f"shared_{s}"] = arr
        feat_names.append(f"shared_{s}")

    # mean, diff, |diff|
    for base in bases:
        ai = pairs[f"{base}_i"].to_numpy(dtype="float32")
        aj = pairs[f"{base}_j"].to_numpy(dtype="float32")
        m  = (ai + aj) * 0.5
        d  = ai - aj
        ad = np.abs(d)
        feats[f"{base}_mean"]  = m
        feats[f"{base}_diff"]  = d
        feats[f"{base}_adiff"] = ad
        feat_names.extend([f"{base}_mean", f"{base}_diff", f"{base}_adiff"])

    # 行列化
    X = np.vstack([feats[name] for name in feat_names]).T.astype("float32")

    # IDs（base用と衝突しないよう ids_top2pair.csv に出す）
    id_cols = []
    def add_if_exists(col):
        nonlocal id_cols
        if col in pairs.columns:
            id_cols.append(col)

    for c in [f"{RACE_KEY}_i", "date_i", "code_i", "R_i", "place_i",
              "wakuban_i", "wakuban_j",
              "player_id_i", "player_id_j", "player_i", "player_j"]:
        add_if_exists(c)

    ids_df = pairs[id_cols].copy()
    # 列名をわかりやすく
    rename_map = {
        f"{RACE_KEY}_i": "race_id",
        "date_i": "date",
        "code_i": "code",
        "R_i": "R",
        "place_i": "place",
    }
    ids_df = ids_df.rename(columns=rename_map)

    return X, y, ids_df, feat_names

def main():
    args = parse_args()
    master_csv = Path(args.master)
    outdir     = Path(args.outdir)

    df0 = load_master(master_csv)
    df  = filter_complete_races(df0)

    # 並び：レース→枠番で安定化（任意）
    df = df.sort_values(["date", RACE_KEY, "wakuban"], na_position="last").reset_index(drop=True)

    # ペア生成
    pairs = build_pair_table(df)

    # 特徴量・ラベル・ID
    X, y, ids_df, feat_names = build_features_and_labels(pairs)

    # 保存
    outdir.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(outdir / "X_top2pair_dense.npz", X=X)
    pd.DataFrame({"y": y}, dtype="int8").to_csv(outdir / "y_top2pair.csv", index=False, encoding="utf-8-sig")
    ids_df.to_csv(outdir / "ids_top2pair.csv", index=False, encoding="utf-8-sig")

    # 使った特徴名も残す（整合チェック用）
    with open(outdir / "features_top2pair.json", "w", encoding="utf-8") as f:
        json.dump(feat_names, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved dataset to: {outdir}")
    print(f" - X_top2pair_dense.npz  shape={X.shape}")
    print(f" - y_top2pair.csv        n={len(y)}  pos={int(y.sum())} ({y.mean():.4f})")
    print(f" - ids_top2pair.csv      shape={ids_df.shape}")
    print(f" - features_top2pair.json ({len(feat_names)} feats)")

if __name__ == "__main__":
    main()
