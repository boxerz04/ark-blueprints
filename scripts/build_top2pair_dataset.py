# scripts/build_top2pair_dataset.py
# -*- coding: utf-8 -*-
"""
Build Top2 pair dataset

- master.csv (1行=1艇) を読み込み
- レース単位 (race_id) で 6艇をグループ化
- 15ペアを作成し、mean/diff/adiff 特徴を生成
- 環境系は shared のみ残す
- LEAK_COLS / ID_COLS は除外
"""

import argparse, json, os, sys
from datetime import datetime
import numpy as np, pandas as pd

def log(*a): print("[LOG]", *a, flush=True)
def err(*a): print("[ERR]", *a, file=sys.stderr, flush=True)

# === features.ipynb 準拠の定義 ===
LEAK_COLS = {
    "entry","is_wakunari","rank","winning_trick",
    "remarks","henkan_ticket","ST","ST_rank","__source_file",
    "is_top2",  # ← これを必ず追加
}

ID_BASE_COLS = {
    "race_id","player","player_id","motor_number","boat_number",
    "section_id","code","R"
}

ENV_SHARED_COLS = ["temperature", "wind_speed", "water_temperature", "wave_height"]

# 数値候補（環境系は除外）
PAIR_NUMERIC_CANDIDATES = [
    "age","weight","ST_tenji","time_tenji","wakuban",  # ← 追加
    "N_winning_rate","N_2rentai_rate","N_3rentai_rate",
    "LC_winning_rate","LC_2rentai_rate","LC_3rentai_rate",
    "motor_2rentai_rate","motor_3rentai_rate",
    "boat_2rentai_rate","boat_3rentai_rate",
    "ST_mean","entry_tenji","day","run_once","Tilt","F","L",
]


META_KEEP_I = ["race_id","date","place","race_grade","race_type","race_attribute",
               "weather","wind_direction","section_id","R","code",
               "day","section","timetable","wakuban","player_id"]
META_KEEP_J = ["wakuban","player_id"]

def parse_args():
    p = argparse.ArgumentParser(description="Build Top2 pair dataset")
    p.add_argument("--master", required=True)
    p.add_argument("--outdir", default="data/processed/features_cache/top2pair")
    p.add_argument("--numeric-auto", action="store_true")
    p.add_argument("--group-key", default="race_id",
                   help="race_id | section_id | date,code,R")
    return p.parse_args()

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def load_master(path):
    log("cwd:", os.getcwd())
    log("loading master:", path)
    if not os.path.exists(path):
        err("master not found:", path); sys.exit(1)
    try:
        df = pd.read_csv(path, parse_dates=["date"])
    except Exception as e:
        err("read_csv failed:", e); sys.exit(1)

    # entry の解釈に応じて安全に処理：
    # - 0/1 の二値（出走可否など）なら >0 を残す
    # - 1..6 の枠番ならフィルタしない（全艇残す）
    if "entry" in df.columns:
        vals = pd.to_numeric(df["entry"], errors="coerce").dropna().unique()
        if len(vals) > 0:
            vmin, vmax = float(np.min(vals)), float(np.max(vals))
            if vmin >= 0.0 and vmax <= 1.0:
                df = df[df["entry"] > 0].copy()
                log("applied entry>0 filter (binary entry detected)")
            else:
                log("skip entry filter (looks like lane 1..6)")
        else:
            log("skip entry filter (non-numeric or empty)")
    log("master shape:", df.shape)
    return df

def filter_to_six(df: pd.DataFrame, key):
    sizes = df.groupby(list(key)).size()
    good_keys = set(sizes[sizes == 6].index)
    before = len(sizes)
    if len(key) == 1:
        mask = df[key[0]].map(lambda k: k in good_keys)
    else:
        mask = df.set_index(list(key)).index.map(lambda k: k in good_keys)
    kept = df[mask].copy()
    log(f"filter to exactly 6 rows per group: {before} -> {len(good_keys)} groups "
        f"({len(kept)}/{len(df)} rows)")
    return kept

def make_pairs(df: pd.DataFrame, key):
    df = filter_to_six(df, key)
    log("making pairs (self-join) with key:", key)
    left, right = df.add_suffix("_i"), df.add_suffix("_j")
    if len(key) == 1:
        k = key[0]
        pairs = left.merge(right, left_on=f"{k}_i", right_on=f"{k}_j", how="inner")
    else:
        lk = [f"{k}_i" for k in key]; rk = [f"{k}_j" for k in key]
        pairs = left.merge(right, left_on=lk, right_on=rk, how="inner")
    # i<j で重複除去
    if "wakuban_i" in pairs.columns and "wakuban_j" in pairs.columns:
        pairs = pairs[pairs["wakuban_i"] < pairs["wakuban_j"]].copy()
    log("pairs shape:", pairs.shape)
    return pairs

def choose_numeric_columns(pairs, prefer, auto):
    BAN_BASE = set(LEAK_COLS) | set(ID_BASE_COLS) | set(ENV_SHARED_COLS)
    cols = []
    for c in prefer:
        if c in BAN_BASE: continue
        if f"{c}_i" in pairs.columns and f"{c}_j" in pairs.columns:
            if pd.api.types.is_numeric_dtype(pairs[f"{c}_i"]) and pd.api.types.is_numeric_dtype(pairs[f"{c}_j"]):
                cols.append(c)
    if auto:
        base_names = set(c[:-2] for c in pairs.columns if c.endswith("_i"))
        for base in base_names:
            if base in BAN_BASE: continue
            ci, cj = f"{base}_i", f"{base}_j"
            if ci in pairs.columns and cj in pairs.columns:
                if pd.api.types.is_numeric_dtype(pairs[ci]) and pd.api.types.is_numeric_dtype(pairs[cj]):
                    if base not in cols: cols.append(base)
    log("numeric_cols after leak/ID/env filter:", len(cols))
    return cols

def build_pair_features(pairs, numeric_cols):
    cols = {}; feats=[]
    # mean/diff/adiff
    for c in numeric_cols:
        ci, cj = f"{c}_i", f"{c}_j"
        v_i = pairs[ci].astype("float64").to_numpy()
        v_j = pairs[cj].astype("float64").to_numpy()
        cols[f"{c}__mean"]  = (v_i + v_j)/2
        cols[f"{c}__diff"]  = v_i - v_j
        cols[f"{c}__adiff"] = np.abs(v_i - v_j)
        feats += [f"{c}__mean", f"{c}__diff", f"{c}__adiff"]
    # 環境系は shared のみ
    for c in ENV_SHARED_COLS:
        if f"{c}_i" in pairs.columns:
            cols[f"{c}__shared"] = pairs[f"{c}_i"].astype("float64").to_numpy()
            feats.append(f"{c}__shared")
    X = pd.DataFrame(cols, index=pairs.index)
    log("feature cols:", len(feats))
    return X, feats

def build_meta_ids(pairs):
    keep_i = [f"{c}_i" for c in META_KEEP_I if f"{c}_i" in pairs.columns]
    keep_j = [f"{c}_j" for c in META_KEEP_J if f"{c}_j" in pairs.columns]
    ids = pairs[keep_i+keep_j].copy()
    ren = {f"{c}_i": c for c in META_KEEP_I if f"{c}_i" in ids.columns}
    ren.update({f"{c}_j": f"{c}_j" for c in META_KEEP_J if f"{c}_j" in ids.columns})
    ids = ids.rename(columns=ren)
    log("ids shape:", ids.shape)
    return ids

def build_labels(pairs):
    if "is_top2_i" not in pairs.columns or "is_top2_j" not in pairs.columns:
        err("is_top2 columns not found"); sys.exit(1)
    y = ((pairs["is_top2_i"]==1) & (pairs["is_top2_j"]==1)).astype("int8").to_numpy()
    log("positive pairs:", int(y.sum()))
    return y

def main():
    args = parse_args()
    ensure_dir(args.outdir)
    df = load_master(args.master)
    key = tuple(args.group_key.split(",")) if args.group_key else ("race_id",)
    pairs = make_pairs(df, key)
    if pairs.empty: err("pairs empty after join"); sys.exit(1)
    numeric_cols = choose_numeric_columns(pairs, PAIR_NUMERIC_CANDIDATES, args.numeric_auto)
    X_df, feat_names = build_pair_features(pairs, numeric_cols)
    ids_df = build_meta_ids(pairs)
    y = build_labels(pairs)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = os.path.join(args.outdir, ts); ensure_dir(outdir)
    ids_path = os.path.join(outdir, "top2pair_ids.csv")
    x_path   = os.path.join(outdir, "top2pair_X_dense.npz")
    y_path   = os.path.join(outdir, "top2pair_y.csv")
    feat_path= os.path.join(outdir, "features.json")
    meta_path= os.path.join(outdir, "dataset_meta.json")
    ids_df.to_csv(ids_path, index=False, encoding="utf-8")
    np.savez_compressed(x_path, X=X_df.to_numpy(dtype="float32"))
    pd.DataFrame({"y":y}).to_csv(y_path, index=False)
    with open(feat_path,"w",encoding="utf-8") as f: json.dump({"feature_names":feat_names}, f, ensure_ascii=False, indent=2)
    with open(meta_path,"w",encoding="utf-8") as f: json.dump({
        "source_master": os.path.abspath(args.master),
        "rows": int(len(X_df)), "features": len(feat_names)
    }, f, ensure_ascii=False, indent=2)
    log("DONE:", outdir)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err("FATAL:", repr(e)); sys.exit(1)
