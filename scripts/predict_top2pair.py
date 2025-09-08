# scripts/predict_top2pair.py
# -*- coding: utf-8 -*-
"""
Top2ペア方式 推論スクリプト（live推論に最適化）
- 既定の保存先: data/live/top2pair/pred_YYYYMMDD_JCD_RR.csv
- コンソール出力: 保存メッセージ + 上位ペアTOP10 + 枠番別Top2含有率

モード:
  A) cache: features_cache から推論（学習時の features.json と同順）
  B) live : master.csv(=6行CSVでもOK) + race_id 指定でオンザフライ生成
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path
import joblib
import numpy as np
import pandas as pd

# ===== 学習時のルール（build_top2pair_dataset.py と合わせる） =====
LEAK_COLS = {
    "entry","is_wakunari","rank","winning_trick",
    "remarks","henkan_ticket","ST","ST_rank","__source_file",
    "is_top2",
}
ID_BASE_COLS = {
    "race_id","player","player_id","motor_number","boat_number",
    "section_id","wakuban","code","R"
}
ENV_SHARED_COLS = ["temperature","wind_speed","water_temperature","wave_height"]

PAIR_NUMERIC_CANDIDATES = [
    "age","weight","ST_tenji","time_tenji","wakuban",
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


def racewise_softmax(ids: pd.DataFrame, raw_scores: np.ndarray) -> np.ndarray:
    """レース内(=同一race_idの15行)でsoftmax正規化"""
    probs = np.zeros_like(raw_scores, dtype=np.float64)
    tmp = pd.DataFrame({"race_id": ids["race_id"].values, "score": raw_scores})
    for _, idx in tmp.groupby("race_id").indices.items():
        s = tmp.iloc[idx]["score"].to_numpy()
        e = np.exp(s - s.max())
        probs[idx] = e / e.sum()
    return probs


# ---------- ユーティリティ（保存先の既定推論） ----------
def _infer_pred_path(master_path: Path, race_id: str, explicit_out: str | None) -> Path:
    if explicit_out:
        p = Path(explicit_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    # live の raw 形式ファイル名: raw_YYYYMMDD_JCD_RR.csv を優先採用
    name = master_path.name
    m = re.match(r"^raw_(\d{8})_(\d{2})_(\d{2})\.csv$", name)
    if m:
        out_dir = master_path.parent / "top2pair"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / f"pred_{m.group(1)}_{m.group(2)}_{m.group(3)}.csv"
    # それ以外: race_id から日付/JCD/R を切り出し（保険）
    m2 = re.match(r"^(\d{8})(\d{2})(\d{2})$", str(race_id))
    out_dir = master_path.parent / "top2pair"
    out_dir.mkdir(parents=True, exist_ok=True)
    if m2:
        return out_dir / f"pred_{m2.group(1)}_{m2.group(2)}_{m2.group(3)}.csv"
    return out_dir / f"pred_{race_id}.csv"


def _print_console_summary(out_df: pd.DataFrame):
    # 上位10ペア
    top = out_df.sort_values("p_top2set", ascending=False).head(10)
    print("\n[TOP10 pairs by p_top2set]")
    with pd.option_context("display.max_columns", None):
        print(top.to_string(index=False))

    # 枠番ごとの Top2含有率（各ペア確率を “その枠を含む” すべてで合計）
    lanes = sorted(pd.unique(pd.concat([out_df["i"], out_df["j"]]).astype(int)))
    contain = {k: 0.0 for k in lanes}
    for _, r in out_df.iterrows():
        contain[int(r["i"])] += float(r["p_top2set"])
        contain[int(r["j"])] += float(r["p_top2set"])
    lane_df = pd.DataFrame({"wakuban": list(contain.keys()),
                            "contain_top2_prob": [contain[k] for k in contain]})
    lane_df = lane_df.sort_values(["contain_top2_prob","wakuban"], ascending=[False,True])
    print("\n[Lane include probability (sum of pair probs containing lane)]")
    print(lane_df.to_string(index=False))

    # 合計チェック
    s = out_df["p_top2set"].sum()
    print(f"\n[check] sum of pair probs (should be ~1.0 per race): {s:.6f}")


# ---------- Cacheモード: features_cache をそのまま使う ----------
def predict_from_cache(cache_dir: str, model_path: str) -> pd.DataFrame:
    ids = pd.read_csv(os.path.join(cache_dir, "top2pair_ids.csv"))
    X = np.load(os.path.join(cache_dir, "top2pair_X_dense.npz"))["X"].astype(np.float32)
    with open(os.path.join(cache_dir, "features.json"), "r", encoding="utf-8") as f:
        feat_cache = json.load(f)["feature_names"]
    model = joblib.load(model_path)
    meta_path_guess = model_path.replace("model.pkl", "train_meta.json")
    if os.path.exists(meta_path_guess):
        with open(meta_path_guess, "r", encoding="utf-8") as f:
            meta = json.load(f)
        feat_model = meta.get("features", feat_cache)
    else:
        feat_model = feat_cache
    if feat_cache != feat_model:
        raise SystemExit("[ERR] features.json と学習時の features が一致しません。")
    raw = model.predict(X, raw_score=True)
    p = racewise_softmax(ids, raw)
    out = ids[["race_id","wakuban","wakuban_j"]].copy()
    out["p_top2set"] = p
    out = out.rename(columns={"wakuban":"i", "wakuban_j":"j"})
    return out


# ---------- Liveモード: master.csv → 指定 race_id でペア生成 ----------
def _load_master(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise SystemExit(f"[ERR] master not found: {path}")
    df = pd.read_csv(path, parse_dates=["date"])
    # entry が0/1なら >0 でフィルタ、1..6（枠番）ならスキップ
    if "entry" in df.columns:
        vals = pd.to_numeric(df["entry"], errors="coerce").dropna().unique()
        if len(vals) > 0:
            vmin, vmax = float(np.min(vals)), float(np.max(vals))
            if vmin >= 0.0 and vmax <= 1.0:
                df = df[df["entry"] > 0].copy()
    return df


def _pairs_for_race(df: pd.DataFrame, race_id: str) -> (pd.DataFrame, pd.DataFrame):
    sub = df[df["race_id"].astype(str) == str(race_id)].copy()
    if len(sub) != 6:
        raise SystemExit(f"[ERR] race_id={race_id} の行数が {len(sub)} 行です（6行必要）")
    sub = sub.sort_values("wakuban").reset_index(drop=True)
    left, right = sub.add_suffix("_i"), sub.add_suffix("_j")
    pairs = left.merge(right, how="inner", left_on="race_id_i", right_on="race_id_j")
    pairs = pairs[pairs["wakuban_i"] < pairs["wakuban_j"]].copy()
    keep_i = [f"{c}_i" for c in META_KEEP_I if f"{c}_i" in pairs.columns]
    keep_j = [f"{c}_j" for c in META_KEEP_J if f"{c}_j" in pairs.columns]
    ids = pairs[keep_i + keep_j].copy()
    ren = {f"{c}_i": c for c in META_KEEP_I if f"{c}_i" in ids.columns}
    ren.update({f"{c}_j": f"{c}_j" for c in META_KEEP_J if f"{c}_j" in ids.columns})
    ids = ids.rename(columns=ren)
    return pairs, ids


def _choose_numeric_bases(pairs: pd.DataFrame) -> list:
    BAN = set(LEAK_COLS) | set(ID_BASE_COLS) | set(ENV_SHARED_COLS)
    bases = []
    for c in PAIR_NUMERIC_CANDIDATES:
        if c in BAN: continue
        if f"{c}_i" in pairs.columns and f"{c}_j" in pairs.columns:
            if pd.api.types.is_numeric_dtype(pairs[f"{c}_i"]) and pd.api.types.is_numeric_dtype(pairs[f"{c}_j"]):
                bases.append(c)
    for base in set(c[:-2] for c in pairs.columns if c.endswith("_i")):
        if base in BAN: continue
        ci, cj = f"{base}_i", f"{base}_j"
        if ci in pairs.columns and cj in pairs.columns:
            if pd.api.types.is_numeric_dtype(pairs[ci]) and pd.api.types.is_numeric_dtype(pairs[cj]):
                if base not in bases:
                    bases.append(base)
    return bases


def _build_pair_features(pairs: pd.DataFrame, bases: list) -> (pd.DataFrame, list):
    cols = {}; feats = []
    for c in bases:
        ci, cj = f"{c}_i", f"{c}_j"
        v_i = pairs[ci].astype("float64").to_numpy()
        v_j = pairs[cj].astype("float64").to_numpy()
        cols[f"{c}__mean"]  = (v_i + v_j)/2
        cols[f"{c}__diff"]  = v_i - v_j
        cols[f"{c}__adiff"] = np.abs(v_i - v_j)
        feats += [f"{c}__mean", f"{c}__diff", f"{c}__adiff"]
    for c in ENV_SHARED_COLS:
        if f"{c}_i" in pairs.columns:
            cols[f"{c}__shared"] = pairs[f"{c}_i"].astype("float64").to_numpy()
            feats.append(f"{c}__shared")
    X = pd.DataFrame(cols)
    return X, feats


def predict_live(master_csv: str, model_path: str, race_id: str) -> pd.DataFrame:
    model = joblib.load(model_path)
    meta_path_guess = model_path.replace("model.pkl", "train_meta.json")
    if not os.path.exists(meta_path_guess):
        raise SystemExit("[ERR] train_meta.json が見つかりません。モデルと同じフォルダに置いてください。")
    with open(meta_path_guess, "r", encoding="utf-8") as f:
        meta = json.load(f)
    feat_model = meta.get("features", [])
    if not feat_model:
        raise SystemExit("[ERR] train_meta.json に features がありません。")

    df = _load_master(master_csv)
    pairs, ids = _pairs_for_race(df, race_id)
    bases = _choose_numeric_bases(pairs)
    X_df, feat_live = _build_pair_features(pairs, bases)

    # 列合わせ（モデルの features 並び）
    for fcol in feat_model:
        if fcol not in X_df.columns:
            X_df[fcol] = 0.0
    X_df = X_df[feat_model]

    raw = model.predict(X_df.to_numpy(dtype=np.float32), raw_score=True)
    p = racewise_softmax(ids, raw)

    out = ids[["race_id","wakuban","wakuban_j"]].copy()
    out["p_top2set"] = p
    out = out.rename(columns={"wakuban":"i", "wakuban_j":"j"})
    return out


# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["cache","live"], default="live")
    ap.add_argument("--cache-dir", help="cacheモード: features_cache/top2pair/<timestamp>")
    ap.add_argument("--master", help="liveモード: data/live/raw_YYYYMMDD_JCD_RR.csv など")
    ap.add_argument("--race-id", help="liveモード: 推論したい race_id（YYYYMMDD+JCD2+R2）")
    ap.add_argument("--model", required=True, help="学習済みモデル pkl（latest でも runs でも可）")
    ap.add_argument("--out", help="出力CSVパス（省略時は data/live/top2pair/pred_YYYYMMDD_JCD_RR.csv）")
    args = ap.parse_args()

    if args.mode == "cache":
        if not args.cache_dir:
            raise SystemExit("--cache-dir が必要です")
        out_df = predict_from_cache(args.cache_dir, args.model)
        # 保存先を決める（cacheでも先頭のrace_idで命名）
        first_race = str(out_df["race_id"].iloc[0]) if not out_df.empty else "unknown"
        pred_path = _infer_pred_path(Path(args.cache_dir), first_race, args.out)
    else:
        if not (args.master and args.race_id):
            raise SystemExit("--master と --race-id が必要です")
        out_df = predict_live(args.master, args.model, args.race_id)
        pred_path = _infer_pred_path(Path(args.master), args.race_id, args.out)

    # 保存（UTF-8-SIG、現行 predict_one_race.py と同様の運用）:
    out_df.to_csv(pred_path, index=False, encoding="utf_8_sig")
    print(f"[OK] saved: {pred_path}  (rows={len(out_df)}, cols={len(out_df.columns)})")

    # コンソール要約（既存 predict_one_race.py と同じく出力を表示）:
    #   → 保存メッセージ＋表を出す挙動は現行と整合。:contentReference[oaicite:1]{index=1}
    _print_console_summary(out_df)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[ERR]", repr(e), file=sys.stderr)
        sys.exit(1)
