# -*- coding: utf-8 -*-
"""
show_feature_importance.py

モデル（LightGBM）と feature_pipeline.pkl を読み込み、
Feature Importance（gain / split）を表示・CSV保存する。

今回の焦点
----------
feature が f142 等になる問題を解消する（実名にする）。

なぜ f142 になるのか
-------------------
- pipeline から最終特徴量名（OneHot展開後）を復元できないため。
- 今回の feature_pipeline.pkl は、OneHotEncoder が sklearn標準の Pipeline/ColumnTransformer
  の “典型パス” にいない（ラッパやカスタム属性に埋まっている）ため、
  従来探索だと見つからない。

対策
----
- Pythonオブジェクトグラフを __dict__/list/dict まで含めて深探索し、
  OneHotEncoder または “get_feature_names_out + categories_” を持つエンコーダを発見する。
- feature_cols_used.json の cat_cols を input_features として
  encoder.get_feature_names_out(...) を呼び、展開名を作る。

使い方（Anaconda Prompt 1行）
-----------------------------
C:\\anaconda3\\python.exe scripts\\show_feature_importance.py ^
  --model models\\finals\\latest\\model.pkl ^
  --pipeline models\\finals\\latest\\feature_pipeline.pkl ^
  --feature-cols-json models\\finals\\latest\\feature_cols_used.json ^
  --top 50 ^
  --out-csv data\\processed\\reports\\fi_finals_latest.csv

デバッグ（まず推奨）
-------------------
C:\\anaconda3\\python.exe scripts\\show_feature_importance.py ^
  --model models\\finals\\latest\\model.pkl ^
  --pipeline models\\finals\\latest\\feature_pipeline.pkl ^
  --feature-cols-json models\\finals\\latest\\feature_cols_used.json ^
  --top 20 ^
  --debug
"""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# CLI
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Show LightGBM feature importances (gain & split).")
    ap.add_argument("--model", required=True, help="Path to model.pkl (pickle/joblib/lightgbm text supported)")
    ap.add_argument("--pipeline", required=True, help="Path to feature_pipeline.pkl (pickle/joblib supported)")
    ap.add_argument("--feature-cols-json", default="", help="feature_cols_used.json to recover feature names")
    ap.add_argument("--top", type=int, default=50, help="Show top N features (default: 50)")
    ap.add_argument("--out-csv", default="", help="Optional output CSV path")
    ap.add_argument("--show-zero", action="store_true", help="Include zero-importance features")
    ap.add_argument("--debug", action="store_true", help="Verbose debug prints for pipeline inspection")
    return ap


# =============================================================================
# Loaders (robust)
# =============================================================================

def load_joblib(path: Path) -> Any:
    import joblib
    return joblib.load(path)


def load_pipeline_any(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(path)

    # 1) pickle
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception:
        pass

    # 2) joblib
    try:
        return load_joblib(path)
    except Exception as e:
        raise RuntimeError(f"Failed to load pipeline by pickle/joblib: {path}") from e


def load_model_any(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(path)

    # 1) pickle
    try:
        with path.open("rb") as f:
            return pickle.load(f)
    except Exception:
        pass

    # 2) joblib
    try:
        return load_joblib(path)
    except Exception:
        pass

    # 3) LightGBM Booster text model
    try:
        import lightgbm as lgb
        return lgb.Booster(model_file=str(path))
    except Exception as e:
        raise RuntimeError(f"Failed to load model by any known method: {path}") from e


# =============================================================================
# LightGBM booster extraction
# =============================================================================

def extract_lgbm_booster(model: Any) -> Any:
    if model.__class__.__module__.startswith("lightgbm") and model.__class__.__name__ == "Booster":
        return model
    if hasattr(model, "booster_"):
        return model.booster_
    if hasattr(model, "_Booster"):
        return model._Booster
    raise TypeError(f"Unsupported model type for LightGBM importance: {type(model)}")


# =============================================================================
# feature_cols_used.json helpers
# =============================================================================

def load_feature_cols_used_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def get_cols_from_feature_json(d: dict) -> Tuple[List[str], List[str], List[str]]:
    num = [str(x) for x in d.get("numeric_cols", [])]
    cat = [str(x) for x in d.get("categorical_cols", [])]
    zero = [str(x) for x in d.get("adv_lr_zero_fill_cols", [])]
    return num, cat, zero


# =============================================================================
# Deep object graph traversal to find encoder
# =============================================================================

def _is_probably_encoder(obj: Any) -> bool:
    """
    OneHotEncoder そのもの、または同等の振る舞いをするエンコーダを “性質” で判定する。
    目安:
      - get_feature_names_out を持つ
      - categories_ を持つ（fit済み OHE で典型）
    """
    if obj is None:
        return False
    name = obj.__class__.__name__
    if name == "OneHotEncoder":
        return True
    if hasattr(obj, "get_feature_names_out") and hasattr(obj, "categories_"):
        return True
    return False


def _iter_children_deep(obj: Any) -> Iterable[Any]:
    """
    sklearn の標準構造に依存しないために、Pythonオブジェクトを深く辿る。
    - list/tuple/set/dict
    - __dict__（カスタムクラスの属性）
    ※ 大きい配列等は回避したいので ndarray/Series/DataFrame は原則スキップ
    """
    if obj is None:
        return

    # 大きい/無意味な型は避ける
    if isinstance(obj, (str, bytes, bytearray, int, float, bool)):
        return
    if isinstance(obj, (np.ndarray, pd.Series, pd.DataFrame)):
        return

    if isinstance(obj, dict):
        for v in obj.values():
            yield v
        return

    if isinstance(obj, (list, tuple, set)):
        for v in obj:
            yield v
        return

    # sklearn Pipeline の steps / named_steps も一応拾う（ただしそれだけに依存しない）
    if hasattr(obj, "steps") and isinstance(getattr(obj, "steps"), list):
        for _, step in obj.steps:
            yield step
    if hasattr(obj, "named_steps"):
        try:
            for _, step in obj.named_steps.items():
                yield step
        except Exception:
            pass

    # ColumnTransformer transformers / transformers_ も拾う
    for attr in ("transformers_", "transformers"):
        if hasattr(obj, attr):
            try:
                trs = getattr(obj, attr)
                if isinstance(trs, list):
                    for t in trs:
                        # (name, trans, cols) 形式が多い
                        if isinstance(t, tuple) and len(t) >= 2:
                            yield t[1]
                        else:
                            yield t
            except Exception:
                pass

    # 最後に __dict__ を辿る（ここが本命）
    if hasattr(obj, "__dict__"):
        try:
            for v in obj.__dict__.values():
                yield v
        except Exception:
            pass


def find_encoder_deep(root: Any, debug: bool = False, max_nodes: int = 5000) -> Optional[Any]:
    """
    root から深探索して encoder を1つ見つける（最初に見つかったもの）。
    """
    seen = set()
    stack = [root]
    n = 0

    while stack:
        cur = stack.pop()
        n += 1
        if n > max_nodes:
            if debug:
                print(f"[DBG] encoder search aborted: reached max_nodes={max_nodes}")
            break

        oid = id(cur)
        if oid in seen:
            continue
        seen.add(oid)

        if _is_probably_encoder(cur):
            if debug:
                print(f"[DBG] encoder found: {cur.__class__.__module__}.{cur.__class__.__name__}")
            return cur

        for ch in _iter_children_deep(cur):
            stack.append(ch)

    if debug:
        print("[DBG] encoder not found in pipeline object graph.")
    return None


# =============================================================================
# Feature name recovery
# =============================================================================

def recover_feature_names(
    pipeline: Any,
    n_expected: int,
    feature_cols_json: Path,
    debug: bool = False,
) -> List[str]:
    """
    最終入力特徴量名（学習時の X の列）を復元する。

    優先順位:
    1) pipeline.get_feature_names_out が通ればそれ
    2) JSON から num/cat を取り、深探索で encoder を見つけて OHE 展開名を作る
    3) だめなら f0.. にフォールバック
    """
    # 1) 正攻法
    if hasattr(pipeline, "get_feature_names_out"):
        try:
            names = [str(x) for x in list(pipeline.get_feature_names_out())]
            if len(names) == n_expected:
                if debug:
                    print("[DBG] recovered via pipeline.get_feature_names_out()")
                return names
            if debug:
                print(f"[DBG] pipeline.get_feature_names_out len={len(names)} expected={n_expected} (ignored)")
        except Exception as e:
            if debug:
                print(f"[DBG] pipeline.get_feature_names_out failed: {e}")

    # 2) JSON + encoder
    d = load_feature_cols_used_json(feature_cols_json)
    num_cols, cat_cols, zero_cols = get_cols_from_feature_json(d)

    ordered_num = list(dict.fromkeys(zero_cols + [c for c in num_cols if c not in set(zero_cols)]))

    if debug:
        print(f"[DBG] numeric={len(num_cols)} zero={len(zero_cols)} ordered_num={len(ordered_num)}")
        print(f"[DBG] cat_cols={len(cat_cols)} : {cat_cols}")

    enc = find_encoder_deep(pipeline, debug=debug)

    ohe_names: List[str] = []
    if enc is not None and hasattr(enc, "get_feature_names_out"):
        # encoder 側が保持している input 名があればそれを優先（順序ズレを避ける）
        input_features = None
        if hasattr(enc, "feature_names_in_"):
            try:
                input_features = [str(x) for x in list(enc.feature_names_in_)]
                if debug:
                    print(f"[DBG] encoder.feature_names_in_={input_features}")
            except Exception:
                input_features = None

        if input_features is None:
            input_features = cat_cols

        try:
            ohe_names = [str(x) for x in list(enc.get_feature_names_out(input_features))]
            if debug:
                print(f"[DBG] encoder expanded names={len(ohe_names)} (head={ohe_names[:10]})")
        except Exception as e:
            if debug:
                print(f"[DBG] enc.get_feature_names_out failed: {e}")

    names = ordered_num + ohe_names

    if len(names) != n_expected:
        print(f"[WARN] recovered feature names length mismatch: recovered={len(names)} expected={n_expected}. "
              "Falling back to f0.. .")
        return [f"f{i}" for i in range(n_expected)]

    return names


# =============================================================================
# Importance
# =============================================================================

def build_importance_df(booster: Any, feature_names: List[str]) -> pd.DataFrame:
    gain = np.asarray(booster.feature_importance(importance_type="gain"), dtype=float)
    split = np.asarray(booster.feature_importance(importance_type="split"), dtype=float)

    if len(feature_names) != len(gain):
        raise ValueError(
            f"Feature length mismatch: names={len(feature_names)} vs gain={len(gain)}. "
            "Ensure model.pkl and feature_pipeline.pkl are from the same run."
        )

    df = pd.DataFrame(
        {"feature": feature_names, "importance_gain": gain, "importance_split": split}
    )
    df["gain_rank"] = df["importance_gain"].rank(ascending=False, method="min").astype(int)
    df["split_rank"] = df["importance_split"].rank(ascending=False, method="min").astype(int)
    return df


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    args = build_parser().parse_args()

    model_path = Path(args.model)
    pipe_path = Path(args.pipeline)

    print(f"[INFO] model   : {model_path}")
    print(f"[INFO] pipeline: {pipe_path}")

    model = load_model_any(model_path)
    pipeline = load_pipeline_any(pipe_path)

    booster = extract_lgbm_booster(model)
    n_features = booster.num_feature()

    if not args.feature_cols_json:
        print("[WARN] --feature-cols-json not provided. Falling back to f0..")
        feature_names = [f"f{i}" for i in range(n_features)]
    else:
        feat_json_path = Path(args.feature_cols_json)
        print(f"[INFO] feature-cols-json: {feat_json_path}")
        feature_names = recover_feature_names(
            pipeline=pipeline,
            n_expected=n_features,
            feature_cols_json=feat_json_path,
            debug=args.debug,
        )

    df = build_importance_df(booster, feature_names)

    if not args.show_zero:
        df = df[(df["importance_gain"] > 0) | (df["importance_split"] > 0)].copy()

    df_gain = df.sort_values(["importance_gain", "importance_split"], ascending=False).head(args.top)
    df_split = df.sort_values(["importance_split", "importance_gain"], ascending=False).head(args.top)

    print("\n=== TOP by GAIN ===")
    print(df_gain[["feature", "importance_gain", "importance_split"]].to_string(index=False))

    print("\n=== TOP by SPLIT ===")
    print(df_split[["feature", "importance_split", "importance_gain"]].to_string(index=False))

    if args.out_csv:
        out_path = Path(args.out_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df_out = df.sort_values(["importance_gain", "importance_split"], ascending=False).reset_index(drop=True)
        df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n[OK] wrote CSV: {out_path} (rows={len(df_out)})")


if __name__ == "__main__":
    main()
