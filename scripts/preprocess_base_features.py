# -*- coding: utf-8 -*-
"""
preprocess_base_features.py  (Phase 2: YAML SSOT + used.yaml + json廃止)

本スクリプトの責務
------------------
- master CSV から学習データ（X / y / ids）を生成する
- 前処理パイプライン（feature_pipeline.pkl）を fit して保存する
- 「実際に使われた列（事実）」を YAML（feature_cols_used.yaml）として保存する
- models/{approach}/runs/<model_id>/ と models/{approach}/latest/ の成果物配置をコードで保証する

Phase 2 の設計原則
------------------
1) 列選別の SSOT は YAML（features/{approach}.yaml 等）
2) json は出力しない（feature_cols_used.json を完全廃止）
3) 最終防御線（FORCE_DROP_COLS）は YAML の有無に関係なく最終段で必ず除外する
4) used.yaml は “事実の固定”（再現性の核）。features/*.yaml は “設計入力”（人が編集）

入出力ルール（固定）
-------------------
- data/processed/{approach}/
    - X.npz または X_dense.npz
    - y.csv
    - ids.csv
- models/{approach}/runs/<model_id>/
    - feature_pipeline.pkl
    - feature_cols_used.yaml
- models/{approach}/latest/
    - feature_pipeline.pkl（runsからコピー）
    - feature_cols_used.yaml（runsからコピー）

運用フロー（推奨）
------------------
(1) columns.use を空で preprocess 実行（auto 推定）
(2) runs/<model_id>/feature_cols_used.yaml の selected_feature_cols を
    features/{approach}.yaml の columns.use に手動コピー
(3) 以後は use/add/drop で設計管理し、FORCE_DROP_COLS で最終防御
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
from scipy import sparse
from scipy.sparse import save_npz
import joblib

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer


# =============================================================================
# Project constants (adjust only with intention)
# =============================================================================

# 学習入力から除外する（ids.csv には残す）列
DEFAULT_ID_COLS = [
    "race_id",
    "player_id",
    "player",
    "motor_id",
    "motor_number",
    "boat_number",
    "section_id",
]

# リーク・結果依存の可能性が高い列（学習入力から除外）
LEAK_COLS = [
    "rank",
    "entry",
    "ST",
    "ST_rank",
    "winning_trick",
    "remarks",
    "finish1_flag_cur",
    "finish2_flag_cur",
    "finish3_flag_cur",
]

# 自動推定時に落とす “重い文字列” 系（必要に応じて増減）
DEFAULT_DROP_COLS = [
    "title",
    "schedule",
    "timetable",
    "weather",
    "place",
    "origin",
    "propeller",
    "parts_exchange",
]


# 数値列のうち「情報なし = 0」が自然なもの（例: adv/lr）
ADV_LR_PREFIXES = ("adv_p", "lr_p", "adv_p_", "lr_p_")

# -------------------------------------------------------------------------
# 最終防御線（Phase 2でも必須）
# - YAML で指定されていても、最終段で必ず除外する
# - v1.1.0 再現目的では最小（3列）に限定
# -------------------------------------------------------------------------
FORCE_DROP_COLS = [
    "boat_color",
    "entry_history",
    "rank_history",
]


# =============================================================================
# Minimal YAML loader/dumper (PyYAML があれば利用、無ければ簡易)
# =============================================================================

def _try_yaml() -> Optional[Any]:
    try:
        import yaml  # type: ignore
        return yaml
    except Exception:
        return None


def load_yaml(path: Path) -> Dict[str, Any]:
    """
    features/*.yaml の仕様を読み取る。
    想定: dict 構造（version/approach/target_col/id_cols/columns/options）
    """
    y = _try_yaml()
    text = path.read_text(encoding="utf-8")
    if y is not None:
        obj = y.safe_load(text)
        return obj or {}
    raise RuntimeError(
        "PyYAML が見つかりません。Phase 2 では YAML SSOT を使うため PyYAML を入れてください。\n"
        "例: pip install pyyaml (Anaconda なら conda install pyyaml)"
    )


def dump_yaml(obj: Dict[str, Any], path: Path) -> None:
    y = _try_yaml()
    path.parent.mkdir(parents=True, exist_ok=True)
    if y is not None:
        with path.open("w", encoding="utf-8") as f:
            y.safe_dump(obj, f, allow_unicode=True, sort_keys=False)
        return
    raise RuntimeError(
        "PyYAML が見つかりません。Phase 2 では YAML 出力が必須です。PyYAML を入れてください。"
    )


# =============================================================================
# Helpers
# =============================================================================

def make_model_id(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    return []


def uniq_preserve(seq: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def apply_force_drop(selected_cols: List[str]) -> Tuple[List[str], List[str]]:
    """
    最終防御線: FORCE_DROP_COLS を selected から除外する。
    返り値: (kept, force_dropped_present)
    """
    force_present = [c for c in FORCE_DROP_COLS if c in selected_cols]
    kept = [c for c in selected_cols if c not in set(FORCE_DROP_COLS)]
    return kept, force_present


# =============================================================================
# Column selection
# =============================================================================

def auto_select_cols(used: pd.DataFrame, max_cat_card: int) -> List[str]:
    """
    YAML columns.use が空の場合の自動推定。
    - 数値列
    - object列（ただし高カーデは除外）
    - DEFAULT_DROP_COLS は除外
    """
    num_cols = [c for c in used.columns if is_numeric_dtype(used[c])]
    obj_cols = used.select_dtypes(include="object").columns.tolist()

    # 高カーデ除外
    if obj_cols:
        nunq = used[obj_cols].nunique(dropna=True)
        obj_cols = nunq[nunq <= int(max_cat_card)].index.tolist()

    selected = sorted(set(num_cols + obj_cols) - set(DEFAULT_DROP_COLS))
    return selected


def select_cols_from_spec(
    used: pd.DataFrame,
    spec: Dict[str, Any],
    allow_missing: bool,
) -> Tuple[List[str], str, List[str], List[str]]:
    """
    spec（features/finals.yaml）から selected_feature_cols を決める。
    - columns.use が空なら auto 推定
    - columns.add/drop を適用
    - 存在チェック
    - 最終防御線（FORCE_DROP_COLS）は main 側で適用（必ず最後）
    """
    columns = spec.get("columns") or {}
    use = ensure_list(columns.get("use"))
    add = ensure_list(columns.get("add"))
    drop = ensure_list(columns.get("drop"))

    options = spec.get("options") or {}
    max_cat_card = int(options.get("max_cat_card", 50))

    if use:
        selection_mode = "yaml"
        selected = uniq_preserve(use)
    else:
        selection_mode = "auto"
        selected = auto_select_cols(used, max_cat_card=max_cat_card)

    # 差し引き
    drop_set = set(drop)
    selected = [c for c in selected if c not in drop_set]
    for c in add:
        if c and c not in selected:
            selected.append(c)

    # 存在チェック
    missing = [c for c in selected if c not in used.columns]
    if missing:
        msg = f"[ERROR] selected cols missing in master: {missing[:30]}" + (f" ...(n={len(missing)})" if len(missing) > 30 else "")
        if allow_missing:
            print(msg.replace("[ERROR]", "[WARN]") + " -> skip missing")
            selected = [c for c in selected if c in used.columns]
        else:
            raise RuntimeError(msg + "\n        Fix features YAML or use --allow-missing-selected-cols")

    return selected, selection_mode, add, drop


# =============================================================================
# CLI
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()

    ap.add_argument("--master", required=True, help="Input master CSV (e.g., data/processed/master_finals.csv)")
    ap.add_argument("--feature-spec-yaml", required=True, help="Feature spec YAML (SSOT), e.g. features/finals.yaml")
    ap.add_argument("--approach", required=True, help="Approach name (e.g. finals)")
    ap.add_argument("--target-col", default="", help="Override target_col (normally read from YAML)")
    ap.add_argument("--model-id", default="", help="Optional model_id (YYYYMMDD_HHMMSS)")
    ap.add_argument("--allow-missing-selected-cols", action="store_true", help="Warn & skip missing selected cols (default: error)")

    return ap


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    args = build_parser().parse_args()

    master_path = Path(args.master)
    spec_path = Path(args.feature_spec_yaml)
    approach = args.approach
    model_id = make_model_id(args.model_id or None)

    if not master_path.exists():
        raise RuntimeError(f"master not found: {master_path}")
    if not spec_path.exists():
        raise RuntimeError(f"feature spec yaml not found: {spec_path}")

    # SSOT 読み込み
    spec = load_yaml(spec_path)

    # target_col は YAML を正とし、CLI override があればそれを優先
    target_col = (args.target_col.strip() or str(spec.get("target_col") or "")).strip()
    if not target_col:
        raise RuntimeError("target_col is empty. Set in YAML (target_col:) or pass --target-col")

    # id_cols は YAML があれば優先（無ければ DEFAULT）
    id_cols = ensure_list(spec.get("id_cols"))
    if not id_cols:
        id_cols = DEFAULT_ID_COLS

    # 出力パス（Phase 2 で固定）
    features_dir = Path("data/processed") / approach
    runs_dir = Path("models") / approach / "runs" / model_id
    latest_dir = Path("models") / approach / "latest"

    features_dir.mkdir(parents=True, exist_ok=True)
    runs_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] master        : {master_path}")
    print(f"[INFO] feature_spec  : {spec_path}")
    print(f"[INFO] approach      : {approach}")
    print(f"[INFO] model_id      : {model_id}")
    print(f"[INFO] target_col    : {target_col}")
    print(f"[INFO] features_dir  : {features_dir}")
    print(f"[INFO] runs_dir      : {runs_dir}")

    # 読み込み（dtypeブレ抑制）
    df = pd.read_csv(master_path, low_memory=False, parse_dates=["date"])

    # 行順安定（存在するものだけでソート）
    sort_keys = [c for c in ["date", "race_id", "wakuban"] if c in df.columns]
    if sort_keys:
        df = df.sort_values(sort_keys).reset_index(drop=True)

    if target_col not in df.columns:
        raise RuntimeError(f"target col not found in master: {target_col}")

    # y / ids
    y = df[target_col].astype(int)

    ids_cols_present = [c for c in id_cols if c in df.columns]
    ids = df[ids_cols_present].copy() if ids_cols_present else pd.DataFrame(index=df.index)

    # 学習入力候補（ID/LEAK/target を除外）
    drop_for_used = [c for c in (id_cols + LEAK_COLS + [target_col]) if c in df.columns]
    used = df.drop(columns=drop_for_used).copy()

    # ---------------------------------------------------------------------
    # 列選別（YAML SSOT）: use/add/drop + auto
    # ---------------------------------------------------------------------
    selected_cols, selection_mode, add_cols, drop_cols = select_cols_from_spec(
        used=used,
        spec=spec,
        allow_missing=bool(args.allow_missing_selected_cols),
    )

    # ---------------------------------------------------------------------
    # 最終防御線（必ず最後に適用）
    # ---------------------------------------------------------------------
    selected_cols_before_force = list(selected_cols)
    selected_cols, force_dropped_present = apply_force_drop(selected_cols)

    # selected_df / num/cat 再計算（YAML指定の型ブレを吸収）
    selected_df = used[selected_cols].copy()
    num_cols = [c for c in selected_df.columns if is_numeric_dtype(selected_df[c])]
    cat_cols = selected_df.select_dtypes(include="object").columns.tolist()
    adv_like_cols = [c for c in num_cols if any(c.startswith(p) for p in ADV_LR_PREFIXES)]

    print(f"[INFO] selection_mode        : {selection_mode}")
    print(f"[INFO] selected cols         : {len(selected_cols)}")
    print(f"[INFO] numeric cols          : {len(num_cols)}")
    print(f"[INFO] categorical cols      : {len(cat_cols)}")
    print(f"[INFO] adv/lr zero-fill cols : {len(adv_like_cols)}")
    print(f"[INFO] FORCE_DROP present    : {force_dropped_present}")

    # ---------------------------------------------------------------------
    # 前処理パイプライン
    # ---------------------------------------------------------------------
    num_zero = adv_like_cols
    num_other = [c for c in num_cols if c not in set(num_zero)]

    transformers = []

    if num_zero:
        transformers.append((
            "num_zero",
            Pipeline([
                ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
                ("scaler", StandardScaler()),
            ]),
            num_zero,
        ))

    if num_other:
        transformers.append((
            "num",
            Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
            ]),
            num_other,
        ))

    if cat_cols:
        # sklearn 旧版互換
        try:
            ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        except TypeError:
            ohe = OneHotEncoder(handle_unknown="ignore", sparse=False)

        transformers.append((
            "cat",
            Pipeline([
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("ohe", ohe),
            ]),
            cat_cols,
        ))

    preprocessor = ColumnTransformer(transformers=transformers, remainder="drop")

    X = preprocessor.fit_transform(selected_df)
    x_shape = list(X.shape)
    print(f"[INFO] X shape: {tuple(x_shape)}  sparse={sparse.issparse(X)}")

    # ---------------------------------------------------------------------
    # 保存: data/processed/{approach}/
    # ---------------------------------------------------------------------
    if sparse.issparse(X):
        x_path = features_dir / "X.npz"
        save_npz(x_path, X)
    else:
        x_path = features_dir / "X_dense.npz"
        np.savez_compressed(x_path, X=X)

    y_path = features_dir / "y.csv"
    y.to_frame(target_col).to_csv(y_path, index=False, encoding="utf-8-sig")

    ids_path = features_dir / "ids.csv"
    if not ids.empty:
        ids.to_csv(ids_path, index=False, encoding="utf-8-sig")
    else:
        # ids が空でもファイルを作る（下流のI/Oを単純化）
        pd.DataFrame(index=df.index).to_csv(ids_path, index=False, encoding="utf-8-sig")

    # ---------------------------------------------------------------------
    # 保存: models/{approach}/runs/<model_id>/ と latest/
    # ---------------------------------------------------------------------
    pipeline_path = runs_dir / "feature_pipeline.pkl"
    joblib.dump(preprocessor, pipeline_path)

    used_yaml = {
        "version": 1,
        "approach": approach,
        "model_id": model_id,
        "master": str(master_path),
        "feature_spec_yaml": str(spec_path),

        "selection_mode": selection_mode,
        "spec_columns_use_empty": (not bool(ensure_list((spec.get("columns") or {}).get("use")))),

        "id_cols": id_cols,
        "target_col": target_col,

        "spec_add_cols": add_cols,
        "spec_drop_cols": drop_cols,

        # 最終防御線の情報
        "force_drop_cols": FORCE_DROP_COLS,
        "selected_feature_cols_before_force_drop": selected_cols_before_force,
        "force_dropped_cols_present": force_dropped_present,

        # 最終確定列（これが “事実”）
        "selected_feature_cols": selected_cols,
        "numeric_cols": num_cols,
        "categorical_cols": cat_cols,
        "adv_like_zero_fill_cols": adv_like_cols,

        "n_rows": int(len(df)),
        "X_shape": x_shape,

        "outputs": {
            "X": str(x_path),
            "y": str(y_path),
            "ids": str(ids_path),
            "pipeline": str(pipeline_path),
        },
    }

    used_yaml_path = runs_dir / "feature_cols_used.yaml"
    dump_yaml(used_yaml, used_yaml_path)

    # latest 更新（runs の成果物をコピー）
    shutil.copy2(pipeline_path, latest_dir / "feature_pipeline.pkl")
    shutil.copy2(used_yaml_path, latest_dir / "feature_cols_used.yaml")

    print("[OK] wrote:")
    print(f" - {x_path}")
    print(f" - {y_path}")
    print(f" - {ids_path}")
    print(f" - {pipeline_path}")
    print(f" - {used_yaml_path}")
    print(f" - {latest_dir / 'feature_pipeline.pkl'}")
    print(f" - {latest_dir / 'feature_cols_used.yaml'}")


if __name__ == "__main__":
    main()
