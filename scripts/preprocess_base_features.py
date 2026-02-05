# -*- coding: utf-8 -*-
"""
preprocess_base_features.py (robust; YAML whitelist + feature_cols_used.json)

目的
----
master CSV（または master_finals.csv 等）から学習用データを生成する。

本スクリプトは以下を行う：
  1) master を読み込む
  2) 学習に使う特徴量列を確定（自動 or YAMLホワイトリスト）
  3) 数値/カテゴリを分離し、欠損補完・エンコード・スケーリングを含む ColumnTransformer を構築
  4) X/y/ids を保存
  5) feature_pipeline.pkl を保存
  6) 今回「実際に採用した列」を feature_cols_used.json に保存（再現性の要）

重要な設計方針
--------------
- 目的は「再学習の再現性」と「列整理（足し引き）」を強固にすること。
- export_base_feature_yaml.py が出す base.yaml は “masterに存在する列一覧のスナップショット” であり、
  “学習に使う列の定義” ではない。
- 学習に使う列の定義は approach 別 YAML（例: features/finals.yaml）にまとめるのが安全。
- YAML が指定された場合は「その列だけ」を基本とし、drop/add で微調整可能にする。

YAML の形式
-----------
以下のいずれかに対応：
  (A) columns:
        - colA
        - colB
  (B) - colA
      - colB

実運用での推奨
--------------
- まず YAML 指定なしで一度前処理を回し、pipeline-dir に出る feature_cols_used.json を確認する。
- その selected_feature_cols を YAML 化して features/finals.yaml を作り、
  以後は --feature-list-yaml で列を固定する。

出力
----
out-dir:
  - X.npz            (疎行列のとき)
  - X_dense.npz      (密行列のとき)
  - y.csv            (ターゲット列 1本)
  - ids.csv          (ID列; 推論・検証用)
pipeline-dir:
  - feature_pipeline.pkl
  - feature_cols_used.json   ★重要: 採用列の真実
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
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
# Constants (project conventions)
# =============================================================================

# ID列: 学習入力からは除外するが ids.csv に残す列
# NOTE: motor_id は “役割を終えるID” なので明示的にここへ入れる（=学習入力から落とす）。
ID_COLS = [
    "race_id",
    "player_id",
    "player",
    "motor_id",          # 明示追加
    "motor_number",
    "boat_number",
    "section_id",
]

# 「推論時に見えてはならない」or 「目的変数と直結しやすい」リーク候補列
# （プロジェクトの都合で増減し得るが、基本はここに集約しておく）
LEAK_COLS = [
    "rank",
    "entry",
    "ST",
    "ST_rank",
    "winning_trick",
    "remarks",
    "henkan_ticket",
    "finish1_flag_cur",
    "finish2_flag_cur",
    "finish3_flag_cur",
    "__source_file",
]

# “adv/lr” 系の相対化特徴量は、欠損 = 情報なし（=0）として扱う方が安定しやすい。
# それ以外（motor_ 含む）は中央値補完を基本とする（ユーザー方針）。
ADV_LR_PREFIXES = ("adv_p", "lr_p", "adv_p_", "lr_p_")

# カテゴリとして「確実に使う」列（存在すれば）
SAFE_CAT_COLS = [
    "AB_class",
    "sex",
    "wind_direction",
    "race_grade",
    "race_type",
    "race_attribute",
    "season_q",
]

# “落としておくと事故が減る” 重い文字列列（ただし YAML 指定時は YAML が優先）
DEFAULT_DROP_COLS = [
    "title",
    "schedule",
    "timetable",
    "precondition_1",
    "precondition_2",
    "propeller",
    "parts_exchange",
    "team",
    "origin",
    "place",
    "weather",
]


# =============================================================================
# YAML utilities (robust; PyYAML optional)
# =============================================================================

def _safe_load_yaml(path: Path) -> Any:
    """
    PyYAML が利用できれば yaml.safe_load を使う。
    ない場合は “- item” 形式だけの簡易パースにフォールバックする。

    このプロジェクトでは「YAMLは列名の配列」を表す用途が主であり、
    本格的YAMLの機能は不要。依存を増やさないための実装。
    """
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore
        return yaml.safe_load(text)
    except Exception:
        # 簡易パース: "- col" だけ拾う
        cols: List[str] = []
        for line in text.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s.startswith("-"):
                v = s[1:].strip()
                if v:
                    cols.append(v)
        return cols


def load_feature_list_yaml(path: Path) -> List[str]:
    """
    YAML から列名リストを取得する。

    対応形式:
      (A) columns: [a, b, c] / columns: - a - b - c
      (B) - a - b - c

    返り値:
      列名リスト（順序を尊重; 重複は後で除去）
    """
    data = _safe_load_yaml(path)
    if data is None:
        return []

    if isinstance(data, dict):
        cols = data.get("columns")
        if isinstance(cols, list):
            return [str(x).strip() for x in cols if str(x).strip()]
        return []

    if isinstance(data, list):
        return [str(x).strip() for x in data if str(x).strip()]

    return []


# =============================================================================
# CLI
# =============================================================================

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Preprocess features for training (YAML whitelist supported; outputs feature_cols_used.json)."
    )

    ap.add_argument("--master", required=True, help="Input master CSV (e.g., data/processed/master_finals.csv)")
    ap.add_argument("--out-dir", required=True, help="Output directory for X/y/ids")
    ap.add_argument("--pipeline-dir", required=True, help="Output directory for feature_pipeline.pkl etc.")

    # ターゲット列名（プロジェクトで固定されているなら変えなくて良いが、柔軟性のため引数化）
    ap.add_argument("--target-col", default="is_top2", help="Target label column name (default: is_top2)")

    # 列固定（ホワイトリスト）: ここが今回の主役
    ap.add_argument("--feature-list-yaml", default="", help="Whitelist YAML for feature columns (optional)")

    # YAML をベースに足し引き（列整理フェーズに便利）
    ap.add_argument("--drop-cols", default="", help="Comma-separated columns to drop from selected features")
    ap.add_argument("--add-cols", default="", help="Comma-separated columns to add to selected features")

    # YAML / add-cols に存在しない列が含まれていた場合の挙動
    # 再現性重視のためデフォルトは “エラーで落とす”。運用で許容したい場合のみ True。
    ap.add_argument("--allow-missing-selected-cols", action="store_true",
                    help="If set, missing selected cols are warned and skipped (default: error)")

    # object列をカテゴリ採用する上限（高カーデ列は事故のもと）
    ap.add_argument("--max-cat-card", type=int, default=50,
                    help="Max cardinality for auto category columns (default: 50)")

    # 既存互換（順序の標準化に使う列が欠けても落ちないように）
    ap.add_argument("--sort-keys", default="date,race_id,wakuban",
                    help="Sort keys for row order stabilization (default: date,race_id,wakuban)")

    return ap


def _split_list(s: str) -> List[str]:
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


# =============================================================================
# Column selection logic
# =============================================================================

def determine_feature_columns(
    df_used: pd.DataFrame,
    feature_list_yaml: Optional[Path],
    drop_cols: Sequence[str],
    add_cols: Sequence[str],
    allow_missing: bool,
    max_cat_card: int,
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """
    学習に使う列（selected_feature_cols）を確定し、NUM/CAT/adv_like を返す。

    戦略:
      - YAML 指定あり:
          selected = YAML列（順序尊重） - forbidden(ID/LEAK/target) - drop + add
      - YAML 指定なし:
          selected = 自動推定(NUM + CAT) - DEFAULT_DROP - forbidden - drop + add

    返り値:
      selected_cols, num_cols, cat_cols, adv_like_cols
    """
    # forbidden は df_used の外で落としている前提だが、念のため最終段で弾く
    forbidden = set()

    # 1) ベースの selected を構築
    if feature_list_yaml is not None:
        yaml_cols = load_feature_list_yaml(feature_list_yaml)
        if not yaml_cols:
            raise SystemExit(f"[ERROR] feature-list-yaml is empty or invalid: {feature_list_yaml}")

        # 順序を尊重しつつ重複排除
        seen = set()
        selected = []
        for c in yaml_cols:
            if c and c not in seen:
                selected.append(c)
                seen.add(c)
    else:
        # 自動推定: 数値列 + 低カーデ object 列（SAFE_CAT は必ず入れる）
        num_candidates = [c for c in df_used.columns if is_numeric_dtype(df_used[c])]

        obj_cols = df_used.select_dtypes(include="object").columns.tolist()
        safe_present = [c for c in SAFE_CAT_COLS if c in obj_cols]

        auto_candidates = [c for c in obj_cols if c not in safe_present]
        auto_add: List[str] = []
        if auto_candidates:
            card = df_used[auto_candidates].nunique(dropna=True)
            auto_add = card[card <= int(max_cat_card)].index.tolist()

        cat_candidates = sorted(set(safe_present + auto_add))

        selected = sorted(set(num_candidates + cat_candidates))

        # DEFAULT_DROP_COLS は “事故防止” のための標準ドロップ
        selected = [c for c in selected if c not in set(DEFAULT_DROP_COLS)]

    # 2) drop/add を適用
    drop_set = set(drop_cols)
    selected = [c for c in selected if c not in drop_set]
    for c in add_cols:
        if c and c not in selected:
            selected.append(c)

    # 3) 存在チェック
    missing = [c for c in selected if c not in df_used.columns]
    if missing:
        msg = f"[ERROR] Selected cols missing in master: {missing[:30]}" + (f" ...(n={len(missing)})" if len(missing) > 30 else "")
        if allow_missing:
            print(msg.replace("[ERROR]", "[WARN]"))
            selected = [c for c in selected if c in df_used.columns]
        else:
            raise SystemExit(msg + "\n       Use --allow-missing-selected-cols to warn&skip.")

    # 4) NUM/CAT を “selected に基づき” 再判定（YAML指定時は特に重要）
    selected_df = df_used[selected].copy()
    num_cols = [c for c in selected_df.columns if is_numeric_dtype(selected_df[c])]
    cat_cols = selected_df.select_dtypes(include="object").columns.tolist()

    # 5) adv/lr 系（0埋め対象）
    adv_like = [c for c in num_cols if any(c.startswith(p) for p in ADV_LR_PREFIXES)]

    return selected, num_cols, cat_cols, adv_like


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    args = build_parser().parse_args()

    master_path = Path(args.master)
    out_dir = Path(args.out_dir)
    pipeline_dir = Path(args.pipeline_dir)

    out_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] master      : {master_path}")
    print(f"[INFO] out_dir     : {out_dir}")
    print(f"[INFO] pipeline_dir: {pipeline_dir}")
    print(f"[INFO] target_col  : {args.target_col}")

    if not master_path.exists():
        raise SystemExit(f"[ERROR] master not found: {master_path}")

    # low_memory=False で dtype のブレを抑え、推論時の列型差異による事故を減らす
    df = pd.read_csv(master_path, encoding="utf-8-sig", low_memory=False, parse_dates=["date"])
    print(f"[INFO] master shape: {df.shape}")

    target_col = args.target_col
    if target_col not in df.columns:
        raise SystemExit(f"[ERROR] target col not found: {target_col}")

    # -------------------------------------------------------------------------
    # 行順の標準化
    # - 学習/推論で “同じ race_id の並び” が揺れると debugging が難しくなる
    # - date,race_id,wakuban が揃っている前提で安定ソートする
    # -------------------------------------------------------------------------
    sort_keys = [k.strip() for k in args.sort_keys.split(",") if k.strip()]
    sort_keys_exist = [k for k in sort_keys if k in df.columns]
    if sort_keys_exist:
        df = df.sort_values(sort_keys_exist).reset_index(drop=True)
        print(f"[INFO] sorted by: {sort_keys_exist}")
    else:
        print(f"[WARN] none of sort keys exist in master: {sort_keys} (skip sorting)")

    # -------------------------------------------------------------------------
    # y / ids / used（学習入力候補）
    # - ids: 運用上参照したいキー群（学習入力からは除外）
    # - used: ID/リーク/target を落とした “候補特徴量DF”
    # -------------------------------------------------------------------------
    y = df[target_col].astype(int)

    ids_cols = [c for c in ID_COLS if c in df.columns]
    ids = df[ids_cols].copy() if ids_cols else pd.DataFrame(index=df.index)

    # 学習入力から落とす列（存在するものだけ）
    drop_for_used = [c for c in (ID_COLS + LEAK_COLS + [target_col]) if c in df.columns]
    used = df.drop(columns=drop_for_used).copy()

    # -------------------------------------------------------------------------
    # YAML の読み込み（あれば）
    # - YAMLパスは相対でも良い（呼び出し元のカレントからの相対）
    # -------------------------------------------------------------------------
    yaml_path: Optional[Path] = None
    if args.feature_list_yaml and args.feature_list_yaml.strip():
        yaml_path = Path(args.feature_list_yaml.strip())
        if not yaml_path.exists():
            raise SystemExit(f"[ERROR] feature-list-yaml not found: {yaml_path}")
        print(f"[INFO] feature-list-yaml: {yaml_path}")

    drop_cols = _split_list(args.drop_cols)
    add_cols = _split_list(args.add_cols)

    # -------------------------------------------------------------------------
    # 列選択の確定（selected / NUM / CAT / adv_like）
    # -------------------------------------------------------------------------
    selected_cols, num_cols, cat_cols, adv_like_cols = determine_feature_columns(
        df_used=used,
        feature_list_yaml=yaml_path,
        drop_cols=drop_cols,
        add_cols=add_cols,
        allow_missing=bool(args.allow_missing_selected_cols),
        max_cat_card=int(args.max_cat_card),
    )

    # motor_ 列が全部数値であることは既に確認済みだが、念のためログを出す
    motor_cols = [c for c in selected_cols if c.startswith("motor_")]
    print(f"[INFO] selected cols        : {len(selected_cols)}")
    print(f"[INFO] numeric cols         : {len(num_cols)}")
    print(f"[INFO] categorical cols     : {len(cat_cols)}")
    print(f"[INFO] adv/lr zero-fill cols: {len(adv_like_cols)}")
    if motor_cols:
        print(f"[INFO] motor cols selected  : {len(motor_cols)} (head: {motor_cols[:10]})")

    # -------------------------------------------------------------------------
    # 前処理パイプライン構築
    # - adv/lr: NaN -> 0  (情報なし)
    # - other numeric: NaN -> median（motor_ を含む）
    # - categorical: OneHot (unknown ignore)
    # -------------------------------------------------------------------------
    num_zero_cols = adv_like_cols
    num_other_cols = [c for c in num_cols if c not in set(num_zero_cols)]

    num_zero_tf = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="constant", fill_value=0.0)),
        ("scaler", StandardScaler()),
    ])

    num_other_tf = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    try:
        ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=True)
    except TypeError:
        # sklearn 旧版互換
        ohe = OneHotEncoder(handle_unknown="ignore", sparse=True)

    cat_tf = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe", ohe),
    ])

    transformers = []
    if num_zero_cols:
        transformers.append(("num_zero", num_zero_tf, num_zero_cols))
    if num_other_cols:
        transformers.append(("num", num_other_tf, num_other_cols))
    if cat_cols:
        transformers.append(("cat", cat_tf, cat_cols))

    preprocessor = ColumnTransformer(transformers=transformers, remainder="drop")
    print(f"[INFO] pipeline: num_zero={len(num_zero_cols)}, num={len(num_other_cols)}, cat={len(cat_cols)}")

    # -------------------------------------------------------------------------
    # fit_transform & 保存
    # -------------------------------------------------------------------------
    X = preprocessor.fit_transform(used[selected_cols])
    print(f"[INFO] X shape: {X.shape}  sparse={sparse.issparse(X)}")
    print(f"[INFO] y balance: {y.value_counts().to_dict()}")

    # X 保存（疎/密）
    if sparse.issparse(X):
        save_npz(out_dir / "X.npz", X)
        x_saved = str(out_dir / "X.npz")
    else:
        np.savez_compressed(out_dir / "X_dense.npz", X=X)
        x_saved = str(out_dir / "X_dense.npz")

    # y / ids 保存
    y.to_frame(target_col).to_csv(out_dir / "y.csv", index=False, encoding="utf-8-sig")
    if not ids.empty:
        ids.to_csv(out_dir / "ids.csv", index=False, encoding="utf-8-sig")

    # pipeline 保存
    joblib.dump(preprocessor, pipeline_dir / "feature_pipeline.pkl")

    # -------------------------------------------------------------------------
    # feature_cols_used.json（最重要）
    # - 「今回の学習前処理で何の列を使ったか」を固定化する “真実”
    # - YAML を作るときの起点にもなる
    # -------------------------------------------------------------------------
    meta: Dict[str, Any] = {
        "master": str(master_path),
        "n_rows": int(len(df)),
        "target_col": target_col,
        "feature_list_yaml": str(yaml_path) if yaml_path is not None else "",
        "drop_cols": drop_cols,
        "add_cols": add_cols,
        "selected_feature_cols": selected_cols,
        "numeric_cols": num_cols,
        "categorical_cols": cat_cols,
        "adv_like_zero_fill_cols": adv_like_cols,
        "motor_cols_selected": motor_cols,
        "outputs": {
            "X": x_saved,
            "y": str(out_dir / "y.csv"),
            "ids": str(out_dir / "ids.csv") if not ids.empty else "",
            "pipeline": str(pipeline_dir / "feature_pipeline.pkl"),
        },
    }

    (pipeline_dir / "feature_cols_used.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("[OK] wrote:")
    print(f" - {x_saved}")
    print(f" - {out_dir / 'y.csv'}")
    if not ids.empty:
        print(f" - {out_dir / 'ids.csv'}")
    print(f" - {pipeline_dir / 'feature_pipeline.pkl'}")
    print(f" - {pipeline_dir / 'feature_cols_used.json'}")


if __name__ == "__main__":
    main()
