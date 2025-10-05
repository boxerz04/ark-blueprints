# scripts/predict_one_race.py
# ----------------------------------------------------
# 単一レース（live_csv）を入力し、指定モデル系列（approach）の最新モデルで予測。
#
# 追加: --approach ensemble
#  - base と sectional を内部で推論し、src/ensemble/meta_features.py でメタ特徴を作成
#  - models/ensemble/latest/meta_model.pkl を適用して最終確率を出力
#  - 既存の base / sectional の挙動には影響なし（後方互換）
#
# 既存の特徴:
# - live_csv（例: build_live_row.py が生成した 6行DataFrame）を入力
# - モデル系列（--approach base / sectional / ensemble）を切り替え可能
# - Adapter方式：モデルごとの追加処理を src/adapters/*.py に分離
# - 既定では models/<approach>/latest/ の model.pkl / feature_pipeline.pkl を使用
# - 出力は data/live/<approach>/pred_<approach>_<入力ファイル名>.csv
# - コンソールには予測結果のサマリを表示（--quiet で抑止）
# - （任意）--show-features 指定時のみ、使用した列の詳細レポートを
#   ログ出力＆ data/live/<approach>/features_<stem>.txt に保存
# ----------------------------------------------------

from __future__ import annotations

import argparse
import sys
from importlib import import_module
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# ===== 重要：プロジェクトルートを sys.path に追加（src パッケージを確実に import するため）=====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# （new）メタ特徴ビルダ（スタッキング用）
try:
    from src.ensemble.meta_features import build_meta_features  # type: ignore
except Exception:
    build_meta_features = None  # ensemble未導入でも単体推論は動かせるようにする


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Predict one race using feature pipeline + model (Adapter runtime).")
    p.add_argument(
        "--live-csv",
        required=True,
        help="予測対象となる1レースの特徴量CSV（例: build_live_row.py の出力 / 6行）",
    )
    p.add_argument(
        "--approach",
        default="base",
        help="モデル系列（base, sectional, ensemble）",
    )
    p.add_argument(
        "--model",
        help="モデル pkl のパス（未指定時は models/<approach>/latest/model.pkl を使用）※ensemble時は無視",
    )
    p.add_argument(
        "--feature-pipeline",
        help="特徴量パイプライン pkl のパス（未指定時は models/<approach>/latest/feature_pipeline.pkl を使用）※ensemble時は無視",
    )
    p.add_argument(
        "--id-cols",
        default="race_id,code,R,wakuban,player",
        help="出力に含めるID列（カンマ区切り）",
    )
    p.add_argument(
        "--show-features",
        action="store_true",
        help="使用した特徴量の一覧を表示・保存する（単体アプローチ時のみ有効）",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="進捗と要約の表示を抑止（ログを抑えたい場合に利用）",
    )
    return p.parse_args()


def load_adapter(approach: str):
    """
    指定された approach に対応する Adapter モジュールをロードする。
    期待するエクスポート：prepare_live_input(df_live: pd.DataFrame, project_root: Path) -> pd.DataFrame
    """
    try:
        module = import_module(f"src.adapters.{approach}")
    except ModuleNotFoundError:
        sys.exit(f"[ERROR] adapter not found: src/adapters/{approach}.py")

    if not hasattr(module, "prepare_live_input"):
        sys.exit(f"[ERROR] adapter '{approach}' に prepare_live_input が未定義です。")

    return module


def _find_ohe_in_pipeline(pipe: Pipeline):
    """Pipeline 内から OneHotEncoder を探して返す（見つからなければ None）。"""
    try:
        from sklearn.preprocessing import OneHotEncoder
    except Exception:
        return None
    if not isinstance(pipe, Pipeline):
        return None
    for _, step in pipe.steps:
        if isinstance(step, OneHotEncoder):
            return step
    return None


def extract_feature_info(preprocessor) -> Tuple[List[str], List[str], List[str]]:
    """
    ColumnTransformer（または類する前処理器）から
    - 数値入力列（num_cols）
    - カテゴリ入力列（cat_cols）
    - 展開後のエンコード済み特徴名（encoded_feature_names）
    を推定して返す。

    可能な限り安全に取得し、未対応の環境では空配列で返す。
    """
    num_cols: List[str] = []
    cat_cols: List[str] = []
    encoded: List[str] = []

    # 1) ColumnTransformer 直接保存パターン（本リポジトリの標準）
    if isinstance(preprocessor, ColumnTransformer):
        try:
            for name, trans, cols in preprocessor.transformers_:
                if trans == "drop" or cols == "drop":
                    continue
                if name == "num":
                    if isinstance(cols, (list, tuple)):
                        num_cols = list(cols)
                    else:
                        num_cols = [str(cols)]
                    # 数値は展開しない（StandardScalerなので列名はそのまま）
                    encoded.extend(num_cols)
                elif name == "cat":
                    if isinstance(cols, (list, tuple)):
                        cat_cols = list(cols)
                    else:
                        cat_cols = [str(cols)]
                    ohe = _find_ohe_in_pipeline(trans) if isinstance(trans, Pipeline) else trans
                    try:
                        if hasattr(ohe, "get_feature_names_out"):
                            encoded_cat = list(ohe.get_feature_names_out(cat_cols))
                        else:
                            encoded_cat = cat_cols
                    except Exception:
                        encoded_cat = cat_cols
                    encoded.extend(encoded_cat)
                else:
                    if isinstance(cols, (list, tuple)):
                        encoded.extend(list(cols))
                    elif cols != "drop":
                        encoded.append(str(cols))
        except Exception:
            pass

    # 2) 直接 get_feature_names_out を持っている場合
    if not encoded:
        try:
            if hasattr(preprocessor, "get_feature_names_out"):
                encoded = list(preprocessor.get_feature_names_out())
        except Exception:
            pass

    # 3) feature_names_in_ があれば最小限の入力列（分類不能時のフォールバック）
    if not num_cols and not cat_cols:
        try:
            if hasattr(preprocessor, "feature_names_in_"):
                num_cols = list(preprocessor.feature_names_in_)  # 全体像のみ
        except Exception:
            pass

    return num_cols, cat_cols, encoded


def build_features_report_text(
    approach: str,
    stem: str,
    model_path: Path,
    pipe_path: Path,
    num_cols: List[str],
    cat_cols: List[str],
    encoded_names: List[str],
    n_live_rows: int,
) -> str:
    """
    指定テンプレに沿った“見やすい”テキストレポートを生成。
    - Numeric / Categorical 入力列
    - OneHot 展開名を列ごとにグルーピング（place_, race_grade_ ...）
    """
    # OneHot のグルーピング: {col_name: [category, ...]}
    grouped: Dict[str, List[str]] = {}
    # 数値パススルーは A) に再掲する
    passthrough_numeric = list(num_cols)

    # cat_cols について、encoded_names から "<col>_<category>" を抽出
    enc_set = list(encoded_names) if encoded_names else []
    for col in cat_cols:
        pref = f"{col}_"
        cats = []
        for name in enc_set:
            if name.startswith(pref):
                cats.append(name[len(pref):])
        if cats:
            grouped[col] = cats

    # テキスト整形（ASCIIのみを使用して cp932 でも落ちないように）
    lines: List[str] = []
    lines.append(f"# Features Report - {approach} ({stem})")
    lines.append(f"Model   : {model_path}")
    lines.append(f"Pipeline: {pipe_path}")
    lines.append(f"Rows    : {n_live_rows} (live)")
    lines.append("-" * 80)
    lines.append("")
    lines.append("[INPUT COLUMNS]")
    lines.append("")
    lines.append(f"■ Numeric ({len(num_cols)})")
    if num_cols:
        lines.extend(wrap_csv_line(num_cols, indent="  "))
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append(f"■ Categorical ({len(cat_cols)})")
    if cat_cols:
        lines.extend(wrap_csv_line(cat_cols, indent="  "))
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("-" * 80)
    lines.append("")
    lines.append(f"[ENCODED FEATURES]  total={len(encoded_names)}")
    lines.append("")
    # A) Passthrough numeric
    lines.append(f"A) Passthrough numeric ({len(passthrough_numeric)})")
    if passthrough_numeric:
        lines.extend(wrap_csv_line(passthrough_numeric, indent="  "))
    else:
        lines.append("  (none)")
    lines.append("")
    # B..) OneHot per categorical（入力の cat_cols 順で安定表示）
    label_ord = list(cat_cols)
    for idx, label in enumerate(label_ord):
        cats = grouped.get(label, [])
        section_letter = chr(ord('A') + 1 + idx)  # B, C, ...
        lines.append(f"{section_letter}) OneHot({label})  ({len(cats)})")
        if cats:
            lines.extend(wrap_csv_line(cats, indent="  "))
        else:
            lines.append("  (none)")
        lines.append("")
    lines.append("-" * 80)
    lines.append("Notes:")
    lines.append('- OneHot は "学習時に出現したカテゴリのみ" が展開対象です (handle_unknown="ignore")。')
    lines.append("  例: ステージ絞り込みにより未出現のカテゴリは列が生成されません。")
    lines.append("- Numeric は StandardScaler を経由しています (列名はそのまま)。")
    return "\n".join(lines)


def wrap_csv_line(items: List[str], width: int = 80, indent: str = "") -> List[str]:
    """
    カンマ区切りの長いリストを可読性重視で改行整形するユーティリティ。
    """
    out: List[str] = []
    line = indent
    first = True
    for it in items:
        token = ("" if first else ", ") + str(it)
        if len(line) + len(token) > width:
            out.append(line)
            line = indent + str(it)
            first = False
        else:
            line += token
            first = False
    if line.strip():
        out.append(line)
    return out


# -------------------------
# 内部ユーティリティ（単体モデルの推論）
# -------------------------
def _predict_with_single_approach(
    approach: str,
    df_live_raw: pd.DataFrame,
    id_cols: List[str],
    show_features: bool,
    quiet: bool,
    model_path_override: Optional[Path] = None,
    pipe_path_override: Optional[Path] = None,
) -> Tuple[pd.DataFrame, pd.Series, Optional[str]]:
    """
    指定アプローチ（base または sectional）で単体推論し、(出力DF, probaシリーズ, ログ名) を返す。
    - df_live_raw は live_csv を読み込んだ“そのまま”の6行（Adapter 内で各系列用に整形）
    - id_cols は最終DFに含める候補（存在する列のみ採用）
    """
    adapter = load_adapter(approach)
    df_live = adapter.prepare_live_input(df_live_raw.copy(), PROJECT_ROOT)

    # モデル／パイプラインのパス決定（latest 既定）
    base_dir = PROJECT_ROOT / "models" / approach / "latest"
    model_path = model_path_override if model_path_override else (base_dir / "model.pkl")
    pipe_path = pipe_path_override if pipe_path_override else (base_dir / "feature_pipeline.pkl")

    if not model_path.exists() or not pipe_path.exists():
        # 見つからなければ空を返す（上位の ensemble でフォールバック）
        return df_live, pd.Series([float("nan")] * len(df_live)), None

    if not quiet:
        print(f"[INFO] ({approach}) Loading model from {model_path}")
        print(f"[INFO] ({approach}) Loading feature pipeline from {pipe_path}")
    try:
        model = joblib.load(model_path)
        pipeline = joblib.load(pipe_path)
        X_live = pipeline.transform(df_live)
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X_live)[:, 1]
        else:
            y_hat = model.predict(X_live)
            proba = y_hat.astype("float64", copy=False)
        proba_s = pd.Series(proba, index=df_live.index, name=f"p_{approach}")
        # 出力DF（ID列のみ拾う）
        cols = [c for c in id_cols if c in df_live.columns]
        out_df = df_live[cols].copy()
        model_label = f"{approach}:{model_path.name}"
        # 単体モード時の features レポートは呼び出し元が実施
        return out_df, proba_s, model_label
    except Exception as e:
        if not quiet:
            print(f"[WARN] ({approach}) prediction failed, treat as missing: {e}")
        return df_live, pd.Series([float("nan")] * len(df_live)), None


def main():
    args = parse_args()

    # 1) live_csv を読み込み（1レース分 / 6行）
    df_live_raw = pd.read_csv(args.live_csv)
    id_cols = [c for c in args.id_cols.split(",") if c in df_live_raw.columns]

    # 2) ensemble 以外（= 従来の単体推論）は従来どおり
    if args.approach in ("base", "sectional"):
        # 単体アプローチの従来フロー
        adapter = load_adapter(args.approach)
        df_live = adapter.prepare_live_input(df_live_raw.copy(), PROJECT_ROOT)

        base_dir = PROJECT_ROOT / "models" / args.approach / "latest"
        model_path = Path(args.model) if args.model else (base_dir / "model.pkl")
        pipe_path = Path(args.feature_pipeline) if args.feature_pipeline else (base_dir / "feature_pipeline.pkl")

        if not model_path.exists():
            sys.exit(f"[ERROR] model not found: {model_path}")
        if not pipe_path.exists():
            sys.exit(f"[ERROR] feature_pipeline not found: {pipe_path}")

        if not args.quiet:
            print(f"[INFO] Loading model from {model_path}")
            print(f"[INFO] Loading feature pipeline from {pipe_path}")
        model = joblib.load(model_path)
        pipeline = joblib.load(pipe_path)
        X_live = pipeline.transform(df_live)
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X_live)[:, 1]
        else:
            y_hat = model.predict(X_live)
            proba = y_hat.astype("float64", copy=False)

        out_df = df_live[[c for c in id_cols if c in df_live.columns]].copy()
        out_df["proba"] = proba

        out_dir = PROJECT_ROOT / "data" / "live" / args.approach
        out_dir.mkdir(parents=True, exist_ok=True)
        stem = Path(args.live_csv).stem
        out_path = out_dir / f"pred_{args.approach}_{stem}.csv"
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

        # features レポートは従来どおり
        if args.show_features:
            pipeline_ct = pipeline
            num_cols, cat_cols, encoded_names = extract_feature_info(pipeline_ct)
            report_text = build_features_report_text(
                approach=args.approach,
                stem=stem,
                model_path=model_path,
                pipe_path=pipe_path,
                num_cols=num_cols,
                cat_cols=cat_cols,
                encoded_names=encoded_names,
                n_live_rows=len(df_live),
            )
            features_txt_path = out_dir / f"features_{stem}.txt"
            try:
                with open(features_txt_path, "w", encoding="utf-8") as f:
                    f.write(report_text)
                if not args.quiet:
                    print(f"\n[FEATURES] wrote: {features_txt_path}")
            except Exception as e:
                if not args.quiet:
                    print(f"[WARN] 特徴量一覧の保存に失敗しました: {e}")

        if not args.quiet:
            print(f"[OK] saved predictions: {out_path}")
            show_cols = [c for c in ["race_id", "code", "R", "wakuban", "player", "proba"] if c in out_df.columns]
            summary = out_df.sort_values("proba", ascending=False)[show_cols].reset_index(drop=True)
            model_name_for_log = f"{args.approach}:{model_path.name}"
            with pd.option_context("display.max_rows", 50,
                                   "display.width", 120,
                                   "display.float_format", "{:,.4f}".format):
                print(f"\n[SUMMARY] ({model_name_for_log}) prob(desc):")
                print(summary.to_string(index=False))
            if "wakuban" in summary.columns:
                top2 = summary.head(2)[["wakuban", "proba"]].to_records(index=False)
                pair = " - ".join([f"{int(w)}({p:.3f})" for w, p in top2])
                print(f"\n[TOP2] {pair}")
        return

    # 3) ここから ensemble モード（新規追加）
    if args.approach == "ensemble":
        if build_meta_features is None:
            sys.exit("[ERROR] ensemble is not available: src/ensemble/meta_features.py が見つかりません。")

        # 3-1) base を推論
        out_base_df, p_base, label_base = _predict_with_single_approach(
            "base", df_live_raw, id_cols, show_features=False, quiet=args.quiet
        )

        # 3-2) sectional を推論（失敗・未整備なら NaN）
        out_sec_df, p_sectional, label_sec = _predict_with_single_approach(
            "sectional", df_live_raw, id_cols, show_features=False, quiet=args.quiet
        )

        # 3-3) ID列を優先度で確定（base→sectional→raw の順で拾う）
        def _pick_ids(*dfs: pd.DataFrame) -> pd.DataFrame:
            for d in dfs:
                if d is not None and len(d.columns) > 0:
                    return d[[c for c in id_cols if c in d.columns]].copy()
            return df_live_raw[[c for c in id_cols if c in df_live_raw.columns]].copy()

        out_ids = _pick_ids(out_base_df, out_sec_df, df_live_raw)

        # 3-4) メタ入力用の DataFrame を構築（必要列: p_base, p_sectional ほか任意の文脈列）
        meta_df = out_ids.copy()
        meta_df["p_base"] = p_base.values if len(p_base) == len(meta_df) else float("nan")
        meta_df["p_sectional"] = p_sectional.values if len(p_sectional) == len(meta_df) else float("nan")

        # 可能なら stage / race_attribute も供給（base側→sectional側→rawの順に探す）
        for c in ["stage", "race_attribute"]:
            if c not in meta_df.columns:
                if out_base_df is not None and c in out_base_df.columns:
                    meta_df[c] = out_base_df[c]
                elif out_sec_df is not None and c in out_sec_df.columns:
                    meta_df[c] = out_sec_df[c]
                elif c in df_live_raw.columns:
                    meta_df[c] = df_live_raw[c]

        # 3-5) メタ特徴を生成
        X_meta, used_cols = build_meta_features(meta_df)
        # ★ここから追加：学習時の列順・列集合に合わせる
        
        import json
        meta_dir = PROJECT_ROOT / "models" / "ensemble" / "latest"
        with open(meta_dir / "meta_features.json", "r", encoding="utf-8") as f:
            meta_info = json.load(f)
        trained_cols = meta_info.get("used_cols", [])
        if trained_cols:
            X_meta = X_meta.reindex(columns=trained_cols, fill_value=0.0)
        # ★ここまで追加

        # 3-6) メタモデルを読み込み（models/ensemble/latest/meta_model.pkl）
        meta_dir = PROJECT_ROOT / "models" / "ensemble" / "latest"
        meta_model_path = meta_dir / "meta_model.pkl"
        if not meta_model_path.exists():
            sys.exit(f"[ERROR] meta_model not found: {meta_model_path}")
        if not args.quiet:
            print(f"[INFO] (ensemble) Loading meta model from {meta_model_path}")
        meta_model = joblib.load(meta_model_path)

        # 3-7) 最終確率を算出
        if hasattr(meta_model, "predict_proba"):
            p_ens = meta_model.predict_proba(X_meta)[:, 1]
        else:
            y_hat = meta_model.predict(X_meta)
            p_ens = y_hat.astype("float64", copy=False)

        # 3-8) 出力DF
        out_df = meta_df.copy()
        out_df["p_base"] = meta_df["p_base"]
        out_df["p_sectional"] = meta_df["p_sectional"]
        out_df["proba"] = p_ens  # 最終（アンサンブル）確率

        # 3-9) 保存先とファイル名
        out_dir = PROJECT_ROOT / "data" / "live" / "ensemble"
        out_dir.mkdir(parents=True, exist_ok=True)
        stem = Path(args.live_csv).stem
        out_path = out_dir / f"pred_ensemble_{stem}.csv"
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

        # 3-10) ログ出力（簡潔）
        if not args.quiet:
            print(f"[OK] saved predictions: {out_path}")
            show_cols = [c for c in ["race_id", "code", "R", "wakuban", "player", "p_base", "p_sectional", "proba"] if c in out_df.columns]
            summary = out_df.sort_values("proba", ascending=False)[show_cols].reset_index(drop=True)
            label_base = label_base or "base:NA"
            label_sec = label_sec or "sectional:NA"
            with pd.option_context("display.max_rows", 50,
                                   "display.width", 140,
                                   "display.float_format", "{:,.4f}".format):
                print(f"\n[SUMMARY] (ensemble <- {label_base} + {label_sec}) prob(desc):")
                print(summary.to_string(index=False))
            if "wakuban" in summary.columns:
                top2 = summary.head(2)[["wakuban", "proba"]].to_records(index=False)
                pair = " - ".join([f"{int(w)}({p:.3f})" for w, p in top2])
                print(f"\n[TOP2] {pair}")

        return

    # 4) 未知のアプローチ
    sys.exit(f"[ERROR] unknown approach: {args.approach}")


if __name__ == "__main__":
    main()
