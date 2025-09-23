# scripts/predict_one_race.py
# ----------------------------------------------------
# 単一レース（live_csv）を入力し、指定モデル系列（approach）の最新モデルで予測。
#
# 特徴:
# - live_csv（例: build_live_row.py が生成した 6行DataFrame）を入力
# - モデル系列（--approach base / sectional ...）を切り替え可能
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
from typing import Dict, List, Tuple

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline


# ===== 重要：プロジェクトルートを sys.path に追加（src パッケージを確実に import するため）=====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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
        help="モデル系列（例: base, sectional）",
    )
    p.add_argument(
        "--model",
        help="モデル pkl のパス（未指定時は models/<approach>/latest/model.pkl を使用）",
    )
    p.add_argument(
        "--feature-pipeline",
        help="特徴量パイプライン pkl のパス（未指定時は models/<approach>/latest/feature_pipeline.pkl を使用）",
    )
    p.add_argument(
        "--id-cols",
        default="race_id,code,R,wakuban,player",
        help="出力に含めるID列（カンマ区切り）",
    )
    p.add_argument(
        "--show-features",
        action="store_true",
        help="使用した特徴量の一覧を表示・保存する（指定時のみ）",
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


def main():
    args = parse_args()

    # 1) live_csv を読み込み（1レース分 / 6行）
    df_live = pd.read_csv(args.live_csv)
    n_live_rows = len(df_live)

    # 2) Adapter をロードし、必要なら追加結合や派生列生成を行う
    adapter = load_adapter(args.approach)
    df_live = adapter.prepare_live_input(df_live, PROJECT_ROOT)

    # 3) モデル／パイプラインのパスを決定（latest ディレクトリが既定）
    base_dir = PROJECT_ROOT / "models" / args.approach / "latest"
    model_path = Path(args.model) if args.model else (base_dir / "model.pkl")
    pipe_path = Path(args.feature_pipeline) if args.feature_pipeline else (base_dir / "feature_pipeline.pkl")

    # 4) ファイル存在チェック
    if not model_path.exists():
        sys.exit(f"[ERROR] model not found: {model_path}")
    if not pipe_path.exists():
        sys.exit(f"[ERROR] feature_pipeline not found: {pipe_path}")

    # 5) モデル / パイプラインをロード
    if not args.quiet:
        print(f"[INFO] Loading model from {model_path}")
        print(f"[INFO] Loading feature pipeline from {pipe_path}")
    model = joblib.load(model_path)
    pipeline = joblib.load(pipe_path)

    # 6) 特徴量変換（live_row → X）
    X_live = pipeline.transform(df_live)

    # 7) 予測確率を計算（is_top2=1 の確率を抽出）
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X_live)[:, 1]
    else:
        y_hat = model.predict(X_live)
        proba = y_hat.astype("float64", copy=False)

    # 8) 出力データフレーム構築（指定ID列＋予測確率）
    id_cols = [c for c in args.id_cols.split(",") if c in df_live.columns]
    out_df = df_live[id_cols].copy()
    out_df["proba"] = proba

    # 9) 出力先（data/live/<approach>/pred_<approach>_<入力ファイル名>.csv）
    out_dir = PROJECT_ROOT / "data" / "live" / args.approach
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(args.live_csv).stem  # 例: raw_20250915_24_8
    out_path = out_dir / f"pred_{args.approach}_{stem}.csv"

    # 10) CSV を UTF-8-SIG で保存（Excelでも文字化けしにくい）
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    # 11) （任意）使用列情報の抽出〜保存〜ログ出力
    if args.show_features:
        # 使用列情報の抽出（数値列 / カテゴリ列 / 展開後名）
        num_cols, cat_cols, encoded_names = extract_feature_info(pipeline)

        # レポート本文を生成
        report_text = build_features_report_text(
            approach=args.approach,
            stem=stem,
            model_path=model_path,
            pipe_path=pipe_path,
            num_cols=num_cols,
            cat_cols=cat_cols,
            encoded_names=encoded_names,
            n_live_rows=n_live_rows,
        )

        # 使用列情報の保存（テキスト）
        features_txt_path = out_dir / f"features_{stem}.txt"
        try:
            with open(features_txt_path, "w", encoding="utf-8") as f:
                f.write(report_text)
            saved_features_txt = True
        except Exception as e:
            saved_features_txt = False
            if not args.quiet:
                print(f"[WARN] 特徴量一覧の保存に失敗しました: {e}")
    else:
        report_text = ""
        features_txt_path = None
        saved_features_txt = False

    # 12) ログ出力（--quiet で抑止）
    if not args.quiet:
        print(f"[OK] saved predictions: {out_path}")

        # ---- サマリ表示（確率降順）----
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

        # ---- 見やすい列レポート（フラグ指定時のみ / cp932対策付き）----
        if args.show_features and report_text:
            def _safe_print_block(text: str):
                try:
                    print("\n" + text)
                except UnicodeEncodeError:
                    enc = sys.stdout.encoding or "utf-8"
                    safe = text.encode(enc, errors="replace").decode(enc, errors="replace")
                    print("\n" + safe)

            _safe_print_block(report_text)
            if saved_features_txt and features_txt_path:
                print(f"\n[FEATURES] wrote: {features_txt_path}")


if __name__ == "__main__":
    main()
