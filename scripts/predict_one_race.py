# scripts/predict_one_race.py
# ----------------------------------------------------
# 単一レース（live_csv）を入力し、指定モデル系列（approach）の最新モデルで予測。
#
# 特徴:
# - live_csv（例: build_live_row.py が生成した 6行DataFrame）を入力
# - モデル系列（--approach base / sectional / top2pair ...）を切り替え可能
# - Adapter方式：モデルごとの追加処理を src/adapters/*.py に分離
# - 既定では models/<approach>/latest/ の model.pkl / feature_pipeline.pkl を使用
# - 出力は data/live/<approach>/pred_<approach>_<入力ファイル名>.csv
# - コンソールには予測結果のサマリを表示（--quiet で抑止）
#
# 想定する使い方:
#   python scripts/predict_one_race.py --live-csv data/live/raw_20250915_19_12.csv --approach base
#   python scripts/predict_one_race.py --live-csv data/live/raw_20250915_24_8.csv  --approach sectional
# ----------------------------------------------------

from __future__ import annotations

import argparse
import sys
from importlib import import_module
from pathlib import Path

import joblib
import pandas as pd


# ===== 重要：プロジェクトルートを sys.path に追加（src パッケージを確実に import するため）=====
# - ユーザーがどのカレントディレクトリで実行しても "src.adapters.xxx" を import できるようにする。
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    """コマンドライン引数を定義・解釈する"""
    p = argparse.ArgumentParser(description="Predict one race using feature pipeline + model (Adapter runtime).")
    p.add_argument(
        "--live-csv",
        required=True,
        help="予測対象となる1レースの特徴量CSV（例: build_live_row.py の出力 / 6行）",
    )
    p.add_argument(
        "--approach",
        default="base",
        help="モデル系列（例: base, sectional, top2pair）",
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


def main():
    args = parse_args()

    # 1) live_csv を読み込み（1レース分 / 6行）
    #    - build_live_row.py の出力を想定
    df_live = pd.read_csv(args.live_csv)

    # 2) Adapter をロードし、必要なら追加結合や派生列生成を行う
    #    - base: そのまま素通し
    #    - sectional: raceinfo日次CSVを JOIN + 派生2列（ST_previous_time_num / race_ct_clip6）など
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
    #    - ColumnTransformer が学習時と同じ列名で選択するため、
    #      Adapter 側で「学習時に必要な列」を用意しておくことが重要。
    X_live = pipeline.transform(df_live)

    # 7) 予測確率を計算（is_top2=1 の確率を抽出）
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X_live)[:, 1]
    else:
        # LightGBM の一部設定など、predict_proba が無い場合のフォールバック
        y_hat = model.predict(X_live)
        # すでに確率が返るモデル想定。確率でない場合は 0/1 を float に変換。
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

    # 11) ログ出力（--quiet で抑止）
    if not args.quiet:
        print(f"[OK] saved predictions: {out_path}")

        # ---- サマリ表示（確率降順）----
        show_cols = [c for c in ["race_id", "code", "R", "wakuban", "player", "proba"] if c in out_df.columns]
        summary = out_df.sort_values("proba", ascending=False)[show_cols].reset_index(drop=True)

        # 見やすく整形して表示
        with pd.option_context("display.max_rows", 50,
                               "display.width", 120,
                               "display.float_format", "{:,.4f}".format):
            print("\n[SUMMARY] prob(desc):")
            print(summary.to_string(index=False))

        # 上位2艇だけ簡易表示（GUI側の拾いにも使える）
        if "wakuban" in summary.columns:
            top2 = summary.head(2)[["wakuban", "proba"]].to_records(index=False)
            pair = " - ".join([f"{int(w)}({p:.3f})" for w, p in top2])
            print(f"\n[TOP2] {pair}")


if __name__ == "__main__":
    main()
