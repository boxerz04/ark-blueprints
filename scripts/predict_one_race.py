# scripts/predict_one_race.py
# ----------------------------------------------------
# 単一レース（live_csv）を入力し、指定モデル系列（approach）の最新モデルで予測。
#
# 入力:
#   --live-csv         レース1件分の特徴量CSV（6行など）
#   --approach         使用するモデル系列（例: base, top2pair）
#   --model            モデル pkl を明示指定（未指定なら models/<approach>/latest/model.pkl）
#   --feature-pipeline 特徴量パイプライン pkl を明示指定（通常不要）
#   --id-cols          出力に含めるID列（デフォ: race_id,code,R,wakuban,player）
#   --quiet            進捗と要約の表示を抑止
#
# 出力:
#   data/live/base/pred_<approach>_<入力ファイル名>.csv（UTF-8-SIG）
#   併せて確率降順のサマリ表をコンソール表示（--quiet で抑止）
# ----------------------------------------------------

import argparse
import sys
from pathlib import Path

import joblib
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(description="Predict one race using feature pipeline + model.")
    p.add_argument("--live-csv", required=True, help="予測対象となる1レースの特徴量CSV")
    p.add_argument("--approach", default="base",
                   help="モデル系列（例: base, top2pair, ...）")
    p.add_argument("--model", help="モデル pkl のパス（未指定: models/<approach>/latest/model.pkl）")
    p.add_argument("--feature-pipeline", help="特徴量パイプライン pkl のパス（通常不要）")
    p.add_argument("--id-cols", default="race_id,code,R,wakuban,player",
                   help="出力に含めるID列（カンマ区切り）")
    p.add_argument("--quiet", action="store_true", help="進捗と要約の表示を抑止")
    return p.parse_args()


def main():
    args = parse_args()

    # 1) プロジェクトルート
    ROOT = Path(__file__).resolve().parents[1]

    # 2) 入力読み込み
    df_live = pd.read_csv(args.live_csv)

    # 3) モデル／パイプラインのパス
    base_dir = ROOT / "models" / args.approach / "latest"
    model_path = Path(args.model) if args.model else (base_dir / "model.pkl")
    pipe_path = Path(args.feature_pipeline) if args.feature_pipeline else (base_dir / "feature_pipeline.pkl")

    # 4) 存在チェック
    if not model_path.exists():
        sys.exit(f"[ERROR] model not found: {model_path}")
    if not pipe_path.exists():
        sys.exit(f"[ERROR] feature_pipeline not found: {pipe_path}")

    # 5) ロード
    if not args.quiet:
        print(f"[INFO] Loading model from {model_path}")
    model = joblib.load(model_path)

    if not args.quiet:
        print(f"[INFO] Loading feature pipeline from {pipe_path}")
    pipeline = joblib.load(pipe_path)

    # 6) 特徴量変換
    X_live = pipeline.transform(df_live)

    # 7) 予測確率
    proba = model.predict_proba(X_live)[:, 1]

    # 8) 出力データフレーム
    id_cols = [c for c in args.id_cols.split(",") if c in df_live.columns]
    out_df = df_live[id_cols].copy()
    out_df["proba"] = proba

    # 9) 出力先（data/live/base に固定）＋ UTF-8-SIG
    out_dir = Path("data") / "live" / "base"
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = Path(args.live_csv).stem  # 例: raw_20250913_12_12
    out_path = out_dir / f"pred_{args.approach}_{stem}.csv"
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    if not args.quiet:
        print(f"[OK] saved predictions: {out_path}")

        # ---- コンソール要約（確率降順）----
        show_cols = [c for c in ["race_id", "code", "R", "wakuban", "player", "proba"] if c in out_df.columns]
        summary = out_df.sort_values("proba", ascending=False)[show_cols].reset_index(drop=True)
        with pd.option_context("display.max_rows", 50, "display.width", 120, "display.float_format", "{:,.4f}".format):
            print("\n[SUMMARY] prob(desc):")
            print(summary.to_string(index=False))

        # 上位2艇の簡易表示
        if "wakuban" in summary.columns:
            top2 = summary.head(2)[["wakuban", "proba"]].to_records(index=False)
            pair = " - ".join([f"{int(w)}({p:.3f})" for w, p in top2])
            print(f"\n[TOP2] {pair}")


if __name__ == "__main__":
    main()
