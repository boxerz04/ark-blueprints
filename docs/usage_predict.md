# Ark Blueprints – レース予測の使い方

このドキュメントでは、1レース単位でスクレイピング→CSV生成→推論までを行う手順を解説します。

## 実行フロー
1. scrape_one_race.py
2. build_live_row.py
3. predict_one_race.py
4. Notebook（ArkRaceRunner.ipynb）GUI実行

# 🚤 Ark Blueprints – 1レース予測ワークフロー

## 1. 準備
- Anaconda Prompt からプロジェクトに移動
  ```bash
  cd C:\Users\user\Desktop\Git\ark-blueprints
* モデルと前処理パイプラインを最新にしておく
  （`train.py` / `build_feature_pipeline.py` を適宜実行）

## 2. スクレイピング（HTML収集）
- 1レース分のHTMLを保存
  ```bash
  python scripts\scrape_one_race.py --date YYYYMMDD --jcd CC --race R
* `YYYYMMDD`: 開催日
* `CC`: 場コード（例: 15）
* `R`: レース番号（1–12）

## 3. ライブCSV生成
- スクレイピングしたHTMLから学習用と同じ形式のCSVを作成
  ```bash
  python scripts\build_live_row.py --date YYYYMMDD --jcd CC --race R --online --out data\live\raw_YYYYMMDD_CC_RR.csv
* `--online` を付けると最新のHTMLを直接取得
* 出力は `data/live/raw_*.csv`

## 4. 推論（予測）
- 保存済みモデルで予測
  ```bash
  python scripts\predict_one_race.py --live-csv data\live\raw_YYYYMMDD_CC_RR.csv --model-dir models\latest
* 出力は `data/live/pred_*.csv`
* カラム例:
    `prediction`: 0=圏外, 1=2連対内
    `proba_1`: 2連対内確率
    `proba_0`: 圏外確率

## 5. 予想の読み方
- `proba_1` が高い選手ほど 連対期待度が高い
- しきい値は 0.5（50%）で区切られるが、実際は順位相対比較で解釈する
- 例:1号艇 0.77 → 本命 ◎2号艇 0.55 → 対抗 ○3号艇以下 0.05 未満 → 3着候補 △
