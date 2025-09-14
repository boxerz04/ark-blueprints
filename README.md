# Ark Blueprints

未完の部品を一つずつ集め、いつか航海に出る箱舟を形にするプロジェクト。⛵

---

## 📝 プロジェクト概要

Ark Blueprints は、ボートレースのデータを **収集 → 前処理 → 特徴量生成 → 機械学習 → 推論** まで行うことを目指す開発プロジェクトです。  
現在は以下の機能が完成しています：

* スクレイピングによるデータ収集（レース情報・オッズ）
* 日次 CSV 生成（raw + refund）
* タイムライン生成（未確定レースの締切予定時刻を取得）
* スケジューラによる直前オッズ収集（準優・優勝戦）
* 前処理（欠損値処理・型変換・失格レース除外・ST/ST展示変換）
* 特徴量生成（数値/カテゴリ列の選定、OneHot + 標準化）
* LightGBM による初回学習・モデル保存

---

## 📂 ディレクトリ構造

```text
ark-blueprints/
│
├─ data/                         # データ格納（.gitignore 推奨）
│   ├─ html/                     # スクレイピング取得HTML
│   │   ├─ odds3t/               # 3連単オッズHTML
│   │   ├─ odds2tf/              # 2連単・2連複オッズHTML
│   │   ├─ pay/                  # 払戻ページHTML
│   │   └─ raceresult/           # レース結果ページHTML
│   ├─ raw/                      # 日次レースCSV（64列: 63 + section_id）
│   ├─ refund/                   # 払戻金CSV
│   ├─ timeline/                 # 直前オッズタイムラインCSV
│   └─ processed/                # 前処理・特徴量・ラベル等の成果物
│       ├─ master.csv            # 全レース統合（基礎）
│       ├─ X_base.npz / y.csv    # baseモデル用の特徴量・ラベル
│       ├─ X_top2pair_dense.npz  # top2pairモデル用の特徴量
│       ├─ y_top2pair.csv
│       └─ ids_top2pair.csv
│
├─ logs/
│
├─ notebooks/
│   ├─ preprocess.ipynb          # 前処理フロー検証
│   └─ features.ipynb            # 特徴量検証
│
├─ scripts/
│   ├─ scrape.py
│   ├─ build_raw_csv.py
│   ├─ build_timeline_live.py
│   ├─ run_odds_scheduler.py
│   ├─ scrape_odds.py
│   ├─ build_feature_pipeline.py # baseモデル用 前処理器生成
│   ├─ train.py                  # baseモデル学習（runs / latest 更新）
│   ├─ build_live_row.py         # 推論用ライブ行生成
│   ├─ predict_one_race.py       # 単発推論（baseモデル）
│   ├─ build_top2pair_dataset.py # top2pair用 データセット生成
│   ├─ train_top2pair.py         # top2pairモデル学習
│   └─ predict_top2pair.py       # top2pairモデル推論
│
├─ src/
│   ├─ __init__.py
│   ├─ data_loader.py
│   ├─ feature_engineering.py
│   ├─ model.py
│   ├─ model_utils.py            # 共通: 保存・ロード・ID生成
│   └─ utils.py
│
├─ models/
│   ├─ base/                     # baseモデル系
│   │   ├─ latest/
│   │   │   ├─ model.pkl
│   │   │   ├─ feature_pipeline.pkl
│   │   │   └─ train_meta.json
│   │   └─ runs/
│   │       └─ <model_id>/       # 例: 20250913_141256
│   │           ├─ model.pkl
│   │           ├─ feature_pipeline.pkl
│   │           └─ train_meta.json
│   │
│   └─ top2pair/                 # top2ペア方式モデル
│       ├─ latest/
│       │   ├─ model.pkl
│       │   └─ train_meta.json
│       └─ runs/
│           └─ <model_id>/
│               ├─ model.pkl
│               ├─ train_meta.json
│               ├─ feature_importance.csv
│               └─ cv_folds.csv
│
├─ docs/
│   ├─ data_dictionary.md
│   └─ design_notes.md
│
├─ tests/
│
├─ requirements.txt
├─ README.md
└─ .gitignore
```

---

# baseモデルの使い方

## 学習フロー

### 1. スクレイピング（HTML保存）

```bash
# 今日の日付を対象に処理
python scripts/scrape.py

# 日付を指定して処理
python scripts/scrape.py --date 2025-08-27
python scripts/scrape.py --date 20250827
```

### 2. CSV生成（raw + refund）

スクレイピングで保存した HTML から日次の CSV を生成します。

* `data/raw/YYYYMMDD_raw.csv` （64列のレースデータ）
* `data/refund/YYYYMMDD_refund.csv` （払戻金データ）

```powershell
# 今日の日付を対象に処理
python scripts/build_raw_csv.py

# 日付を指定して処理
python scripts/build_raw_csv.py --date 2025-08-27
python scripts/build_raw_csv.py --date 20250827
```

### 3. 前処理（master.csv生成 + 例外検知レポート）

```powershell
python scripts/preprocess.py --raw-dir data/raw --out data/processed/master.csv --reports-dir data/processed/reports

```

### 4. 特徴量生成

当面は `notebooks/features.ipynb` を実行する。  
👉出力:

- `data/processed/X.npz`  
- `data/processed/y.csv`  
- `data/processed/ids.csv`  
- `models/latest/feature_pipeline.pkl`


### 5-1. 学習（baseモデル生成 + 評価指標記録）

```powershell
python scripts/train.py --version-tag v1.0.2 --notes "人間予想上位互換モデル"

```
👉 出力:

- `models/runs/<model_id>/model.pkl`
- `models/runs/<model_id>/feature_pipeline.pkl`
- `models/runs/<model_id>/train_meta.json`
- `models/latest/` にもコピー

# Top2ペア方式モデルの使い方

## データセット生成

`master.csv` から Top2ペア学習用データを作成します。

```bash
# デフォルトで data/processed/master.csv を読み込み
# 成果物は data/processed/ に保存されます
python scripts/build_top2pair_dataset.py
```
👉 出力:

- `data/processed/X_top2pair_dense.npz`
- `data/processed/y_top2pair.csv`
- `data/processed/ids.csv`
- `data/processed/features_top2pair.json`

### 5-2. 学習（Top2ペアモデル生成 + 評価指標記録）

```powershell
python scripts/train_top2pair.py --version-tag v1.0.0 --notes "初回CV学習"
```
👉 出力:

- `models/top2pair/runs/<model_id>/model.pkl`
- `models/top2pair/runs/<model_id>/train_meta.json`
- `models/top2pair/runs/<model_id>/feature_importance.csv`
- `models/top2pair/runs/<model_id>/cv_folds.csv`
- `models/top2pair/latest/` にもコピー

# 推論フロー（1レース予測）

## 1) 1レースをスクレイピング（live/html に保存）
```powershell
python scripts\scrape_one_race.py --date 20250913 --jcd 12 --race 12
```
※ 取得HTMLは data/live/html/<kind>/... に .bin で保存され、raceresult は保存しません。
## 2) ライブ6行CSVの生成（直前で取得したHTMLをそのまま利用）
```powershell
python scripts\build_live_row.py --date 20250913 --jcd 12 --race 12 --out data\live\raw_20250913_12_12.csv
```
※ヒント：ここで --online は不要です（手順1のHTMLキャッシュを使います）。必要なら --online でも可。
## 3) Base モデルで単発推論（models/base/latest を使用）
```powershell
python scripts\predict_one_race.py --live-csv data\live\raw_20250913_12_12.csv --model-dir models\base\latest
```
## 4) Top2ペア モデルでペア推論（models/top2pair/latest を使用）
```powershell
python scripts\predict_top2pair.py --mode live --master data\live\raw_20250913_12_12.csv --race-id 202509131212
```

---
## 🕒 別途：直前オッズ収集フロー
- タイムライン生成

未確定レースの締切予定時刻を取得して CSV を生成します。

```powershell
python scripts/build_timeline_live.py --date 20250901
```

👉 `data/timeline/20250901_timeline_live.csv` が生成されます。

- スケジューラで直前オッズを収集

👉 締切5分前に scrape_odds.py が実行され、準優進出戦・準優勝戦・優勝戦のオッズを保存します。

```powershell
python scripts/run_odds_scheduler.py --timeline data/timeline/20250901_timeline_live.csv
```


---

## 🔮 今後の予定

* 推論スクリプト（predict.py）: 予測出力
* 特徴量エンジニアリング（例: ST差・艇ごとの比較特徴）
* 時系列検証の強化
* モデルチューニング（LightGBMパラメータ最適化）

---

## ⚙️ 開発メモ

* Python 3.9 / 3.10 / 3.12 系で動作確認済み
* 必要なライブラリは requirements.txt に記載予定
* 大容量データは Git 管理せず data/ 以下に直接保存
* ログは logs/ 以下に保存（.gitignore 済み）






