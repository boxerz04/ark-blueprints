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
├─ data/
│   ├─ html/
│   │   ├─ odds3t/             # 3連単オッズHTML
│   │   ├─ odds2tf/            # 2連単・2連複オッズHTML
│   │   ├─ pay/                # 払戻ページHTML
│   │   └─ raceresult/         # レース結果ページHTML
│   ├─ raw/                    # 日次レースCSV（64列: 63 + section_id）
│   ├─ refund/                 # 払戻金CSV
│   ├─ timeline/               # 直前オッズのタイムラインCSV
│   └─ processed/
│       └─ features_cache/
│           └─ top2pair/
│               └─ <timestamp>/                # 例: 2025-09-13_10-30-00
│                   ├─ top2pair_ids.csv
│                   ├─ top2pair_y.csv
│                   ├─ top2pair_X_dense.npz
│                   └─ features.json           # feature_names を保持
│
├─ logs/
│
├─ notebooks/
│   ├─ preprocess.ipynb
│   └─ features.ipynb
│
├─ scripts/
│   ├─ scrape.py
│   ├─ build_raw_csv.py
│   ├─ build_timeline_live.py
│   ├─ run_odds_scheduler.py
│   ├─ scrape_odds.py
│   ├─ train.py                 # 汎用トレーニング（runs / latest を更新）
│   ├─ build_live_row.py        # 推論用ライブ行生成
│   ├─ predict_one_race.py      # 単発推論
│   └─ train_top2pair.py        # Top2ペア方式の学習（下記 models に出力）
│
├─ src/
│   ├─ __init__.py
│   ├─ data_loader.py
│   ├─ feature_engineering.py
│   ├─ model.py
│   └─ utils.py
│
├─ models/
│   ├─ latest/
│   │   ├─ model.pkl                     # （train.py 系の「現行採用版」）
│   │   ├─ feature_pipeline.pkl
│   │   └─ train_meta.json
│   │
│   ├─ runs/
│   │   └─ <model_id>/                   # 例: 20250913_141256
│   │       ├─ model.pkl
│   │       ├─ feature_pipeline.pkl
│   │       └─ train_meta.json
│   │
│   └─ top2pair/                         # ←（見やすさ用に論理的にまとめる場合の棚）
│       ├─ latest/                       # train_top2pair.py が毎回更新
│       │   ├─ model.pkl
│       │   └─ train_meta.json
│       │
│       └─ runs/
│           └─ <model_id>/               # 例: 20250913_141256
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

## 🚀 使い方

### 1. スクレイピング（HTML保存）

```powershell
# 今日の日付を対象に処理
python scripts/scrape.py

# 日付を指定して処理
python scripts/scrape.py --date 2025-08-27
python scripts/scrape.py --date 20250827
```

👉取得データは `data/html/` 以下に保存されます。保存先フォルダが存在しない場合でも自動作成されます。

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


### 5. 学習（モデル生成 + 評価指標記録）

```powershell
python scripts/train.py --version-tag v1.0.2 --notes "人間予想上位互換モデル"

```
👉 出力:

- `models/runs/<model_id>/model.pkl`
- `models/runs/<model_id>/feature_pipeline.pkl`
- `models/runs/<model_id>/train_meta.json`
- `models/latest/` にもコピー

### 6. 推論（1レース予測）

```powershell
# 事前に公式HTMLを取得して保存
python scripts\scrape_one_race.py --date 20250907 --jcd 19 --race 12

# ライブ用の “raw相当(6行)” を生成（--online で必要HTMLを自動取得＆cache）
python scripts\build_live_row.py --date 20250907 --jcd 19 --race 12 --online --out data\live\raw_20250907_19_12.csv

# 予測（models\latest の model.pkl / feature_pipeline.pkl を使用）
python scripts\predict_one_race.py --live-csv data\live\raw_20250907_19_12.csv --model-dir models\latest

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





