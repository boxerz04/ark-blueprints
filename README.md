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
├─ data/                    # データ格納（.gitignore 推奨）
│   ├─ html/                # スクレイピングで取得したHTML(.html)
│   │   ├─ odds3t/          # 3連単オッズ
│   │   ├─ odds2tf/         # 2連単・2連複オッズ
│   │   ├─ pay/             # 払戻ページHTML
│   │   └─ raceresult/      # レース結果ページHTML
│   ├─ raw/                 # 日次レースデータCSV (64列: 基本63列 + section_id)
│   ├─ refund/              # 払戻金データCSV
│   ├─ timeline/            # 直前オッズ収集用のタイムラインCSV
│   └─ processed/           # 前処理・特徴量・ラベルなどの成果物
│
├─ logs/                    # 実行ログ（.gitignore 推奨）
│
├─ notebooks/               # Jupyter Notebookでの探索・分析
│   ├─ preprocess.ipynb     # 前処理検証
│   └─ features.ipynb       # 特徴量検証
│
├─ scripts/                 # スクリプト群
│   ├─ scrape.py
│   ├─ build_raw_csv.py
│   ├─ build_timeline_live.py
│   ├─ run_odds_scheduler.py
│   ├─ scrape_odds.py
│   └─ train.py             # 学習スクリプト（model.pkl 保存）
│
├─ src/                     # 共通関数・クラス
│   ├─ __init__.py
│   ├─ data_loader.py
│   ├─ feature_engineering.py
│   ├─ model.py
│   └─ utils.py
│
├─ models/                  # 保存済みモデル
│   └─ latest/
│       ├─ model.pkl
│       ├─ feature_pipeline.pkl
│       └─ train_meta.json
│
├─ docs/                    # プロジェクトドキュメント
│   ├─ data_dictionary.md
│   └─ design_notes.md
│
├─ tests/                   # テストコード
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

### 3. タイムライン生成

未確定レースの締切予定時刻を取得して CSV を生成します。

```powershell
python scripts/build_timeline_live.py --date 20250901
```

👉 `data/timeline/20250901_timeline_live.csv` が生成されます。

### 4. スケジューラで直前オッズを収集

👉 締切5分前に scrape_odds.py が実行され、準優進出戦・準優勝戦・優勝戦のオッズを保存します。

```powershell
python scripts/run_odds_scheduler.py --timeline data/timeline/20250901_timeline_live.csv
```

### 5. 前処理

JupyterLab で以下を実行し、`data/processed/master.csv` を生成します:
```powershell
notebooks/preprocess.ipynb
```

### 6. 特徴量生成

JupyterLab で以下を実行し、成果物を保存します:
```powershell
notebooks/features.ipynb
```
出力:
* `data/processed/X.npz`, `y.csv`, `ids.csv`
* `models/latest/feature_pipeline.pkl`

### 7. 学習

スクリプトで学習を実行します:
```powershell
python scripts/train.py
```
出力:
* `models/latest/model.pkl`
* `models/latest/train_meta.json`
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


