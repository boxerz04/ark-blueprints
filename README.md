# Ark Blueprints

未完の部品を一つずつ集め、いつか航海に出る箱舟を形にするプロジェクト。⛵

---

## 📝 プロジェクト概要

Ark Blueprints は、ボートレースのデータを **収集・前処理・特徴量生成・機械学習・推論** まで行うことを目指す開発プロジェクトです。
現在は以下の機能が完成しています：

* スクレイピングによるデータ収集（レース情報・オッズ）
* 日次 CSV 生成（raw + refund）
* タイムライン生成（未確定レースの締切予定時刻を取得）
* スケジューラによる直前オッズ収集（準優・優勝戦）

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
│   └─ timeline/            # 直前オッズ収集用のタイムラインCSV
│
├─ logs/                    # 実行ログ（.gitignore 推奨）
│
├─ notebooks/               # Jupyter Notebookでの探索・分析
│
├─ scripts/                 # スクリプト群
│   ├─ scrape.py            # レースデータスクレイピング（HTML保存）
│   ├─ build_raw_csv.py     # HTML → CSV変換 (raw.csv + refund.csv)
│   ├─ build_timeline_live.py # 未確定レースのタイムライン生成
│   ├─ run_odds_scheduler.py  # タイムラインに基づき直前オッズ収集をスケジューリング
│   └─ scrape_odds.py       # オッズスクレイピング（3連単・2連単/複）
│
├─ src/                     # 共通関数・クラス
│   ├─ __init__.py
│   ├─ data_loader.py
│   ├─ feature_engineering.py
│   ├─ model.py
│   └─ utils.py
│
├─ docs/                    # プロジェクトドキュメント
│   ├─ data_dictionary.md   # 64列CSVのカラム仕様
│   └─ design_notes.md      # 設計ノート・分析メモ
│
├─ tests/                   # テストコード
│
├─ requirements.txt         # 必要なライブラリ
├─ README.md                # プロジェクト概要・説明書
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

---

## 🔮 今後の予定

* 前処理スクリプト（preprocess.py）: 欠損値処理、型変換
* 特徴量生成スクリプト（features.py）: ST差、展示データ加工など
* モデル学習スクリプト（train.py）: LightGBM等による学習
* 推論スクリプト（predict.py）: 予測出力

---

## ⚙️ 開発メモ

* Python 3.9 / 3.10 / 3.12 系で動作確認済み
* 必要なライブラリは requirements.txt に記載予定
* 大容量データは Git 管理せず data/ 以下に直接保存
* ログは logs/ 以下に保存（.gitignore 済み）

