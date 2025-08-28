# Ark Blueprints
未完の部品を一つずつ集め、いつか航海に出る箱舟を形にするプロジェクト。⛵

---

## 📝 プロジェクト概要
Ark Blueprints は、ボートレースのデータを **収集・前処理・特徴量生成・機械学習・推論** まで行うことを目指す開発プロジェクトです。  
まずはスクレイピングと日次 CSV 生成の自動化まで完成しました。

---

## 📂 ディレクトリ構造

```text
ark-blueprints/
│
├─ data/                # データ格納（.gitignore推奨）
│   ├─ html/            # スクレイピングで取得したHTML(.bin)
│   ├─ raw/             # 日次レースデータCSV (63列)
│   └─ refund/          # 払戻金データCSV
│
├─ notebooks/           # Jupyter Notebookでの探索・分析
│
├─ scripts/             # スクリプト群
│   ├─ scrape.py        # スクレイピング（HTML保存）
│   └─ build_raw_csv.py # bin → CSV変換 (raw.csv + refund.csv)
│
├─ src/                 # 共通関数・クラス（今後追加予定）
│   ├─ __init__.py
│   ├─ data_loader.py
│   ├─ feature_engineering.py
│   ├─ model.py
│   └─ utils.py
│
├─ tests/               # テストコード
│
├─ requirements.txt     # 必要なライブラリ
├─ README.md            # プロジェクト概要・説明書
└─ .gitignore
🚀 使い方
1. スクレイピング（HTML保存）
powershell
コードをコピーする
# 今日の日付を対象に処理
python scripts/scrape.py

# 日付を指定して処理
python scripts/scrape.py --date 2025-08-27
python scripts/scrape.py --date 20250827
取得データは data/html/ 以下に .bin 形式で保存されます

保存先フォルダが存在しない場合でも、自動的に作成されます

2. CSV生成（raw + refund）
スクレイピングで保存した HTML (.bin) から日次の CSV を生成します。
出力されるのは以下の2種類です：

data/raw/YYYYMMDD_raw.csv（63列のレースデータ）

data/refund/YYYYMMDD_refund.csv（払戻金データ）

powershell
コードをコピーする
# 今日の日付を対象に処理
python scripts/build_raw_csv.py

# 日付を指定して処理
python scripts/build_raw_csv.py --date 2025-08-27
python scripts/build_raw_csv.py --date 20250827
🔮 今後の予定
前処理スクリプト（preprocess.py）: 欠損値処理、型変換

特徴量生成スクリプト（features.py）: ST差、展示データ加工など

モデル学習スクリプト（train.py）: LightGBM等による学習

推論スクリプト（predict.py）: 予測出力

⚙️ 開発メモ
Python 3.9 / 3.10 / 3.12 系で動作確認済み

必要なライブラリは requirements.txt に記載予定

大容量データは Git 管理せず data/ 以下に直接保存
