# 学習フロー

## 0. 最小構成

```bash
data/
  raw/                # 日次生CSV（スクレイプ→整形）
  processed/
    master.csv        # 前処理済みの全体マスタ
    base/             # base 用の特徴量・ターゲット
    sectional/        # sectional 用の特徴量・ターゲット
    reports/          # 前処理レポート
models/
  base/latest/
  sectional/latest/
```

## 1. スクレイピング（HTML保存）

```bash
# 今日分
python scripts\scrape.py

# 日付指定（例: 2025-09-22）
python scripts\scrape.py --date 2025-09-22
# または 8桁表記
python scripts\scrape.py --date 20250922
```

## 2. CSV生成（raw + refund）

```bash
# スクレイピングで保存した HTML から日次の CSV を生成します
#  今日の日付を対象に処理
python scripts\build_raw_csv.py

# 日付を指定して処理
python scripts\build_raw_csv.py --date 2025-09-22
python scripts\build_raw_csv.py --date 20250922
```
* `data/raw/YYYYMMDD_raw.csv` （64列のレースデータ）
* `data/refund/YYYYMMDD_refund.csv` （払戻金データ）

## 3. 前処理（master.csv生成 + 例外検知レポート）

```powershell
python scripts\preprocess.py --raw-dir data\raw --out data\processed\master.csv --reports-dir data\processed\reports
```
* `data/processed/master.csv` （64列のレースデータ）
* `data/processed/reports/anomalies_report_YYYYMMDD-hhmmss.csv` （ログに除外行・除外レースが出力されます）

## 4. 特徴量生成
## 4-1. Base モデル用
（※ notebooks/features.ipynb は不要になりました）

- ノートブックの処理を scripts/preprocess_base_features.py に置き換えました。
- これ1本で 数値/カテゴリの選抜・標準化・OneHot まで実施し、学習に必要な成果物を吐き出します。
```powershell
python scripts\preprocess_base_features.py ^
  --master data\processed\master.csv ^
  --out-dir data\processed\base ^
  --pipeline-dir models\base\latest
```
👉出力:
```bash
data/processed/base/
  X_dense.npz        # または X.npz（疎行列の場合）
  y.csv
  ids.csv
models/base/latest/
  feature_pipeline.pkl
```
- 読み込む `master.csv` は `--master` で指定
- 出力ディレクトリは `--out-dir` に集約（再学習時は上書き）
- 前処理器（`feature_pipeline.pkl`）は `--pipeline-dir` へ保存（学習・推論で共用）

## 4-2. Sectional モデル用（節間特徴）
### 1.節間結合（master_sectional.csv を作る）
```powershell
python scripts\preprocess_sectional.py --out data\processed\sectional\master_sectional.csv
```
### 2.特徴量化（短期型・列整理済）
```powershell
# 全レースを学習に使う場合
python scripts\features_sectional.py ^
  --in data\processed\sectional\master_sectional.csv ^
  --out-dir data\processed\sectional ^
  --model-dir models\sectional\latest
```
※優勝戦／準優勝戦／準優進出戦【のみ】で学習セットを作る場合は --stage-filter を追加：
```powershell
python scripts\features_sectional.py ^
  --in data\processed\sectional\master_sectional.csv ^
  --out-dir data\processed\sectional ^
  --model-dir models\sectional\latest ^
  --stage-filter "finals,semi,semi-entry"
```
👉出力:
```bash
data/processed/sectional/
  X_dense.npz
  y.csv
  ids.csv
  master_sectional.csv
models/sectional/latest/
  feature_pipeline.pkl
```


## 5.学習（baseモデル生成 + 評価指標記録）
- 学習スクリプトは scripts/train.py を共通使用します。
- --approach に base or sectional を指定してください（既定は base）。
### 5-1.Base モデルの学習
```powershell
python scripts\train.py --approach base --version-tag v1.0.5-base-20250922 --notes "master更新 + base featuresスクリプト化"
```
### 5-2.Sectional モデルの学習
```powershell
python scripts\train.py --approach sectional --version-tag v1.0.5-sectional-20250922 --notes "優勝/準優/準優進出戦に絞った短期モデル (weather除外)"
```
👉 出力:
```perl
models/<approach>/runs/<model_id>/
  model.pkl
  feature_pipeline.pkl
  train_meta.json
models/<approach>/latest/
  model.pkl                 # 上記のシンボリック的な最新版（コピー）
  feature_pipeline.pkl
```
- Base/Sectional ともスクリプトが自動判定します（疎か密か）。
- `train.py` 側は `X.npz`（疎）優先で探す実装です。密の場合は古い `X.npz` を残さないよう注意してください。

## 6.モデルのバージョニング運用
- `--version-tag` と `--notes` はメタ管理用。
- `models/<approach>/runs/<model_id>/train_meta.json` に以下が記録されます：
- - `model_id`, `created_at`, `version_tag`, `notes`, `git_commit`, データ行数や特徴量数、指標（AUC/PR-AUC/Logloss/Accuracy/MCC/Top2Hit など）

## 7.ワンライナー（Windows PowerShell 例）
Base 一気通貫
```powershell
python scripts\preprocess.py --raw-dir data\raw --out data\processed\master.csv --reports-dir data\processed\reports ; `
python scripts\preprocess_base_features.py --master data\processed\master.csv --out-dir data\processed\base --pipeline-dir models\base\latest ; `
python scripts\train.py --approach base --version-tag v1.0.5-base-20250922 --notes "master更新 + base featuresスクリプト化"
```
Sectional（ステージ絞り込み）
```powershell
python scripts\preprocess_sectional.py --out data\processed\sectional\master_sectional.csv ; `
python scripts\features_sectional.py --in data\processed\sectional\master_sectional.csv --out-dir data\processed\sectional --model-dir models\sectional\latest --stage-filter "finals,semi,semi-entry" ; `
python scripts\train.py --approach sectional --version-tag v1.0.5-sectional-20250922 --notes "ステージ絞り込み短期モデル"
```
