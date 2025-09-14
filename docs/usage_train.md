# 学習フロー

## 1. スクレイピング（HTML保存）

```bash
# 今日の日付を対象に処理
python scripts/scrape.py

# 日付を指定して処理
python scripts/scrape.py --date 2025-08-27
python scripts/scrape.py --date 20250827
```

## 2. CSV生成（raw + refund）

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

## 3. 前処理（master.csv生成 + 例外検知レポート）

```powershell
python scripts/preprocess.py --raw-dir data/raw --out data/processed/master.csv --reports-dir data/processed/reports

```

## 4-1. baseモデルデータセット生成

当面は `notebooks/features.ipynb` を実行する。  
👉出力:

- `data/processed/X.npz`  
- `data/processed/y.csv`  
- `data/processed/ids.csv`  
- `models/latest/feature_pipeline.pkl`

## 4-2. Top2ペアモデルデータセット生成

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


## 5-1. 学習（baseモデル生成 + 評価指標記録）

```powershell
python scripts/train.py --version-tag v1.0.2 --notes "人間予想上位互換モデル"

```
👉 出力:

- `models/runs/<model_id>/model.pkl`
- `models/runs/<model_id>/feature_pipeline.pkl`
- `models/runs/<model_id>/train_meta.json`
- `models/latest/` にもコピー

## 5-2. 学習（Top2ペアモデル生成 + 評価指標記録）

```powershell
python scripts/train_top2pair.py --version-tag v1.0.0 --notes "初回CV学習"
```
👉 出力:

- `models/top2pair/runs/<model_id>/model.pkl`
- `models/top2pair/runs/<model_id>/train_meta.json`
- `models/top2pair/runs/<model_id>/feature_importance.csv`
- `models/top2pair/runs/<model_id>/cv_folds.csv`
- `models/top2pair/latest/` にもコピー
