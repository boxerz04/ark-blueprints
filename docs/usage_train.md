# 学習フロー（最新版）

本ドキュメントは **master 生成 → course 履歴付与 → 特徴量化（base/sectional）→ 学習** の最新フローをまとめたものです。現在の標準手順は **`preprocess_course.py` を実行してから `preprocess_base_features.py`** の順です。

---

## 0. 最小構成

```bash
data/
  raw/                # 日次生CSV（スクレイプ→整形）
  processed/
    master.csv        # 前処理済みの全体マスタ（prior入り）
    base/             # base 用の特徴量・ターゲット
    sectional/        # sectional 用の特徴量・ターゲット
    reports/          # 前処理レポート
models/
  base/latest/
  sectional/latest/
```

---

## 1. スクレイピング（HTML保存）

```powershell
# 今日分
python scripts\scrape.py

# 日付指定（例: 2025-09-22）
python scripts\scrape.py --date 2025-09-22
# 8桁表記でも可
python scripts\scrape.py --date 20250922
```

---

## 2. CSV生成（raw + refund）

```powershell
# 保存済みHTMLから日次CSVを作成
python scripts\build_raw_csv.py             # 今日
python scripts\build_raw_csv.py --date 2025-09-22
python scripts\build_raw_csv.py --date 20250922
```
生成物:
- `data/raw/YYYYMMDD_raw.csv`（レースデータ）
- `data/refund/YYYYMMDD_refund.csv`（払戻金）

---

## 3. 前処理（master.csv 生成 + 期間指定 + レポート）

```powershell
# 全期間で master.csv を作成（prior 結合を含む）
python scripts\preprocess.py --raw-dir data\raw --out data\processed\master.csv --reports-dir data\processed\reports

# 期間を指定（start/end は当日を含む・inclusive）
python scripts\preprocess.py ^
  --raw-dir data\raw ^
  --out data\processed\master.csv ^
  --reports-dir data\processed\reports ^
  --start-date 2025-05-21 --end-date 2025-09-21
```
出力と補足:
- `data/processed/master.csv` … 前処理済みマスタ（**prior 結合済み**）
- `<reports-dir>/anomalies_report_*.csv` … 異常値スキャン
- `<reports-dir>/excluded_races*.{csv,txt}` … 除外レースの内訳
- `<reports-dir>/master_run_*.txt` … 実行メタ（期間・行数など）
- prior 参照先は既定で `data/priors`（変更は `--priors-root`）。

---

## 4. 特徴量生成（**順序に注意：先に Course、次に Base**）

### 4-1. Course モデル用（コース別履歴特徴を master に上書き付与）
**目的**: 除外“前”の `data/raw` を用いて、選手×entry／×wakuban の直前 **N** 走の着別率・ST統計を**リークなく**作成し、**master.csv に上書き**します。

**実行（例：対象期間 2025-05-21〜2025-09-21、N=10、助走=180日）**
```powershell
python scripts\preprocess_course.py ^
  --master data\processed\master.csv ^
  --raw-dir data\raw ^
  --out data\processed\master.csv ^            # 上書き
  --reports-dir data\processed\course_meta ^
  --start-date 2025-05-21 --end-date 2025-09-21 ^
  --warmup-days 180 ^
  --n-last 10
```
出力: `data/processed/master.csv`（以下の列が**追加**された状態）
```
# entry 基準（直前N走）
finish1_rate_last10_entry, finish1_cnt_last10_entry
finish2_rate_last10_entry, finish2_cnt_last10_entry
finish3_rate_last10_entry, finish3_cnt_last10_entry
st_mean_last10_entry, st_std_last10_entry

# wakuban 基準（直前N走）
finish1_rate_last10_waku, finish1_cnt_last10_waku
finish2_rate_last10_waku, finish2_cnt_last10_waku
finish3_rate_last10_waku, finish3_cnt_last10_waku
st_mean_last10_waku, st_std_last10_waku

# 検証用（特徴には使わない）
finish1_flag_cur, finish2_flag_cur, finish3_flag_cur
```
メモ:
- `--warmup-days` は分母確保のために **開始日の過去まで読む助走**。N を増やす場合は大きめ（例：365）推奨。
- 集計は `groupby(player_id, entry|wakuban) → shift(1) → rolling(N)` で **当該レース除外**を保証。

---

### 4-2. Base モデル用（特徴量・前処理器の作成）
`preprocess_base_features.py` は master.csv を読み、数値/カテゴリの選択・標準化・OneHot などを行い、**学習に必要な X/y/ids と `feature_pipeline.pkl`** を出力します。

```powershell
python scripts\preprocess_base_features.py ^
  --master data\processed\master.csv ^
  --out-dir data\processed\base ^
  --pipeline-dir models\base\latest
```
出力:
```
data/processed/base/
  X_dense.npz or X.npz   # 疎密は自動判定（疎優先）
  y.csv
  ids.csv
models/base/latest/
  feature_pipeline.pkl
```

---

### 4-3. Sectional モデル用（節間特徴）［任意］
1) 節間 master の作成  
```powershell
python scripts\preprocess_sectional.py --out data\processed\sectional\master_sectional.csv
```
2) 特徴量化（フィルタ任意）  
```powershell
python scripts\features_sectional.py ^
  --in data\processed\sectional\master_sectional.csv ^
  --out-dir data\processed\sectional ^
  --model-dir models\sectional\latest ^
  --stage-filter "finals,semi,semi-entry"   # 任意
```
出力:
```
data/processed/sectional/{X_dense.npz,y.csv,ids.csv,master_sectional.csv}
models/sectional/latest/feature_pipeline.pkl
```

---

## 5. 学習（train.py）
`--approach` に `base` か `sectional` を指定。成果物と評価は `models/<approach>/runs/<model_id>/` に保存、`latest/` にもコピーされます。

### 5-1. Base
```powershell
python scripts\train.py --approach base --version-tag v1.2.2-base-20251020 --notes "course上書き込み + base features"
```

### 5-2. Sectional
```powershell
python scripts\train.py --approach sectional --version-tag v1.0.5-sectional-20250922 --notes "ステージ絞り込み短期モデル"
```

出力（共通）:
```
models/<approach>/runs/<model_id>/ {model.pkl, feature_pipeline.pkl, train_meta.json}
models/<approach>/latest/          {model.pkl, feature_pipeline.pkl}
```

---

## 6. モデルのメタ・バージョニング
- `train_meta.json` には `model_id`, `created_at`, `version_tag`, `notes`, `git_commit`, データ件数/特徴量数、指標（AUC/PR-AUC/Logloss/Accuracy/MCC/Top2Hit 等）が記録されます。
- `--version-tag` と `--notes` は必ず明記して履歴を管理。

---

## 7. ワンライナー（Windows PowerShell 例）
### Base を一気通貫（**Course → Base** の順）
```powershell
python scripts\preprocess.py --raw-dir data\raw --out data\processed\master.csv --reports-dir data\processed\reports ; `
python scripts\preprocess_course.py --master data\processed\master.csv --raw-dir data\raw --out data\processed\master.csv --reports-dir data\processed\course_meta --start-date 2025-05-21 --end-date 2025-09-21 --warmup-days 180 --n-last 10 ; `
python scripts\preprocess_base_features.py --master data\processed\master.csv --out-dir data\processed\base --pipeline-dir models\base\latest ; `
python scripts\train.py --approach base --version-tag v1.2.2-base-20251020 --notes "course上書き込み + base features"
```

### Sectional だけを学習
```powershell
python scripts\preprocess_sectional.py --out data\processed\sectional\master_sectional.csv ; `
python scripts\features_sectional.py --in data\processed\sectional\master_sectional.csv --out-dir data\processed\sectional --model-dir models\sectional\latest --stage-filter "finals,semi,semi-entry" ; `
python scripts\train.py --approach sectional --version-tag v1.0.5-sectional-20250922 --notes "ステージ絞り込み短期モデル"
```

---

## 8. 備考（構成方針）
- **prior は preprocess.py（学習）と adapter（推論）で同等の列名**を使用します（`prior_` の接頭辞は付けない）。
- **履歴系の新特徴**は `preprocess_course.py` と同様に“上書き付与”スクリプトを別ファイルで増やす設計が拡張に強いです（学習・推論で同じ順番で適用）。
- `models/<approach>/latest/feature_pipeline.pkl` は推論でも使われるため、**モデルと前処理器のバージョン整合**に注意してください。
