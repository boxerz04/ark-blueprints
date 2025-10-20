# USAGE v1.3（base一本化 / prior=adapter / course→sectional上書き）

v1.3 系の**学習・推論フロー**の最新手順まとめ。ポイントは次の3点：

- **prior は adapter（推論時）で結合**：列名は学習時と同じ“素の名前”（例: `N_winning_rate`）。  
- **course → sectional を master / live に“上書き付与”**：学習・推論で同じスキーマに揃える。  
- **学習は base 一本化**：将来はアダプタや列選別で枝分かれ予定。現状は base のみ運用。

---

## ディレクトリ最小構成

```
data/
  raw/                     # 日次 raw CSV
  priors/                  # prior の latest.csv 群
  processed/
    master.csv             # prior+course+sectional 付与済み
    reports/               # 前処理レポート
    course_meta/           # course 付与ログ
    sectional_meta/        # sectional 付与ログ（必要に応じて）
    raceinfo/              # 学習時のsectional参照CSV（任意）
  live/
    html/                  # racelist{race_id}.bin/.html など
    base/                  # 予測CSV出力先
    _debug_merged__final.csv
    debug_sectional_join.csv
models/
  base/
    latest/
      model.pkl
      feature_pipeline.pkl
    runs/<model_id>/
scripts/
  preprocess.py
  preprocess_course.py
  preprocess_sectional.py  # v1.3: 必須10列のみ上書き付与（派生列なし）
  preprocess_base_features.py
  train.py
  scrape_one_race.py
  build_live_row.py
  predict_one_race.py
src/
  base.py                  # adapter（prior結合）
  raceinfo_features.py     # racelist(.bin/.html) → 今節スナップショット抽出
```

---

## 学習フロー

### 1) master → course → sectional（PS1で一括）
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\batch\build_master_range.ps1 `
  -StartDate 20241201 -EndDate 20250930 -WarmupDays 180 -NLast 10
```

- 出力: `data\processed\master.csv`  
  （**prior + course + sectional** が上書き付与済み）

### 2) 特徴量 → 学習（base）
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\batch\train_base_from_master.ps1 `
  -VersionTag 'v1.3.0-base-20251020' `
  -Notes 'prior+course+sectional 全部盛り（sectional採用10列）'
```

> 備考  
> - `preprocess_sectional.py` は **必須10列のみ**を出力（派生 `ST_previous_time_num` / `race_ct_clip6` は出しません）。  
> - `preprocess_base_features.py` が採用列を制御するため、列が増えてもパイプラインで吸収されます。

---

## 推論フロー

**スクレイプ → live生成 → course付与 → sectional付与 → 予測(base)**

### GUI 内部フロー（追加点）
- `preprocess_course.py` の**直後**に  
  `preprocess_sectional.py --master <live.csv> --date <YYYYMMDD> --live-html-root data\live\html --out <live.csv>`  
  を **1行追加**（これで「全部盛り」スキーマに揃う）

### CLI 例（1レース）
```powershell
# 1) スクレイプ（racelist*.bin を保存）
python scripts\scrape_one_race.py --date 20251020 --jcd 12 --race 11

# 2) ライブ6行CSV
python scripts\build_live_row.py --date 20251020 --jcd 12 --race 11 --out data\live\raw_20251020_12_11.csv

# 3) course付与（助走=180, N=10）
python scripts\preprocess_course.py `
  --start-date 2025-10-20 --end-date 2025-10-20 `
  --warmup-days 180 --n-last 10 `
  --master data\live\raw_20251020_12_11.csv `
  --raw-dir data\raw `
  --out data\live\raw_20251020_12_11.csv `
  --reports-dir data\processed\course_meta_live

# 4) sectional付与（HTML→raceinfo_features→JOIN）
python scripts\preprocess_sectional.py `
  --master data\live\raw_20251020_12_11.csv `
  --date 20251020 `
  --live-html-root data\live\html `
  --out data\live\raw_20251020_12_11.csv

# 5) 予測（base, priorはadapterが結合）
python scripts\predict_one_race.py `
  --live-csv data\live\raw_20251020_12_11.csv `
  --approach base `
  --model-dir models\base\latest
```

---

## v1.3 の仕様（重要）

- **prior は推論時に adapter（`src/base.py`）が結合**  
  → 学習時と同じ列名で扱う（`prior_` 接頭辞は使わない方針）。

- **sectional（推論）は必須10列のみを上書き付与**（`scripts/preprocess_sectional.py` 経由）  
  ```
  ST_mean_current, ST_rank_current, ST_previous_time,
  score, score_rate,
  ranking_point_sum, ranking_point_rate,
  condition_point_sum, condition_point_rate,
  race_ct_current
  ```
  - racelist（`data/live/html/racelist{race_id}.bin/.html`）を自動探索  
  - 解析失敗時は **安全フォールバック（NaN→0.0）**

- **学習側（PS1）**でも `course → sectional` を適用し、**同じスキーマ**の master.csv を生成。

---

## トラブルシュート

### A. sectional 10列が全部 0.0
- `raceinfo_features not found` が出る → **import 失敗**  
  - `preprocess_sectional.py` 冒頭で `sys.path` に repo ルートを追加済み（v1.3反映）。  
- `racelist not found` → **HTML未保存**または探索パスのズレ  
  - `--live-html-root data\live\html` を明示。`racelist/` サブフォルダも自動探索。  
- JOIN ミス → `data/live/debug_sectional_join.csv` を確認  
  - `race_id` / `player_id` の整合／`join hits` ログを確認。

### B. 推論で「列が足りない」
- `models/base/latest/feature_pipeline.pkl` の版と整合しているか確認。  
- デバッグ：`predict_one_race.py` 内で `pipeline.get_feature_names_out()` と照合（開発時のみ）。

---

## 変更履歴（v1.2.x → v1.3.0）

- `preprocess_sectional.py`：**HTML→parse→points→['player_id','race_id'] JOIN** に集約。  
- **派生列の出力を廃止**（`ST_previous_time_num`, `race_ct_clip6` は作らない）。  
- 学習・推論ともに **必須10列のみ**数値化・0埋めで統一。  
- GUI フローに **sectional付与**（course直後）を1行追加。  
- モデルは **base 一本化**（従来 sectional 単独モデルは棚上げ。将来はアダプタ/列選別で分岐予定）。

---

## よく使うワンライナー

### 学習（期間同じで再現）
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\batch\build_master_range.ps1 `
  -StartDate 20241201 -EndDate 20250930 -WarmupDays 180 -NLast 10 ; `
powershell -NoProfile -ExecutionPolicy Bypass -File .\batch\train_base_from_master.ps1 `
  -VersionTag 'v1.3.0-base-20251020' `
  -Notes 'prior+course+sectional 全部盛り（sectional採用10列）'
```

### 推論（1レース）
```powershell
python scripts\scrape_one_race.py --date 20251020 --jcd 12 --race 11 ; `
python scripts\build_live_row.py --date 20251020 --jcd 12 --race 11 --out data\live\raw_20251020_12_11.csv ; `
python scripts\preprocess_course.py --start-date 2025-10-20 --end-date 2025-10-20 --warmup-days 180 --n-last 10 --master data\live\raw_20251020_12_11.csv --raw-dir data\raw --out data\live\raw_20251020_12_11.csv --reports-dir data\processed\course_meta_live ; `
python scripts\preprocess_sectional.py --master data\live\raw_20251020_12_11.csv --date 20251020 --live-html-root data\live\html --out data\live\raw_20251020_12_11.csv ; `
python scripts\predict_one_race.py --live-csv data\live\raw_20251020_12_11.csv --approach base --model-dir models\base\latest
```
