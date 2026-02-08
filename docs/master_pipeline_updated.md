# master 作成パイプライン（master_pipeline）

本ドキュメントは、学習に使用する **master（共通 master.csv）** を作成し、さらに  
「優勝戦 / 準優勝戦 / 準優進出戦」だけを抽出して **master_finals.csv** を作るまでの工程を整理したものです。

現状は、**モーター関連特徴量（motor_id / motor_section）も master に組み込み済み**で、
学習用の入力（`master.csv` と `master_finals.csv`）を **1本の PowerShell（PS1）で確定**できる状態になっています。

---

## 全体目的（SSOT）

1. `data/raw/`（日次CSV群）から、型変換・欠損/異常の除外・prior結合を行い **共通 master.csv** を生成  
2. master に対し、リーク無しの履歴系特徴量を付与（上書き運用）  
3. master にモーター関連特徴量（motor_id / motor_section）を付与（上書き運用）  
4. master から対象レースのみ抽出し、**master_finals.csv** を出力  

最終成果物：
- `data/processed/master.csv`（共通 master / すべて付与後の完成形）
- `data/processed/master_finals.csv`（ステージ抽出後）

---

## ディレクトリ前提

```
ark-blueprints/
├─ batch/
│  └─ build_master_range.ps1        # 学習用 master/master_finals を確定する “唯一の入口”
├─ data/
│  ├─ raw/                          # 日次 raw CSV 群（入力）
│  ├─ priors/                       # latest.csv を参照（tenji / season_course / winning_trick）
│  └─ processed/
│     ├─ master.csv                 # 共通 master（出力 / 上書き対象 / 完成形）
│     ├─ master_finals.csv          # 抽出後（学習の主入力）
│     ├─ reports/                   # preprocess.py のレポート（除外・異常など）
│     ├─ course_meta/               # preprocess_course.py のメタログ
│     ├─ raceinfo/                  # preprocess_sectional.py 学習JOIN用（期間指定時）
│     └─ motor/
│        ├─ motor_id_map__all.csv              # motor_id 付与マップ（run_build_motor_pipeline.ps1 が更新）
│        └─ motor_section_features_n__all.csv  # motor_section 特徴量（run_build_motor_pipeline.ps1 が更新）
└─ scripts/
   ├─ preprocess.py
   ├─ preprocess_course.py
   ├─ preprocess_sectional.py
   ├─ preprocess_motor_id.py
   ├─ preprocess_motor_section.py
   └─ make_master_finals.py
```

---

## パイプライン全体像（推奨：PS1を唯一の入口にする）

### ✅ 推奨（1コマンドで master / master_finals を両方生成）

`batch/build_master_range.ps1` が以下の工程を **順番固定で実行**します（master.csv は各工程で上書きされる）。

1. **preprocess.py** → `master.csv` を作る（基礎の正本）
2. **preprocess_course.py（上書き）** → course履歴特徴量を付与（warmup + 直前N）
3. **preprocess_sectional.py（上書き）** → 節間スナップショット（必須10列）を付与
4. **preprocess_motor_id.py（上書き）** → motor_id を付与（motor_id_map__all.csv参照）
5. **preprocess_motor_section.py（上書き）** → motor_section(prev/delta) を安全結合で付与（リーク防止）
6. **make_master_finals.py** → finals/semi/semi-entry を抽出して `master_finals.csv` を作成

---

## 0. build_master_range.ps1（学習用 master / master_finals を確定する唯一の入口）

### 役割
- 学習期間を指定して `master.csv` を作成し、必要な特徴量を **順次上書き付与**
- 最後に `master.csv` からステージ抽出して `master_finals.csv` を生成
- 「学習用入力のスキーマ」を PS1 側で固定し、作り忘れ/順序違いを防ぐ

### 実行コマンド（例：学習期間 2024-10-01〜2025-12-31）
（Anaconda Prompt / cmd）

```bat
cd C:\Users\user\Desktop\Git\ark-blueprints
powershell -NoProfile -ExecutionPolicy Bypass -File batch\build_master_range.ps1 -StartDate 20241001 -EndDate 20251231 -WarmupDays 180 -NLast 10
```

### 主要オプション
- `-SkipCourse` / `-SkipSectional` / `-SkipMotorId` / `-SkipMotorSection`  
  → 各工程をスキップ（通常は使わない。検証・トラブルシュート用）
- `-SkipFinals`  
  → `master_finals.csv` の生成のみスキップ（`master.csv` だけ欲しい場合）
- `-BackupBeforeMotorSection`（既定OFF） + `-BackupFormat pklgz|csv`  
  → Step 5 直前の master をバックアップ（運用では基本OFF）

---

## 1. preprocess.py（共通 master の生成）

### 役割
- `data/raw/*.csv` を全読み込みして結合し、型変換・正規化を実施
- 明らかに壊れたレース（着順非数値、気象欠損、展示ST不正など）をレース単位で除外
- `priors/latest.csv` を結合（tenji / season_course / winning_trick）
- `master.csv` を出力（学習基盤の正本）
- 除外ルールや異常トークンの頻度レポートを `data/processed/reports/` に保存

### 入力
- `data/raw/*.csv`
- `data/priors/tenji/latest.csv`
- `data/priors/season_course/latest.csv`
- `data/priors/winning_trick/latest.csv`

### 出力
- `data/processed/master.csv`
- `data/processed/reports/*`（除外レース・異常レポート等）

※ 以前存在した `master_pre_drop.csv` は役目を終えたため、現行では生成しない方針。

---

## 2. preprocess_course.py（上書き：course履歴特徴量の付与）

### 役割
- raw（warmup含む）から、選手×進入（entry）／選手×枠（wakuban）の **直前N走** 集計をリーク無しで作成し、master に LEFT JOIN
- **shift(1)** + rolling で「直前まで」しか見ない（リーク防止）

代表例（suffix 付き）：
- `finish1_rate_lastN_entry`, `finish2_rate_lastN_entry`, `finish3_rate_lastN_entry`
- `st_mean_lastN_entry`, `st_std_lastN_entry`
- 同様に `_waku` 版も出力

### 入力
- `data/processed/master.csv`
- `data/raw/*.csv`（warmup のため過去期間も参照）

### 出力（上書き運用）
- `data/processed/master.csv`
- `data/processed/course_meta/*`（ログ）

---

## 3. preprocess_sectional.py（上書き：節間10列の付与）

### 役割
- master に **必須10列（SECTIONAL_10）** を付与し、学習・推論でスキーマを揃える  
- 学習モード（期間指定）：`data/processed/raceinfo` のCSV群を `(race_id, player_id)` で直接JOIN  
- 10列は数値化し、欠損は 0.0 で埋める（推論安定）

必須10列（代表）：
- `ST_mean_current`, `ST_rank_current`, `ST_previous_time`
- `score`, `score_rate`
- `ranking_point_sum`, `ranking_point_rate`
- `condition_point_sum`, `condition_point_rate`
- `race_ct_current`

### 入力（学習：期間指定）
- `data/processed/master.csv`
- `data/processed/raceinfo/*.csv`

### 出力（上書き運用）
- `data/processed/master.csv`

---

## 4. preprocess_motor_id.py（上書き：motor_id の付与）

### 役割
- `motor_id_map__all.csv` を参照し、master の各行に `motor_id` を付与
- 学習・推論で motor_id が安定して付くことが重要（miss rate を QC として監視）

### 入力
- `data/processed/master.csv`
- `data/processed/motor/motor_id_map__all.csv`

### 出力（上書き運用）
- `data/processed/master.csv`（motor_id が追加される）

---

## 5. preprocess_motor_section.py（上書き：motor_section(prev/delta) の付与）

### 役割
- `motor_section_features_n__all.csv` を参照し、master に motor の prev/delta 特徴量を付与
- 安全のため `master.tmp.csv` に出力 → 成功後に `master.csv` を置換（破損防止）
- 結合QC（例）：`merge status`（both/left_only）や NA率を出力

### 入力
- `data/processed/master.csv`
- `data/processed/motor/motor_section_features_n__all.csv`

### 出力（上書き運用）
- `data/processed/master.csv`
- （任意）`data/processed/reports/qc_motor_section_join.csv`（QCレポート）

---

## 6. make_master_finals.py（対象レース抽出 → master_finals.csv）

### 役割
- 共通 master.csv から `race_name` を使って、以下ステージのみ抽出して `master_finals.csv` を作成  
  - 優勝戦（finals）
  - 準優勝戦（semi）
  - 準優進出戦 / 準優進出（semi-entry）

### 入力
- `data/processed/master.csv`

### 出力
- `data/processed/master_finals.csv`

---

## 典型的な運用手順（推奨：PS1のみ）

```bat
cd C:\Users\user\Desktop\Git\ark-blueprints
powershell -NoProfile -ExecutionPolicy Bypass -File batch\build_master_range.ps1 -StartDate 20241001 -EndDate 20251231 -WarmupDays 180 -NLast 10
```

### master だけ欲しい場合（finals抽出をスキップ）
```bat
cd C:\Users\user\Desktop\Git\ark-blueprints
powershell -NoProfile -ExecutionPolicy Bypass -File batch\build_master_range.ps1 -StartDate 20241001 -EndDate 20251231 -WarmupDays 180 -NLast 10 -SkipFinals
```

---

## 最小QC（確認項目）

- build_master_range.ps1 実行後：
  - `data/processed/master.csv` が更新される（完成形）
  - `data/processed/master_finals.csv` が更新される（学習の主入力）
- ログ上の目安（例）：
  - `motor_id miss rate: 0.000%`
  - `merge status: both=..., left_only=0`
  - `master` の行数が工程間で不自然に増減していない

---

## 更新履歴
- 2026-02 : PS1（build_master_range.ps1）を SSOT とし、motor_id / motor_section / finals 抽出までを一気通貫化。既存ドキュメントを現行パイプラインに追従させた。
