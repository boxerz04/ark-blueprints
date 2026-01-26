# master 作成パイプライン（master_pipeline）

本ドキュメントは、学習に使用する **master（共通 master.csv）** を作成し、さらに  
「優勝戦 / 準優勝戦 / 準優進出戦」だけを抽出して **master_finals.csv** を作るまでの工程を整理したものです。  
（現状：モーター関連特徴量はまだ組み込まない前提）

---

## 全体目的

1. `data/raw/`（日次CSV群）から、型変換・欠損/異常の除外・prior結合を行い **共通 master.csv** を生成  
2. master に対し、リーク無しの履歴系特徴量を付与（上書き運用）  
3. master から対象レースのみ抽出し、**master_finals.csv** を出力

最終成果物：
- `data/processed/master.csv`（共通 master / 上書き後）
- `data/processed/master_finals.csv`（ステージ抽出後）

---

## ディレクトリ前提

```
ark-blueprints/
├─ data/
│  ├─ raw/                       # 日次 raw CSV 群（入力）
│  ├─ priors/                    # latest.csv を参照（tenji / season_course / winning_trick）
│  └─ processed/
│     ├─ master.csv              # 共通 master（出力 / 上書き対象）
│     ├─ reports/                # preprocess.py のレポート（除外・異常など）
│     ├─ course/                 # preprocess_course.py の出力（設定次第）
│     ├─ course_meta/            # preprocess_course.py のメタログ
│     └─ raceinfo/               # preprocess_sectional.py 学習JOIN用（期間指定時）
└─ scripts/
```

---

## パイプライン全体像（実行順）

1. **preprocess.py** → `master.csv` を作る（基礎の正本）
2. **preprocess_course.py（上書き）** → course履歴特徴量を付与
3. **preprocess_sectional.py（上書き）** → 節間（今節スナップショット）10列を付与
4. **make_master_finals.py** → master から finals/semi/semi-entry を抽出して `master_finals.csv`

---

## 1. preprocess.py（共通 master の生成）

### 役割
- `data/raw/*.csv` を全読み込みして結合し、型変換・正規化を実施
- 明らかに壊れたレース（着順非数値、気象欠損、展示ST不正など）をレース単位で除外
- `priors/latest.csv` を結合（tenji / season_course / winning_trick）
- `master.csv` を出力（学習基盤の正本）

実装上の重要点：
- `master_pre_drop.csv`（drop前の母集団）も別途保存（参照用途）  
- 除外ルールや異常トークンの頻度レポートを `data/processed/reports/` に保存

### 入力
- `data/raw/*.csv`
- `data/priors/tenji/latest.csv`
- `data/priors/season_course/latest.csv`
- `data/priors/winning_trick/latest.csv`

### 出力
- `data/processed/master.csv`
- `data/processed/master_pre_drop.csv`
- `data/processed/reports/*`（除外レース・異常レポート等）

### 実行コマンド（例：全期間）
```bat
python scripts\preprocess.py --raw-dir data\raw --out data\processed\master.csv
```

### 実行コマンド（例：期間指定）
```bat
python scripts\preprocess.py --raw-dir data\raw --out data\processed\master.csv --start-date 2023-10-01 --end-date 2026-01-10
```

---

## 2. preprocess_course.py（上書き：course履歴特徴量の付与）

### 役割
- raw（除外前）から、選手×進入（entry）／選手×枠（wakuban）の **直前N走** 集計をリーク無しで作成し、master に LEFT JOIN
- 出走判定（分母）のルールを明確化：非数値でも「欠」以外は出走扱い（F/L/転/落/妨/不/エ/沈 等は出走）  
- **shift(1)** + rolling で「直前まで」しか見ない（リーク防止）

出力される代表例（suffix 付き）：
- `finish1_rate_lastN_entry`, `finish2_rate_lastN_entry`, `finish3_rate_lastN_entry`
- `st_mean_lastN_entry`, `st_std_lastN_entry`
- 同様に `_waku` 版も出力

### 入力
- `data/processed/master.csv`（preprocess.py の成果物）
- `data/raw/*.csv`（warmup のため過去期間も参照）

### 出力
- 上書き運用の場合：`data/processed/master.csv` を更新
- ログ：`data/processed/course_meta/*`

※ スクリプトのデフォルト出力は `data/processed/course/master_course.csv` ですが、  
「上書き」運用では `--out data/processed/master.csv` を指定します。

### 実行コマンド（上書き例）
```bat
python scripts\preprocess_course.py --master data\processed\master.csv --raw-dir data\raw --out data\processed\master.csv
```

（任意）パラメータ調整：
- `--warmup-days`（既定 180）
- `--n-last`（既定 10）

---

## 3. preprocess_sectional.py（上書き：節間10列の付与）

### 役割
- master に **必須10列（SECTIONAL_10）** を付与し、学習・推論でスキーマを揃える  
- 学習モード（期間指定）：`data/processed/raceinfo` のCSV群を `(race_id, player_id)` で直接JOIN  
- 推論モード（単日指定）：`data/live/html` の racelist から raceinfo_features で解析してJOIN（アダプタ互換）
- 10列は数値化し、欠損は 0.0 で埋める（推論安定）

必須10列：
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

### 実行コマンド（学習：期間指定、上書き）
```bat
python scripts\preprocess_sectional.py --master data\processed\master.csv --raceinfo-dir data\processed\raceinfo --start-date 2023-10-01 --end-date 2026-01-10 --out data\processed\master.csv
```

---

## 4. make_master_finals.py（対象レース抽出）

### 役割
- 共通 master.csv から `race_name` を使って、以下ステージのみ抽出して `master_finals.csv` を作成  
  - 優勝戦（finals）
  - 準優勝戦（semi）
  - 準優進出戦 / 準優進出（semi-entry）

### 入力
- `data/processed/master.csv`

### 出力
- `data/processed/master_finals.csv`

### 実行コマンド（デフォルト）
```bat
python scripts\make_master_finals.py --master-in data\processed\master.csv --master-out data\processed\master_finals.csv
```

（任意）抽出対象の変更：
```bat
python scripts\make_master_finals.py --master-in data\processed\master.csv --master-out data\processed\master_finals.csv --stage-filter "finals,semi"
```

---

## 典型的な運用手順（上書き前提・最短）

```bat
python scripts\preprocess.py --raw-dir data\raw --out data\processed\master.csv
python scripts\preprocess_course.py --master data\processed\master.csv --raw-dir data\raw --out data\processed\master.csv
python scripts\preprocess_sectional.py --master data\processed\master.csv --raceinfo-dir data\processed\raceinfo --start-date 2023-10-01 --end-date 2026-01-10 --out data\processed\master.csv
python scripts\make_master_finals.py --master-in data\processed\master.csv --master-out data\processed\master_finals.csv
```

---

## 最小QC（確認項目）

- preprocess.py 実行後：
  - `data/processed/master.csv` が更新される
  - `data/processed/reports/` に除外・異常レポートが出力される
- preprocess_course.py 実行後：
  - `*_lastN_entry` / `*_lastN_waku` 系列が master に増える
- preprocess_sectional.py 実行後：
  - 必須10列（SECTIONAL_10）が master に存在し、数値（欠損は0.0）になっている
- make_master_finals.py 実行後：
  - `race_name` が存在し、抽出後の行数がログに出る

---

（更新履歴）
- 2026-01 : master（共通）→ course → sectional → finals 抽出の流れを文書化
