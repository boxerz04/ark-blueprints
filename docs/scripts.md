# scripts 辞典（現行運用版）

本ページは `scripts/` 直下の現行 `.py` を対象に、運用・開発の参照辞典として全面再構築したものです（`_archive` は除外）。

---

## A. この辞典の使い方

### 対象と定義
- **現行**: `scripts/` 直下の `.py`（本書では 29 件を収録）。
- **旧/アーカイブ**: `scripts/_archive/**`（本書の対象外）。
- 除外ルール: `_archive`, `.ipynb_checkpoints`, `__pycache__`, `*checkpoint*` を除外。

### 入口（called_by）との関係
- **日次収集～中間生成**: `batch/run_scrape_build_raceinfo_range.ps1`（`scrape.py` → `build_raw_csv.py` → `build_raceinfo.py`）。
- **学習用マスタ生成**: `batch/build_master_range.ps1`（`preprocess*.py` 群 + `make_master_finals.py`）。
- **学習/チューニング**: `batch/train_model_from_master.ps1`（`preprocess_base_features.py`, `train.py`, `tune_hyperparams.py`）。
- **事前分布更新**: `batch/update_priors.ps1`（季節/展示系 prior 生成）。
- **Vault運用**: `batch/run_all_vaults_full_rebuild.ps1`, `batch/run_html_vault_full_rebuild.ps1`（`vault_csv_by_pattern.py`）。
- **リアルタイム系**: GUI/手動実行で `scrape_one_race.py`, `build_live_row.py`, `predict_one_race.py`, `run_odds_scheduler.py` を利用。

---

## B. 全体カタログ（1行=1スクリプト）

| script | category | called_by | inputs | outputs | schedule | notes |
|---|---|---|---|---|---|---|
| `__init__.py` | ops | import時 | - | - | - | パッケージマーカー |
| `scrape.py` | ingest | `run_scrape_build_raceinfo_range.ps1`/手動 | boatrace公式HTML | `data/html/*/*.bin` | 日次 | 通信依存 |
| `build_raw_csv.py` | transform | `run_scrape_build_raceinfo_range.ps1` | `data/html/*/*.bin` | `data/raw/*_raw.csv`, `data/refund/*_refund.csv` | 日次 | 重め |
| `build_raceinfo.py` | transform | `run_scrape_build_raceinfo_range.ps1` | `data/html/racelist/*.bin` | `data/processed/raceinfo/raceinfo_*.csv` | 日次 | 欠損日はskip |
| `preprocess.py` | feature | `build_master_range.ps1` | `data/raw` + `data/priors` | `data/processed/master.csv` | 日次/随時 | 中核処理 |
| `preprocess_course.py` | feature | `build_master_range.ps1` | `master.csv`, `data/raw` | `data/processed/course/master_course.csv` | 随時 | 履歴窓あり |
| `preprocess_sectional.py` | feature | `build_master_range.ps1` | `master.csv`, `raceinfo` | `master.csv`(上書き) | 随時 | 破壊的更新 |
| `preprocess_motor_id.py` | feature/qc | `build_master_range.ps1` | `master.csv`, motor map | motor_id付与CSV | 随時 | miss率閾値 |
| `preprocess_motor_section.py` | feature/qc | `build_master_range.ps1` | `master.csv`, motor section特徴 | `out_master_csv` | 随時 | キー厳密結合 |
| `make_master_finals.py` | transform | `build_master_range.ps1` | `master.csv` | `master_finals.csv` | 学習前 | finals抽出 |
| `preprocess_base_features.py` | feature | `train_model_from_master.ps1`/手動 | `master(_finals).csv`, YAML | 学習用中間CSV | 学習前 | 列選択仕様に依存 |
| `train.py` | train | `train_model_from_master.ps1`/手動 | `data/processed/*` | モデル成果物 | 学習時 | 重い |
| `tune_hyperparams.py` | train | 手動/学習バッチ | 特徴量データ | チューニング結果 | 必要時 | 長時間 |
| `predict_one_race.py` | infer | GUI/手動 | `data/live/raw_*.csv` | 標準出力/予測結果 | レース前 | モデル整合が必要 |
| `scrape_one_race.py` | ingest/live | GUI/手動 | boatrace公式（1R） | `data/live/html/*` | レース前 | 通信依存 |
| `build_live_row.py` | transform/live | GUI/手動 | `data/live/html` or online | `data/live/raw_*.csv` | レース前 | ST異常で停止 |
| `build_timeline_live.py` | ops/live | 手動 | 開催情報 | `data/timeline/*.csv`（推定） | 日次 | 要確認 |
| `run_odds_scheduler.py` | ops/live | 手動 | timeline, python実行環境 | scrape/predict呼び出し | レース日 | 常駐系 |
| `scrape_odds.py` | ingest/live | `run_odds_scheduler.py`/手動 | oddsページ | oddsキャッシュ | レース前 | 要確認 |
| `build_tenji_prior_from_raw.py` | feature | `update_priors.ps1` | `data/raw` | `data/priors/tenji/*.csv` | 定期更新 | prior更新 |
| `build_season_course_prior_from_raw.py` | feature | `update_priors.ps1` | `data/raw` | `data/priors/season_course/*.csv` | 定期更新 | 範囲指定必須 |
| `build_season_winningtrick_prior_from_raw.py` | feature | `update_priors.ps1` | `data/raw` | `data/priors/winning_trick/*.csv` | 定期更新 | 範囲指定必須 |
| `build_motor_artifacts_from_bins.py` | feature | `run_build_motor_pipeline.ps1`/手動 | HTML bins/履歴 | motor snapshot/map CSV | 定期更新 | 重い |
| `build_raw_with_motor_joined.py` | transform | `run_build_motor_pipeline.ps1`/手動 | raw + motor artifact | join済みraw | 定期更新 | サンプル出力あり |
| `build_motor_section_base.py` | feature | `run_build_motor_pipeline.ps1`/手動 | master系CSV | motor section base | 定期更新 | 集約前段 |
| `build_motor_section_features_n.py` | feature | `run_build_motor_pipeline.ps1`/手動 | section base CSV | N窓特徴CSV | 定期更新 | mean_ns指定 |
| `export_base_feature_yaml.py` | ops | 手動 | `master.csv` | feature YAML | 必要時 | 設定出力 |
| `vault_csv_by_pattern.py` | ops | vault系バッチ | 任意CSV/HTML | sqlite vault | 定期/随時 | I/O大 |
| `export_vault.py` | ops | 手動 | sqlite vault | CSVエクスポート | 随時 | pattern抽出 |

---

## C. カテゴリ別セクション（俯瞰）

### 1) ingest / live ingest
```
scrape.py ─┬─> data/html/*
           └─> build_raw_csv.py
scrape_one_race.py -> data/live/html/* -> build_live_row.py
```
- 公式サイト依存。通信エラー時は再試行前提。
- live 系はレース時刻制約があるため、ログ監視を優先。

### 2) transform / feature
```
build_raw_csv -> preprocess.py -> preprocess_course/sectional/motor_* -> master.csv
                                                   └-> make_master_finals.py
raw -> prior builders -> preprocess.py(結合)
```
- 学習用の主系統。`preprocess_sectional.py` は上書き更新のためバックアップ推奨。

### 3) train / infer
```
master(_finals) -> preprocess_base_features -> train / tune_hyperparams
live raw -> predict_one_race
```
- 学習成果物の版管理（approach/model id）を運用ルール化する。

### 4) ops / vault / QC
```
vault_csv_by_pattern -> sqlite vault -> export_vault
run_odds_scheduler -> scrape_odds / 予測呼び出し
```
- 定期実行系。lock/log 監視とディスク容量管理が重要。

---

## D. 個別ページ（辞典本体）

> 記載の一部はコード読解ベースの推定です。断定困難な点は **要確認** と明記。

### `scripts/__init__.py`
- 概要: `scripts` ディレクトリをPythonパッケージとして扱うためのファイル。
- 役割: import解決。
- called_by: Python import時。
- 入出力: なし。
- 主な引数: なし。
- 実行例: `python -c "import scripts"`
- 依存関係: なし。
- 失敗しやすい点: 直接実行用途ではない。
- 実行コスト: 軽い。

### `scripts/scrape.py`
- 概要: 指定日の公式HTMLを一括取得してbin保存。
- 役割: `pay/index/racelist/...` の収集。
- called_by: `batch/run_scrape_build_raceinfo_range.ps1`。
- 入出力: `--date` → `data/html/*/*.bin`。
- 主な引数:

| name | required | default | meaning |
|---|---:|---|---|
| `--date` | no | 当日 | 取得日 |

- 実行例: `python scripts/scrape.py --date 20250115`
- 依存関係: 下流 `build_raw_csv.py`, `build_raceinfo.py`。
- 失敗と対処: 通信エラー→再実行。ディレクトリ欠如→作成。
- 実行コスト: 中～重（対象日件数依存）。

### `scripts/build_raw_csv.py`
- 概要: HTML bin から日次 raw/refund CSV を構築。
- 役割: 学習の基礎データ生成。
- called_by: `run_scrape_build_raceinfo_range.ps1`。
- 入出力: `data/html/*` → `data/raw/YYYYMMDD_raw.csv`, `data/refund/YYYYMMDD_refund.csv`。
- 主な引数: `--date`（任意、当日既定）。
- 実行例: `python scripts/build_raw_csv.py --date 20250115`
- 依存関係: 上流 `scrape.py`。
- 失敗と対処: 欠損binはログ確認し該当日再scrape。
- 実行コスト: 重い（全場×R処理）。

### `scripts/build_raceinfo.py`
- 概要: racelist bin から raceinfo 日次CSVを生成。
- 役割: sectional系特徴の源泉を作成。
- called_by: `run_scrape_build_raceinfo_range.ps1`。
- 入力/出力: `data/html/racelist/*.bin` → `data/processed/raceinfo/raceinfo_YYYYMMDD.csv`。
- 主な引数:

| name | required | default | meaning |
|---|---:|---|---|
| `--date` | no | - | 単日 |
| `--start-date` | no | - | 開始日 |
| `--end-date` | no | - | 終了日 |
| `--all-available` | no | false | 利用可能日を自動処理 |
| `--html-dir` | no | `data/html/racelist` | 入力 |
| `--out-dir` | no | `data/processed/raceinfo` | 出力 |

- 実行例: `python scripts/build_raceinfo.py --start-date 20250101 --end-date 20250131`
- 依存関係: 下流 `preprocess_sectional.py`。
- 失敗と対処: bin未検出日はskip（要ログ確認）。
- 実行コスト: 中。

### `scripts/preprocess.py`
- 概要: raw から学習用 `master.csv` を生成し prior を結合。
- 役割: 特徴量基盤の統合。
- called_by: `batch/build_master_range.ps1`。
- 入出力: `data/raw`, `data/priors` → `data/processed/master.csv`。
- 主な引数: `--raw-dir`, `--out`, `--reports-dir`, `--priors-root`, `--start-date`, `--end-date`, `--no-join-*`。
- 実行例: `python scripts/preprocess.py --start-date 20250101 --end-date 20250131`
- 依存関係: 下流 course/sectional/motor 系。
- 失敗と対処: prior欠損時は `--no-join-*` で切り分け。
- 実行コスト: 重い。

### `scripts/preprocess_course.py`
- 概要: コース履歴系特徴を計算。
- 役割: `master_course.csv` 出力。
- called_by: `build_master_range.ps1`。
- 入出力: `master.csv`, `data/raw` → `data/processed/course/master_course.csv`。
- 主な引数: `--master`, `--raw-dir`, `--out`, `--warmup-days`, `--n-last` 他。
- 実行例: `python scripts/preprocess_course.py --warmup-days 180 --n-last 10`
- 依存関係: 上流 `preprocess.py`。
- 失敗と対処: 期間不足で特徴欠落（要確認）。
- 実行コスト: 中。

### `scripts/preprocess_sectional.py`
- 概要: raceinfo を master に上書き結合し sectional列を補完。
- 役割: 必須列の埋め戻し。
- called_by: `build_master_range.ps1`。
- 入出力: `master.csv`, `raceinfo` → `master.csv`（既定上書き）。
- 主な引数: `--master`, `--raceinfo-dir`, `--date/start-date/end-date`, `--out`。
- 実行例: `python scripts/preprocess_sectional.py --start-date 20250101 --end-date 20250131`
- 依存関係: 上流 `build_raceinfo.py`。
- 失敗と対処: 上書き前バックアップ推奨。
- 実行コスト: 中。

### `scripts/preprocess_motor_id.py`
- 概要: motor番号に対して一意IDを付与。
- 役割: motor特徴結合キー整備。
- called_by: `build_master_range.ps1`。
- 入出力: `--in_csv`, `--map_csv` → `--out_csv`。
- 主な引数: `--in_csv`(必須), `--map_csv`, `--max_miss_rate` 他。
- 実行例: `python scripts/preprocess_motor_id.py --in_csv data/processed/master.csv`
- 依存関係: 下流 `preprocess_motor_section.py`。
- 失敗と対処: miss率超過で停止（マップ更新）。
- 実行コスト: 軽い。

### `scripts/preprocess_motor_section.py`
- 概要: motor section特徴を master に安全結合。
- 役割: `motor_*` 列の追加。
- called_by: `build_master_range.ps1`。
- 入出力: `master_csv`, `motor_section_csv` → `out_master_csv`。
- 主な引数: `--master_csv`, `--motor_section_csv`, `--out_master_csv`(必須), `--strict_key_match` 他。
- 実行例: `python scripts/preprocess_motor_section.py --master_csv ... --motor_section_csv ... --out_master_csv ...`
- 依存関係: 上流 motor pipeline。
- 失敗と対処: キー不整合はQCレポートで確認。
- 実行コスト: 中。

### `scripts/make_master_finals.py`
- 概要: masterから finals 系のみ抽出。
- 役割: `master_finals.csv` 生成。
- called_by: `build_master_range.ps1`。
- 入出力: `master.csv` → `master_finals.csv`。
- 主な引数: `--master-in`, `--master-out`, `--stage-filter`。
- 実行例: `python scripts/make_master_finals.py`
- 依存関係: 下流学習。
- 失敗と対処: stage列欠損時は前段確認。
- 実行コスト: 軽い。

### `scripts/preprocess_base_features.py`
- 概要: 学習用の列選択・特徴加工。
- 役割: approach別の入力データ整形。
- called_by: `train_model_from_master.ps1`/手動。
- 入出力: `--master`, `--feature-spec-yaml` → `data/processed`配下成果物。
- 主な引数: `--master`, `--feature-spec-yaml`, `--approach`(必須), `--target-col` 他。
- 実行例: `python scripts/preprocess_base_features.py --master data/processed/master_finals.csv --feature-spec-yaml configs/base.yaml --approach finals`
- 依存関係: 下流 `train.py`。
- 失敗と対処: YAML列不一致は `--allow-missing-selected-cols` で検証。
- 実行コスト: 中。

### `scripts/train.py`
- 概要: モデル学習を実行。
- 役割: approach別モデル生成。
- called_by: `train_model_from_master.ps1`/手動。
- 入出力: `data/processed/*` → モデル・メタ情報（要確認）。
- 主な引数: `--approach`, `--n-estimators`, `--learning-rate`, `--num-leaves` 他。
- 実行例: `python scripts/train.py --approach finals --n-estimators 400`
- 依存関係: 上流 `preprocess_base_features.py`。
- 失敗と対処: 特徴量欠損/クラス不均衡ログを確認。
- 実行コスト: 重い。

### `scripts/tune_hyperparams.py`
- 概要: ハイパーパラメータ探索。
- 役割: 学習設定の最適化。
- called_by: 手動/学習バッチ。
- 入出力: 学習データ → 探索結果ファイル（`--out`）。
- 主な引数: `--approach`, `--n-iter`, `--scoring`, `--out`, `--project-root`。
- 実行例: `python scripts/tune_hyperparams.py --approach finals --n-iter 80`
- 依存関係: `train.py` と同一データ基盤。
- 失敗と対処: 実行時間超過は `--n-iter` 縮小。
- 実行コスト: 重い（長時間）。

### `scripts/predict_one_race.py`
- 概要: 1レース分 live raw から推論。
- 役割: レース前予測の生成。
- called_by: GUI/手動。
- 入出力: `--live-csv` → 標準出力（予測結果）。
- 主な引数: `--live-csv`(必須), `--approach`, `--model`, `--feature-pipeline`, `--quiet` 他。
- 実行例: `python scripts/predict_one_race.py --live-csv data/live/raw_20250903_12_03.csv --approach base`
- 依存関係: 上流 `build_live_row.py`。
- 失敗と対処: モデル/特徴量版不一致に注意。
- 実行コスト: 軽い。

### `scripts/scrape_one_race.py`
- 概要: 1レース対象のHTML群を収集。
- 役割: live推論前の素材取得。
- called_by: GUI/手動。
- 入出力: `--date --jcd --race` → `data/live/html/*`。
- 主な引数: 全て必須（`--date`, `--jcd`, `--race`）。
- 実行例: `python scripts/scrape_one_race.py --date 20250903 --jcd 12 --race 3`
- 依存関係: 下流 `build_live_row.py`。
- 失敗と対処: 取得漏れは再実行。
- 実行コスト: 軽い。

### `scripts/build_live_row.py`
- 概要: live HTMLから raw互換6行を生成。
- 役割: 推論入力CSV作成。
- called_by: GUI/手動。
- 入出力: `data/live/html/*` or `--online` → `data/live/raw_*.csv`。
- 主な引数: `--date --jcd --race`(必須), `--online`, `--out`。
- 実行例: `python scripts/build_live_row.py --date 20250903 --jcd 12 --race 3 --online --out data/live/raw_20250903_12_03.csv`
- 依存関係: 下流 `predict_one_race.py`。
- 失敗と対処: ST非数値時は停止（入力確認）。
- 実行コスト: 軽い。

### `scripts/build_timeline_live.py`
- 概要: live運用用タイムラインを作る補助スクリプト。
- 役割: スケジューラ入力生成（推定）。
- called_by: 手動。
- 入出力: `--date` → `data/timeline/*`（要確認）。
- 主な引数: `--date`。
- 実行例: `python scripts/build_timeline_live.py --date 20250903`
- 依存関係: `run_odds_scheduler.py` で参照される可能性。
- 失敗と対処: 出力パス/フォーマットは要確認。
- 実行コスト: 軽い。

### `scripts/run_odds_scheduler.py`
- 概要: タイムラインに従って odds/予測処理を起動するランナー。
- 役割: レース当日の定期実行。
- called_by: 手動（常駐起動）。
- 入出力: `--timeline`, `--python`, `--log_file`。
- 主な引数: `--timeline`, `--mins_before`(既定5), `--python`, `--log_file`。
- 実行例: `python scripts/run_odds_scheduler.py --timeline data/timeline/20250903.csv --mins_before 5`
- 依存関係: `scrape_odds.py` 等（コード読解推定）。
- 失敗と対処: 長時間運用時のログローテート必須。
- 実行コスト: 中（常駐）。

### `scripts/scrape_odds.py`
- 概要: 指定レースのodds情報を取得。
- 役割: 直前オッズ連携。
- called_by: scheduler/手動。
- 入出力: `--date --jcd --rno` → odds関連出力（要確認）。
- 主な引数: 全て必須。
- 実行例: `python scripts/scrape_odds.py --date 20250903 --jcd 12 --rno 3`
- 依存関係: live推論補助。
- 失敗と対処: 通信系再試行。
- 実行コスト: 軽い。

### `scripts/build_tenji_prior_from_raw.py`
- 概要: 展示ST系 prior を raw から推定。
- 役割: priorデータ更新。
- called_by: `update_priors.ps1`。
- 入出力: `data/raw` → `data/priors/tenji/*.csv`。
- 主な引数: `--from --to --out`(必須), `--m-strength`, `--sd-floor`, `--link-latest`。
- 実行例: `python scripts/build_tenji_prior_from_raw.py --from 20240101 --to 20241231 --out data/priors/tenji/tenji_prior_20241231.csv`
- 依存関係: 下流 `preprocess.py`。
- 失敗と対処: 期間列型不一致に注意。
- 実行コスト: 中。

### `scripts/build_season_course_prior_from_raw.py`
- 概要: season×course の prior を作成。
- 役割: コース傾向補正の事前分布更新。
- called_by: `update_priors.ps1`。
- 入出力: `data/raw` → `data/priors/season_course/*.csv`。
- 主な引数: `--from --to --out`(必須), `--finish-col`, `--entry-col`, `--m-strength` 他。
- 実行例: `python scripts/build_season_course_prior_from_raw.py --from 20240101 --to 20241231 --out data/priors/season_course/season_course_prior_20241231.csv`
- 依存関係: `preprocess.py`。
- 失敗と対処: 列名差異は引数で調整。
- 実行コスト: 中。

### `scripts/build_season_winningtrick_prior_from_raw.py`
- 概要: season×決まり手 prior を生成。
- 役割: 勝ちパターン事前分布更新。
- called_by: `update_priors.ps1`。
- 入出力: `data/raw` → `data/priors/winning_trick/*.csv`。
- 主な引数: `--from --to --out`(必須), `--trick-col`, `--m-strength` 他。
- 実行例: `python scripts/build_season_winningtrick_prior_from_raw.py --from 20240101 --to 20241231 --out data/priors/winning_trick/winning_trick_prior_20241231.csv`
- 依存関係: `preprocess.py`。
- 失敗と対処: trick列欠損時に要確認。
- 実行コスト: 中。

### `scripts/build_motor_artifacts_from_bins.py`
- 概要: motor関連のsnapshot/map成果物をbin由来で生成。
- 役割: motor ID・遷移の基盤作成。
- called_by: `run_build_motor_pipeline.ps1`/手動。
- 入出力: `--bins_dir` → `--out_snapshot_csv`, `--out_map_csv`。
- 主な引数: 上記3必須 + `--gap_days`, `--no_use_transition`, `--start_date`, `--end_date`, `--limit`。
- 実行例: `python scripts/build_motor_artifacts_from_bins.py --bins_dir data/html/racelist --out_snapshot_csv data/processed/motor/snapshot.csv --out_map_csv data/processed/motor/motor_id_map__all.csv`
- 依存関係: 下流 `preprocess_motor_id.py`, `build_raw_with_motor_joined.py`。
- 失敗と対処: 大量データ時は `--limit` で試験実行。
- 実行コスト: 重い。

### `scripts/build_raw_with_motor_joined.py`
- 概要: rawにmotor artifactを結合した派生データを生成。
- 役割: motor強化版rawの出力。
- called_by: motor pipeline バッチ/手動。
- 入出力: `--raw_dir`, `--snapshot_csv`, `--map_csv` → `--out_dir`。
- 主な引数: 上記4必須 + `--write_full_csv`, `--sample_n`。
- 実行例: `python scripts/build_raw_with_motor_joined.py --raw_dir data/raw --snapshot_csv ... --map_csv ... --out_dir data/processed/motor`
- 依存関係: 上流 motor artifacts。
- 失敗と対処: 結合率低下時はmap更新確認。
- 実行コスト: 中～重。

### `scripts/build_motor_section_base.py`
- 概要: motor section特徴のベース集約を作成。
- 役割: N窓特徴の前段データ生成。
- called_by: `run_build_motor_pipeline.ps1`/手動。
- 入出力: `--input` → `--out_csv`。
- 主な引数: `--input`, `--out_csv`(必須) + 列名指定群。
- 実行例: `python scripts/build_motor_section_base.py --input data/processed/master.csv --out_csv data/processed/motor/motor_section_base.csv`
- 依存関係: 下流 `build_motor_section_features_n.py`。
- 失敗と対処: 列名差異は *_col 引数で調整。
- 実行コスト: 中。

### `scripts/build_motor_section_features_n.py`
- 概要: section base から N窓平均特徴を展開。
- 役割: `motor_section_features` の本体生成。
- called_by: `run_build_motor_pipeline.ps1`/手動。
- 入出力: `--input` → `--out_csv`。
- 主な引数: `--input`, `--out_csv`(必須), `--mean_ns`(既定`3,5`) 他。
- 実行例: `python scripts/build_motor_section_features_n.py --input ... --out_csv ... --mean_ns 3,5,10`
- 依存関係: 下流 `preprocess_motor_section.py`。
- 失敗と対処: section日付列の型ズレに注意。
- 実行コスト: 中。

### `scripts/export_base_feature_yaml.py`
- 概要: masterからベース特徴量YAMLを出力。
- 役割: feature spec初期化。
- called_by: 手動。
- 入出力: `--master` → `--out`。
- 主な引数: `--master`, `--out`（必須）。
- 実行例: `python scripts/export_base_feature_yaml.py --master data/processed/master.csv --out configs/base_features.yaml`
- 依存関係: 下流 `preprocess_base_features.py`。
- 失敗と対処: 目的列混入に注意。
- 実行コスト: 軽い。

### `scripts/vault_csv_by_pattern.py`
- 概要: パターン一致CSV/binをsqlite vaultへ取り込み。
- 役割: 可搬アーカイブ作成。
- called_by: `run_all_vaults_full_rebuild.ps1`, `run_html_vault_full_rebuild.ps1`。
- 入出力: `--input-dir` + pattern条件 → `--db`。
- 主な引数: `--db`(必須), `--glob`, `--regex`, `--all`, `--start/end`, `--gzip` 他。
- 実行例: `python scripts/vault_csv_by_pattern.py --input-dir data/raw --db data/sqlite/vault.sqlite --regex '^(?P<ymd>\d{8})_raw\.csv$' --all --gzip`
- 依存関係: 下流 `export_vault.py`。
- 失敗と対処: DB肥大化時はVACUUM。
- 実行コスト: 重い（I/O大）。

### `scripts/export_vault.py`
- 概要: sqlite vault からCSVを書き出し。
- 役割: 共有/検証向けエクスポート。
- called_by: 手動。
- 入出力: `--db`, `--dest` → CSV群。
- 主な引数: `--db`(必須), `--dest`(必須), `--pattern`, `--limit`。
- 実行例: `python scripts/export_vault.py --db data/sqlite/csv_vault_2025Q3_compact.sqlite --dest out/vault --pattern '202501.*'`
- 依存関係: 上流 `vault_csv_by_pattern.py`。
- 失敗と対処: pattern誤りで0件（条件確認）。
- 実行コスト: 中。

## 付録: 対象スクリプト確定リスト（29件）

1. `scripts/__init__.py`
2. `scripts/build_live_row.py`
3. `scripts/build_motor_artifacts_from_bins.py`
4. `scripts/build_motor_section_base.py`
5. `scripts/build_motor_section_features_n.py`
6. `scripts/build_raceinfo.py`
7. `scripts/build_raw_csv.py`
8. `scripts/build_raw_with_motor_joined.py`
9. `scripts/build_season_course_prior_from_raw.py`
10. `scripts/build_season_winningtrick_prior_from_raw.py`
11. `scripts/build_tenji_prior_from_raw.py`
12. `scripts/build_timeline_live.py`
13. `scripts/export_base_feature_yaml.py`
14. `scripts/export_vault.py`
15. `scripts/make_master_finals.py`
16. `scripts/predict_one_race.py`
17. `scripts/preprocess.py`
18. `scripts/preprocess_base_features.py`
19. `scripts/preprocess_course.py`
20. `scripts/preprocess_motor_id.py`
21. `scripts/preprocess_motor_section.py`
22. `scripts/preprocess_sectional.py`
23. `scripts/run_odds_scheduler.py`
24. `scripts/scrape.py`
25. `scripts/scrape_odds.py`
26. `scripts/scrape_one_race.py`
27. `scripts/train.py`
28. `scripts/tune_hyperparams.py`
29. `scripts/vault_csv_by_pattern.py`

> ユーザー認識の 29 件と一致。
