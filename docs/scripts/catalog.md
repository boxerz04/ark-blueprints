# scripts カタログ（一覧表）

## 現行入口（SSOT）

このカタログの `used_by` 列は、以下の入口（SSOT）に対する紐付けを示します。

| SSOTキー | 入口 | 意味 |
|---|---|---|
| `training_pipeline` | `docs/training_pipeline.md` | 学習データ生成系の公式入口 |
| `usage_odds` | `docs/usage_odds.md` | 運用手順（オッズ/日次運用）の入口 |
| `inference_pipeline` | `docs/inference_pipeline.md` | 推論運用（GUI/CLI）の入口 |
| `priors_pipeline` | `docs/priors_pipeline.md` | prior 生成・更新の入口 |
| `batch_ps1` | `batch/*.ps1` | PowerShell バッチの実行入口 |
| `gui` | `gui_predict_one_race.py` | GUI エントリポイント |

## scripts 一覧

| script | purpose | main input | main output | used_by（SSOTとの紐付け） | detail |
|---|---|---|---|---|---|
| `build_live_row.py` | 1レース分の live raw 相当6行を生成 | `data/live/html/**` または `data/html/**` | `data/live/raw_YYYYMMDD_JCD_R.csv` | `inference_pipeline`, `gui`, `usage_odds` | [build_live_row.md](./build_live_row.md) |
| `build_raceinfo.py` | racelist `.bin` から日次 raceinfo を生成 | `data/html/racelist/*.bin` | `data/processed/raceinfo/raceinfo_YYYYMMDD.csv` | `training_pipeline`, `batch_ps1` | [build_raceinfo.md](./build_raceinfo.md) |
| `build_raw_csv.py` | 公式HTML群から日次 raw/refund を生成 | `data/html/{pay,index,racelist,pcexpect,beforeinfo,raceresult,raceindex}` | `data/raw/YYYYMMDD_raw.csv`, `data/refund/YYYYMMDD_refund.csv` | `training_pipeline`, `batch_ps1` | [build_raw_csv.md](./build_raw_csv.md) |
| `build_season_course_prior_from_raw.py` | 季節×場×entry別 prior を生成 | `data/raw/*.csv` | `data/priors/season_course/*.csv` | `priors_pipeline`, `batch_ps1`, `training_pipeline` | [build_season_course_prior_from_raw.md](./build_season_course_prior_from_raw.md) |

## 更新時チェックリスト

| チェック項目 | Yes/No |
|---|---|
| `catalog.md` に script 行を追加したか |  |
| 個票を `_template.md` 準拠で作成したか |  |
| `used_by` に SSOTキーを記載したか |  |
| 引数表・入出力表・注意点表を埋めたか |  |
