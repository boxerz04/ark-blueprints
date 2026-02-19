# scripts ドキュメント（一覧 + 個票）

`docs/scripts/` は、`scripts/*.py` の運用ドキュメントを **スケール可能な構成** で管理するためのディレクトリです。  
「入口（SSOT）→ 一覧（catalog）→ 個票（script detail）」の順で参照します。

## 読み方（運用者向け）

| ステップ | 参照先 | 目的 |
|---|---|---|
| 1 | 入口ドキュメント（SSOT） | 業務フロー全体でどのスクリプトを使うか把握 |
| 2 | [`catalog.md`](./catalog.md) | 対象スクリプトの責務・I/O・入口との紐付けを一覧で確認 |
| 3 | 各個票（例: [`build_live_row.md`](./build_live_row.md)） | 引数・処理フロー・注意点を詳細確認 |
| 4 | 新規追記時 | [`_template.md`](./_template.md) | 同じフォーマットで個票を追加 |

## 現行入口（SSOT）

以下を「入口（Single Source of Truth）」として扱います。詳細は catalog の `used_by` 列でも明示します。

| 種別 | 入口（SSOT） | 備考 |
|---|---|---|
| 学習/データ作成 | `docs/training_pipeline.md` | 学習データ生成・前処理の全体導線 |
| オッズ/運用 | `docs/usage_odds.md` | 運用系バッチ・日次運用導線 |
| 推論（ライブ） | `docs/inference_pipeline.md` | GUI/CLI 共通の推論導線 |
| 事前分布（prior） | `docs/priors_pipeline.md` | prior 更新手順の入口 |
| バッチ実行 | `batch/*.ps1` | 実運用の定常バッチ入口 |
| GUI 実行 | `gui_predict_one_race.py` | 1レース推論の GUI 入口 |

## 目次

| スクリプト | 個票 |
|---|---|
| `build_live_row.py` | [`build_live_row.md`](./build_live_row.md) |
| `build_raceinfo.py` | [`build_raceinfo.md`](./build_raceinfo.md) |
| `build_raw_csv.py` | [`build_raw_csv.md`](./build_raw_csv.md) |
| `build_season_course_prior_from_raw.py` | [`build_season_course_prior_from_raw.md`](./build_season_course_prior_from_raw.md) |

## 追記ルール（簡易）

| ルール | 内容 |
|---|---|
| 1 | 新規 script 追加時は、まず `catalog.md` に1行追加 |
| 2 | 次に `_template.md` を複製して個票を作成 |
| 3 | `used_by` に入口（SSOT）を必ず明記 |
| 4 | 引数・I/O・失敗時の挙動は表で記載 |
| 5 | 互換リンクのため、必要に応じて `docs/scripts.md` から誘導リンクを更新 |
