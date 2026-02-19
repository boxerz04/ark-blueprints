# scripts/build_raceinfo.py

## 1. 概要

| 項目 | 内容 |
|---|---|
| 目的 | `racelist` の `.bin` から日次「今節スナップショット」CSVを生成 |
| 実行単位 | 単日 / 日付範囲 / 利用可能日全件 |
| 主な利用シーン | 学習前処理で使う raceinfo の日次更新 |
| used_by（SSOT） | `training_pipeline`, `batch_ps1` |

## 2. 入出力

### 2.1 入力

| 種別 | パス/形式 | 必須 | 説明 |
|---|---|:---:|---|
| HTMLバイナリ | `data/html/racelist/*.bin` | ✅ | レースリスト元データ |
| 解析ロジック | `src/raceinfo_features.py` | ✅ | `process_racelist_content`, `calculate_raceinfo_points` を利用 |
| CLI引数 | `--date` / `--start-date --end-date` / `--all-available` | ✅ | 対象日指定 |

### 2.2 出力

| 種別 | パス/形式 | 説明 |
|---|---|---|
| ファイル | `data/processed/raceinfo/raceinfo_YYYYMMDD.csv` | 日次 raceinfo |
| ログ | 処理日ごとの件数・合計件数 | 進捗確認用 |

## 3. 提供関数

| 関数 | 役割 | 補足 |
|---|---|---|
| `process_one_day(html_dir, out_dir, ymd)` | 指定日の `.bin` 群を処理し日次CSVを作成 | `.bin` 不在時は `None` |
| `extract_dates_from_filenames(dirpath)` | ファイル名中の `YYYYMMDD` を抽出 | `--all-available` で利用 |
| `iter_dates_from_range(start, end)` | 開始〜終了（両端含む）日付を生成 | 1日刻み |
| `main()` | 引数解析と日次ループ制御 | I/O とフロー制御中心 |

## 4. 処理フロー

| Step | 処理 | 失敗時挙動 |
|---|---|---|
| 1 | 対象日を決定（単日/範囲/自動抽出） | 不正日付はエラー |
| 2 | `.bin` を読み込み `process_racelist_content()` でDataFrame化 | 個別失敗はログ出力 |
| 3 | `calculate_raceinfo_points()` でポイント算出 | 算出不可行は欠損/除外 |
| 4 | `raceinfo_YYYYMMDD.csv` を出力 | 対象 `.bin` 不在日はスキップ |

## 5. CLI 引数

| 引数 | 必須 | デフォルト | 説明 | 例 |
|---|:---:|---|---|---|
| `--date` | 条件付き必須 | なし | 単一日付を処理 | `--date 20250901` |
| `--start-date` | 条件付き必須 | なし | 範囲開始日 | `--start-date 20250901` |
| `--end-date` | 条件付き必須 | なし | 範囲終了日 | `--end-date 20250903` |
| `--all-available` | 条件付き必須 | `False` | `.bin` 名から日付自動抽出 | `--all-available` |
| `--html-dir` |  | `data/html/racelist` | 入力フォルダ | `data/html/racelist` |
| `--out-dir` |  | `data/processed/raceinfo` | 出力フォルダ | `data/processed/raceinfo` |

## 6. 実行例

```bash
python scripts/build_raceinfo.py --date 20250901
python scripts/build_raceinfo.py --start-date 20250901 --end-date 20250910
python scripts/build_raceinfo.py --all-available
```

## 7. 運用メモ / 注意点

| 観点 | 内容 |
|---|---|
| 役割分離 | HTML構造変更は `src/raceinfo_features.py` 側で吸収 |
| race_id | ファイル名中の連続数字から抽出（例: `20240914_racelist_12R.bin` → `2024091412`） |
| 利用先 | 出力CSVは master 生成など学習前処理で利用 |
