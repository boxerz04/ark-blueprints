# scripts/build_live_row.py

## 1. 概要

| 項目 | 内容 |
|---|---|
| 目的 | レース直前に **1レース分の raw 相当6行データ** を生成する |
| 実行単位 | `date + jcd + race` の1レース |
| 主な利用シーン | ライブ推論前の特徴量生成 |
| used_by（SSOT） | `inference_pipeline`, `gui`, `usage_odds` |

## 2. 入出力

### 2.1 入力

| 種別 | パス/形式 | 必須 | 説明 |
|---|---|:---:|---|
| HTML（オンライン） | Boatrace公式 (`pay/index/racelist/pcexpect/beforeinfo/raceindex`) | 任意 | `--online` 時に取得しキャッシュ |
| HTML（オフライン） | `data/live/html/**` → `data/html/**` | 任意 | ローカルキャッシュ探索順 |
| 参照CSV（列順整列） | `data/raw/*_raw.csv` | 任意 | 既存 raw の列順に自動整列 |
| CLI引数 | `--date --jcd --race [--online --out]` | 必須/任意 | 対象レース指定 |

### 2.2 出力

| 種別 | パス/形式 | 説明 |
|---|---|---|
| ファイル | `data/live/raw_YYYYMMDD_JCD_R.csv` | 6艇×1レースの raw 互換CSV |
| ログ/終了コード | exit code `2`（展示STが非数値を含む場合） | 安全のため推論入力生成を中止 |

## 3. 主な仕様（要点）

| 観点 | 内容 |
|---|---|
| データソース | `raceresult` は参照せず、公開HTMLのみで構築 |
| race_id | `YYYYMMDD + jcd2桁 + R2桁` |
| 空列 | `entry`, `is_wakunari` は将来用に `Int64` の NA |
| 枠番 | `beforeinfo` 由来。欠損時は `1..6` で補完 |
| ST展示 | `ST_tenji` を数値化し `ST_tenji_rank` を自動生成（小さい値=上位） |

## 4. 処理フロー

| Step | 処理 | 失敗時挙動 |
|---|---|---|
| 1 | `pay/index/racelist/pcexpect/beforeinfo/raceindex` を取得・読込 | 取得不可時はキャッシュ探索、必要データ不足でエラー |
| 2 | `parse_pay`, `parse_index`, `parse_st` などで正規化 | 解析不能値は欠損で保持 |
| 3 | `build_live_raw()` で6行DataFrameへ統合 | ST非数値（L単独等）を検知した場合は中止 |
| 4 | 列順整列してCSV保存 | 参照raw不在時は整列をスキップして保存 |

## 5. CLI 引数

| 引数 | 必須 | デフォルト | 説明 | 例 |
|---|:---:|---|---|---|
| `--date` | ✅ | なし | 開催日（`YYYYMMDD`） | `20250903` |
| `--jcd` | ✅ | なし | 場コード（2桁 or 数値） | `12` |
| `--race` | ✅ | なし | レース番号（1〜12） | `3` |
| `--online` |  | `False` | 公式HTMLを直接取得 | `--online` |
| `--out` |  | 規定パス | 出力CSVパス | `data/live/raw_20250903_12_03.csv` |

## 6. 実行例

```bash
python scripts/build_live_row.py \
  --date 20250903 --jcd 12 --race 3 --online \
  --out data/live/raw_20250903_12_03.csv
```

## 7. 運用メモ / 注意点

| 観点 | 内容 |
|---|---|
| 結果未確定時 | `raceresult` 不使用のため、結果確定前でも生成可能 |
| キャッシュ探索 | `data/live/html` → `data/html` の順 |
| 列順整列 | `data/raw/*_raw.csv` が無い場合はスキップ |
| 推論互換 | 出力は `predict_one_race.py` に直接入力可能 |
