# scripts/build_raw_csv.py

## 1. 概要

| 項目 | 内容 |
|---|---|
| 目的 | 日次で「raw / refund」CSVを生成する |
| 実行単位 | 指定日の全開催場 × 1〜12R |
| 主な利用シーン | 学習データ作成の基盤データ構築 |
| used_by（SSOT） | `training_pipeline`, `batch_ps1` |

## 2. 入出力

### 2.1 入力

| 種別 | パス/形式 | 必須 | 説明 |
|---|---|:---:|---|
| HTML（pay） | `data/html/pay/*.bin` | ✅ | 開催場・グレード・属性 |
| HTML（index） | `data/html/index/*.bin` | ✅ | 開催タイトル・日程情報 |
| HTML（racelist） | `data/html/racelist/*.bin` | ✅ | 出走表 |
| HTML（pcexpect） | `data/html/pcexpect/*.bin` | ✅ | 予想印・進入固定/安定板 |
| HTML（beforeinfo） | `data/html/beforeinfo/*.bin` | ✅ | 展示・気象 |
| HTML（raceresult） | `data/html/raceresult/*.bin` | ✅ | 結果（着/ST/決まり手等） |
| HTML（raceindex） | `data/html/raceindex/*.bin` | ✅ | 性別推定補助 |
| CLI引数 | `--date` |  | 対象日（未指定時は当日） |

### 2.2 出力

| 種別 | パス/形式 | 説明 |
|---|---|---|
| ファイル | `data/raw/YYYYMMDD_raw.csv` | 学習互換の生特徴量 |
| ファイル | `data/refund/YYYYMMDD_refund.csv` | 払戻テーブル（レースID付） |
| 一時ファイル | `data/{race_id}_raw.pickle`, `..._refund.pickle` | 最終結合後に削除 |

## 3. 処理フロー

| Step | 処理 | 失敗時挙動 |
|---|---|---|
| 1 | `pay/index` を読み開催一覧を作成 | 欠落時はログ警告 |
| 2 | 各 `code × R(1..12)` で `race_id` を生成 | 欠損レースはスキップ |
| 3 | `racelist` から選手基本情報を抽出 | 取得不能値は欠損 |
| 4 | `pcexpect` / `beforeinfo` を結合（予想印・展示・気象） | 部分欠落は継続 |
| 5 | `raceresult` / `raceindex` を結合（結果・性別） | 解析不能でも可能範囲で継続 |
| 6 | 列正規化・`section_id` 付与・日次CSV保存 | 最後に一時pickleを削除 |

## 4. 主な仕様（抜粋）

| 観点 | 内容 |
|---|---|
| `section_id` | `YYYYMMDD_code` を付与（集計キー） |
| 列リネーム | 日本語列を学習ノート準拠へ正規化（例: `展示 タイム`→`time_tenji`） |
| 欠落時方針 | 荒天・不成立・展示欠落等はログ出力して継続 |
| 対象範囲 | 1日ぶんの全「場 × 12R」 |

## 5. CLI 引数

| 引数 | 必須 | デフォルト | 説明 | 例 |
|---|:---:|---|---|---|
| `--date` |  | 当日 | 処理対象日（`YYYY-MM-DD` or `YYYYMMDD`） | `--date 20250907` |

## 6. 実行例

```bash
python scripts/build_raw_csv.py
python scripts/build_raw_csv.py --date 2025-09-07
python scripts/build_raw_csv.py --date 20250907
```

## 7. 主要出力カラム（raw）

| 区分 | 例 |
|---|---|
| 基本キー | `race_id, date, code, R, wakuban, section_id` |
| 選手属性 | `player, player_id, AB_class, age, weight, team, origin` |
| モーター/艇 | `motor_number, motor_2rentai_rate, boat_number, boat_2rentai_rate` |
| 展示/気象 | `entry_tenji, ST_tenji, time_tenji, Tilt, weather, wind_speed, wave_height` |
| レース情報 | `title, day, section, schedule, race_grade, race_type, race_attribute` |
| 結果 | `rank, ST, ST_rank, winning_trick, henkan_ticket, remarks` |

## 8. 運用メモ / 注意点

| 観点 | 内容 |
|---|---|
| 前提 | `.bin` が `data/html/**` に存在する前提 |
| 障害耐性 | HTML欠落・構造差異時も可能な範囲で継続 |
| クリーンアップ | 実行末尾で `data/*.pickle` を削除 |
