# 全レース直前オッズ収集フロー運用ガイド（検証版）

## 1. 概要
既存運用（準優進出戦/準優勝戦/優勝戦のみ保存）は維持したまま、**全レース対象**で保存可否を検証するための別系統です。

- タイムライン生成: `scripts/build_timeline_live.py`（既存のまま）
- スケジューラ: `scripts/run_odds_scheduler_all.py`（新規）
- スクレイパー: `scripts/scrape_odds_all.py`（新規）

## 2. 収集対象と保存先
- 3連単: `data/html/odds3t/YYYYMMDD/odds3tYYYYMMDDxxRR.html`
- 2連単/2連複: `data/html/odds2tf/YYYYMMDD/odds2tfYYYYMMDDxxRR.html`

※ `xx` は2桁場コード、`RR` は2桁レース番号。

## 3. ログ/ステータス出力
- スケジューラログ（人間向け）
  - 既定: `logs/run_odds_scheduler_all.log`
  - `--log_file` で変更可
- ステータスCSV（日次）
  - 既定: `data/odds_status/YYYYMMDD_odds_status.csv`
  - `--status_csv` で変更可

### ステータスCSV主要カラム
- 実行情報: `date, race_id, seq, jcd, rno, title, deadline_dt, scheduled_at, started_at, finished_at`
- 成否情報: `returncode, status, saved_odds3t, saved_odds2tf`
- 補助情報: `stdout_last, stderr_last`

`status` は `success / partial / failed`。

## 4. 実行手順

### 4-1. タイムライン生成（既存）
```bash
python scripts/build_timeline_live.py --date 20260326
```

### 4-2. 全レース版スケジューラ起動（新規）
```bash
python scripts/run_odds_scheduler_all.py --timeline data/timeline/20260326_timeline_live.csv --mins_before 5
```

### 4-3. 単発デバッグ（新規スクレイパー）
```bash
python scripts/scrape_odds_all.py --date 20260326 --jcd 12 --rno 11
```

`scrape_odds_all.py` は最後に必ず `RESULT ...` 行を出力します。

例:
```text
RESULT status=success saved_odds3t=1 saved_odds2tf=1 jcd=12 rno=11 date=20260326
```

## 5. よくあるトラブル
- `timeline` がない
  - 先に `build_timeline_live.py` を実行、または `--timeline` 明示指定。
- 実行が始まらない
  - `deadline_dt - mins_before` が過去時刻の行はスキップされます。
- `partial` / `failed` が出る
  - `stderr_last` とスケジューラログを確認。
  - 片系統（odds3t / odds2tf）のみ保存成功なら `partial`。

## 6. 既存版との使い分け
- 既存本番運用: `scripts/run_odds_scheduler.py` + `scripts/scrape_odds.py`
- 検証運用（全レース）: `scripts/run_odds_scheduler_all.py` + `scripts/scrape_odds_all.py`

既存版は変更せず、検証は新規ファイル側で完結します。
