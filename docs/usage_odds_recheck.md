# odds3t 再利用型 締切再確認スケジューラの使い方

## 追加ファイル
- `scripts/run_odds_scheduler_recheck.py`
- `scripts/scrape_odds_reuse.py`
- `usage_odds_recheck.md`（本書）

## 前提
- 朝の仮タイムラインは既存 `scripts/build_timeline_live.py` で作成します。
- 既存版（`scripts/run_odds_scheduler_all.py`, `scripts/scrape_odds_all.py`）は変更しません。

## 1) タイムライン作成（既存）
```bash
python scripts/build_timeline_live.py --date 20260326
```

## 2) recheck版スケジューラ実行
```bash
python scripts/run_odds_scheduler_recheck.py \
  --timeline data/timeline/20260326_timeline_live.csv \
  --mins_before 5 \
  --log_file logs/run_odds_scheduler_recheck.log
```

必要なら status CSV の保存先を固定できます。
```bash
python scripts/run_odds_scheduler_recheck.py \
  --timeline data/timeline/20260326_timeline_live.csv \
  --mins_before 5 \
  --status_csv data/odds_status/20260326_odds_status.csv
```

## 挙動（odds3t再利用型 recheck）
1. timeline の `deadline_dt` を朝時点の仮締切としてジョブ登録。
2. 各ジョブ発火時に `odds3t` を毎回取得（recheckソース）。
3. 取得した `odds3t` HTML から「締切予定時刻」を再解析。
4. `latest_deadline_dt > now + mins_before` の場合のみ再スケジュール。
5. 実行タイミングになったら、取得済み odds3t HTML をそのまま保存対象として再利用し、`odds2tf` を追加取得して保存。
6. recheck失敗時も取り逃し回避を優先し、その時点の取得済み odds3t を保存対象として `odds2tf` 取得へ進む。

## ログの見方
ログ出力先: `logs/run_odds_scheduler_recheck.log`

主要タグ:
- 起動、timeline 読込
- `[SCHEDULED]`
- `[RECHECK]`
- `[RESCHEDULE]`
- `[RECHECK_FAIL]`
- `[START]`
- `[END]`
- `[SUMMARY]`

### recheck 正常系ログ例
```text
[RECHECK] seq=31 race_id=202603260703 initial_deadline=2026-03-26 14:35 latest_deadline=2026-03-26 14:35
[START] seq=31 race_id=202603260703 jcd=7 rno=3 title=... 
[END] seq=31 race_id=202603260703 returncode=0 status=success saved_odds3t=1 saved_odds2tf=1 recheck_ok=1 reschedule_count=0
```

### reschedule 発生時ログ例
```text
[RECHECK] seq=31 race_id=202603260703 initial_deadline=2026-03-26 14:35 latest_deadline=2026-03-26 14:45
[RESCHEDULE] seq=31 race_id=202603260703 old_run_at=2026-03-26 14:30:00 new_run_at=2026-03-26 14:40:00 latest_deadline=2026-03-26 14:45 count=1
```

## status CSV の追加列
既存列に加えて次を記録します。
- `initial_deadline_dt`: 朝タイムラインの仮締切
- `latest_deadline_dt`: recheckで取得した最新締切（取得不可なら空）
- `recheck_ok`: 再確認が成功したか（1/0）
- `reschedule_count`: 何回延期再登録されたか

既定保存先:
- `data/odds_status/YYYYMMDD_odds_status.csv`

## 通常系テスト手順
1. タイムライン作成。
2. `--mins_before` を小さめ（例: `1`）で recheck版スケジューラ実行。
3. ログで `[RECHECK] -> [START] -> [END]` を確認。
4. status CSV で `status`, `recheck_ok`, `latest_deadline_dt` を確認。
5. `data/html/odds3t/<date>/...` と `data/html/odds2tf/<date>/...` の両方が保存されることを確認。

## 締切延期テスト手順（開発用）
`--debug_force_deadline_delay_minutes N` を指定すると、recheckで取得した最新締切に疑似的に N分加算して再スケジュールを再現できます。

```bash
python scripts/run_odds_scheduler_recheck.py \
  --timeline data/timeline/20260326_timeline_live.csv \
  --mins_before 5 \
  --debug_force_deadline_delay_minutes 10
```

確認ポイント:
- `[RECHECK]` の `latest_deadline` が延長される
- `[RESCHEDULE]` が出る
- 再発火時に `[START]` / `[END]` が出る
- `reschedule_count` が 1 以上になる

## 既存版との違い
- 既存 `run_odds_scheduler_all.py`: タイムライン時刻でそのまま実行し、`scrape_odds_all.py` が都度 `odds3t/odds2tf` を新規取得。
- 新規 `run_odds_scheduler_recheck.py`: 発火時に `odds3t` を必ず再取得して締切再確認し、取得済み `odds3t` を `scrape_odds_reuse.py` へ受け渡して再利用保存。
- recheck失敗時も fail-fast せず、取得機会を優先して保存処理へ進む。
