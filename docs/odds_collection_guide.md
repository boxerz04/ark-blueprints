# 直前オッズ収集 運用ガイド（現行 canonical）

このドキュメントは、`ark-blueprints` における**直前オッズ取得の現行運用**を 1 本化した canonical ガイドです。  
対象は Raspberry Pi + SSD 上の cron 本番運用で、旧仕様（対象レース限定保存 / Windows 常駐前提）は扱いません。

---

## 1. 概要

### 何を取得するか
- `odds3t`（3連単オッズ）
- `odds2tf`（2連単 / 2連複オッズ）

### 何のためのデータか
- レース締切直前のオッズスナップショットを保存し、分析・検証・再現に使うため。
- 「朝時点の締切」ではなく、発火時の再確認を挟んだ時系列整合データを残すため。

### 対象レース
- **現行は全レース対象（1〜12R × 開催場）**。
- 旧運用の「準優進出戦 / 準優勝戦 / 優勝戦のみ保存」は廃止済み。

---

## 2. 現行フロー全体図

1. **7:45**: `scripts/build_timeline_live.py` で当日タイムラインを生成。
2. **8:15**: `scripts/run_odds_scheduler_recheck.py` を起動し、各レースの「締切5分前」でジョブ登録。
3. 各ジョブ発火時に `odds3t` を再取得して締切を再確認（recheck）。
4. もし `latest_deadline_dt` が先に延びていれば、再計算した時刻で再スケジュール。
5. 実行タイミング到達時は、**prefetched `odds3t` を再利用保存**し、`odds2tf` を追加取得して保存。
6. recheck 失敗時も取り逃し回避を優先し、保存処理へ進む（必要時 fallback）。
7. 実行結果は status CSV / logs / HTML に記録。

---

## 3. 主要スクリプトの役割

### `scripts/build_timeline_live.py`
- boatrace の `raceresult` から当日レースの `deadline_dt` / タイトル等を抽出。
- `data/timeline/YYYYMMDD_timeline_live.csv` を生成。

### `scripts/run_odds_scheduler_recheck.py`
- タイムラインを読み、`deadline_dt - 5分` でジョブ登録。
- 発火時に `odds3t` を再取得して締切再確認。
- 締切が後ろに動いた場合は `[RESCHEDULE]`。
- 再確認成功時は `scrape_odds_reuse.py` を実行。
- 再確認で HTML 取得不能などの場合は `scrape_odds_all.py` を fallback 実行。
- status CSV（`data/odds_status/YYYYMMDD_odds_status.csv`）へ行追加。

### `scripts/scrape_odds_reuse.py`
- recheck で取得済み `odds3t` HTML をそのまま保存。
- 追加で `odds2tf` を取得・保存。
- `RESULT status=... saved_odds3t=... saved_odds2tf=...` を出力。

### `scripts/scrape_odds_all.py`（fallback）
- `odds3t` / `odds2tf` を新規取得して保存。
- recheck 処理で prefetched HTML が使えないときの保険。

---

## 4. 保存先

- タイムライン: `data/timeline/`
  - 例: `data/timeline/20260326_timeline_live.csv`
- status CSV: `data/odds_status/`
  - 例: `data/odds_status/20260326_odds_status.csv`
- `odds3t` HTML: `data/html/odds3t/YYYYMMDD/`
  - 例: `data/html/odds3t/20260326/odds3t202603261205.html`
- `odds2tf` HTML: `data/html/odds2tf/YYYYMMDD/`
  - 例: `data/html/odds2tf/20260326/odds2tf202603261205.html`
- ログ: `logs/`
  - 例: `logs/build_timeline_live.log`, `logs/run_odds_scheduler_recheck.log`

---

## 5. status CSV の主要カラム

- `status`: `success / partial / failed`
- `saved_odds3t`: `odds3t` 保存成功なら `1`、失敗なら `0`
- `saved_odds2tf`: `odds2tf` 保存成功なら `1`、失敗なら `0`
- `initial_deadline_dt`: 朝 timeline の締切
- `latest_deadline_dt`: recheck で再取得した最新締切（取得不可なら空）
- `recheck_ok`: 締切再確認に成功したか（`1/0`）
- `reschedule_count`: 再スケジュール回数

補助確認でよく使う列:
- `scheduled_at`, `started_at`, `finished_at`
- `stdout_last`, `stderr_last`

---

## 6. cron 本番運用（Raspberry Pi + SSD）

運用前提:
- プロジェクト配置: `/mnt/ssd/projects/ark-blueprints`
- 同一日で重複起動させないため `flock` を使用
- `flock` は二重実行による重複保存・ログ混線・競合を防ぐため

`crontab -e` 例:

```cron
# 7:45 タイムライン生成
45 7 * * * cd /mnt/ssd/projects/ark-blueprints && \
  /usr/bin/flock -n /tmp/ark_build_timeline.lock \
  /usr/bin/python3 scripts/build_timeline_live.py >> logs/cron_build_timeline.log 2>&1

# 8:15 recheck スケジューラ起動（締切5分前運用）
15 8 * * * cd /mnt/ssd/projects/ark-blueprints && \
  /usr/bin/flock -n /tmp/ark_odds_recheck.lock \
  /usr/bin/python3 scripts/run_odds_scheduler_recheck.py --mins_before 5 >> logs/cron_odds_recheck.log 2>&1
```

---

## 7. 実行例

### 手動: タイムライン生成
```bash
cd /mnt/ssd/projects/ark-blueprints
python3 scripts/build_timeline_live.py --date 20260326
```

### 手動: recheck スケジューラ起動
```bash
cd /mnt/ssd/projects/ark-blueprints
python3 scripts/run_odds_scheduler_recheck.py \
  --timeline data/timeline/20260326_timeline_live.csv \
  --mins_before 5
```

### 単発確認コマンド
```bash
# 最新 timeline 先頭確認
head -n 5 data/timeline/20260326_timeline_live.csv

# status 確認
tail -n 20 data/odds_status/20260326_odds_status.csv

# 保存件数確認
find data/html/odds3t/20260326 -type f | wc -l
find data/html/odds2tf/20260326 -type f | wc -l
```

---

## 8. ログの読み方

`logs/run_odds_scheduler_recheck.log` の主要タグ:

- `[SCHEDULED]`: ジョブ登録完了
- `[RECHECK]`: 発火時の締切再確認結果（initial / latest）
- `[RESCHEDULE]`: 締切延長により再登録
- `[RECHECK_FAIL]`: 再確認失敗（取得不能 / 解析不能 など）
- `[START]`: 保存処理開始
- `[END]`: 保存処理終了（returncode, status, 保存成否）
- `[SUMMARY]`: 日次サマリ（total/success/partial/failed）

---

## 9. よくあるトラブル

### 1) timeline が無い
- 症状: `timeline CSV が指定されず...` で終了
- 対処: `build_timeline_live.py` を先に実行、または `--timeline` 明示

### 2) recheck 失敗
- 症状: `[RECHECK_FAIL]`
- 対処:
  - まずは取り逃し回避が優先され、保存処理へ進む設計
  - HTML 空などで reuse 不可なら fallback（`scrape_odds_all.py`）が実行される

### 3) fallback が動くケース
- prefetched `odds3t` が空 / 取得不可 / 例外発生
- ログで `[FALLBACK] ... scrape_odds_all.py` を確認

### 4) status が `partial` / `failed`
- `partial`: `odds3t` or `odds2tf` の片方のみ成功
- `failed`: 両方失敗
- `stdout_last`, `stderr_last`, `[END]` 行で原因追跡

### 5) 取得ファイル数の確認
```bash
find data/html/odds3t/20260326 -type f | wc -l
find data/html/odds2tf/20260326 -type f | wc -l
```

---

## 10. 運用上の補足

- 現行設定の取得タイミングは **締切5分前固定**（`--mins_before 5`）。
- 5分前採用理由:
  - 通信遅延・再確認・再スケジュール余地を確保しつつ、締切直前性を維持できるバランス。
- 将来的に 4分前などへ調整する余地はあるが、**現行本番値は 5分前**。

---

## 11. 旧仕様からの変更点

- 対象レース限定保存（準優進出戦 / 準優勝戦 / 優勝戦のみ）を廃止。
- Windows タスクスケジューラ常駐前提から、Raspberry Pi + SSD の cron 運用へ移行。
- recheck なし運用から、**recheck あり（`run_odds_scheduler_recheck.py`）を現行標準**へ移行。

---

## ドキュメント整理方針（本リポジトリ）

- canonical: `docs/odds_collection_guide.md`
- 旧 odds ドキュメントは重複回避のため削除。
- top-level の `usage_odds*.md` は現時点で存在しないため新規作成しない（正本は docs 配下に一本化）。
