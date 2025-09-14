# 直前オッズ収集フロー

準優・優勝戦の直前オッズを自動収集するフローです。

---

## 1. タイムライン生成
毎日 8:30 にタスクスケジューラで自動起動

未確定レースの締切予定時刻を取得し、タイムラインCSVを作成します。
```bash
# 今日の日付を対象に処理
python scripts/build_timeline_live.py

# 日付を指定して処理
python scripts/build_timeline_live.py --date 2025-09-14
```
👉 出力:
- `data/timeline/YYYYMMDD_timeline_live.csv`
---
## 2. スケジューラによる直前オッズ収集
毎日 12:00 にタスクスケジューラで自動起動

タイムラインをもとに、締切5分前に scrape_odds.py を自動実行します

準優進出戦・準優勝戦・優勝戦のみ対象です
```bash
# timeline 未指定 → data/timeline から最新を自動検出
python scripts/run_odds_scheduler.py --mins_before 5

# timeline を明示指定
python scripts/run_odds_scheduler.py --timeline data/timeline/20250914_timeline_live.csv --mins_before 5
```
👉 出力:
- `data/html/odds3t/YYYYMMDD/odds3tYYYYMMDDxxRR.html`
- `data/html/odds2tf/YYYYMMDD/odds2tfYYYYMMDDxxRR.html`
---
## 3. 単発スクレイピング（デバッグ用）
- 個別に1レースのオッズを取得したい場合は直接呼び出します。
```bash
python scripts/scrape_odds.py --date 20250914 --jcd 12 --rno 11
```
👉 対応するHTMLが保存されます。
