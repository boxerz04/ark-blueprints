# 直前オッズ収集フロー運用ガイド

## A. 目的（何を集めるか、いつ使うか）
このフローは、**当日レースのうち「準優進出戦・準優勝戦・優勝戦」だけを対象**に、
締切直前のオッズHTMLを自動保存する運用手順です。

- 収集対象オッズ
  - 3連単: `odds3t`
  - 2連単/2連複: `odds2tf`
- 主な利用タイミング
  - 当日予測・分析バッチの直前データ取得
  - レース締切前の最終オッズ確認

前提として、Windows タスクスケジューラで次を毎日実行します。

- **8:30**: `build_timeline_live.py`
- **12:00**: `run_odds_scheduler.py`

---

## B. 全体フロー（8:30 → 12:00 → 締切5分前実行）

### 1) 8:30 `build_timeline_live.py`（タイムライン作成）
- 入力
  - 対象日（省略時は当日、`--date YYYYMMDD` で指定可能）
  - boatrace の `raceresult` ページ（24場 x 1〜12R）
- 処理
  - 各場の開催可否（中止/順延）を確認
  - 「締切予定時刻」を抽出して `deadline_dt` を作成
  - 1〜12Rのタイトルを取得
  - `deadline_dt` を時刻順に並び替え、`seq` を振り直し
- 出力
  - `data/timeline/YYYYMMDD_timeline_live.csv`
- ログ
  - `logs/build_timeline_live.log`
  - 標準出力にも保存先などを表示

### 2) 12:00 `run_odds_scheduler.py`（ジョブ登録・常駐）
- 入力
  - タイムラインCSV（`--timeline` 指定、または `data/timeline/*_timeline_live.csv` から最新自動選択）
  - `--mins_before`（デフォルト5）
- 処理
  - `deadline_dt - mins_before` を実行時刻としてレースごとにジョブ登録
  - 過去時刻（12:00時点ですでに実行時刻を過ぎたレース）は登録しない
  - 登録後は `schedule.run_pending()` で待機し、時刻到達で `scrape_odds.py` を起動
- 重要
  - **`run_odds_scheduler.py` は単発バッチではなく、「12:00に起動して、その日登録された締切時刻まで常駐」します。**
  - つまり実運用では **「12:00から当日レースの締切まで常駐」** が正しい挙動です。
- ログ
  - `logs/run_odds_scheduler.log`
  - 標準出力にも同内容を出力

### 3) 締切5分前 `scrape_odds.py`（対象レースのみ保存）
- 入力
  - `--date YYYYMMDD --jcd 場コード --rno レース番号`
- 処理
  - `odds3t` / `odds2tf` を取得
  - ページタイトル（`h3.title16_titleDetail__add2020`）を確認
  - 次のいずれかを含む場合のみ保存
    - `準優進出戦`
    - `準優勝戦`
    - `優勝戦`
- 標準出力
  - 保存時: `[SAVED] ...`
  - 対象外: `[SKIP] ...`
  - 取得失敗: `[ERROR] ...`

---

## C. 成果物（timeline CSV / odds HTML）

### 1) タイムラインCSV
- パス規約
  - `data/timeline/YYYYMMDD_timeline_live.csv`
- 主要カラム
  - `seq`: 時刻順の連番（1始まり、再採番済み）
  - `race_id`: `YYYYMMDD + jcd(2桁) + rno(2桁)`
  - `jcd`: 先頭ゼロなし（例: `05` → `5`）
  - `rno`: レース番号
  - `title`: レースタイトル
  - `deadline`: HH:MM
  - `deadline_dt`: `YYYY-MM-DD HH:MM`

### 2) オッズHTML
- 3連単
  - `data/html/odds3t/YYYYMMDD/odds3tYYYYMMDDxxRR.html`
- 2連単/2連複
  - `data/html/odds2tf/YYYYMMDD/odds2tfYYYYMMDDxxRR.html`
- `xx` は2桁場コード、`RR` は2桁レース番号

---

## D. 実行方法

### 手動実行（当日/日付指定）
```bash
# 当日タイムライン作成
python scripts/build_timeline_live.py

# 日付指定でタイムライン作成
python scripts/build_timeline_live.py --date 20250914
```

```bash
# スケジューラ起動（timeline自動検出）
python scripts/run_odds_scheduler.py --mins_before 5

# スケジューラ起動（timeline明示指定）
python scripts/run_odds_scheduler.py --timeline data/timeline/20250914_timeline_live.csv --mins_before 5
```

```bash
# 単発スクレイピング（デバッグ用）
python scripts/scrape_odds.py --date 20250914 --jcd 12 --rno 11
```

### timeline 自動検出と明示指定
- 自動検出（`--timeline` なし）
  - `data/timeline/` の `*_timeline_live.csv` から、ファイル名先頭の `YYYYMMDD` が最大のものを採用
- 明示指定（`--timeline` あり）
  - 任意日のCSVを確実に使いたい場合に推奨

### `mins_before` の意味
- `run_at = deadline_dt - mins_before 分`
- 例: `mins_before=5` なら締切5分前に `scrape_odds.py` を起動
- 12:00起動時点で `run_at` を過ぎているレースはスキップ

---

## E. 監視・ログ

### `logs/build_timeline_live.log`
- 取得失敗（HTTP異常、パース失敗）
- 出力CSV保存先
- 当日対象が空だった場合の警告

### `logs/run_odds_scheduler.log`
- 使用したtimeline（自動選択/明示指定）
- 各ジョブの登録時刻（`[SCHEDULED]`）
- 登録0件時の終了理由
- 全ジョブ消化後の正常停止

### `scrape_odds.py` の標準出力
- `[SKIP]`: タイトルが対象外（準優進出戦/準優勝戦/優勝戦に非該当）
- `[SAVED]`: 対象タイトルのためHTML保存
- `[ERROR]`: 通信・保存などの例外

---

## F. よくあるトラブルと対処

### 1) timeline が無い
- 症状
  - `timeline CSV が指定されず、自動検出もできませんでした。終了します。`
- 対処
  1. `build_timeline_live.py` を先に実行してCSV生成
  2. `--timeline` で対象CSVを明示

### 2) `deadline_dt` が parse できない
- 症状
  - `deadline_dt を解釈できません` の警告で該当行がスキップ
- 対処
  1. timeline CSV の `deadline_dt` 形式を `YYYY-MM-DD HH:MM` に統一
  2. 必要なら 8:30 のタイムライン生成を再実行

### 3) タスクスケジューラで多重起動しそう
- 注意点
  - `run_odds_scheduler.py` は常駐型なので、同時刻に複数起動すると重複実行の原因になる
- 推奨
  - タスク設定を「既存インスタンスがある場合は新しいインスタンスを開始しない」にする
  - 実行時間上限は当日最終締切までをカバーする値にする

### 4) 当日すでに過ぎたレースはスキップされる
- 仕様
  - 12:00時点（または手動起動時点）で `deadline_dt - mins_before` が過去なら登録しない
- 対処
  - より早い時刻にスケジューラを起動する
  - `mins_before` を短くして取りこぼしを減らす

### 5) 「対象タイトルなし」の挙動
- `run_odds_scheduler.py` はタイトルで事前フィルタしていないため、
  タイムラインに基づきジョブを登録し `scrape_odds.py` を起動します。
- 実際の保存可否は `scrape_odds.py` 側で判定され、対象外タイトルは `[SKIP]` となり保存されません。
- その結果、保存ファイルが0件でもスケジューラ自体は正常終了し得ます。

---

## G. 設定（Windows タスクスケジューラ登録例）

> 例では Python を `C:\Python312\python.exe`、プロジェクトを `C:\work\ark-blueprints` とします。

### 1) 8:30 タイムライン生成タスク
- プログラム/スクリプト
  - `C:\Python312\python.exe`
- 引数の追加
  - `scripts\build_timeline_live.py`
- 開始（作業フォルダ）
  - `C:\work\ark-blueprints`
- 実行結果ログ
  - `logs\build_timeline_live.log`

### 2) 12:00 直前オッズスケジューラタスク
- プログラム/スクリプト
  - `C:\Python312\python.exe`
- 引数の追加（自動検出運用）
  - `scripts\run_odds_scheduler.py --mins_before 5`
- あるいは引数の追加（明示指定運用）
  - `scripts\run_odds_scheduler.py --timeline data\timeline\20250914_timeline_live.csv --mins_before 5`
- 開始（作業フォルダ）
  - `C:\work\ark-blueprints`
- 実行結果ログ
  - `logs\run_odds_scheduler.log`

### 運用上の補足
- 12:00起動タスクは常駐するため、タスクの「停止条件」を短くしすぎない
- 当日最終レース締切まで動かせるよう、実行時間制限を設定
- 二重起動防止設定を有効化
