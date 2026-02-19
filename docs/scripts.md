# scripts/build_live_row.py の役割

レース直前に **1レース分の“raw相当6行データ”** を生成するスクリプト。  
学習用 `raceresult` は参照せず、Boatrace公式のHTML（`pay`, `index`, `racelist`, `pcexpect`, `beforeinfo`, `raceindex`）のみから構築します。  
オンラインモードではHTMLを取得・キャッシュし、`data/live/html/` 配下に保存します。

## 主な仕様

- 出力: `data/live/raw_YYYYMMDD_JCD_R.csv`（6艇×1レース）
- オンライン指定 (`--online`) 時:
  - Boatrace公式からHTMLを直接取得し `data/live/html/<kind>/` にキャッシュ
- オフライン時:
  - `data/live/html` → `data/html` の順にローカルキャッシュを探索
- 出力の基本構造:
  - `race_id = YYYYMMDD + jcd2桁 + R2桁`
  - `entry`, `is_wakunari` は将来用の空列（`Int64`, 全て NA）
  - `wakuban` は `beforeinfo` から生成、欠損時は `1..6` で補完
  - 列順は `data/raw` 内の最新 `*_raw.csv` に自動整列
  - ST展示 (`ST_tenji`) の数値化と `ST_tenji_rank` を自動生成（小さい値=1位）

## 主な処理フロー

1. **HTML取得・キャッシュ**
   - `pay`, `index`, `racelist`, `pcexpect`, `beforeinfo`, `raceindex`
   - BeautifulSoup + `requests` によりパース

2. **解析関数**
   - `parse_pay`: 開催場・グレード・属性を抽出  
   - `parse_index`: 日程・節日数などを抽出  
   - `read_html_tables_robust`: `lxml`／`html5lib` 両対応の堅牢な `read_html` ラッパ  
   - `parse_st`: `'F.01'→-0.01`, `'L.03'→+0.03`, `'.07'→0.07` の形式に統一  

3. **`build_live_raw()`**
   - すべての情報を結合して6行DataFrameを構築  
   - `ST_tenji` の正規化／順位付け  
   - 性別・気象・予想印などを統合  
   - 数値列を型補正  
   - 欠損列（結果系など）は空で埋める  

4. **`main()`**
   - コマンドライン引数を受け取り、`build_live_raw()` を実行  
   - 出力CSVを作成し、自動的に列順を合わせて保存  
   - 展示STが非数値（L単独等）を含む場合は推論中止（exit code 2）

## 引数

| 引数 | 内容 | 例 |
|------|------|----|
| `--date` | 開催日（必須, YYYYMMDD） | `20250903` |
| `--jcd`  | 場コード（2桁 or 数値） | `12` |
| `--race` | レース番号（1〜12） | `3` |
| `--online` | 公式からHTML取得（省略時はローカル参照） | `--online` |
| `--out` | 出力パス | `data/live/raw_20250903_12_03.csv` |

## 実行例

```bash
python scripts/build_live_row.py \
  --date 20250903 --jcd 12 --race 3 --online \
  --out data/live/raw_20250903_12_03.csv
```
## 注意点

- `raceresult` は使用しない（結果が未確定でも出力可）
- `data/live/html` → `data/html` の順でキャッシュを探索
- `data/raw/*_raw.csv` が存在しない場合、列順アラインをスキップ
- `ST_tenji` に非数値が含まれるとエラー終了（安全設計）
- 出力CSVは学習raw互換形式のため、`predict_one_race.py` に直接入力可能
---
# scripts/build_raceinfo.py の役割

`data/html/racelist/*.bin` を解析して、**日次の「今節スナップショット」CSV** を生成するスクリプト。  
HTML解析ロジックはすべて `src/raceinfo_features.py` に委譲し、本スクリプトは  
フロー制御とファイルI/O（入出力・日付処理）のみに徹しています。

## 主な仕様

- 入力: racelistの `.bin` ファイル群（`data/html/racelist`）  
- 出力: `data/processed/raceinfo/raceinfo_YYYYMMDD.csv`  
- 日付指定方式:
  - `--date` 単一日付  
  - `--start-date --end-date` 範囲指定（両端含む）  
  - `--all-available` `.bin` ファイル名から自動抽出
- 各 `.bin` を `src.raceinfo_features` の以下関数で処理  
  - `process_racelist_content()`: HTML解析 → DataFrame化  
  - `calculate_raceinfo_points()`: `ranking_point_map`・`condition_point_map` に基づくポイント付与  
- race_id はファイル名中の連続数字を抽出（例: `20240914_racelist_12R.bin` → `2024091412`）

## 提供関数と挙動

### `process_one_day(html_dir, out_dir, ymd)`
- 指定日 (`ymd`) の `.bin` をまとめて処理。  
- 各 `.bin` → HTML読込 → 特徴抽出 → ポイント算出 → 結合。  
- 出力先 `out_dir/raceinfo_YYYYMMDD.csv` を返す。  
- `.bin` が見つからない場合は `None` を返す。

### `extract_dates_from_filenames(dirpath)`
- `.bin` ファイル名中の 8桁数字 (`YYYYMMDD`) を抽出し、日付一覧を返す。  
- `--all-available` オプションで利用。

### `iter_dates_from_range(start, end)`
- 開始～終了の両端を含む日付レンジを1日刻みで生成。

### `main()`
- CLI引数を解析し、指定範囲の `.bin` を順に処理。  
- 日次ごとの出力を作成し、合計レコード数をログ表示。

## コマンドライン引数

| 引数 | 説明 | 例 |
|------|------|----|
| `--date` | 単一日付を処理 | `--date 20250901` |
| `--start-date`, `--end-date` | 範囲指定（両端含む） | `--start-date 20250901 --end-date 20250903` |
| `--all-available` | `.bin` ファイル名から自動抽出 | `--all-available` |
| `--html-dir` | 入力フォルダ | `data/html/racelist` |
| `--out-dir` | 出力先 | `data/processed/raceinfo` |

## 実行例

### 単一日
```bash
python scripts/build_raceinfo.py --date 20250901
```
### 期間指定
```bash
python scripts/build_raceinfo.py --start-date 20250901 --end-date 20250910
```
### 全期間自動検出
```bash
python scripts/build_raceinfo.py --all-available
```
## 注意点

- HTML構造変更時は src/raceinfo_features.py 側で対応（本スクリプトに修正不要）。
- 出力CSVは学習前処理（master.csv）生成で使用される。
- .bin ファイル名に日付が含まれない場合は警告を出力しスキップ。
- 処理完了後、全出力CSVの行数合計を概算表示。

## ✅ 要約:

- racelist .bin を日単位で集約し、src/raceinfo_features.py に定義された解析関数を呼び出して
- 「当日のレース情報スナップショット」をCSV出力するシンプルなドライバ。

---
# scripts/build_raw_csv.py の役割

Boatrace公式の保存済みHTML（`.bin`）から、**日次の “raw” データ** と **“refund（払戻）” データ** を生成します。  
入力は `data/html/{pay,index,racelist,pcexpect,beforeinfo,raceresult,raceindex}` にある当日分の `.bin` 群、  
出力は `data/raw/YYYYMMDD_raw.csv` と `data/refund/YYYYMMDD_refund.csv` です。

## 主な仕様

- 入力日付: `--date`（`YYYY-MM-DD` or `YYYYMMDD`、未指定なら当日）
- 参照HTML:
  - **pay**: 開催場・場コード・グレード/タイプ/属性
  - **index**: 開催タイトル、会期（初日/最終日/○日目→数値化）、日程
  - **racelist**: 出走表（登録番号・級別・支部/出身・年齢/体重 など）
  - **pcexpect**: 予想印、レース名、進入固定/安定板使用の条件
  - **beforeinfo**: 展示・気象（展示タイム、部品交換、気温/風/水温/波高/風向 など）
  - **raceresult**: 結果（着、ST、ST順位、払い戻し、備考）
  - **raceindex**: 性別（女性/男性/不明）推定
- 1日ぶんの全「場 × 12R」を処理
  - 中間成果は `data/{race_id}_raw.pickle`・`..._refund.pickle` に一時保存 → 最後に日次CSVへ結合
- 列正規化
  - 日本語列名を学習ノート準拠にリネーム（例: `展示 タイム→time_tenji`, `チルト→Tilt` など）
  - `section_id = YYYYMMDD_code` を付与（集計キー）
- エラー/欠落ハンドリング
  - 荒天・不成立・展示欠落などはログ出力しつつ継続
  - 最後に一時pickleを削除

## 処理フロー（要点）

1. **pay / index** を読み、開催一覧（`place, code, race_grade, race_type, race_attribute, title, day, section, schedule`）を作成  
2. 各 `code × R(1..12)` について `race_id = YYYYMMDD + code + RR` を組み立て  
3. **racelist** から選手・F/L/ST平均・機/ボート成績を抽出して `raw` を構成  
4. **pcexpect** で予想印・レース名・進入固定/安定板使用を付加  
5. **beforeinfo** で展示（`entry_tenji, ST_tenji`）と気象を付加  
6. **raceresult** で `rank, ST, ST_rank, winning_trick, henkan_ticket, remarks` を付加  
7. **raceindex** で `sex` を推定して付加  
8. 場コードで開催情報を結合、`wakunari` 判定、日次pickleを後で結合  
9. 最終的に **raw/refund の日次CSV** を保存

## 出力

- `data/raw/YYYYMMDD_raw.csv`
  - 主要列（例）:  
    `race_id, date, code, R, wakuban, player, player_id, AB_class, age, weight, team, origin, run_once, F, L, ST_mean, motor_number, motor_2rentai_rate, boat_number, boat_2rentai_rate, entry_tenji, ST_tenji, time_tenji, Tilt, propeller, parts_exchange, counter_weight, temperature, weather, wind_speed, wind_direction, water_temperature, wave_height, title, day, section, schedule, race_grade, race_type, race_attribute, timetable, rank, ST, ST_rank, winning_trick, henkan_ticket, remarks, sex, is_wakunari, section_id`
- `data/refund/YYYYMMDD_refund.csv`
  - 払戻テーブルをレースID付きでフラット化

## 実行例

```bash
# 当日分を処理（デフォルト: “今日”）
python scripts/build_raw_csv.py

# 任意日付を処理
python scripts/build_raw_csv.py --date 2025-09-07
# または
python scripts/build_raw_csv.py --date 20250907
```
## 注意点

- .bin はあらかじめ data/html/**/ に存在している前提
- 途中でHTML欠落・構造差異があっても、可能な範囲で処理を継続（ログに警告）
- 気象0:00現在など情報が無い場合は適切に N/A/欠損で充填
- 最終行で一時pickleを掃除（data/*.pickle）
---
# scripts/build_season_course_prior_from_raw.py の役割

`data/raw/*.csv`（日次raw）を結合し、**季節×場×進入コース（entry）別の入着率 prior** を生成するスクリプト。  
完走（1〜6着の数値着）だけを分母・分子に計上し、DNS/欠/落/転/妨/F/L 等は除外。  
同一 **season_q × entry** の**全場平均**を基準として、**差分（adv_）** と **対数比（lr_）** による相対化も出力します。

## 主な仕様

- 入力: `data/raw/*.csv`（`preprocess.load_raw` で結合、`cast_and_clean` で型正規化）
- 期間: `--from YYYYMMDD` 〜 `--to YYYYMMDD`（両端含む, inclusive）
- 集計キー: `place, entry(1..6), season_q(spring/summer/autumn/winter)`
- 出力列（主要）
  - 件数: `n_finished, c1..c6`
  - 絶対率: `p1..p6`（0〜1）
  - 基準（全場平均）: `base_p1..base_p6`
  - 差分: `adv_p1..adv_p6 = p* - base_p*`
  - 対数比: `lr_p1..lr_p6 = log((p*+eps)/(base_p*+eps))`, `eps = 1/(n_finished + m_strength + 1)`
- 平滑化（任意）: `--m-strength m`（Dirichlet 等配、m=0 で平滑化なし）

## 列自動検出

- 着順列: `rank/arrival/chakujun/chaku/finish/finish_order` のいずれかを自動検出（`--finish-col` 明示指定も可）
- 進入コース列: `entry/course` を自動検出（`--entry-col` 明示指定も可）
- 全角数字は半角へ正規化、`entry` と `rank` はそれぞれ 1..6 のみ採用

## 出力の意味

- `p1..p6`: その **場×entry×季節** の素の入着率  
  - m=0（デフォルト）: `p_k = c_k / n_finished`  
  - m>0（平滑化）: `p_k = (c_k + m/6) / (n_finished + m)`
- `base_p1..base_p6`: 同一 `season_q × entry` の **全場平均**（ベースライン）
- `adv_p*`: ベースからの差（+なら相対的に強い）
- `lr_p*`: ベースとの比の対数（ロバストな相対指標）

## ファイルI/O

- 出力: `--out` で指定（例: `data/priors/season_course/20240101_20250630.csv` 推奨）
- 便利機能: `--link-latest` を付けると同ディレクトリに `latest.csv` を上書き作成

## コマンドライン引数

| 引数 | 必須 | 説明 | 例 |
|---|:---:|---|---|
| `--raw-dir` |  | raw日次CSVのディレクトリ | `data/raw` |
| `--from` | ✅ | 開始日（YYYYMMDD） | `20240101` |
| `--to` | ✅ | 終了日（YYYYMMDD） | `20250630` |
| `--finish-col` |  | 着順列名（未指定は自動検出） | `rank` |
| `--entry-col` |  | 進入コース列名（未指定は自動検出） | `entry` |
| `--m-strength` |  | Dirichlet疑似件数m（0=なし） | `50` |
| `--out` | ✅ | 出力CSVパス | `data/priors/season_course/2024H1.csv` |
| `--link-latest` |  | `latest.csv` を作成/更新 | `--link-latest` |

## 実行例

```bash
# 2024年〜2025年6月までの prior（平滑化なし）
python scripts/build_season_course_prior_from_raw.py ^
  --raw-dir data/raw ^
  --from 20240101 --to 20250630 ^
  --out data/priors/season_course/20240101_20250630.csv ^
  --link-latest

# 2025年上半期、m=60 の等配平滑化で安定化
python scripts/build_season_course_prior_from_raw.py ^
  --from 20250101 --to 20250630 ^
  --m-strength 60 ^
  --out data/priors/season_course/2025H1_m60.csv
```
## 生成されるカラム（順序）
```bash
place, entry, season_q, n_finished,
c1..c6, p1..p6, base_p1..base_p6, adv_p1..adv_p6, lr_p1..lr_p6,
built_from, built_to, m_strength, keys, version
```
## 注意点

- 期間内に行が無い場合は、利用可能期間をエラーメッセージで案内します
- version=3（相対化列を追加した版）
- 生成物は 学習の事前参照（prior） や 特徴量の外部結合 に使用可能（place×entry×season_q で join）
