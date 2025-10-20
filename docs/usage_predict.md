# 推論フロー（1レース予測）— 最新版

本ドキュメントは `gui_predict_one_race.py` と CLI の両方で、**学習時と揃った列**（prior＋course系）を確実に用いて推論するための手順をまとめた最新版です。

---

## 全体像（学習と整合する列を使う）
**live生成 → course履歴付与 → （adapterで）prior結合 → 変換＆推論**

- **priorの結合は `predict_one_race.py` のアダプタ（`base.py`）内で自動実行**されます。
- **course系の履歴特徴は `preprocess_course.py` で live CSV に上書き付与**します（助走日数・直近本数を指定）。

---

## GUI（1レース）
1. **スクレイプ**（GUIボタン）
2. **ライブ行生成**（GUIボタン）
3. **course特徴を上書き付与**（GUIが `preprocess_course.py` を呼び出し、live CSV を上書き）
4. **推論**（GUIが `predict_one_race.py` を呼び出し、adapterが prior を結合 → 予測）

> ヒント：GUIの「デバッグCSV出力」をONにすると、結合後の最終入力が `_debug_merged__final.csv` として保存されます。

---

## CLI（PowerShell想定）
### 1) スクレイプ → HTML をキャッシュ
```powershell
python scripts\scrape_one_race.py --date 20250922 --jcd 11 --race 12
```

### 2) ライブ6行CSVを生成（直前のHTMLを使用）
```powershell
python scripts\build_live_row.py --date 20250922 --jcd 11 --race 12 --out data\live\raw_20250922_11_12.csv
```

### 3) course特徴を live CSV に上書き付与
- 助走日数（既定: **180日**）、直近本数（既定: **10**）
- `--out` に **同じ live CSV** を渡して**上書き**します。
```powershell
python scripts\preprocess_course.py ^
  --start-date 2025-09-22 --end-date 2025-09-22 ^
  --warmup-days 180 --n-last 10 ^
  --master data\live\raw_20250922_11_12.csv ^
  --raw-dir data\raw ^
  --out data\live\raw_20250922_11_12.csv ^
  --reports-dir data\processed\course_meta_live
```

### 4) 推論（Base）— adapter が prior を結合
- `--model-dir` を省略すると `models\base\latest` を自動使用。
```powershell
python scripts\predict_one_race.py ^
  --live-csv data\live\raw_20250922_11_12.csv ^
  --approach base ^
  --model-dir models\base\latest
```

### 5) 推論（Sectional）— 任意
```powershell
python scripts\predict_one_race.py ^
  --live-csv data\live\raw_20250922_11_12.csv ^
  --approach sectional ^
  --model-dir models\sectional\latest
```

---

## prior と course の役割分担
- **course（`preprocess_course.py`）**
  - 過去 **N本** の `finish{1..3}_rate/cnt`、`st_mean/std` を **entry**／**waku** 軸で計算して **live CSV に上書き**。
  - `--start-date/--end-date` は **対象レース日**、`--warmup-days` でどこまで遡るかを決定します。
- **prior（`predict_one_race.py` の adapter / `base.py`）**
  - 学習時と同じ **素の列名**（例：`N_winning_rate`, `LC_2rentai_rate`, `motor_2rentai_rate`, `boat_3rentai_rate` など）で **LEFT JOIN**。
  - `tenji`／`season_course`／`winning_trick` 等の prior を **一括で結合**し、`tenji_resid` などの派生もここで生成。

> 重要：**推論で prior 列を別名（`prior_` 接頭辞）にはしません。** 学習と同名で揃います。

---

## デバッグ方法
### A. 列の存在確認（PowerShell）
```powershell
$csv = Import-Csv data\live\raw_20250922_11_12.csv
$need = 'N_winning_rate','LC_winning_rate','motor_2rentai_rate','boat_2rentai_rate'
$cols = $csv[0].psobject.Properties.Name
$need | %% { "{0} : {1}" -f $_, (@($cols) -contains $_) }
```

### B. ほんとうに足りない列の特定（推奨・開発者向け）
`predict_one_race.py` の `pipeline.transform(X)` 直前で以下を一時追加：
```python
req = set(getattr(pipeline, "feature_names_in_", X.columns))
miss = [c for c in req if c not in X.columns]
if miss:
    print("[ERROR] missing columns:", sorted(miss)[:50])
    return
```

### C. デバッグCSV（GUI）
- GUIの「デバッグCSV出力」をON → `_debug_merged__final.csv` が生成されます。
  - prior系の列（素名）と course系の `*_last10_{entry|waku}` 系が入っていることを確認。

---

## よくあるつまずき
- **モデルとパイプラインの不整合**：`models/base/latest/model.pkl` と `feature_pipeline.pkl` が同版か確認。
- **日付パース**：live CSV の `date` が `YYYY-MM-DD` で渡っているか（season判定に使用）。
- **entry 未確定**：推論は `entry_tenji` で補完済み。NaN が残ると season/course の join が欠落。

---

## ワンライナー（実運用）
```powershell
# 1) スクレイプ
python scripts\scrape_one_race.py --date 20251020 --jcd 11 --race 12
# 2) ライブ生成
python scripts\build_live_row.py --date 20251020 --jcd 11 --race 12 --out data\live\raw_20251020_11_12.csv
# 3) course付与
python scripts\preprocess_course.py --start-date 2025-10-20 --end-date 2025-10-20 --warmup-days 180 --n-last 10 --master data\live\raw_20251020_11_12.csv --raw-dir data\raw --out data\live\raw_20251020_11_12.csv --reports-dir data\processed\course_meta_live
# 4) 推論（base）
python scripts\predict_one_race.py --live-csv data\live\raw_20251020_11_12.csv --approach base --model-dir models\base\latest
```

---

## 補足（構成の方針）
- **priorは adapter に一本化**（学習・推論とも同じ join 仕様）。
- **新しい prior** を追加する場合は `data/priors/<name>/latest.csv` を整備し、adapter 側に読み込みロジックを足す。
- **新しい履歴特徴** は `preprocess_course.py` と同様の“付与スクリプト”を別ファイルで増やし、**live CSV を上書き**するスタイルで拡張（学習・推論どちらも同じ順序で適用）。
