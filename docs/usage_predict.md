# 推論フロー（1レース予測）

## 1) 1レースをスクレイピング（live/html に保存）
```powershell
python scripts\scrape_one_race.py --date 20250922 --jcd 11 --race 12
```
※ 取得HTMLは data/live/html/<kind>/... に .bin で保存されます。
## 2) ライブ6行CSVの生成（直前で取得したHTMLをそのまま利用）
```powershell
python scripts\build_live_row.py --date 20250922 --jcd 11 --race 12 --out data\live\raw_20250922_11_12.csv
```
※ヒント：ここで --online は不要です（手順1のHTMLキャッシュを使います）。必要なら --online でも可。
## 3) Base モデルで単発推論（models/base/latest を使用）
```powershell
python scripts\predict_one_race.py --live-csv data\live\raw_20250922_11_12.csv --approach base --model-dir models\base\latest
```
## 4) Sectional モデルで推論（models/sectional/latest を使用）
```powershell
python scripts\predict_one_race.py --live-csv data\live\raw_20250922_11_12.csv --approach sectional --model-dir models\sectional\latest
```
※ --approach で切り替えます。指定がない場合は base が既定です。
※ モデルDIRを省略した場合は自動的に models/<approach>/latest が使われます。
