# 推論フロー（1レース予測）

## 1) 1レースをスクレイピング（live/html に保存）
```powershell
python scripts\scrape_one_race.py --date 20250913 --jcd 12 --race 12
```
※ 取得HTMLは data/live/html/<kind>/... に .bin で保存され、raceresult は保存しません。
## 2) ライブ6行CSVの生成（直前で取得したHTMLをそのまま利用）
```powershell
python scripts\build_live_row.py --date 20250913 --jcd 12 --race 12 --out data\live\raw_20250913_12_12.csv
```
※ヒント：ここで --online は不要です（手順1のHTMLキャッシュを使います）。必要なら --online でも可。
## 3) Base モデルで単発推論（models/base/latest を使用）
```powershell
python scripts\predict_one_race.py --live-csv data\live\raw_20250913_12_12.csv --model-dir models\base\latest
```
## 4) Top2ペア モデルでペア推論（models/top2pair/latest を使用）
```powershell
python scripts\predict_top2pair.py --mode live --master data\live\raw_20250913_12_12.csv --race-id 202509131212
```
