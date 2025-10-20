# Ark Blueprints

未完の部品を一つずつ集め、いつか航海に出る箱舟を形にするプロジェクト。⛵

---

## 📝 プロジェクト概要

Ark Blueprints は、ボートレースのデータを **収集 → 前処理 → 特徴量生成 → 機械学習 → 推論** まで行うことを目指す開発プロジェクトです。  
現在は以下の機能が完成しています：

* スクレイピングによるデータ収集（レース情報・オッズ）
* 日次 CSV 生成（raw + refund）
* タイムライン生成（未確定レースの締切予定時刻を取得）
* スケジューラによる直前オッズ収集（準優・優勝戦）
* 前処理（欠損値処理・型変換・失格レース除外・ST/ST展示変換）
* 特徴量生成（数値/カテゴリ列の選定、OneHot + 標準化）
* LightGBM による初回学習・モデル保存

---

## 📂 ディレクトリ構造

```text
ark-blueprints/
├─ scripts/                # 実行スクリプト群
│  ├─ scrape_one_race.py   # 公式サイトから1Rデータ取得
│  ├─ build_live_row.py    # 直前データを加工してCSV化
│  ├─ preprocess_base_features.py  # base特徴量生成（notebook不要）
│  ├─ preprocess_sectional.py      # sectional特徴量前処理
│  ├─ features_sectional.py        # sectional用特徴量生成
│  ├─ predict_one_race.py          # 単レース推論（base / sectional対応）
│  ├─ predict_top2pair.py          # ペアモデル推論（任意）
│  └─ ...
│
├─ src/
│  └─ adapters/
│     ├─ base.py           # baseモデル用アダプタ
│     └─ sectional.py      # sectionalモデル用アダプタ
│
├─ models/                 # 学習済みモデル
│  ├─ base/latest/
│  ├─ sectional/latest/
│  └─ top2pair/latest/
│
├─ data/
│  ├─ live/                # 直前データと推論結果
│  ├─ processed/
│  │  ├─ base/             # base特徴量保存
│  │  └─ sectional/        # sectional特徴量保存
│  └─ config/settings.json # GUIの設定保存
│
├─ docs/
│  ├─ usage_train.md       # 学習手順
│  └─ usage_predict.md     # 推論手順
│
├─ gui_predict_one_race.py # GUIランチャー（base/sectional切替対応）
└─ suji_strategy.py        # スジ舟券生成ロジック（現在はGUIから分離）
```






