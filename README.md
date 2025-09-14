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
│
├─ data/                         # データ格納（.gitignore 推奨）
│   ├─ html/                     # スクレイピング取得HTML
│   │   ├─ odds3t/               # 3連単オッズHTML
│   │   ├─ odds2tf/              # 2連単・2連複オッズHTML
│   │   ├─ pay/                  # 払戻ページHTML
│   │   └─ raceresult/           # レース結果ページHTML
│   ├─ raw/                      # 日次レースCSV（64列: 63 + section_id）
│   ├─ refund/                   # 払戻金CSV
│   ├─ timeline/                 # 直前オッズタイムラインCSV
│   └─ processed/                # 前処理・特徴量・ラベル等の成果物
│       ├─ master.csv            # 全レース統合（基礎）
│       ├─ X_base.npz / y.csv    # baseモデル用の特徴量・ラベル
│       ├─ X_top2pair_dense.npz  # top2pairモデル用の特徴量
│       ├─ y_top2pair.csv
│       └─ ids_top2pair.csv
│
├─ logs/
│
├─ notebooks/
│   ├─ preprocess.ipynb          # 前処理フロー検証
│   └─ features.ipynb            # 特徴量検証
│
├─ scripts/
│   ├─ scrape.py
│   ├─ build_raw_csv.py
│   ├─ build_timeline_live.py
│   ├─ run_odds_scheduler.py
│   ├─ scrape_odds.py
│   ├─ build_feature_pipeline.py # baseモデル用 前処理器生成
│   ├─ train.py                  # baseモデル学習（runs / latest 更新）
│   ├─ build_live_row.py         # 推論用ライブ行生成
│   ├─ predict_one_race.py       # 単発推論（baseモデル）
│   ├─ build_top2pair_dataset.py # top2pair用 データセット生成
│   ├─ train_top2pair.py         # top2pairモデル学習
│   └─ predict_top2pair.py       # top2pairモデル推論
│
├─ src/
│   ├─ __init__.py
│   ├─ data_loader.py
│   ├─ feature_engineering.py
│   ├─ model.py
│   ├─ model_utils.py            # 共通: 保存・ロード・ID生成
│   └─ utils.py
│
├─ models/
│   ├─ base/                     # baseモデル系
│   │   ├─ latest/
│   │   │   ├─ model.pkl
│   │   │   ├─ feature_pipeline.pkl
│   │   │   └─ train_meta.json
│   │   └─ runs/
│   │       └─ <model_id>/       # 例: 20250913_141256
│   │           ├─ model.pkl
│   │           ├─ feature_pipeline.pkl
│   │           └─ train_meta.json
│   │
│   └─ top2pair/                 # top2ペア方式モデル
│       ├─ latest/
│       │   ├─ model.pkl
│       │   └─ train_meta.json
│       └─ runs/
│           └─ <model_id>/
│               ├─ model.pkl
│               ├─ train_meta.json
│               ├─ feature_importance.csv
│               └─ cv_folds.csv
│
├─ docs/
│   ├─ data_dictionary.md
│   └─ design_notes.md
│
├─ tests/
│
├─ requirements.txt
├─ README.md
└─ .gitignore
```





