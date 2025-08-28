# Ark Blueprints
未完の部品を一つずつ集め、いつか航海に出る箱舟を形にするプロジェクト。⛵

---

## 📝 プロジェクト概要
Ark Blueprints は、ボートレースのデータを **収集・前処理・特徴量生成・機械学習・推論** まで行うことを目指す開発プロジェクトです。  
まずはスクレイピングによるデータ収集から始めています。

---

## 📂 ディレクトリ構造
```
ark-blueprints/
├─ scripts/ # 実行用スクリプト
│ └─ scrape.py # スクレイピングスクリプト
├─ data/ # スクレイピング結果を保存（Git管理外）
├─ .gitignore # data/ を除外設定
└─ README.md # この説明書
```

---

## 🚀 使い方

### スクレイピング実行（今日の日付で取得）
```powershell
python scripts/scrape.py
日付を指定して取得
powershell
コードをコピーする
python scripts/scrape.py --date 2025-08-27
python scripts/scrape.py --date 20250827
データは data/html/ 以下に保存されます。

保存先ディレクトリが存在しない場合でも、自動的に作成されます。

.bin ファイルやエラーログなど大容量データは data/ に保存されますが、.gitignore によりGit管理からは除外されます。

🔮 今後の予定
前処理スクリプト（preprocess.py）

特徴量生成スクリプト（features.py）

モデル学習スクリプト（train.py）

推論スクリプト（predict.py）

