# 学習パイプライン（training_pipeline）

本ドキュメントは、**特徴量成果物（X/y/ids）→ 学習 → モデル成果物保存**までの工程を整理したものです。  
`base` / `sectional` など approach 別に同一の学習器・保存規約で運用します。

参照実装：
- `scripts/train.py`（学習・評価・保存の正本）
- `usage_train.md`（最新フローの運用ガイド）

---

## 0. ディレクトリ前提

```
ark-blueprints/
├─ data/
│  ├─ raw/                       # 日次 raw CSV（スクレイピング → CSV化）
│  └─ processed/
│     ├─ master.csv              # 前処理済み master（prior 結合済み）
│     ├─ reports/                # preprocess.py のレポート
│     ├─ course_meta/            # preprocess_course.py のメタログ
│     ├─ base/                   # base 用の学習入力（X/y/ids）
│     └─ sectional/              # sectional 用の学習入力（X/y/ids）
├─ models/
│  ├─ base/
│  │  ├─ latest/                 # 推論で使う “最新” アーティファクト
│  │  └─ runs/<model_id>/        # 学習実行ごとの保存先
│  └─ sectional/
│     ├─ latest/
│     └─ runs/<model_id>/
└─ scripts/
   └─ train.py
```

---

## 1. 学習の入力と出力

### 入力（approach 別）
`data/processed/{approach}/` に以下が揃っていること。

- `X.npz`（疎行列） **または** `X_dense.npz`（密行列）
- `y.csv`（列名は `y` / `is_top2` / 先頭列 のいずれでも可）
- `ids.csv`（最低限 `race_id` 列を含む。時間分割に使用）
- `models/{approach}/latest/feature_pipeline.pkl`（特徴量パイプライン）

### 出力（approach 別）
- `models/{approach}/runs/<model_id>/`
  - `model.pkl`
  - `feature_pipeline.pkl`
  - `train_meta.json`
- `models/{approach}/latest/`
  - `model.pkl`
  - `feature_pipeline.pkl`
  - `train_meta.json`

---

## 2. 学習アルゴリズムと評価設計

### 学習器
- **LightGBM / LGBMClassifier**（2値分類）

主なハイパーパラメータ（train.py のデフォルト）：
- `n_estimators=400`
- `learning_rate=0.05`
- `num_leaves=63`
- `subsample=0.8`
- `colsample_bytree=0.8`
- `random_state=42`
- `n_jobs=-1`

### 分割方法（ホールドアウト）
- `ids.csv` の `race_id` の**登場順（時系列順）**でユニーク race を並べ、
  先頭 80% を train、残り 20% を valid にする（ratio=0.8）。
- 分割単位は **race_id**。同一レースの行（6艇）が train/valid に跨らない。

### 評価指標（valid）
- ROC AUC
- PR AUC（Average Precision）
- LogLoss
- Accuracy（閾値 0.5）
- MCC
- Top2Hit（各レースで予測確率上位2艇に “当たり（y=1）が含まれるか” の率）

評価後、**全データで再学習**して保存する（モデルは full-fit が正本）。

---

## 3. 実行コマンド

### 3.1 base を学習
```powershell
python scripts\train.py --approach base --version-tag vX.Y.Z-base-YYYYMMDD --notes "任意メモ"
```

### 3.2 sectional を学習
```powershell
python scripts\train.py --approach sectional --version-tag vX.Y.Z-sectional-YYYYMMDD --notes "任意メモ"
```

---

## 4. 学習前提（学習データ生成までの最小フロー）

> ここは「学習の前段（特徴量成果物の生成）」の要点のみ。詳細は master_pipeline / priors_pipeline を参照。

### base（推奨標準フロー）
1) master 生成（prior 結合込み）  
2) course 履歴特徴を master に上書き  
3) base 用特徴量化（X/y/ids + feature_pipeline.pkl）  
4) train.py

※ “特徴量化”は `preprocess_base_features.py` が担当（usage_train.md 参照）。

### sectional（短期モデル）
1) sectional 用 master 作成（節間10列の付与など）
2) sectional 用特徴量化（X/y/ids + feature_pipeline.pkl）
3) train.py

---

## 5. 生成物の意味（train_meta.json）

`train_meta.json` には、以下が記録されます。

- `model_id`（生成ID）
- `created_at`
- `version_tag`（手動指定）
- `notes`（手動指定）
- `git_commit`（学習時点のコミット）
- `n_rows`, `n_features`, `sparse`
- `eval`（valid 指標一式）

このメタは「どのデータで・どのコードで・どういう性能だったか」を後から再現するための正本です。

---

## 6. 最小QC（失敗を早期検知するチェック）

### 入力欠損チェック（train.py が検出）
- `X.npz` / `X_dense.npz`
- `y.csv`
- `ids.csv`
- `models/{approach}/latest/feature_pipeline.pkl`

が存在しない場合、即エラーで停止。

### 次元整合チェック
- `feature_pipeline.pkl` の特徴量次元と `X` の列数が一致しない場合、即エラー。

### ids.csv の必須
- `race_id` がないと時間分割が成立しない（運用上、必須列）。

---

## 7. 運用ポリシー（重要）

- `models/{approach}/latest/` は **推論が参照する唯一の入口**  
  → 学習後に latest を更新するのが標準運用。
- `runs/<model_id>/` は **監査ログ（immutable）**  
  → 過去モデルの再現・比較・ロールバックに使用。
- `--version-tag` と `--notes` は必ず記入し、モデルの意図を残す。

---

（更新履歴）
- 2026-01 : LightGBM + race_id 時系列ホールドアウト + latest/runs 保存規約を文書化
