# 学習パイプライン（training_pipeline）

本ドキュメントは、**master / master_finals.csv が完成した後**、
機械学習モデルを再現性・安全性を保って学習するためのパイプラインを整理したものです。

※ master 作成までの工程は `master_pipeline.md` に委ね、本書では **学習開始以降のみ**を扱います。

---

## 全体目的（SSOT）

1. `master.csv` または `master_finals.csv` を入力として使用
2. 列選別・前処理を **preprocess_base_features.py** で一元管理
3. 学習用特徴量（X / y）と前処理パイプラインを生成
4. `train.py` によりモデルを学習・評価
5. 成果物を **runs / latest** 配下に整理して保存

最終成果物：
- 学習済みモデル
- 前処理パイプライン
- 学習メタ情報（評価指標・特徴量数など）

---

## ディレクトリ前提

```
ark-blueprints/
├─ batch/
│  └─ train_model_from_master.ps1   # 学習パイプラインの唯一の入口（SSOT）
├─ features/
│  └─ finals.yaml                   # 列選別ルール（入力側SSOT）
├─ data/
│  └─ processed/
│     └─ finals/
│        ├─ X_dense.npz              # 学習用特徴量行列
│        ├─ y.csv                    # 正解ラベル
│        └─ ids.csv                  # race_id / player_id 等
├─ models/
│  └─ finals/
│     ├─ latest/                     # 最新モデルへのシンボリック保存先
│     └─ runs/
│        └─ <model_id>/              # 学習run単位の成果物
└─ scripts/
   ├─ preprocess_base_features.py
   └─ train.py
```

---

## パイプライン全体像

```
master(_finals).csv
        │
        ▼
preprocess_base_features.py
        │
        ├─ X_dense.npz
        ├─ y.csv
        ├─ ids.csv
        ├─ feature_pipeline.pkl
        └─ feature_cols_used.yaml
        │
        ▼
train.py
        │
        ├─ model.pkl
        ├─ train_meta.json
        └─ eval metrics
```

すべての工程は **train_model_from_master.ps1** から実行され、
手動で Python スクリプトを直接呼ぶ運用は想定しない。

---

## 0. train_model_from_master.ps1（学習パイプラインの唯一の入口）

### 役割
- 学習に必要な工程を **順序固定で実行**
- 各 run を一意な `model_id`（日時）で管理
- 成果物を `models/<approach>/runs/<model_id>/` に集約
- `models/<approach>/latest/` を常に最新へ更新

### 主な処理フロー
1. 前処理（特徴量生成）
2. モデル学習
3. 学習メタ情報の整理・保存

### 実行例（finals モデル）

```bat
powershell -NoProfile -ExecutionPolicy Bypass ^
  -File .\batch\train_model_from_master.ps1 ^
  -Approach "finals" ^
  -VersionTag "v1.1.1-finals" ^
  -Notes "finals モデル再学習" ^
  -MasterCsv "data\\processed\\master_finals.csv" ^
  -FeatureSpecYaml "features\\finals.yaml"
```

---

## 1. preprocess_base_features.py（列選別・特徴量生成）

### 役割
- 学習に使用する列を **一箇所で制御**
- 数値 / カテゴリ列の判定
- 欠損補完・OneHotEncoding・スケーリングを含む前処理を構築
- 学習・推論で共通利用する **feature_pipeline.pkl** を生成

### 入力
- `master.csv` または `master_finals.csv`
- `features/<approach>.yaml`（必須）

### 列選別の考え方

- YAML が **入力側SSOT**
- `columns.use: []` の場合：auto 推定
- 設計上不要な列は `DEFAULT_DROP_COLS` で除外
- 将来の絶対禁止列は `FORCE_DROP_COLS`（最終防御線）で除外

### 出力
- `data/processed/<approach>/X_dense.npz`
- `data/processed/<approach>/y.csv`
- `data/processed/<approach>/ids.csv`
- `models/<approach>/runs/<model_id>/feature_pipeline.pkl`
- `models/<approach>/runs/<model_id>/feature_cols_used.yaml`

---

## 2. train.py（モデル学習・評価）

### 役割
- 前処理済み特徴量を読み込みモデルを学習
- 学習データに対する評価指標を算出
- 学習条件・評価結果をメタ情報として保存

### 入力
- `X_dense.npz`
- `y.csv`
- `feature_pipeline.pkl`

### 出力
- `model.pkl`（学習済みモデル）
- `train_meta.json`
  - model_id
  - version_tag
  - n_rows / n_features
  - eval metrics（AUC, PR-AUC, Logloss, Accuracy, MCC, Top2_hit）

---

## 成果物の配置ルール

### run 単位（再現性の核）
```
models/<approach>/runs/<model_id>/
├─ model.pkl
├─ feature_pipeline.pkl
├─ feature_cols_used.yaml
└─ train_meta.json
```

### latest
```
models/<approach>/latest/
├─ model.pkl
├─ feature_pipeline.pkl
└─ train_meta.json
```

`latest` は **常に最後に成功した run を指す**。

---

## 運用上の原則

- 学習は必ず **train_model_from_master.ps1 経由**で行う
- Python スクリプトの単体実行は検証用途に限定
- 列の変更は YAML or preprocess 側で管理し、
  train.py には影響を波及させない

---

## 更新履歴
- 2026-02 : master パイプライン確定後の学習工程を分離し、
  preprocess / train / 成果物管理を明確化

