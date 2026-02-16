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

### 特徴量YAMLの役割（どちらを編集するか）

列の追加・削除など「特徴量の設計」を変えたいときは、**編集するのは次の一方だけ**です。

| 役割 | 場所 | 編集する？ | 説明 |
|------|------|------------|------|
| **設計の入力（SSOT）** | `features/<approach>.yaml`<br>（例: `features/finals.yaml`） | **する** | 列選別の正本。ここを編集して学習を回す。 |
| **その run の記録** | `models/<approach>/runs/<model_id>/feature_cols_used.yaml`<br>および `models/<approach>/latest/feature_cols_used.yaml` | **しない** | 学習のたびに上書きされる「実際に使った列」のログ。設計変更には使わない。 |

**ルール**: 列を変えたいときは **`features/*.yaml` だけ** を編集する。`models/` 配下の `feature_cols_used.yaml` は編集しない（上書きされる記録用であり、次回学習時に無視される）。

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
├─ feature_cols_used.yaml   # 記録用（編集しない）
└─ train_meta.json
```

`latest` は **常に最後に成功した run を指す**。

---

## ハイパーパラメータ探索（任意）

学習と同じデータ・同じ時系列ホールドアウトで LightGBM のハイパーパラメータを探索し、**結果を確認してから**運用に反映できる。

### 探索指標（重要）

目標は **LogLoss 改善・PR-AUC 改善・外さないAI** であり、**AUC のみで最適化すると確率の質が崩れ、LogLoss が劣化しやすい**。  
そのため探索の既定は **`neg_log_loss`（LogLoss 最小化）** とする。

- 既定: `--scoring neg_log_loss`（確率の質を維持しつつ探索）
- 必要に応じて `--scoring average_precision`（PR-AUC）、`--scoring roc_auc` も指定可能（roc_auc は確率劣化のリスクあり）

### 実行方法

```bat
python scripts/tune_hyperparams.py --approach finals --n-iter 24
```

- `--n-iter`: ランダムに試す組み合わせ数（既定 24）。増やすと時間がかかる。
- `--scoring`: 最適化する指標（既定 `neg_log_loss`）。上記を参照。
- 結果は **`models/<approach>/hpo_results.json`** に保存される。

### 結果の確認

`hpo_results.json` には以下が含まれる。

- **scoring**: 探索に使った指標（例: neg_log_loss）
- **best_params**: 最良だったパラメータ（n_estimators, learning_rate, num_leaves 等）
- **best_cv_score**: 探索時の CV スコア（scoring に応じた値。neg_log_loss なら大きいほど LogLoss が小さい）
- **eval_metrics**: ホールドアウトでの AUC / PR-AUC / **Logloss** / MCC / Top2 hit（train.py と同じ指標）

**Logloss が現行モデルより悪化していないか必ず確認する。** そのうえで PR-AUC・Top2 hit も見て運用に載せるか判断する。

### 運用への反映

- **手動で再学習する場合**:  
  `train.py` に `hpo_results.json` の `best_params` を渡して再学習する。  
  例: `python scripts/train.py --approach finals --version-tag v1.2 --n-estimators 600 --learning-rate 0.03 --num-leaves 31 ...`
- **PS1 から渡す**: 将来、`train_model_from_master.ps1` にオプションを追加し、`best_params` を `train.py` に渡す運用も可能。

探索は **学習パイプラインの外** で行い、結果を確認してから本番学習に反映する想定。

---

## 運用上の原則

- 学習は必ず **train_model_from_master.ps1 経由**で行う
- Python スクリプトの単体実行は検証用途に限定
- 列の変更は **features/*.yaml** で行う（`models/` の `feature_cols_used.yaml` は編集しない・記録用）
- 列の変更は YAML or preprocess 側で管理し、
  train.py には影響を波及させない

---

## 更新履歴
- 2026-02 : ハイパーパラメータ探索（tune_hyperparams.py）の手順を追記
- 2026-02 : 特徴量YAMLの役割を明示（編集するのは features/*.yaml のみ、models/ の feature_cols_used.yaml は記録用）
- 2026-02 : master パイプライン確定後の学習工程を分離し、
  preprocess / train / 成果物管理を明確化

