# 推論パイプライン（inference_pipeline）

本ドキュメントは、**学習済みモデルを用いて1レース単位で推論を行うパイプライン**を整理したものです。

- 学習工程は `training_pipeline.md`
- master生成工程は `master_pipeline.md`

に委ね、本書では **GUI / CLI 共通の推論フロー**のみを扱います。

---

## 全体目的（SSOT）

1. レース当日のHTML・binデータから live CSV を生成
2. 学習時と同一ロジックで特徴量を付与
3. 学習済み `feature_pipeline.pkl` を用いて前処理
4. 学習済み `model.pkl` により確率推論を実行
5. 結果を GUI 表示および CSV / ログに出力

推論は **列名・前処理・モデルを完全固定**し、
学習時との差異を生まないことを最優先とする。

---

## ディレクトリ前提

```
ark-blueprints/
├─ gui_predict_one_race.py          # GUIエントリポイント
├─ scripts/
│  ├─ scrape_one_race.py
│  ├─ build_live_row.py
│  ├─ preprocess_motor_id.py
│  ├─ preprocess_motor_section.py
│  ├─ preprocess_course.py
│  ├─ preprocess_sectional.py
│  └─ predict_one_race.py
├─ data/
│  └─ live/
│     ├─ html/                      # 取得HTML
│     ├─ raw_YYYYMMDD_JCD_RR.csv    # live master（生）
│     └─ _debug_merged.csv          # 推論直前の確認用CSV
└─ models/
   └─ <approach>/latest/
      ├─ model.pkl
      ├─ feature_pipeline.pkl
      └─ train_meta.json
```

---

## パイプライン全体像

```
GUI操作
  │
  ▼
scrape_one_race.py
  │
  ▼
build_live_row.py
  │
  ▼
preprocess_motor_id.py
  │
  ▼
preprocess_motor_section.py
  │
  ▼
preprocess_course.py
  │
  ▼
preprocess_sectional.py
  │
  ▼
predict_one_race.py
  │
  ▼
推論結果（確率）
```

すべての工程は GUI から **逐次・同期的**に実行される。

---

## 各工程の役割

### 1. `gui_predict_one_race.py`
**GUIオーケストレーター**

- 日付 / 場 / レース番号の入力受付
- 推論開始・停止制御
- 下流スクリプトの subprocess 実行
- 結果ログ・進捗表示

---

### 2. `scrape_one_race.py`
**当該レースのデータ取得**

- raceindex / racelist / beforeinfo / rankingmotor 等を取得
- 出力：`data/live/html/**`
- 推論時点で取得可能な情報のみを対象とする

---

### 3. `build_live_row.py`
**live master（1レース6艇分）生成**

- HTML / bin を解析
- master と同一カラム体系で CSV を生成
- 出力：`data/live/raw_YYYYMMDD_JCD_RR.csv`

※ この時点で **team を含む全基本列が揃う**

---

### 4. `preprocess_motor_id.py`
**motor_id 付与**

- rankingmotor 情報を基に motor_id を算出
- 学習時と完全同一ロジック

---

### 5. `preprocess_motor_section.py`
**motor_section 特徴量付与**

- モーターの節・期スナップショット特徴量を結合
- 欠損時も安全に処理

---

### 6. `preprocess_course.py`
**コース・場特性の派生特徴量**

- 開催場 × 枠順の静的特徴量
- 推論時も deterministic

---

### 7. `preprocess_sectional.py`
**節間（今節）特徴量付与**

- racelist HTML を直接解析
- 必須列を保証し、欠損は 0.0 埋め

---

### 8. `predict_one_race.py`
**最終推論処理（CLI本体）**

- 入力：live CSV（特徴量付与済み）
- 読み込み：
  - `model.pkl`
  - `feature_pipeline.pkl`
- ColumnTransformer により前処理
- `predict_proba` による確率推論

#### 出力
- 標準出力（SUMMARY）
- `data/live/_debug_merged.csv`

---

## モデル・パイプラインの扱い

- 推論では **必ず models/<approach>/latest/** を参照
- `approach` 表示はログ用ラベルであり、
  実際に使用されるモデルは model_path により決定される

---

## 運用上の原則

- 推論時に列の追加・削除は行わない
- 列不整合は upstream（build / preprocess）で解決する
- 学習と推論は **同一 feature_pipeline.pkl** を必ず共有する

---

## 更新履歴
- 2026-02 : 学習パイプライン分離後、推論工程を独立ドキュメント化

