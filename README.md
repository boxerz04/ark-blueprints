# ark-blueprints

ボートレースの **データ収集 → 前処理 → 学習 → 推論（GUI/CLI）** を、
再現性と運用性を重視して一気通貫で扱う開発プロジェクトです。

本リポジトリは「実験用ノートブックの寄せ集め」ではなく、
**日次運用・再学習・GUI推論までを前提とした実運用コードベース**として整備しています。

---

## プロジェクトの思想

- **SSOT（Single Source of Truth）を明確にする**
- 学習と推論で **同一の前処理・特徴量定義を必ず共有**
- 「一度動いたコード」を **壊さずに改良する**
- 将来の特徴量設計・モデル追加に耐える構造を保つ

---

## ドキュメント（docs/）

本プロジェクトは、運用の核となるパイプラインを **3本のドキュメント**として固定しています。

| フェーズ | 内容 | ドキュメント |
|---|---|---|
| master | 生データから学習・推論共通の master CSV を生成 | `docs/master_pipeline.md` |
| training | master から学習用特徴量を作りモデルを学習 | `docs/training_pipeline.md` |
| inference | 学習済みモデルを用いた単レース推論 | `docs/inference_pipeline.md` |

---

## ディレクトリ構造（要点）

```text
ark-blueprints/
├─ README.md
├─ docs/                          # ドキュメント群（パイプラインSSOT）
│  ├─ master_pipeline.md
│  ├─ training_pipeline.md
│  └─ inference_pipeline.md
│
├─ batch/                         # パイプライン実行用 PowerShell
│  ├─ build_master_range.ps1      # master生成の入口（SSOT）
│  └─ train_model_from_master.ps1 # 学習の入口（SSOT）
│
├─ scripts/                       # 実行スクリプト群（CLI）
│  ├─ _archive/                   # 旧multi-approach（top2pair/sectional/ensemble）退避
│  ├─ scrape_one_race.py
│  ├─ build_live_row.py
│  ├─ preprocess_base_features.py
│  ├─ preprocess_motor_id.py
│  ├─ preprocess_motor_section.py
│  ├─ preprocess_course.py
│  ├─ preprocess_sectional.py
│  └─ predict_one_race.py
│
├─ src/                           # ライブラリ層（再利用・責務分離）
│  ├─ adapters/                   # 推論時の入力整形（base）
│  ├─ raceinfo_features.py        # racelist HTML → 今節スナップショット特徴量
│  ├─ st.py                       # STパーサ
│  └─ rank.py                     # 着順パーサ
│
├─ features/                      # 特徴量定義（YAML = 入力側SSOT）
│  └─ finals.yaml
│
├─ data/
│  ├─ processed/
│  │  ├─ master.csv
│  │  ├─ master_finals.csv
│  │  └─ finals/                  # 学習用 X / y / ids
│  └─ live/                       # 推論用一時データ
│
└─ models/
   └─ finals/
      ├─ runs/                    # 学習run単位の成果物
      └─ latest/                  # 最新モデル（運用対象）
```

---

## できること（現在）

- レース公式サイトからの **単レースHTML取得**
- live master（1レース6艇）の生成
- 学習時と同一ロジックによる特徴量付与
- 二値分類モデル学習（Top2）
- 学習済みモデルを用いた **GUI推論**
- モデル・前処理・評価指標の run 単位保存と再現

---

## 学習の基本フロー（概要）

```text
master(_finals).csv
  ↓
preprocess_base_features.py
  ↓
X_dense.npz / y.csv / ids.csv
  ↓
train.py
  ↓
models/<approach>/runs/<model_id>/
```

※ 詳細は `docs/training_pipeline.md` を参照。

---

## 推論の基本フロー（概要）

```text
GUI操作
  ↓
scrape_one_race.py
  ↓
build_live_row.py
  ↓
各種 preprocess
  ↓
predict_one_race.py
  ↓
確率出力（GUI / CSV）
```

※ 詳細は `docs/inference_pipeline.md` を参照。

---

## 設計上の重要ルール

- 推論では **学習済み feature_pipeline.pkl を必ず使用**（列整合のSSOT）
- 列の追加・削除は学習側でのみ行い、推論側は upstream で担保する
- `models/<approach>/latest/` は「運用対象の安定版」

---

## ステータス

- master パイプライン：安定
- 学習パイプライン：安定
- 推論GUI：運用可能

本リポジトリは **「いま動いているものを壊さずに育てる」** 方針で継続開発中です。
