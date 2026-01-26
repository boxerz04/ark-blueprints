# モーター特徴量パイプライン（motor_pipeline）

本ドキュメントは、モーター関連データを **raw → 学習用 master** まで一貫して生成するための
スクリプト実行順・入出力・役割を整理したものです。

---

## 全体目的

- `rankingmotor.bin` と `raw` を基に **motor_id（モーター世代）** を確定
- `motor_id × section_id` 単位で **節単位モーター特徴量（過去節のみ）** を生成
- master に **リーク無しで結合**し、学習・推論でそのまま使える正本を作成

最終成果物：
- `data/processed/master__with_motor_section.csv`

---

## ディレクトリ前提

```
ark-blueprints/
├─ data/
│  ├─ raw/                         # raw ファイル群
│  ├─ html/
│  │  └─ rankingmotor/             # rankingmotor.bin 群
│  └─ processed/
│     ├─ motor/                    # モーター関連の中間・成果物
│     └─ master.csv                # master 正本
└─ scripts/
```

---

## パイプライン全体像（実行順）

1. build_motor_artifacts_from_bins.py  
2. build_raw_with_motor_joined.py  
3. build_motor_section_base.py  
4. build_motor_section_features_n.py  
5. preprocess_motor_id.py  
6. preprocess_motor_section.py  

---

## 1. build_motor_artifacts_from_bins.py

### 役割
- rankingmotor.bin から **motor_id 世代管理の正本**を生成
- モーター交換・世代切替（effective_from / effective_to）を確定

### 入力
- `data/html/rankingmotor/*.bin`

### 出力
- `data/processed/motor/motor_id_map__all.csv`
- `data/processed/motor/motor_section_snapshot__all.csv`

### 実行コマンド
```bat
python scripts\build_motor_artifacts_from_bins.py ^
  --bins_dir data/html/rankingmotor ^
  --out_snapshot_csv data/processed/motor/motor_section_snapshot__all.csv ^
  --out_map_csv data/processed/motor/motor_id_map__all.csv
```

---

## 2. build_raw_with_motor_joined.py

### 役割
- raw に motor_id / section_id を付与した **raw 正本**を生成

### 入力
- `data/raw/`
- `motor_section_snapshot__all.csv`
- `motor_id_map__all.csv`

### 出力
- `data/processed/motor/raw_with_motor__all.csv`

### 実行コマンド
```bat
python scripts\build_raw_with_motor_joined.py ^
  --raw_dir data/raw ^
  --snapshot_csv data/processed/motor/motor_section_snapshot__all.csv ^
  --map_csv data/processed/motor/motor_id_map__all.csv ^
  --out_dir data/processed/motor ^
  --write_full_csv 1
```

---

## 3. build_motor_section_base.py

### 役割
- raw_with_motor から **節単位の集計正本（base）**を作成
- 当該節の集計のみ（prev/delta は作らない）

### 入力
- `data/processed/motor/raw_with_motor__all.csv`

### 出力
- `data/processed/motor/motor_section_base__all.csv`

### 実行コマンド
```bat
python scripts\build_motor_section_base.py ^
  --input data/processed/motor/raw_with_motor__all.csv ^
  --out_csv data/processed/motor/motor_section_base__all.csv
```

---

## 4. build_motor_section_features_n.py

### 役割
- 過去節のみを使ったモーター特徴量を生成（リーク防止）
- motor_id を **6桁ゼロ埋めに正規化**

生成される主な特徴量：
- prev1_*
- prev3_sum_* / prev3_mean_*
- prev5_sum_* / prev5_mean_*
- delta_1_3_* / delta_1_5_*

### 入力
- `data/processed/motor/motor_section_base__all.csv`

### 出力
- `data/processed/motor/motor_section_features_n__all.csv`

### 実行コマンド
```bat
python scripts\build_motor_section_features_n.py ^
  --input data/processed/motor/motor_section_base__all.csv ^
  --out_csv data/processed/motor/motor_section_features_n__all.csv
```

---

## 5. preprocess_motor_id.py

### 役割
- master に raw と同条件で **motor_id を付与**
- motor_section 特徴量を結合できる状態にする

### 入力
- `data/processed/master.csv`
- `motor_id_map__all.csv`

### 出力
- `data/processed/master.csv`（motor_id 付与済み）

※ 引数は実装依存のため、必ず `--help` を確認して実行する。

---

## 6. preprocess_motor_section.py

### 役割
- motor_section_features を master に **安全に JOIN**
- 結合するのは **prev / delta 系のみ**

### 入力
- `data/processed/master.csv`
- `motor_section_features_n__all.csv`

### 出力
- `data/processed/master__with_motor_section.csv`

### 実行コマンド
```bat
python scripts\preprocess_motor_section.py ^
  --master_csv data/processed/master.csv ^
  --motor_section_csv data/processed/motor/motor_section_features_n__all.csv ^
  --out_master_csv data/processed/master__with_motor_section.csv
```

---

## 最小QC（確認項目）

### motor_id 表記確認
```bat
powershell -NoProfile -Command "Get-Content 'data/processed/motor/motor_section_features_n__all.csv' -TotalCount 2"
```
- motor_id が `011101` のように 6桁であること

### 結合成否確認
- `preprocess_motor_section.py` 実行ログで
  - `left_only = 0`
  - `both = master 行数`

---

## 備考

- 本パイプラインは **設計上の例外（廃番モーター、追加あっせん）を理解した上で確定**している
- motor_id 周りはこれ以上触らない前提で、motor_section JOIN に進んでよい

---

（更新履歴）
- 2026-01 : motor_section_features 導入・正規化確定
