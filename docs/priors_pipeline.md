
# prior パイプライン ドキュメント

このドキュメントは、学習・推論で使用する **prior（事前分布・事前統計）系データ**の生成手順を整理したものです。  
raw データから再現可能に生成できること、列の意味が明確であることを目的とします。

---

## ディレクトリ前提

```
data/
├─ raw/                 # 日次レースCSV（公式スクレイピング結果）
├─ priors/
│   ├─ tenji/           # 展示タイム prior
│   ├─ season_course/   # 季節×場×コース prior
│   └─ winning_trick/   # 決まり手 prior
```

すべての prior は **data/raw** を唯一の入力とし、再生成可能です。

---

## 共通仕様

### 入力
- `data/raw/*.csv`
- `preprocess.load_raw()` により全期間を結合
- `preprocess.cast_and_clean()` により型・欠損を正規化

### 期間指定
- `--from YYYYMMDD`
- `--to   YYYYMMDD`
- **inclusive（両端含む）**

### 季節区分（season_q）
| 月 | season_q |
|----|----------|
| 3–5 | spring |
| 6–8 | summer |
| 9–11 | autumn |
| 12,1,2 | winter |

---

## 1. 展示タイム prior

### スクリプト
```
build_tenji_prior_from_raw.py
```

### 目的
- 展示タイムの「場×枠×季節」ごとの平均・分散を prior として推定
- サンプル不足セルは Empirical Bayes により全体平均へ縮約

### キー
- `place`
- `wakuban`
- `season_q`

### 主な出力列
| 列名 | 説明 |
|----|----|
| tenji_mu | 縮約後の平均展示タイム |
| tenji_sd | 縮約後の標準偏差 |
| n_tenji | サンプル数 |
| m_strength | 事前強度 |
| sd_floor | SD 下限 |

---

## 2. 季節×場×進入コース prior

### スクリプト
```
build_season_course_prior_from_raw.py
```

### 目的
- 季節・場・進入コースごとの入着傾向を事前確率として集計
- 枠の有利不利・季節特性を prior としてモデルに供給

### キー
- `place`
- `entry`
- `season_q`

---

## 3. 決まり手 prior

### スクリプト
```
build_season_winningtrick_prior_from_raw.py
```

### 目的
- 「1着になったとき、どの決まり手が出やすいか」を prior 化
- 場×進入×季節ごとの戦術傾向を数値化

### キー
- `place`
- `entry`
- `season_q`

### 出力系列
- 件数: `c_*`
- 絶対確率: `p_*`
- 全場平均: `base_p_*`
- 差分: `adv_p_*`
- 対数比: `lr_p_*`

---

## 運用ポリシー

- prior は **学習・推論で共通**
- 推論時は **latest.csv** を参照
- raw が更新されたら再生成するだけ

---
