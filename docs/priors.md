# Priors リファレンス

本書は `data/priors/` に保存される固定参照テーブルの仕様・生成手順・利用方法をまとめたものです。  
現在の対象は **展示タイム prior**, **entry入着率 prior**, **決まり手 prior** の3種。

---

- `latest.csv` は直近スナップショットへのエイリアス。学習/推論時は基本こちらを参照します。  
- バージョン付きファイルは**再現性**のために残します。

---

## 1. 命名規則
``` powershell
<name>__<from>_<to>keys-<join_keys><options>__m<m_strength>__v<version>.csv
```
- `<name>`: `tenji_prior` / `season_course_prior` / `season_winningtrick_prior`
- `<from>_<to>`: 参照期間（YYYYMMDD）
- `keys-...`: 結合キーの並び（例: `place-entry-seasonq`）
- `<options>`: 主要ハイパラのラベル（例: `sdfloor-0.02`）
- `m<m_strength>`: 平滑化の疑似件数
- `v<version>`: スキーマ世代

---

## 2. 生成コマンド（例）

- 展示タイム prior（season_q版）
``` powershell
python scripts/build_tenji_prior_from_raw.py --raw-dir data/raw --from 20241001 --to 20250331
--m-strength 200 --sd-floor 0.02
--out data/priors/tenji/tenji_prior__20241001_20250331__keys-place-wakuban-seasonq__sdfloor-0.02__m200__v1.csv
--link-latest
```

- entry入着率 prior（相対化入り）
``` powershell
python scripts/build_season_course_prior_from_raw.py --raw-dir data/raw --from 20241001 --to 20250331
--out data/priors/season_course/season_course_prior__20241001_20250331__keys-place-entry-seasonq__m0__v3.csv
--link-latest
```
> 列名が環境で異なる場合は `--finish-col` / `--entry-col` / `--trick-col` を明示。

---

## 3. スキーマ

### 3.1 展示タイム prior (`tenji/latest.csv`) — v1
**キー**: `place, wakuban, season_q`

| 列名 | 意味 |
|---|---|
| place, wakuban, season_q | 結合キー |
| tenji_mu, tenji_sd | そのセルの展示タイム平均/標準偏差（Empirical Bayes 縮約済み） |
| n_tenji | サンプル数 |
| built_from, built_to | 参照期間 |
| sd_floor, m_strength | ハイパラ（SD下限 / 事前強度） |
| keys, version | メタ |

> preprocess 側で `tenji_resid = time_tenji - tenji_mu`, `tenji_z = tenji_resid / tenji_sd` を生成。

---

### 3.2 entry入着率 prior（相対化入り）(`season_course/latest.csv`) — v3  
**キー**: `place, entry, season_q`（分母=完走のみ）

| 列名群 | 意味 |
|---|---|
| n_finished | 分母（1〜6の数値着） |
| c1..c6 | 1〜6着の件数 |
| p1..p6 | 絶対率（0〜1） |
| base_p1..base_p6 | 同一 `season_q×entry` の全場平均 |
| adv_p1..adv_p6 | 差分: `p* - base_p*` |
| lr_p1..lr_p6 | 対数比: `log((p*+eps)/(base_p*+eps))`, `eps=1/(n_finished+m+1)` |
| built_from, built_to, m_strength, keys, version | メタ |

---

### 3.3 決まり手 prior（相対化入り）(`winning_trick/latest.csv`) — v3  
**キー**: `place, entry, season_q`（分母=n_win: rank==1 かつ決まり手が6種で取得できた件数）

| 列名群 | 意味 |
|---|---|
| c_nige..c_megumare | 6種の件数（逃げ/差し/まくり/まくり差し/抜き/恵まれ） |
| p_nige..p_megumare | 絶対率（0〜1） |
| base_p_* | 同一 `season_q×entry` の全場平均 |
| adv_p_* | 差分 |
| lr_p_* | 対数比（`eps=1/(n_win+m+1)`） |
| built_from, built_to, m_strength, keys, version | メタ |

---

## 4. 結合と利用（preprocess での扱い）

- **結合キー**  
- tenji: `["place","wakuban","season_q"]`  
- season_course: `["place","entry","season_q"]`  
- winning_trick: `["place","entry","season_q"]`

- **推奨投入列**  
- tenji: `tenji_mu, tenji_sd`（→ `tenji_resid, tenji_z` を生成）
- season_course: `p1..p6, adv_p1..adv_p6, lr_p1..lr_p6, n_finished`
- winning_trick: `p_nige..p_megumare, adv_p_*, lr_p_*, n_win`

- **メモ**  
- 学習/推論とも `latest.csv` を参照（スナップショット更新で切替）。  
- 時系列CV時は fold の過去データのみで prior を作り直し、リークを防止。  
- 木系モデルは p系の共線性を気にせず投入可。線形系は 1列ドロップ or lr系を推奨。

---

## 5. バージョニング方針

- **v1**: tenji（season_q化）  
- **v3**: season_course / winning_trick に相対化（base/adv/lr）を追加  
- 破壊的変更時は version を +1。`latest.csv` は常に最新スキーマ。

---

## 6. 再現性 & ログ

- 各スクリプトは実行時に `[INFO]` ログを出力（期間・行数・使用列）。  
- 実験ではコマンドを `docs/usage_train.md` に貼って残すと吉。  
- ファイル名に期間・ハイパラを埋め込む命名規則により追跡容易。

---

```
