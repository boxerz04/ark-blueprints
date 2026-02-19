# scripts/build_season_course_prior_from_raw.py

## 1. 概要

| 項目 | 内容 |
|---|---|
| 目的 | `data/raw/*.csv` から季節×場×entry別の入着率 prior を生成 |
| 実行単位 | 期間（`--from`〜`--to`） |
| 主な利用シーン | 学習時の外部 prior 参照・特徴量結合 |
| used_by（SSOT） | `priors_pipeline`, `batch_ps1`, `training_pipeline` |

## 2. 入出力

### 2.1 入力

| 種別 | パス/形式 | 必須 | 説明 |
|---|---|:---:|---|
| raw日次CSV | `data/raw/*.csv` | ✅ | `preprocess.load_raw` で結合 |
| 期間指定 | `--from YYYYMMDD --to YYYYMMDD` | ✅ | 両端含む |
| 列指定（任意） | `--finish-col`, `--entry-col` |  | 未指定時は自動検出 |
| 平滑化（任意） | `--m-strength` |  | Dirichlet 疑似件数 |

### 2.2 出力

| 種別 | パス/形式 | 説明 |
|---|---|---|
| ファイル | `--out` 指定先CSV | season-course prior 本体 |
| 追加リンク | `latest.csv`（`--link-latest`時） | 同ディレクトリに最新を上書き |

## 3. 集計仕様

| 観点 | 内容 |
|---|---|
| 集計キー | `place, entry(1..6), season_q(spring/summer/autumn/winter)` |
| 分母/分子 | 完走（1〜6着の数値着）のみ計上 |
| 除外 | DNS/欠/落/転/妨/F/L 等 |
| 相対化 | 同一 `season_q × entry` の全場平均を基準化 |

## 4. 出力指標

| 列群 | 意味 |
|---|---|
| `n_finished, c1..c6` | 完走件数と着順別件数 |
| `p1..p6` | 絶対入着率（0〜1） |
| `base_p1..base_p6` | 全場平均ベースライン |
| `adv_p1..adv_p6` | 差分（`p - base_p`） |
| `lr_p1..lr_p6` | 対数比（`log((p+eps)/(base+eps))`） |

## 5. 列自動検出

| 対象 | 自動検出候補 | 備考 |
|---|---|---|
| 着順列 | `rank/arrival/chakujun/chaku/finish/finish_order` | `--finish-col` で固定可 |
| 進入列 | `entry/course` | `--entry-col` で固定可 |
| 値正規化 | 全角数字→半角、`entry/rank` は 1..6 のみ採用 |  |

## 6. CLI 引数

| 引数 | 必須 | デフォルト | 説明 | 例 |
|---|:---:|---|---|---|
| `--raw-dir` |  | `data/raw` | raw日次CSVのディレクトリ | `data/raw` |
| `--from` | ✅ | なし | 開始日（`YYYYMMDD`） | `20240101` |
| `--to` | ✅ | なし | 終了日（`YYYYMMDD`） | `20250630` |
| `--finish-col` |  | 自動検出 | 着順列名 | `rank` |
| `--entry-col` |  | 自動検出 | 進入列名 | `entry` |
| `--m-strength` |  | `0` | 平滑化の疑似件数m | `60` |
| `--out` | ✅ | なし | 出力CSVパス | `data/priors/season_course/2025H1.csv` |
| `--link-latest` |  | `False` | `latest.csv` を更新 | `--link-latest` |

## 7. 実行例

```bash
python scripts/build_season_course_prior_from_raw.py \
  --raw-dir data/raw \
  --from 20240101 --to 20250630 \
  --out data/priors/season_course/20240101_20250630.csv \
  --link-latest

python scripts/build_season_course_prior_from_raw.py \
  --from 20250101 --to 20250630 \
  --m-strength 60 \
  --out data/priors/season_course/2025H1_m60.csv
```

## 8. 生成カラム（順序）

| カラム |
|---|
| `place, entry, season_q, n_finished` |
| `c1..c6, p1..p6` |
| `base_p1..base_p6, adv_p1..adv_p6, lr_p1..lr_p6` |
| `built_from, built_to, m_strength, keys, version` |

## 9. 運用メモ / 注意点

| 観点 | 内容 |
|---|---|
| データ空期間 | 期間内に有効行が無い場合、利用可能期間をエラーメッセージで案内 |
| バージョン | `version=3`（相対化列を含む） |
| 利用方法 | `place×entry×season_q` で学習特徴量へ JOIN 可能 |
