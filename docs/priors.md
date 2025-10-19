# Priors リファレンス（更新版）

本書は `data/priors/` に保存される **固定参照テーブル（prior）** の仕様・生成手順・利用方法をまとめたものです。  
対象は **展示タイム prior**, **entry入着率 prior**, **決まり手 prior** の3種。

- `latest.csv` は直近スナップショットへのエイリアス。学習/推論時は基本こちらを参照します。  
- バージョン付きファイルは **再現性** のために残します（過去の学習を完全再現できます）。

---

## 0. クイックスタート（標準環境）

- Python は Anaconda 最新の `C:\anaconda3\python.exe` を想定（3.13系）。
- 期間を例として **20231201〜20241130** を使用。

```powershell
# リポジトリ直下で実行
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\batch\update_priors.ps1 `
  -From 20231201 -To 20241130 `
  -PythonExe "C:\anaconda3\python.exe"
```

出力先（＋ latest リンク更新）：
- `data/priors/tenji/…csv` ＆ `data/priors/tenji/latest.csv`
- `data/priors/season_course/…csv` ＆ `data/priors/season_course/latest.csv`
- `data/priors/winning_trick/…csv` ＆ `data/priors/winning_trick/latest.csv`

ログ：
- `logs/prior_update_YYYYMMDD_HHMMSS.log`

> `update_priors.ps1` は標準エラー（警告）で停止しない設計です。非ゼロ終了のみ失敗扱い。

---

## 1. 命名規則

```
<name>__<from>_<to>__keys-<join_keys><options>__m<m_strength>__v<version>.csv
```

- `<name>`  
  - `tenji_prior` / `season_course_prior` / `winning_trick_prior`
- `<from>_<to>`: 参照期間（YYYYMMDD）
- `keys-...`: 結合キーの並び（例: `place-entry-seasonq`）
- `<options>`: 主要ハイパラのラベル（例: `sdfloor-0.02`）
- `m<m_strength>`: 平滑化の疑似件数（エンピリカルベイズの強さ）
- `v<version>`: スキーマ世代

---

## 2. 生成コマンド

### 2.1 PS1（推奨）
```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\batch\update_priors.ps1 `
  -From 20231201 -To 20241130 `
  -PythonExe "C:\anaconda3\python.exe"
```

### 2.2 直接 Python で（代替）
```powershell
$Py = "C:\anaconda3\python.exe"

# tenji（season_q版）
& $Py .\scripts\build_tenji_prior_from_raw.py `
  --raw-dir data\raw --from 20231201 --to 20241130 `
  --m-strength 200 --sd-floor 0.02 `
  --out data\priors\tenji\tenji_prior__20231201_20241130__keys-place-wakuban-seasonq__sdfloor-0.02__m200__v1.csv `
  --link-latest

# entry入着率（相対化入り）
& $Py .\scripts\build_season_course_prior_from_raw.py `
  --raw-dir data\raw --from 20231201 --to 20241130 `
  --out data\priors\season_course\season_course_prior__20231201_20241130__keys-place-entry-seasonq__m0__v3.csv `
  --link-latest

# 決まり手（相対化入り）
& $Py .\scripts\build_season_winningtrick_prior_from_raw.py `
  --raw-dir data\raw --from 20231201 --to 20241130 `
  --out data\priors\winning_trick\winning_trick_prior__20231201_20241130__keys-place-entry-seasonq__m0__v3.csv `
  --link-latest
```

> 列名が環境で異なる場合は `--finish-col` / `--entry-col` / `--trick-col` を明示。

---

## 3. スキーマ

### 3.1 展示タイム prior（`tenji/latest.csv`）— v1  
**キー**: `place, wakuban, season_q`

| 列名 | 意味 |
|---|---|
| place, wakuban, season_q | 結合キー |
| tenji_mu, tenji_sd | 展示タイムの平均/標準偏差（EB縮約済み） |
| n_tenji | サンプル数 |
| built_from, built_to | 参照期間 |
| sd_floor, m_strength | ハイパラ（SD下限 / 事前強度） |
| keys, version | メタ |

> preprocess 側で `tenji_resid = time_tenji - tenji_mu`, `tenji_z = tenji_resid / tenji_sd` を生成。

---

### 3.2 entry入着率 prior（`season_course/latest.csv`）— v3  
**キー**: `place, entry, season_q`（分母=完走のみ）

| 列名群 | 意味 |
|---|---|
| n_finished | 分母（1〜6の数値着） |
| c1..c6 / p1..p6 | 1〜6着の件数/率 |
| base_p1..base_p6 | 同一 `season_q×entry` の全場平均 |
| adv_p1..adv_p6 | 差分: `p* - base_p*` |
| lr_p1..lr_p6 | 対数比: `log((p*+eps)/(base_p*+eps))`, `eps=1/(n+m+1)` |
| built_from, built_to, m_strength, keys, version | メタ |

---

### 3.3 決まり手 prior（`winning_trick/latest.csv`）— v3  
**キー**: `place, entry, season_q`（分母=n_win: rank==1 かつ決まり手6種が取得できた件数）

| 列名群 | 意味 |
|---|---|
| c_nige..c_megumare / p_nige..p_megumare | 6種の件数/率（逃げ/差し/まくり/まくり差し/抜き/恵まれ） |
| base_p_* / adv_p_* / lr_p_* | 全場平均との相対化（差分・対数比） |
| built_from, built_to, m_strength, keys, version | メタ |

---

## 4. 結合と投入

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
  - 時系列CV時は fold の **過去データのみ** で prior を組み直してリーク防止。  
  - 木系は p系の共線性を気にせず投入可。線形系は 1列ドロップ or `lr_*` を推奨。

---

## 5. 運用ノート

- **Python/環境**: 標準は `C:\anaconda3\python.exe`（3.13系）。  
- **文字コード**: スクリプトは **UTF-8** で保存。CP932由来のエラーはUTF-8へ変換で解消。  
- **ログ**: 期間・件数・ハイパラ・出力先を `[INFO]` で記録（`logs/`）。  
- **エラー抑止**: `update_priors.ps1` は警告(stderr)で止まりません。非0終了のみ失敗扱い。

---

## 6. バージョニング

- **v1**: `tenji`（season_q化）  
- **v3**: `season_course` / `winning_trick` に相対化（base/adv/lr）を追加  
- 破壊的変更時は `v` を +1。`latest.csv` は常に最新スキーマ。

---

## 7. 変更履歴（抜粋）

- 2025-10-19: **update_priors.ps1** を導入（stderr安全化／ログ集約）。標準 Python を `C:\anaconda3\python.exe` に統一。  
- 2025-10-19: 本ドキュメントを最新フロー（20231201〜20241130例・命名統一）に更新。
