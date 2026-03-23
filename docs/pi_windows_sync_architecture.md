# Raspberry Pi + Windows 同期運用アーキテクチャ

本書は、現行の本番運用である **「Raspberry Pi を生成系の正本、Windows を同期・master構築・学習担当」** という役割分担を、後から短時間で再理解できるように整理した運用メモです。

既存の `master_pipeline.md`、`training_pipeline.md`、`motor_pipeline.md`、および `sync_from_pi.ps1` / `batch/*.ps1` の実装に合わせて、運用判断・確認ポイント・障害切り分け観点を最小限の密度でまとめます。

---

## 概要

- **Pi 3B+ + SSD** を生成系データの正本（source of truth）として運用する。
- Pi 側では **cron によりスクレイピングと中間生成を継続実行**し、`raw` / `refund` / `raceinfo` / `motor` を生成・保存する。
- Windows 側では `sync_from_pi.ps1` を**定時実行**し、Pi 上の共有ディレクトリから SMB 経由でローカル `data` 配下へ同期する。
- Windows 側では同期済みデータを使って `batch/build_master_range.ps1` による **master 構築**、`batch/train_model_from_master.ps1` による **学習**を行う。
- 短期比較で **Pi 生成データを Windows 学習へ投入した結果が従来データと完全一致**し、さらに **2024-10-01〜2025-12-31 学習で `v1.2.0-finals-hpo` 相当モデルを再現**できたため、現行構成を本番採用した。

---

## 構成図

```mermaid
flowchart LR
    subgraph PI[Raspberry Pi 3B+ + SSD]
        Cron[cron]
        Scrape[scrape.py\nHTML取得]
        Raw[build_raw_csv.py\nraw/refund生成]
        Raceinfo[build_raceinfo.py\nraceinfo生成]
        Motor[build_motor_artifacts_from_bins.py\n+ build_raw_with_motor_joined.py\n+ build_motor_section_*.py]
        PiData[/mnt/ssd/.../arkdata\nSMB共有]
        PiLogs[Pi側ログ保存]

        Cron --> Scrape --> Raw
        Scrape --> Raceinfo
        Raw --> PiData
        Raceinfo --> PiData
        Motor --> PiData
        Cron --> PiLogs
    end

    subgraph WIN[Windows]
        Sync[sync_from_pi.ps1\n定時同期]
        LocalData[data\\raw\ndata\\refund\ndata\\processed\\raceinfo\ndata\\processed\\motor]
        Master[batch/build_master_range.ps1\nmaster / master_finals 構築]
        Train[batch/train_model_from_master.ps1\n学習]
        SyncLogs[data\\_sync_logs]

        Sync --> LocalData
        Sync --> SyncLogs
        LocalData --> Master --> Train
    end

    PiData -- SMB / Samba --> Sync
```

---

## 役割分担

### Pi 側の役割（生成正本）

Pi 側は **生成系の責務を一元化**する。

- cron による定期実行
- `scripts/scrape.py` による HTML / `.bin` 取得
- `scripts/build_raw_csv.py` による `raw` / `refund` 生成
- `scripts/build_raceinfo.py` による `raceinfo` 生成
- モーター系スクリプト群による `motor_id_map__all.csv` / `motor_section_features_n__all.csv` の生成
- 生成処理のログ保存
- Samba/SMB 共有による Windows への配布元提供

### Windows 側の役割（同期・master構築・学習）

Windows 側は **消費系・学習系の責務を一元化**する。

- `sync_from_pi.ps1` を定時実行して Pi の生成物をローカル `data` に同期
- 同期後の `data` を入力に `batch/build_master_range.ps1` で `master.csv` / `master_finals.csv` を構築
- `batch/train_model_from_master.ps1` で学習を実行
- 必要に応じて `batch/update_priors.ps1` で prior を更新
- 必要に応じて `batch/run_build_motor_pipeline.ps1` 相当の処理内容を参照し、motor 成果物の整合確認を行う

### 本番運用としての判断

現時点の本番方針は次のとおりです。

- **生成の正本は Pi** とする
- **master 構築と学習の正本は Windows** とする
- したがって、障害切り分けではまず **「生成異常か、同期異常か、学習異常か」** の3層に分けて確認する

---

## パス構成

### Pi 側

運用上の正本ストレージは **`/mnt/ssd`** 配下です。

- SSD マウントポイント: `/mnt/ssd`
- プロジェクト配置: `/mnt/ssd/projects/ark-blueprints`
- SMB 共有ルート: `\\192.168.10.125\arkdata`

共有ルート配下で、Windows が参照する想定パスは以下です。

| 用途 | Pi 側パス | 備考 |
|---|---|---|
| raw | `\\192.168.10.125\arkdata\raw` | `*_raw.csv` を同期 |
| refund | `\\192.168.10.125\arkdata\refund` | `*_refund.csv` を同期 |
| raceinfo | `\\192.168.10.125\arkdata\processed\raceinfo` | `raceinfo_*.csv` を同期 |
| motor | `\\192.168.10.125\arkdata\processed\motor` | 必要2ファイルのみ同期 |

### Windows 側

`sync_from_pi.ps1` の既定値では、Windows ローカルの同期先は以下です。

- ローカル data ルート: `C:\Users\user\Desktop\Git\ark-blueprints\data`
- 同期ログ: `data\_sync_logs`

既定の主要ディレクトリは次のとおりです。

| 用途 | Windows 側パス |
|---|---|
| raw | `data\raw` |
| refund | `data\refund` |
| raceinfo | `data\processed\raceinfo` |
| motor | `data\processed\motor` |
| sync log | `data\_sync_logs` |

---

## 同期仕様

### 同期の入口

Windows 側では `sync_from_pi.ps1` を入口として同期します。実装上は `robocopy` を使い、ログを `data\_sync_logs\sync_from_pi_yyyyMMdd_HHmmss.log` に追記保存します。

### 同期対象

`sync_from_pi.ps1` で実際に同期している対象は以下です。

| No. | Pi 側 | Windows 側 | 対象ファイル |
|---|---|---|---|
| 1 | `\\192.168.10.125\arkdata\raw` | `data\raw` | `*_raw.csv` |
| 2 | `\\192.168.10.125\arkdata\refund` | `data\refund` | `*_refund.csv` |
| 3 | `\\192.168.10.125\arkdata\processed\raceinfo` | `data\processed\raceinfo` | `raceinfo_*.csv` |
| 4 | `\\192.168.10.125\arkdata\processed\motor` | `data\processed\motor` | `motor_id_map__all.csv`, `motor_section_features_n__all.csv` |

### 実装上のポイント

- `raw` / `refund` / `raceinfo` は `/E` 付きでディレクトリ単位同期する
- `motor` は **学習に必要な2ファイルだけ**を明示指定で同期する
- `robocopy` は `/Z /FFT /R:2 /W:5 /COPY:DAT /DCOPY:DAT /XJ /NP /TEE /LOG+` を利用する
- `robocopy` の終了コードが **8以上なら失敗**として停止する
- `-Preview` スイッチ時は `/L` を付けてドライランできる

### 同期後に Windows 側で使う主な処理

同期完了後、Windows 側では以下の流れでデータを消費します。

1. `batch/build_master_range.ps1`
   - `scripts/preprocess.py`
   - `scripts/preprocess_course.py`
   - `scripts/preprocess_sectional.py`
   - `scripts/preprocess_motor_id.py`
   - `scripts/preprocess_motor_section.py`
   - `scripts/make_master_finals.py`
2. `batch/train_model_from_master.ps1`
   - `scripts/preprocess_base_features.py`
   - `scripts/train.py`

### 関連スクリプトの位置づけ

Pi 側の生成責務に対応する、repo 上の主なスクリプト/バッチは以下の対応で理解すると混乱しにくいです。

| 区分 | 主な実体 | 役割 |
|---|---|---|
| スクレイピング | `scripts/scrape.py` / `batch/run_scrape_build_raceinfo_range.ps1` | HTML取得の入口 |
| raw/refund 生成 | `scripts/build_raw_csv.py` | `data/raw`, `data/refund` を生成 |
| raceinfo 生成 | `scripts/build_raceinfo.py` / `batch/run_scrape_build_raceinfo_range.ps1` | `data/processed/raceinfo` を生成 |
| motor 生成 | `batch/run_build_motor_pipeline.ps1` と motor 系 `scripts/*.py` | `data/processed/motor` を生成 |
| master 構築 | `batch/build_master_range.ps1` | `master.csv`, `master_finals.csv` を確定 |
| 学習 | `batch/train_model_from_master.ps1` | 特徴量生成と学習 |

---

## 検証結果

本構成を本番化した根拠は次の2点です。

1. **Pi 生成データを Windows 側学習に投入した短期比較で、従来データと完全一致した**
2. **2024-10-01〜2025-12-31 の学習で `v1.2.0-finals-hpo` 相当モデルを再現できた**

このため、現時点では以下の設計判断を維持します。

- 生成責務を Pi に寄せる
- 学習責務を Windows に寄せる
- Windows 側は「生成し直す」のではなく、まず **Pi 正本を同期して使う**

---

## 運用方針

### 基本方針

- 日次生成の正本は Pi 側で持つ
- Windows 側は Pi 成果物の同期先として扱う
- 学習・master 再構築は Windows で行う
- 生成系に問題がない限り、Windows 側で元データを独自再生成しない

### 変更判断の基準

以下の条件が崩れない限り、現行方針を維持してよいです。

- Pi 側 cron による生成が継続して成功している
- `sync_from_pi.ps1` による同期が安定している
- `build_master_range.ps1` が `master.csv` / `master_finals.csv` を正常生成できる
- `train_model_from_master.ps1` による学習再現性が維持される

### 過去障害の教訓

既知の同期失敗は **Pi 側の生成不良ではなく、Windows 側の SMB 認証不足**が原因でした。したがって、同期失敗時は最初に Pi の生成処理を疑うのではなく、**Windows から共有に入れるか、認証状態が維持されているか**を確認するのが正しい順序です。

---

## 監視ポイント

### Pi 側

- cron 実行ログが更新されているか
- 当日分の `raw` / `refund` / `raceinfo` が生成されているか
- `processed/motor` の2成果物が期待どおり存在するか
- SSD 空き容量が十分に残っているか
- SMB 共有が生きているか

### Windows 側

- `data\_sync_logs` に定時ログが出ているか
- `sync_from_pi.ps1` のログで `robocopy` 失敗が出ていないか
- 同期対象の最新ファイルがローカルに反映されているか
- `build_master_range.ps1` 実行時に `raceinfo` / `motor` 結合で異常が出ていないか
- `train_model_from_master.ps1` が期待 run を出力しているか

### 日次で最低限見るべきもの

- Pi 側: 当日 `*_raw.csv` と `raceinfo_*.csv` の有無
- Windows 側: `data\_sync_logs` 最新ログとローカル同期結果
- 学習前: `data\processed\motor` の2ファイル更新日時

---

## 障害切り分け

### 1. Windows にファイルが来ない

最初に見る順序は以下です。

1. Windows から `\\192.168.10.125\arkdata` にアクセスできるか
2. SMB 認証が切れていないか
3. `sync_from_pi.ps1` の最新ログに `robocopy` 失敗がないか
4. Pi 側共有ディレクトリに対象ファイル自体があるか

この症状では、**Windows 側認証問題の可能性が高い**です。過去実績でも、原因は Pi ではなく Windows の SMB 認証不足でした。

### 2. Pi にはあるが Windows の一部しか更新されない

- 対象ファイル名が `sync_from_pi.ps1` のフィルタに一致しているか確認する
- 特に `motor` は全件同期ではなく、**2ファイルのみ明示同期**である点に注意する
- `raceinfo` は `raceinfo_*.csv` 以外の名前だと同期対象外になる

### 3. master 構築で失敗する

- `data\raw` が揃っているか
- `data\processed\raceinfo` が揃っているか
- `data\processed\motor\motor_id_map__all.csv` があるか
- `data\processed\motor\motor_section_features_n__all.csv` があるか
- 必要に応じて `build_master_range.ps1` の `-SkipSectional` / `-SkipMotorId` / `-SkipMotorSection` で切り分ける

### 4. 学習が再現しない

- 同期元データの期間が揃っているか
- `master.csv` / `master_finals.csv` の生成条件が前回と一致しているか
- `features/*.yaml` と `models/*/params.yaml` が前回条件と一致しているか
- `train_model_from_master.ps1` の引数差分がないか

### 5. Pi 側を疑うべきケース

以下は Pi 側生成異常を優先して疑うべきケースです。

- 共有先に当日ファイル自体が存在しない
- `raw` はあるが `refund` / `raceinfo` だけ継続的に欠落する
- `motor_id_map__all.csv` や `motor_section_features_n__all.csv` が更新されない
- cron ログが止まっている

---

## 容量状況

以下は現行構成を判断した時点の容量スナップショットです。

### SSD 全体

- マウントポイント: `/mnt/ssd`
- `df -h` 時点: **458G 中 6.4G 使用 / 428G 空き / 使用率 2%**

### プロジェクト使用量

- `/mnt/ssd/projects/ark-blueprints`: **約 2.4G**
- うち `data` 配下: **約 1.9G**

### 運用上の解釈

- 現時点では容量逼迫は起きていない
- ボトルネックはディスクよりも **同期失敗・認証・日次生成停止** を先に疑うべき段階
- ただし `raw` / `html` / ログが長期蓄積するため、月次または四半期で容量推移は確認する

---

## 更新履歴

- 2026-03-23: 初版作成。現行の Pi 正本 / Windows 同期・master構築・学習運用を文書化。
