# Vault化・復元ガイド（raw / raceinfo 共通）

このドキュメントは、**CSVを“そのままのバイト列”でSQLiteに保管（Vault化）**し、どこでも**1ファイルで持ち運び＆復元**できる運用手順のまとめです。

---

## TL;DR（最短手順）

- **毎日自動で最新Vaultを作る**（タスク名の例：`ark-vault-snapshot`）  
  - 実行ファイル: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`  
  - 開始（オプション）: `C:\Users\user\Desktop\Git\ark-blueprints`  
  - 引数（成功実績のある安定構成）:
    ```
    -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -Command "Set-Location 'C:\Users\user\Desktop\Git\ark-blueprints'; & '.\batch\run_all_vaults_full_rebuild.ps1' -PythonExe 'C:\anaconda3\python.exe' -SqliteExe 'C:\anaconda3\Library\bin\sqlite3.exe' 2>&1 | Tee-Object -FilePath 'C:\ProgramData\ArkVault\logs\task_last_run.log'"
    ```

- **持ち運び**：できあがるポータブルDB（2つ）  
  - `data\sqlite\csv_vault_compact.sqlite`（raw）  
  - `data\sqlite\raceinfo_vault_compact.sqlite`（raceinfo）

- **別環境での復元（1行×2系統 or 1行まとめ）**
  ```bat
  (mkdir restore_all\raw 2>nul & python scripts\export_vault.py --db "D:\csv_vault_compact.sqlite" --dest "restore_all\raw" --pattern "%.csv") && (mkdir restore_all\processed\raceinfo 2>nul & python scripts\export_vault.py --db "D:\raceinfo_vault_compact.sqlite" --dest "restore_all\processed\raceinfo" --pattern "%.csv")
  ```

---

## 1. Vaultの考え方（共通スキーマ）

- **保存形式**：ファイルの**生バイト列**をそのままBLOB保存（任意で**gzip圧縮**）。  
- **重複排除**：`sha256`（内容アドレス化）で**同一内容は1回だけ**保存。  
- **索引**：`file_index` に相対パス・更新時刻・元サイズ・`date_ymd` を記録。  
- **可搬性**：`*_compact.sqlite` は **WALなし・断片化ゼロのスナップショット**＝1ファイルで持ち運び可。

**テーブル**（共通）
```
object_store(sha256 TEXT PK, size INTEGER, is_gzip INTEGER, bytes BLOB)
file_index(id INTEGER PK, rel_path TEXT UNIQUE, mtime REAL, size INTEGER, sha256 TEXT FK, date_ymd TEXT)
```

---

## 2. 使うスクリプト

- **取り込み（共通化）**：`scripts/vault_csv_by_pattern.py`  
  - 任意ディレクトリのCSVを**glob/regex**で拾い、生バイトで格納（`--all` で全件、または `--start/--end` で期間指定）。
- **一括フル再構築（タスク側のエントリポイント）**：`batch/run_all_vaults_full_rebuild.ps1`  
  - **raw + raceinfo を毎回ゼロからVault化** → `VACUUM INTO` で安全に `*_compact.sqlite` に置換。  
  - 引数 `-PythonExe` / `-SqliteExe` で実行ファイルのフルパスを指定可能（タスク環境でPATHが無い問題を回避）。
- **復元**：`scripts/export_vault.py`  
  - Vault DBから**元バイト列でCSV再出力**（gzip格納でも自動で解凍）。

---

## 3. フル再構築（手動）

> ふだんはタスクでOK。必要に応じて手動実行。

```bat
powershell -NoProfile -ExecutionPolicy Bypass -File batch\run_all_vaults_full_rebuild.ps1 -PythonExe "C:\anaconda3\python.exe" -SqliteExe "C:\anaconda3\Library\bin\sqlite3.exe"
```

**出力**：
- `data\sqlite\csv_vault_compact.sqlite`
- `data\sqlite\raceinfo_vault_compact.sqlite`

**ログ**：
- リポジトリ側：`logs\all_vaults_full_*.log`
- タスク経由時（例）：`C:\ProgramData\ArkVault\logs\task_last_run.log`

---

## 4. 復元（別環境）

- **raw → `restore_all\raw`**  
  `python scripts\export_vault.py --db "D:\csv_vault_compact.sqlite" --dest "restore_all\raw" --pattern "%.csv"`
- **raceinfo → `restore_all\processed\raceinfo`**
  `python scripts\export_vault.py --db "D:\raceinfo_vault_compact.sqlite" --dest "restore_all\processed\raceinfo" --pattern "%.csv"`

- **1行にまとめる（両方）**
  ```bat
  (mkdir restore_all\raw 2>nul & python scripts\export_vault.py --db "D:\csv_vault_compact.sqlite" --dest "restore_all\raw" --pattern "%.csv") && (mkdir restore_all\processed\raceinfo 2>nul & python scripts\export_vault.py --db "D:\raceinfo_vault_compact.sqlite" --dest "restore_all\processed\raceinfo" --pattern "%.csv")
  ```

**検証（件数一致）**
```bat
sqlite3 D:\csv_vault_compact.sqlite "SELECT COUNT(*) FROM file_index;"
dir /b restore_all\raw\*_raw.csv | find /c ".csv"

sqlite3 D:\raceinfo_vault_compact.sqlite "SELECT COUNT(*) FROM file_index;"
dir /b restore_all\processed\raceinfo\raceinfo_*.csv | find /c ".csv"
```

**整合性**
```bat
sqlite3 D:\csv_vault_compact.sqlite "PRAGMA integrity_check;"
sqlite3 D:\raceinfo_vault_compact.sqlite "PRAGMA integrity_check;"
```

---

## 5. タスク スケジューラ設定（例：毎日 02:40 / `ark-vault-snapshot`）

- **プログラム/スクリプト**  
  `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
- **開始（オプション）**  
  `C:\Users\user\Desktop\Git\ark-blueprints`
- **引数の追加（オプション）**（実績あり・ログ採取つき）
  ```
  -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -Command "Set-Location 'C:\Users\user\Desktop\Git\ark-blueprints'; & '.\batch\run_all_vaults_full_rebuild.ps1' -PythonExe 'C:\anaconda3\python.exe' -SqliteExe 'C:\anaconda3\Library\bin\sqlite3.exe' 2>&1 | Tee-Object -FilePath 'C:\ProgramData\ArkVault\logs\task_last_run.log'"
  ```
- **推奨オプション**：  
  - 「**ユーザーがログオンしているかどうかにかかわらず実行**」  
  - 「**最上位の特権で実行**」  
  - **履歴を有効化**（失敗時の原因追跡が容易）

---

## 6. よく使うコマンド（チートシート）

**USBへコピー（手動、バージョンなし）**
```bat
copy /Y data\sqlite\csv_vault_compact.sqlite D:\ & copy /Y data\sqlite\raceinfo_vault_compact.sqlite D:\
```

**サイズ・更新時刻の確認**
```bat
powershell -NoProfile -Command "Get-Item .\data\sqlite\*_vault_compact.sqlite | Select Name,Length,LastWriteTime | ft -Auto"
```

**Vault内サマリ**
```bat
sqlite3 data\sqlite\csv_vault_compact.sqlite      "SELECT COUNT(*), printf('%.1f MB', SUM(size)/1048576.0) FROM file_index;"
sqlite3 data\sqlite\raceinfo_vault_compact.sqlite "SELECT COUNT(*), printf('%.1f MB', SUM(size)/1048576.0) FROM file_index;"
```

---

## 7. トラブルシューティング

- **タスクで落ちる／何も起きない**  
  - 実行ユーザーの権限と「開始（オプション）」を確認。
  - `-Command "Set-Location ...; & .\batch\... | Tee-Object ..."` 方式で**必ずログ**を採る。  
  - `-PythonExe`/`-SqliteExe` に**フルパス**を指定（例：`C:\anaconda3\python.exe` / `C:\anaconda3\Library\bin\sqlite3.exe`）。
- **sqlite3が見つからない**  
  - `where sqlite3` で場所を確認 → `-SqliteExe` に渡す。
  - 代替：`scripts/sqlite_vacuum_into.py` を使えば Python だけでも VACUUM 可能（必要なら追加）。
- **処理が一瞬で終わる**  
  - 正常です。ログに件数・MBが出ます。

---

## 8. Git管理の注意

- 復元物はコミットしない：`.gitignore` に入れておく  
  ```
  /restore_all/
  /restore_q3/
  /restore_raceinfo/
  ```

---

## 付録：手動で直接インポート（高度な用途）

> 通常は不要。`vault_csv_by_pattern.py` を単体で使いたい場合。

- **raw**
  ```bat
  python scripts\vault_csv_by_pattern.py --input-dir data\raw --db data\sqlite\csv_vault_build.sqlite --glob "*.csv" --regex "^(?P<ymd>\d{8})_raw\.csv$" --all --gzip --no-progress
  sqlite3 data\sqlite\csv_vault_build.sqlite "PRAGMA wal_checkpoint(FULL); VACUUM INTO 'data/sqlite/csv_vault_compact.sqlite';"
  ```
- **raceinfo**
  ```bat
  python scripts\vault_csv_by_pattern.py --input-dir data\processed\raceinfo --db data\sqlite\raceinfo_vault_build.sqlite --glob "*.csv" --regex "^raceinfo_(?P<ymd>\d{8})\.csv$" --all --gzip --no-progress
  sqlite3 data\sqlite\raceinfo_vault_build.sqlite "PRAGMA wal_checkpoint(FULL); VACUUM INTO 'data/sqlite/raceinfo_vault_compact.sqlite';"
  ```

---

以上。**Vault = 「原本のCSVをそのまま安全に持ち運ぶための金庫」**、分析用DBはいつでも再生成できます。運用でハマったら、このガイドの「タスク設定」と「トラブルシュート」を参照してください。
