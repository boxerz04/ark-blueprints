# batch/run_html_vault_full_rebuild.ps1
# Build a portable vault of HTML bins (exclude odds2tf / odds3t).
# Output: data\sqlite\html_vault_compact.sqlite

param(
  [string]$PythonExe = "",
  [string]$SqliteExe = ""
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Paths
$RepoRoot   = Split-Path $PSScriptRoot -Parent
$HtmlRoot   = Join-Path $RepoRoot 'data\html'          # 入力ルート（ここからの相対パスで保存）
$SqliteDir  = Join-Path $RepoRoot 'data\sqlite'
$BuildDb    = Join-Path $SqliteDir 'html_vault_build.sqlite'
$CompactTmp = Join-Path $SqliteDir 'html_vault_compact.sqlite.tmp'
$CompactDb  = Join-Path $SqliteDir 'html_vault_compact.sqlite'
$LogDir     = Join-Path $RepoRoot 'logs'
$LogPath    = Join-Path $LogDir ("html_vault_full_{0}.log" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
$LockPath   = Join-Path $LogDir 'html_vault_full.lock'

New-Item -ItemType Directory -Force -Path $SqliteDir, $LogDir | Out-Null

# Resolve executables
function Resolve-Cmd {
  param([string]$hint, [string[]]$candidates)
  if ($hint) { if (Test-Path $hint) { return $hint }; $c=Get-Command $hint -ErrorAction SilentlyContinue; if($c){return $c.Source}; throw "Not found: $hint" }
  foreach ($x in $candidates) { if ($x -match '\\'){ if(Test-Path $x){return $x} } else { $c=Get-Command $x -ErrorAction SilentlyContinue; if($c){return $c.Source} } }
  return $null
}

$PythonExe = Resolve-Cmd $PythonExe @(
  "$env:CONDA_PREFIX\python.exe",
  "C:\anaconda3\python.exe",
  "$env:USERPROFILE\anaconda3\python.exe",
  "python","py"
) ; if (-not $PythonExe) { throw "Python not found. Use -PythonExe to specify." }

$SqliteExe = Resolve-Cmd $SqliteExe @(
  "C:\anaconda3\Library\bin\sqlite3.exe",
  "$env:USERPROFILE\anaconda3\Library\bin\sqlite3.exe",
  "sqlite3.exe","sqlite3"
) ; if (-not $SqliteExe) { throw "sqlite3 not found. Use -SqliteExe to specify." }

# Prevent double-run
if (Test-Path $LockPath) { Write-Host "[SKIP] lock exists: $LockPath"; exit 0 }
New-Item -ItemType File -Force -Path $LockPath | Out-Null

try {
  Write-Host "[INFO] HTML vault full rebuild (root: $HtmlRoot)"
  Remove-Item $BuildDb,$CompactTmp -Force -ErrorAction SilentlyContinue

  # 取り込み対象（odds系は入れない）
  $Patterns = @(
    'beforeinfo\*.bin',
    'index\*.bin',
    'pay\*.bin',
    'pcexpect\*.bin',
    'raceindex\*.bin',
    'racelist\*.bin',
    'raceresult\*.bin',
    'rankingmotor\*.bin'
  )

  $env:TQDM_DISABLE = "1"  # 進捗抑止（PSでstderrをエラー扱いさせない）
  $ingest = Join-Path $RepoRoot 'scripts\vault_csv_by_pattern.py'

  foreach ($pat in $Patterns) {
    Write-Host "[INFO] ingest: $pat"
    & "$PythonExe" $ingest `
      --input-dir "$HtmlRoot" `
      --db "$BuildDb" `
      --glob "$pat" `
      --all `
      --gzip `
      --no-progress `
      --commit-every '2000' 2>&1 | Tee-Object -FilePath $LogPath
    if ($LASTEXITCODE -ne 0) { throw "ingest failed at $pat (exit $LASTEXITCODE)" }
  }
  Remove-Item Env:\TQDM_DISABLE -ErrorAction SilentlyContinue

  # コンパクト・スナップショット作成（原子的置換）
  & "$SqliteExe" "$BuildDb" "PRAGMA wal_checkpoint(FULL); VACUUM INTO '$CompactTmp';" 2>&1 | Tee-Object -FilePath $LogPath -Append
  if ($LASTEXITCODE -ne 0) { throw "sqlite3 VACUUM INTO failed: $LASTEXITCODE" }
  Move-Item -Force $CompactTmp $CompactDb

  # サマリ
  & "$SqliteExe" "$CompactDb" "SELECT COUNT(*), printf('%.1f MB', SUM(size)/1048576.0) FROM file_index;" 2>&1 | Tee-Object -FilePath $LogPath -Append
  & "$SqliteExe" "$CompactDb" "SELECT COALESCE(substr(rel_path,1,instr(rel_path,'/')-1), rel_path) AS top, COUNT(*), printf('%.1f MB', SUM(size)/1048576.0) FROM file_index GROUP BY top ORDER BY top;" 2>&1 | Tee-Object -FilePath $LogPath -Append

  Write-Host "[DONE] Rebuilt: $CompactDb"
}
finally {
  Remove-Item $LockPath -ErrorAction SilentlyContinue
}
