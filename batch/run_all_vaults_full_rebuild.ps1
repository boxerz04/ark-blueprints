# batch/run_all_vaults_full_rebuild.ps1
# Rebuild BOTH vaults (raw and raceinfo) from ALL CSVs, then create compact snapshots safely.
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File batch\run_all_vaults_full_rebuild.ps1
#   (no PATH) add:
#     -PythonExe "C:\Users\user\anaconda3\python.exe" -SqliteExe "C:\tools\sqlite3.exe"
# Selection:
#   -RawOnly     -> run only raw
#   -RaceinfoOnly-> run only raceinfo

param(
  [string]$PythonExe = "",
  [string]$SqliteExe = "",
  [switch]$RawOnly,
  [switch]$RaceinfoOnly
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# ---------- paths ----------
$RepoRoot  = Split-Path $PSScriptRoot -Parent
$SqliteDir = Join-Path $RepoRoot 'data\sqlite'
$LogDir    = Join-Path $RepoRoot 'logs'
New-Item -ItemType Directory -Force -Path $SqliteDir, $LogDir | Out-Null

# sources
$RawSrc      = Join-Path $RepoRoot 'data\raw'
$RaceSrc     = Join-Path $RepoRoot 'data\processed\raceinfo'

# outputs (DBs)
$RawBuildDb      = Join-Path $SqliteDir 'csv_vault_build.sqlite'
$RawCompactDb    = Join-Path $SqliteDir 'csv_vault_compact.sqlite'
$RaceBuildDb     = Join-Path $SqliteDir 'raceinfo_vault_build.sqlite'
$RaceCompactDb   = Join-Path $SqliteDir 'raceinfo_vault_compact.sqlite'

$LogPath = Join-Path $LogDir ("all_vaults_full_{0}.log" -f (Get-Date -Format 'yyyyMMdd_HHmmss'))
$LockPath = Join-Path $LogDir 'all_vaults_full.lock'

# ---------- resolve executables robustly ----------
function Resolve-Cmd {
  param([string]$hint, [string[]]$candidates)
  if ($hint) {
    if (Test-Path $hint) { return $hint }
    $cmd = Get-Command $hint -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    throw "Not found: $hint"
  }
  foreach ($c in $candidates) {
    if ($c -match '\\') {
      if (Test-Path $c) { return $c }
    } else {
      $cmd = Get-Command $c -ErrorAction SilentlyContinue
      if ($cmd) { return $cmd.Source }
    }
  }
  return $null
}

$PythonExe = Resolve-Cmd $PythonExe @(
  "$env:CONDA_PREFIX\python.exe",
  "$env:USERPROFILE\anaconda3\python.exe",
  "$env:USERPROFILE\miniconda3\python.exe",
  "$env:ProgramData\Anaconda3\python.exe",
  "python","py"
)
if (-not $PythonExe) { throw "Python not found. Use Anaconda Prompt or pass -PythonExe." }

$SqliteExe = Resolve-Cmd $SqliteExe @("sqlite3.exe","sqlite3")
if (-not $SqliteExe) { throw "sqlite3 not found. Add to PATH or pass -SqliteExe." }

# ---------- helpers ----------
function Invoke-RebuildVault {
  param(
    [string]$Label,      # e.g., "raw" or "raceinfo"
    [string]$SrcDir,
    [string]$BuildDb,
    [string]$CompactDb,
    [string]$Glob,
    [string]$Regex       # must contain (?P<ymd>...) to enable date extraction if ever needed
  )

  $CompactTmp = "$CompactDb.tmp"
  Write-Host "[INFO][$Label] full rebuild (ALL files in $SrcDir)"

  # clean previous artifacts
  Remove-Item $BuildDb, $CompactTmp -Force -ErrorAction SilentlyContinue

  # ingest ALL (gzip, dedupe) with progress disabled (avoid stderr noise)
  $env:TQDM_DISABLE = "1"
  $vaultScript = Join-Path $RepoRoot 'scripts\vault_csv_by_pattern.py'
  $pythonArgs = @(
    $vaultScript,
    '--input-dir', $SrcDir,
    '--db',        $BuildDb,
    '--glob',      $Glob,
    '--regex',     $Regex,
    '--all',
    '--gzip',
    '--no-progress',
    '--commit-every', '5000'
  )
  & "$PythonExe" @pythonArgs 2>&1 | Tee-Object -FilePath $LogPath
  if ($LASTEXITCODE -ne 0) { throw "[$Label] ingest failed: $LASTEXITCODE" }
  Remove-Item Env:\TQDM_DISABLE -ErrorAction SilentlyContinue

  # compact snapshot (tmp -> atomic replace)
  & "$SqliteExe" "$BuildDb" "PRAGMA wal_checkpoint(FULL); VACUUM INTO '$CompactTmp';" 2>&1 | Tee-Object -FilePath $LogPath -Append
  if ($LASTEXITCODE -ne 0) { throw "[$Label] VACUUM INTO failed: $LASTEXITCODE" }
  Move-Item -Force $CompactTmp $CompactDb

  # quick summary
  & "$SqliteExe" "$CompactDb" "SELECT COUNT(*), printf('%.1f MB', SUM(size)/1048576.0) FROM file_index;" 2>&1 | Tee-Object -FilePath $LogPath -Append
  Write-Host "[DONE][$Label] rebuilt: $CompactDb"
}

# which targets?
$doRaw      = $true
$doRaceinfo = $true
if ($RawOnly)      { $doRaceinfo = $false }
if ($RaceinfoOnly) { $doRaw      = $false }

# prevent double-run
if (Test-Path $LockPath) { Write-Host "[SKIP] lock exists: $LockPath"; exit 0 }
New-Item -ItemType File -Force -Path $LockPath | Out-Null

try {
  if ($doRaw) {
    Invoke-RebuildVault -Label "raw" `
      -SrcDir $RawSrc `
      -BuildDb $RawBuildDb `
      -CompactDb $RawCompactDb `
      -Glob "*.csv" `
      -Regex "^(?P<ymd>\d{8})_raw\.csv$"
  }

  if ($doRaceinfo) {
    Invoke-RebuildVault -Label "raceinfo" `
      -SrcDir $RaceSrc `
      -BuildDb $RaceBuildDb `
      -CompactDb $RaceCompactDb `
      -Glob "*.csv" `
      -Regex "^raceinfo_(?P<ymd>\d{8})\.csv$"
  }

  Write-Host "[ALL DONE] vaults rebuilt successfully."
}
finally {
  Remove-Item $LockPath -ErrorAction SilentlyContinue
}
