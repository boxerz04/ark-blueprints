# batch/build_master_range.ps1
# ---------------------------------------------
# master.csv を期間指定で再生成するランチャー
# - 既定期間: 2024-12-01 .. 2025-09-30（prior: 2023-12-01 .. 2024-11-30 と非重複）
# - 既定の Python: C:\anaconda3\python.exe
# - repo 直下に scripts\preprocess.py または preprocess.py がある前提
# - data/priors/*/latest.csv を使用（prior はユーザーが更新済みのもの）
# ---------------------------------------------

param(
  [string]$RepoRoot = "",
  [string]$PythonPath = "C:\anaconda3\python.exe",

  [int]$StartDate = 20241201,
  [int]$EndDate   = 20250930,

  [string]$RawDir     = "data\raw",
  [string]$Out        = "data\processed\master.csv",
  [string]$ReportsDir = "data\processed\reports",
  [string]$PriorsRoot = "data\priors",

  [switch]$NoJoinTenji,
  [switch]$NoJoinSeasonCourse,
  [switch]$NoJoinWinningTrick
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function ToIsoDate([int]$yyyymmdd) {
  $s = "{0:d8}" -f $yyyymmdd
  if ($s.Length -ne 8) { throw "StartDate/EndDate must be yyyymmdd (8 digits). Input: '$s'" }
  return "$($s.Substring(0,4))-$($s.Substring(4,2))-$($s.Substring(6,2))"
}

if (-not $RepoRoot -or $RepoRoot.Trim() -eq "") {
  $RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
}

if ([int]$EndDate -lt [int]$StartDate) {
  throw "EndDate must be >= StartDate. Start=$StartDate End=$EndDate"
}

$StartIso = ToIsoDate $StartDate
$EndIso   = ToIsoDate $EndDate

if (-not (Test-Path $PythonPath)) {
  Write-Host "WARN: PythonPath not found: $PythonPath  -> fallback to 'python'"
  $PythonPath = "python"
}

$candidates = @(
  (Join-Path $RepoRoot "scripts\preprocess.py"),
  (Join-Path $RepoRoot "preprocess.py")
)
$ScriptPath = $null
foreach ($c in $candidates) { if (Test-Path $c) { $ScriptPath = $c; break } }
if (-not $ScriptPath) {
  $list = ($candidates -join "`n  - ")
  throw "preprocess.py not found. Searched: `n  - $list`nRepoRoot=$RepoRoot"
}

$RawFull     = Join-Path $RepoRoot $RawDir
$OutFull     = Join-Path $RepoRoot $Out
$ReportsFull = Join-Path $RepoRoot $ReportsDir
$PriorsFull  = Join-Path $RepoRoot $PriorsRoot

Write-Host "INFO RepoRoot : $RepoRoot"
Write-Host "INFO Python   : $PythonPath"
Write-Host "INFO Script   : $ScriptPath"
Write-Host "INFO Period   : $StartIso .. $EndIso"
Write-Host "INFO RawDir   : $RawFull"
Write-Host "INFO Out      : $OutFull"
Write-Host "INFO Reports  : $ReportsFull"
Write-Host "INFO Priors   : $PriorsFull"

$argv = @(
  $ScriptPath,
  "--raw-dir", $RawFull,
  "--out", $OutFull,
  "--reports-dir", $ReportsFull,
  "--priors-root", $PriorsFull,
  "--start-date", $StartIso,
  "--end-date", $EndIso
)
if ($NoJoinTenji)        { $argv += "--no-join-tenji" }
if ($NoJoinSeasonCourse) { $argv += "--no-join-season-course" }
if ($NoJoinWinningTrick) { $argv += "--no-join-winning-trick" }

Push-Location $RepoRoot
try {
  & $PythonPath @argv
  if ($LASTEXITCODE -ne 0) { throw "preprocess.py exited with code $LASTEXITCODE" }
  Write-Host "OK: master.csv built -> $OutFull"
}
finally {
  Pop-Location
}
