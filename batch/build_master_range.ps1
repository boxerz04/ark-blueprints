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

  # Period (yyyymmdd). Defaults avoid overlap with priors (20231201-20241130).
  [int]$StartDate = 20241201,
  [int]$EndDate   = 20250930,

  # Paths (relative to RepoRoot)
  [string]$RawDir        = "data\raw",
  [string]$MasterOut     = "data\processed\master.csv",
  [string]$ReportsDir    = "data\processed\reports",
  [string]$PriorsRoot    = "data\priors",
  [string]$CourseReports = "data\processed\course_meta",

  # preprocess.py join toggles
  [switch]$NoJoinTenji,
  [switch]$NoJoinSeasonCourse,
  [switch]$NoJoinWinningTrick,

  # course features settings
  [int]$WarmupDays = 180,
  [int]$NLast      = 10,
  [switch]$SkipCourse
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function ToIsoDate([int]$yyyymmdd) {
  $s = "{0:d8}" -f $yyyymmdd
  if ($s.Length -ne 8) { throw "StartDate/EndDate must be yyyymmdd (8 digits). Got: '$s'" }
  return "$($s.Substring(0,4))-$($s.Substring(4,2))-$($s.Substring(6,2))"
}

if (-not $RepoRoot -or $RepoRoot.Trim() -eq "") {
  $RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
}
if ([int]$EndDate -lt [int]$StartDate) { throw "EndDate must be >= StartDate. Start=$StartDate End=$EndDate" }

$StartIso = ToIsoDate $StartDate
$EndIso   = ToIsoDate $EndDate

if (-not (Test-Path $PythonPath)) {
  Write-Host "WARN: PythonPath not found: $PythonPath -> fallback to 'python'"
  $PythonPath = "python"
}

# Resolve paths
$RawFull        = Join-Path $RepoRoot $RawDir
$MasterFull     = Join-Path $RepoRoot $MasterOut
$ReportsFull    = Join-Path $RepoRoot $ReportsDir
$PriorsFull     = Join-Path $RepoRoot $PriorsRoot
$CourseReportsF = Join-Path $RepoRoot $CourseReports

# Detect scripts
$preprocess_py = $null
foreach ($c in @("scripts\preprocess.py","preprocess.py")) {
  $p = Join-Path $RepoRoot $c
  if (Test-Path $p) { $preprocess_py = $p; break }
}
if (-not $preprocess_py) { throw "preprocess.py not found under repo." }

$course_py = Join-Path $RepoRoot "scripts\preprocess_course.py"
if (-not (Test-Path $course_py) -and -not $SkipCourse) { throw "scripts\preprocess_course.py not found." }

Write-Host "INFO RepoRoot   : $RepoRoot"
Write-Host "INFO Python     : $PythonPath"
Write-Host "INFO Period     : $StartIso .. $EndIso"
Write-Host "INFO MasterOut  : $MasterFull"
Write-Host "INFO PriorsRoot : $PriorsFull"
Write-Host "INFO RawDir     : $RawFull"
Write-Host "INFO Reports    : $ReportsFull"
Write-Host "INFO CourseRpt  : $CourseReportsF"
Write-Host "INFO CourseStep : " -NoNewline; if ($SkipCourse) { Write-Host "SKIPPED" } else { Write-Host "ENABLED (WarmupDays=$WarmupDays, NLast=$NLast)" }

# Step 1) Build master.csv (preprocess.py)
$argv1 = @(
  $preprocess_py,
  "--raw-dir", $RawFull,
  "--out", $MasterFull,
  "--reports-dir", $ReportsFull,
  "--priors-root", $PriorsFull,
  "--start-date", $StartIso,
  "--end-date", $EndIso
)
if ($NoJoinTenji)        { $argv1 += "--no-join-tenji" }
if ($NoJoinSeasonCourse) { $argv1 += "--no-join-season-course" }
if ($NoJoinWinningTrick) { $argv1 += "--no-join-winning-trick" }

Push-Location $RepoRoot
& $PythonPath @argv1
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess.py exited with code $LASTEXITCODE" }
Pop-Location
if (-not (Test-Path $MasterFull)) { throw "master.csv was not created at $MasterFull" }
Write-Host "OK  master.csv built."

# Step 2) Overwrite master.csv with course features (preprocess_course.py)
if (-not $SkipCourse) {
  $argv2 = @(
    $course_py,
    "--master", $MasterFull,
    "--raw-dir", $RawFull,
    "--out", $MasterFull,           # overwrite same master.csv
    "--reports-dir", $CourseReportsF,
    "--start-date", $StartIso,
    "--end-date", $EndIso,
    "--warmup-days", $WarmupDays,
    "--n-last", $NLast
  )
  Push-Location $RepoRoot
  & $PythonPath @argv2
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess_course.py exited with code $LASTEXITCODE" }
  Pop-Location
  Write-Host "OK  master.csv overwritten with course features."
}
