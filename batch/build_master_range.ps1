# batch/build_master_range.ps1
# -----------------------------------------------------------------------------
# 学習用 master.csv を作成し、course/sectional の特徴列を「上書き付与」するスクリプト
# フロー: preprocess.py → preprocess_course.py(上書き) → preprocess_sectional.py(上書き)
# 使い方（例）:
#   powershell -NoProfile -ExecutionPolicy Bypass -File ".\batch\build_master_range.ps1" `
#     -StartDate 20241201 -EndDate 20250930 -WarmupDays 180 -NLast 10
# オプション:
#   -SkipCourse / -SkipSectional で各上書き付与をスキップ可能
# 備考:
#   コンソール出力は ASCII のみ（文字化け対策）。コメントは日本語。
# -----------------------------------------------------------------------------

param(
  # ルート/実行環境
  [string]$RepoRoot = "",
  [string]$PythonPath = "C:\anaconda3\python.exe",

  # 期間（yyyymmdd）
  [int]$StartDate = 20241201,
  [int]$EndDate   = 20250930,

  # パス（RepoRoot からの相対）
  [string]$RawDir        = "data\raw",
  [string]$MasterOut     = "data\processed\master.csv",
  [string]$ReportsDir    = "data\processed\reports",
  [string]$PriorsRoot    = "data\priors",
  [string]$CourseReports = "data\processed\course_meta",
  [string]$RaceinfoDir   = "data\processed\raceinfo",

  # preprocess.py の結合トグル（必要に応じて）
  [switch]$NoJoinTenji,
  [switch]$NoJoinSeasonCourse,
  [switch]$NoJoinWinningTrick,

  # course 特徴の設定
  [int]$WarmupDays = 180,
  [int]$NLast      = 10,
  [switch]$SkipCourse,

  # sectional 上書き付与の有無
  [switch]$SkipSectional
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function ToIsoDate([int]$yyyymmdd) {
  $s = "{0:d8}" -f $yyyymmdd
  if ($s.Length -ne 8) { throw "StartDate/EndDate must be yyyymmdd (8 digits). Got: '$s'" }
  return "$($s.Substring(0,4))-$($s.Substring(4,2))-$($s.Substring(6,2))"
}
function Ensure-ParentDir([string]$path) {
  $parent = Split-Path -Parent $path
  if ($parent -and -not (Test-Path $parent)) { New-Item -ItemType Directory -Path $parent | Out-Null }
}

# RepoRoot 未指定ならスクリプトの親をルートに
if (-not $RepoRoot -or $RepoRoot.Trim() -eq "") {
  $RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
}
if ([int]$EndDate -lt [int]$StartDate) { throw "EndDate must be >= StartDate. Start=$StartDate End=$EndDate" }

$StartIso = ToIsoDate $StartDate
$EndIso   = ToIsoDate $EndDate

# Python 実行確認
if (-not (Test-Path $PythonPath)) {
  Write-Host "WARN  PythonPath not found: $PythonPath -> fallback to 'python'"
  $PythonPath = "python"
}

# フルパス解決
$RawFull        = Join-Path $RepoRoot $RawDir
$MasterFull     = Join-Path $RepoRoot $MasterOut
$ReportsFull    = Join-Path $RepoRoot $ReportsDir
$PriorsFull     = Join-Path $RepoRoot $PriorsRoot
$CourseReportsF = Join-Path $RepoRoot $CourseReports
$RaceinfoFull   = Join-Path $RepoRoot $RaceinfoDir

Ensure-ParentDir $MasterFull
Ensure-ParentDir $ReportsFull
Ensure-ParentDir $CourseReportsF

# スクリプト検出
$preprocess_py = $null
foreach ($c in @("scripts\preprocess.py","preprocess.py")) {
  $p = Join-Path $RepoRoot $c
  if (Test-Path $p) { $preprocess_py = $p; break }
}
if (-not $preprocess_py) { throw "preprocess.py not found under repo." }

$course_py = $null
foreach ($c in @("scripts\preprocess_course.py","preprocess_course.py")) {
  $p = Join-Path $RepoRoot $c
  if (Test-Path $p) { $course_py = $p; break }
}
if (-not $SkipCourse -and -not $course_py) { throw "preprocess_course.py not found." }

$sectional_py = $null
foreach ($c in @("scripts\preprocess_sectional.py","preprocess_sectional.py")) {
  $p = Join-Path $RepoRoot $c
  if (Test-Path $p) { $sectional_py = $p; break }
}
if (-not $SkipSectional -and -not $sectional_py) { throw "preprocess_sectional.py not found." }

Write-Host  "INFO  RepoRoot     : $RepoRoot"
Write-Host  "INFO  Python       : $PythonPath"
Write-Host  "INFO  Period       : $StartIso .. $EndIso"
Write-Host  "INFO  MasterOut    : $MasterFull"
Write-Host  "INFO  PriorsRoot   : $PriorsFull"
Write-Host  "INFO  RawDir       : $RawFull"
Write-Host  "INFO  Reports      : $ReportsFull"
Write-Host  "INFO  CourseReport : $CourseReportsF"
Write-Host  "INFO  RaceinfoDir  : $RaceinfoFull"
Write-Host  "INFO  CourseStep   : " -NoNewline; if ($SkipCourse) { Write-Host "SKIPPED" } else { Write-Host "ENABLED (WarmupDays=$WarmupDays, NLast=$NLast)" }
Write-Host  "INFO  SectionalStep: " -NoNewline; if ($SkipSectional) { Write-Host "SKIPPED" } else { Write-Host "ENABLED" }

# -----------------------------------------------------------------------------
# Step 1) master.csv 生成（preprocess.py）
#  - priors-root を渡して prior を結合
#  - start/end は inclusive
# -----------------------------------------------------------------------------
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
Write-Host "OK    master.csv built."

# -----------------------------------------------------------------------------
# Step 2) course 特徴を master.csv に上書き付与（preprocess_course.py）
# -----------------------------------------------------------------------------
if (-not $SkipCourse) {
  $argv2 = @(
    $course_py,
    "--master", $MasterFull,
    "--raw-dir", $RawFull,
    "--out", $MasterFull,              # 同じパスに上書き
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
  Write-Host "OK    master.csv overwritten with course features."
}

# -----------------------------------------------------------------------------
# Step 2.5) sectional 列を master.csv に上書き付与（preprocess_sectional.py）
#  - raceinfo 由来の節間列を many-to-one で安全に JOIN
#  - 結果列（*_flag_cur 等）はスクリプト側で除外する前提
# -----------------------------------------------------------------------------
if (-not $SkipSectional) {
  $argv25 = @(
    $sectional_py,
    "--master", $MasterFull,
    "--raceinfo-dir", $RaceinfoFull,
    "--start-date", $StartIso,
    "--end-date",   $EndIso,
    "--out", $MasterFull
    # ※ preprocess_sectional.py は --reports-dir 非対応
  )
  Push-Location $RepoRoot
  & $PythonPath @argv25
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess_sectional.py exited with code $LASTEXITCODE" }
  Pop-Location
  Write-Host "OK    master.csv overwritten with sectional features."
}

# -----------------------------------------------------------------------------
# Step 3) master.csv の列一覧を YAML にエクスポート（features/base.yaml）
# -----------------------------------------------------------------------------
$FeaturesDir = Join-Path $RepoRoot "features"
if (-not (Test-Path $FeaturesDir)) {
  New-Item -ItemType Directory -Force -Path $FeaturesDir | Out-Null
}
$BaseYaml = Join-Path $FeaturesDir "base.yaml"
$ExportScript = Join-Path $RepoRoot "scripts\export_base_feature_yaml.py"

if (Test-Path $ExportScript) {
  Write-Host "INFO  Export base feature columns -> $BaseYaml"
  $argvFeat = @(
    $ExportScript,
    "--master", $MasterFull,
    "--out", $BaseYaml
  )
  Push-Location $RepoRoot
  & $PythonPath @argvFeat
  if ($LASTEXITCODE -ne 0) {
    Pop-Location
    throw "export_base_feature_yaml.py exited with code $LASTEXITCODE"
  }
  Pop-Location
} else {
  Write-Host "WARN  export_base_feature_yaml.py not found; skip base.yaml export."
}

Write-Host "DONE  All steps completed. Output: $MasterFull"
