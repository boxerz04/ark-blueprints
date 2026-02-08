# batch/build_master_range.ps1
# -----------------------------------------------------------------------------
# 学習用 master.csv を「期間指定で作成」し、必要な特徴列を master.csv に順次「上書き付与」する。
#
# 目的（固定の正）:
#   - data/processed/master.csv を “学習の共通入力（完成形）” として確定させる
#   - 同一PS1内で make_master_finals.py を実行し、master_finals.csv も併せて生成する
#
# フロー（上書き順）:
#   Step 1  preprocess.py
#           : master.csv を作成（期間指定）
#   Step 2  preprocess_course.py
#           : course 履歴特徴量を上書き付与（期間指定 + warmup）
#   Step 3  preprocess_sectional.py
#           : sectional(必須10列) を上書き付与（期間指定）
#   Step 4  preprocess_motor_id.py
#           : motor_id を上書き付与（motor_id_map__all.csv参照）
#   Step 5  preprocess_motor_section.py
#           : motor_section(prev/delta) を上書き付与（安全結合・リーク防止）
#   Step 6  make_master_finals.py
#           : finals/semi/semi-entry を抽出して master_finals.csv を作成（既定で実行）
#
# 使い方（例 / Anaconda Prompt = cmd から）:
#   cd C:\Users\user\Desktop\Git\ark-blueprints
#   powershell -NoProfile -ExecutionPolicy Bypass -File batch\build_master_range.ps1 -StartDate 20241001 -EndDate 20251231 -WarmupDays 180 -NLast 10
#
# オプション:
#   -SkipCourse / -SkipSectional / -SkipMotorId / -SkipMotorSection で各工程をスキップ
#   -SkipFinals で finals 抽出（master_finals.csv生成）だけをスキップ（master.csv は常に生成）
#
# バックアップ（運用品質 / 既定OFF）:
#   -BackupBeforeMotorSection を付けた時だけ、Step 5 の直前に master をバックアップする
#   -バックアップ形式は -BackupFormat で選ぶ（既定: pklgz）
#       pklgz : master.bak.pkl.gz（圧縮pickle, 小さい, 復旧用途向き）
#       csv   : master.bak.csv     （従来互換, 大きい）
#
# 実装メモ:
#   - PowerShell 5.1/7 両対応のため RepoRoot 自動推定は $PSScriptRoot を使用
#   - mkdir -p 相当は New-Item -Force で実現
#   - motor_section 付与は tmp 出力 → 成功後に master.csv を置換（破損防止）
# -----------------------------------------------------------------------------

param(
  # ルート/実行環境
  [string]$RepoRoot   = "",
  [string]$PythonPath = "C:\anaconda3\python.exe",

  # 期間（yyyymmdd）
  [int]$StartDate = 20241201,
  [int]$EndDate   = 20250930,

  # パス（RepoRoot からの相対）
  [string]$RawDir          = "data\raw",
  [string]$MasterOut       = "data\processed\master.csv",
  [string]$MasterFinalsOut = "data\processed\master_finals.csv",
  [string]$ReportsDir      = "data\processed\reports",
  [string]$PriorsRoot      = "data\priors",

  # preprocess.py の結合トグル（必要に応じて）
  [switch]$NoJoinTenji,
  [switch]$NoJoinSeasonCourse,
  [switch]$NoJoinWinningTrick,

  # Step 2) course
  [int]$WarmupDays = 180,
  [int]$NLast      = 10,
  [string]$CourseReportsDir = "data\processed\course_meta",
  [switch]$SkipCourse,

  # Step 3) sectional
  [string]$RaceinfoDir = "data\processed\raceinfo",
  [switch]$SkipSectional,

  # Step 4) motor_id
  [switch]$SkipMotorId,
  [string]$MotorIdMapCsv = "data\processed\motor\motor_id_map__all.csv",
  [double]$MotorIdMaxMissRate = 0.0,

  # Step 5) motor_section(prev/delta)
  [switch]$SkipMotorSection,
  [string]$MotorSectionCsv     = "data\processed\motor\motor_section_features_n__all.csv",
  [string]$MotorSectionPrefix  = "motor_",
  [switch]$MotorSectionStrictKeyMatch,
  [string]$MotorSectionQcReportCsv = "data\processed\reports\qc_motor_section_join.csv",

  # Backup (Step 5 直前) - 既定OFF
  [switch]$BackupBeforeMotorSection,
  [ValidateSet("pklgz", "csv")]
  [string]$BackupFormat = "pklgz",

  # Step 6) finals - 既定で実行（スキップは明示的に）
  [switch]$SkipFinals,
  # make_master_finals.py の --stage-filter を使いたい場合のみ指定（空ならスクリプト側デフォルト）
  [string]$FinalsStageFilter = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function ToIsoDate([int]$yyyymmdd) {
  $s = "{0:d8}" -f $yyyymmdd
  if ($s.Length -ne 8) { throw "Invalid yyyymmdd: $yyyymmdd" }
  return "$($s.Substring(0,4))-$($s.Substring(4,2))-$($s.Substring(6,2))"
}

function Resolve-RepoRoot([string]$root) {
  if ($root -and (Test-Path $root)) {
    return (Resolve-Path $root).Path
  }
  if ($PSScriptRoot) {
    $candidate = Split-Path -Parent $PSScriptRoot
    return (Resolve-Path $candidate).Path
  }
  throw "RepoRoot cannot be resolved. Please specify -RepoRoot explicitly."
}

function Ensure-Path([string]$p, [string]$label) {
  if (-not (Test-Path $p)) { throw "$label not found: $p" }
}

function Ensure-Dir([string]$p) {
  if (-not (Test-Path $p)) {
    New-Item -ItemType Directory -Path $p -Force | Out-Null
  }
}

function Print-Header([string]$title) {
  Write-Host ""
  Write-Host "===================================================================="
  Write-Host $title
  Write-Host "===================================================================="
}

function Backup-Master([string]$masterCsvFull, [string]$bakFull, [string]$format, [string]$pyExe) {
  if ($format -eq "csv") {
    Copy-Item -Force $masterCsvFull $bakFull
    Write-Host "OK    backup (csv) : $bakFull"
    return
  }

  if ($format -eq "pklgz") {
    $py = @"
import pandas as pd
src = r"""$masterCsvFull"""
dst = r"""$bakFull"""
df = pd.read_csv(src, low_memory=False)
df.to_pickle(dst, compression="gzip")
print("[OK] wrote backup:", dst, "rows=", len(df), "cols=", df.shape[1])
"@
    & $pyExe -c $py
    if ($LASTEXITCODE -ne 0) { throw "backup (pklgz) failed with code $LASTEXITCODE" }
    Write-Host "OK    backup (pklgz): $bakFull"
    return
  }

  throw "Unknown BackupFormat: $format"
}

# -----------------------------------------------------------------------------
# Resolve paths
# -----------------------------------------------------------------------------
$RepoRoot = Resolve-RepoRoot $RepoRoot

$RawFull            = Join-Path $RepoRoot $RawDir
$MasterFull         = Join-Path $RepoRoot $MasterOut
$MasterFinalsFull   = Join-Path $RepoRoot $MasterFinalsOut
$ReportsFull        = Join-Path $RepoRoot $ReportsDir
$PriorsFull         = Join-Path $RepoRoot $PriorsRoot

$CourseReportsFull  = Join-Path $RepoRoot $CourseReportsDir
$RaceinfoFull       = Join-Path $RepoRoot $RaceinfoDir

$MotorMapFull       = Join-Path $RepoRoot $MotorIdMapCsv
$MotorSectionFull   = Join-Path $RepoRoot $MotorSectionCsv

$MotorSectionQcFull = ""
if ($MotorSectionQcReportCsv) { $MotorSectionQcFull = Join-Path $RepoRoot $MotorSectionQcReportCsv }

$startIso = ToIsoDate $StartDate
$endIso   = ToIsoDate $EndDate

# scripts（RepoRoot 相対）
$preprocess_py               = "scripts\preprocess.py"
$preprocess_course_py        = "scripts\preprocess_course.py"
$preprocess_sectional_py     = "scripts\preprocess_sectional.py"
$preprocess_motor_id_py      = "scripts\preprocess_motor_id.py"
$preprocess_motor_section_py = "scripts\preprocess_motor_section.py"
$make_master_finals_py       = "scripts\make_master_finals.py"

# -----------------------------------------------------------------------------
# Pre-flight
# -----------------------------------------------------------------------------
Print-Header "BUILD MASTER RANGE (start=$StartDate end=$EndDate)  RepoRoot=$RepoRoot"

Ensure-Path $PythonPath "PythonPath"
Ensure-Dir  (Split-Path -Parent $MasterFull)
Ensure-Dir  $ReportsFull

Write-Host "INFO  python     : $PythonPath"
Write-Host "INFO  raw_dir    : $RawFull"
Write-Host "INFO  master_out : $MasterFull"
Write-Host "INFO  priors_root: $PriorsFull"
Write-Host "INFO  start/end  : $startIso .. $endIso"
Write-Host "INFO  warmup/n   : $WarmupDays / $NLast"
Write-Host "INFO  skip flags : course=$SkipCourse sectional=$SkipSectional motor_id=$SkipMotorId motor_section=$SkipMotorSection finals=$SkipFinals"
Write-Host "INFO  backup     : enabled=$BackupBeforeMotorSection format=$BackupFormat"

Ensure-Path $RawFull "RawDir"
Ensure-Path (Join-Path $RepoRoot $preprocess_py) "scripts/preprocess.py"

# =============================================================================
# Step 1) preprocess.py -> master.csv
# =============================================================================
Print-Header "Step 1) preprocess.py -> master.csv"

$argv1 = @(
  $preprocess_py,
  "--raw-dir", $RawDir,
  "--out", $MasterOut,
  "--reports-dir", $ReportsDir,
  "--priors-root", $PriorsRoot,
  "--start-date", $startIso,
  "--end-date", $endIso
)

if ($NoJoinTenji)        { $argv1 += "--no-join-tenji" }
if ($NoJoinSeasonCourse) { $argv1 += "--no-join-season-course" }
if ($NoJoinWinningTrick) { $argv1 += "--no-join-winning-trick" }

Push-Location $RepoRoot
& $PythonPath @argv1
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess.py exited with code $LASTEXITCODE" }
Pop-Location

Ensure-Path $MasterFull "master.csv"
Write-Host "OK    wrote master.csv: $MasterFull"

# =============================================================================
# Step 2) preprocess_course.py (overwrite master.csv)
# =============================================================================
if (-not $SkipCourse) {
  Print-Header "Step 2) preprocess_course.py (overwrite master.csv)"

  Ensure-Path (Join-Path $RepoRoot $preprocess_course_py) "scripts/preprocess_course.py"
  Ensure-Dir  $CourseReportsFull

  $argv2 = @(
    $preprocess_course_py,
    "--master", $MasterOut,
    "--raw-dir", $RawDir,
    "--out", $MasterOut,
    "--reports-dir", $CourseReportsDir,
    "--start-date", $startIso,
    "--end-date", $endIso,
    "--warmup-days", $WarmupDays,
    "--n-last", $NLast
  )

  Push-Location $RepoRoot
  & $PythonPath @argv2
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess_course.py exited with code $LASTEXITCODE" }
  Pop-Location

  Write-Host "OK    master.csv overwritten with course features."
}

# =============================================================================
# Step 3) preprocess_sectional.py (overwrite master.csv)
# =============================================================================
if (-not $SkipSectional) {
  Print-Header "Step 3) preprocess_sectional.py (overwrite master.csv)"

  Ensure-Path (Join-Path $RepoRoot $preprocess_sectional_py) "scripts/preprocess_sectional.py"
  Ensure-Path $RaceinfoFull "RaceinfoDir"

  $argv3 = @(
    $preprocess_sectional_py,
    "--master", $MasterOut,
    "--raceinfo-dir", $RaceinfoDir,
    "--start-date", $startIso,
    "--end-date", $endIso,
    "--out", $MasterOut
  )

  Push-Location $RepoRoot
  & $PythonPath @argv3
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess_sectional.py exited with code $LASTEXITCODE" }
  Pop-Location

  Write-Host "OK    master.csv overwritten with sectional features."
}

# =============================================================================
# Step 4) preprocess_motor_id.py (overwrite master.csv)
# =============================================================================
if (-not $SkipMotorId) {
  Print-Header "Step 4) preprocess_motor_id.py (overwrite master.csv)"

  Ensure-Path (Join-Path $RepoRoot $preprocess_motor_id_py) "scripts/preprocess_motor_id.py"
  Ensure-Path $MotorMapFull "motor_id_map__all.csv"

  $argv4 = @(
    $preprocess_motor_id_py,
    "--in_csv", $MasterOut,
    "--map_csv", $MotorIdMapCsv,
    "--max_miss_rate", $MotorIdMaxMissRate
  )

  Push-Location $RepoRoot
  & $PythonPath @argv4
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess_motor_id.py exited with code $LASTEXITCODE" }
  Pop-Location

  Write-Host "OK    master.csv overwritten with motor_id."
}

# =============================================================================
# Step 5) preprocess_motor_section.py (safe overwrite master.csv via tmp)
# =============================================================================
if (-not $SkipMotorSection) {
  Print-Header "Step 5) preprocess_motor_section.py (safe overwrite master.csv via tmp)"

  Ensure-Path (Join-Path $RepoRoot $preprocess_motor_section_py) "scripts/preprocess_motor_section.py"
  Ensure-Path $MotorSectionFull "motor_section_features_n__all.csv"
  Ensure-Path $MasterFull "master.csv"

  $masterDir = Split-Path -Parent $MasterFull
  $masterTmp = Join-Path $masterDir "master.tmp.csv"

  if ($BackupBeforeMotorSection) {
    $bakName = if ($BackupFormat -eq "csv") { "master.bak.csv" } else { "master.bak.pkl.gz" }
    $bakFull = Join-Path $masterDir $bakName
    Backup-Master -masterCsvFull $MasterFull -bakFull $bakFull -format $BackupFormat -pyExe $PythonPath
  }

  $argv5 = @(
    $preprocess_motor_section_py,
    "--master_csv", $MasterFull,
    "--motor_section_csv", $MotorSectionCsv,
    "--out_master_csv", $masterTmp,
    "--prefix", $MotorSectionPrefix
  )

  if ($MotorSectionStrictKeyMatch) { $argv5 += "--strict_key_match" }
  if ($MotorSectionQcFull) {
    Ensure-Dir (Split-Path -Parent $MotorSectionQcFull)
    $argv5 += @("--qc_report_csv", $MotorSectionQcFull)
  }

  Push-Location $RepoRoot
  & $PythonPath @argv5
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess_motor_section.py exited with code $LASTEXITCODE" }
  Pop-Location

  Ensure-Path $masterTmp "master.tmp.csv"
  Move-Item -Force $masterTmp $MasterFull

  Write-Host "OK    master.csv overwritten with motor_section(prev/delta)."
  if ($MotorSectionQcFull) { Write-Host "OK    motor_section QC report: $MotorSectionQcFull" }
}

# =============================================================================
# Step 6) make_master_finals.py -> master_finals.csv  (default ON)
# =============================================================================
if (-not $SkipFinals) {
  Print-Header "Step 6) make_master_finals.py -> master_finals.csv"

  Ensure-Path (Join-Path $RepoRoot $make_master_finals_py) "scripts/make_master_finals.py"
  Ensure-Path $MasterFull "master.csv"
  Ensure-Dir  (Split-Path -Parent $MasterFinalsFull)

  $argv6 = @(
    $make_master_finals_py,
    "--master-in", $MasterOut,
    "--master-out", $MasterFinalsOut
  )

  if ($FinalsStageFilter -and $FinalsStageFilter.Trim().Length -gt 0) {
    $argv6 += @("--stage-filter", $FinalsStageFilter)
  }

  Push-Location $RepoRoot
  & $PythonPath @argv6
  if ($LASTEXITCODE -ne 0) { Pop-Location; throw "make_master_finals.py exited with code $LASTEXITCODE" }
  Pop-Location

  Ensure-Path $MasterFinalsFull "master_finals.csv"
  Write-Host "OK    wrote master_finals.csv: $MasterFinalsFull"
} else {
  Print-Header "Step 6) make_master_finals.py (SKIP)"
  Write-Host "SKIP  finals extraction (SkipFinals=ON)"
}

Print-Header "DONE"
Write-Host "OK    master ready       : $MasterFull"
if (-not $SkipFinals) {
  Write-Host "OK    master_finals ready: $MasterFinalsFull"
} else {
  Write-Host "INFO  master_finals not generated (SkipFinals=ON)"
}
