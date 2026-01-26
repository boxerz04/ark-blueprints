# run_build_motor_pipeline.ps1
# motor pipeline を順に更新し、ログをファイルに保存する
# - Task Scheduler 実行前提（ExecutionPolicy Bypass）
# - 失敗時は非0終了
#
# 実行順:
#   1) build_motor_artifacts_from_bins.py
#   2) build_raw_with_motor_joined.py
#   3) build_motor_section_base.py
#   4) build_motor_section_features_n.py

$ErrorActionPreference = "Stop"

# ========= パス設定 =========
$PROJECT_ROOT = "C:\Users\user\Desktop\Git\ark-blueprints"
$PYTHON_EXE   = "C:\anaconda3\python.exe"

# ---- scripts
$SCRIPT_ARTIFACTS = "$PROJECT_ROOT\scripts\build_motor_artifacts_from_bins.py"
$SCRIPT_RAW_JOIN  = "$PROJECT_ROOT\scripts\build_raw_with_motor_joined.py"
$SCRIPT_BASE      = "$PROJECT_ROOT\scripts\build_motor_section_base.py"
$SCRIPT_FEATURES  = "$PROJECT_ROOT\scripts\build_motor_section_features_n.py"

# ---- inputs
$BINS_DIR     = "$PROJECT_ROOT\data\html\rankingmotor"
$RAW_DIR      = "$PROJECT_ROOT\data\raw"

# ---- outputs (artifacts)
$OUT_DIR_MOTOR = "$PROJECT_ROOT\data\processed\motor"

$OUT_SNAPSHOT  = "$OUT_DIR_MOTOR\motor_section_snapshot__all.csv"
$OUT_MAP       = "$OUT_DIR_MOTOR\motor_id_map__all.csv"

$OUT_RAW_WITH_MOTOR = "$OUT_DIR_MOTOR\raw_with_motor__all.csv"
$OUT_BASE           = "$OUT_DIR_MOTOR\motor_section_base__all.csv"
$OUT_FEATURES       = "$OUT_DIR_MOTOR\motor_section_features_n__all.csv"

# ========= ログ設定 =========
$LOG_DIR = "$PROJECT_ROOT\data\logs\motor_pipeline"
New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null

$TS = Get-Date -Format "yyyyMMdd_HHmmss"
$LOG_PATH = Join-Path $LOG_DIR "run_build_motor_pipeline__$TS.log"

Start-Transcript -Path $LOG_PATH -Append | Out-Null

function Write-Info([string]$msg) {
    $t = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[INFO] $t $msg"
}

function Write-Err([string]$msg) {
    $t = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[ERROR] $t $msg"
}

function Ensure-Dir([string]$path) {
    if (-not (Test-Path $path)) {
        New-Item -ItemType Directory -Force -Path $path | Out-Null
    }
}

function Assert-Exists([string]$path, [string]$label) {
    if (-not (Test-Path $path)) {
        throw "$label not found: $path"
    }
}

function Run-Step([string]$name, [scriptblock]$cmd, [string[]]$mustExistPaths) {
    Write-Info "----- STEP START: $name -----"
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    & $cmd

    $exit = $LASTEXITCODE
    $sw.Stop()

    Write-Info ("Elapsed: {0:N1} sec" -f $sw.Elapsed.TotalSeconds)
    Write-Info "ExitCode: $exit"

    if ($exit -ne 0) {
        throw "$name failed (ExitCode=$exit)"
    }

    foreach ($p in $mustExistPaths) {
        if (-not (Test-Path $p)) { throw "$name output missing: $p" }
        $size = (Get-Item $p).Length
        Write-Info ("output size: {0:N0} bytes | {1}" -f $size, $p)
    }

    Write-Info "----- STEP SUCCESS: $name -----"
}

try {
    Set-Location $PROJECT_ROOT

    Write-Info "START run_build_motor_pipeline"
    Write-Info "PROJECT_ROOT: $PROJECT_ROOT"
    Write-Info "PYTHON_EXE  : $PYTHON_EXE"
    Write-Info "LOG_PATH   : $LOG_PATH"

    # ---- pre-checks
    Assert-Exists $PYTHON_EXE "python"
    Assert-Exists $BINS_DIR "bins_dir"
    Assert-Exists $RAW_DIR "raw_dir"

    Assert-Exists $SCRIPT_ARTIFACTS "script"
    Assert-Exists $SCRIPT_RAW_JOIN  "script"
    Assert-Exists $SCRIPT_BASE      "script"
    Assert-Exists $SCRIPT_FEATURES  "script"

    Ensure-Dir $OUT_DIR_MOTOR

    # =========================================================
    # STEP1: artifacts (map/snapshot)
    # =========================================================
    Run-Step "build_motor_artifacts_from_bins" {
        & $PYTHON_EXE $SCRIPT_ARTIFACTS `
            --bins_dir $BINS_DIR `
            --out_snapshot_csv $OUT_SNAPSHOT `
            --out_map_csv $OUT_MAP
    } @($OUT_MAP, $OUT_SNAPSHOT)

    # =========================================================
    # STEP2: raw_with_motor
    # =========================================================
    Run-Step "build_raw_with_motor_joined" {
        & $PYTHON_EXE $SCRIPT_RAW_JOIN `
            --raw_dir $RAW_DIR `
            --snapshot_csv $OUT_SNAPSHOT `
            --map_csv $OUT_MAP `
            --out_dir $OUT_DIR_MOTOR `
            --write_full_csv 1
    } @($OUT_RAW_WITH_MOTOR)

    # =========================================================
    # STEP3: motor_section_base
    # =========================================================
    Run-Step "build_motor_section_base" {
        & $PYTHON_EXE $SCRIPT_BASE `
            --input $OUT_RAW_WITH_MOTOR `
            --out_csv $OUT_BASE
    } @($OUT_BASE)

    # =========================================================
    # STEP4: motor_section_features_n
    # =========================================================
    Run-Step "build_motor_section_features_n" {
        & $PYTHON_EXE $SCRIPT_FEATURES `
            --input $OUT_BASE `
            --out_csv $OUT_FEATURES
    } @($OUT_FEATURES)

    Write-Info "SUCCESS run_build_motor_pipeline"
    Stop-Transcript | Out-Null
    exit 0
}
catch {
    Write-Err $_.Exception.Message
    Write-Err "FAILED run_build_motor_pipeline"
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}
