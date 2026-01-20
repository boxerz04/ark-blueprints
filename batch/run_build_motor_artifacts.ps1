# run_build_motor_artifacts.ps1
# motor artifacts (map/snapshot) を更新し、ログをファイルに保存する
# - Task Scheduler 実行前提（ExecutionPolicy Bypass）
# - 失敗時は非0終了

$ErrorActionPreference = "Stop"

# ========= パス設定 =========
$PROJECT_ROOT = "C:\Users\user\Desktop\Git\ark-blueprints"
$PYTHON_EXE   = "python"

$SCRIPT_PATH  = "$PROJECT_ROOT\scripts\build_motor_artifacts_from_bins.py"

$BINS_DIR     = "$PROJECT_ROOT\data\html\rankingmotor"
$OUT_SNAPSHOT = "$PROJECT_ROOT\data\processed\motor\motor_section_snapshot__all.csv"
$OUT_MAP      = "$PROJECT_ROOT\data\processed\motor\motor_id_map__all.csv"

# ========= ログ設定 =========
$LOG_DIR = "$PROJECT_ROOT\data\logs\motor_artifacts"
New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null

$TS = Get-Date -Format "yyyyMMdd_HHmmss"
$LOG_PATH = Join-Path $LOG_DIR "run_build_motor_artifacts__$TS.log"

# Console + File 両方に出す（Start-Transcriptは出力を広く拾える）
Start-Transcript -Path $LOG_PATH -Append | Out-Null

function Write-Info([string]$msg) {
    $t = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[INFO] $t $msg"
}

function Write-Err([string]$msg) {
    $t = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[ERROR] $t $msg"
}

try {
    Set-Location $PROJECT_ROOT

    Write-Info "START run_build_motor_artifacts"
    Write-Info "PROJECT_ROOT: $PROJECT_ROOT"
    Write-Info "PYTHON_EXE  : $PYTHON_EXE"
    Write-Info "SCRIPT     : $SCRIPT_PATH"
    Write-Info "BINS_DIR   : $BINS_DIR"
    Write-Info "OUT_MAP    : $OUT_MAP"
    Write-Info "OUT_SNAPSHOT: $OUT_SNAPSHOT"
    Write-Info "LOG_PATH   : $LOG_PATH"

    if (-not (Test-Path $SCRIPT_PATH))   { throw "script not found: $SCRIPT_PATH" }
    if (-not (Test-Path $BINS_DIR))      { throw "bins_dir not found: $BINS_DIR" }
    if (-not (Test-Path (Split-Path $OUT_MAP -Parent)))      { New-Item -ItemType Directory -Force -Path (Split-Path $OUT_MAP -Parent) | Out-Null }
    if (-not (Test-Path (Split-Path $OUT_SNAPSHOT -Parent))) { New-Item -ItemType Directory -Force -Path (Split-Path $OUT_SNAPSHOT -Parent) | Out-Null }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    & $PYTHON_EXE $SCRIPT_PATH `
        --bins_dir $BINS_DIR `
        --out_snapshot_csv $OUT_SNAPSHOT `
        --out_map_csv $OUT_MAP

    $exit = $LASTEXITCODE
    $sw.Stop()

    Write-Info ("Elapsed: {0:N1} sec" -f $sw.Elapsed.TotalSeconds)
    Write-Info "ExitCode: $exit"

    if ($exit -ne 0) {
        throw "build_motor_artifacts_from_bins.py failed (ExitCode=$exit)"
    }

    # 成果物の存在確認（最小限）
    if (-not (Test-Path $OUT_MAP))      { throw "output missing: $OUT_MAP" }
    if (-not (Test-Path $OUT_SNAPSHOT)) { throw "output missing: $OUT_SNAPSHOT" }

    # ファイルサイズをログに残す（更新有無の目安）
    $mapSize = (Get-Item $OUT_MAP).Length
    $snapSize = (Get-Item $OUT_SNAPSHOT).Length
    Write-Info ("OUT_MAP size: {0:N0} bytes" -f $mapSize)
    Write-Info ("OUT_SNAPSHOT size: {0:N0} bytes" -f $snapSize)

    Write-Info "SUCCESS run_build_motor_artifacts"
    Stop-Transcript | Out-Null
    exit 0
}
catch {
    Write-Err $_.Exception.Message
    Write-Err "FAILED run_build_motor_artifacts"
    try { Stop-Transcript | Out-Null } catch {}
    exit 1
}
