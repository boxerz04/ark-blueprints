param(
    [string]$PiRoot = "\\192.168.10.125\arkdata",
    [string]$LocalRoot = "C:\Users\user\Desktop\Git\ark-blueprints\data_pi_sync",
    [switch]$Preview
)

$ErrorActionPreference = "Stop"

Write-Host "sync_from_pi.ps1 started"
Write-Host "PiRoot    = $PiRoot"
Write-Host "LocalRoot = $LocalRoot"
Write-Host "Preview   = $Preview"

$LogDir = Join-Path $LocalRoot "_sync_logs"
$RawDst = Join-Path $LocalRoot "raw"
$RaceinfoDst = Join-Path $LocalRoot "processed\raceinfo"
$MotorDst = Join-Path $LocalRoot "processed\motor"

New-Item -ItemType Directory -Force -Path $LocalRoot | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
New-Item -ItemType Directory -Force -Path $RawDst | Out-Null
New-Item -ItemType Directory -Force -Path $RaceinfoDst | Out-Null
New-Item -ItemType Directory -Force -Path $MotorDst | Out-Null

$TimeStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogDir "sync_from_pi_$TimeStamp.log"

$CommonArgs = @(
    "/Z",
    "/FFT",
    "/R:2",
    "/W:5",
    "/COPY:DAT",
    "/DCOPY:DAT",
    "/XJ",
    "/NP",
    "/TEE",
    "/LOG+:$LogFile"
)

if ($Preview) {
    $CommonArgs += "/L"
}

Write-Host ""
Write-Host "[1/3] raw sync preview/start"
robocopy `
    (Join-Path $PiRoot "raw") `
    $RawDst `
    "*_raw.csv" "*_refund.csv" `
    /E `
    @CommonArgs

if ($LASTEXITCODE -ge 8) {
    throw "raw robocopy failed: exit code $LASTEXITCODE"
}

Write-Host ""
Write-Host "[2/3] raceinfo sync preview/start"
robocopy `
    (Join-Path $PiRoot "processed\raceinfo") `
    $RaceinfoDst `
    "raceinfo_*.csv" `
    /E `
    @CommonArgs

if ($LASTEXITCODE -ge 8) {
    throw "raceinfo robocopy failed: exit code $LASTEXITCODE"
}

Write-Host ""
Write-Host "[3/3] motor sync preview/start"
robocopy `
    (Join-Path $PiRoot "processed\motor") `
    $MotorDst `
    "motor_id_map__all.csv" "motor_section_features_n__all.csv" `
    @CommonArgs

if ($LASTEXITCODE -ge 8) {
    throw "motor robocopy failed: exit code $LASTEXITCODE"
}

Write-Host ""
Write-Host "同期完了"
Write-Host "ログ: $LogFile"