param(
    [string]$PiRoot = "\\192.168.10.125\arkdata",
    [string]$LocalRoot = "C:\Users\user\Desktop\Git\ark-blueprints\data_pi_sync",
    [switch]$Preview
)

$ErrorActionPreference = "Stop"

$LogDir = Join-Path $LocalRoot "_sync_logs"
New-Item -ItemType Directory -Force -Path $LocalRoot | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$TimeStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogDir "sync_from_pi_$TimeStamp.log"

function Write-Log {
    param([string]$Message)
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    $line | Tee-Object -FilePath $LogFile -Append
}

function Invoke-RobocopyChecked {
    param(
        [string]$Source,
        [string]$Destination,
        [string[]]$FileFilter = @("*"),
        [switch]$Recursive
    )

    if (-not (Test-Path $Source)) {
        throw "Source not found: $Source"
    }

    New-Item -ItemType Directory -Force -Path $Destination | Out-Null

    $args = @(
        $Source
        $Destination
    )

    $args += $FileFilter

    if ($Recursive) {
        $args += "/E"
    }

    $args += @(
        "/Z"        # 再開可能コピー
        "/FFT"      # Samba/Unix との時刻差にやや寛容
        "/R:2"      # リトライ回数
        "/W:5"      # 待機秒
        "/COPY:DAT" # Data/Attributes/Timestamps
        "/DCOPY:DAT"
        "/XJ"       # Junction除外
        "/NP"       # 進捗率を省略
        "/TEE"
        "/NJH"
        "/NJS"
        "/LOG+:$LogFile"
    )

    if ($Preview) {
        $args += "/L"
    }

    Write-Log "robocopy start: $Source -> $Destination | filter=$($FileFilter -join ',') | recursive=$Recursive | preview=$Preview"

    & robocopy @args
    $exitCode = $LASTEXITCODE

    Write-Log "robocopy exit code: $exitCode"

    if ($exitCode -ge 8) {
        throw "robocopy failed with exit code $exitCode"
    }
}

try {
    Write-Log "=== Sync start ==="
    Write-Log "PiRoot   = $PiRoot"
    Write-Log "LocalRoot= $LocalRoot"
    Write-Log "Preview  = $Preview"

    # 1) raw 全体
    Invoke-RobocopyChecked `
        -Source (Join-Path $PiRoot "raw") `
        -Destination (Join-Path $LocalRoot "raw") `
        -FileFilter @("*_raw.csv", "*_refund.csv") `
        -Recursive

    # 2) raceinfo 全体
    Invoke-RobocopyChecked `
        -Source (Join-Path $PiRoot "processed\raceinfo") `
        -Destination (Join-Path $LocalRoot "processed\raceinfo") `
        -FileFilter @("raceinfo_*.csv") `
        -Recursive

    # 3) motor 必須2ファイルのみ
    Invoke-RobocopyChecked `
        -Source (Join-Path $PiRoot "processed\motor") `
        -Destination (Join-Path $LocalRoot "processed\motor") `
        -FileFilter @("motor_id_map__all.csv", "motor_section_features_n__all.csv")

    Write-Log "=== Sync completed successfully ==="
    Write-Host ""
    Write-Host "同期完了"
    Write-Host "ログ: $LogFile"
}
catch {
    Write-Log "ERROR: $($_.Exception.Message)"
    Write-Host ""
    Write-Host "同期失敗"
    Write-Host "ログ: $LogFile"
    throw
}
