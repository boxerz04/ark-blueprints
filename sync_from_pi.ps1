@'
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

    $args = @($Source, $Destination)
    $args += $FileFilter

    if ($Recursive) {
        $args += "/E"
    }

    $args += @(
        "/Z",
        "/FFT",
        "/R:2",
        "/W:5",
        "/COPY:DAT",
        "/DCOPY:DAT",
        "/XJ",
        "/NP",
        "/TEE",
        "/NJH",
        "/NJS",
        "/LOG+:$LogFile"
    )

    if ($Preview) {
        $args += "/L"
    }

    Write-Log ("robocopy start: " + $Source + " -> " + $Destination + " | filter=" + ($FileFilter -join ",") + " | recursive=" + $Recursive.IsPresent + " | preview=" + $Preview.IsPresent)

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

    $srcRaw = Join-Path $PiRoot "raw"
    $dstRaw = Join-Path $LocalRoot "raw"
    Invoke-RobocopyChecked -Source $srcRaw -Destination $dstRaw -FileFilter @("*_raw.csv", "*_refund.csv") -Recursive

    $srcRaceinfo = Join-Path $PiRoot "processed\raceinfo"
    $dstRaceinfo = Join-Path $LocalRoot "processed\raceinfo"
    Invoke-RobocopyChecked -Source $srcRaceinfo -Destination $dstRaceinfo -FileFilter @("raceinfo_*.csv") -Recursive

    $srcMotor = Join-Path $PiRoot "processed\motor"
    $dstMotor = Join-Path $LocalRoot "processed\motor"
    Invoke-RobocopyChecked -Source $srcMotor -Destination $dstMotor -FileFilter @("motor_id_map__all.csv", "motor_section_features_n__all.csv")

    Write-Log "=== Sync completed successfully ==="
    Write-Host ""
    Write-Host "同期完了"
    Write-Host "ログ: $LogFile"
}
catch {
    Write-Log ("ERROR: " + $_.Exception.Message)
    Write-Host ""
    Write-Host "同期失敗"
    Write-Host "ログ: $LogFile"
    throw
}
'@ | Set-Content -Path .\sync_from_pi.ps1 -Encoding UTF8
