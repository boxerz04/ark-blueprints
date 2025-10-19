# batch\update_priors.ps1
param(
  [Parameter(Mandatory=$true)][string]$From,
  [Parameter(Mandatory=$true)][string]$To,
  [Parameter(Mandatory=$false)][string]$PythonExe,
  [Parameter(Mandatory=$false)][string]$RawDir = "data\raw"
)

$ErrorActionPreference = 'Stop'

function Resolve-Cmd([string]$hint, [string[]]$candidates) {
  if ($hint) {
    if ($hint -like "*\*") { if (Test-Path $hint) { return $hint } }
    $cmd = Get-Command $hint -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    throw "Not found: $hint"
  }
  foreach ($c in $candidates) {
    if ($c -like "*\*") { if (Test-Path $c) { return $c } }
    else { $cmd = Get-Command $c -ErrorAction SilentlyContinue; if ($cmd) { return $cmd.Source } }
  }
  throw "Python not found. Please pass -PythonExe."
}

# ← 引数名を 'Arguments' に変更（$args と衝突しない）
function Run-Step {
  param([string]$label,[string]$exe,[string[]]$Arguments)
  Write-Host "[RUN] $label"

  # null/空要素を除去（保険）
  $Arguments = @($Arguments | Where-Object { $_ -ne $null -and $_ -ne "" })

  $tmpOut = [System.IO.Path]::GetTempFileName()
  $tmpErr = [System.IO.Path]::GetTempFileName()
  try {
    $p = Start-Process -FilePath $exe -ArgumentList $Arguments `
         -NoNewWindow -Wait -PassThru `
         -RedirectStandardOutput $tmpOut -RedirectStandardError $tmpErr
    Get-Content $tmpOut, $tmpErr | Tee-Object -FilePath $LogPath -Append | Out-Host
    if ($p.ExitCode -ne 0) { throw "$label failed: exit $($p.ExitCode)" }
  } finally {
    Remove-Item $tmpOut,$tmpErr -ErrorAction SilentlyContinue
  }
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "logs") | Out-Null
$LogPath = Join-Path $RepoRoot ("logs\prior_update_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))

if ($From -notmatch '^\d{8}$' -or $To -notmatch '^\d{8}$') { throw "From/To must be YYYYMMDD (8 digits)." }
if (-not (Test-Path $RawDir)) { throw "RawDir not found: $RawDir" }

$PythonExe = Resolve-Cmd $PythonExe @("C:\anaconda3\python.exe","$env:CONDA_PREFIX\python.exe","python")
Write-Host "[INFO] Python: $PythonExe"
"[INFO] repo : $RepoRoot`n[INFO] from : $From`n[INFO] to   : $To`n[INFO] raw  : $RawDir" |
  Tee-Object -FilePath $LogPath -Append | Out-Host

$TenjiOut = Join-Path $RepoRoot "data\priors\tenji\tenji_prior__${From}_${To}__keys-place-wakuban-seasonq__sdfloor-0.02__m200__v1.csv"
$SCOut    = Join-Path $RepoRoot "data\priors\season_course\season_course_prior__${From}_${To}__keys-place-entry-seasonq__m0__v3.csv"
$WTOut    = Join-Path $RepoRoot "data\priors\winning_trick\winning_trick_prior__${From}_${To}__keys-place-entry-seasonq__m0__v3.csv"

$dirs = @((Split-Path $TenjiOut -Parent),(Split-Path $SCOut -Parent),(Split-Path $WTOut -Parent))
$dirs | ForEach-Object { New-Item -ItemType Directory -Force -Path $_ | Out-Null }

$TenjiPy = Join-Path $RepoRoot "scripts\build_tenji_prior_from_raw.py"
$SCPy    = Join-Path $RepoRoot "scripts\build_season_course_prior_from_raw.py"
$WTPy    = Join-Path $RepoRoot "scripts\build_season_winningtrick_prior_from_raw.py"
@($TenjiPy,$SCPy,$WTPy) | ForEach-Object { if (-not (Test-Path $_)) { throw "Script not found: $_" } }

Run-Step "tenji prior" $PythonExe @(
  $TenjiPy, "--raw-dir", $RawDir, "--from", $From, "--to", $To,
  "--m-strength", "200", "--sd-floor", "0.02",
  "--out", $TenjiOut, "--link-latest"
)
Run-Step "season_course prior" $PythonExe @(
  $SCPy, "--raw-dir", $RawDir, "--from", $From, "--to", $To,
  "--out", $SCOut, "--link-latest"
)
Run-Step "winning_trick prior" $PythonExe @(
  $WTPy, "--raw-dir", $RawDir, "--from", $From, "--to", $To,
  "--out", $WTOut, "--link-latest"
)

$Summary = @(
  "[DONE] priors updated",
  "  tenji         : $TenjiOut",
  "  season_course : $SCOut",
  "  winning_trick : $WTOut",
  "  log           : $LogPath"
) -join "`n"
$Summary | Tee-Object -FilePath $LogPath -Append | Out-Host
