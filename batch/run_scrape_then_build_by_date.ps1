param(
  [string]$Start = "20250910",   # YYYYMMDD
  [string]$End   = "20250915",   # YYYYMMDD
  [string]$PythonPath = "C:\anaconda3\python.exe"
)

# ★ batch/ に置くため、リポジトリ直下を作業ディレクトリにする
Set-Location -Path (Join-Path $PSScriptRoot "..")

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:TQDM_MININTERVAL = "0.3"

try {
  $s = [datetime]::ParseExact($Start,'yyyyMMdd',$null)
  $e = [datetime]::ParseExact($End  ,'yyyyMMdd',$null)
} catch {
  Write-Host "Invalid date. Use YYYYMMDD." -ForegroundColor Red
  exit 2
}

if (-not (Test-Path $PythonPath)) {
  Write-Host ("python not found: {0}" -f $PythonPath) -ForegroundColor Red
  exit 3
}

# ==== Step 1: Scraping all days ====
for ($d=$s; $d -le $e; $d=$d.AddDays(1)) {
  $day = $d.ToString('yyyyMMdd')
  Write-Host ("=== SCRAPE {0} ===" -f $day) -ForegroundColor Cyan
  & $PythonPath -u "scripts\scrape.py" --date $day
  if ($LASTEXITCODE -ne 0) {
    Write-Host ("[WARN] scrape failed ({0}): {1}" -f $LASTEXITCODE,$day) -ForegroundColor Yellow
  }
}

# ==== Step 2: Build all days ====
for ($d=$s; $d -le $e; $d=$d.AddDays(1)) {
  $day = $d.ToString('yyyyMMdd')
  Write-Host ("=== BUILD {0} ===" -f $day) -ForegroundColor Cyan
  & $PythonPath -u "scripts\build_raw_csv.py" --date $day
  if ($LASTEXITCODE -ne 0) {
    Write-Host ("[WARN] build failed ({0}): {1}" -f $LASTEXITCODE,$day) -ForegroundColor Yellow
  }
}

Write-Host "[DONE] scrape all -> build all finished." -ForegroundColor Green
