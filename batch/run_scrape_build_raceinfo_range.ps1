param(
  [string]$StartDate = (Get-Date).ToString("yyyyMMdd"),
  [string]$EndDate   = (Get-Date).ToString("yyyyMMdd")
)

$RepoRoot = "C:\Users\user\Desktop\Git\ark-blueprints"
$PyDir    = "scripts"   # ← ここを scripts に
$Python   = "C:\anaconda3\python.exe"   # 必要に応じて変更

function ToYmd([string]$s) {
  if ($s -match '^\d{8}$') { return $s }
  if ($s -match '^\d{4}-\d{2}-\d{2}$') { return (Get-Date $s).ToString('yyyyMMdd') }
  throw "Date must be YYYYMMDD or YYYY-MM-DD: $s"
}
function ToDate([string]$ymd) { return [datetime]::ParseExact($ymd, 'yyyyMMdd', $null) }
function YMD([datetime]$d) { return $d.ToString('yyyyMMdd') }

$S  = ToYmd $StartDate
$E  = ToYmd $EndDate
$Sd = ToDate $S
$Ed = ToDate $E
if ($Sd -gt $Ed) { throw "StartDate must be <= EndDate" }

$totalDays = ($Ed - $Sd).Days + 1

Set-Location $RepoRoot

# Quick sanity check
foreach ($f in @("scrape.py","build_raw_csv.py","build_raceinfo.py")) {
  $p = Join-Path $RepoRoot (Join-Path $PyDir $f)
  if (-not (Test-Path $p)) { throw "File not found: $p" }
}

# STEP 1: scrape
Write-Host ("=== STEP 1: scrape ({0} - {1}) ===" -f $S, $E)
$d = $Sd; $i = 0
while ($d -le $Ed) {
  $i++
  $ymd = YMD $d
  Write-Progress -Activity "Scraping HTML" -Status ("{0} ({1}/{2})" -f $ymd, $i, $totalDays) -PercentComplete (($i / $totalDays) * 100)
  & $Python (Join-Path $PyDir "scrape.py") --date $ymd   # scripts/scrape.py 
  $d = $d.AddDays(1)
}
Write-Progress -Activity "Scraping HTML" -Completed -Status "Done"

# STEP 2: build_raw_csv
Write-Host ("=== STEP 2: build_raw_csv ({0} - {1}) ===" -f $S, $E)
$d = $Sd; $i = 0
while ($d -le $Ed) {
  $i++
  $ymd = YMD $d
  Write-Progress -Activity "Building raw/refund CSV" -Status ("{0} ({1}/{2})" -f $ymd, $i, $totalDays) -PercentComplete (($i / $totalDays) * 100)
  & $Python (Join-Path $PyDir "build_raw_csv.py") --date $ymd   # scripts/build_raw_csv.py 
  $d = $d.AddDays(1)
}
Write-Progress -Activity "Building raw/refund CSV" -Completed -Status "Done"

# STEP 3: build_raceinfo (range)
Write-Host ("=== STEP 3: build_raceinfo ({0} - {1}) ===" -f $S, $E)
Write-Progress -Activity "Building raceinfo" -Status ("{0} - {1}" -f $S, $E) -PercentComplete 0
& $Python (Join-Path $PyDir "build_raceinfo.py") --start-date $S --end-date $E   # scripts/build_raceinfo.py 
Write-Progress -Activity "Building raceinfo" -Completed -Status "Done"

Write-Host ("ALL DONE: {0} - {1}" -f $S, $E)
