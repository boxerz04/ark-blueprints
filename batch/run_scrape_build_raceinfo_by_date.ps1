param(
  [string]$Start = "20250618",          # YYYYMMDD
  [string]$End   = "20250619",          # YYYYMMDD
  [string]$PythonPath = "C:\anaconda3\python.exe",
  [string]$LogDir = "logs",
  [switch]$ConsoleMode                   # 付けたら True / 付けなければ False
)

# リポジトリ直下を基準にする（この ps1 は batch/ 配下）
Set-Location -Path (Join-Path $PSScriptRoot "..")

# 出力・tqdm
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:TQDM_MININTERVAL = "0.3"
$env:TQDM_DISABLE = "0"

# ログ
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$MainLog = Join-Path $LogDir ("scrape_build_raceinfo_{0}_{1}.log" -f $Start,$End)

# 日付パース
try {
  $s = [datetime]::ParseExact($Start,'yyyyMMdd',$null)
  $e = [datetime]::ParseExact($End  ,'yyyyMMdd',$null)
} catch {
  Write-Host "Invalid date. Use YYYYMMDD." -ForegroundColor Red
  exit 2
}

# python チェック
if (-not (Test-Path $PythonPath)) {
  Write-Host ("python not found: {0}" -f $PythonPath) -ForegroundColor Red
  exit 3
}

# 主要スクリプトの存在チェック（念のため）
$req = @("scripts\scrape.py","scripts\build_raw_csv.py","scripts\build_raceinfo.py")
foreach($p in $req){ if(-not (Test-Path $p)){ Write-Host ("missing: {0}" -f $p) -ForegroundColor Red; exit 4 } }

function Loop-Days {
  param([datetime]$from,[datetime]$to,[scriptblock]$body)
  for ($d=$from; $d -le $to; $d=$d.AddDays(1)) {
    $day = $d.ToString('yyyyMMdd')
    & $body $day
  }
}

# ★ 修正ポイント：引数は配列で渡す
function Run-Py {
  param([string[]]$ArgList)
  if ($ConsoleMode.IsPresent) {
    & $PythonPath -u @ArgList
    return $LASTEXITCODE
  } else {
    & $PythonPath -u @ArgList *> $MainLog
    return $LASTEXITCODE
  }
}

# ==== STEP 1: SCRAPE ====
Write-Host "==== STEP 1: SCRAPE all days ====" -ForegroundColor Cyan
Loop-Days $s $e {
  param($day)
  Write-Host ("[SCRAPE] {0}" -f $day) -ForegroundColor Cyan
  $code = Run-Py @("scripts\scrape.py","--date",$day)
  if ($code -ne 0) {
    Write-Host ("[WARN] scrape failed ({0}): {1}" -f $code,$day) -ForegroundColor Yellow
  }
}

# ==== STEP 2: BUILD raw/refund ====
Write-Host "==== STEP 2: BUILD raw/refund for all days ====" -ForegroundColor Cyan
Loop-Days $s $e {
  param($day)
  Write-Host ("[BUILD-RAW] {0}" -f $day) -ForegroundColor Cyan
  $code = Run-Py @("scripts\build_raw_csv.py","--date",$day)
  if ($code -ne 0) {
    Write-Host ("[WARN] build_raw_csv failed ({0}): {1}" -f $code,$day) -ForegroundColor Yellow
  }
}

# ==== STEP 3: BUILD raceinfo ====
Write-Host "==== STEP 3: BUILD raceinfo for all days ====" -ForegroundColor Cyan
Loop-Days $s $e {
  param($day)
  Write-Host ("[RACEINFO] {0}" -f $day) -ForegroundColor Cyan
  $code = Run-Py @("scripts\build_raceinfo.py","--date",$day)
  if ($code -ne 0) {
    Write-Host ("[WARN] build_raceinfo failed ({0}): {1}" -f $code,$day) -ForegroundColor Yellow
  }
}

if ($ConsoleMode.IsPresent) {
  Write-Host ("[DONE] finished. (log hint: {0})" -f $MainLog) -ForegroundColor Green
} else {
  Write-Host ("[DONE] finished. Log saved: {0}" -f $MainLog) -ForegroundColor Green
}
