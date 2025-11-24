# batch/train_base_from_master.ps1
# -----------------------------------------------------------------------------
# master.csv（既に prior + course + sectional を上書き済み）から
# Base モデル用の特徴量作成 → 学習 → メタの要約表示までを行うスクリプト
#
# 例：
#   powershell -NoProfile -ExecutionPolicy Bypass -File ".\batch\train_base_from_master.ps1" `
#     -VersionTag "v1.3.1-base-20251124" `
#     -Notes "prior: 20231001–20240930, train: 20241001–20251031, prior+course+sectional 全部盛り（sectional10列）"
#
#   # さらに base/latest の中身を models/finals/* にもコピーしたい場合
#   powershell -NoProfile -ExecutionPolicy Bypass -File ".\batch\train_base_from_master.ps1" `
#     -VersionTag "v1.3.2-base-20251124" `
#     -Notes "..." `
#     -ModelAlias "finals"
# -----------------------------------------------------------------------------

param(
  [string]$RepoRoot    = "",
  [string]$PythonPath  = "C:\anaconda3\python.exe",

  # 入出力
  [string]$MasterCsv   = "data\processed\master.csv",
  [string]$BaseOutDir  = "data\processed\base",
  [string]$PipelineDir = "models\base\latest",

  # 学習メタ
  [Parameter(Mandatory=$true)][string]$VersionTag,
  [string]$Notes = "",

  # 追加: base/latest を別名モデルとしても保存したい場合のエイリアス名
  # 例）"finals" や "semi" など。空文字なら何もしない。
  [string]$ModelAlias = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-ParentDir([string]$path) {
  $parent = Split-Path -Parent $path
  if ($parent -and -not (Test-Path $parent)) {
    New-Item -ItemType Directory -Path $parent | Out-Null
  }
}

# RepoRoot 未指定ならこのファイルの親ディレクトリの上をルートに
if (-not $RepoRoot -or $RepoRoot.Trim() -eq "") {
  $RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
}

# Python 実行確認
if (-not (Test-Path $PythonPath)) {
  Write-Host "WARN  PythonPath not found: $PythonPath -> fallback to 'python'"
  $PythonPath = "python"
}

# フルパス
$MasterFull   = Join-Path $RepoRoot $MasterCsv
$BaseOutFull  = Join-Path $RepoRoot $BaseOutDir
$PipelineFull = Join-Path $RepoRoot $PipelineDir

Ensure-ParentDir $MasterFull
Ensure-ParentDir $BaseOutFull
Ensure-ParentDir $PipelineFull

# スクリプト検出
$prep_base_py = $null
foreach ($c in @("scripts\preprocess_base_features.py","preprocess_base_features.py")) {
  $p = Join-Path $RepoRoot $c
  if (Test-Path $p) { $prep_base_py = $p; break }
}
if (-not $prep_base_py) { throw "preprocess_base_features.py not found under repo." }

$train_py = $null
foreach ($c in @("scripts\train.py","train.py")) {
  $p = Join-Path $RepoRoot $c
  if (Test-Path $p) { $train_py = $p; break }
}
if (-not $train_py) { throw "train.py not found under repo." }

Write-Host "INFO RepoRoot     : $RepoRoot"
Write-Host "INFO Python       : $PythonPath"
Write-Host "INFO master.csv   : $MasterFull"
Write-Host "INFO base out dir : $BaseOutFull"
Write-Host "INFO pipeline dir : $PipelineFull"
if ($ModelAlias -and $ModelAlias.Trim() -ne "") {
  Write-Host "INFO ModelAlias   : $ModelAlias (base/latest を別名モデルとしてコピー保存)"
} else {
  Write-Host "INFO ModelAlias   : (なし)"
}

# -----------------------------------------------------------------------------
# Step 1) Base 用 特徴量作成（feature_pipeline.pkl も出力）
# -----------------------------------------------------------------------------
$argv1 = @(
  $prep_base_py,
  "--master", $MasterFull,
  "--out-dir", $BaseOutFull,
  "--pipeline-dir", $PipelineFull
)
Push-Location $RepoRoot
& $PythonPath @argv1
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "preprocess_base_features.py exited with code $LASTEXITCODE" }
Pop-Location
Write-Host "OK  base features prepared."

# -----------------------------------------------------------------------------
# Step 2) 学習（approach=base）
# -----------------------------------------------------------------------------
$argv2 = @(
  $train_py,
  "--approach", "base",
  "--version-tag", $VersionTag,
  "--notes", $Notes
)
Push-Location $RepoRoot
& $PythonPath @argv2
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "train.py exited with code $LASTEXITCODE" }
Pop-Location
Write-Host "OK  training finished."

# -----------------------------------------------------------------------------
# Step 3) メタ読み込み（UTF-8 明示）＆ 要約表示
#   - models/base/latest/train_meta.json を優先
#   - 無ければ runs 下の最新を探す
#   - ModelAlias が指定されていれば base/latest の中身を
#     models/<ModelAlias>/<model_id>/ と models/<ModelAlias>/latest/ にコピー
# -----------------------------------------------------------------------------
$latestMeta = Join-Path $RepoRoot "models\base\latest\train_meta.json"
if (-not (Test-Path $latestMeta)) {
  $runs = Get-ChildItem -Path (Join-Path $RepoRoot "models\base\runs") -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending
  foreach ($r in $runs) {
    $cand = Join-Path $r.FullName "train_meta.json"
    if (Test-Path $cand) { $latestMeta = $cand; break }
  }
}

if (Test-Path $latestMeta) {
  try {
    # UTF-8 強制（日本語 notes の文字化け対策）
    $jsonText = [System.IO.File]::ReadAllText($latestMeta, [System.Text.Encoding]::UTF8)
    $m = $jsonText | ConvertFrom-Json
  } catch {
    # フォールバック
    $m = Get-Content $latestMeta -Raw -Encoding UTF8 | ConvertFrom-Json
  }

  Write-Host "OK  latest meta   : $latestMeta"
  Write-Host ("ID/Tag            : {0} / {1}" -f $m.model_id, $m.version_tag)
  Write-Host ("Rows/Feats        : {0} / {1}" -f $m.n_rows, $m.n_features)
  if ($m.eval) {
    Write-Host ("Eval(AUC/PR/LL)   : {0:N6} / {1:N6} / {2:N6}" -f $m.eval.auc, $m.eval.pr_auc, $m.eval.logloss)
    Write-Host ("Acc/MCC/Top2Hit   : {0:N6} / {1:N6} / {2:N6}" -f $m.eval.accuracy, $m.eval.mcc, $m.eval.top2_hit)
  }

  # ここから追加: base/latest の内容を ModelAlias 配下にもコピー
  if ($ModelAlias -and $ModelAlias.Trim() -ne "") {
    $aliasRoot   = Join-Path $RepoRoot ("models\" + $ModelAlias)
    $aliasRunDir = Join-Path $aliasRoot $m.model_id
    $aliasLatest = Join-Path $aliasRoot "latest"

    New-Item -ItemType Directory -Force -Path $aliasRunDir  | Out-Null
    New-Item -ItemType Directory -Force -Path $aliasLatest  | Out-Null

    $baseLatestDir = Join-Path $RepoRoot "models\base\latest"
    if (Test-Path $baseLatestDir) {
      Get-ChildItem -Path $baseLatestDir | ForEach-Object {
        Copy-Item $_.FullName -Destination (Join-Path $aliasRunDir $_.Name) -Force
        Copy-Item $_.FullName -Destination (Join-Path $aliasLatest $_.Name) -Force
      }
      Write-Host ("OK  aliased copy  : models\{0}\{1} + latest" -f $ModelAlias, $m.model_id)
    } else {
      Write-Host "WARN base latest dir not found; alias copy skipped: $baseLatestDir"
    }
  }
} else {
  Write-Host "WARN latest train_meta.json not found. (models\base\latest or runs\*)"
}
