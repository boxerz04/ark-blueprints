# batch/train_base_from_master.ps1  (ASCII-safe)
param(
  [string]$RepoRoot = "",
  [string]$PythonPath = "C:\anaconda3\python.exe",

  # master.csv を既に build_master_range.ps1 で作成済みと想定
  [string]$MasterPath   = "data\processed\master.csv",

  # 出力先（preprocess_base_features.py / train.py に準拠）
  [string]$BaseOutDir   = "data\processed\base",
  [string]$PipelineDir  = "models\base\latest",

  # 学習設定（train.py に準拠）
  [int]$NEstimators = 400,
  [double]$LearningRate = 0.05,
  [int]$NumLeaves = 63,
  [double]$Subsample = 0.8,
  [double]$ColsampleByTree = 0.8,
  [int]$RandomState = 42,
  [int]$NJobs = -1,

  # メタ情報
  [string]$VersionTag = "",
  [string]$Notes = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $RepoRoot -or $RepoRoot.Trim() -eq "") {
  $RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
}

if (-not (Test-Path $PythonPath)) {
  Write-Host "WARN: PythonPath not found: $PythonPath -> fallback to 'python'"
  $PythonPath = "python"
}

# パス解決
$MasterFull   = Join-Path $RepoRoot $MasterPath
$BaseOutFull  = Join-Path $RepoRoot $BaseOutDir
$PipelineFull = Join-Path $RepoRoot $PipelineDir

# スクリプト検出
$prep_base_py = Join-Path $RepoRoot "scripts\preprocess_base_features.py"
$train_py     = Join-Path $RepoRoot "scripts\train.py"
if (-not (Test-Path $prep_base_py)) { throw "scripts\preprocess_base_features.py not found." }
if (-not (Test-Path $train_py))     { throw "scripts\train.py not found." }

Write-Host "INFO RepoRoot     : $RepoRoot"
Write-Host "INFO Python       : $PythonPath"
Write-Host "INFO master.csv   : $MasterFull"
Write-Host "INFO base out dir : $BaseOutFull"
Write-Host "INFO pipeline dir : $PipelineFull"

if (-not (Test-Path $MasterFull)) { throw "master.csv not found at $MasterFull" }

# Step A) 特徴量前処理（X, y, ids, feature_pipeline.pkl を生成）
New-Item -ItemType Directory -Force -Path $BaseOutFull  | Out-Null
New-Item -ItemType Directory -Force -Path $PipelineFull | Out-Null

$argvA = @(
  $prep_base_py,
  "--master", $MasterFull,
  "--out-dir", $BaseOutFull,
  "--pipeline-dir", $PipelineFull
)

Push-Location $RepoRoot
try {
  & $PythonPath @argvA
  if ($LASTEXITCODE -ne 0) { throw "preprocess_base_features.py exited with code $LASTEXITCODE" }
}
finally { Pop-Location }
Write-Host "OK  base features prepared."

# Step B) 学習（approach=base）
if (-not $VersionTag -or $VersionTag.Trim() -eq "") { $VersionTag = "base_from_master" }
if (-not $Notes -or $Notes.Trim() -eq "") { $Notes = "Train base from existing master.csv" }

$argvB = @(
  $train_py,
  "--approach", "base",
  "--n-estimators", $NEstimators,
  "--learning-rate", $LearningRate,
  "--num-leaves", $NumLeaves,
  "--subsample", $Subsample,
  "--colsample-bytree", $ColsampleByTree,
  "--random-state", $RandomState,
  "--n-jobs", $NJobs,
  "--version-tag", $VersionTag,
  "--notes", $Notes
)

Push-Location $RepoRoot
try {
  & $PythonPath @argvB
  if ($LASTEXITCODE -ne 0) { throw "train.py exited with code $LASTEXITCODE" }
}
finally { Pop-Location }
Write-Host "OK  training finished."

# 仕上げ：最新メタを表示
$latestMeta = Join-Path $RepoRoot "models\base\latest\train_meta.json"
if (Test-Path $latestMeta) {
  $m = Get-Content $latestMeta -Raw | ConvertFrom-Json
  $e = $m.eval
  Write-Host ("EVAL auc={0} pr_auc={1} logloss={2} acc={3} mcc={4} top2_hit={5}" -f `
    $e.auc, $e.pr_auc, $e.logloss, $e.accuracy, $e.mcc, $e.top2_hit)
  Write-Host "OK  artifacts -> models\base\latest  and  models\base\runs\$($m.model_id)"
} else {
  Write-Host "WARN: train_meta.json not found at $latestMeta"
}
