# batch/train_model_from_master.ps1
# -----------------------------------------------------------------------------
# 共通 master（または派生 master）から任意のモデルを学習する汎用スクリプト。
#
# 目的
# ----
# 1) 特徴量生成（preprocess_*）を実行して X/y/ids と feature_pipeline を用意する
# 2) train.py を approach 指定で実行し、モデルを models/<Approach>/runs/<model_id>/ に保存する
# 3) models/<Approach>/latest/train_meta.json を読み、学習結果を要約表示する
# 4) ModelAlias が指定されていれば models/<Approach>/<ModelAlias>/ も更新する
#
# 前提（Phase 2）
# --------------
# - YAML を SSOT とし、列選択は features/<approach>.yaml で管理する
# - preprocess 側は Phase 2 の固定出力（data/processed/<approach> と models/<approach>/latest）を想定
# - 本PS1は「繰り返し実行」されるため、引数の取りこぼしが事故（列爆増・master誤用）に直結する
#
# 今回の小改修（安全性強化）
# ------------------------
# - MasterCsv のデフォルトを data\processed\master_finals.csv に変更
# - FeatureSpecYaml を必須化（未指定時はPS1で即停止）
# -----------------------------------------------------------------------------

[CmdletBinding()]
param(
  [string]$RepoRoot   = "",
  [string]$PythonPath = "C:\anaconda3\python.exe",

  # ★ デフォルトを master_finals.csv に変更
  [string]$MasterCsv      = "data\processed\master_finals.csv",

  [string]$FeaturesOutDir = "",
  [string]$PipelineDir    = "",

  [string]$PrepScript = "scripts\preprocess_base_features.py",

  # ★ 必須化
  [string]$FeatureSpecYaml = "",

  [string]$Approach   = "",
  [string]$VersionTag = "",
  [string]$Notes      = "",

  [string]$ModelAlias = "",

  [string]$LgbmParamsYaml = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($k, $v) { Write-Host ("INFO {0,-15}: {1}" -f $k, $v) }
function Ensure-Dir([string]$p) { if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null } }

# ---------------------------------------------------------------------------
# RepoRoot 解決（★修正点：.Path を使わない）
# ---------------------------------------------------------------------------
if (-not $RepoRoot -or $RepoRoot.Trim() -eq "") {
  $RepoRoot = Resolve-Path (Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) "..")
}

# 必須引数チェック（従来挙動＋安全化）
if (-not $Approach -or $Approach.Trim() -eq "") {
  throw "Approach is required. Pass -Approach <name>"
}
if (-not $VersionTag -or $VersionTag.Trim() -eq "") {
  throw "VersionTag is required. Pass -VersionTag <tag>"
}
if (-not $FeatureSpecYaml -or $FeatureSpecYaml.Trim() -eq "") {
  throw "FeatureSpecYaml is required. Pass -FeatureSpecYaml <yaml>"
}

# フルパス化
$PythonFull = $PythonPath
if (-not [System.IO.Path]::IsPathRooted($PythonFull)) {
  $PythonFull = Join-Path $RepoRoot $PythonFull
}
if (-not (Test-Path $PythonFull)) {
  throw "Python not found ('$PythonFull')"
}

$MasterFull = $MasterCsv
if (-not [System.IO.Path]::IsPathRooted($MasterFull)) {
  $MasterFull = Join-Path $RepoRoot $MasterFull
}
if (-not (Test-Path $MasterFull)) {
  throw "MasterCsv not found ('$MasterFull')"
}

$SpecFull = $FeatureSpecYaml
if (-not [System.IO.Path]::IsPathRooted($SpecFull)) {
  $SpecFull = Join-Path $RepoRoot $SpecFull
}
if (-not (Test-Path $SpecFull)) {
  throw "FeatureSpecYaml not found ('$SpecFull')"
}

$LgbmParamsFull = ""
if ($LgbmParamsYaml -and $LgbmParamsYaml.Trim() -ne "") {
  $LgbmParamsFull = $LgbmParamsYaml
  if (-not [System.IO.Path]::IsPathRooted($LgbmParamsFull)) {
    $LgbmParamsFull = Join-Path $RepoRoot $LgbmParamsFull
  }
  if (-not (Test-Path $LgbmParamsFull)) {
    throw "LgbmParamsYaml not found ('$LgbmParamsFull')"
  }
}

$prep_full = Join-Path $RepoRoot $PrepScript
if (-not (Test-Path $prep_full)) {
  throw "PrepScript not found ('$prep_full')"
}

if (-not $FeaturesOutDir -or $FeaturesOutDir.Trim() -eq "") {
  $FeaturesOutDir = "data\processed\$Approach"
}
if (-not $PipelineDir -or $PipelineDir.Trim() -eq "") {
  $PipelineDir = "models\$Approach\latest"
}

$FeatOutFull  = Join-Path $RepoRoot $FeaturesOutDir
$PipelineFull = Join-Path $RepoRoot $PipelineDir
Ensure-Dir $FeatOutFull
Ensure-Dir $PipelineFull

# ログ
Info "RepoRoot"        $RepoRoot
Info "Python"          $PythonFull
Info "MasterCsv"       $MasterFull
Info "Approach"        $Approach
Info "VersionTag"      $VersionTag
Info "Notes"           $Notes
Info "PrepScript"      $prep_full
Info "FeatureSpecYaml" "$SpecFull (Phase 2: YAML SSOT)"
Info "FeaturesOutDir"  $FeatOutFull
Info "PipelineDir"     $PipelineFull
Info "ModelAlias"      ($(if ($ModelAlias -and $ModelAlias.Trim() -ne "") { $ModelAlias } else { "(none)" }))
Info "LgbmParamsYaml"  ($(if ($LgbmParamsFull -and $LgbmParamsFull.Trim() -ne "") { $LgbmParamsFull } else { "(none)" }))

# ---------------------------------------------------------------------------
# 1) preprocess
# ---------------------------------------------------------------------------
& $PythonFull $prep_full `
  --master $MasterFull `
  --feature-spec-yaml $SpecFull `
  --approach $Approach

if ($LASTEXITCODE -ne 0) {
  throw "PrepScript exited with code $LASTEXITCODE"
}
Write-Host "OK  features prepared."

# ---------------------------------------------------------------------------
# 2) train（既存仕様を尊重）
# ---------------------------------------------------------------------------
$train_full = Join-Path $RepoRoot "scripts\train.py"
if (-not (Test-Path $train_full)) {
  throw "Train script not found ('$train_full')"
}

$train_args = @(
  $train_full,
  "--approach", $Approach,
  "--version-tag", $VersionTag
)

if ($Notes -and $Notes.Trim() -ne "") {
  $train_args += @("--notes", $Notes)
}
if ($ModelAlias -and $ModelAlias.Trim() -ne "") {
  $train_args += @("--model-alias", $ModelAlias)
}
if ($LgbmParamsFull -and $LgbmParamsFull.Trim() -ne "") {
  $train_args += @("--lgbm-params-yaml", $LgbmParamsFull)
}

& $PythonFull @train_args
if ($LASTEXITCODE -ne 0) {
  throw "Train script exited with code $LASTEXITCODE"
}

Write-Host "OK  training finished."

# ---------------------------------------------------------------------------
# 3) summary
# ---------------------------------------------------------------------------
$latest_meta = Join-Path $RepoRoot "models\$Approach\latest\train_meta.json"
if (Test-Path $latest_meta) {
  Write-Host ("OK  latest meta      : {0}" -f $latest_meta)
}

Write-Host "DONE all steps completed."
