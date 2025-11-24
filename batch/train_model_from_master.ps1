# batch/train_model_from_master.ps1
# -----------------------------------------------------------------------------
# 共通 master（または派生 master）から任意のモデルを学習する汎用スクリプト。
#
# フロー:
#   1) 特徴量生成（任意の Python スクリプトを指定可能）
#   2) train.py を approach 指定で実行
#   3) models/<Approach>/latest/train_meta.json を読み要約表示
#   4) ModelAlias が指定されていれば models/<ModelAlias>/ にもコピー
#
# 例1: 通常の base モデル（共通 master を使用）
#   powershell -NoProfile -ExecutionPolicy Bypass `
#     -File .\batch\train_model_from_master.ps1 `
#     -Approach "base" `
#     -VersionTag "v1.3.3-base-20251124" `
#     -Notes "train_model_from_master.ps1 動作テスト（base）" `
#     -MasterCsv "data\processed\master.csv" `
#     -FeaturesOutDir "data\processed\base" `
#     -PipelineDir "models\base\latest"
#
# 例2: finals モデル（優勝/準優/準優進出戦だけの master_finals.csv を使用）
#   powershell -NoProfile -ExecutionPolicy Bypass `
#     -File .\batch\train_model_from_master.ps1 `
#     -Approach "finals" `
#     -VersionTag "v1.0.0-finals-20251124" `
#     -Notes "優勝/準優/準優進出戦専用モデル" `
#     -MasterCsv "data\processed\master_finals.csv" `
#     -FeaturesOutDir "data\processed\finals" `
#     -PipelineDir "models\finals\latest" `
#     -ModelAlias "finals"
#
# ※ Approach は train.py に渡す --approach の値。
#    ModelAlias は「models/<alias>/... にもコピー保存したいとき」に使うラベル。
# -----------------------------------------------------------------------------

param(
  # ルート/実行環境
  [string]$RepoRoot   = "",
  [string]$PythonPath = "C:\anaconda3\python.exe",

  # 入出力（master → 特徴量 → モデル）
  [string]$MasterCsv      = "data\processed\master.csv",
  [string]$FeaturesOutDir = "",   # 空なら data\processed/<Approach>
  [string]$PipelineDir    = "",   # 空なら models/<Approach>/latest

  # 前処理スクリプト（特徴量生成）
  # 例: scripts\preprocess_base_features.py / scripts\preprocess_sectional_features.py など
  [string]$PrepScript = "scripts\preprocess_base_features.py",

  # 学習メタ
  [Parameter(Mandatory=$true)][string]$Approach,   # train.py --approach
  [Parameter(Mandatory=$true)][string]$VersionTag,
  [string]$Notes = "",

  # models/<Approach>/latest → models/<ModelAlias>/ にもコピーしたいときの別名
  # finals など。空ならコピーしない。
  [string]$ModelAlias = "",

  # 特徴量生成をスキップしたい場合（既に FeaturesOutDir に X/y があるとき）
  [switch]$SkipPreprocess
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

# デフォルトパスの補完（Approach から推定）
if (-not $FeaturesOutDir -or $FeaturesOutDir.Trim() -eq "") {
  $FeaturesOutDir = "data\processed\$Approach"
}
if (-not $PipelineDir -or $PipelineDir.Trim() -eq "") {
  $PipelineDir = "models\$Approach\latest"
}

# フルパスに変換
$MasterFull   = Join-Path $RepoRoot $MasterCsv
$FeatOutFull  = Join-Path $RepoRoot $FeaturesOutDir
$PipelineFull = Join-Path $RepoRoot $PipelineDir

Ensure-ParentDir $MasterFull
Ensure-ParentDir $FeatOutFull
Ensure-ParentDir $PipelineFull

# 前処理スクリプト検出
$prep_full = $null
foreach ($c in @($PrepScript, "scripts\preprocess_base_features.py")) {
  $p = $c
  if (-not [System.IO.Path]::IsPathRooted($p)) {
    $p = Join-Path $RepoRoot $p
  }
  if (Test-Path $p) {
    $prep_full = $p
    break
  }
}
if (-not $SkipPreprocess -and -not $prep_full) {
  throw "PrepScript not found (指定: '$PrepScript')"
}

# train.py 検出
$train_py = $null
foreach ($c in @("scripts\train.py","train.py")) {
  $p = Join-Path $RepoRoot $c
  if (Test-Path $p) {
    $train_py = $p
    break
  }
}
if (-not $train_py) { throw "train.py not found under repo." }

Write-Host "INFO RepoRoot      : $RepoRoot"
Write-Host "INFO Python        : $PythonPath"
Write-Host "INFO master.csv    : $MasterFull"
Write-Host "INFO features out  : $FeatOutFull"
Write-Host "INFO pipeline dir  : $PipelineFull"
Write-Host "INFO PrepScript    : $prep_full"
Write-Host "INFO Approach      : $Approach"
Write-Host "INFO VersionTag    : $VersionTag"
Write-Host "INFO Notes         : $Notes"
if ($ModelAlias -and $ModelAlias.Trim() -ne "") {
  Write-Host "INFO ModelAlias    : $ModelAlias (models\<alias>\* にコピー保存)"
} else {
  Write-Host "INFO ModelAlias    : (none)"
}

# -----------------------------------------------------------------------------
# Step 1) 特徴量生成（任意の PrepScript） → X/y/ids + feature_pipeline.pkl
# -----------------------------------------------------------------------------
if (-not $SkipPreprocess) {
  if (-not (Test-Path $MasterFull)) {
    throw "master.csv not found: $MasterFull"
  }

  $argv1 = @(
    $prep_full,
    "--master", $MasterFull,
    "--out-dir", $FeatOutFull,
    "--pipeline-dir", $PipelineFull
  )
  Push-Location $RepoRoot
  & $PythonPath @argv1
  if ($LASTEXITCODE -ne 0) { 
    Pop-Location
    throw "PrepScript exited with code $LASTEXITCODE"
  }
  Pop-Location
  Write-Host "OK  features prepared."
} else {
  Write-Host "SKIP features step (SkipPreprocess specified)"
}

# -----------------------------------------------------------------------------
# Step 2) 学習（train.py --approach <Approach>）
# -----------------------------------------------------------------------------
$argv2 = @(
  $train_py,
  "--approach", $Approach,
  "--version-tag", $VersionTag,
  "--notes", $Notes
)
Push-Location $RepoRoot
& $PythonPath @argv2
if ($LASTEXITCODE -ne 0) { 
  Pop-Location
  throw "train.py exited with code $LASTEXITCODE"
}
Pop-Location
Write-Host "OK  training finished."

# -----------------------------------------------------------------------------
# Step 3) メタ読み込み（models/<Approach>/latest/train_meta.json）＆ 要約表示
# -----------------------------------------------------------------------------
$latestMeta = Join-Path $RepoRoot ("models\" + $Approach + "\latest\train_meta.json")
if (-not (Test-Path $latestMeta)) {
  $runsRoot = Join-Path $RepoRoot ("models\" + $Approach + "\runs")
  $runs = Get-ChildItem -Path $runsRoot -Directory -ErrorAction SilentlyContinue | Sort-Object Name -Descending
  foreach ($r in $runs) {
    $cand = Join-Path $r.FullName "train_meta.json"
    if (Test-Path $cand) {
      $latestMeta = $cand
      break
    }
  }
}

if (Test-Path $latestMeta) {
  try {
    $jsonText = [System.IO.File]::ReadAllText($latestMeta, [System.Text.Encoding]::UTF8)
    $m = $jsonText | ConvertFrom-Json
  } catch {
    $m = Get-Content $latestMeta -Raw -Encoding UTF8 | ConvertFrom-Json
  }

  Write-Host "OK  latest meta    : $latestMeta"
  Write-Host ("ID/Tag             : {0} / {1}" -f $m.model_id, $m.version_tag)
  Write-Host ("Rows/Feats         : {0} / {1}" -f $m.n_rows, $m.n_features)
  if ($m.eval) {
    Write-Host ("Eval(AUC/PR/LL)    : {0:N6} / {1:N6} / {2:N6}" -f $m.eval.auc, $m.eval.pr_auc, $m.eval.logloss)
    Write-Host ("Acc/MCC/Top2Hit    : {0:N6} / {1:N6} / {2:N6}" -f $m.eval.accuracy, $m.eval.mcc, $m.eval.top2_hit)
  }

  # ---------------------------------------------------------------------------
  # Step 4) ModelAlias が指定されていれば models/<ModelAlias>/ にもコピー
  # ---------------------------------------------------------------------------
  if ($ModelAlias -and $ModelAlias.Trim() -ne "") {
    $srcLatestDir = Join-Path $RepoRoot ("models\" + $Approach + "\latest")
    $aliasRoot    = Join-Path $RepoRoot ("models\" + $ModelAlias)
    $aliasRunDir  = Join-Path $aliasRoot $m.model_id
    $aliasLatest  = Join-Path $aliasRoot "latest"

    New-Item -ItemType Directory -Force -Path $aliasRunDir | Out-Null
    New-Item -ItemType Directory -Force -Path $aliasLatest | Out-Null

    if (Test-Path $srcLatestDir) {
      $sameLatest = ($srcLatestDir -eq $aliasLatest)

      Get-ChildItem -Path $srcLatestDir | ForEach-Object {
        # 履歴ディレクトリ（models/<alias>/<model_id>/）には常にコピー
        Copy-Item $_.FullName -Destination (Join-Path $aliasRunDir $_.Name) -Force

        # latest へは「パスが同じでない場合のみ」コピー
        if (-not $sameLatest) {
          Copy-Item $_.FullName -Destination (Join-Path $aliasLatest $_.Name) -Force
        }
      }

      if ($sameLatest) {
        Write-Host ("OK  aliased copy   : models\{0}\{1} (latest は元々同じのためコピー省略)" -f $ModelAlias, $m.model_id)
      } else {
        Write-Host ("OK  aliased copy   : models\{0}\{1} + latest" -f $ModelAlias, $m.model_id)
      }
    } else {
      Write-Host "WARN source latest dir not found; alias copy skipped: $srcLatestDir"
    }
  }
} else {
  Write-Host ("WARN latest train_meta.json not found for approach='{0}'." -f $Approach)
}

Write-Host "DONE all steps completed."
