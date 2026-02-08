# batch/train_model_from_master.ps1
# -----------------------------------------------------------------------------
# 共通 master（または派生 master）から任意のモデルを学習する汎用スクリプト。
#
# 目的
# ----
# 1) 特徴量生成（preprocess_*）を実行して X/y/ids と feature_pipeline を用意する
# 2) train.py を approach 指定で実行し、モデルを models/<Approach>/runs/<model_id>/ に保存する
# 3) models/<Approach>/latest/train_meta.json を読み、学習結果を要約表示する
# 4) ModelAlias が指定されていれば models/<ModelAlias>/ にも成果物をコピーする
#
# Phase 2（YAML固定）対応
# ----------------------
# preprocess_base_features.py は Phase 2 で以下を要求する：
#   --master <csv> --feature-spec-yaml <yaml> --approach <approach>
#
# ただし移行期の互換性のため、本PS1は二つの呼び出し形をサポートする：
#
# [A] Phase 2 モード（YAML固定）
#   -FeatureSpecYaml が指定されている場合：
#     python preprocess_base_features.py --master ... --feature-spec-yaml ... --approach ...
#
# [B] 旧モード（従来互換）
#   -FeatureSpecYaml が未指定の場合：
#     python preprocess_*.py --master ... --out-dir ... --pipeline-dir ...
#
# 注意
# ----
# - Phase 2 モードでは、preprocess 側の出力先は「コードで固定」されている前提。
#   そのため FeaturesOutDir / PipelineDir は「ログ表示・互換目的」で残す。
# - 旧モードの preprocess を使う場合は従来通り FeaturesOutDir / PipelineDir が有効。
# -----------------------------------------------------------------------------

param(
  # ---------------------------------------------------------------------------
  # 実行環境
  # ---------------------------------------------------------------------------
  [string]$RepoRoot   = "",
  [string]$PythonPath = "C:\anaconda3\python.exe",

  # ---------------------------------------------------------------------------
  # 入出力（master → 特徴量 → モデル）
  # ---------------------------------------------------------------------------
  [string]$MasterCsv      = "data\processed\master.csv",

  # 旧モード互換の出力指定（Phase 2 モードでは preprocess 側が固定出力の想定）
  [string]$FeaturesOutDir = "",   # 空なら data\processed\<Approach>
  [string]$PipelineDir    = "",   # 空なら models\<Approach>\latest

  # ---------------------------------------------------------------------------
  # 前処理スクリプト（特徴量生成）
  # ---------------------------------------------------------------------------
  [string]$PrepScript = "scripts\preprocess_base_features.py",

  # Phase 2: YAML固定（列SSOT）
  # 例: features\finals.yaml
  [string]$FeatureSpecYaml = "",

  # ---------------------------------------------------------------------------
  # 学習メタ
  # ---------------------------------------------------------------------------
  [Parameter(Mandatory=$true)][string]$Approach,   # train.py --approach
  [Parameter(Mandatory=$true)][string]$VersionTag,
  [string]$Notes = "",

  # models/<Approach>/latest → models/<ModelAlias>/ にもコピーしたいときの別名（例: finals）
  [string]$ModelAlias = "",

  # 特徴量生成をスキップ（既に X/y/ids が揃っている場合）
  [switch]$SkipPreprocess
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$path) {
  if (-not (Test-Path $path)) {
    New-Item -ItemType Directory -Force -Path $path | Out-Null
  }
}

function Ensure-ParentDir([string]$path) {
  $parent = Split-Path -Parent $path
  if ($parent -and -not (Test-Path $parent)) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
  }
}

# RepoRoot 未指定なら、このファイルの親ディレクトリの上をルートにする
if (-not $RepoRoot -or $RepoRoot.Trim() -eq "") {
  $RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
}

# Python 実行確認（無ければ python を試す）
if (-not (Test-Path $PythonPath)) {
  Write-Host "WARN  PythonPath not found: $PythonPath -> fallback to 'python'"
  $PythonPath = "python"
}

# デフォルトパス補完（Approach から推定）
if (-not $FeaturesOutDir -or $FeaturesOutDir.Trim() -eq "") {
  $FeaturesOutDir = "data\processed\$Approach"
}
if (-not $PipelineDir -or $PipelineDir.Trim() -eq "") {
  $PipelineDir = "models\$Approach\latest"
}

# フルパス化（ログ表示・互換確保）
$MasterFull   = Join-Path $RepoRoot $MasterCsv
$FeatOutFull  = Join-Path $RepoRoot $FeaturesOutDir
$PipelineFull = Join-Path $RepoRoot $PipelineDir

# 旧モードで必要になるディレクトリを作っておく（Phase 2 でも害はない）
Ensure-ParentDir $MasterFull
Ensure-Dir $FeatOutFull
Ensure-Dir $PipelineFull

# FeatureSpecYaml フルパス化（指定時）
$SpecFull = ""
if ($FeatureSpecYaml -and $FeatureSpecYaml.Trim() -ne "") {
  $SpecFull = $FeatureSpecYaml
  if (-not [System.IO.Path]::IsPathRooted($SpecFull)) {
    $SpecFull = Join-Path $RepoRoot $SpecFull
  }
}

# preprocess スクリプト検出
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

# 実行情報
Write-Host "INFO RepoRoot        : $RepoRoot"
Write-Host "INFO Python          : $PythonPath"
Write-Host "INFO MasterCsv       : $MasterFull"
Write-Host "INFO Approach        : $Approach"
Write-Host "INFO VersionTag      : $VersionTag"
Write-Host "INFO Notes           : $Notes"
Write-Host "INFO PrepScript      : $prep_full"

if ($SpecFull -and $SpecFull.Trim() -ne "") {
  Write-Host "INFO FeatureSpecYaml : $SpecFull (Phase 2: YAML固定)"
} else {
  Write-Host "INFO FeatureSpecYaml : (none) (旧モード: out-dir / pipeline-dir を使用)"
}

Write-Host "INFO FeaturesOutDir  : $FeatOutFull"
Write-Host "INFO PipelineDir     : $PipelineFull"

if ($ModelAlias -and $ModelAlias.Trim() -ne "") {
  Write-Host "INFO ModelAlias      : $ModelAlias (models\<alias>\* にコピー保存)"
} else {
  Write-Host "INFO ModelAlias      : (none)"
}

# -----------------------------------------------------------------------------
# Step 1) 特徴量生成（PrepScript）
# -----------------------------------------------------------------------------
if (-not $SkipPreprocess) {
  if (-not (Test-Path $MasterFull)) {
    throw "master.csv not found: $MasterFull"
  }

  # Phase 2（YAML固定）: --feature-spec-yaml と --approach を渡す
  # 旧モード互換         : --out-dir と --pipeline-dir を渡す
  $argv1 = @($prep_full)

  if ($SpecFull -and $SpecFull.Trim() -ne "") {
    if (-not (Test-Path $SpecFull)) {
      throw "FeatureSpecYaml not found: $SpecFull"
    }
    $argv1 += @(
      "--master", $MasterFull,
      "--feature-spec-yaml", $SpecFull,
      "--approach", $Approach
    )
  } else {
    $argv1 += @(
      "--master", $MasterFull,
      "--out-dir", $FeatOutFull,
      "--pipeline-dir", $PipelineFull
    )
  }

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
  # latest が無い場合は runs の中から新しいものを拾う
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

  Write-Host "OK  latest meta      : $latestMeta"
  Write-Host ("ID/Tag               : {0} / {1}" -f $m.model_id, $m.version_tag)
  Write-Host ("Rows/Feats           : {0} / {1}" -f $m.n_rows, $m.n_features)
  if ($m.eval) {
    Write-Host ("Eval(AUC/PR/LL)      : {0:N6} / {1:N6} / {2:N6}" -f $m.eval.auc, $m.eval.pr_auc, $m.eval.logloss)
    Write-Host ("Acc/MCC/Top2Hit      : {0:N6} / {1:N6} / {2:N6}" -f $m.eval.accuracy, $m.eval.mcc, $m.eval.top2_hit)
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
        Write-Host ("OK  aliased copy     : models\{0}\{1} (latest は元々同じのためコピー省略)" -f $ModelAlias, $m.model_id)
      } else {
        Write-Host ("OK  aliased copy     : models\{0}\{1} + latest" -f $ModelAlias, $m.model_id)
      }
    } else {
      Write-Host "WARN source latest dir not found; alias copy skipped: $srcLatestDir"
    }
  }
} else {
  Write-Host ("WARN latest train_meta.json not found for approach='{0}'." -f $Approach)
}

Write-Host "DONE all steps completed."
