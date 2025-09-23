# ================================
# base モデルの一連処理 (batch ディレクトリから実行)
# 1. master.csv 生成
# 2. base 特徴量生成
# 3. base モデル学習
# ================================

$PYTHON = "python"

# リポジトリルートを基準にする
$REPO_ROOT = (Resolve-Path "$PSScriptRoot\..").Path
Set-Location $REPO_ROOT

Write-Host "=== Step 1: preprocess.py (master.csv生成) ==="
& $PYTHON scripts/preprocess.py --raw-dir data/raw --out data/processed/master.csv --reports-dir data/processed/reports
if ($LASTEXITCODE -ne 0) { throw "preprocess.py failed" }

Write-Host "=== Step 2: preprocess_base_features.py (base特徴量生成) ==="
& $PYTHON scripts/preprocess_base_features.py --master data/processed/master.csv --out-dir data/processed/base --pipeline-dir models/base/latest
if ($LASTEXITCODE -ne 0) { throw "preprocess_base_features.py failed" }

Write-Host "=== Step 3: train.py (baseモデル学習) ==="
# バージョンタグとメモをここで指定
$VERSION_TAG = "v1.0.5-base-$(Get-Date -Format yyyyMMdd)"
$NOTES = "master更新 + base featuresスクリプト化"

& $PYTHON scripts/train.py --approach base --version-tag $VERSION_TAG --notes $NOTES
if ($LASTEXITCODE -ne 0) { throw "train.py failed" }

Write-Host "=== All steps completed successfully ==="
