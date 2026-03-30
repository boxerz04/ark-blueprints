#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"

"${PYTHON_BIN}" "${SCRIPT_DIR}/run_odds_scheduler_recheck.py" \
  --profile 2m \
  --mins_before 2 \
  "$@"
