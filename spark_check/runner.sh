#!/usr/bin/env bash
set -euo pipefail
CHECK_DIR="/home/spark/notebooks/study/store_hse/spark_check"
if [[ $# -gt 0 ]]; then
  DATES=("$@")
else
  DATES=("2026-03-05" "2026-03-06")
fi

for d in "${DATES[@]}"; do
  bash "$CHECK_DIR/run_check.sh" --load-date "$d" --log-file checks.log
done

echo "ALL CHECKS PASSED"