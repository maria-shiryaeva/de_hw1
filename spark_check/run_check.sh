#!/usr/bin/env bash
set -euo pipefail

LOAD_DATE="2026-03-05"
LOG_FILE="checks.log"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --load-date)
      LOAD_DATE="$2"
      shift 2
      ;;
    --log-file)
      LOG_FILE="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

BASE_DIR="/home/spark/notebooks/study/store_hse/spark"
CHECK_DIR="/home/spark/notebooks/study/store_hse/spark_check"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"

run_if_exists() {
  local script_path="$1"
  if [[ -f "$script_path" ]]; then
    bash "$script_path" --load-date "$LOAD_DATE"
  fi
}

run_if_exists "$BASE_DIR/raw/orders/orders.sh"
run_if_exists "$BASE_DIR/raw/payments/payments.sh"
run_if_exists "$BASE_DIR/staging/stg_orders/stg_orders.sh"
run_if_exists "$BASE_DIR/staging/stg_payments/stg_payments.sh"
run_if_exists "$BASE_DIR/intermediate/int_orders_with_payments/int_orders_with_payments.sh"
run_if_exists "$BASE_DIR/marts/fct_orders/fct_orders.sh"

set +e
OUT="$(python3 "$CHECK_DIR/check.py" --load-date "$LOAD_DATE" --run-id "$RUN_ID" 2>&1)"
RC=$?
set -e

TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "$TS | $OUT" >> "$CHECK_DIR/$LOG_FILE"
echo "$OUT"
exit "$RC"