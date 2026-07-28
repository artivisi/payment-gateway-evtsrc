#!/usr/bin/env bash
# scenarios/run-benchmark.sh
#
# Produces traceable k6 benchmark artifacts for payment-gateway-evtsrc's real BSI adapter
# workload (scenarios/suite-bsi.js). See docs/benchmark-remediation-guideline.md (G6): every
# number in perf_benchmark_report.md must trace back to a committed file under
# scenarios/results/, not a hand-typed table.
#
# Usage:
#   RUN_ID=$(date +%Y%m%d%H%M%S) BSI_SHARED_SECRET=... ./scenarios/run-benchmark.sh [TARGET_URL]
#
# Required environment variables (fail loud, no defaults):
#   RUN_ID             Unique per invocation. Prefixes every idTransaksi/bankReference this run
#                       generates, so a later audit can filter one run's payments out of an
#                       accumulating table instead of conflating runs.
#   BSI_SHARED_SECRET  Must match the target app's app.bank-secrets.secrets.bsi (BANK_SECRET_BSI
#                       env var on the app). An empty/missing secret makes the checksum trivially
#                       forgeable - this is the exact bug the bsi-adapter stage fixed server-side.
#
# Optional:
#   TARGET_URL (arg 1) Defaults to http://localhost:8080 (evtsrc's default port).
#
# Output (commit these to git, per G6):
#   scenarios/results/<date>-evtsrc-summary.json  (--summary-export: aggregate stats)
#   scenarios/results/<date>-evtsrc-raw.json      (--out json: full per-request time series,
#                                                   including the payment_outcomes custom metric)
#
# To produce the RDBMS-side comparison artifacts, run the sibling payment-gateway repo's own
# equivalent wrapper (or `k6 run scenarios/suite-rdbms.js` directly there) with the SAME RUN_ID and
# BSI_SHARED_SECRET, writing to that repo's scenarios/results/<date>-rdbms-{summary,raw}.json.

set -euo pipefail

if [ -z "${RUN_ID:-}" ]; then
    echo "[ERROR] RUN_ID is required (e.g. RUN_ID=\$(date +%Y%m%d%H%M%S)). No default." >&2
    exit 1
fi

if [ -z "${BSI_SHARED_SECRET:-}" ]; then
    echo "[ERROR] BSI_SHARED_SECRET is required and must match the target app's BANK_SECRET_BSI. No default." >&2
    exit 1
fi

TARGET_URL="${1:-http://localhost:8080}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
DATE_STAMP="$(date +%Y-%m-%d)"
SUMMARY_FILE="$RESULTS_DIR/$DATE_STAMP-evtsrc-summary.json"
RAW_FILE="$RESULTS_DIR/$DATE_STAMP-evtsrc-raw.json"

mkdir -p "$RESULTS_DIR"

echo "=========================================================================="
echo " evtsrc BSI benchmark"
echo " RUN_ID  : $RUN_ID"
echo " target  : $TARGET_URL"
echo " summary : $SUMMARY_FILE"
echo " raw     : $RAW_FILE"
echo "=========================================================================="

k6 run \
    -e RUN_ID="$RUN_ID" \
    -e BSI_SHARED_SECRET="$BSI_SHARED_SECRET" \
    -e TARGET_URL="$TARGET_URL" \
    --summary-export="$SUMMARY_FILE" \
    --out "json=$RAW_FILE" \
    "$SCRIPT_DIR/suite-bsi.js"
