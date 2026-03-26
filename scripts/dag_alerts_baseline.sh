#!/usr/bin/env bash
set -euo pipefail

PROM_URL="${1:-}"
PROFILE="${2:-prod}"
LOOKBACK_HOURS="${3:-24}"
STEP_SECONDS="${4:-60}"
OUTPUT="${5:-docs/dashboards/dag_alerts_baseline_latest.md}"
ARCHIVE_DIR="${6:-docs/dashboards/archive}"
ARCHIVE_PREFIX="${7:-dag_alerts_baseline}"
SUMMARY_JSON="${8:-}"
FAIL_ON_GATE="${9:-0}"

if [[ -z "${PROM_URL}" ]]; then
  echo "usage: $0 <prom_url> [profile=prod] [lookback_hours=24] [step_seconds=60] [output=docs/dashboards/dag_alerts_baseline_latest.md] [archive_dir=docs/dashboards/archive] [archive_prefix=dag_alerts_baseline] [summary_json=''] [fail_on_gate=0|1]"
  exit 1
fi

echo "Collecting DAG alert baseline from ${PROM_URL} (profile=${PROFILE}, lookback=${LOOKBACK_HOURS}h, step=${STEP_SECONDS}s)"

CMD=(
  python3 scripts/dag_alerts_baseline.py
  --prom-url "${PROM_URL}"
  --profile "${PROFILE}"
  --lookback-hours "${LOOKBACK_HOURS}"
  --step-seconds "${STEP_SECONDS}"
  --output "${OUTPUT}"
  --archive-dir "${ARCHIVE_DIR}"
  --archive-prefix "${ARCHIVE_PREFIX}"
)

if [[ -n "${SUMMARY_JSON}" ]]; then
  CMD+=(--summary-json "${SUMMARY_JSON}")
fi
if [[ "${FAIL_ON_GATE}" == "1" ]]; then
  CMD+=(--fail-on-gate)
fi

"${CMD[@]}"
