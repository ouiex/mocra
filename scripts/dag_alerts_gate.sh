#!/usr/bin/env bash
set -euo pipefail

PROM_URL="${1:-}"
PROFILE="${2:-prod}"
LOOKBACK_HOURS="${3:-24}"
STEP_SECONDS="${4:-60}"
RELEASE_TAG="${5:-$(date -u +%Y%m%d_%H%M%S)}"
OUTPUT="${6:-docs/dashboards/dag_alerts_baseline_latest.md}"
SUMMARY_JSON="${7:-docs/dashboards/dag_alerts_baseline_latest.json}"
ARCHIVE_DIR="${8:-docs/dashboards/archive}"

if [[ -z "${PROM_URL}" ]]; then
  echo "usage: $0 <prom_url> [profile=prod] [lookback_hours=24] [step_seconds=60] [release_tag=UTC_yyyymmdd_HHMMSS] [output=docs/dashboards/dag_alerts_baseline_latest.md] [summary_json=docs/dashboards/dag_alerts_baseline_latest.json] [archive_dir=docs/dashboards/archive]"
  exit 1
fi

ARCHIVE_PREFIX="dag_alerts_baseline_${RELEASE_TAG}"

echo "Running DAG rollout gate baseline (release_tag=${RELEASE_TAG}, profile=${PROFILE})"

bash scripts/dag_alerts_baseline.sh \
  "${PROM_URL}" \
  "${PROFILE}" \
  "${LOOKBACK_HOURS}" \
  "${STEP_SECONDS}" \
  "${OUTPUT}" \
  "${ARCHIVE_DIR}" \
  "${ARCHIVE_PREFIX}" \
  "${SUMMARY_JSON}" \
  "1"
