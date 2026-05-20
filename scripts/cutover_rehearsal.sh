#!/usr/bin/env bash
set -euo pipefail

PROM_URL="${1:-}"
CUT_IN_COMMAND="${2:-}"
ROLLBACK_COMMAND="${3:-}"
PROFILE="${4:-prod}"
LOOKBACK_HOURS="${5:-2}"
STEP_SECONDS="${6:-30}"
RELEASE_TAG="${7:-$(date -u +%Y%m%d_%H%M%S)}"
OUTPUT_DIR="${8:-docs/dashboards/rehearsal}"

if [[ -z "${PROM_URL}" || -z "${CUT_IN_COMMAND}" || -z "${ROLLBACK_COMMAND}" ]]; then
  echo "usage: $0 <prom_url> <cut_in_command> <rollback_command> [profile=prod] [lookback_hours=2] [step_seconds=30] [release_tag=UTC_yyyymmdd_HHMMSS] [output_dir=docs/dashboards/rehearsal]"
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"
GATE_SUMMARY="${OUTPUT_DIR}/cutover_gate_${RELEASE_TAG}.json"
REPORT="${OUTPUT_DIR}/cutover_rehearsal_${RELEASE_TAG}.md"

STARTED_AT="$(date -u +'%Y-%m-%d %H:%M:%S')"
STATUS="success"
FAILED_STEP=""
ERROR_TEXT=""

run_step() {
  local name="$1"
  local cmd="$2"
  echo "==> ${name}"
  echo "    ${cmd}"
  bash -lc "${cmd}"
}

{
  run_step "1) Cut in scheduler path" "${CUT_IN_COMMAND}"

  run_step "2) Observe gate metrics" \
    "bash scripts/dag_alerts_gate.sh '${PROM_URL}' '${PROFILE}' '${LOOKBACK_HOURS}' '${STEP_SECONDS}' '${RELEASE_TAG}' 'docs/dashboards/dag_alerts_baseline_latest.md' '${GATE_SUMMARY}' 'docs/dashboards/archive'"

  run_step "3) Rollback" "${ROLLBACK_COMMAND}"

  run_step "4) Post-rollback gate check" \
    "bash scripts/dag_alerts_gate.sh '${PROM_URL}' '${PROFILE}' '${LOOKBACK_HOURS}' '${STEP_SECONDS}' '${RELEASE_TAG}_rollback' 'docs/dashboards/dag_alerts_baseline_latest.md' '${GATE_SUMMARY}' 'docs/dashboards/archive'"
} || {
  STATUS="failed"
  FAILED_STEP="cutover_or_gate"
  ERROR_TEXT="command failed"
}

ENDED_AT="$(date -u +'%Y-%m-%d %H:%M:%S')"

cat > "${REPORT}" <<EOF
# Cutover Rehearsal Report (${RELEASE_TAG})

- started_at: ${STARTED_AT}
- ended_at: ${ENDED_AT}
- status: ${STATUS}
- failed_step: ${FAILED_STEP}
- gate_summary_json: ${GATE_SUMMARY}

## Commands

- cut_in: ${CUT_IN_COMMAND}
- rollback: ${ROLLBACK_COMMAND}

## Success Criteria

1. Gate check exits with code 0 after cut-in.
2. Gate check exits with code 0 after rollback.
3. No sustained alert for scheduler fallback high ratio or failure gate blocked.

## Failure Handling

1. Execute rollback command immediately.
2. Re-run gate check with the same release tag and archive output.
3. Keep rollout frozen until two consecutive gate runs pass.
4. Investigate summary json and Prometheus alert history before retry.

## Error

${ERROR_TEXT}
EOF

if [[ "${STATUS}" != "success" ]]; then
  exit 1
fi

echo "Rehearsal report written to ${REPORT}"
