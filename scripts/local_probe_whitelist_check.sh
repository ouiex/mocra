#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8805}"
API_KEY="${API_KEY:-local-dev}"
ACCOUNT="${ACCOUNT:-local}"
PLATFORM="${PLATFORM:-monitoring}"
MODULE="${MODULE:-local_probe_module}"
MAX_POLL_ATTEMPTS="${MAX_POLL_ATTEMPTS:-40}"

metric_total() {
  local metrics="$1"
  local pattern="$2"
  awk -v pattern="$pattern" '$0 ~ pattern {sum += $NF} END {if (sum == "") sum = 0; printf "%.10f", sum}' <<<"${metrics}"
}

snapshot_value() {
  local metrics="$1"
  local key="$2"
  case "$key" in
    dispatch_success)
      metric_total "$metrics" '^mocra_stage_events_total\{[^}]*pipeline="control_plane",stage="dispatch",action="dispatch_task",result="success"[^}]*\}[[:space:]][0-9]+(\.[0-9]+)?$'
      ;;
    dag_scheduler_success)
      metric_total "$metrics" '^mocra_stage_events_total\{[^}]*pipeline="dag",stage="scheduler",action="execute",result="success"[^}]*\}[[:space:]][0-9]+(\.[0-9]+)?$'
      ;;
    downloader_success)
      metric_total "$metrics" '^mocra_stage_events_total\{[^}]*pipeline="engine",stage="downloader",action="http_request",result="success"[^}]*\}[[:space:]][0-9]+(\.[0-9]+)?$'
      ;;
    scheduler_ingress_total)
      metric_total "$metrics" '^mocra_scheduler_ingress_total(\{[^}]*\})?[[:space:]][0-9]+(\.[0-9]+)?$'
      ;;
    stage_errors_total)
      metric_total "$metrics" '^mocra_stage_errors_total(\{[^}]*\})?[[:space:]][0-9]+(\.[0-9]+)?$'
      ;;
  esac
}

delta_ge_one() {
  awk -v before="$1" -v after="$2" 'BEGIN { exit !((after - before) >= 1) }'
}

delta_eq_zero() {
  awk -v before="$1" -v after="$2" 'BEGIN { exit !((after - before) == 0) }'
}

delta_gt_zero() {
  awk -v before="$1" -v after="$2" 'BEGIN { exit !((after - before) > 0) }'
}

before_metrics="$(curl -sS "${BASE_URL}/metrics")"
before_dispatch="$(snapshot_value "$before_metrics" dispatch_success)"
before_scheduler="$(snapshot_value "$before_metrics" dag_scheduler_success)"
before_downloader="$(snapshot_value "$before_metrics" downloader_success)"
before_scheduler_ingress="$(snapshot_value "$before_metrics" scheduler_ingress_total)"
before_stage_errors="$(snapshot_value "$before_metrics" stage_errors_total)"

curl -sS \
  -H "x-api-key: ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d "{\"account\":\"${ACCOUNT}\",\"platform\":\"${PLATFORM}\",\"module\":[\"${MODULE}\"],\"priority\":\"normal\"}" \
  "${BASE_URL}/tasks/dispatch" >/dev/null

after_dispatch="$before_dispatch"
after_scheduler="$before_scheduler"
after_downloader="$before_downloader"
after_scheduler_ingress="$before_scheduler_ingress"
after_stage_errors="$before_stage_errors"

for ((attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++)); do
  metrics="$(curl -sS "${BASE_URL}/metrics")"
  after_dispatch="$(snapshot_value "$metrics" dispatch_success)"
  after_scheduler="$(snapshot_value "$metrics" dag_scheduler_success)"
  after_downloader="$(snapshot_value "$metrics" downloader_success)"
  after_scheduler_ingress="$(snapshot_value "$metrics" scheduler_ingress_total)"
  after_stage_errors="$(snapshot_value "$metrics" stage_errors_total)"

  if delta_gt_zero "$before_scheduler_ingress" "$after_scheduler_ingress" || delta_gt_zero "$before_stage_errors" "$after_stage_errors"; then
    break
  fi

  if delta_ge_one "$before_dispatch" "$after_dispatch" \
    && delta_ge_one "$before_scheduler" "$after_scheduler" \
    && delta_ge_one "$before_downloader" "$after_downloader"; then
    break
  fi
done

has_dispatch_success=false
has_scheduler_success=false
has_downloader_success=false
has_scheduler_ingress=false
has_stage_errors=false

delta_ge_one "$before_dispatch" "$after_dispatch" && has_dispatch_success=true || true
delta_ge_one "$before_scheduler" "$after_scheduler" && has_scheduler_success=true || true
delta_ge_one "$before_downloader" "$after_downloader" && has_downloader_success=true || true
delta_gt_zero "$before_scheduler_ingress" "$after_scheduler_ingress" && has_scheduler_ingress=true || true
delta_gt_zero "$before_stage_errors" "$after_stage_errors" && has_stage_errors=true || true

if ${has_dispatch_success} && ${has_scheduler_success} && ${has_downloader_success} && ! ${has_scheduler_ingress} && ! ${has_stage_errors}; then
  printf '{"status":"PASS","module":"%s"}\n' "${MODULE}"
else
  printf '{"status":"FAIL","module":"%s","dispatch_success":%s,"dag_scheduler_success":%s,"downloader_success":%s,"scheduler_ingress_absent":%s,"stage_errors_absent":%s}\n' \
    "${MODULE}" \
    "${has_dispatch_success}" \
    "${has_scheduler_success}" \
    "${has_downloader_success}" \
    "$([ "${has_scheduler_ingress}" = false ] && echo true || echo false)" \
    "$([ "${has_stage_errors}" = false ] && echo true || echo false)"
  exit 1
fi