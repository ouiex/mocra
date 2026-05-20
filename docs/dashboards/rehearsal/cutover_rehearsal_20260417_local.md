# Cutover Rehearsal Report (20260417_local)

- started_at: 2026-04-17 10:50:49
- ended_at: 2026-04-17 10:50:52
- status: success
- failed_step: 
- gate_summary_json: docs\dashboards\rehearsal\cutover_gate_20260417_local.json

## Commands

- cut_in: Invoke-WebRequest -UseBasicParsing -Headers @{ 'x-api-key'='local-dev' } -Method POST http://127.0.0.1:8805/control/pause | Out-Null
- rollback: Invoke-WebRequest -UseBasicParsing -Headers @{ 'x-api-key'='local-dev' } -Method POST http://127.0.0.1:8805/control/resume | Out-Null

## Success Criteria

1. Gate check exits with code 0 after cut-in.
2. Gate check exits with code 0 after rollback.
3. No sustained alert for scheduler fallback high ratio or failure gate blocked.

## Threshold Results

| Signal Class | Metric | Result |
|---|---|---|
| cutover | scheduler_ingress_fallback_ratio | OK (avg=0, p95=0, max=0) |
| cutover | scheduler_ingress_failure_gate_blocked | OK (avg=0, p95=0, max=0) |
| cutover | scheduler_ingress_stage_errors | OK (avg=0, p95=0, max=0) |
| cutover | snapshot_save_errors | OK (avg=0, p95=0, max=0) |
| business_failure | critical_events | OK (avg=0, p95=0, max=0) |
| business_failure | warning_events | OK (avg=0, p95=0, max=0) |
| business_failure | non_retryable_errors | OK (avg=0, p95=0, max=0) |
| business_failure | retryable_errors | OK (avg=0, p95=0, max=0) |

## Metric Snapshot Artifacts

- cut-in archive: `docs/dashboards/archive/dag_alerts_baseline_20260417_local_20260417_025050.md`
- rollback archive: `docs/dashboards/archive/dag_alerts_baseline_20260417_local_rollback_20260417_025052.md`
- gate summary json: `docs/dashboards/rehearsal/cutover_gate_20260417_local.json`
- local control plane target: `http://127.0.0.1:8805` with header `x-api-key: local-dev`

## Failure Handling

1. Execute rollback command immediately.
2. Re-run gate check with the same release tag and archive output.
3. Keep rollout frozen until two consecutive gate runs pass.
4. Investigate summary json and Prometheus alert history before retry.

## Error


