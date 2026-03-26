# DAG Runtime Alerts Calibration Runbook

## Scope

This runbook defines practical threshold calibration and triage for DAG runtime alerts:

- dag_alert_event_total{source,severity,event,...}
- dag_execute_error_total{run_guard,error_code,error_class}
- dag_run_state_store_total{stage,result}

It is intended for M4 gray rollout and post-cutover steady state.

## Alert Rules Covered

Current alert definitions are in docs/alerts/logging_alerts.yml under group dag_runtime_alerts:

- DagCriticalEventDetected
- DagWarningEventSustained
- DagExecuteErrorNonRetryableHigh
- DagExecuteErrorRetryableStorm
- DagRunStateSnapshotSaveError

## Environment Profiles

Use the following baseline profiles before real traffic tuning.

### DEV

- DagCriticalEventDetected: keep as is, for=1m
- DagWarningEventSustained threshold: > 1.0/s for 5m
- DagExecuteErrorNonRetryableHigh threshold: > 0.5/s for 5m
- DagExecuteErrorRetryableStorm threshold: > 2.0/s for 3m
- DagRunStateSnapshotSaveError: > 0 for 1m

### STAGING

- DagCriticalEventDetected: keep as is, for=2m
- DagWarningEventSustained threshold: > 0.8/s for 8m
- DagExecuteErrorNonRetryableHigh threshold: > 0.3/s for 8m
- DagExecuteErrorRetryableStorm threshold: > 1.5/s for 5m
- DagRunStateSnapshotSaveError: > 0 for 2m

### PROD

- DagCriticalEventDetected: keep as is, for=2m
- DagWarningEventSustained threshold: > 0.5/s for 10m
- DagExecuteErrorNonRetryableHigh threshold: > 0.2/s for 10m
- DagExecuteErrorRetryableStorm threshold: > 1.0/s for 5m
- DagRunStateSnapshotSaveError: > 0 for 3m

## Source and Event Triage Map

### source=dag_run_guard

- event=lost or error or failed_final
- Severity: critical
- Typical causes:
  - lock renew failure
  - transient redis/network instability
  - lock ownership loss under partition
- First actions:
  1. Check redis latency/errors and network packet loss.
  2. Verify run_guard lock key contention trend.
  3. If sustained, pause cutover traffic and keep preview mode.

### source=dag_scheduler

- event maps to error_code (for example RunGuardRenewFailed, FencingTokenRejected, ExecutionTimeout)
- Severity: warning or critical (by event class)
- First actions:
  1. Split by error_class Retryable vs NonRetryable.
  2. If NonRetryable grows, inspect last deployment and rollback scope.
  3. If Retryable storm, tune retry and backpressure controls first.

### source=dag_remote_dispatcher

- event=timeout or remote_execute_failed
- Severity: warning
- Typical causes:
  - worker saturation
  - queue lag and delayed response polling
  - remote handler failures
- First actions:
  1. Split by worker_group.
  2. Correlate with remote worker execute failed totals.
  3. Evaluate short-term scale up and request throttling.

### source=dag_remote_worker

- event=delivery_exhausted or handler_failed
- Severity: warning
- Typical causes:
  - repeated reclaim/retry loops
  - handler runtime errors
- First actions:
  1. Check DLQ growth and handler error logs.
  2. Verify cancel behavior and queue churn.
  3. Reduce retry pressure before broad rollback.

## PromQL Quick Queries

- Critical events by source/event:
  - sum by (source, event) (rate(dag_alert_event_total{severity="critical"}[5m]))
- Warning events by source/event:
  - sum by (source, event) (rate(dag_alert_event_total{severity="warning"}[5m]))
- Execute errors by class:
  - sum by (error_class) (rate(dag_execute_error_total[5m]))
- Execute errors by code:
  - sum by (error_code) (rate(dag_execute_error_total[5m]))
- Snapshot save errors:
  - sum(rate(dag_run_state_store_total{result="save_error"}[5m]))

## Calibration Procedure

1. Run in shadow/preview mode for at least 24 hours in target environment.
2. Generate baseline report from Prometheus:
   - PowerShell:
     - `./scripts/dag_alerts_baseline.ps1 -PromUrl http://127.0.0.1:9090 -Profile prod -LookbackHours 24 -StepSeconds 60 -Output docs/dashboards/dag_alerts_baseline_latest.md -ArchiveDir docs/dashboards/archive -ArchivePrefix dag_alerts_baseline -SummaryJson docs/dashboards/dag_alerts_baseline_latest.json -FailOnGate`
   - Bash:
     - `./scripts/dag_alerts_baseline.sh http://127.0.0.1:9090 prod 24 60 docs/dashboards/dag_alerts_baseline_latest.md docs/dashboards/archive dag_alerts_baseline docs/dashboards/dag_alerts_baseline_latest.json 1`
   - Python:
     - `python scripts/dag_alerts_baseline.py --prom-url http://127.0.0.1:9090 --profile prod --lookback-hours 24 --step-seconds 60 --output docs/dashboards/dag_alerts_baseline_latest.md --archive-dir docs/dashboards/archive --archive-prefix dag_alerts_baseline --summary-json docs/dashboards/dag_alerts_baseline_latest.json --fail-on-gate`
3. Use release-window gate wrappers for operational runs:
   - PowerShell:
     - `./scripts/dag_alerts_gate.ps1 -PromUrl http://127.0.0.1:9090 -Profile prod -LookbackHours 24 -StepSeconds 60 -ReleaseTag gray_20260325_01`
   - Bash:
     - `./scripts/dag_alerts_gate.sh http://127.0.0.1:9090 prod 24 60 gray_20260325_01`
4. Archive one markdown report per release window under docs/dashboards.
5. Record p50 and p95 event rates per source/event.
6. Set warning threshold above normal p95 by 20% to 30%.
7. Keep critical thresholds near zero for lock/fencing classes.
8. Re-check thresholds after each major release and traffic change.
If baseline collection fails and deeper diagnostics are required, add `--debug` to the Python command to print traceback.

## Rollout Gates

- Gate A: zero sustained critical alerts for 24h.
- Gate B: warning alerts within expected baseline bands for 24h.
- Gate C: no increasing trend in NonRetryable execute errors.

## Post-Incident Recording

Record these fields for each incident:

- source and event
- threshold and observed value
- start/end time and duration
- mitigation action
- permanent fix reference
- threshold change made after review
