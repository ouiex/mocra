# DAG Alerts Baseline Report

- Generated at (UTC): 2026-04-17T02:50:52Z
- Prometheus URL: http://127.0.0.1:9090
- Profile: dev
- Range: 2026-04-17T00:50:52Z to 2026-04-17T02:50:52Z
- Step: 30s

## Summary

| Metric | Avg | P95 | Max | Verdict |
|---|---:|---:|---:|---|
| critical_events | 0.0000 | 0.0000 | 0.0000 | PASS |
| warning_events | 0.0000 | 0.0000 | 0.0000 | PASS |
| non_retryable_errors | 0.0000 | 0.0000 | 0.0000 | PASS |
| retryable_errors | 0.0000 | 0.0000 | 0.0000 | PASS |
| snapshot_save_errors | 0.0000 | 0.0000 | 0.0000 | PASS |

## Alert Classification

### Cutover Signals

| Metric | Avg | P95 | Max | Status |
|---|---:|---:|---:|---|
| scheduler_ingress_fallback_ratio | 0.0000 | 0.0000 | 0.0000 | OK |
| scheduler_ingress_failure_gate_blocked | 0.0000 | 0.0000 | 0.0000 | OK |
| scheduler_ingress_stage_errors | 0.0000 | 0.0000 | 0.0000 | OK |
| snapshot_save_errors | 0.0000 | 0.0000 | 0.0000 | OK |

### Business Failure Signals

| Metric | Avg | P95 | Max | Status |
|---|---:|---:|---:|---|
| critical_events | 0.0000 | 0.0000 | 0.0000 | OK |
| warning_events | 0.0000 | 0.0000 | 0.0000 | OK |
| non_retryable_errors | 0.0000 | 0.0000 | 0.0000 | OK |
| retryable_errors | 0.0000 | 0.0000 | 0.0000 | OK |

## Critical Events By Source/Event

| n/a | 0.0000 |
|---|---:|

## Warning Events By Source/Event

| n/a | 0.0000 |
|---|---:|

## Execute Errors By Code/Class

| n/a | 0.0000 |
|---|---:|

## Run-State Save Errors By Stage

| n/a | 0.0000 |
|---|---:|

## Scheduler Ingress Fallback Summary

| Metric | Avg | P95 | Max |
|---|---:|---:|---:|
| scheduler_ingress_fallback_ratio | 0.0000 | 0.0000 | 0.0000 |
| scheduler_ingress_failure_gate_blocked | 0.0000 | 0.0000 | 0.0000 |
| scheduler_ingress_stage_errors | 0.0000 | 0.0000 | 0.0000 |

## Scheduler Ingress Fallback Hotspots By Path/Reason/Module

| n/a | 0.0000 |
|---|---:|

## Scheduler Ingress Result Mix By Path

| n/a | 0.0000 |
|---|---:|

## Calibration Notes

- Compare Avg/P95 with docs/alerts/dag_alerts_calibration_runbook.md environment profile.
- If critical_events max > 0, treat as an immediate release block in gray rollout.
- If warning/non-retryable stays above profile for 24h, keep preview mode and postpone cutover.
- Scheduler ingress fallback daily report must retain hotspot rows grouped by path/reason/module for operator review.
- Treat scheduler fallback / failure gate / scheduler_ingress stage errors as cutover signals; treat critical/warning/non-retryable/retryable as business failure signals.
