# Backpressure Runbook

## Scope

This runbook covers pipeline backpressure signals introduced in T10/T11:

- `request_publish_backpressure_total{reason=...}`
- `parser_chain_backpressure_total{queue=...,reason=...}`
- `download_response_backpressure_total{queue=...,reason=...}`

Critical signal:

- `BackpressureQueueClosedDetected`

## Quick Triage (5 minutes)

1. Confirm alert source and queue dimension:
   - Request publish path (`request_publish_backpressure_total`)
   - Parser chain path (`parser_chain_backpressure_total`)
   - Download response path (`download_response_backpressure_total`)
2. Split by reason:
   - `queue_full`: sustained throughput pressure
   - `queue_closed`: consumer/sender lifecycle fault or shutdown ripple
3. Check burst vs sustained:
   - burst (<2m) can be transient
   - sustained (>=5m) requires mitigation

## PromQL Checklist

- Request publish rate by reason:
  - `sum by (reason) (rate(request_publish_backpressure_total[5m]))`
- Parser chain by queue/reason:
  - `sum by (queue, reason) (rate(parser_chain_backpressure_total[5m]))`
- Download response by queue/reason:
  - `sum by (queue, reason) (rate(download_response_backpressure_total[5m]))`
- Global closed-signal:
  - `sum(rate(request_publish_backpressure_total{reason="queue_closed"}[5m])) + sum(rate(parser_chain_backpressure_total{reason="queue_closed"}[5m])) + sum(rate(download_response_backpressure_total{reason="queue_closed"}[5m]))`

## Decision Tree

### A) `queue_full` dominates

Likely cause:

- local channel saturation under burst traffic
- publish side outpaces consumer side

Actions:

1. Verify current `crawler.publish_concurrency` and worker count.
2. Reduce ingress pressure temporarily (rate limit / batch size / traffic gate).
3. Increase consumer capacity where safe.
4. Tune `crawler.backpressure_retry_delay_ms` to reduce retry amplification.

Exit criteria:

- `queue_full` rates fall below alert threshold for 10+ minutes.

### B) `queue_closed` present

Likely cause:

- queue worker terminated/restarted
- lifecycle ordering issues during shutdown/reload

Actions:

1. Inspect recent process restarts and deployment timeline.
2. Check for panic/fatal errors in related processors.
3. Confirm channel ownership/lifecycle in startup/shutdown sequence.
4. If continuous, rollback latest rollout and isolate change set.

Exit criteria:

- `queue_closed` rates return to zero and remain zero for 10+ minutes.

## Configuration Knobs

- `crawler.publish_concurrency`
- `crawler.parser_concurrency`
- `crawler.error_task_concurrency`
- `crawler.backpressure_retry_delay_ms`

## Post-Incident Notes

Record:

- Alert fired time and duration
- Dominant metric and labels (`queue`, `reason`)
- Temporary mitigations applied
- Permanent fix PR/commit and config deltas
- Follow-up test added (if applicable)
