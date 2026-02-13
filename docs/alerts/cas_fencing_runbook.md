# CAS and Fencing Runbook

## Scope

This runbook covers distributed chain consistency signals for parser task model (PTM):

- ptm_claim_total{result=...}
- ptm_commit_total{result=...}

Key failure-oriented labels:

- claim side: stale, out_of_order
- commit side: fencing_reject, cas_conflict

## Quick Triage (5 minutes)

1. Identify which metric family is dominant:
   - claim anomalies (`ptm_claim_total`)
   - commit anomalies (`ptm_commit_total`)
2. Confirm if anomalies are burst or sustained:
   - burst can happen during deployment or consumer rebalance
   - sustained indicates ordering or ownership consistency issues
3. Correlate with recent events:
   - rollout/restart window
   - queue claim/retry storms
   - redis/network instability

## PromQL Checklist

- Claim results by type:
  - sum by (result) (rate(ptm_claim_total[5m]))
- Commit results by type:
  - sum by (result) (rate(ptm_commit_total[5m]))
- Fencing reject rate:
  - sum(rate(ptm_commit_total{result="fencing_reject"}[5m]))
- CAS conflict rate:
  - sum(rate(ptm_commit_total{result="cas_conflict"}[5m]))
- Out-of-order claim rate:
  - sum(rate(ptm_claim_total{result="out_of_order"}[5m]))
- Stale claim rate:
  - sum(rate(ptm_claim_total{result="stale"}[5m]))

## Decision Tree

### A) fencing_reject is high

Likely cause:

- lease owner/version drift between claim and commit
- stale worker commits after ownership moved

Actions:

1. Check deploy/restart timeline around the spike.
2. Confirm lease timeout settings are aligned with processing duration.
3. Validate ownership keys and version increments in Redis.
4. Reduce long-running task variance or split oversized parser batches.

Exit criteria:

- fencing_reject rate returns to near zero and remains stable for 10+ minutes.

### B) cas_conflict is high

Likely cause:

- concurrent commits on same task/module step
- retry overlap causing commit races

Actions:

1. Check duplicate message/retry amplification signs in queue metrics.
2. Inspect parser task retry path for repeated concurrent execution.
3. Increase backpressure retry delay if retry storm observed.
4. Verify advance/fallback gate transitions remain monotonic.

Exit criteria:

- cas_conflict rate decreases below threshold and no sustained growth trend.

### C) out_of_order or stale claim is high

Likely cause:

- queue ordering pressure or delayed claims
- consumer lag / rebalance / idle-claim timing mismatch

Actions:

1. Check stream lag and claim interval settings.
2. Inspect queue consumer health and restart churn.
3. Tune claim_min_idle, claim_interval, claim_count conservatively.
4. If recent rollout exists, canary rollback and compare rates.

Exit criteria:

- out_of_order/stale rates return to baseline and align with historical range.

## Configuration Knobs

- channel_config.redis.claim_min_idle
- channel_config.redis.claim_interval
- channel_config.redis.claim_count
- crawler.backpressure_retry_delay_ms
- crawler.publish_concurrency

## Post-Incident Notes

Record:

- Dominant anomaly type and metric values
- Redis and queue health at incident time
- Config changes applied and rollback checkpoints
- Commit/PR references for permanent fix
- Added regression test (if any)
