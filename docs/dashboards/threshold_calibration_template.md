# Threshold Calibration Template (T11)

## Purpose

Use this template after pre-production load tests to calibrate alert thresholds for:

- Backpressure metrics
- CAS/Fencing consistency metrics

## Environment

- Date:
- Cluster/Node count:
- Runtime mode: distributed / single_node
- Build/Commit:
- Config snapshot:
  - crawler.publish_concurrency:
  - crawler.parser_concurrency:
  - crawler.error_task_concurrency:
  - crawler.backpressure_retry_delay_ms:
  - channel_config.capacity:
  - channel_config.redis.claim_min_idle:
  - channel_config.redis.claim_interval:
  - channel_config.redis.claim_count:

## Workload Profile

- Test duration:
- Average input QPS:
- Peak input QPS:
- Message mix (% task / parser / error):
- Retry ratio baseline:
- Known failure injection (if any):

## Observed Metrics (5m rate unless noted)

### Backpressure

- request_publish_backpressure_total{reason="queue_full"}:
- request_publish_backpressure_total{reason="queue_closed"}:
- parser_chain_backpressure_total{queue,reason}:
- download_response_backpressure_total{queue,reason}:

### CAS/Fencing

- ptm_claim_total{result="out_of_order"}:
- ptm_claim_total{result="stale"}:
- ptm_commit_total{result="fencing_reject"}:
- ptm_commit_total{result="cas_conflict"}:

## Proposed Thresholds

> Keep two levels where possible: warning and critical.

### Backpressure Alerts

- RequestPublishBackpressureHigh:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min
- ParserChainBackpressureHigh:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min
- DownloadResponseBackpressureHigh:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min
- BackpressureQueueClosedDetected:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min

### CAS/Fencing Alerts

- PtmFencingRejectHigh:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min
- PtmCasConflictHigh:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min
- PtmClaimOutOfOrderHigh:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min
- PtmClaimStaleHigh:
  - warning: > _____ /s for _____ min
  - critical: > _____ /s for _____ min

## Decision and Rationale

- Accepted thresholds:
- Rejected alternatives:
- Trade-offs (noise vs sensitivity):
- Rollout scope (canary/full):

## Follow-up Actions

- [ ] Update docs/alerts/logging_alerts.yml
- [ ] Update docs/alerts/policy_alerts.yml
- [ ] Update dashboard panels if required
- [ ] Attach report in release notes
