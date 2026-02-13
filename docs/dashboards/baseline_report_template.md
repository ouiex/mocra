# Baseline Report Template (T11)

## Summary

- Report date:
- Version/Commit:
- Environment:
- Final result: PASS / CONDITIONAL PASS / FAIL

## Test Matrix

| Scenario | Target | Observed | Status | Notes |
|---|---:|---:|---|---|
| Normal throughput |  |  |  |  |
| Burst traffic |  |  |  |  |
| Retry storm |  |  |  |  |
| Queue pressure |  |  |  |  |
| CAS/Fencing conflict injection |  |  |  |  |

## Core Metrics Snapshot

### Backpressure

- request_publish_backpressure_total (queue_full):
- request_publish_backpressure_total (queue_closed):
- parser_chain_backpressure_total by queue/reason:
- download_response_backpressure_total by queue/reason:

### CAS/Fencing

- ptm_commit_total (fencing_reject):
- ptm_commit_total (cas_conflict):
- ptm_claim_total (out_of_order):
- ptm_claim_total (stale):

## Alert Validation

| Alert | Triggered | Expected | Verdict |
|---|---|---|---|
| RequestPublishBackpressureHigh |  |  |  |
| ParserChainBackpressureHigh |  |  |  |
| DownloadResponseBackpressureHigh |  |  |  |
| BackpressureQueueClosedDetected |  |  |  |
| PtmFencingRejectHigh |  |  |  |
| PtmCasConflictHigh |  |  |  |
| PtmClaimOutOfOrderHigh |  |  |  |
| PtmClaimStaleHigh |  |  |  |

## Incidents and Mitigations

- Incident #1:
  - Symptom:
  - Root cause:
  - Temporary mitigation:
  - Permanent fix:

## Final Threshold Set

- logging_alerts.yml updates:
- policy_alerts.yml updates:
- Rationale:

## Release Recommendation

- Go / No-Go:
- Required conditions before rollout:
- Follow-up owner and ETA:
