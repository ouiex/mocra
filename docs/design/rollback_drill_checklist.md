# Rollback Drill Checklist (T12)

## Drill Goal

Prove that unified chain rollout can be reverted quickly and safely with data-path consistency preserved.

## Roles

- Incident commander:
- Operator (switch execution):
- Observer (metrics + logs):
- Recorder (timeline + evidence):

## Pre-Drill Setup

- [ ] Confirm current rollout stage and active traffic percentage
- [ ] Confirm rollback switches/flags and expected values
- [ ] Confirm dashboards and alert panels open
- [ ] Confirm baseline thresholds available
- [ ] Confirm message queues and Redis health are normal

## Drill Scenario Matrix

### Scenario 1: Backpressure escalation

Trigger:

- Simulated sustained queue_full increase

Expected:

- Rollback initiated within 2 minutes
- queue_closed stays near zero

### Scenario 2: CAS/Fencing anomaly

Trigger:

- Simulated ptm fencing_reject/cas_conflict spike

Expected:

- Rollback decision based on threshold
- anomaly rate declines post-rollback

### Scenario 3: Mixed degradation

Trigger:

- Backpressure + CAS anomalies together

Expected:

- Priority follows critical signal policy
- rollback completes without message-path collapse

## Execution Checklist

- [ ] Announce drill start time
- [ ] Record pre-rollback metric snapshots
- [ ] Execute rollback switch
- [ ] Validate legacy path traffic recovery
- [ ] Validate new path traffic drop
- [ ] Check DLQ trend and queue lag
- [ ] Check ptm claim/commit anomaly trends
- [ ] Announce drill end time

## Success Criteria

- Rollback completed within target SLO: _____ seconds
- No sustained queue_closed after rollback
- No abnormal DLQ spike after rollback
- Error rate returns to pre-drill envelope
- Core processing throughput recovers to >= _____% baseline

## Failure Handling

If any criterion fails:

1. Keep legacy path active.
2. Escalate to incident workflow.
3. Capture logs, metrics, and config diffs.
4. Create remediation task and block next rollout stage.

## Evidence Collection

- Metric screenshots before/after rollback
- Alert timeline
- Config diff and switch values
- Operator command log
- Post-drill summary and action items
