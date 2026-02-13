# Gray Release SOP (T12)

## Purpose

Provide a reversible rollout procedure for unified chains, with explicit stop/rollback criteria.

## Scope

- Unified ingress chain switch-over
- Legacy parser/error chain coexistence period
- Rollback to legacy path within seconds

## Prerequisites

- Backpressure and CAS/Fencing alerts active
- Dashboards available and on-call owner assigned
- Latest threshold calibration and baseline report completed
- Release window and rollback approver confirmed

## Rollout Stages

### Stage A: Parallel Ingress (Canary)

- Traffic split: 5% -> 10% -> 20%
- Duration per step: 30-60 minutes
- Required checks:
  - error rate does not regress beyond agreed threshold
  - queue_closed remains near zero
  - ptm fencing/cas anomaly rates stay within baseline

Stop criteria:

- Any critical alert sustained >= 2 minutes
- Error rate breaches rollback threshold

Action on stop:

- Immediate rollback to legacy ingress
- Freeze rollout and start incident timeline

### Stage B: Responsibility Convergence

- Enable unified responsibility path for parser/error routing
- Keep legacy switches available (do not remove)
- Verify parity on:
  - retry behavior
n- terminate behavior
  - DLQ routing semantics

### Stage C: Threshold Convergence

- Keep strict threshold decisions on ErrorTask chain
- Validate event and metric consistency:
  - TaskTerminatedByThreshold
  - error_task threshold decision counters

### Stage D: Legacy Cleanup

- Decommission legacy parser/error chain only after stable window
- Stable window recommendation: >= 3 days with no rollback

## Operational Checklist

- [ ] Config snapshot stored before rollout
- [ ] Canary percentage documented per stage
- [ ] Alert mute policy reviewed (only non-critical allowed)
- [ ] Rollback command/flag verified before Stage A
- [ ] Incident channel ready

## Rollback Procedure (Seconds-Level)

1. Flip feature/config switches back to legacy chain.
2. Verify new ingress processing rate drops to near zero.
3. Confirm legacy chain throughput and error rates normalize.
4. Keep enhanced observability enabled for postmortem.
5. Record exact timestamps and affected traffic percentages.

## Post-Rollout Validation

- No sustained queue_closed signals
- No sustained fencing_reject/cas_conflict spikes
- Throughput/latency/error-rate within baseline envelope
- No unexplained DLQ growth

## Artifacts to Attach

- Threshold calibration template output
- Baseline report template output
- Alert timeline screenshots
- Final decision log (Go/No-Go)
