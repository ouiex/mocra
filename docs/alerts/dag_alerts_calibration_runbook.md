# DAG Alerts Calibration Runbook

## Daily Baseline Report

PowerShell:

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/dag_alerts_baseline.ps1 \
  -PromUrl http://127.0.0.1:9090 \
  -Profile prod \
  -LookbackHours 24 \
  -StepSeconds 60 \
  -Output docs/dashboards/dag_alerts_baseline_latest.md \
  -SummaryJson docs/dashboards/dag_alerts_baseline_latest.json
```

Shell:

```bash
bash scripts/dag_alerts_baseline.sh \
  http://127.0.0.1:9090 \
  prod \
  24 \
  60 \
  docs/dashboards/dag_alerts_baseline_latest.md \
  docs/dashboards/archive \
  dag_alerts_baseline \
  docs/dashboards/dag_alerts_baseline_latest.json
```

## Required Review Items

1. Confirm `Scheduler Ingress Fallback Summary` is present in the markdown report.
2. Confirm `Alert Classification` is present and split into `Cutover Signals` and `Business Failure Signals`.
3. Review `Scheduler Ingress Fallback Hotspots By Path/Reason/Module` and archive the output.
4. Check `scheduler_ingress_fallback_ratio`, `scheduler_ingress_failure_gate_blocked`, and `scheduler_ingress_stage_errors` in the JSON summary.
5. If hotspot rows stay elevated, inspect `GET /control/fallback-gates/{module}` before continuing gray rollout.

## Alert Classification

- `cutover`: scheduler fallback ratio, `failure_gate_blocked`, `scheduler_ingress` stage errors, and snapshot save errors. These indicate compatibility drift, gate blocking, or cutover/control-plane instability.
- `business_failure`: critical DAG alerts plus retryable/non-retryable execution errors. These indicate runtime or data-quality pressure in the business pipeline, not a scheduler cutover-only issue.
- Operator rule: fix `cutover` alerts before widening rollout; fix `business_failure` alerts before interpreting a failed rollout as a scheduler compatibility regression.

## Fallback-Off Baseline

- For the local rehearsal batch, use `scripts/local_probe_whitelist_check.ps1` or `scripts/local_probe_whitelist_check.sh` against `local_probe_module`.
- A passing baseline requires post-dispatch metric deltas that show control-plane dispatch success, DAG scheduler success, downloader success, and no increase in either `mocra_scheduler_ingress_total` or `mocra_stage_errors_total`.
- The local `single-hop-baseline` batch for `local_probe_module` now passes this delta-based check and can serve as the first local fallback-off whitelist batch.
- Keep multi-hop parser/error modules on fallback until they have separate parser-ingress validation evidence.

## Acceptance Evidence

- Latest markdown report: `docs/dashboards/dag_alerts_baseline_latest.md`
- Latest JSON summary: `docs/dashboards/dag_alerts_baseline_latest.json`
- Archived daily report: `docs/dashboards/archive/dag_alerts_baseline_YYYYMMDD_HHMMSS.md`

## Rollback Anchors

| Batch | Scope | Minimal rollback anchor |
|---|---|---|
| P8-B batch-1 | scheduler ingress parser/error typed runtime input | keep `compatibility wrapper -> ParserDispatch/ErrorEnvelope` log chain and `build_legacy_generate_runtime_input` fallback |
| P8-E batch-1 | `Module::generate/parser` scheduler bridge | keep `build_legacy_generate_runtime_input` and `build_legacy_parse_runtime_input` in `module_node_runtime_bridge.rs` |

## Gray Rollout Checks

1. Run the daily baseline report before cutover.
2. Check `GET /control/fallback-gates/{module}` for the module being rolled out.
3. Do not proceed if any `cutover` signal is in `ATTENTION` or if `blocked=true`.
4. If only `business_failure` signals are elevated, hold rollout expansion and investigate module/runtime behavior instead of rolling back cutover by default.
5. Freeze rollout until two consecutive focused regressions or gate runs pass.

## Cutover Rehearsal

PowerShell:

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/cutover_rehearsal.ps1 \
  -PromUrl http://127.0.0.1:9090 \
  -Profile dev \
  -CutInCommand "curl.exe -X POST http://127.0.0.1:8080/control/pause" \
  -RollbackCommand "curl.exe -X POST http://127.0.0.1:8080/control/resume"
```

Shell:

```bash
bash scripts/cutover_rehearsal.sh \
  "http://127.0.0.1:9090" \
  "curl -sS -X POST http://127.0.0.1:8080/control/pause" \
  "curl -sS -X POST http://127.0.0.1:8080/control/resume"
```

## Rollback Procedure

1. Execute the rollback command immediately.
2. Re-run the gate check and archive the report under `docs/dashboards/rehearsal/`.
3. Confirm `GET /control/fallback-gates/{module}` is readable and that the failure reason is captured.
4. Keep the preserved rollback anchors in place until the next batch is verified.