# Fallback-Off Whitelist Baseline 2026-04-17 Local

## Scope

- Runtime: `target/debug/metrics_node.exe`
- Config: `monitoring/local_engine.toml`
- Control plane: `http://127.0.0.1:8805`
- Validation batch: `single-hop-baseline`
- Representative module: `local_probe_module`

## Validation Method

1. Start the local runtime with `target/debug/metrics_node.exe`.
2. Dispatch one manual task for `local_probe_module` through `POST /tasks/dispatch` with `x-api-key: local-dev`.
3. Capture `/metrics` before and after dispatch, then validate the metric deltas with `scripts/local_probe_whitelist_check.ps1` or `scripts/local_probe_whitelist_check.sh`.

## Required Signals

- `pipeline="control_plane",stage="dispatch",action="dispatch_task",result="success"` delta `>= 1`
- `pipeline="dag",stage="scheduler",action="execute",result="success"` delta `>= 1`
- `pipeline="engine",stage="downloader",action="http_request",result="success"` delta `>= 1`
- `mocra_scheduler_ingress_total` delta `= 0`
- `mocra_stage_errors_total` delta `= 0`

## Local Result

- Dispatch response: `202 Accepted`
- Validation script result: `PASS`
- Metric deltas confirmed control-plane dispatch success
- Metric deltas confirmed DAG scheduler success
- Metric deltas confirmed downloader success
- Metric deltas showed no increase in `mocra_scheduler_ingress_total`
- Metric deltas showed no increase in `mocra_stage_errors_total`

## Current Decision

- Move `single-hop-baseline` into the local fallback-off whitelist as the first approved batch
- Keep multi-hop parser/error modules outside the fallback-off whitelist until they complete a dedicated parser-ingress validation run

## Retained Exit Conditions

- Any increase in `mocra_scheduler_ingress_total` during the baseline dispatch blocks whitelist expansion
- Any increase in `mocra_stage_errors_total` during the baseline dispatch blocks whitelist expansion
- Multi-hop modules require separate parser-ingress evidence before entering the whitelist