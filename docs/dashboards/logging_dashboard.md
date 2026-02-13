# Logging Metrics Dashboard (PromQL)

## Throughput
- Log events per second:
  - `sum(rate(log_events_total[1m]))`
- Log dropped per second:
  - `sum(rate(log_dropped_total[1m]))`
- Drop ratio:
  - `sum(rate(log_dropped_total[5m])) / (sum(rate(log_events_total[5m])) + sum(rate(log_dropped_total[5m])))`

## Queue Health
- Log queue lag (max):
  - `max(log_queue_lag)`
- Log batch size (avg):
  - `avg(log_batch_size)`

## Sink Errors
- Sink error rate:
  - `sum(rate(log_sink_errors_total[5m]))`

## Pipeline Backpressure
- Request publish backpressure rate:
  - `sum by (reason) (rate(request_publish_backpressure_total[5m]))`
- Parser chain backpressure rate:
  - `sum by (queue, reason) (rate(parser_chain_backpressure_total[5m]))`
- Download response backpressure rate:
  - `sum by (queue, reason) (rate(download_response_backpressure_total[5m]))`
- Queue closed signal (all pipelines):
  - `sum(rate(request_publish_backpressure_total{reason="queue_closed"}[5m])) + sum(rate(parser_chain_backpressure_total{reason="queue_closed"}[5m])) + sum(rate(download_response_backpressure_total{reason="queue_closed"}[5m]))`

## CAS and Fencing Integrity
- PTM claim result rates:
  - `sum by (result) (rate(ptm_claim_total[5m]))`
- PTM commit result rates:
  - `sum by (result) (rate(ptm_commit_total[5m]))`
- Fencing reject rate:
  - `sum(rate(ptm_commit_total{result="fencing_reject"}[5m]))`
- CAS conflict rate:
  - `sum(rate(ptm_commit_total{result="cas_conflict"}[5m]))`
- Out-of-order claim rate:
  - `sum(rate(ptm_claim_total{result="out_of_order"}[5m]))`

## Recommended Alerts
- LogDropRateHigh: drop ratio > 0.5% for 10m
- LogQueueLagHigh: log_queue_lag > 5000 for 5m
- LogSinkErrorsHigh: error rate > 1/s for 5m
- RequestPublishBackpressureHigh: request publish queue_full rate > 5/s for 5m
- ParserChainBackpressureHigh: parser queue_full rate > 5/s for 5m
- DownloadResponseBackpressureHigh: response queue_full rate > 5/s for 5m
- BackpressureQueueClosedDetected: queue_closed signal持续 2m
- PtmFencingRejectHigh: fencing_reject rate > 1/s for 5m
- PtmCasConflictHigh: cas_conflict rate > 2/s for 5m
- PtmClaimOutOfOrderHigh: out_of_order rate > 2/s for 5m
- PtmClaimStaleHigh: stale rate > 2/s for 5m

## Runbook
- Backpressure triage and mitigation:
  - [docs/alerts/backpressure_runbook.md](docs/alerts/backpressure_runbook.md)
- CAS/fencing triage and mitigation:
  - [docs/alerts/cas_fencing_runbook.md](docs/alerts/cas_fencing_runbook.md)

## Calibration and Baseline Templates
- Threshold calibration template:
  - [docs/dashboards/threshold_calibration_template.md](docs/dashboards/threshold_calibration_template.md)
- Baseline report template:
  - [docs/dashboards/baseline_report_template.md](docs/dashboards/baseline_report_template.md)
