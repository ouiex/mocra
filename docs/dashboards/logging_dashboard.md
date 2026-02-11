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

## Recommended Alerts
- LogDropRateHigh: drop ratio > 0.5% for 10m
- LogQueueLagHigh: log_queue_lag > 5000 for 5m
- LogSinkErrorsHigh: error rate > 1/s for 5m
