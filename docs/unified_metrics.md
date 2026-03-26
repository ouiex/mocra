# Unified Node Metrics System (Prometheus)

This project now uses a rebuilt, node-scoped metrics architecture for a standalone distributed crawler node.

- Prefix: `mocra_`
- Recorder: `metrics` + `metrics-exporter-prometheus`
- Endpoint: `GET /metrics`
- Scope: only current node runtime metrics (no cluster aggregation in-process)

## Rebuilt Architecture

The metrics system is reconstructed around 6 first-class categories:

- Performance
  - resource usage and in-flight load
- Health
  - node and component health states
- Throughput
  - event flow rate by pipeline/stage/operation/result
- Latency
  - stage-level duration distribution
- Errors
  - normalized error count by kind/code
- Backlog
  - queue/topic depth and pending pressure

Core implementation is centralized in `src/common/metrics.rs` and exposed as helper APIs:

- `set_node_up`
- `set_component_health`
- `observe_resource`
- `inc_throughput`
- `observe_latency`
- `inc_error`
- `set_backlog`
- `inc_inflight` / `dec_inflight`

## Unified Metric Families

### 1) Health

- `mocra_node_up{node}` gauge
- `mocra_component_health{node,component}` gauge

### 2) Performance

- `mocra_resource_usage{node,resource}` gauge
- `mocra_inflight{node,pipeline,stage}` gauge

### 3) Throughput

- `mocra_throughput_total{node,pipeline,stage,operation,result}` counter

### 4) Latency

- `mocra_latency_seconds{node,pipeline,stage,operation,result}` histogram

### 5) Errors

- `mocra_errors_total{node,pipeline,stage,kind,code}` counter

### 6) Backlog

- `mocra_backlog_depth{node,pipeline,queue}` gauge

## Node Label Strategy

- `node` label is initialized from `config.name` during engine startup.
- This keeps node identity stable and avoids high-cardinality dynamic IDs.

## Current Reporting Coverage

- Engine startup
  - node up + engine component health
- System monitor
  - CPU/memory/swap resource metrics + monitor health
- Runner
  - in-flight task tracking
- Queue (Redis)
  - producer/consumer/ack/retry/dlq/claim throughput
  - queue backlog depth
- Downloader
  - stage latency + throughput + network error count
- DAG scheduler
  - execute success/error throughput
  - execute latency
  - normalized DAG error metrics

## PromQL Examples

```promql
sum(rate(mocra_throughput_total{pipeline="queue",stage="producer",operation="publish",result="success"}[5m])) by (node)
```

```promql
histogram_quantile(0.95, sum(rate(mocra_latency_seconds_bucket{pipeline="engine",stage="downloader",operation="http_request"}[5m])) by (node, le))
```

```promql
sum(rate(mocra_errors_total{pipeline="dag"}[10m])) by (node, stage, kind, code)
```

```promql
max(mocra_backlog_depth{pipeline="queue"}) by (node, queue)
```

```promql
avg_over_time(mocra_resource_usage{resource="cpu_usage_percent"}[5m]) by (node)
```

## Dashboard Panels (Recommended)

- Throughput: queue producer/consumer success rate
- Latency: downloader p95/p99 + DAG execute p95
- Errors: top error kinds/codes by stage
- Backlog: queue depth heatmap by topic
- Health: node up + component health grid
- Performance: CPU/memory/swap + inflight workload
