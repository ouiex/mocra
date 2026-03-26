# Mocra Node Metrics Dashboard

This dashboard layout is designed for the rebuilt `mocra_*` node-level metrics.

## Variables

- `node`: `label_values(mocra_node_up, node)`
- `pipeline`: `label_values(mocra_throughput_total{node=~"$node"}, pipeline)`
- `stage`: `label_values(mocra_throughput_total{node=~"$node",pipeline=~"$pipeline"}, stage)`

## Panels

1. Node Health

```promql
mocra_node_up{node=~"$node"}
```

```promql
mocra_component_health{node=~"$node"}
```

2. Resource Usage

```promql
mocra_resource_usage{node=~"$node",resource="cpu_usage_percent"}
```

```promql
mocra_resource_usage{node=~"$node",resource="memory_usage_percent"}
```

```promql
mocra_resource_usage{node=~"$node",resource=~"memory_used_bytes|memory_total_bytes"}
```

3. Throughput

```promql
sum by (stage, operation, result) (
  rate(mocra_throughput_total{node=~"$node",pipeline=~"$pipeline"}[5m])
)
```

4. Latency (P50/P95/P99)

```promql
histogram_quantile(0.50, sum by (le, stage, operation) (rate(mocra_latency_seconds_bucket{node=~"$node",pipeline=~"$pipeline"}[5m])))
```

```promql
histogram_quantile(0.95, sum by (le, stage, operation) (rate(mocra_latency_seconds_bucket{node=~"$node",pipeline=~"$pipeline"}[5m])))
```

```promql
histogram_quantile(0.99, sum by (le, stage, operation) (rate(mocra_latency_seconds_bucket{node=~"$node",pipeline=~"$pipeline"}[5m])))
```

5. Error Breakdown

```promql
sum by (pipeline, stage, kind, code) (
  rate(mocra_errors_total{node=~"$node"}[5m])
)
```

6. Backlog

```promql
max by (queue) (mocra_backlog_depth{node=~"$node",pipeline="queue"})
```

7. Inflight

```promql
mocra_inflight{node=~"$node",pipeline=~"$pipeline",stage=~"$stage"}
```

## Recommended Rows

- Row 1: Health + Resources
- Row 2: Throughput + Latency
- Row 3: Errors + Backlog + Inflight

## SLO Starter Targets

- Availability: `mocra_node_up == 1`
- Error budget: `rate(mocra_errors_total[5m]) / rate(mocra_throughput_total[5m]) < 1%`
- Downloader p95 latency: `< 3s`
- Queue backlog steady state: near zero or bounded by business threshold
