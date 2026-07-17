# Deployment Guide

This guide covers running mocra in production: a zero-infrastructure single node, an embedded Raft
cluster, cross-node data-plane queues, and monitoring.

> **中文版：** [docs/zh/deployment.md](zh/deployment.md)

mocra scales along two independent axes — the **control plane** (coordination) and the **data
plane** (the message queue). Pick a topology below; see [Architecture](architecture.md) for how the
pieces fit together.

## Single-node (zero infrastructure)

The default. No database, no external services, no message broker — the facade builds an in-memory,
single-process engine.

### When to use

- Development and testing
- One-shot scrapes and standalone scripts
- Low-volume collection that fits in one process

### Setup

Depend on mocra and run a spider — that is the whole deployment:

```toml
[dependencies]
mocra = "0.4"
```

```rust
use mocra::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(MySpider, on_item(|item: Item| async move {
            // persist / print / forward
        }))
        .run()          // no .from_toml, no .cluster → in-memory single node
        .await
}
```

```bash
cargo run --release
```

### What happens

- Queues are in-process Tokio mpsc channels.
- Coordination is in-process (no distributed locks).
- Without `.from_toml`, the builder auto-seeds each spider and **stops when idle** — perfect for
  one-shot runs. A spider that keeps producing tasks keeps running.

No Postgres and no external services are required. The account × platform × module database model is opt-in via
the `store` feature and the lower-level `ModuleTrait` path — the `Spider` facade does not need it.

## Embedded cluster (`cluster-embedded`)

Run a **self-organizing Raft cluster** whose control plane is an embedded **redb + Raft** — leader
election, fenced distributed locks, membership, and partition ownership with **no external
coordinator**.

```toml
mocra = { version = "0.4", features = ["cluster-embedded"] }
```

### Registering nodes

The first node **bootstraps** a new cluster; every other node **joins** through any known node's
address (a "seed"). Register any node to any node already in the network:

```rust
// First core node — bootstraps a new cluster (no seeds)
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;

// Any additional node joins via a seed address
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::join(2, "127.0.0.1:7002", "./data/n2", "127.0.0.1:7001"))
    .run().await?;
```

`ClusterConfig::bootstrap(node_id, http_addr, data_dir)` starts a new cluster; `ClusterConfig::join`
adds the seed address of any existing node. `data_dir` is the redb state-machine + Raft log
directory, so give each node its own path.

Run the bundled example to bring up three nodes (three terminals — the first bootstraps, the rest
join through it):

```bash
# First core node (no seed → bootstrap)
cargo run --example cluster_quickstart --features cluster-embedded -- 1 127.0.0.1:7001
# Additional nodes (third arg = seed = first node's address → join)
cargo run --example cluster_quickstart --features cluster-embedded -- 2 127.0.0.1:7002 127.0.0.1:7001
cargo run --example cluster_quickstart --features cluster-embedded -- 3 127.0.0.1:7003 127.0.0.1:7001
```

### Containerized deployment

For the same image across nodes, use `ClusterConfig::from_env()` and vary only environment
variables:

| Variable | Meaning |
|---|---|
| `MOCRA_NODE_ID` | Unique node id (u64, required) |
| `MOCRA_HTTP_ADDR` | This node's advertised address, e.g. `10.0.0.4:7001` (required) |
| `MOCRA_DATA_DIR` | redb + Raft directory (default `./mocra-data/node-{id}`) |
| `MOCRA_SEEDS` | Comma-separated seed addresses; empty = bootstrap, non-empty = join |

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::from_env()?)
    .run().await?;
```

### Data plane

The embedded cluster is only the control plane. The **queue backend is chosen independently**:

- **In-memory** (default) — the in-process queue does not cross nodes, so seeded work stays on the node that produced it.
- **Kafka / NATS JetStream** — tasks fan out across nodes, routed by `hash(account)`
  for consumer affinity (same account → same consumer). Enable `queue-kafka` or `queue-nats` and
  configure the backend in your TOML (see [Configuration](configuration.md)).

## Cross-node data plane (Kafka / NATS)

The embedded cluster is the control plane; the in-memory queue never leaves the process that
produced the work. To fan tasks out across nodes, point the **data-plane queue** at a shared
broker — Kafka (`queue-kafka`) or NATS JetStream (`queue-nats`) — by loading a TOML config with
`.from_toml(cfg)`. Tasks are routed by `hash(account)` for consumer affinity (same account → same
consumer):

```toml
# config.toml — Kafka as the data-plane queue
[channel_config.kafka]
brokers = "kafka-host:9092"

# or NATS JetStream
# [channel_config.nats]
# url = "nats://nats-host:4222"
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .from_toml("config.toml")            // data-plane queue (Kafka / NATS)
    .cluster(ClusterConfig::from_env()?) // control plane (leader election, locks)
    .run().await?;
```

```bash
# Same binary, same config.toml, on each node
cargo run --release --features "cluster-embedded queue-kafka"
```

See [Configuration](configuration.md) for the full `[channel_config]` reference.

## Choosing a topology

| | Single-node | Embedded cluster |
|---|---|---|
| Feature flag | none | `cluster-embedded` |
| External infra | **none** | **none** |
| Coordination | in-process | redb + Raft |
| Enable via | facade default | `.cluster(ClusterConfig::…)` |
| Data-plane queue | in-memory | in-memory / Kafka / NATS |
| Best for | dev, one-shot, low volume | self-contained clusters, cross-node scale |

## Monitoring

### Dashboard, metrics, and web UI

Enable the `dashboard` feature and call `.dashboard(port)`. The engine hosts a read-only
observability HTTP API **and** a built-in single-file web UI — open the port in a browser for
**metrics / logs / tasks / performance**, no frontend build required:

```toml
mocra = { version = "0.4", features = ["dashboard"] }
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .dashboard(12800)   // GET / → web UI;  /metrics,  /health,  /observability/*
    .run().await?;
```

```bash
cargo run --example dashboard --features dashboard   # then open http://127.0.0.1:12800
```

Endpoints:

| Route | Auth | Description |
|---|---|---|
| `GET /` | none | Built-in dashboard web UI |
| `GET /metrics` | none (rate-limited) | Prometheus metrics (`mocra_*` families) |
| `GET /health` | none | Health / liveness probe |
| `GET /observability/engine` | none | Engine + per-queue snapshot |
| `GET /observability/cluster` | none | Raft cluster status (`null` when standalone) |
| `GET /observability/system` | none | Host CPU / memory / swap |
| `GET /observability/logs?limit=N` | none | Recent structured logs |
| `POST /control/pause` \| `resume` | API key | Pause / resume the engine |
| `POST /start_work` | API key | Inject a manual task |

The read-only endpoints are CORS-enabled and need no API key (a standalone frontend can consume them
cross-origin); write endpoints stay authenticated. See the [API Reference](api-reference.md) for
details.

### Prometheus + Grafana

The repository ships a monitoring stack. `docker-compose.monitoring.yml` runs Prometheus and Grafana;
Prometheus scrapes your engine's `/metrics` (the scrape target lives in
`monitoring/prometheus/prometheus.yml`, reachable from the container via `host.docker.internal`):

```bash
docker compose -f docker-compose.monitoring.yml up -d

# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000   (admin / admin)
# Metrics:    http://localhost:12800/metrics   (your .dashboard(port))
```

Provisioned Grafana datasources and dashboards live under `monitoring/grafana/provisioning`.

### Logging

mocra uses the `log` / `tracing` ecosystem. Configure with `RUST_LOG`:

```bash
RUST_LOG=mocra=info cargo run          # or mocra=debug for more detail
```

With `.dashboard(...)`, recent logs are also captured into an in-memory ring buffer and served at
`GET /observability/logs`.

## Production checklist

- [ ] Pick a topology: single-node or embedded cluster (`cluster-embedded`).
- [ ] For a cluster, give each node a unique `node_id` and its own `data_dir`; seed additional nodes
      through a known address (or `ClusterConfig::from_env()` in containers).
- [ ] Choose the data-plane queue: in-memory, Kafka (`queue-kafka`), or NATS
      JetStream (`queue-nats`).
- [ ] Enable `dashboard` and expose `/metrics`; scrape it with Prometheus and chart with Grafana.
- [ ] Set `RUST_LOG` to an appropriate level.
- [ ] If you inject tasks or pause/resume in production, protect the write endpoints with an API key
      (`[api] api_key`).
- [ ] Tune retries / error thresholds and rate limits in your TOML (`[crawler]`,
      `[download_config]`) — see [Configuration](configuration.md).
- [ ] Keep the `mimalloc` feature on for production builds (it is a default feature).

## Related guides

- [Architecture](architecture.md) — control plane vs data plane, workspace layout
- [Configuration](configuration.md) — full TOML reference (`[cache]`, `[channel_config]`, `[api]`)
- [API Reference](api-reference.md) — HTTP control plane and metrics
- [Getting Started](getting-started.md) — installation and first spider
