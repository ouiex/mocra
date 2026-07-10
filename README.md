<div align="center">

# mocra

**A distributed, event-driven crawling and data collection framework for Rust.**

[![Crates.io](https://img.shields.io/crates/v/mocra.svg)](https://crates.io/crates/mocra)
[![docs.rs](https://docs.rs/mocra/badge.svg)](https://docs.rs/mocra)
[![License](https://img.shields.io/crates/l/mocra.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

English | [中文](README.zh.md)

</div>

---

mocra is a Rust framework for building scalable data collection pipelines. It models crawling as **queue-driven processing stages** orchestrated by a **DAG execution engine**, with automatic scaling from single-process to distributed clusters.

## Features

- **DAG-based module system** — define linear chains or custom fan-out/fan-in graphs
- **Queue-driven pipeline** — task → request → download → parse → store, fully decoupled
- **Auto-scaling runtime** — single-node (in-memory) or distributed (Redis/Kafka) with zero code changes
- **Bounded concurrency** — semaphore-controlled worker pools with pause/resume/shutdown
- **Middleware pipeline** — download, data transformation, and storage middleware with weight-based ordering
- **Pluggable downloaders** — the default is reqwest; swap it wholesale (`.default_downloader()`) or register per-module downloaders (`.downloader()`) for browser rendering, proxy rotation, or custom retry
- **Admin dashboard** — enable the `dashboard` feature for a read-only observability HTTP API **and** a built-in single-file web UI (metrics / logs / tasks / performance) — no frontend build required
- **Built-in control plane** — HTTP API for health, metrics, pause/resume, task injection, and DLQ inspection
- **Prometheus metrics** — unified `mocra_*` metric families for throughput, latency, errors, and backlog
- **Cron scheduling** — periodic task execution with cron expressions
- **Error recovery** — policy-driven retry, fallback gates, circuit breakers, and dead-letter queues

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
mocra = "0.2"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

Implement a `Spider` and run it — **no database, no Redis, three steps** (single-node, in-memory):

```rust
use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Page { url: String, status: u16 }

struct Httpbin;

#[async_trait]
impl Spider for Httpbin {
    type Item = Page;
    fn name(&self) -> &str { "httpbin" }

    async fn start(&self, s: &mut Seeds) {
        s.get("https://httpbin.org/get");
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit(Page { url: res.module_id(), status: res.status_code });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(Httpbin, on_item(|p: Page| async move {
            println!("[item] {} -> {}", p.url, p.status);
        }))
        .run()
        .await
}
```

Run it:

```bash
cargo run
```

### Distributed cluster (no Redis)

Enable `cluster-embedded` and start a **self-organizing Raft cluster** — register any node to any known node to form the network:

```toml
mocra = { version = "0.2", features = ["cluster-embedded"] }
```

```rust
// First core node — bootstraps a new cluster
Mocra::builder()
    .spider(Httpbin, on_item(|p: Page| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;

// Any additional node joins via a seed address (or ClusterConfig::from_env() for containers)
Mocra::builder()
    .spider(Httpbin, on_item(|p: Page| async move { /* ... */ }))
    .cluster(ClusterConfig::join(2, "127.0.0.1:7002", "./data/n2", "127.0.0.1:7001"))
    .run().await?;
```

The **control plane** (leader election, distributed locks, membership, partition ownership) runs on an embedded **redb + Raft** — no external Redis required. The **data plane** keeps pluggable message queues (Kafka / Redis / **NATS JetStream** / in-memory), with task routing by `hash(account)` for consumer affinity.

> **Advanced (multi-stage DAG)**: for multi-node pipelines with login, pagination, and custom middleware, implement `ModuleTrait` / `ModuleNodeTrait` directly (enable the `store` feature for the account × platform × module model). See [Module Development](docs/module-development.md).

### Admin dashboard

Enable the `dashboard` feature and call `.dashboard(port)` — the engine hosts a read-only observability API **and** a built-in single-file web UI. Open the port in a browser to see **metrics / logs / tasks / performance**; no frontend build, and no endpoint to type in (the page targets its own engine):

```toml
mocra = { version = "0.2", features = ["dashboard"] }
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .dashboard(8080)   // GET / → web UI;  /metrics,  /observability/{engine,cluster,system,logs}
    .run().await?;
```

```bash
cargo run --example dashboard --features dashboard   # then open http://127.0.0.1:8080
```

The read-only endpoints (`/`, `/metrics`, `/health`, `/observability/*`) are CORS-enabled and need no API key, so a standalone frontend can consume them cross-origin; write endpoints (`/control/*`, `/start_work`) stay authenticated.

### Custom downloaders

The default downloader is reqwest. Implement the `Downloader` trait to swap it for browser rendering, proxy rotation, or a custom retry policy — either globally or per module:

```rust
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .default_downloader(BrowserDownloader::new())  // replace reqwest globally
    // .downloader(MyDownloader::new())            // or register by name; routed when a
    //                                             // module's config.downloader == name()
    .run().await?;
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                         Engine                          │
│                                                         │
│  TaskEvent ──▶ generate() ──▶ download() ──▶ parser()  │
│      │              │              │              │      │
│  [Task Q]     [Request Q]   [Response Q]   [Parser Q]  │
│                                                  │      │
│                              ┌────────────┬──────┘      │
│                              ▼            ▼             │
│                         [Data Store]  [Next Node]       │
│                                       [Error Q → DLQ]  │
└─────────────────────────────────────────────────────────┘
```

Each stage is decoupled by a message queue. Queues are local Tokio channels in single-node mode, or Redis Streams / Kafka / NATS (JetStream) in distributed mode — **same code, zero changes**.

### Workspace crates

mocra is a Cargo workspace. The entire runtime lives in `mocra-core`; the `mocra` crate you depend on is a **thin facade** over it (12 direct dependencies). Reusable subsystems ship as standalone crates with a single, acyclic dependency direction (`mocra → mocra-core → {mocra-cluster, mocra-dag, mocra-proxy, mocra-store}` — the inner crates never depend back):

| Crate | What it is |
|---|---|
| [`mocra`](.) | **Thin facade** — `Spider` trait, `Mocra` builder, prelude, default sinks. The only crate most users import. |
| [`mocra-core`](crates/mocra-core) | The full runtime: domain models, downloader, queue, sync, scheduler, engine + observability/admin API. |
| [`mocra-cluster`](crates/mocra-cluster) | Embedded control plane: Raft + redb (election, fenced locks, membership, partition ownership) — no external coordinator. |
| [`mocra-dag`](crates/mocra-dag) | Generic distributed DAG execution engine (zero crawler coupling). |
| [`mocra-proxy`](crates/mocra-proxy) | Configuration-driven proxy pool / manager (standalone). |
| [`mocra-store`](crates/mocra-store) | Multi-tenant sea-orm entity models (behind the `store` feature). |

## DAG Execution

Define complex pipelines with fan-out and fan-in:

```rust
async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
    Some(ModuleDagDefinition {
        nodes: vec![
            ModuleDagNodeDef { node_id: "start".into(), node: Arc::new(StartNode), ..Default::default() },
            ModuleDagNodeDef { node_id: "branch_a".into(), node: Arc::new(BranchA), ..Default::default() },
            ModuleDagNodeDef { node_id: "branch_b".into(), node: Arc::new(BranchB), ..Default::default() },
            ModuleDagNodeDef { node_id: "merge".into(), node: Arc::new(MergeNode), ..Default::default() },
        ],
        edges: vec![
            ModuleDagEdgeDef { from: "start".into(), to: "branch_a".into() },
            ModuleDagEdgeDef { from: "start".into(), to: "branch_b".into() },
            ModuleDagEdgeDef { from: "branch_a".into(), to: "merge".into() },
            ModuleDagEdgeDef { from: "branch_b".into(), to: "merge".into() },
        ],
        entry_nodes: vec!["start".into()],
        ..Default::default()
    })
}
```

```
       ┌── branch_a ──┐
start ─┤               ├── merge
       └── branch_b ──┘
```

## Single-Node vs Distributed

| | Single-Node | Embedded cluster (`cluster-embedded`) |
|---|---|---|
| **Control plane** | In-process | Embedded **redb + Raft** (elections / locks / membership / partition ownership) — **no Redis** |
| **Queues (data plane)** | Tokio mpsc (in-memory) | Pluggable MQ: Kafka / NATS JetStream / Redis Streams / in-memory |
| **Locks / election** | Local | Raft-consensus (fencing tokens) |
| **Workers** | 1 process | N nodes, same binary; register any node to any known node |
| **Work distribution** | — | Cron by `hash(account)` ownership + MQ consumer affinity |
| **Code changes** | None | Add `.cluster(ClusterConfig::…)` |

Enable the embedded cluster (no external Redis required):

```rust
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;
```

A Redis-backed control plane is also available without the embedded cluster: provide Redis in your TOML config (`from_toml`) and coordination (locks / election) routes through Redis instead of Raft. The data plane (message queue) is selected independently — Kafka (`queue-kafka`), NATS JetStream (`queue-nats`), Redis Streams, or in-memory.

## Documentation

| Document | Description |
|---|---|
| [Getting Started](docs/getting-started.md) | Installation and first module |
| [Architecture](docs/architecture.md) | System design and pipeline internals |
| [Module Development](docs/module-development.md) | ModuleTrait, ModuleNodeTrait, data passing |
| [DAG Guide](docs/dag-guide.md) | DAG definition, fan-out/fan-in, advance gates |
| [Middleware](docs/middleware-guide.md) | Download, data, and storage middleware |
| [Configuration](docs/configuration.md) | Full TOML configuration reference |
| [API Reference](docs/api-reference.md) | HTTP control plane endpoints |
| [Deployment](docs/deployment.md) | Single-node, distributed, monitoring |

## Examples

Runnable examples in [`examples/`](examples/):

- [`examples/spider_quickstart.rs`](examples/spider_quickstart.rs) — minimal `Spider` (no DB / no Redis)
- [`examples/dashboard.rs`](examples/dashboard.rs) — built-in observability dashboard (`--features dashboard`)
- [`examples/cluster_quickstart.rs`](examples/cluster_quickstart.rs) — self-organizing embedded cluster (`--features cluster-embedded`)

Advanced `ModuleTrait` / DAG usage: see [Module Development](docs/module-development.md) and the [DAG Guide](docs/dag-guide.md).

## Monitoring

```bash
# Start Prometheus + Grafana
docker compose -f docker-compose.monitoring.yml up -d

# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000
# Metrics:    http://localhost:8080/metrics
```

## License

Licensed under either of:

- MIT license
- Apache License, Version 2.0

at your option.
