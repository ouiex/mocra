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
tokio = { version = "1", features = ["full"] }
serde_json = "1"
futures = "0.3"
```

Write a module:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use futures::stream;
use mocra::prelude::*;
use mocra::common::model::{NodeGenerateContext, NodeParseContext, NodeParseOutput};
use mocra::common::state::State;
use mocra::engine::engine::Engine;

struct FetchNode;

#[async_trait]
impl ModuleNodeTrait for FetchNode {
    async fn generate(
        &self,
        _ctx: NodeGenerateContext<'_>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new("https://httpbin.org/get", RequestMethod::Get.as_ref());
        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(
        &self, response: Response, _ctx: NodeParseContext<'_>,
    ) -> Result<NodeParseOutput> {
        println!("Status: {}, Body: {} bytes", response.status_code, response.content.len());
        Ok(NodeParseOutput::default())
    }
}

struct MyModule;

#[async_trait]
impl ModuleTrait for MyModule {
    fn name(&self) -> &'static str { "my_module" }
    fn version(&self) -> i32 { 1 }
    fn should_login(&self) -> bool { false }
    fn default_arc() -> Arc<dyn ModuleTrait> { Arc::new(Self) }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(FetchNode)]
    }
}

#[tokio::main]
async fn main() {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(state, None).await;
    engine.register_module(MyModule::default_arc()).await;
    engine.run().await;
}
```

Create `config.toml`:

```toml
name = "my_crawler"

[db]
url = "sqlite://data/crawler.db?mode=rwc"

[cache]
ttl = 60

[download_config]
timeout = 30
rate_limit = 10

[crawler]
request_max_retries = 3

[channel_config]
capacity = 5000
```

Run:

```bash
cargo run
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

Each stage is decoupled by a message queue. Queues are local Tokio channels in single-node mode, or Redis Streams / Kafka in distributed mode — **same code, zero changes**.

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

| | Single-Node | Distributed |
|---|---|---|
| **Config** | No `cache.redis` | Add `cache.redis` |
| **Queues** | Tokio mpsc (in-memory) | Redis Streams or Kafka |
| **Cache** | In-memory | Redis |
| **Locks** | Local mutex | Redis distributed locks |
| **Workers** | 1 process | N processes, same binary |
| **Code changes** | None | None |

Switch to distributed by adding Redis to your config:

```toml
[cache.redis]
url = "redis://localhost:6379"
```

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

See the [`simple/`](simple/) directory for runnable examples:

- [`simple/module_node_trait_dag.rs`](simple/module_node_trait_dag.rs) — Fan-out/fan-in DAG module

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
