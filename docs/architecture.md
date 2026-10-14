# Architecture

This document describes how mocra is put together: the package layout, the queue-driven
pipeline, where the high-level facade sits relative to the lower-level engine, and how the same
code scales from a single process to a distributed cluster.

> **中文版：** [docs/zh/architecture.md](zh/architecture.md)

## Overview

mocra models data collection as a set of **queue-driven processing stages** orchestrated by a
**DAG execution engine**. Each stage is decoupled from the next by a message queue, so the same
pipeline runs unchanged whether the queues are in-process Tokio channels (single-node) or a
distributed message broker (Kafka / NATS JetStream). Scaling out is a
configuration/builder concern, not a rewrite.

## Package layout

mocra is a single Cargo package. The high-level facade, full runtime, and subsystems live under
`src/` and are published together as `mocra`. Existing `mocra` imports and feature names are
preserved; the former internal crates are source modules:

| Module | What it is |
|---|---|
| [`mocra::facade`](../src/facade.rs) / [`mocra::prelude`](../src/prelude.rs) | The `Spider` trait, the `Mocra` builder, the prelude, and default data sinks. |
| [`mocra::engine`](../src/engine) and other runtime modules | Domain models, downloader, queue, coordination (`sync`), scheduler, engine, and the observability/admin HTTP API. |
| [`mocra::cluster`](../src/cluster) | Embedded control plane: Raft + redb — leader election, fenced distributed locks, membership, and partition ownership, with **no external coordinator**. |
| [`mocra::schedule::dag`](../src/schedule/dag) | Generic distributed DAG execution engine, also re-exported by `mocra::schedule`. |
| [`mocra::proxy`](../src/proxy) | Configuration-driven proxy pool / manager. |
| [`mocra::common::model::entity`](../src/common/model/entity) | Multi-tenant sea-orm entity models (account × platform × module), behind the `store` feature. |

The proxy pool and DAG modules are always available. Entity models and the embedded cluster are
compiled only when `store` and `cluster-embedded`, respectively, are enabled.

When migrating code that directly depended on an internal crate, remove that dependency and use
the corresponding `mocra` module above. For the former core crate, replace `mocra_core::` with
`mocra::`; its runtime modules remain at the same paths inside `mocra`.

## The facade and the engine

There are two ways into the runtime, and they compile down to the same engine:

- **The `Spider` facade** — the 80% path. Implement one [`Spider`](../src/facade.rs) (a `name`, a
  `start` that seeds URLs, and a `parse` that emits typed items), register it with
  `Mocra::builder().spider(spider, sink)`, and call `.run()`. Typed items reach your code through a
  [`DataSink`](../src/facade.rs) — a closure via `on_item(...)`, a channel via `ChannelSink::new(tx)`,
  or your own `impl DataSink`. Everything you need is re-exported from `mocra::prelude`.

- **The lower-level engine** — `ModuleTrait` / `ModuleNodeTrait` in `mocra::common::interface`. This is the
  advanced path for multi-node pipelines with login, pagination, custom middleware, and hand-built
  DAGs. Enable the `store` feature for the account × platform × module model.

Internally, a `Spider` is adapted into a **single-node module** and compiled into the very same
engine the advanced path uses: `Spider::start` becomes the node's `generate()` (seed requests) and
`Spider::parse` becomes its `parser()` (emit items + follow-up requests). Choosing the facade costs
nothing at runtime — it is a thinner surface over identical machinery.

See [Module Development](module-development.md) for the lower-level traits and the
[DAG Guide](dag-guide.md) for multi-node graphs.

## Queue-driven pipeline

The pipeline is fully queue-driven. A task fans out into requests, requests are downloaded into
responses, responses are parsed into items and follow-up tasks — each stage handed to the next
through a queue:

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

| Stage | Input | Output | What happens |
|---|---|---|---|
| **Task** | `TaskEvent` | `Request` stream | Resolves the module/DAG node and calls `generate()` to produce seed (or follow-up) requests |
| **Download** | `Request` | `Response` | Fetches via the active downloader (reqwest by default, or a custom one), with proxy, rate-limit, and session support |
| **Parse** | `Response` | `TaskOutputEvent` | Calls `parser()`; routes emitted items to the sink/data store, follow-up tasks to the next node, and failures to the error queue |
| **Error** | error event | retry or terminate | Threshold-based retry with backoff; exhausted work lands in the dead-letter queue (DLQ) |

The key property: **each stage is decoupled by a message queue.** Queues are local Tokio channels
in single-node mode, or Kafka / NATS JetStream in distributed mode — **same code,
zero changes.** Only the queue backend behind the boundary changes.

## Single-node vs distributed

Two things scale independently: the **control plane** (who leads, who holds a lock, who owns which
partition) and the **data plane** (the message queue that carries tasks between stages).

| | Single-node | Embedded cluster (`cluster-embedded`) |
|---|---|---|
| **Control plane** | In-process | Embedded **redb + Raft** — elections / fenced locks / membership / partition ownership, with **no external coordinator** |
| **Data plane (queues)** | Tokio mpsc (in-memory) | Pluggable MQ: Kafka / NATS JetStream / in-memory |
| **Locks / election** | Local | Raft consensus (fencing tokens) |
| **DAG gates / stop / global pause** | Local cache | Raft KV/CAS, visible to every node |
| **General cache / sessions** | Local memory | Local memory on each node |
| **Workers** | 1 process | N nodes, same binary; register any node to any known node |
| **Work distribution** | — | Cron by `hash(account)` ownership + competing MQ consumers |
| **Code changes** | None (facade default) | Add `.cluster(ClusterConfig::…)` |

Two concrete topologies:

- **Single-node (facade default).** No `.cluster(...)`, no `.from_toml(...)`: the builder uses an
  in-memory, no-infra configuration. The engine auto-seeds each spider and stops when idle — ideal
  for one-shot scrapes and development. No database, no external services.

- **Embedded distributed control plane.** Enable `cluster-embedded` and add
  `.cluster(ClusterConfig::bootstrap(...))` on the first node, `.cluster(ClusterConfig::join(...))`
  on the rest. Coordination (election, locks, membership, partition ownership) runs on an embedded
  **redb + Raft** — **no external coordinator**. Any node registers to any known node to form the network.

In every distributed case the **data-plane queue is selected independently** of the control plane —
in-memory, Kafka (`queue-kafka`), or NATS JetStream (`queue-nats`). A distributed MQ can deliver
successive stages of one account to different nodes; the general cache and sessions remain local.
With the in-memory queue, work stays on the seeding node.

See the [Deployment Guide](deployment.md) for the exact steps and commands for each topology.

## DAG orchestration

Every module is compiled into a DAG. A `Spider` compiles to a single node; the lower-level path can
declare linear chains (`add_step()`) or custom fan-out/fan-in graphs (`dag_definition()`):

```
       ┌── branch_a ──┐
start ─┤               ├── merge
       └── branch_b ──┘
```

The DAG **topology is static** (fixed at init); the **execution count per node is dynamic**, driven
by how many parse events flow through the queues. See the [DAG Guide](dag-guide.md) for definition,
routing, and advance/fallback gates.

## Observability and admin

Enable the `dashboard` feature and call `.dashboard(port)` to have the engine host a **read-only
observability HTTP API** plus a **built-in single-file web UI** (no frontend build required):

- `GET /` — the built-in dashboard page (metrics / logs / tasks / performance)
- `GET /metrics` — Prometheus metrics (unified `mocra_*` families)
- `GET /health` — health check
- `GET /observability/{engine,cluster,system,logs}` — queue/engine snapshot, Raft cluster status
  (`null` when standalone), host CPU/memory/swap, and recent structured logs

The read-only endpoints (`/`, `/metrics`, `/health`, `/observability/*`) are CORS-enabled and need
no API key, so a standalone frontend can consume them cross-origin; write endpoints (`/control/*`,
`/start_work`) stay authenticated. See the [API Reference](api-reference.md) for the full route
table and the [Deployment Guide](deployment.md#monitoring) for the Prometheus/Grafana stack.

## Related guides

- [Getting Started](getting-started.md) — installation and first spider
- [Module Development](module-development.md) — `ModuleTrait` / `ModuleNodeTrait`, the advanced path
- [DAG Guide](dag-guide.md) — fan-out/fan-in, routing, advance gates
- [Middleware](middleware-guide.md) — download, data, and storage middleware
- [Configuration](configuration.md) — full TOML reference
- [Deployment](deployment.md) — single-node, embedded cluster, monitoring
