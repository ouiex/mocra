# Architecture

This document describes how mocra is put together: the workspace layout, the queue-driven
pipeline, where the high-level facade sits relative to the lower-level engine, and how the same
code scales from a single process to a distributed cluster.

> **中文版：** [docs/zh/architecture.md](zh/architecture.md)

## Overview

mocra models data collection as a set of **queue-driven processing stages** orchestrated by a
**DAG execution engine**. Each stage is decoupled from the next by a message queue, so the same
pipeline runs unchanged whether the queues are in-process Tokio channels (single-node) or a
distributed message broker (Kafka / NATS JetStream). Scaling out is a
configuration/builder concern, not a rewrite.

## Workspace layout

mocra is a Cargo workspace. The entire runtime lives in `mocra-core`; the `mocra` crate you
depend on is a **thin facade** over it. Reusable subsystems ship as standalone crates with a
single, **acyclic** dependency direction — the facade depends on the core, the core depends on the
subsystem crates, and the subsystem crates never depend back:

```
mocra  ──▶  mocra-core  ──▶  { mocra-cluster, mocra-dag, mocra-proxy, mocra-store }
(facade)     (runtime)              (reusable subsystems, no back-edges)
```

| Crate | What it is |
|---|---|
| [`mocra`](..) | **Thin facade** — the `Spider` trait, the `Mocra` builder, the prelude, and default data sinks. The only crate most users import. |
| [`mocra-core`](../crates/mocra-core) | The full runtime: domain models, downloader, queue, coordination (`sync`), scheduler, engine, and the observability/admin HTTP API. |
| [`mocra-cluster`](../crates/mocra-cluster) | Embedded control plane: Raft + redb — leader election, fenced distributed locks, membership, and partition ownership, with **no external coordinator**. |
| [`mocra-dag`](../crates/mocra-dag) | Generic distributed DAG execution engine, with zero crawler coupling. |
| [`mocra-proxy`](../crates/mocra-proxy) | Configuration-driven proxy pool / manager (standalone). |
| [`mocra-store`](../crates/mocra-store) | Multi-tenant sea-orm entity models (account × platform × module), behind the `store` feature. |

`mocra-core` pulls in `mocra-proxy` and `mocra-dag` unconditionally, and `mocra-store` /
`mocra-cluster` only when the `store` / `cluster-embedded` features are enabled. Because the
subsystem crates have no dependency back on the core, each can be understood, tested, and reused on
its own.

## The facade and the engine

There are two ways into the runtime, and they compile down to the same engine:

- **The `Spider` facade** — the 80% path. Implement one [`Spider`](../src/facade.rs) (a `name`, a
  `start` that seeds URLs, and a `parse` that emits typed items), register it with
  `Mocra::builder().spider(spider, sink)`, and call `.run()`. Typed items reach your code through a
  [`DataSink`](../src/facade.rs) — a closure via `on_item(...)`, a channel via `ChannelSink::new(tx)`,
  or your own `impl DataSink`. Everything you need is re-exported from `mocra::prelude`.

- **The lower-level engine** — `ModuleTrait` / `ModuleNodeTrait` in `mocra-core`. This is the
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
| **Workers** | 1 process | N nodes, same binary; register any node to any known node |
| **Work distribution** | — | Cron by `hash(account)` ownership + MQ consumer affinity |
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
in-memory, Kafka (`queue-kafka`), or NATS JetStream (`queue-nats`). With a
distributed MQ, tasks fan out across nodes with account affinity (`hash(account)`); with the
in-memory queue, work stays on the seeding node.

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
