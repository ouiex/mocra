# Architecture

This document describes the internal architecture of mocra.

## Overview

mocra is a **distributed, event-driven crawling and data collection framework** for Rust. It models data collection as a pipeline of queue-driven stages with DAG-based module orchestration.

```
┌─────────────────────────────────────────────────────────────┐
│                          Engine                             │
│                                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────┐ │
│  │  Task     │──▶│ Request  │──▶│ Response │──▶│ Parser  │ │
│  │  Queue    │   │ Queue    │   │ Queue    │   │ Queue   │ │
│  └──────────┘   └──────────┘   └──────────┘   └────┬────┘ │
│       ▲                                             │      │
│       │         ┌──────────┐                        │      │
│       └─────────│ Parser   │◀───────────────────────┘      │
│                 │ Task Q   │                               │
│                 └──────────┘                               │
│                 ┌──────────┐                               │
│                 │ Error    │◀── (on failure)                │
│                 │ Task Q   │                               │
│                 └──────────┘                               │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### State

`State` is the **runtime composition root**. It loads TOML configuration and initializes all shared services:

- **Database** connection (PostgreSQL / SQLite via SeaORM)
- **CacheService** (in-memory or Redis-backed)
- **DistributedLockManager** for concurrency control
- **QueueManager** for message routing
- **StatusTracker** for error threshold management
- **ProxyPool** for proxy rotation

Runtime mode is **auto-detected** — no manual switch:
- `cache.redis` configured → **distributed mode**
- `cache.redis` absent → **single-node mode**

### Engine

`Engine` is the main orchestrator. It wires together:

- **TaskManager** — module registry, task creation, DAG compilation
- **QueueManager** — queue subscriptions and publishing
- **ProcessorRunner** — bounded-concurrency worker loops
- **CronScheduler** — cron-based periodic task scheduling
- **DownloaderManager** — HTTP/WebSocket client pool
- **MiddlewareManager** — download, data, and storage middleware chains
- **API server** — Axum-based control plane
- **Metrics** — Prometheus exporter

### Processing Pipeline

The pipeline is fully queue-driven. Each stage is decoupled by a message queue:

```
TaskEvent ──▶ [Task Queue]
                  │
                  ▼
            Module.generate()  ──▶  Stream<Request>
                                        │
                                        ▼
                                  [Request Queue]
                                        │
                                        ▼
                              Downloader.download()  ──▶  Response
                                                            │
                                                            ▼
                                                      [Response Queue]
                                                            │
                                                            ▼
                                                    Module.parser()  ──▶  TaskOutputEvent
                                                                              │
                                              ┌───────────────┬───────────────┤
                                              ▼               ▼               ▼
                                        [Data Store]    [Parser Task Q]   [Error Task Q]
                                                            │               │
                                                            ▼               ▼
                                                      (next node)    (retry / DLQ)
```

**Stage details:**

| Stage | Input | Output | Description |
|---|---|---|---|
| **Task** | `TaskEvent` | `Request` stream | Resolves module, calls `generate()` on the current DAG node |
| **Download** | `Request` | `Response` | HTTP/WebSocket fetch with proxy, rate-limit, session sync |
| **Parse** | `Response` | `TaskOutputEvent` | Calls `parser()`, routes results to data store, next tasks, or error queue |
| **Error** | `TaskErrorEvent` | Retry or terminate | Threshold-based retry with backoff, module/task termination |

### ProcessorRunner

Each pipeline stage runs as a `ProcessorRunner` — a worker loop with:

- **Bounded concurrency** via semaphore
- **Pause/resume** support (via `watch` channel)
- **Graceful shutdown** (via `broadcast` channel)
- **Batch receive** for throughput (up to 100 items per iteration)

### QueueManager

`QueueManager` bridges local Tokio channels to optional remote backends:

| Backend | When | Description |
|---|---|---|
| **Local (Tokio mpsc)** | Always | In-process channels for single-node |
| **Redis Streams** | `channel_config.redis` set | Distributed queue with consumer groups, sharding, claim recovery |
| **Kafka** | `channel_config.kafka` set | Kafka topics with consumer groups |

Features:
- **Per-priority topics** — separate channels for different priorities
- **DLQ / NACK** — configurable retry count and backoff before dead-letter
- **Compression** — zstd compression above configurable threshold
- **Blob offload** — large payloads offloaded to local blob storage
- **Codec** — MsgPack (default) or JSON for remote queue encoding

### Module System

Modules are the user-facing abstraction. A module defines **what to crawl** and **how to parse**:

```
ModuleTrait                          ModuleNodeTrait
┌───────────────────┐                ┌───────────────────┐
│ name()            │                │ generate()        │
│ version()         │                │   → Stream<Req>   │
│ should_login()    │                │                   │
│ add_step()        │──── returns ──▶│ parser()          │
│ dag_definition()  │   Vec<Node>    │   → TaskOutput    │
│ pre_process()     │                │                   │
│ post_process()    │                │ retryable()       │
│ cron()            │                └───────────────────┘
└───────────────────┘
```

Each `ModuleNodeTrait` represents a DAG node with two operations:
- **`generate()`** — produces a stream of HTTP requests
- **`parser()`** — processes the response and returns data + next tasks

### DAG Execution

Every module is compiled into a DAG at registration time:

- **`add_step()`** returns a linear chain: `step_0 → step_1 → step_2`
- **`dag_definition()`** returns a custom graph with fan-out/fan-in
- If both are present, they are **merged** (linear nodes get `legacy_` prefix)

The DAG topology is **static** (fixed at init). Execution count per node is **dynamic** — driven by how many `TaskParserEvent`s flow through the queue.

See [DAG Execution](dag-guide.md) for details.

### Middleware Pipeline

Three middleware layers, each sorted by `weight()` (lower = earlier):

| Layer | Trait | Hook Points |
|---|---|---|
| **Download** | `DownloadMiddleware` | `before_request()`, `after_response()` |
| **Data** | `DataMiddleware` | `handle_data()` |
| **Storage** | `DataStoreMiddleware` | `before_store()`, `store_data()`, `after_store()` |

Middleware instances are shared as `Arc<Mutex<Box<dyn ...>>>`.

See [Middleware](middleware-guide.md) for details.

### Error Handling

Errors are handled at three levels:

| Level | Mechanism | Description |
|---|---|---|
| **Generate failure** | One-shot cache fallback | Non-entry nodes fall back to the previous request via Redis-cached `prefix_request` |
| **Parser failure** | `TaskErrorEvent` + error queue | Emits error with `stay_current_step: true` for same-node retry |
| **Threshold exceeded** | `StatusTracker` | Tracks error counts per request/module/task; decides retry, skip, or terminate |

### Cron Scheduling

Modules can declare a cron schedule via `ModuleTrait::cron()`. The `CronScheduler`:
- Evaluates cron expressions on a configurable refresh interval
- Fires with misfire tolerance
- Injects `TaskEvent` into the task queue on trigger

### Control Plane API

Built-in Axum HTTP server (configured via `[api]`):

| Route | Method | Auth | Description |
|---|---|---|---|
| `/health` | GET | No | Health check |
| `/metrics` | GET | Rate-limited | Prometheus metrics |
| `/start_work` | POST | API key | Inject a manual task |
| `/nodes` | GET | API key | List active cluster nodes |
| `/dlq` | GET | API key | Inspect dead letter queue |
| `/control/pause` | POST | API key | Pause global engine |
| `/control/resume` | POST | API key | Resume global engine |
