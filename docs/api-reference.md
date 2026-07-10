# API Reference

> **Start with the facade.** Running a crawler needs none of this — the [Getting Started](getting-started.md) quickstart (`Mocra::builder().spider(...).run()`) is the primary path. This page documents the **optional** observability / admin HTTP API, an advanced feature behind the `dashboard` cargo feature.

Enabling the `dashboard` feature gives the engine two things on a single port:

1. A **read-only observability HTTP API** (metrics, health, engine/cluster/system snapshots, recent logs) — CORS-enabled and unauthenticated, so a browser or external monitor can consume it directly.
2. A **built-in single-file web UI** at `GET /` — metrics / logs / tasks / performance panels, no frontend build and no endpoint to type in (the page targets its own engine).

A small set of **write / control** endpoints (task injection, pause/resume, node and DLQ inspection) is also served, but these require an API key.

## Enabling

Add the feature:

```toml
mocra = { version = "0.4", features = ["dashboard"] }
```

Then enable it one of two ways.

**Facade (primary):** call `.dashboard(port)` on the builder. This exposes the read-only observability API and the web UI with no API key. In single-node mode it also keeps the engine alive (disables idle auto-stop) and turns on log capture so the logs panel has data.

```rust
Mocra::builder()
    .spider(MySpider, on_item(|item: Item| async move { /* ... */ }))
    .dashboard(8080)   // GET / → web UI; /metrics, /observability/*
    .run()
    .await?;
```

```bash
cargo run --example dashboard --features dashboard   # then open http://127.0.0.1:8080
```

**TOML config (`from_toml`):** add an `[api]` section. This is the way to set an **API key**, which the write / control endpoints require.

```toml
[api]
port = 8080
api_key = "your-secret-key"   # required to use the write / control endpoints
rate_limit = 50               # optional: requests per second, per caller
```

The `[api]` table has exactly three keys: `port` (u16), `api_key` (optional string), and `rate_limit` (optional float, requests per second). There is no separate rate-limit window setting.

> With `.dashboard(port)` alone, no API key is configured, so the authenticated endpoints below return `403 Forbidden`. Configure `[api]` with an `api_key` to use them.

## Access tiers

| Endpoint | Method | Auth | Rate-limited |
|---|---|---|---|
| `/` | GET | none | no |
| `/health` | GET | none | no |
| `/metrics` | GET | none | yes |
| `/observability/engine` | GET | none | yes |
| `/observability/cluster` | GET | none | yes |
| `/observability/system` | GET | none | yes |
| `/observability/logs` | GET | none | yes |
| `/start_work` | POST | API key | yes |
| `/nodes` | GET | API key | yes |
| `/dlq` | GET | API key | yes |
| `/control/pause` | POST | API key | yes |
| `/control/resume` | POST | API key | yes |

## Authentication

Authenticated routes accept the API key as either header:

- `Authorization: Bearer <api_key>`
- `x-api-key: <api_key>`

A wrong key returns `401 Unauthorized`. If no `api_key` is configured at all, every authenticated route returns `403 Forbidden`.

The read-only endpoints (`/`, `/health`, `/metrics`, `/observability/*`) need no key and are served with a permissive CORS layer, so a standalone frontend can consume them cross-origin.

## Rate limiting

When `rate_limit` is set, all routes **except `/` and `/health`** are rate-limited. The limiter is keyed by caller identity — the `x-api-key` / `Bearer` value, or `"anonymous"` when absent — at `rate_limit` requests per second. Exceeding the limit returns `429 Too Many Requests`.

---

## Read-only endpoints

### GET /

The built-in single-file web dashboard (HTML). Compiled into the binary with the `dashboard` feature; open the port in a browser to view the panels.

**Auth:** none · **Rate-limited:** no

---

### GET /health

Liveness / readiness of the engine's backing services.

**Auth:** none · **Rate-limited:** no

**Response:**
```json
{
  "status": "up",
  "components": {
    "redis": { "status": "up" },
    "db":    { "status": "up" }
  }
}
```

`status` is `"up"` when every component is up, otherwise `"degraded"`. Each component is `"up"` or `"down"`; a down component adds an `error` string. Without the `store` feature (or in no-DB mode) the `db` component reports `"up"`.

---

### GET /metrics

Prometheus metrics in text exposition format.

**Auth:** none · **Rate-limited:** yes

**Response:**
```
# TYPE mocra_latency_seconds histogram
mocra_latency_seconds_bucket{le="0.05"} 1200
# TYPE mocra_errors_total counter
mocra_errors_total 4
# TYPE mocra_inflight gauge
mocra_inflight 12
# TYPE mocra_backlog_depth gauge
mocra_backlog_depth 0
```

Metric names are prefixed `mocra_` (latency, errors, in-flight, backlog depth, cache hits/misses, dedup, and more).

---

### GET /observability/engine

Engine / queue runtime snapshot for this node.

**Auth:** none · **Rate-limited:** yes

**Response:**
```json
{
  "namespace": "my_crawler",
  "single_node": true,
  "clustered": false,
  "pending": {
    "task": 0,
    "download": 3,
    "response": 1,
    "parser": 0,
    "error": 0,
    "remote_task": 0,
    "total": 4
  }
}
```

`namespace` is the configured `name`; `pending` breaks down local queue depth by stage.

---

### GET /observability/cluster

Embedded-Raft cluster status. Returns `null` when running standalone (no cluster coordination).

**Auth:** none · **Rate-limited:** yes

**Response (clustered):**
```json
{
  "node_id": 1,
  "is_leader": true,
  "current_leader": 1,
  "term": 4,
  "last_applied_index": 128,
  "member_count": 3,
  "voter_count": 3
}
```

---

### GET /observability/system

Latest host resource snapshot (sampled shortly after startup). Returns `null` before the first sample.

**Auth:** none · **Rate-limited:** yes

**Response:**
```json
{
  "cpu_usage_percent": 12.4,
  "memory_used_bytes": 5368709120,
  "memory_total_bytes": 17179869184,
  "memory_usage_percent": 31.2,
  "swap_used_bytes": 0,
  "swap_total_bytes": 2147483648,
  "updated_at_ms": 1720598400000
}
```

---

### GET /observability/logs

Recent structured log records, newest first.

**Auth:** none · **Rate-limited:** yes

**Query parameters:**

| Param | Default | Notes |
|---|---|---|
| `limit` | `200` | Max records to return (capped at `1000`) |

**Response:**
```json
[
  {
    "time": "2026-07-10T10:30:00.123Z",
    "level": "INFO",
    "module": "engine",
    "message": "task completed"
  }
]
```

Records may also carry optional `status` and `event_type` fields.

---

## Authenticated endpoints

### POST /start_work

Inject a task into the engine's processing queue.

**Auth:** required · **Rate-limited:** yes

**Request body** — a `TaskEvent`:
```json
{
  "account": "demo",
  "platform": "example",
  "module": ["my_module"]
}
```

`account` and `platform` are required. `module` is an optional list of module names (omit or leave empty to target all modules); `priority` and `run_id` are optional and default automatically.

**Response:** `200 OK` (no body).

---

### GET /nodes

List the active nodes in the cluster.

**Auth:** required · **Rate-limited:** yes

**Response:**
```json
[
  {
    "id": "node-abc123",
    "ip": "10.0.0.4",
    "hostname": "worker-1",
    "last_heartbeat": 1720598400,
    "version": "0.4.0"
  }
]
```

---

### GET /dlq

Inspect the Dead Letter Queue.

**Auth:** required · **Rate-limited:** yes

**Query parameters:**

| Param | Default | Notes |
|---|---|---|
| `topic` | `task` | Which DLQ topic to read |
| `count` | `10` | Number of messages to retrieve |

**Response:**
```json
[
  {
    "id": "1720598400000-0",
    "payload": "{...}",
    "reason": "max retries exceeded",
    "original_id": "1720598399000-0"
  }
]
```

---

### POST /control/pause

Pause the global engine. Processor runners stop consuming from queues (the pause flag is stored under the engine namespace).

**Auth:** required · **Rate-limited:** yes

**Response:** `200 OK`.

---

### POST /control/resume

Clear the pause flag and resume consumption.

**Auth:** required · **Rate-limited:** yes

**Response:** `200 OK`.

## See also

- [Getting Started](getting-started.md) — the facade quickstart (primary path)
- [Deployment](deployment.md) — running single-node vs distributed, monitoring
- [Configuration](configuration.md) — full TOML reference, including `[api]`
- [Middleware](middleware-guide.md) — advanced engine-level pipeline hooks
- [`examples/dashboard.rs`](../examples/dashboard.rs) — a runnable observability demo
