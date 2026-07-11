# Configuration Reference

> The single-node facade needs no config — `Mocra::builder().spider(..).run()` runs with no DB and no external services. This reference is for the advanced path loaded via `.from_toml("config.toml")` (DB-backed tasks, distributed coordination, the dashboard API, etc.).

> For architecture, pipeline internals, and distributed deployment, see [docs/README.md](README.md).

Configuration uses TOML. Every key below is verified against the config structs in
`crates/mocra-core/src/common/model/config.rs` (plus `logger_config.rs`, `policy.rs`, and
`mocra-proxy`). Prefer **URL + a few necessary fields** over exhaustively listing every option.

## When do you need a config file?

You usually don't. The facade builds a no-DB, in-memory single-node config for you:

```rust
// No TOML, no database, no external services — runs in-memory.
Mocra::builder()
    .spider(MySpider, on_item(|item| async move { /* ... */ }))
    .run()
    .await?;
```

Add `mocra = "0.4"` to `Cargo.toml`. Reach for a TOML file only when you need the **advanced /
distributed** path — a database-backed task model, Kafka/NATS data-plane queues,
the dashboard/observability HTTP API, cron scheduling, proxy pools, or custom error policies. Load
it with:

```rust
Mocra::builder()
    .from_toml("config.toml")
    .spider(MySpider, on_item(|item| async move { /* ... */ }))
    .run()
    .await?;
```

## Single-node vs. distributed

The runtime mode is **determined at runtime**, not by any config field — `Config::is_single_node_mode()`
no longer exists. Distribution is decided by whether an embedded coordination backend is active.

- **Single-node (the default).** In-process coordination (locks, leader election), an in-memory
  cache, and in-memory queues. No external services.
- **Distributed.** Enable the `cluster-embedded` Cargo feature and start an embedded cluster with
  `.cluster(…)` on the builder. This brings up an embedded **Raft + redb** control plane —
  cross-node leader election, distributed locks, and a KV store — injected at runtime. Distribution
  is active whenever this coordination backend is running.

(In the observability API, `single_node` means no coordination backend is active and `clustered`
means one is.)

## Feature flags

Config keys are inert unless the runtime code they drive is compiled in. Enable with
`mocra = { version = "0.4", features = ["…"] }`.

| Feature | Needed for |
|---|---|
| `store` | The DB-backed `account × platform × module` task model behind `[db].url`. |
| `dashboard` | The admin/observability HTTP API + web UI enabled by `[api]` (or `.dashboard(port)`). |
| `queue-kafka` | Kafka data-plane queue (`channel_config.kafka`, `sync.kafka`, Kafka log output). |
| `queue-nats` | NATS JetStream data-plane queue (`channel_config.nats`). |
| `cluster-embedded` | Embedded Raft + redb control plane (`.cluster(…)`) — the distributed coordination backend: cross-node leader election, distributed locks, and a KV store. |

## Minimal valid config

The smallest config the loader accepts (from `test_config_deserialization` in `config.rs`). The
`name`, `[db]`, `[download_config]`, `[cache]`, `[crawler]`, and `[channel_config]` sections are
required by the schema; everything else is optional. Note `[db]` is present but has no `url`, so no
database is used:

```toml
name = "test_app"

[db]
url = "postgres://user:password@localhost:5432/db"
database_schema = "public"

[download_config]
downloader_expire = 3600
timeout = 30
rate_limit = 10.0
enable_session = true
enable_locker = false
enable_rate_limit = true
cache_ttl = 600
wss_timeout = 60

[cache]
ttl = 3600

[crawler]
request_max_retries = 3
task_max_errors = 5
module_max_errors = 10
module_locker_ttl = 60

[channel_config]
minid_time = 0
capacity = 1000
```

With no `.cluster(…)` coordination backend, this config runs in **single-node** mode.

## Config precedence

Runtime config resolution follows three layers, highest first:

1. ORM / module config (per-module overrides)
2. `config.toml` (this file)
3. Hard-coded defaults (only when the two layers above are absent)

Typical layered fields: `enable_session`, `enable_locker`, `enable_rate_limit`, `module_locker_ttl`,
`wss_timeout`.

## Unit conventions

- **Seconds:** `*_secs`, `timeout`, `wss_timeout`, `downloader_expire`, `cache_ttl`, `ttl`
- **Milliseconds:** `*_ms`
- **Bytes:** `compression_threshold`, `max_response_size`

---

## Field reference

### Top-level

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `name` | yes | string | Instance name / namespace; prefixes cache and distributed keys. |
| `db` | yes | table | Database config (may be empty; see `[db]`). |
| `download_config` | yes | table | Downloader and request settings. |
| `cache` | yes | table | In-memory cache settings. |
| `crawler` | yes | table | Crawler runtime behavior and concurrency. |
| `channel_config` | yes | table | Queue / message-channel settings. |
| `scheduler` | no | table | Cron scheduler settings. |
| `sync` | no | table | Distributed state-sync settings. |
| `proxy` | no | table | Inline proxy-pool config. |
| `api` | no | table | Built-in HTTP API / dashboard (requires `dashboard`). |
| `event_bus` | no | table | Event-bus capacity and concurrency. |
| `logger` | no | table | Logging outputs (multi-sink). |
| `policy` | no | table | Error-handling policy overrides. |

### [db]

The `[db]` table is required by the schema but every field is optional. **Without `url`, no database
is used.** The DB-backed task model (`account × platform × module`) requires the **`store`** feature.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `url` | no | string | Connection URL, e.g. `postgres://user:pass@host:5432/db` or `sqlite://path?mode=rwc`. |
| `database_schema` | no | string | Database schema. |
| `pool_size` | no | integer | Connection pool size. |
| `tls` | no | boolean | Enable TLS (`sslmode=require`). |

### [download_config]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `downloader_expire` | yes | integer | Downloader instance expiration (seconds). |
| `timeout` | yes | integer | Request timeout (seconds). |
| `rate_limit` | yes | float | Global request rate limit (QPS); `0` means unlimited. |
| `enable_session` | yes | boolean | Enable session-state sync (headers/cookies) and related cache logic. |
| `enable_locker` | yes | boolean | Enable distributed locking to prevent concurrent conflicts. |
| `enable_rate_limit` | yes | boolean | Enable rate limiting (uses `rate_limit`). |
| `cache_ttl` | yes | integer | Response cache TTL (seconds). |
| `wss_timeout` | yes | integer | WebSocket timeout (seconds). |
| `pool_size` | no | integer | HTTP client connection-pool size (default 200). |
| `max_response_size` | no | integer | Max response body size in bytes (default 10 MB). |

#### `enable_session` behavior

When `enable_session = true`, the downloader persists session state through the cache:

- **Object:** `SessionState` — fields `session_id`, `module_id`, `headers`, `cookies`, `version`.
- **Storage:** the in-process `CacheService` (a local in-memory store).
- **Scope:** `module_id + run_id`, so different run batches of a module don't pollute each other.
- **Read (before send):** pull `SessionState` and merge its `headers`/`cookies` into the request;
  the request's own `headers`/`cookies` win — the session only fills gaps.
- **Write (after response):** update `cookies` from the response; only header keys listed in
  `request.cache_headers` are written back to the session.
- **Host-only cookies:** if a response cookie has no `Domain`, the host of `request.url` is filled
  in before storing.

Enable it for tasks that need login/session continuity; leave it off for stateless scraping to save
cache round-trips.

### [cache]

An in-process, in-memory cache (`CacheService` backed by a local store).

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `ttl` | yes | integer | Default cache TTL (seconds). |
| `compression_threshold` | no | integer | Compress cached payloads larger than this (bytes). |

### [crawler]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `request_max_retries` | yes | integer | Max retries per request. |
| `task_max_errors` | yes | integer | Max errors per task. |
| `module_max_errors` | yes | integer | Max errors per module. |
| `module_locker_ttl` | yes | integer | Module-lock TTL (seconds). |
| `node_id` | no | string | Stable node identity across restarts. |
| `task_concurrency` | no | integer | Concurrency for the task processor. |
| `publish_concurrency` | no | integer | Concurrency for request publishing. |
| `parser_concurrency` | no | integer | Concurrency for the parser task processor. |
| `error_task_concurrency` | no | integer | Concurrency for the error task processor. |
| `backpressure_retry_delay_ms` | no | integer | Retry delay (ms) when queue backpressure (full/closed) occurs; omitted → default retry policy. |
| `idle_stop_secs` | no | integer | Auto-stop the engine after this many idle seconds; `0`/omitted disables. Stops only when local queues are empty and the cron scheduler has no running tasks. |

### [scheduler]

Cron scheduling. All fields optional.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `misfire_tolerance_secs` | no | integer | Tolerance for missed cron jobs (default 300). |
| `concurrency` | no | integer | Max concurrency for scheduled contexts (default 100). |
| `refresh_interval_secs` | no | integer | Scheduler cache refresh interval (default 60). |
| `max_staleness_secs` | no | integer | Max staleness before forcing a refresh (default 120). |

### [channel_config]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `minid_time` | yes | integer | MinID/snowflake base time for ordered IDs. |
| `capacity` | yes | integer | Local in-memory queue capacity; too small causes backpressure. |
| `blob_storage` | no | table | Spill large payloads to disk (see below). |
| `kafka` | no | table | Kafka queue backend (see [KafkaConfig](#kafkaconfig-shared)). Requires `queue-kafka`. |
| `nats` | no | table | NATS JetStream queue backend (see [NatsConfig](#natsconfig-shared)). Requires `queue-nats`. |
| `queue_codec` | no | string | Remote-queue codec: `json` or `msgpack` (must match producers/consumers). |
| `batch_concurrency` | no | integer | Max concurrency for batch flushing to remote queues (default 10). |
| `compression_threshold` | no | integer | Compress queue payloads larger than this (bytes). |
| `nack_max_retries` | no | integer | Max NACK retries before routing to the DLQ (default 0). |
| `nack_backoff_ms` | no | integer | Backoff (ms) before retrying a NACK (default 0). |

#### [channel_config.blob_storage]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `path` | no | string | Local directory for large-payload spillover. |

### [sync]

Distributed state synchronization. All fields optional.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `kafka` | no | table | Kafka config for sync (see [KafkaConfig](#kafkaconfig-shared)). Requires `queue-kafka`. |
| `allow_rollback` | no | boolean | Allow rollback to older values (default true). |
| `envelope_enabled` | no | boolean | Enable versioned envelope for sync payloads (default false). |

### [api]

**Feature: `dashboard`.** Enables the admin/observability HTTP API and built-in single-file web UI
(metrics, logs, tasks, performance). The same thing can be enabled programmatically with
`.dashboard(port)`; `[api]` is the TOML equivalent.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `port` | yes | integer | HTTP server port. |
| `api_key` | no | string | API key for write endpoints. Read-only observability endpoints need no key. |
| `rate_limit` | no | float | API rate limit (QPS). |

### [event_bus]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `capacity` | yes | integer | Event channel capacity (default 1024). |
| `concurrency` | yes | integer | Event-handler concurrency (default 64). |

### [logger]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `enabled` | no | boolean | Enable logging (default true). Alias: `enable`. |
| `level` | no | string | Global log level (default `info`). |
| `format` | no | string | Output format; `console`/`file` support `text` only. |
| `include` | no | array | Structured-field allowlist. |
| `buffer` | no | integer | MQ-output buffer size (default 10000). |
| `flush_interval_ms` | no | integer | Flush interval for MQ output (default 500). |
| `outputs` | yes | array | List of output sinks (see below); configure at least one or nothing is logged. |
| `prometheus` | no | table | Prometheus log-stats settings. |

#### logger.outputs[]

Each output is tagged by `type`: `console`, `file`, or `mq`.

**`type = "console"`** — no additional fields.

**`type = "file"`**

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `path` | yes | string | File path. |
| `rotation` | no | string | Rotation policy (`daily`/`hourly`/`minutely`/`never`). |
| `max_size_mb` | no | integer | Max file size (reserved). |
| `max_files` | no | integer | Max file count (reserved). |

**`type = "mq"`**

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `backend` | yes | string | `kafka`. |
| `topic` | yes | string | Topic name. |
| `format` | no | string | `json` only. |
| `buffer` | no | integer | Buffer size (default 10000). |
| `batch_size` | no | integer | Batch size (reserved). |
| `compression` | no | string | Compression algorithm (reserved). |
| `kafka` | no | table | Kafka config when `backend = "kafka"` (see [KafkaConfig](#kafkaconfig-shared)). |

#### logger.prometheus

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `enabled` | yes | boolean | Enable log-stats metrics. |
| `port` | no | integer | Metrics port (reuses the API/metrics port). |
| `path` | no | string | Metrics path (default `/metrics`). |

Example (console + file + mq):

```toml
[logger]
enabled = true
level = "info"
format = "text"
buffer = 10000
flush_interval_ms = 500

[[logger.outputs]]
type = "console"

[[logger.outputs]]
type = "file"
path = "logs/app.log"
rotation = "daily"

[[logger.outputs]]
type = "mq"
backend = "kafka"
topic = "mocra-logs"
format = "json"
kafka = { brokers = "localhost:9095" }

[logger.prometheus]
enabled = true
```

Environment variables `DISABLE_LOGS` / `MOCRA_DISABLE_LOGS` disable logging entirely.

### [policy]

Override the built-in error-handling policy (retry / backoff / DLQ / alert) per error class. The
only key is `overrides`, an array of rules (`[[policy.overrides]]`); the best-matching rule wins.
Fields marked "match" narrow which errors a rule applies to; the rest override the resolved policy.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `kind` | yes | string | Match: error kind. One of `Request`, `Response`, `Command`, `Service`, `Proxy`, `Download`, `Queue`, `Orm`, `Task`, `Module`, `RateLimit`, `ProcessorChain`, `Parser`, `DataMiddleware`, `DataStore`, `DynamicLibrary`, `CacheService`. |
| `domain` | no | string | Match: domain, e.g. `engine`, `system`. |
| `event_type` | no | string | Match: event type, e.g. `download`, `parser`. |
| `phase` | no | string | Match: lifecycle phase, e.g. `failed`, `retry`. |
| `retryable` | no | boolean | Override: whether retry is allowed. |
| `backoff` | no | string / table | Override: `"None"`, or `{ Linear = { base_ms, max_ms } }`, or `{ Exponential = { base_ms, max_ms } }`. |
| `dlq` | no | string | Override: DLQ routing — `Never`, `OnExhausted`, or `Always`. |
| `alert` | no | string | Override: alert level — `Info`, `Warn`, `Error`, or `Critical`. |
| `max_retries` | no | integer | Override: max retry attempts. |
| `backoff_ms` | no | integer | Override: initial backoff (ms). |

```toml
[[policy.overrides]]
domain = "engine"
event_type = "download"
phase = "failed"
kind = "Download"
retryable = true
dlq = "OnExhausted"
alert = "Warn"
max_retries = 5
backoff_ms = 500
backoff = { Exponential = { base_ms = 500, max_ms = 60000 } }

[[policy.overrides]]
kind = "Parser"
retryable = false
dlq = "Always"
backoff = "None"
```

### [proxy]

Inline proxy-pool config (no external proxy file). All fields optional.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `tunnel` | no | array | Static tunnel proxies (see `[[proxy.tunnel]]`). |
| `direct` | no | array | Direct URL proxies (see `[[proxy.direct]]`); supports `http/https/ws/wss`. |
| `ip_provider` | no | array | Dynamic IP-provider list (see `[[proxy.ip_provider]]`). |
| `pool_config` | no | table | Pool strategy (see `[proxy.pool_config]`). |

#### [[proxy.direct]]

A single proxy URL, e.g. `https://127.0.0.1:8888`; no provider needed. `ws`/`wss` are mapped
internally to `http`/`https` proxy channels.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `url` | yes | string | Proxy URL: `http://`, `https://`, `ws://`, or `wss://`. |
| `name` | no | string | Proxy name; auto-generated (`direct_{index}`) if omitted. |
| `rate_limit` | no | float | Per-proxy QPS limit (default 10). |
| `expire_time` | no | string | Expiry (RFC3339); far-future by default. |

#### [[proxy.tunnel]]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `name` | yes | string | Tunnel name. |
| `endpoint` | yes | string | Endpoint (`host:port`). |
| `tunnel_type` | yes | string | Protocol, e.g. `http` / `socks5`. |
| `expire_time` | yes | string | Expiry (RFC3339). |
| `rate_limit` | yes | float | Per-tunnel rate limit (QPS). |
| `username` | no | string | Username. |
| `password` | no | string | Password. |

#### [[proxy.ip_provider]]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `name` | yes | string | Provider name. |
| `url` | yes | string | API URL for fetching proxy IPs. |
| `retry_codes` | yes | array | HTTP status codes that trigger a refresh. |
| `timeout` | yes | integer | Provider request timeout (seconds). |
| `rate_limit` | yes | float | Provider request rate limit (QPS). |
| `proxy_expire_time` | yes | integer | Expiry of fetched proxies (seconds). |
| `provider_expire_time` | no | string | Provider-credential expiry (RFC3339). |
| `weight` | no | integer | Provider weight (higher = preferred). |

#### [proxy.pool_config]

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `min_size` | no | integer | Minimum pool size (default 5). |
| `max_size` | no | integer | Maximum pool size (default 50). |
| `max_errors` | no | integer | Max errors per proxy before eviction (default 3). |
| `health_check_interval_secs` | no | integer | Health-check interval (seconds, default 300). |
| `refill_threshold` | no | float | Refill trigger ratio (default 0.3). |

---

## Shared types

### KafkaConfig (shared)

**Feature: `queue-kafka`.** Used by `channel_config.kafka`, `sync.kafka`, and
`logger.outputs[].kafka`.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `brokers` | yes | string | Comma-separated broker list. |
| `username` | no | string | SASL username. |
| `password` | no | string | SASL password. |
| `tls` | no | boolean | Enable TLS. |

### NatsConfig (shared)

**Feature: `queue-nats`.** Used by `channel_config.nats`.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `url` | yes | string | Server address(es), e.g. `nats://127.0.0.1:4222` (comma-separate multiple). |
| `username` | no | string | Username. |
| `password` | no | string | Password. |
| `token` | no | string | Token auth. |

---

## Example: distributed config

A production-shaped config: PostgreSQL task store, Kafka data-plane queue, dashboard API, and
multi-sink logging. Cross-node coordination (leader election, locks) is enabled separately in code
with the `cluster-embedded` feature and `.cluster(…)` — see [deployment](deployment.md).

```toml
name = "crawler"

[api]                       # requires the `dashboard` feature
port = 8080

[db]                        # requires the `store` feature for the DB task model
url = "postgres://user:password@127.0.0.1:5432/crawler"
database_schema = "base"
pool_size = 20

[download_config]
downloader_expire = 3600
timeout = 20
rate_limit = 0
enable_session = false
enable_locker = false
enable_rate_limit = false
cache_ttl = 60
wss_timeout = 30
pool_size = 200
max_response_size = 10485760

[cache]
ttl = 300

[crawler]
request_max_retries = 2
task_max_errors = 50
module_max_errors = 10
module_locker_ttl = 5
task_concurrency = 200
publish_concurrency = 200
idle_stop_secs = 0

[sync]
allow_rollback = true
envelope_enabled = false

[channel_config]
minid_time = 12
capacity = 20000
queue_codec = "msgpack"
compression_threshold = 1024
batch_concurrency = 500

[channel_config.kafka]      # requires the `queue-kafka` feature
brokers = "127.0.0.1:9092"

[event_bus]
capacity = 200000
concurrency = 2000

[logger]
enabled = true
level = "info"
format = "text"

[[logger.outputs]]
type = "console"

[logger.prometheus]
enabled = true
```

## Test config samples

- [tests/config.test.toml](../tests/config.test.toml)
- [tests/config.mock.toml](../tests/config.mock.toml)
- [tests/config.mock.pure.toml](../tests/config.mock.pure.toml)
- [tests/config.mock.pure.engine.toml](../tests/config.mock.pure.engine.toml)
- [tests/config.prod_like.toml](../tests/config.prod_like.toml)
