# Configuration Reference

> The single-node facade needs no config — `Mocra::builder().spider(..).run()` runs with no DB/Redis. This reference is for the advanced path loaded via `.from_toml("config.toml")` (DB-backed tasks, Redis coordination, the dashboard API, etc.).

> For architecture, pipeline internals, and distributed deployment, see [docs/README.md](README.md).

Configuration uses TOML. Every key below is verified against the config structs in
`crates/mocra-core/src/common/model/config.rs` (plus `logger_config.rs`, `policy.rs`, and
`mocra-proxy`). Prefer **URL + a few necessary fields** over exhaustively listing every option.

## When do you need a config file?

You usually don't. The facade builds a no-DB, no-Redis, in-memory single-node config for you:

```rust
// No TOML, no database, no Redis — runs in-memory.
Mocra::builder()
    .spider(MySpider, on_item(|item| async move { /* ... */ }))
    .run()
    .await?;
```

Add `mocra = "0.4"` to `Cargo.toml`. Reach for a TOML file only when you need the **advanced /
distributed** path — a database-backed task model, Redis (or Kafka/NATS) coordination and queues,
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

The runtime mode is **inferred from config** — there is no `RuntimeMode` switch. The rule
(`Config::is_single_node_mode()` in `crates/mocra-core/src/common/model/config.rs`):

- **`[cache.redis]` is set → distributed.** Coordination (locks, leader election, shared cache)
  routes through Redis; the Redis-backed atomic-script paths are enabled.
- **`[cache.redis]` is absent → single-node.** In-process coordination and in-memory queues; the
  distributed Redis paths are skipped.

(An embedded Raft control plane is a separate, code-driven alternative to Redis coordination —
enable the `cluster-embedded` feature and call `.cluster(ClusterConfig::…)`. See the README.)

## Feature flags

Config keys are inert unless the runtime code they drive is compiled in. Enable with
`mocra = { version = "0.4", features = ["…"] }`.

| Feature | Needed for |
|---|---|
| `store` | The DB-backed `account × platform × module` task model behind `[db].url`. |
| `dashboard` | The admin/observability HTTP API + web UI enabled by `[api]` (or `.dashboard(port)`). |
| `queue-kafka` | Kafka data-plane queue (`channel_config.kafka`, `sync.kafka`, Kafka log output). |
| `queue-nats` | NATS JetStream data-plane queue (`channel_config.nats`). |
| `cluster-embedded` | Embedded Raft + redb control plane (`.cluster(…)`), an alternative to Redis coordination. |

Redis Streams queues and Redis coordination need **no** feature flag.

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

Because there is no `[cache.redis]`, this config runs in **single-node** mode.

## Config precedence

Runtime config resolution follows three layers, highest first:

1. ORM / module config (per-module overrides)
2. `config.toml` (this file)
3. Hard-coded defaults (only when the two layers above are absent)

Typical layered fields: `enable_session`, `enable_locker`, `enable_rate_limit`, `module_locker_ttl`,
`wss_timeout`.

## Unit conventions

- **Seconds:** `*_secs`, `timeout`, `wss_timeout`, `downloader_expire`, `cache_ttl`, `ttl`
- **Milliseconds:** `claim_*` (Redis Stream reclaim), `*_ms`
- **Bytes:** `compression_threshold`, `max_response_size`

---

## Field reference

### Top-level

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `name` | yes | string | Instance name / namespace; prefixes cache and distributed keys. |
| `db` | yes | table | Database config (may be empty; see `[db]`). |
| `download_config` | yes | table | Downloader and request settings. |
| `cache` | yes | table | Cache settings (and Redis, which selects distributed mode). |
| `crawler` | yes | table | Crawler runtime behavior and concurrency. |
| `channel_config` | yes | table | Queue / message-channel settings. |
| `scheduler` | no | table | Cron scheduler settings. |
| `sync` | no | table | Distributed state-sync settings. |
| `cookie` | no | table | Redis config for cookie/login-state storage. |
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

When `enable_session = true`, the downloader syncs a distributed session:

- **Object:** `SessionState` — fields `session_id`, `module_id`, `headers`, `cookies`, `version`.
- **Storage:** Redis (via `CacheService`) — so this is effectively a distributed-mode feature.
- **Scope:** `module_id + run_id`, so different run batches of a module don't pollute each other.
- **Read (before send):** pull `SessionState` and merge its `headers`/`cookies` into the request;
  the request's own `headers`/`cookies` win — the session only fills gaps.
- **Write (after response):** update `cookies` from the response; only header keys listed in
  `request.cache_headers` are written back to the session.
- **Host-only cookies:** if a response cookie has no `Domain`, the host of `request.url` is filled
  in before storing.

Enable it for tasks that need login/session continuity; leave it off for stateless scraping to save
Redis round-trips.

### [cache]

Setting `[cache.redis]` switches the runtime into **distributed** mode.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `ttl` | yes | integer | Default cache TTL (seconds). |
| `redis` | no | table | Redis backend (see [RedisConfig](#redisconfig-shared)). Presence → distributed mode. |
| `compression_threshold` | no | integer | Compress cached payloads larger than this (bytes). |
| `enable_l1` | no | boolean | Enable a local L1 cache layer to reduce Redis reads (default false). |
| `l1_ttl_secs` | no | integer | L1 cache TTL (seconds, default 30); too small lowers hit rate. |
| `l1_max_entries` | no | integer | Max L1 entries before eviction (default 10000). |

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
| `dedup_ttl_secs` | no | integer | Request-deduplication TTL (seconds, default 3600). |
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
| `redis` | no | table | Redis Streams queue backend (see [RedisConfig](#redisconfig-shared)). |
| `kafka` | no | table | Kafka queue backend (see [KafkaConfig](#kafkaconfig-shared)). Requires `queue-kafka`. |
| `nats` | no | table | NATS JetStream queue backend (see [NatsConfig](#natsconfig-shared)). Requires `queue-nats`. |
| `compensator` | no | table | Redis config for the compensator (retry / dead-letter) (see [RedisConfig](#redisconfig-shared)). |
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
| `redis` | no | table | Redis config for sync (see [RedisConfig](#redisconfig-shared)). |
| `kafka` | no | table | Kafka config for sync (see [KafkaConfig](#kafkaconfig-shared)). Requires `queue-kafka`. |
| `allow_rollback` | no | boolean | Allow rollback to older values (default true). |
| `envelope_enabled` | no | boolean | Enable versioned envelope for sync payloads (default false). |

### [cookie]

A [RedisConfig](#redisconfig-shared) used to read per-account login-state cookies. Without it,
cookies cannot be fetched from Redis. Key format in Redis:

```
{namespace}:cookie:login_info:{account}-{platform}
```

e.g. `crawler_local:cookie:login_info:benchmark-test`. The `{account}-{platform}` portion matches
`Task::id()`.

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
| `backend` | yes | string | `kafka` or `redis`. |
| `topic` | yes | string | Topic name. |
| `format` | no | string | `json` only. |
| `buffer` | no | integer | Buffer size (default 10000). |
| `batch_size` | no | integer | Batch size (reserved). |
| `compression` | no | string | Compression algorithm (reserved). |
| `kafka` | no | table | Kafka config when `backend = "kafka"` (see [KafkaConfig](#kafkaconfig-shared)). |
| `redis` | no | table | Redis config when `backend = "redis"` (see [RedisConfig](#redisconfig-shared)). |

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

### RedisConfig (shared)

Used by `cache.redis`, `channel_config.redis`, `channel_config.compensator`, `cookie`, `sync.redis`,
and `logger.outputs[].redis`.

| Key | Required | Type | Description |
| --- | --- | --- | --- |
| `redis_host` | yes | string | Redis host. |
| `redis_port` | yes | integer | Redis port. |
| `redis_db` | yes | integer | Redis DB index. |
| `redis_username` | no | string | Redis username. |
| `redis_password` | no | string | Redis password. |
| `pool_size` | no | integer | Connection pool size. |
| `shards` | no | integer | Stream shard count (queues); affects parallelism and throughput. |
| `tls` | no | boolean | Enable TLS. |
| `claim_min_idle` | no | integer | Idle ms after which stuck messages are claimed (default 600000). |
| `claim_count` | no | integer | Messages claimed per run (default 10). |
| `claim_interval` | no | integer | Interval (ms) between claim scans (default 60000). |
| `listener_count` | no | integer | Inbound listener tasks for sharded multiplexing (default 4). |

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

A production-shaped config: PostgreSQL task store, Redis coordination + queue, dashboard API, and
multi-sink logging. The `[cache.redis]` block is what makes this **distributed**.

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

[cache.redis]               # presence → distributed mode
redis_host = "127.0.0.1"
redis_port = 6379
redis_db = 0
pool_size = 100

[crawler]
request_max_retries = 2
task_max_errors = 50
module_max_errors = 10
module_locker_ttl = 5
task_concurrency = 200
publish_concurrency = 200
dedup_ttl_secs = 3600
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

[channel_config.redis]
redis_host = "127.0.0.1"
redis_port = 6379
redis_db = 0
pool_size = 200
shards = 8
listener_count = 8

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
