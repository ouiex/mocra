# Configuration Reference

`State::new(path).await` loads the runtime configuration from a TOML file.

## Top-Level Shape

```toml
name = "mocra-node"

[db]

[download_config]

[cache]
ttl = 3600
backend = "local"

[crawler]
request_max_retries = 3
task_max_errors = 100
module_max_errors = 20
module_locker_ttl = 60

[channel_config]
minid_time = 0
capacity = 1024
queue_codec = "msgpack"
```

The concrete fields are defined by `src/common/model/config.rs`.

## Cache

```toml
[cache]
ttl = 3600
backend = "local"
compression_threshold = 65536
enable_l1 = true
l1_ttl_secs = 30
l1_max_entries = 10000
```

Supported `backend` values:

- `local`
- `raft_rocksdb`

Use `local` for development and single-node deployments. Use `raft_rocksdb` when cache and coordination state must be backed by the Raft/RocksDB control plane.

## Crawler

```toml
[crawler]
request_max_retries = 3
task_max_errors = 100
module_max_errors = 20
module_locker_ttl = 60
node_id = "node-a"
task_concurrency = 8
publish_concurrency = 8
parser_concurrency = 8
error_task_concurrency = 2
backpressure_retry_delay_ms = 200
dedup_ttl_secs = 3600
idle_stop_secs = 0
```

`node_id` should be stable across restarts when a deployment needs stable node identity.

## Scheduler

```toml
[scheduler]
misfire_tolerance_secs = 300
concurrency = 100
refresh_interval_secs = 60
max_staleness_secs = 120
```

Use scheduler settings when modules rely on `cron()`.

## Channel and Kafka

```toml
[channel_config]
minid_time = 0
capacity = 1024
queue_codec = "msgpack"
batch_concurrency = 10
compression_threshold = 65536
nack_max_retries = 3
nack_backoff_ms = 500
federation_request_namespaces = []

[channel_config.kafka]
brokers = "localhost:9092"
username = ""
password = ""
tls = false
```

`queue_codec` supports `json` and `msgpack`.

When `channel_config.kafka` is absent, the application uses local queue behavior. When it is present, the queue manager can use Kafka-backed transport.

## Blob Storage

```toml
[channel_config.blob_storage]
path = "./data/blobs"
```

Blob storage is used for large queue payloads or payloads that should be stored outside the queue message body.

## Sync

```toml
[sync]
envelope_enabled = true

[sync.kafka]
brokers = "localhost:9092"
tls = false
```

`sync.envelope_enabled` enables typed envelope synchronization behavior. Kafka settings here are separate from `channel_config.kafka`.

## API

```toml
[api]
port = 3000
api_key = "change-me"
rate_limit = 100
```

`/metrics` and `/health` are public. Protected routes require the configured API key.

## Raft

```toml
[raft]
addr = "0.0.0.0:7001"
peers = ["10.0.0.2:7001"]
heartbeat_interval_ms = 500
election_timeout_ms = 1500
snapshot_interval = 500
data_dir = "./raft_data/mocra-node"
```

Omit `[raft]` for local-only coordination. Add `[raft]` when the node should participate in a Raft group for `name`.

## Unsupported Configuration

Redis is unsupported. Do not add Redis settings for cache, queue, locks, sync, events, or rate limiting.

