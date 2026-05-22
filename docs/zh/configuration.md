# 配置参考

`State::new(path).await` 会从 TOML 文件加载运行时配置。

## 顶层结构

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

具体字段定义位于 `src/common/model/config.rs`。

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

支持的 `backend`：

- `local`
- `raft_rocksdb`

开发和单节点部署使用 `local`。需要由 Raft/RocksDB 控制面承载缓存与协调状态时使用 `raft_rocksdb`。

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

部署需要稳定节点身份时，`node_id` 应跨重启保持稳定。

## Scheduler

```toml
[scheduler]
misfire_tolerance_secs = 300
concurrency = 100
refresh_interval_secs = 60
max_staleness_secs = 120
```

模块依赖 `cron()` 时使用 scheduler 配置。

## Channel 和 Kafka

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

`queue_codec` 支持 `json` 和 `msgpack`。

省略 `channel_config.kafka` 时应用使用本地队列行为。配置后队列管理器可以使用 Kafka 传输。

## Blob Storage

```toml
[channel_config.blob_storage]
path = "./data/blobs"
```

Blob storage 用于大队列载荷，或需要放在队列消息体之外保存的载荷。

## Sync

```toml
[sync]
envelope_enabled = true

[sync.kafka]
brokers = "localhost:9092"
tls = false
```

`sync.envelope_enabled` 启用 typed envelope 同步行为。这里的 Kafka 配置与 `channel_config.kafka` 分开。

## API

```toml
[api]
port = 3000
api_key = "change-me"
rate_limit = 100
```

`/metrics` 和 `/health` 是公开端点。受保护路由需要配置的 API key。

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

本地协调可省略 `[raft]`。节点需要加入 `name` 对应的 Raft group 时配置 `[raft]`。

## 不支持的配置

当前不支持 Redis。不要为缓存、队列、锁、同步、事件或限流添加 Redis 配置。

