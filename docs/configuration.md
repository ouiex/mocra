# 配置指南 (Configuration)

> 架构、流程、分布式部署等统一入口见 `docs/README.md`。

本项目使用 TOML 作为统一配置格式。建议使用 **URL + 少量必要字段** 的方式，减少冗余与错误配置。

单位约定：
- 秒：`*_secs`、`timeout`、`wss_timeout`、`downloader_expire`
- 毫秒：`claim_*`（Redis Stream 相关）
- 字节：`compression_threshold`、`max_response_size`

## 运行模式判定（单节点 / 分布式）

当前版本**不需要也不支持**通过 `RuntimeMode` 手动指定模式。

Engine 在启动时会根据配置自动判断：

- 当 `cache.redis` 已配置：判定为**分布式模式**。
- 当 `cache.redis` 未配置：判定为**单节点模式**。

对应代码入口：`Config::is_single_node_mode()`（`src/common/model/config.rs`）。

影响（简述）：

- 单节点模式：跳过依赖分布式 Redis 原子脚本的链路（例如 Lua-first 协调流程）。
- 分布式模式：启用分布式相关链路（如 Lua 预加载与分布式协调）。

## 1. 推荐配置结构 (Recommended Structure)

```toml
name = "crawler"

[db]
# 推荐使用 url，减少冗余字段
url = "postgres://user:password@localhost:5432/crawler"
# 可选：schema
database_schema = "base"
# 可选：连接池
pool_size = 10

[cache]
ttl = 60

[cache.redis]
redis_host = "127.0.0.1"
redis_port = 6379
redis_db = 0
pool_size = 50

[sync]
# Default: allow_rollback = true, envelope_enabled = false
allow_rollback = true
envelope_enabled = false

[proxy.pool_config]
min_size = 5
max_size = 50
max_errors = 3
health_check_interval_secs = 300
refill_threshold = 0.3

[[proxy.direct]]
name = "local_https_proxy"
url = "https://127.0.0.1:8888"
rate_limit = 50

[[proxy.tunnel]]
name = "default_tunnel"
endpoint = "127.0.0.1:7890"
tunnel_type = "http"
expire_time = "2099-12-31T23:59:59Z"
rate_limit = 50

[channel_config]
minid_time = 12
capacity = 5000
compression_threshold = 1024
```

## 2. 字段说明 (Field Reference)

### 顶层字段 (Top-Level)

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| name | 是 | string | 实例名称/命名空间，影响缓存与分布式键前缀。 | Instance name/namespace; affects cache and distributed key prefix. |
| db | 是 | object | 数据库配置。 | Database configuration. |
| download_config | 是 | object | 下载器与网络请求配置。 | Downloader and request settings. |
| cache | 是 | object | 缓存配置。 | Cache settings. |
| crawler | 是 | object | 爬虫运行时行为与并发配置。 | Runtime behavior and concurrency for crawler. |
| scheduler | 否 | object | 定时任务配置（Cron）。 | Scheduler configuration (Cron). |
| sync | 否 | object | 分布式同步配置。 | Distributed sync settings. |
| proxy | 否 | object | 代理池配置（内联在 config.toml）。 | Proxy pool config (inline in config.toml). |
| cookie | 否 | object | Cookie 存储 Redis 配置。 | Redis config for cookie storage. |
| channel_config | 是 | object | 队列/消息通道配置。 | Queue/channel settings. |
| api | 否 | object | 内置 API 服务配置。 | Built-in API server settings. |
| event_bus | 否 | object | 事件总线配置。 | Event bus settings. |
| logger | 否 | object | 日志输出配置（多输出）。 | Logger outputs configuration (multi-sink). |

### [db]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| url | 否 | string | 数据库连接 URL（推荐使用）。 | DB connection URL (recommended). |
| database_schema | 否 | string | 数据库 schema。 | Database schema. |
| pool_size | 否 | number | 连接池大小。 | Connection pool size. |
| tls | 否 | bool | 是否启用 TLS。 | Enable TLS. |

### [cache]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| ttl | 是 | number | 默认缓存 TTL（秒）。 | Default cache TTL in seconds. |
| redis | 否 | object | Redis 配置（见 Redis 通用配置）。 | Redis config (see Redis common section). |
| compression_threshold | 否 | number | 缓存压缩阈值（字节）。 | Cache compression threshold in bytes. |
| enable_l1 | 否 | bool | 是否启用本地 L1 缓存（减少 Redis 读）。 | Enable local L1 cache (reduces Redis reads). |
| l1_ttl_secs | 否 | number | L1 缓存 TTL（秒），过小会降低命中率。 | L1 cache TTL (seconds); too small reduces hit rate. |
| l1_max_entries | 否 | number | L1 缓存最大条目数，超出将驱逐。 | Max entries for L1 cache; evicted when exceeded. |

### Redis 通用配置 (RedisConfig)

适用于：`cache.redis`、`channel_config.redis`、`channel_config.compensator`、`cookie`、`logger.outputs[].redis`（当输出到 Redis 时）。

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| redis_host | 是 | string | Redis 主机地址。 | Redis host. |
| redis_port | 是 | number | Redis 端口。 | Redis port. |
| redis_db | 是 | number | Redis DB 索引。 | Redis DB index. |
| redis_username | 否 | string | Redis 用户名。 | Redis username. |
| redis_password | 否 | string | Redis 密码。 | Redis password. |
| pool_size | 否 | number | 连接池大小。 | Connection pool size. |
| shards | 否 | number | Stream 分片数量（队列用），影响并发监听与吞吐。 | Stream shards count for queues; affects parallelism and throughput. |
| tls | 否 | bool | 是否启用 TLS。 | Enable TLS. |
| claim_min_idle | 否 | number | 超过该空闲时间的消息会被认领，用于恢复卡住消费（毫秒）。 | Messages idle longer than this are claimed to recover stuck consumers (ms). |
| claim_count | 否 | number | 每次认领的消息上限，过大可能增加延迟。 | Max messages to claim per run; too large may add latency. |
| claim_interval | 否 | number | 认领扫描间隔（毫秒）。 | Interval for claim scans (ms). |
| listener_count | 否 | number | 入站订阅/监听任务数，用于从 Redis Stream 拉取并投递到本地队列。 | Inbound listener task count to pull from Redis Stream and dispatch to local queues. |

### Kafka 通用配置 (KafkaConfig)

适用于：`channel_config.kafka`、`logger.outputs[].kafka`（当输出到 Kafka 时）。

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| brokers | 是 | string | Kafka broker 列表（逗号分隔）。 | Broker list (comma-separated). |
| username | 否 | string | SASL 用户名。 | SASL username. |
| password | 否 | string | SASL 密码。 | SASL password. |
| tls | 否 | bool | 是否启用 TLS。 | Enable TLS. |

### [channel_config]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| blob_storage | 否 | object | 大对象落盘配置。 | Blob storage for large payloads. |
| redis | 否 | object | 队列 Redis 配置。 | Redis queue config. |
| kafka | 否 | object | 队列 Kafka 配置。 | Kafka queue config. |
| compensator | 否 | object | 补偿/死信 Redis 配置。 | Compensator/DLQ Redis config. |
| minid_time | 是 | number | MinID/雪花时间基准，用于生成有序 ID。 | MinID/snowflake base time for ordered IDs. |
| capacity | 是 | number | 本地内存队列容量，过小会导致背压。 | Local in-memory queue capacity; too small causes backpressure. |
| queue_codec | 否 | string | 远程队列序列化格式：`json` 或 `msgpack`（需与生产/消费一致）。 | Remote queue codec: `json` or `msgpack` (must match producers/consumers). |
| batch_concurrency | 否 | number | 远程队列批量写入并发上限。 | Max concurrency for batch publishing to remote queues. |
| compression_threshold | 否 | number | 消息体超过阈值时压缩（字节）。 | Compress payloads above this size (bytes). |
| nack_max_retries | 否 | number | NACK 重试次数上限（默认 0）。 | Max NACK retries (default: 0). |
| nack_backoff_ms | 否 | number | NACK 重试退避毫秒（默认 0）。 | Backoff ms before retrying NACK (default: 0). |

### [sync]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| redis | 否 | object | 同步 Redis 配置（见 Redis 通用配置）。 | Redis config for sync (see Redis section). |
| kafka | 否 | object | 同步 Kafka 配置（见 Kafka 通用配置）。 | Kafka config for sync (see Kafka section). |
| allow_rollback | 否 | bool | 是否允许回滚到旧值（默认 true）。 | Allow rollback to older values (default: true). |
| envelope_enabled | 否 | bool | 是否启用版本化 envelope（默认 false）。 | Enable versioned envelope (default: false). |

### [cookie]

用于读取账号登录态 Cookie。未配置时将无法从 Redis 获取 Cookie。

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| redis_host | 是 | string | Redis 主机地址。 | Redis host. |
| redis_port | 是 | number | Redis 端口。 | Redis port. |
| redis_db | 是 | number | Redis DB 索引。 | Redis DB index. |
| redis_username | 否 | string | Redis 用户名。 | Redis username. |
| redis_password | 否 | string | Redis 密码。 | Redis password. |
| pool_size | 否 | number | 连接池大小。 | Connection pool size. |
| tls | 否 | bool | 是否启用 TLS。 | Enable TLS. |

Cookie 在 Redis 中的 key 格式：

```
{namespace}:cookie:login_info:{account}-{platform}
```

示例：

```
crawler_local:cookie:login_info:benchmark-test
```

说明：`{account}-{platform}` 与 `Task::id()` 一致。

### [proxy]

用于内联配置代理，不再通过 `proxy_path` 读取外部文件。

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| tunnel | 否 | array | 静态隧道代理列表（见 `[[proxy.tunnel]]`）。 | Static tunnel proxies (see `[[proxy.tunnel]]`). |
| direct | 否 | array | 直接 URL 代理列表（见 `[[proxy.direct]]`）。支持 `http/https/ws/wss`。 | Direct URL proxy list (see `[[proxy.direct]]`). Supports `http/https/ws/wss`. |
| ip_provider | 否 | array | 动态 IP 代理提供商列表（见 `[[proxy.ip_provider]]`）。 | Dynamic IP provider list (see `[[proxy.ip_provider]]`). |
| pool_config | 否 | object | 代理池策略（见 `[proxy.pool_config]`）。 | Proxy pool strategy (see `[proxy.pool_config]`). |

#### [[proxy.direct]]

用于直接声明单个代理 URL（例如 `https://127.0.0.1:8888`），无需配置 provider。

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| name | 否 | string | 代理名称，不填自动生成。 | Proxy name, auto-generated if omitted. |
| url | 是 | string | 代理 URL，支持 `http://`、`https://`、`ws://`、`wss://`。 | Proxy URL supporting `http://`, `https://`, `ws://`, `wss://`. |
| rate_limit | 否 | number | 该代理 QPS 上限（默认 10）。 | Per-proxy QPS limit (default 10). |
| expire_time | 否 | string | 过期时间（RFC3339，默认远期时间）。 | Expire time (RFC3339, far-future by default). |

说明：
- HTTP/HTTPS 请求可直接通过该 URL 代理。
- WebSocket (`ws`/`wss`) 也支持；内部会按协议自动映射为可用的 HTTP/HTTPS 代理通道。

#### [[proxy.tunnel]]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| name | 是 | string | 隧道名称。 | Tunnel name. |
| endpoint | 是 | string | 代理地址（host:port）。 | Tunnel endpoint (host:port). |
| username | 否 | string | 用户名。 | Username. |
| password | 否 | string | 密码。 | Password. |
| tunnel_type | 是 | string | 协议类型，如 `http`/`socks5`。 | Protocol type, e.g. `http`/`socks5`. |
| expire_time | 是 | string | 过期时间（RFC3339）。 | Expire time (RFC3339). |
| rate_limit | 是 | number | 每秒限速（QPS）。 | Per-tunnel rate limit (QPS). |

#### [[proxy.ip_provider]]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| name | 是 | string | 提供商名称。 | Provider name. |
| url | 是 | string | 拉取代理 IP 的接口地址。 | API URL for fetching proxy IPs. |
| retry_codes | 是 | array[number] | 触发重试拉取的 HTTP 状态码。 | HTTP status codes triggering refresh. |
| timeout | 是 | number | 提供商请求超时（秒）。 | Provider request timeout (seconds). |
| rate_limit | 是 | number | 提供商请求限速（QPS）。 | Provider request rate limit (QPS). |
| provider_expire_time | 否 | string | 提供商凭据过期时间（RFC3339）。 | Provider credential expire time (RFC3339). |
| proxy_expire_time | 是 | number | 拉取到的代理 IP 过期时间（秒）。 | Expiration of fetched proxies (seconds). |
| weight | 否 | number | 提供商权重（越大越优先）。 | Provider weight (higher = preferred). |

#### [proxy.pool_config]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| min_size | 否 | number | 池最小容量（默认 5）。 | Minimum pool size (default 5). |
| max_size | 否 | number | 池最大容量（默认 50）。 | Maximum pool size (default 50). |
| max_errors | 否 | number | 单代理最大错误次数（默认 3）。 | Max errors per proxy (default 3). |
| health_check_interval_secs | 否 | number | 健康检查间隔秒数（默认 300）。 | Health check interval in seconds (default 300). |
| refill_threshold | 否 | number | 触发补充阈值比例（默认 0.3）。 | Refill threshold ratio (default 0.3). |

### [channel_config.blob_storage]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| path | 否 | string | 大对象落盘目录。 | Local path for blob storage. |

### [download_config]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| downloader_expire | 是 | number | 下载器实例过期时间（秒）。 | Downloader instance expiration (seconds). |
| timeout | 是 | number | 请求超时（秒）。 | Request timeout (seconds). |
| rate_limit | 是 | number | 全局请求速率限制（QPS），0 表示不限制。 | Global request rate limit (QPS); 0 means unlimited. |
| enable_cache | 是 | bool | 是否启用响应缓存（配合 `cache` 配置）。 | Enable response caching (uses `cache`). |
| enable_locker | 是 | bool | 是否启用分布式锁（防止并发冲突）。 | Enable distributed locking to prevent concurrent conflicts. |
| enable_rate_limit | 是 | bool | 是否启用限速逻辑（依赖 `rate_limit`）。 | Enable rate limiting (uses `rate_limit`). |
| cache_ttl | 是 | number | 响应缓存 TTL（秒）。 | Response cache TTL (seconds). |
| wss_timeout | 是 | number | WebSocket 超时（秒）。 | WebSocket timeout (seconds). |
| pool_size | 否 | number | HTTP 客户端连接池大小。 | HTTP client pool size. |
| max_response_size | 否 | number | 最大响应体大小（字节）。 | Max response size in bytes. |

### [crawler]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| request_max_retries | 是 | number | 请求最大重试次数。 | Max retries per request. |
| task_max_errors | 是 | number | 单任务最大错误数。 | Max errors per task. |
| module_max_errors | 是 | number | 单模块最大错误数。 | Max errors per module. |
| module_locker_ttl | 是 | number | 模块锁 TTL（秒）。 | Module lock TTL (seconds). |
| node_id | 否 | string | 节点 ID（稳定身份）。 | Node ID for stable identity. |
| task_concurrency | 否 | number | 本地处理器并发上限，影响任务消费速度。 | Max local processor concurrency; affects task consumption speed. |
| publish_concurrency | 否 | number | 发布到队列的并发上限。 | Max concurrency for publishing into queues. |
| backpressure_retry_delay_ms | 否 | number | 发生队列背压（full/closed）时的重试基准延迟（毫秒）。未设置则沿用默认重试策略。 | Base retry delay (ms) when queue backpressure (full/closed) occurs. Uses default retry policy when omitted. |
| dedup_ttl_secs | 否 | number | 请求去重 TTL（秒），用于去重窗口。 | Deduplication TTL (seconds) for request windowing. |
| idle_stop_secs | 否 | number | 空闲超时秒数，超过则自动停止；0/不填为关闭。判定条件：本地队列无待处理且 CronScheduler 无运行中任务。 | Idle timeout before auto-stop; 0/omitted disables. Stops only when local queues are empty and CronScheduler has no running tasks. |

### [scheduler]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| misfire_tolerance_secs | 否 | number | Cron 误触发容忍秒数。 | Misfire tolerance in seconds. |
| concurrency | 否 | number | 计划任务并发上限。 | Concurrency for scheduled contexts. |
| refresh_interval_secs | 否 | number | 调度配置刷新间隔（默认 60s）。 | Refresh interval for scheduler cache (default: 60s). |
| max_staleness_secs | 否 | number | 最大允许陈旧窗口（默认 120s），超出则强制刷新。 | Max allowed staleness before forcing refresh (default: 120s). |

### [api]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| port | 是 | number | API 服务端口。 | API server port. |
| api_key | 否 | string | API 访问密钥。 | API access key. |
| rate_limit | 否 | number | API 速率限制（QPS）。 | API rate limit (QPS). |

### [event_bus]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| capacity | 是 | number | 事件通道容量。 | Event channel capacity. |
| concurrency | 是 | number | 事件处理并发数。 | Event handler concurrency. |

### [logger]

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| enabled | 否 | bool | 是否启用日志（默认 true）。 | Enable logging (default true). |
| level | 否 | string | 全局日志级别（默认 info）。 | Global log level (default info). |
| format | 否 | string | console/file 仅支持 `text`。 | console/file only supports `text`. |
| include | 否 | array | 结构化字段白名单（可选）。 | Structured field allowlist (optional). |
| buffer | 否 | number | MQ 输出缓冲区大小（默认 10000）。 | MQ buffer size (default 10000). |
| flush_interval_ms | 否 | number | 刷新间隔（占位，默认 500ms）。 | Flush interval placeholder (default 500ms). |
| outputs | 是 | array | 日志输出列表（见下文）。 | Output list (see below). |
| prometheus | 否 | object | Prometheus 日志统计配置。 | Prometheus log stats settings. |

#### logger_config 详细说明 (LoggerConfig Details)

`logger` 作为统一入口，等价于 “logger_config”。它决定：
- 日志是否启用、基础级别、格式
- 具体输出目标（console/file/mq）
- 输出缓冲与刷新行为

常见约定：
- `level` 影响全局默认级别
- `format` 仅 `console/file` 支持 `text`；`mq` 只支持 `json`
- `buffer` 与 `flush_interval_ms` 只影响 MQ 输出（批量写入）
- `outputs[]` 至少配置一个，否则不会有输出
- `enabled` 为正式字段，兼容别名 `enable`

最小可用示例：
```toml
[logger]
enabled = true
level = "info"

[[logger.outputs]]
type = "console"
```

多输出示例（console + file + mq）：
```toml
[logger]
enabled = true
level = "info"
format = "text"
buffer = 10000
flush_interval_ms = 500

[[logger.outputs]]
type = "console"
level = "debug"

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
```

#### logger.outputs[] (LogOutputConfig)

按 `type` 区分：`console` / `file` / `mq`。

**Console**

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |

**File**

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| path | 是 | string | 文件路径。 | File path. |
| rotation | 否 | string | 轮转策略（daily/hourly/minutely/never）。 | Rotation policy (daily/hourly/minutely/never). |
| max_size_mb | 否 | number | 最大文件大小（占位）。 | Max file size (placeholder). |
| max_files | 否 | number | 最大文件数（占位）。 | Max file count (placeholder). |

**MQ**

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| backend | 是 | string | MQ 类型：`kafka` 或 `redis`。 | MQ backend: `kafka` or `redis`. |
| topic | 是 | string | Topic 名称。 | Topic name. |
| format | 否 | string | 仅支持 `json`。 | Only supports `json`. |
| buffer | 否 | number | 缓冲区大小（默认 10000）。 | Buffer size (default 10000). |
| batch_size | 否 | number | 批量大小（占位）。 | Batch size (placeholder). |
| compression | 否 | string | 压缩算法（占位）。 | Compression (placeholder). |
| kafka | 否 | object | Kafka 配置（当 backend=kafka）。 | Kafka config (when backend=kafka). |
| redis | 否 | object | Redis 配置（当 backend=redis）。 | Redis config (when backend=redis). |

#### logger.prometheus

| Key | 必填 | 类型 | 说明（中文） | Description (EN) |
| --- | --- | --- | --- | --- |
| enabled | 是 | bool | 是否启用日志指标统计。 | Enable log stats metrics. |
| port | 否 | number | 指标端口（复用 API/metrics）。 | Metrics port (reuses API/metrics). |
| path | 否 | string | 指标路径（默认 `/metrics`）。 | Metrics path (default `/metrics`). |

## 3. 数据库配置 (Database)

仅支持 `db.url`：
- `db.url = "postgres://user:password@host:5432/db"`

## 4. Redis 配置 (Redis)

目前使用 `redis_host/redis_port/redis_db` 结构。用于：
- 缓存 (`cache.redis`)
- 队列 (`channel_config.redis`)
- Cookie/锁/限流 (`cookie`/`cache` 共用池)

## 5. 日志配置（简化版） (Logger, simplified)

默认写入 `logs/mocra.{name}`。可用环境变量：
- `DISABLE_LOGS` / `MOCRA_DISABLE_LOGS`（禁用日志）

简化示例（仅日志相关）：
```toml
[logger]
enabled = true
level = "INFO"
format = "text" # console/file only

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
format = "json" # mq only
kafka = { brokers = "localhost:9095" }

[logger.prometheus]
enabled = true
```

## 6. 测试配置参考 (Test Config Samples)

- [tests/config.test.toml](tests/config.test.toml)
- [tests/config.mock.toml](tests/config.mock.toml)
- [tests/config.mock.pure.toml](tests/config.mock.pure.toml)
- [tests/config.mock.pure.engine.toml](tests/config.mock.pure.engine.toml)
- [tests/config.prod_like.toml](tests/config.prod_like.toml)
