# 配置参考 (Configuration Reference)

> 单节点门面无需任何配置 —— `Mocra::builder().spider(..).run()` 无需 DB、无需任何外部服务即可运行。本参考面向通过 `.from_toml("config.toml")` 加载的**进阶路径**(DB 驱动的任务模型、分布式协调、dashboard API 等)。

> 架构、流程内幕与分布式部署,见 [docs/README.md](README.md)。

配置采用 TOML。下表每个键都对照配置结构体核对过:`crates/mocra-core/src/common/model/config.rs`(以及 `logger_config.rs`、`policy.rs` 和 `mocra-proxy`)。推荐使用 **URL + 少量必要字段**,而非把每个选项都罗列出来。

## 什么时候才需要配置文件?

通常并不需要。门面会为你构建一个无 DB、内存单节点的配置:

```rust
// 无 TOML、无数据库、无外部服务 —— 纯内存运行。
Mocra::builder()
    .spider(MySpider, on_item(|item| async move { /* ... */ }))
    .run()
    .await?;
```

在 `Cargo.toml` 中加入 `mocra = "0.4"`。只有当你需要**进阶 / 分布式**路径时才需要 TOML 文件 —— 数据库驱动的任务模型、Kafka/NATS 数据面队列、dashboard/可观测 HTTP API、Cron 调度、代理池,或自定义错误策略。用以下方式加载:

```rust
Mocra::builder()
    .from_toml("config.toml")
    .spider(MySpider, on_item(|item| async move { /* ... */ }))
    .run()
    .await?;
```

## 单节点 vs. 分布式

运行模式在**运行时决定**,而非由任何配置字段决定 —— `Config::is_single_node_mode()` 已被移除。是否分布式取决于内嵌协调后端是否处于激活状态。

- **单节点(默认)。** 进程内协调(锁、选主)、内存缓存与内存队列。无任何外部服务。
- **分布式。** 开启 `cluster-embedded` Cargo 特性,并在 builder 上通过 `.cluster(…)` 启动内嵌集群。这会拉起一个内嵌的 **Raft + redb** 控制面 —— 提供跨节点选主、分布式锁与 KV 存储 —— 在运行时注入。只要该协调后端在运行,即为分布式。

(在可观测 API 中,`single_node` 表示没有协调后端处于激活状态,`clustered` 表示有一个正在运行。)

## 特性开关 (Feature flags)

除非驱动某配置键的运行时代码被编译进来,否则该键无效。用 `mocra = { version = "0.4", features = ["…"] }` 开启。

| 特性 | 用于 |
|---|---|
| `store` | `[db].url` 背后的 `账号 × 平台 × 模块` DB 任务模型。 |
| `dashboard` | `[api]`(或 `.dashboard(port)`)开启的 admin/可观测 HTTP API + Web UI。 |
| `queue-kafka` | Kafka 数据面队列(`channel_config.kafka`、`sync.kafka`、Kafka 日志输出)。 |
| `queue-nats` | NATS JetStream 数据面队列(`channel_config.nats`)。 |
| `cluster-embedded` | 内嵌 Raft + redb 控制面(`.cluster(…)`)—— 分布式协调后端:跨节点选主、分布式锁与 KV 存储。 |

## 最小可用配置

加载器能接受的最小配置(取自 `config.rs` 中的 `test_config_deserialization`)。`name`、`[db]`、`[download_config]`、`[cache]`、`[crawler]`、`[channel_config]` 这些段由 schema 要求必须存在;其余皆为可选。注意 `[db]` 存在但没有 `url`,因此不使用数据库:

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

由于没有 `.cluster(…)` 协调后端,此配置运行在**单节点**模式。

## 配置优先级 (Precedence)

运行时配置解析遵循三层优先级,从高到低:

1. ORM / 模块配置(按模块覆盖)
2. `config.toml`(本文件)
3. 硬编码默认值(仅当上面两层都缺失时生效)

典型的分层字段:`enable_session`、`enable_locker`、`enable_rate_limit`、`module_locker_ttl`、`wss_timeout`。

## 单位约定

- **秒:** `*_secs`、`timeout`、`wss_timeout`、`downloader_expire`、`cache_ttl`、`ttl`
- **毫秒:** `*_ms`
- **字节:** `compression_threshold`、`max_response_size`

---

## 字段参考 (Field reference)

### 顶层字段 (Top-Level)

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `name` | 是 | string | 实例名称 / 命名空间;作为缓存与分布式键的前缀。 |
| `db` | 是 | table | 数据库配置(可为空;见 `[db]`)。 |
| `download_config` | 是 | table | 下载器与请求配置。 |
| `cache` | 是 | table | 内存缓存配置。 |
| `crawler` | 是 | table | 爬虫运行时行为与并发。 |
| `channel_config` | 是 | table | 队列 / 消息通道配置。 |
| `scheduler` | 否 | table | Cron 调度配置。 |
| `sync` | 否 | table | 分布式状态同步配置。 |
| `proxy` | 否 | table | 内联代理池配置。 |
| `api` | 否 | table | 内置 HTTP API / dashboard(需 `dashboard`)。 |
| `event_bus` | 否 | table | 事件总线容量与并发。 |
| `logger` | 否 | table | 日志输出(多输出)。 |
| `policy` | 否 | table | 错误处理策略覆盖。 |

### [db]

`[db]` 段由 schema 要求必须存在,但所有字段皆可选。**不填 `url` 则不使用数据库。** DB 任务模型(`账号 × 平台 × 模块`)需要 **`store`** 特性。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `url` | 否 | string | 连接 URL,如 `postgres://user:pass@host:5432/db` 或 `sqlite://path?mode=rwc`。 |
| `database_schema` | 否 | string | 数据库 schema。 |
| `pool_size` | 否 | integer | 连接池大小。 |
| `tls` | 否 | boolean | 启用 TLS(`sslmode=require`)。 |

### [download_config]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `downloader_expire` | 是 | integer | 下载器实例过期时间(秒)。 |
| `timeout` | 是 | integer | 请求超时(秒)。 |
| `rate_limit` | 是 | float | 全局请求速率限制(QPS);`0` 表示不限制。 |
| `enable_session` | 是 | boolean | 启用会话态同步(headers/cookies)及相关缓存逻辑。 |
| `enable_locker` | 是 | boolean | 启用分布式锁以防止并发冲突。 |
| `enable_rate_limit` | 是 | boolean | 启用限速(依赖 `rate_limit`)。 |
| `cache_ttl` | 是 | integer | 响应缓存 TTL(秒)。 |
| `wss_timeout` | 是 | integer | WebSocket 超时(秒)。 |
| `pool_size` | 否 | integer | HTTP 客户端连接池大小(默认 200)。 |
| `max_response_size` | 否 | integer | 最大响应体大小(字节,默认 10 MB)。 |

#### `enable_session` 行为说明

当 `enable_session = true` 时,下载器会通过缓存持久化会话状态:

- **会话对象:** `SessionState` —— 字段 `session_id`、`module_id`、`headers`、`cookies`、`version`。
- **存储:** 进程内 `CacheService`(本地内存存储)。
- **作用域:** `module_id + run_id`,使同一模块的不同运行批次互不污染。
- **读取(发送前):** 拉取 `SessionState` 并将其 `headers`/`cookies` 合并进请求;请求自带的 `headers`/`cookies` 优先 —— session 仅补充缺失项。
- **写入(响应后):** 从响应更新 `cookies`;仅 `request.cache_headers` 列出的 header 键会写回 session。
- **Host-only Cookie:** 若响应 cookie 未携带 `Domain`,系统会用 `request.url` 的 host 回填后再存储。

需要登录态 / 会话连续性的任务开启它;纯无状态抓取可关闭以减少缓存往返。

### [cache]

进程内的内存缓存(`CacheService`,由本地存储支撑)。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `ttl` | 是 | integer | 默认缓存 TTL(秒)。 |
| `compression_threshold` | 否 | integer | 超过该阈值的缓存载荷将被压缩(字节)。 |

### [crawler]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `request_max_retries` | 是 | integer | 单请求最大重试次数。 |
| `task_max_errors` | 是 | integer | 单任务最大错误数。 |
| `module_max_errors` | 是 | integer | 单模块最大错误数。 |
| `module_locker_ttl` | 是 | integer | 模块锁 TTL(秒)。 |
| `node_id` | 否 | string | 跨重启保持稳定的节点身份。 |
| `task_concurrency` | 否 | integer | 任务处理器并发。 |
| `publish_concurrency` | 否 | integer | 请求发布并发。 |
| `parser_concurrency` | 否 | integer | 解析任务处理器并发。 |
| `error_task_concurrency` | 否 | integer | 错误任务处理器并发。 |
| `backpressure_retry_delay_ms` | 否 | integer | 发生队列背压(full/closed)时的重试延迟(毫秒);不填则用默认重试策略。 |
| `idle_stop_secs` | 否 | integer | 空闲这么多秒后自动停止引擎;`0`/不填为关闭。仅当本地队列为空且 Cron 调度器无运行中任务时才停止。 |

### [scheduler]

Cron 调度。所有字段可选。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `misfire_tolerance_secs` | 否 | integer | Cron 误触发容忍(默认 300)。 |
| `concurrency` | 否 | integer | 计划任务上下文最大并发(默认 100)。 |
| `refresh_interval_secs` | 否 | integer | 调度器缓存刷新间隔(默认 60)。 |
| `max_staleness_secs` | 否 | integer | 强制刷新前的最大陈旧窗口(默认 120)。 |

### [channel_config]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `minid_time` | 是 | integer | MinID/雪花时间基准,用于生成有序 ID。 |
| `capacity` | 是 | integer | 本地内存队列容量;过小会引发背压。 |
| `blob_storage` | 否 | table | 将大载荷落盘(见下)。 |
| `kafka` | 否 | table | Kafka 队列后端(见 [KafkaConfig](#kafkaconfig-共享))。需 `queue-kafka`。 |
| `nats` | 否 | table | NATS JetStream 队列后端(见 [NatsConfig](#natsconfig-共享))。需 `queue-nats`。 |
| `queue_codec` | 否 | string | 远程队列编解码:`json` 或 `msgpack`(须与生产/消费方一致)。 |
| `batch_concurrency` | 否 | integer | 向远程队列批量刷写的最大并发(默认 10)。 |
| `compression_threshold` | 否 | integer | 超过该阈值的队列载荷将被压缩(字节)。 |
| `nack_max_retries` | 否 | integer | 进入 DLQ 前的 NACK 最大重试次数(默认 0)。 |
| `nack_backoff_ms` | 否 | integer | 重试 NACK 前的退避(毫秒,默认 0)。 |

#### [channel_config.blob_storage]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `path` | 否 | string | 大载荷溢写的本地目录。 |

### [sync]

分布式状态同步。所有字段可选。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `kafka` | 否 | table | 同步用 Kafka 配置(见 [KafkaConfig](#kafkaconfig-共享))。需 `queue-kafka`。 |
| `allow_rollback` | 否 | boolean | 是否允许回滚到旧值(默认 true)。 |
| `envelope_enabled` | 否 | boolean | 是否为同步载荷启用版本化 envelope(默认 false)。 |

### [api]

**特性:`dashboard`。** 开启 admin/可观测 HTTP API 与内置单文件 Web UI(指标、日志、任务、性能)。同样的功能也可用 `.dashboard(port)` 以编程方式开启;`[api]` 是其 TOML 等价物。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `port` | 是 | integer | HTTP 服务端口。 |
| `api_key` | 否 | string | 写操作端点的 API key。只读可观测端点无需 key。 |
| `rate_limit` | 否 | float | API 速率限制(QPS)。 |

### [event_bus]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `capacity` | 是 | integer | 事件通道容量(默认 1024)。 |
| `concurrency` | 是 | integer | 事件处理并发(默认 64)。 |

### [logger]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `enabled` | 否 | boolean | 是否启用日志(默认 true)。别名:`enable`。 |
| `level` | 否 | string | 全局日志级别(默认 `info`)。 |
| `format` | 否 | string | 输出格式;`console`/`file` 仅支持 `text`。 |
| `include` | 否 | array | 结构化字段白名单。 |
| `buffer` | 否 | integer | MQ 输出缓冲区大小(默认 10000)。 |
| `flush_interval_ms` | 否 | integer | MQ 输出刷新间隔(默认 500)。 |
| `outputs` | 是 | array | 输出列表(见下);至少配置一个,否则不会有任何输出。 |
| `prometheus` | 否 | table | Prometheus 日志统计配置。 |

#### logger.outputs[]

每个输出按 `type` 区分:`console`、`file` 或 `mq`。

**`type = "console"`** —— 无额外字段。

**`type = "file"`**

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `path` | 是 | string | 文件路径。 |
| `rotation` | 否 | string | 轮转策略(`daily`/`hourly`/`minutely`/`never`)。 |
| `max_size_mb` | 否 | integer | 最大文件大小(保留字段)。 |
| `max_files` | 否 | integer | 最大文件数(保留字段)。 |

**`type = "mq"`**

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `backend` | 是 | string | `kafka`。 |
| `topic` | 是 | string | Topic 名称。 |
| `format` | 否 | string | 仅支持 `json`。 |
| `buffer` | 否 | integer | 缓冲区大小(默认 10000)。 |
| `batch_size` | 否 | integer | 批量大小(保留字段)。 |
| `compression` | 否 | string | 压缩算法(保留字段)。 |
| `kafka` | 否 | table | 当 `backend = "kafka"` 时的 Kafka 配置(见 [KafkaConfig](#kafkaconfig-共享))。 |

#### logger.prometheus

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `enabled` | 是 | boolean | 启用日志统计指标。 |
| `port` | 否 | integer | 指标端口(复用 API/metrics 端口)。 |
| `path` | 否 | string | 指标路径(默认 `/metrics`)。 |

示例(console + file + mq):

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

环境变量 `DISABLE_LOGS` / `MOCRA_DISABLE_LOGS` 可整体禁用日志。

### [policy]

按错误类别覆盖内置的错误处理策略(重试 / 退避 / DLQ / 告警)。唯一的键是 `overrides` —— 一个规则数组(`[[policy.overrides]]`);匹配度最高的规则胜出。标注为「匹配」的字段用于收窄规则适用的错误范围,其余字段覆盖解析出的策略。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `kind` | 是 | string | 匹配:错误类别。取值 `Request`、`Response`、`Command`、`Service`、`Proxy`、`Download`、`Queue`、`Orm`、`Task`、`Module`、`RateLimit`、`ProcessorChain`、`Parser`、`DataMiddleware`、`DataStore`、`DynamicLibrary`、`CacheService` 之一。 |
| `domain` | 否 | string | 匹配:domain,如 `engine`、`system`。 |
| `event_type` | 否 | string | 匹配:事件类型,如 `download`、`parser`。 |
| `phase` | 否 | string | 匹配:生命周期阶段,如 `failed`、`retry`。 |
| `retryable` | 否 | boolean | 覆盖:是否允许重试。 |
| `backoff` | 否 | string / table | 覆盖:`"None"`,或 `{ Linear = { base_ms, max_ms } }`,或 `{ Exponential = { base_ms, max_ms } }`。 |
| `dlq` | 否 | string | 覆盖:DLQ 路由 —— `Never`、`OnExhausted` 或 `Always`。 |
| `alert` | 否 | string | 覆盖:告警级别 —— `Info`、`Warn`、`Error` 或 `Critical`。 |
| `max_retries` | 否 | integer | 覆盖:最大重试次数。 |
| `backoff_ms` | 否 | integer | 覆盖:初始退避(毫秒)。 |

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

内联代理池配置(不再依赖外部代理文件)。所有字段可选。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `tunnel` | 否 | array | 静态隧道代理(见 `[[proxy.tunnel]]`)。 |
| `direct` | 否 | array | 直连 URL 代理(见 `[[proxy.direct]]`);支持 `http/https/ws/wss`。 |
| `ip_provider` | 否 | array | 动态 IP 提供商列表(见 `[[proxy.ip_provider]]`)。 |
| `pool_config` | 否 | table | 代理池策略(见 `[proxy.pool_config]`)。 |

#### [[proxy.direct]]

单个代理 URL,如 `https://127.0.0.1:8888`,无需 provider。`ws`/`wss` 内部会归一为 `http`/`https` 代理通道。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `url` | 是 | string | 代理 URL:`http://`、`https://`、`ws://` 或 `wss://`。 |
| `name` | 否 | string | 代理名称;不填则自动生成(`direct_{index}`)。 |
| `rate_limit` | 否 | float | 单代理 QPS 上限(默认 10)。 |
| `expire_time` | 否 | string | 过期时间(RFC3339);默认远期。 |

#### [[proxy.tunnel]]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `name` | 是 | string | 隧道名称。 |
| `endpoint` | 是 | string | 代理地址(`host:port`)。 |
| `tunnel_type` | 是 | string | 协议类型,如 `http` / `socks5`。 |
| `expire_time` | 是 | string | 过期时间(RFC3339)。 |
| `rate_limit` | 是 | float | 单隧道限速(QPS)。 |
| `username` | 否 | string | 用户名。 |
| `password` | 否 | string | 密码。 |

#### [[proxy.ip_provider]]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `name` | 是 | string | 提供商名称。 |
| `url` | 是 | string | 拉取代理 IP 的接口地址。 |
| `retry_codes` | 是 | array | 触发重新拉取的 HTTP 状态码。 |
| `timeout` | 是 | integer | 提供商请求超时(秒)。 |
| `rate_limit` | 是 | float | 提供商请求限速(QPS)。 |
| `proxy_expire_time` | 是 | integer | 拉取到的代理过期时间(秒)。 |
| `provider_expire_time` | 否 | string | 提供商凭据过期时间(RFC3339)。 |
| `weight` | 否 | integer | 提供商权重(越大越优先)。 |

#### [proxy.pool_config]

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `min_size` | 否 | integer | 池最小容量(默认 5)。 |
| `max_size` | 否 | integer | 池最大容量(默认 50)。 |
| `max_errors` | 否 | integer | 单代理被驱逐前的最大错误数(默认 3)。 |
| `health_check_interval_secs` | 否 | integer | 健康检查间隔(秒,默认 300)。 |
| `refill_threshold` | 否 | float | 触发补充的比例阈值(默认 0.3)。 |

---

## 共享类型 (Shared types)

### KafkaConfig (共享)

**特性:`queue-kafka`。** 用于 `channel_config.kafka`、`sync.kafka` 与 `logger.outputs[].kafka`。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `brokers` | 是 | string | 逗号分隔的 broker 列表。 |
| `username` | 否 | string | SASL 用户名。 |
| `password` | 否 | string | SASL 密码。 |
| `tls` | 否 | boolean | 启用 TLS。 |

### NatsConfig (共享)

**特性:`queue-nats`。** 用于 `channel_config.nats`。

| 键 | 必填 | 类型 | 说明 |
| --- | --- | --- | --- |
| `url` | 是 | string | 服务器地址,如 `nats://127.0.0.1:4222`(逗号分隔多个)。 |
| `username` | 否 | string | 用户名。 |
| `password` | 否 | string | 密码。 |
| `token` | 否 | string | Token 认证。 |

---

## 示例:分布式配置

一份贴近生产形态的配置:PostgreSQL 任务存储、Kafka 数据面队列、dashboard API,以及多路日志。跨节点协调(选主、锁)需在代码中另行开启 —— 启用 `cluster-embedded` 特性并调用 `.cluster(…)`,详见 [部署](deployment.md)。

```toml
name = "crawler"

[api]                       # 需要 `dashboard` 特性
port = 12800

[db]                        # DB 任务模型需要 `store` 特性
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

[channel_config.kafka]      # 需要 `queue-kafka` 特性
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

## 测试配置样例

- [tests/config.test.toml](../../tests/config.test.toml)
- [tests/config.mock.toml](../../tests/config.mock.toml)
- [tests/config.mock.pure.toml](../../tests/config.mock.pure.toml)
- [tests/config.mock.pure.engine.toml](../../tests/config.mock.pure.engine.toml)
- [tests/config.prod_like.toml](../../tests/config.prod_like.toml)
