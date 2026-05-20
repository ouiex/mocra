# mocra 去中心化爬虫架构设计 v2

> 本文档基于 `src/` 实际代码结构撰写，是对当前项目的重构与优化。本项目是一个rust crate包，而非单一二进制应用，因此架构设计更侧重于模块划分、组件交互、数据流动、以及核心机制的实现细节，而非传统意义上的部署架构图。

---

## 目录

1. [项目概述](#1-项目概述)
2. [核心设计原则](#2-核心设计原则)
3. [系统整体架构](#3-系统整体架构)
4. [队列系统](#4-队列系统)
5. [处理管道](#5-处理管道)
6. [分布式协调](#6-分布式协调)
7. [当前实现状态评估](#7-当前实现状态评估)
8. [去中心化改造路径](#8-去中心化改造路径)
9. [Raft 集成设计](#9-raft-集成设计)
10. [通信层设计](#10-通信层设计)
11. [数据模型](#11-数据模型)
12. [模块与中间件](#12-模块与中间件)
13. [API 与控制面](#13-api-与控制面) → 完整规范见 [api-architecture.md](./api-architecture.md)
14. [部署拓扑](#14-部署拓扑)
15. [渐进式演进路径](#15-渐进式演进路径)
16. [最终架构结论](#16-最终架构结论)
17. [代码地图](#17-代码地图)
18. [Cookie 与 Headers：数据结构与手动管理](#18-cookie-与-headers数据结构与手动管理)
19. [Task Profile：配置路由与 Typed Metadata 的统一数据模型](#19-task-profile配置路由与-typed-metadata-的统一数据模型)
20. [ModuleTrait / ModuleNodeTrait 配置与上下文重构](#20-moduletrait--modulenodetrait-配置与上下文重构)

---

## 1. 项目概述

mocra 是一个基于 Rust 实现的**分布式数据采集框架**，具备以下核心特征：

- **模块化任务定义**：通过 `ModuleTrait` 定义采集逻辑，支持线性步骤（`add_step`）与 DAG（`dag_definition`）两种执行模式
- **队列驱动处理**：任务流经 `task → request → response → parser_task → error_task` 五阶段管道
- **Raft + RocksDB 控制面**：统一缓存、协调、锁、限流、状态追踪全部复用同一套强一致状态机
- **消息队列解耦**：`QueueManager` 负责远程消息队列接入，支持优先级 topic、批处理、压缩、补偿回放
- **中间件数据落地**：`DataStoreMiddleware` 三段式生命周期（`before_store / store_data / after_store`）承担所有落地逻辑
- **下载缓存去中心化**：Response Cache 采用“本地磁盘 + Raft 索引 + gRPC 读取”模型，不再依赖外部缓存服务

### 技术栈

| 层次 | 组件 | crate |
|------|------|-------|
| 异步运行时 | Tokio 多线程 | `tokio` |
| HTTP 下载 | reqwest（连接池、代理、TLS）| `reqwest` |
| ORM / DB | SeaORM | `sea-orm` |
| Kafka 客户端 | rdkafka | `rdkafka` |
| 序列化 | serde + MsgPack（默认）/ JSON | `rmp-serde`, `serde_json` |
| Web 框架 | axum | `axum` |
| 一致性复制 | openraft | `openraft` |
| 本地状态存储 | RocksDB | `rocksdb` |
| 分布式锁/限流/缓存 | 统一 CacheService 抽象 | — |
| 并发 Map | DashMap | `dashmap` |
| DAG 执行 | 自实现拓扑排序 + `ModuleDagOrchestrator` | — |
| 日志 | tracing | `tracing` |

---

## 2. 核心设计原则

### 2.1 无中心协调者（目标态）

系统以 **Raft Leader** 作为唯一调度触发者和元数据提交入口，不再依赖外部分布式协调服务。

### 2.2 命名空间隔离与联邦

### 2.2.1 `config.name` 的命名空间语义

`config.name` 不是普通实例名，而是**任务命名空间根（Task Namespace Root）**。每个任务拥有独立的 `config.toml`，`config.name` 是该任务所有分布式状态的 key 前缀，负责在共享基础设施上实现完整的状态隔离。

**命名空间隔离约束：**

- **同一任务** 的所有节点（无论哪台机器）必须使用同一个 `config.name`
- **不同任务** 即使共享同一套消息队列、数据库、下载池基础设施，也必须使用不同的 `config.name`
- `config.name` 不是 `node_id`、不是 hostname、也不是模块名

它是以下状态键空间的统一根前缀：

- cache keys
- node registry keys
- pause / resume keys
- distributed lock keys
- rate limit keys
- queue-related distributed state
- **Raft 元数据键空间**（如启用）

**所有写入 Raft 的元数据都必须先按 `config.name` 做命名空间隔离**，否则不同任务会共享同一套元数据视图。

### 2.2.2 命名空间联邦（Namespace Federation）

不同命名空间虽然状态完全隔离，但**可以在同一套分布式网络基础设施上组成联邦（Federation）**，共同提供处理能力。

#### 联邦的核心设计原则

| 维度 | 隔离 / 独立 | 可跨命名空间共享 |
| --- | --- | --- |
| `config.toml` | ✅ 每个任务独立 | — |
| 状态/元数据 | ✅ 按 `config.name` 完全隔离 | — |
| Leader 选举 | ✅ 每个命名空间独立选 leader | — |
| 调度触发 | ✅ 由所属命名空间的 leader 负责 | — |
| `generate()` 执行 | ✅ 由拥有该模块代码的命名空间节点执行 | — |
| `parser()` 执行 | ✅ 由拥有该模块代码的命名空间节点执行 | — |
| **下载（HTTP 请求）** | — | ✅ **可跨命名空间池化共用** |
| 消息队列 / 数据库 / Download Pool | — | ✅ 可共享同一套基础设施 |

#### 联邦中的职责划分

```text
┌──────────────────────┐    ┌──────────────────────┐
│   Namespace A        │    │   Namespace B        │
│   config.name = 'A'  │    │   config.name = 'B'  │
│                      │    │                      │
│  Leader(A)           │    │  Leader(B)           │
│   └─ 触发调度         │    │   └─ 触发调度         │
│  Module nodes(A)     │    │  Module nodes(B)     │
│   └─ generate()      │    │   └─ generate()      │
│   └─ parser()        │    │   └─ parser()        │
└──────────┬───────────┘    └──────────┬───────────┘
           │   Request（含命名空间标记，路由到共享下载层）  │
           └───────────────┬───────────────────────┘
                           ▼
              ┌─────────────────────────┐
              │   Shared Download Pool  │
              │   （任意联邦节点均可处理）  │
              │   DownloadProcessor     │
              └───────────┬─────────────┘
                          │ Response（携带原命名空间标记）
                          ▼
           ┌──────────────┴───────────────┐
           │  路由回各自命名空间的 parser 节点  │
           │  A.parser() / B.parser()     │
           └──────────────────────────────┘
```

#### 联邦规则

1. **Leader 触发调度**：每个命名空间由本命名空间选出的 leader 触发 cron / 手动任务注入，不依赖其他命名空间的 leader
2. **`generate()` 归属命名空间**：`generate()` 需要模块代码，只能由持有该模块的命名空间节点执行
3. **download 跨命名空间共享**：`Request` 发往联邦共享的下载队列 topic，任何联邦节点的 `DownloadProcessor` 均可处理；`Request` 消息中必须携带 `namespace` 标记
4. **`parser()` 归属命名空间**：`Response` 返回后根据携带的 `namespace` 标记路由回所属命名空间节点，由该节点的 `parser()` 逻辑解析
5. **状态始终归属命名空间**：所有执行状态 key、队列 ack/nack 记录、DLQ 条目都带 `config.name` 前缀，不同命名空间不互扰

### 2.3 处理管道不可变性

任务一旦进入管道，状态迁移严格遵循 `task → request → response → parser_task / error_task` 的单向流动，不允许回流。

### 2.4 完整 Config 结构

以下为 **v2 目标态** `config.toml` 示例，展示“Raft + RocksDB + QueueManager”方案下的配置语义：

```toml
# ── 顶层 ──────────────────────────────────────────────────────────
name = "my_task"          # Config.name，命名空间根；所有分布式 key 的公共前缀

# ── 数据库（DatabaseConfig）──────────────────────────────────────
[db]
url              = "postgres://user:pass@localhost:5432/mydb"
database_schema  = "public"
pool_size        = 5
tls              = false

# ── 下载器（DownloadConfig）──────────────────────────────────────
[download_config]
downloader_expire  = 3600     # 下载器实例过期时间（秒）
timeout            = 30       # 单次 HTTP 请求超时（秒）（④ 层全局默认，可被 task_profile / ModuleTrait 覆盖）
rate_limit         = 0.0      # 全局限速（req/s），0 = 不限速；各模块通过 ModuleTrait::rate_limit() 或 task_profile 覆盖
# serial_execution（原 enable_locker）已移至 ModuleTrait 层，不再在此配置
cache_ttl          = 86400    # Response Cache 默认文件 TTL（秒）；Request.enable_response_cache*(ttl) 未指定时使用，默认 24h
wss_timeout        = 60       # WebSocket 连接超时（秒）
pool_size          = 200      # HTTP 连接池大小
max_response_size  = 10485760 # 最大响应体大小（字节，默认 10MB）

# ── 通用缓存 / 协调层（CacheConfig；基于 Raft + RocksDB）────────────
[cache]
backend                = "raft_rocksdb"
ttl                    = 3600   # 通用 KV 默认 TTL（秒）
enable_l1              = true   # 是否启用 L1 本地缓存
l1_ttl_secs            = 30     # L1 TTL（秒）
l1_max_entries         = 10000  # L1 最大条目数
lease_ttl_ms           = 10000  # 分布式锁 / leader lease 默认 TTL（ms）
counter_shards         = 64     # 计数器/限流分片数
data_dir               = "./cache_data" # 实际路径 cache_data/{config.name}/

# ── 爬虫行为（CrawlerConfig）──────────────────────────────────────
[crawler]
request_max_retries       = 3      # 单条请求最大重试次数
task_max_errors           = 5      # 单个任务最大错误次数（超出 → 终止）
module_max_errors         = 10     # 单个模块最大累计错误数
module_locker_ttl         = 60     # 模块级分布式锁 TTL（秒）
node_id                   = ""     # 节点 ID，留空则自动生成（hostname+PID）
task_concurrency          = 4      # 任务处理并发数
publish_concurrency       = 4      # 请求发布并发数
parser_concurrency        = 4      # 解析任务并发数
error_task_concurrency    = 2      # 错误任务处理并发数
backpressure_retry_delay_ms = 100  # 背压检测到时的等待时间（ms）
idle_stop_secs            = 300    # 本地队列空闲多久后停止 Engine（秒，0=不停）

# ── 消息通道（ChannelConfig）──────────────────────────────────────
[channel_config]
backend                = "kafka"    # 远程消息队列后端，也可替换为其他 MqBackend
capacity               = 1000       # 本地 Tokio channel 缓冲容量
queue_codec            = "msgpack"  # 远程队列编码："msgpack" | "json"
batch_concurrency      = 10         # 批次 flush 并发数
compression_threshold  = 4096       # Payload 压缩阈值（字节）
nack_max_retries       = 3          # NACK 最大重试次数（超出 → DLQ）
nack_backoff_ms        = 500        # NACK 重试退避（ms）
reclaim_timeout_ms     = 600000     # Broker 原生 redelivery / reclaim 超时时间
listener_count         = 4          # 远程订阅并发监听数

[channel_config.kafka]
brokers            = ["127.0.0.1:9092"]
topic_prefix       = "mocra"
consumer_group     = "mocra"
security_protocol  = "plaintext"

# ── 调度器（SchedulerConfig，可选）──────────────────────────────────
[scheduler]
misfire_tolerance_secs  = 300   # 错过触发的容忍秒数（默认 300）
concurrency             = 100   # 调度上下文并发数
refresh_interval_secs   = 60    # 调度缓存刷新间隔（秒）
max_staleness_secs      = 120   # 强制刷新前的最大过期时间（秒）

# ── 同步（SyncConfig，可选）──────────────────────────────────────
[sync]
envelope_enabled = false   # 是否启用版本 envelope 封装

# ── HTTP API（可选）──────────────────────────────────────────────
[api]
port      = 8080
api_key   = "secret"      # 保护受限路由（Debug 输出中自动脱敏）
rate_limit = 100.0        # API 限速（req/s）

# ── EventBus（可选）──────────────────────────────────────────────
[event_bus]
capacity    = 1024   # broadcast channel 容量
concurrency = 64     # 事件 handler 并发数

# ── Raft 控制面（可选）───────────────────────────────────────────
[raft]
addr                  = "0.0.0.0:7001"       # 本节点 Raft gRPC 监听地址
peers                 = ["192.168.1.1:7001"] # 任意一个现有节点地址即可加入
heartbeat_interval_ms = 500
election_timeout_ms   = 1500
snapshot_interval     = 500
data_dir              = "./raft_data"        # 实际路径 data_dir/{config.name}/
```

**配置节与结构体的对应关系：**

| TOML 节 | Rust 结构体 | 是否必填 |
|---------|-----------|---------|
| 顶层 `name` | `Config.name` | ✅ 必填 |
| `[db]` | `DatabaseConfig` | ✅ 必填 |
| `[download_config]` | `DownloadConfig` | ✅ 必填 |
| `[cache]` | `CacheConfig` | ✅ 必填 |
| `[crawler]` | `CrawlerConfig` | ✅ 必填 |
| `[channel_config]` | `ChannelConfig` | ✅ 必填 |
| `[channel_config.kafka]` | `KafkaConfig` | 视 `backend` 而定 |
| `[scheduler]` | `Option<SchedulerConfig>` | 可选 |
| `[sync]` | `Option<SyncConfig>` | 可选 |
| `[api]` | `Option<Api>` | 可选 |
| `[event_bus]` | `Option<EventBusConfig>` | 可选 |
| `[raft]` | `Option<RaftConfig>` | 可选 |

**当前代码中的运行模式判定与目标态约束：**

1. `Config::is_single_node_deployment()` 现在只表达本地部署标签，不再由外部 KV 配置派生。
2. `[raft]` 是控制面配置入口；`[cache].backend = "raft_rocksdb"` 时 cache / lock / rate-limit / status 共享 `ProfileControlPlaneStore`。
3. 当前 `Config` 不再保留旧外部 KV 后端、队列补偿后端和 cookie 后端兼容字段；新增配置应进入 typed profile、queue、cache 或 raft 的明确边界。

---

## 3. 系统整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                        mocra Node                           │
│                                                             │
│  ┌──────────┐   ┌──────────────────────────────────────┐   │
│  │  State   │──▶│              Engine                  │   │
│  │          │   │                                      │   │
│  │ Config   │   │  QueueManager  ProcessorRunner       │   │
│  │ CacheSvc │   │  TaskManager   DownloaderManager     │   │
│  │ DBConn   │   │  MiddlewareMgr CronScheduler         │   │
│  │ CookieKV │   │  NodeRegistry  StatusTracker         │   │
│  │ LockSvc  │   │  LeaderGuard   EventBus              │   │
│  │ RateLim  │   │  API / Metrics LuaRegistry           │   │
│  └──────────┘   └──────────────────────────────────────┘   │
│                                 │                           │
│               ┌─────────────────┼──────────────────┐       │
│               ▼                 ▼                  ▼       │
│        ┌─────────┐      ┌────────────┐     ┌──────────┐    │
│        │  Local  │      │   Remote   │     │  HTTP    │    │
│        │Channels │      │Queue(MQ)   │     │  API     │    │
│        │(Tokio)  │      │            │     │ Server   │    │
│        └─────────┘      └────────────┘     └──────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 3.1 State — 当前代码中的资源装配入口

`State`（`src/common/state.rs`）仍是整个运行时的组合根；当前控制面已经收口到 `Raft + RocksDB` / `ProfileControlPlaneStore`。

`State::try_new_with_provider()` 的当前装配顺序大致如下：

1. 读取配置，并通过显式 cache backend / raft 配置派生当前 runtime capability。
2. 初始化数据库连接（当前代码使用 **SeaORM** `DatabaseConnection`，而不是单独的 SQLx pool 抽象）。
3. 打开 `ProfileControlPlaneStore`，并按 `[cache].backend` 构造 `CacheService`。
4. `raft_rocksdb` 模式下注入 `RaftLockBackend` 与 `RaftRateLimitBackend`；本地模式使用进程内 backend。
5. 构造 `DistributedLockManager`、下载限流器、可选 API 限流器、`StatusTracker` 和 cookie service。
6. 基于 `CacheService + DistributedLockManager` 初始化运行时状态跟踪。
7. 启动配置 watcher；当配置热更新时，动态刷新内存中的 `Config` 和限流阈值。

需要特别注意：当前 `State` 的 watcher 还不是“全量运行时重绑定”。  
它目前只会：

- 更新 `Arc<RwLock<Config>>` 中的配置快照；
- 调用下载限流器和 API 限流器的 `set_all_limit(...)` 推送新的阈值。

它**不会**在运行中重建：

- `QueueManager`
- `CacheService` / `cookie_service`
- `DistributedLockManager`
- MQ backend
- API listener 端口

因此，当前真正支持在线生效的主要仍是**业务行为配置和限流阈值**，而不是 queue codec / backend / cache topology / 监听端口这类基础设施配置切换。

### 3.2 Engine — 主协调器与运行时装配

`Engine`（`src/engine/engine.rs`）当前真实持有的核心组件包括：

- `QueueManager`：统一的本地/远程消息入口
- `TaskManager`：模块注册、Task 装配、DAG 预编译与切换门控
- `DownloaderManager`：下载器实例生命周期
- `MiddlewareManager`：下载/数据/存储中间件注册与调度
- `NodeRegistry`：节点心跳与活跃节点视图
- `CronScheduler`：分布式 cron 调度
- `EventBus`：进程内事件总线（可选）
- `shutdown_tx` / `pause_tx` / `prometheus_handle` / `inflight_counter`：运行时控制与观测组件

其中 `pause_tx` 现在由控制面 `ProfileControlPlaneStore` 的本地 `watch::channel<bool>` 驱动：  
`/control/pause`、`/control/resume` 或 Raft 状态机 apply 一旦更新 pause 状态，就会直接把变更推送到 `Engine`，因此当前 pause/resume 已经不是 5 秒轮询，而是**事件驱动的本地传播**。

#### 3.2.1 `Engine::start()` 的实际启动顺序

当前代码中的启动不是单个 `start_work()` 调用，而是 `State::new()` → `Engine::new()` → `engine.start()` 的多阶段流程：

1. 若配置了 `[api]`，启动 axum API。
2. 若启用 `EventBus`，启动进程内 typed event bus。
3. 启动下载器后台清理器、Ctrl+C 监听、zombie cleaner、system monitor、idle-stop watcher。
4. 启动 `CronScheduler`。
5. 构造 `UnifiedTaskIngressChain`、download chain、parser chain 等责任链。
6. 并发拉起 `TaskProcessor / DownloadProcessor / ParserProcessor / ErrorProcessor / HealthMonitor`。

`HealthMonitor` 也需要和 HTTP `/health` 区分开看：  
当前 `start_health_monitor()` 每 30 秒主要做两件事：

1. 清理限流器中的过期键；
2. 向 `EventBus` 发布 `SystemHealth` 事件。

它**不会**直接驱动 `/health` 路由的返回值；`/health` 仍由 API handler 单独检查 `cache_service` 和 DB。

#### 3.2.2 Processor 的运行监管

`Engine::start()` 对各 processor 外层再包一层“panic 后重启”保护：

- 单次 panic 后按指数退避重启；
- 在短时间内连续 panic 达到阈值后进入冷却窗口；
- 这一层和 `ProcessorRunner` 的有界并发职责分离：前者负责**进程级韧性**，后者负责**队列消费并发控制**。

---

## 4. 队列系统

### 4.1 QueueManager 架构

`QueueManager`（`src/queue/manager.rs`）桥接本地 Tokio channel 与远程消息队列 backend（`MqBackend`）。

主要职责：
- 订阅并发布各优先级 topic
- DLQ（死信队列）/ NACK 处理
- Payload 压缩（`zstd`，可选，超阈值才压缩）
- 大体积 Payload 转存 Blob 存储（可选）
- 编解码（MsgPack 默认，可切换 JSON）

#### 4.1.1 本地队列与远程队列的桥接形态

当前代码里，processor 永远只消费本地 `Channel`；是否经过远端 MQ，由 `QueueManager::get_*_push_channel()` 决定：

| 阶段 | push API | 无远端 backend | 有远端 backend |
|------|----------|----------------|----------------|
| Task | `get_task_push_channel()` | 直接写 `task_sender` | 先写 `remote_task_sender`，再由 forwarder 发往 MQ |
| Request | `get_request_push_channel()` | 直接写 `download_request_sender` | 写 `request_sender`，再由 forwarder 发往 MQ |
| Response | `get_response_push_channel()` | 直接写 `remote_response_sender` | 写 `response_sender`，再由 forwarder 发往 MQ |
| ParserTask | `get_parser_task_push_channel()` | 直接写 `remote_parser_task_sender` | 写 `parser_task_sender`，再由 forwarder 发往 MQ |
| ErrorTask | `get_error_push_channel()` | 直接写 `remote_error_sender` | 写 `error_sender`，再由 forwarder 发往 MQ |

`QueueManager::subscribe()` 当前会启动：

- **6 条出站 forwarder**：`task / request / response / parser_task / error_task / log`
- **5 组入站订阅**：`task / request / response / parser_task / error_task`

其中 `request` 的远端入站统一落到 `download_request_sender`，因此 DownloadProcessor 不需要关心消息到底来自本地还是远端。

### 4.2 优先级队列

支持 3 档优先级（`High / Normal / Low`），逻辑上每个优先级对应独立的远程 topic：

```
{namespace}-task-high
{namespace}-task-normal
{namespace}-task-low
```

文档层可以继续把它理解成“`topic_base + priority`”的抽象命名；当前代码里的物理 topic / stream key 则是由 backend 再拼入命名空间前缀。

当前远程 consumer group 由 MQ backend 决定；Kafka 后端使用 `{namespace}-crawler_group`。

模块级优先级通过 `ModuleTrait::priority_level()` 声明，映射关系如下：

| ModuleTrait | Request / Queue |
|------------|-----------------|
| `PriorityLevel::Height` | `Priority::High` → `...-high` |
| `PriorityLevel::Middle` | `Priority::Normal` → `...-normal` |
| `PriorityLevel::Low` | `Priority::Low` → `...-low` |

### 4.3 NACK 策略与 QueueManager 原生补偿队列

NACK 重试行为由 `ChannelConfig` 中的两个字段驱动：

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `nack_max_retries` | 0 | NACK 最大重试次数，超出后投入 DLQ |
| `nack_backoff_ms` | 0 | 每次 NACK 重试前的退避等待（ms） |

补偿队列的职责不是普通重试，而是为**“消息已从主队列取出，但处理节点在 ack 前崩溃 / 卡死”**提供最后一道恢复安全网。

#### 4.3.1 核心思路

补偿机制不再依赖独立存储，而是被定义为 **QueueManager 管理下的一组专用补偿 topic**。  
补偿数据与主数据流共享同一个 `MqBackend`，复用现有的：

- 优先级 topic 路由
- 批量编码 / 压缩
- Blob offload
- 远程订阅 / 回放能力

补偿不再是“可删除的 KV 条目”，而是“可回放的事件流”。

#### 4.3.2 事件模型

```rust
pub enum CompensationEventKind {
    Begin,   // 某条消息开始进入本节点处理
    Done,    // 已成功处理完成
    Cancel,  // 明确放弃（例如正常 NACK，交回主队列重试）
}

pub struct CompensationEvent {
    pub op_id:        String,   // 全局唯一处理流水号
    pub source_topic: String,   // task / request / response / parser_task / error_task
    pub priority:     Priority,
    pub message_id:   String,
    pub payload:      Option<Vec<u8>>,   // 或 blob storage 引用
    pub created_at:   i64,
    pub deadline_at:  i64,      // 超过该时间仍未 Done/Cancel 则触发回放
    pub node_id:      String,
    pub kind:         CompensationEventKind,
}
```

#### 4.3.3 补偿 topic 设计

```
{config.name}:queue:comp_begin:high
{config.name}:queue:comp_begin:normal
{config.name}:queue:comp_begin:low
{config.name}:queue:comp_done
```

- `comp_begin:*`：记录“某条消息已进入处理”
- `comp_done`：记录 `Done / Cancel` 事件，作为完成标记
- payload 过大时，继续复用 QueueManager 已有的 Blob offload 机制

#### 4.3.4 工作流程

```text
主队列消息到达
    │
    ├─ 1. QueueManager 解码消息
    │
    ├─ 2. 先发布 CompensationEvent(Begin) 到 comp_begin:{priority}
    │
    ├─ 3. 再把消息投递给本地 ProcessorRunner
    │
    ├─ 4a. 成功
    │      ├─ ack 原消息
    │      └─ 发布 CompensationEvent(Done) 到 comp_done
    │
    ├─ 4b. 正常失败 / NACK
    │      ├─ nack 原消息（交回主队列正常重试链路）
    │      └─ 发布 CompensationEvent(Cancel) 到 comp_done
    │
    └─ 4c. 节点崩溃 / 卡死
           └─ 没有 Done / Cancel
                → CompensationReplayer 超时后回放到原始 topic
```

#### 4.3.5 CompensationReplayer

新增后台 `CompensationReplayer`，它本身也通过 QueueManager 订阅补偿 topic：

1. 消费 `comp_begin:*`，把 `op_id -> CompensationEvent` 写入本地轻量状态（建议 RocksDB / WAL）
2. 消费 `comp_done`，把对应 `op_id` 标记完成并从本地状态删除
3. 定期扫描超时未完成的 `op_id`
4. 对超时项重新发布原 payload 到 `source_topic:{priority}`

这样可以做到：

- **补偿数据也走统一消息中间件**
- **不需要独立外部补偿存储**
- **与 QueueManager 的优先级、批处理、压缩、Blob offload 完全复用**

#### 4.3.6 与 broker 原生 redelivery 的关系

如果底层消息 broker 已经支持“未 ACK 消息重新认领 / redelivery”，  
则 `CompensationReplayer` 不应立即重发，而应优先等待 broker 原生恢复机制生效。  
只有在超出 `reclaim_timeout_ms` / `lease_timeout` 后仍未恢复时，才执行补偿回放，避免重复投递。

#### 4.3.7 当前代码中的实现状态

当前代码已删除外部 KV 补偿实现，远程 queue backend 默认安装 `QueueNativeCompensator`：

- `QueueManager` 在订阅侧反序列化成功后，通过 `Compensator::add_task(topic, id, payload)` 记录“进入处理”。
- 各 processor 成功完成后，通过 `comp.remove_task(topic, id)` 删除补偿记录。
- `QueueNativeCompensator` 提供 `scan_incomplete()` 与 `drain_pending()`，用于启动恢复和 crash/replay 测试。
- 当前补偿状态仍是进程内 pending map；目标态如需跨进程恢复，应把 pending log 落到 queue-native event topic 或 RocksDB/WAL。

### 4.4 去重功能（暂时移除）

去重功能在当前方案中**暂时删除**，后续如需恢复，将基于统一 `CacheService` 重新设计，而不是单独引入额外存储层。

### 4.5 队列编解码与 Payload 卸载

**编解码**（`ChannelConfig.queue_codec`）：

| 值 | 格式 | 场景 |
|----|------|------|
| `"msgpack"` | 二进制，紧凑 | 默认，推荐生产 |
| `"json"` | 文本，可读 | 调试、跨语言 |

**Payload 卸载**（`blob_storage.enable = true`）：

当序列化后 payload > `blob_storage.threshold`（默认 64KB）时，`QueueManager` 将 payload 写入 Blob 存储，队列消息中仅保留引用 URI。消费端自动拉取还原。

**当前实现中的批处理与消息头细节：**

- 出站 forwarder 通过 `Batcher` 以 **500 条或 5ms** 为窗口刷批。
- 入站订阅通过 `Batcher` 以 **50 条或 5ms** 为窗口解批。
- 当批次达到 32 条以上，或总 payload 接近 64KB 时，编码/解码会切到 blocking 线程池，避免阻塞 async runtime。
- `queue_codec` 当前通过进程级 `OnceCell` 在第一次 `QueueManager::from_config(...)` 时确定；后续配置热更新**不会在线切换**编解码路径，因此 JSON/MsgPack 切换仍属于滚动重启级变更。
- 队列元数据头当前至少包含：
  - `x-attempt`：当前重试次数
  - `x-created-at`：消息初始入队时间
  - `x-nack-reason`：NACK 时补充的失败原因
- 反序列化失败的 poison message 会被立即 NACK，而不是伪装成成功消费。
- `QueueManager` 还提供 `try_send_local_response()` 这样的本地快路径；在“响应下一跳仍在本机可消费”时，可以绕过远端 MQ，减少一次序列化/网络往返。

---

## 5. 处理管道

### 5.1 管道阶段

```
TaskModel
    │  (task channel)
    ▼
RequestModel  ◀── generate() ── Module
    │  (request channel)
    ▼
ResponseModel ◀── download()
    │  (response channel)
    ▼
ParserTaskModel ◀── parser() ── Module
    │  (parser_task channel)
    ▼
ErrorTaskModel  (error channel, 失败分支)
```

当前代码中的真实载体名称为：

- `TaskEvent`
- `Request`
- `Response`
- `TaskParserEvent`
- `TaskErrorEvent`

文档中的 `TaskModel / RequestModel / ResponseModel / ParserTaskModel / ErrorTaskModel` 是架构概念名，用来描述目标态数据契约。

#### 5.1.1 上下文传播规则

当前实现里，跨阶段上下文仍有一部分通过 `MetaData` 兼容传播；目标态会把它拆成边界清晰的几类结构：

- `RoutingMeta`：`namespace/account/platform/module/node_key/run_id/request_id/parent_request_id/priority`，负责路由和归属
- `ExecutionMeta`：`retry_count/task_retry_count/profile_version/trace_id/fence_token/created_at` 等执行态字段
- `ResolvedNodeConfig`：当前节点绑定的不可变配置快照，随 `profile_version` 固定
- `NodeInput`：上游节点传给下游节点的业务参数，载体为 `TypedEnvelope`
- `context / prefix_request`：当前代码仍保留的执行串联字段，迁移阶段继续兼容

因此，新的节点状态应优先进入 `RoutingMeta / ExecutionMeta / NodeInput`；若按当前开发路线直切实施，`MetaData` 可以直接退出运行时主路径，而不是继续保留为长期兼容壳。

### 5.2 模块 DAG 执行

当前主链路里，DAG 的“图定义编译”由 `TaskManager + ModuleDagOrchestrator` 负责，“单次 run 的节点推进”由 `ModuleDagProcessor` 负责；目标态在此之上再叠加 `src/schedule/dag/*` 中的 `DagScheduler` 作为分布式调度外壳。三层职责如下：

| 层 | 当前主实现 | 目标态增强 |
|----|------------|-----------|
| DAG 定义层 | `ModuleTrait::dag_definition()` + `add_step()` → `ModuleDagOrchestrator` 编译 | 继续保留，作为唯一 DAG 声明入口 |
| DAG 运行层 | `ModuleDagProcessor` 根据 `ExecutionMark.node_id` / `step_idx` 推进节点 | 升级为 typed context + `NodeDispatch`，减少隐式 `Value` 元数据 |
| 分布式调度层 | 当前主要依靠 QueueManager + 队列阶段串联 | 由 `DagScheduler + DagNodeDispatcher + DagRunGuard + DagRunStateStore + DagFencingStore` 承担 run 级一致性与远程放置 |

当前需要特别注意一条实现边界：`ModuleDagCompiler` 已经能把 `ModuleNodeTrait` 包装成 scheduler DAG 节点，而 `ModuleNodeDagAdapter` 现在已经能在 `NodeExecutionContext.runtime_input` 提供 generate/parser 所需 typed runtime 输入时执行真实节点逻辑。scheduler runtime override 也可以按 `node_id` 注入这份 opaque payload，`ModuleDagOrchestrator` 已提供 generate/parser 专用 helper 来完成这一步；同时，`Module::generate()` 与 `Module::parser()` 都已经会把当前 target node 编译成单节点 scheduler DAG，并通过 bridge 执行。当前仍未完成的是“整张 DAG 自动 runtime input 填充与全链路 cutover”而非单节点 bridge 本身。因此：

- `DagScheduler` 可以安全承接 placement / policy / run guard / fencing / state store 这类运行时控制语义
- `NodeExecutionContext` 已经预留了一个与 `common::model` 解耦的 opaque runtime input 通道，并且可通过 runtime override 把 typed engine runtime 封装后按节点注入，再跨本地/远程 worker 传递
- `ModuleNodeDagAdapter` 已经能消费这份 opaque runtime input 执行真实 `generate()` / `parser()`，并分别编码 request batch / parser output payload
- `ModuleDagOrchestrator` 已经能为指定 node 注入 generate/parser runtime input，所以“compiled DAG + target node + typed runtime payload”链路可以单点跑通
- `Module::generate()` 与 `Module::parser()` 已接入单节点 scheduler bridge；remote placement 下会依赖 `DagNodeDispatcher` 执行，未配置 dispatcher 时显式 fail closed
- 本地 placement 下，generate/parser 在未配置 `dag_dispatcher` 时可由 scheduler 直接本地执行；parser local scheduler path 现已可编码 `NodeDispatch + data + finished` 输出，且若当前 parser target 已无法解析成 scheduler node 也会直接 fail-closed；generate 侧若 pending target 已无法解析成 scheduler node 也同样直接 fail-closed；一旦 scheduler bridge 已接管却执行失败，generate/parser 仍均 fail-closed，不再静默回退到旧本地执行路径；remote placement 同样保持 fail-closed

`ModuleDagOrchestrator`（`src/engine/task/module_dag_orchestrator.rs`）合并 `ModuleTrait::dag_definition()` 与 `add_step()` 两种定义方式：

- 若模块同时定义了 DAG 和线性步骤，当前以 `dag_definition()` 为准；`add_step()` 只在没有 custom DAG 时生效
- 节点执行并发度由 DAG 拓扑决定（无依赖节点并行）
- 入口节点优先使用 `definition.entry_nodes`；若未显式声明，则自动选择“无前驱节点”的节点集合

#### 5.2.1 编译边界与运行绑定

**TaskManager DAG 编译与切换**（`src/engine/task/task_manager.rs`）：

```rust
// 统一编译模块 DAG：同时覆盖 custom DAG、linear steps 和混合定义
pub async fn compile_module_dag(&self, module_name: &str) -> Result<Dag, DagError>
```

一次分布式 DAG run 在目标态里应固定以下绑定关系：

1. **命名空间与模块运行时**：由 `config.name + account + platform + module` 唯一确定。
2. **配置版本**：run 创建时读取 `TaskProfileSnapshot.version`，并写入 `ExecutionMeta.profile_version`；进行中的 run 不允许隐式穿透到新版本。
3. **DAG 版本**：由 `TaskManager` 预编译结果或 cutover 后的新图决定，必须与 `profile_version` 一起形成 run 的不可变执行视图。
4. **run_id**：作为一次 DAG 执行实例的全局主键；所有 gate、stop signal、resume state、fencing commit 都按 `run_id` 作用域隔离。

当前代码有一个需要显式记录的 caveat：`Module` 运行时可能来自 factory cache，而 `ModuleDagProcessor` 内部持有的 `run_id` 不是天然随每次 `load_parser_model / load_error_model` 同步更新。  
这也是为什么目标态应把 run 级状态（resume state、stop signal、fencing token）外置到 `DagRunStateStore` / 控制面，而把 `ModuleDagProcessor` 收敛为“拓扑 + 路由规则容器”。

#### 5.2.2 分布式调度职责

在分布式模式下，DAG 调度不再只是“哪个节点先跑”，而是要同时解决**run 所有权、节点放置、恢复点、幂等和推进去重**。目标态建议把这些责任收敛为以下抽象：

| 构件 | 目标职责 | 当前代码线索 |
|------|----------|-------------|
| `DagRunGuard` | 保障同一 `run_id` 只被一个调度实例持有 | `src/schedule/dag/types.rs` |
| `DagFencingStore` | 为节点提交分配单调递增 token，避免旧执行覆盖新结果 | `src/schedule/dag/types.rs` |
| `DagRunStateStore` | 保存恢复点（已成功节点输出、run token、run_id） | `src/schedule/dag/types.rs` |
| `DagNodeDispatcher` | 决定节点是本地执行还是远程派发 | `src/schedule/dag/types.rs` |
| `NodePlacement` | `Local` / `Remote { worker_group }` 节点放置策略 | `src/schedule/dag/types.rs` |
| `DagNodeExecutionPolicy` | 节点级 timeout / retry / idempotency / circuit breaker | `src/schedule/dag/types.rs` |

职责边界建议如下：

- **Leader 节点**：创建 run、绑定 `profile_version`、获取 run guard、写初始 resume state、投递 entry node。
- **Worker 节点**：消费 task/parser_task，执行 `generate()` / `parser()`，推进 DAG，提交节点结果。
- **共享控制面（Raft + RocksDB）**：保存 `TaskProfileSnapshot`、run guard、resume state、advance gate、stop signal、fencing token。
- **共享数据面（MQ / Download Pool）**：承载 task / parser_task / request / response / error 队列，以及跨节点下载能力。

#### 5.2.3 分布式执行流程图

下面的流程图描述了**单命名空间内一次 DAG run** 的标准分布式执行路径；若开启联邦共享下载池，`Download Pool` 可以由其他命名空间节点承载，但 `generate()` / `parser()` 仍回到归属命名空间。

```mermaid
flowchart TD
    subgraph CP["Leader 节点（命名空间控制面）"]
        A1["CronScheduler / API<br/>start_work"]
        A2["TaskManager<br/>读取 TaskProfileSnapshot<br/>绑定 profile_version"]
        A3["ModuleDagOrchestrator<br/>获取/编译 DAG"]
        A4["DagRunGuard / Fencing<br/>申请本次 run 所有权"]
        A5["写入 DagRunState<br/>(run_id, entry_nodes, succeeded_outputs=空)"]
        A6["投递入口节点<br/>TaskEvent / TaskParserEvent"]
    end

    subgraph MQ["共享数据面（QueueManager + MQ）"]
        Q1["task / parser_task topic"]
        Q2["request topic"]
        Q3["response topic"]
        Q4["error / retry topic"]
    end

    subgraph WK["执行节点（同命名空间，多副本）"]
        W1["TaskProcessor<br/>load Module + ResolvedNodeConfig"]
        W2["ModuleDagProcessor.execute_generate<br/>按 node_id 定位当前节点"]
        W3["Downloader / Shared Download Pool"]
        W4["ResponseParserProcessor"]
        W5["ModuleDagProcessor.execute_parse<br/>生成 next / retry / stop"]
    end

    subgraph ST["Raft + RocksDB 状态"]
        S1["TaskProfileSnapshot"]
        S2["DagRunStateStore"]
        S3["AdvanceGate / StopSignal"]
        S4["StatusTracker / FencingStore"]
    end

    S1 -. 本地读 .-> A2
    S4 -. guard / token .-> A4
    A1 --> A2 --> A3 --> A4 --> A5 --> A6 --> Q1
    S2 -. 保存初始恢复点 .-> A5

    Q1 --> W1 --> W2 --> Q2
    Q2 --> W3 --> Q3
    Q3 --> W4 --> W5

    W5 -->|后继节点推进| Q1
    W5 -->|解析失败 / 同节点重试| Q4
    Q4 --> Q1

    W4 -. 阶段状态更新 .-> S4
    W5 -. 成功节点输出 / resume snapshot .-> S2
    W5 -. advance gate / stop signal .-> S3
```

#### 5.2.4 节点推进规则

`ModuleDagProcessor` 当前已经实现了较完整的节点推进语义；目标态应保留这些规则，但把上下文从 `Value` 迁移到 typed metadata：

1. **当前节点定位**
   - 首选 `ExecutionMark.node_id`
   - 目标态不再依赖 `step_idx` 作为长期后备定位；若 `node_id` 缺失，应视为非法上下文并尽早失败
   - 仅在 run 初始化阶段允许由 scheduler 明确推导 entry node

2. **parser 显式指定目标节点**
   - 若 `TaskParserEvent.context.node_id` 已设置为其他节点，则按指定节点推进

3. **同节点重试**
   - 若 `stay_current_step = true`，则保持 `node_id` 不变，进入当前节点重试路径
   - 解析失败时，`ModuleDagProcessor` 会产出带 `stay_current_step=true` 的 `TaskErrorEvent`

4. **自动推进到后继节点**
   - parser 返回 task，但未显式指定后继节点时：
     - 单后继：直接路由到该后继
     - 多后继：复制 task，分别 fan-out 到所有 successors

5. **空输出推进**
   - 若 parser 成功，但 `parser_task` 为空且当前节点存在 successors，则通过 per-successor **advance gate** 合成一次占位 `TaskParserEvent`
   - gate 是“每个 `(run_id, module_id, from_node, to_node)` 只允许赢一次”的分布式事实，用于避免重试或重复消费造成二次 fan-out

6. **叶子节点完成**
   - 若当前节点无后继且 parser 没有显式产出下一跳，则该路径自然结束
   - `Module::parser()` 在确认 `parser_task` 为空后触发 `post_process()`

7. **显式停止**
   - parser 返回 `stop=true` 时，处理器会写入 `DagStopSignal`
   - 后续队列消息即使晚到，也会先检查 stop signal，避免已经完成的 run 被错误重启

#### 5.2.5 并发层次、放置策略与本地快路径

需要区分三层并发：

| 并发层 | 作用域 | 说明 |
|--------|--------|------|
| DAG 拓扑并发 | 同一 run 内 | 无依赖节点可并行推进；`serial_execution=true` 时强制一次只推进一个 ready node |
| Stage 并发 | 同一节点进程内 | 由 `ProcessorRunner` 的 semaphore 控制每个阶段的 in-flight 数 |
| 队列批次并发 | QueueManager | 由 `Batcher` 进行出站/入站窗口批处理，属于数据面优化，不决定 DAG 顺序 |

目标态的节点放置应结合 `NodePlacement`：

- `Local`：本机直接执行 node executor
- `Remote { worker_group }`：由 `DagNodeDispatcher` 把执行上下文派发到远程 worker 组
- 同一 run 中，放置策略由 DAG 定义和节点能力决定，不能由 worker 自行篡改

当前代码里还有一个重要的**本地快路径**：  
`ResponseParserProcessor` 在发现“下一跳仍是同 module、同 account/platform”时，会尝试**直接本地调用 `generate()`**，绕过 `parser_task` 队列；只有本地生成失败或跨 module/context 时，才回退到 `parser_task` topic。这一优化应在目标态继续保留，但要从“隐式本地判断”收敛为“显式 placement/local-fast-path 策略”。

#### 5.2.6 失败恢复、恢复点与 ACK 顺序

分布式 DAG 的正确性核心不在“图能跑起来”，而在于**失败后不会乱推进、重复推进、旧结果覆盖新结果**。目标态建议遵循以下顺序：

1. **节点执行前**
   - 获取/续租 `DagRunGuard`
   - 读取 `TaskProfileSnapshot(version)` 和当前 `DagRunState`
   - 为本次节点执行绑定 fencing token（如该节点需要提交共享结果）

2. **节点执行成功后**
   - 先把节点输出和 `succeeded_outputs` 写入 `DagRunStateStore`
   - 再写入 advance gate / successor task / parser_task
   - 最后 ACK 当前消息

3. **节点执行失败后**
   - generate 失败：保留当前 node，不推进 DAG，交由当前 stage 的 retry policy 重试
   - parser 失败：写出 `TaskErrorEvent`，并带 `stay_current_step=true` 让同节点重试
   - 若达到 circuit breaker / failure gate 阈值，则触发 `DagCutoverStateTracker` 阻断继续切换，或让 run 级失败终止

4. **节点恢复**
   - 调度器从 `DagRunStateStore` 读取 `DagRunResumeState`
   - 已成功节点直接视为完成，仅重建剩余前驱计数和 ready queue
   - fencing token 必须保证“旧 worker 晚到的结果不能提交覆盖新 worker”

这也是 `src/schedule/dag/scheduler.rs` 中 `DagSchedulerOptions.max_in_flight`、`cancel_inflight_on_failure`、`DagRunStateStore`、`DagFencingStore` 存在的根本原因：  
**DAG 调度器不仅要决定“接下来跑谁”，还要保证“恢复后仍然只跑一次、只推进一次、只提交最新一次”。**

### 5.3 DAG 蓝绿切换

`DagCutoverStateTracker`（当前嵌入 `src/engine/task/task_manager.rs`）管理 DAG 版本切换门禁：

- **warmup 计数器**：shadow compare 连续 match 达到门槛后允许切换
- **failure_streak**：连续失败次数超阈值时阻断切换
- **gate snapshot**：通过 `ModuleDagCutoverGateState` 暴露 `failure_streak / last_failure_ms / blocked`

### 5.4 ProcessorRunner 批处理

`ProcessorRunner`（`src/engine/runner.rs`）管理单个管道阶段的并发处理：

- **有界并发**：每个 item 获取一个 `Semaphore` permit，严格限制同时执行数
- **贪婪小批次拉取**：先 `recv()` 一个，再 `try_recv()` 额外拉取最多 `min(100, concurrency/2).max(1)` 条
- **暂停/恢复**：监听 `pause_rx: watch::Receiver<bool>`，暂停时不再拉新任务
- **优雅关闭**：监听 `shutdown_rx: broadcast::Receiver<()>`
- **in-flight 观测**：每个 item 执行前后更新 `inflight_counter` 与 metrics
- **背压反馈**：真正的时间窗口批处理在 `QueueManager::Batcher`，而 stage 内背压主要体现在下游发送阻塞和 permit 获取等待

批处理伪代码：
```rust
loop {
    select! {
        _ = shutdown_rx.recv() => break,
        _ = pause_rx.changed() => continue,
        item = rx.recv() => {
            let mut batch = vec![item];
            while batch.len() < greedy_limit {
                match rx.try_recv() {
                    Ok(next) => batch.push(next),
                    Err(_) => break,
                }
            }
            for item in batch {
                let permit = semaphore.acquire_owned().await;
                spawn(process(item, permit));
            }
        }
    }
}
```

#### 5.4.1 Processor 级故障恢复

`ProcessorRunner` 只负责单阶段并发控制；真正的“panic 后重启”由 `Engine::start()` 外层统一处理：

- 连续 panic 会触发指数退避；
- 超过阈值进入冷却窗口；
- 这样单个 processor 异常不会直接把整个节点拖垮。

### 5.5 DagCutoverStateTracker 详解

```rust
pub struct DagCutoverStateTracker {
    failures: Arc<DashMap<String, DagCutoverFailureState>>,
    warmup: Arc<DashMap<String, DagCutoverWarmupState>>,
    now_ms_provider: Arc<dyn Fn() -> u64 + Send + Sync>,
}
```

状态迁移：
- `record_shadow_compare_result(scope, "match")` 累加 warmup match。
- shadow mismatch / shadow error 清空该 scope 的 warmup。
- `record_cutover_failure(scope)` 累加 failure streak。
- `should_allow_cutover(scope, max_failures, recovery_window_secs)` 在超出失败阈值时阻断切换，冷却窗口后允许重新探测。

---

## 6. 分布式协调

### 6.1 Raft Leader

系统不再维护独立的“软 leader”选举器，而是直接复用 openraft 的领导权：

- **当前 Raft Leader**：唯一允许触发 cron 调度、提交控制面写请求、发起节点清理
- **Follower**：只读本地已 apply 状态，参与下载 / 解析 / 队列处理
- **Learner**：可同步状态、提供只读能力，但不参与投票

**Leader vs Follower 职责对比：**

| 职责 | Leader 节点 | Follower 节点 |
|------|------------|--------------|
| cron 调度触发 | ✅ 执行 `generate()` | ❌ 跳过 |
| 节点死亡清理 | ✅ 扫描节点心跳状态并提交移除命令 | ❌ 不执行 |
| gRPC 广播 | ✅ 发起 | ✅ 接收 |
| 任务下载/解析 | ✅ 参与 | ✅ 参与 |
| 节点注册心跳 | ✅ 参与 | ✅ 参与 |
| API `/start_work` | ✅ 立即执行 | ❌ 转发至 leader |

### 6.2 StatusTracker

`StatusTracker` 跟踪任务执行状态，底层不再依赖外部 KV，而是直接写入本命名空间的 Raft 状态机：

```rust
pub struct StatusEntry {
    pub task_id:     String,
    pub stage:       PipelineStage,   // Task / Request / Response / ParserTask / Error
    pub status:      TaskStatus,      // Pending / Running / Done / Failed / Retrying
    pub retry_count: u32,
    pub node_id:     String,
    pub updated_at:  i64,
    pub error_msg:   Option<String>,
}
```

推荐键布局：

```
{config.name}:status:{task_id}         -> StatusEntry
{config.name}:status:index:{stage}     -> OrderedSet<task_id, updated_at>
```

查询接口：
- `get_status(task_id)` → `StatusEntry`
- `list_by_stage(stage, limit)` → `Vec<StatusEntry>`
- `count_by_status()` → `HashMap<TaskStatus, u64>`

当前代码已通过调试 API 暴露这些查询能力：`GET /debug/status/{task_id}`、`GET /debug/status/stage/{stage}?limit=N`、`GET /debug/status/counts`。

### 6.3 NodeRegistry

`NodeRegistry` 维护集群节点列表，同样持久化在 Raft 状态机：

```
{config.name}:node:{node_id} -> NodeInfo {node_id, addr, roles, started_at, last_heartbeat}
{config.name}:node:index     -> OrderedSet<node_id, last_heartbeat>
```

节点每个心跳周期提交一次 `UpdateHeartbeat`；leader 定期扫描过期节点并提交 `UnregisterNode`。

### 6.4 通用缓存 / 协调抽象

新的 `CacheService` 不再只是“缓存”，而是统一承载：

- 通用 KV
- TTL 过期
- CAS / compare-and-set
- lease（分布式锁）
- counter / window（限流）
- 状态索引

抽象接口示意：

```rust
pub trait CacheBackend: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, value: &[u8], ttl_secs: Option<u64>) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn compare_set(&self, key: &str, expected: Option<&[u8]>, value: &[u8]) -> Result<bool>;
    async fn acquire_lease(&self, key: &str, owner: &str, ttl_ms: u64) -> Result<Option<LeaseToken>>;
    async fn renew_lease(&self, token: &LeaseToken, ttl_ms: u64) -> Result<bool>;
    async fn release_lease(&self, token: LeaseToken) -> Result<()>;
    async fn incr_by(&self, key: &str, delta: i64, ttl_secs: Option<u64>) -> Result<i64>;
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}
```

底层实现：

- **复制协议**：Raft
- **本地状态存储**：RocksDB
- **本地热点读优化**：L1 内存缓存（可选）

### 6.5 分布式锁

分布式锁复用 `CacheService.acquire_lease()`，不再单独维护 Redlock 体系：

```
lock key:   {config.name}:lock:{scope}:{resource_id}
value:      LeaseToken { owner, version, expires_at }
```

关键语义：

- 获取锁 = 原子写入 lease
- 续约 = 仅 owner 可续
- 释放 = 仅持有正确 `LeaseToken` 的 owner 可释放
- fencing token = `version` 单调递增，用于防止旧持有者晚到写入

### 6.6 分布式限流

限流同样复用 `CacheService`，以“窗口计数器 / 令牌桶状态”写入 Raft 状态机：

```
{config.name}:ratelimit:{limit_id}:window:{slot}
{config.name}:ratelimit:{limit_id}:state
```

支持：

- 全局限速（模块默认）
- `Request.limit_id` 分组限速
- leader / worker 跨节点共享统一额度
- 限流状态本地只读缓存，加速热路径

### 6.7 原子执行状态机

原先通过脚本完成的原子状态迁移，在新方案中改为显式 Raft Command：

| Command | 用途 |
|---------|------|
| `ClaimParserTask` | Pending → Running，写入 `node_id` 与 `fence_token` |
| `CommitParserTaskSuccess` | Running → Done |
| `CommitParserTaskFailure` | Running → Failed，记录错误并增加重试次数 |
| `RetryErrorTask` | 增加错误计数并写入退避截止时间 |
| `TerminateTask` | 标记为 Terminated |

这些命令由 leader 串行提交，天然具备线性一致性，不再需要外部脚本引擎。

### 6.8 CronScheduler 机制

`CronScheduler` 当前实现位于 `src/engine/scheduler.rs`，并不是一个“单纯包一层 cron 库”的薄封装，而是包含：

- 周期性刷新启用中的 `(module, account, platform)` context 列表
- 缓存 module 的 cron 配置和解析后的 `cron::Schedule`
- 借助 `LeaderElector` 保证当前只有 leader 节点真正触发 `generate()`
- `misfire_tolerance_secs`、`refresh_interval_secs`、`max_staleness_secs` 等调度韧性参数
- `right_now` / `run_now_and_schedule` 等立即触发语义

v2 目标态中，这里的 leader guard 将从 `LeaderElector` 平滑切换到 Raft Leader，但 scheduler 的“缓存 context + 只由 leader 触发”职责边界不变。

### 6.9 EventBus

`EventBus` 当前实现位于 `src/engine/events/event_bus.rs`，是一个**进程内 fan-out 总线**：

- 入口使用 `tokio::sync::mpsc` 接收 `EventEnvelope`
- 内部用 `DashMap<String, Vec<Sender<_>>>` 维护订阅关系
- `publish()` 使用 `try_send`，队列满时直接丢弃事件，避免把背压传回主处理链路
- `start()` 会启动一个专用线程/运行时，将事件分发给精确 topic 和 `*` 通配订阅者

因此当前 `EventBus` 更接近“轻量异步事件分发器”，而不是依赖 `broadcast` 的全局总线；外部输出能力应挂在 handler 层，而不是 EventBus 本体。

### 6.10 Policy 框架

`Policy`（`src/common/policy.rs`）是错误处理的核心决策结构：

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub retryable:   bool,           // 是否可重试
    pub backoff:     BackoffPolicy,  // 退避策略
    pub dlq:         DlqPolicy,      // DLQ 路由行为
    pub alert:       AlertLevel,     // 告警级别
    pub max_retries: u32,            // 最大重试次数
    pub backoff_ms:  u64,            // 初始退避时间（ms）
}

pub enum BackoffPolicy {
    None,
    Linear      { base_ms: u64, max_ms: u64 },
    Exponential { base_ms: u64, max_ms: u64 },
}

pub enum DlqPolicy {
    Never,          // 永不路由 DLQ
    OnExhausted,    // 重试耗尽后路由 DLQ
    Always,         // 直接路由 DLQ（不重试）
}

pub enum AlertLevel { Info, Warn, Error, Critical }
```

**PolicyOverride — 按错误类型细粒度覆盖默认策略：**

```rust
pub struct PolicyOverride {
    pub domain:      Option<String>,    // 如 "engine"、"downloader"
    pub event_type:  Option<String>,    // 如 "download"、"parse"
    pub phase:       Option<String>,    // 如 "failed"、"timeout"
    pub kind:        ErrorKind,         // 错误类型（必填）
    pub retryable:   Option<bool>,
    pub backoff:     Option<BackoffPolicy>,
    pub dlq:         Option<DlqPolicy>,
    pub alert:       Option<AlertLevel>,
    pub max_retries: Option<u32>,
    pub backoff_ms:  Option<u64>,
}
```

**PolicyResolver — 运行时策略解析：**

```rust
// 按错误对象解析出最终决策
pub fn resolve_with_error(&self, domain: &str, event_type: Option<&str>,
    phase: Option<&str>, err: &Error) -> Decision

// 按错误类型解析
pub fn resolve_with_kind(&self, domain: &str, event_type: Option<&str>,
    phase: Option<&str>, kind: ErrorKind) -> Decision
```

解析优先级：`PolicyOverride`（按 domain + event_type + phase + kind 匹配） > `Policy` 默认值。

策略通过 `config.toml` 的 `[policy]` 段或 `ModuleTrait::policy()` 方法注入。

---

## 7. 当前实现状态评估

### 7.1 当前重构进度（截至当前代码）

| 重构包 | 状态 | 当前说明 |
|------|------|----------|
| Pack 1：Canonical Model | ✅ 已完成 | canonical model / typed envelope / control-plane 写入模型已建立 |
| Pack 2：Module Runtime | ✅ 已完成 | `ModuleTrait` / `ModuleNodeTrait` 已切到 typed context 与 profile runtime contract |
| Pack 3：Scheduler Core | ✅ 已完成 | DAG 调度恢复、放置、停止收敛、run guard / fencing 已落地 |
| Pack 4：Transport Layer | ✅ 已完成 | queue codec、topic、batch、DLQ envelope 已收口 |
| Pack 5：Execution Pipeline | ✅ 已完成 | task/request/response/parser/error 主路径已切到 typed envelope；生产代码无 `"legacy.*"` schema 与 `build_legacy_*` builder |
| Pack 6：Control Plane API | ✅ 已完成 | `/config/*`、`/tasks/dispatch`、`/cluster/*`、`/debug/*`、`/control/*` 已接入 `ProfileControlPlaneStore` |
| Pack 7：Observability | ✅ 已完成 | metrics facade、统一标签、dashboard/alert 基线已落地 |
| Pack 8：Bootstrap / Legacy Removal | ✅ 已完成 | Redis/Lua/rollback 兼容路径、旧检查脚本名、旧补偿后端和旧归档证据已清理 |

**当前验证状态（2026-05-20）：**

- `cargo check` 通过。
- `cargo check --tests` 通过。
- `cargo check --manifest-path tests\Cargo.toml --bins` 通过。
- `powershell -ExecutionPolicy Bypass -File scripts\check_typed_hot_path.ps1` 通过。

### 7.2 已实现（稳定）

| 组件 | 状态 | 说明 |
|------|------|------|
| `State` | ✅ 稳定 | 组合根，按 `[cache].backend` 装配 Local 或 Raft/RocksDB 控制面 |
| `Engine` | ✅ 稳定 | 主协调器，事件驱动 pause/resume 与 processor 生命周期完整 |
| `QueueManager` | ✅ 稳定 | 本地 channel + Kafka/MQ backend 桥接；batch/codec/compress/DLQ/NACK |
| `QueueNativeCompensator` | ✅ 稳定 | 远程 queue backend 默认补偿器，不依赖外部 KV |
| `ProcessorRunner` | ✅ 稳定 | 有界并发、暂停/关闭完整 |
| `TaskManager` | ✅ 稳定 | 任务 CRUD、DAG 预编译、warmup/failure gate |
| `CacheService` | ✅ 稳定 | Local 与 Raft/RocksDB backend；KV / TTL / ZSET / L1 facade |
| `DistributedLockManager` | ✅ 稳定 | Local / Raft lock backend；模块文件为 `src/utils/distributed_lock.rs` |
| `DistributedSlidingWindowRateLimiter` | ✅ 稳定 | Local / Raft rate-limit backend；`raft_rocksdb` 模式写入 `ProfileControlPlaneStore` |
| `CronScheduler` | ✅ 稳定 | cron 触发 + leader guard；启用 `[raft]` 时走 Raft leader 视图 |
| `NodeRegistry` | ✅ 稳定 | 基于 cache/ZSET 的节点心跳索引；Raft backend 可作为底层一致性存储 |
| `StatusTracker` | ✅ 稳定 | 错误/状态跟踪器，经 CacheService + lock 接入 Local/Raft backend |
| `EventBus` | ✅ 稳定 | 进程内 typed fan-out 总线 |
| `RaftRuntime` | ✅ 稳定 | `openraft 0.10.0-alpha.17 + RocksDB`，支持单节点 bootstrap、metrics/leader、状态机 apply |

### 7.3 当前剩余工作

| 组件 | 状态 | 后续方向 |
|------|------|----------|
| `CompensationReplayer` | ⚠️ 可增强 | 当前已有 `QueueNativeCompensator` pending scan/drain；若要求跨进程 crash recovery，应落 queue-native event topic 或 RocksDB/WAL |
| `Response Cache` | ⚠️ 可增强 | 当前已有 owner index、远端 HTTP 回源和本地 warm copy；大 body 文件化 + Raft 索引 + gRPC 回源仍可作为后续优化 |
| `Policy / CircuitBreaker` | ⚠️ 可增强 | 框架存在，断路器策略可继续细化 |
| `BlobStorage` | ⚠️ 可增强 | 当前以本地文件系统实现为主，可按部署需要扩展 |
| `Metrics` | ⚠️ 可增强 | 主路径指标已落地，仍可继续压缩标签漂移并补齐业务指标 |

### 7.4 当前不再支持的历史路径

| 历史路径 | 当前处理 |
|------|------|
| Redis cache / queue / event / sync / lock | 已删除，不再支持历史兼容 |
| Lua 原子脚本路径 | 已删除，不再支持 |
| rollback / cutover rehearsal 脚本和归档证据 | 已删除，不再作为上线流程 |
| `check_legacy_hot_path.*` | 已替换为 `check_typed_hot_path.*` |
| `build_legacy_*` runtime builder / `"legacy.*"` schema ID | 生产代码零命中，并由 typed hot-path 检查阻断新增 |

### 7.5 数据落地策略

**数据落地由 `DataStoreMiddleware` 三段式生命周期承担，不引入独立 Sink 抽象。**

```rust
pub trait DataStoreMiddleware: Middleware {
    async fn before_store(&self, data: &mut ParsedData) -> Result<()>;
    async fn store_data(&self, data: &ParsedData) -> Result<StoreReceipt>;
    async fn after_store(&self, data: &ParsedData, receipt: StoreReceipt) -> Result<()>;
}
```

- `before_store`：数据清洗、字段映射、验证
- `store_data`：实际写入（DB / 文件 / 对象存储 / MQ 下游）
- `after_store`：写入后回调（更新缓存、发送通知、触发下游任务）

中间件通过 `weight()` 排序，多个 `DataStoreMiddleware` 可串联，实现多目标写入。

---

## 8. 去中心化改造路径

### 8.1 改造目标

建立 **“Raft + RocksDB 控制面 / QueueManager 数据面 / 本地磁盘 Response Cache”** 的统一架构：

- 通用缓存、锁、限流、状态、节点注册全部复用同一套 `CacheService`
- Response Cache 使用“本地磁盘 + Raft 索引 + gRPC 读取”
- 补偿队列使用 QueueManager 原生补偿 topic
- 登录态完全由 `ModuleTrait` 自行维护
- 去重功能暂时移除，后续如有需要再基于 `CacheService` 重新设计

### 8.2 改造阶段

**阶段 0（基线）**：Raft 控制面成型
```
Raft Cluster  → leader / node registry / status / cache / lock / rate-limit
QueueManager  → task / request / response / parser_task / error_task
```

**阶段 1（增强）**：Response Cache 去中心化
```
下载节点本地磁盘  → 存 Response 文件
Raft 状态机      → 存 cache_key -> owner_node_id / expires_at
gRPC            → 跨节点读取缓存内容
```

**阶段 2（增强）**：QueueManager 原生补偿
```
comp_begin:* / comp_done  → 补偿事件流
CompensationReplayer      → 超时回放原始消息
```

**阶段 3（后续）**：可选能力补回
```
去重 / 更细粒度缓存策略 / 跨命名空间优化
全部基于统一 CacheService 增量实现
```

### 8.3 Raft 组成员管理

节点加入：
1. 新节点通过 HTTP(JSON) 联系任意现有节点（`POST /raft/admin/join`）
2. 现有节点转发 `AddLearner` 请求到 Raft leader
3. Raft leader 提交配置变更日志
4. 新节点同步日志后升级为 Voter

节点退出：
1. 优雅退出：发送 `RemoveNode` 请求，等待日志同步
2. 异常退出：心跳超时（3 轮选举超时）后 leader 发起 `RemoveNode`

---

## 9. Raft 集成设计

### 9.1 配置驱动的 Raft 启动

在 `config.toml` 中配置 `[raft]` 段后，节点自动加入本命名空间的 Raft 集群；省略该段则以单节点本地模式运行。

```toml
[raft]
addr  = "0.0.0.0:7001"          # 本节点 Raft RPC 监听地址（HTTP/JSON）
peers = ["192.168.1.1:7001"]    # 任意一个已在线节点地址，留空仅在单节点引导时使用
data_dir = "./raft_data"        # RocksDB 持久化目录
```

**关键设计：只需一个 peer 地址**

新节点启动时，仅需在 `peers` 中配置**任意一个**现有在线节点的 `raft.addr`。
加入流程如下：

```
新节点 N                      任意在线节点 P              Raft Leader L
    │                              │                          │
    │── HTTP: POST /raft/admin/join▶│                          │
    │   (携带 node_id + node addr) │── forward-to-leader ───▶│
    │                              │                    写 AddLearner 日志
    │                              │◀── 日志复制 ────────────│
    │◀─ 日志同步 ──────────────────│                          │
    │   (同步完成后 Learner → Voter)│                          │
```

节点 P 无需是 Leader，收到 `JoinRequest` 后自动转发给当前 Leader。

### 9.2 命名空间隔离原则

**每个 `config.name` 命名空间对应一个独立的 Raft 组。**

节点只参与自身命名空间的 Raft，不感知其他命名空间的 Raft 状态：

| 组件 | 命名空间隔离 | 说明 |
|------|------------|------|
| Raft 日志 / 状态机 | ✅ 严格隔离 | 每个命名空间独立的 RocksDB 实例（`data_dir/{config.name}`） |
| Leader 选举 | ✅ 严格隔离 | 命名空间 A 的 Leader 对命名空间 B 不可见 |
| NodeRegistry | ✅ 严格隔离 | 节点只注册到本命名空间的 Raft 状态机 |
| StatusTracker | ✅ 严格隔离 | key 前缀 `{config.name}:...` 天然隔离 |
| **下载器（例外）** | ❌ 跨命名空间共享 | Download Pool 联邦共享，不经过 Raft 协调 |

下载器之所以是例外：HTTP 下载是无状态的纯 I/O 操作，不需要共识保证，任何节点都可以处理任意命名空间的下载请求，结果通过 `Response.namespace` 路由回归属命名空间。

### 9.3 openraft 选型理由

- 纯 Rust 实现，无 C FFI
- 存储层可插拔（实现 `RaftStorage` trait）
- 支持 learner 节点（只读副本，不参与投票）
- 内置成员变更（`AddLearner` / `ChangeMembership`）API

### 9.4 状态机定义

```rust
pub enum ControlPlaneRaftCommand {
    UpsertTaskProfile { snapshot: TaskProfileSnapshot },
    DisableTaskProfile { identity: TaskProfileIdentity },
    BatchUpsertTaskProfiles { snapshots: Vec<TaskProfileSnapshot> },

    UpsertAccountDefault { default: DefaultConfigUpsert },
    UpsertPlatformDefault { default: DefaultConfigUpsert },
    UpsertModuleDefault { default: DefaultConfigUpsert },
    UpsertMiddleware { middleware: MiddlewareUpsert },

    UpsertNodeHeartbeat { namespace: String, node: NodeInfo },
    RemoveNode { namespace: String, node_id: String },

    SetModuleLock { namespace: String, module_id: String, locked_at: u64 },
    RemoveModuleLock { namespace: String, module_id: String },

    UpsertStatusEntry {
        namespace: String,
        entry: StatusEntry,
        previous_stage: Option<PipelineStage>,
    },
    UpsertStatusCounter { namespace: String, counter_key: String, value: i64 },

    SetPauseState { namespace: String, paused: bool },
}
```

所有 `ControlPlaneRaftCommand` 都携带命名空间语义（通过 key 前缀隔离），节点只 apply 自身命名空间命令。
其中 Response Cache 只把**索引元数据**写入 Raft，避免把大体积响应体复制到日志中；实际内容由 `owner_node_id` 对应节点的本地磁盘持有。

### 9.5 存储层

当前 `RaftStorage` 实现基于 RocksDB（`rocksdb` crate），每个命名空间使用独立目录；实现上采用 key 前缀而非多 CF：

- **Raft 日志**：`raft/log/{index}`
- **Raft 元数据**：`raft/meta/*`（vote/committed/last_purged）
- **快照**：`raft/snapshot/meta`、`raft/snapshot/data`
- **状态机镜像**：`raft/sm/state`（含 `applied_requests`，用于重放控制面）
- **控制面读模型（ProfileControlPlaneStore）**：独立 RocksDB，使用 `raft_log / state_machine / raft_meta` 三个 CF
- **存储路径**：Raft store 在 `{raft.data_dir}/{config.name}`，控制面 store 在 `{raft.data_dir}/{config.name}/control_plane`

### 9.6 网络层

当前 Raft 节点间通信通过 axum + HTTP(JSON) 路由：

```text
POST /raft/append
POST /raft/vote
POST /raft/snapshot
POST /raft/admin/join
POST /raft/client-write
```

其中：

- `join` 用于新节点加入并支持 forward-to-leader
- `client-write` 用于 follower 写入转发到 leader
- 节点间复制（append/vote/snapshot）由 `RaftNetworkV2` 的 HTTP client 承担

`NodeCacheService.GetCachedResponse` 目前仍是目标态接口，尚未在当前代码主线落地。

| 能力 | 当前实现 |
|------|----------|
| leader 选举 | Raft 内置选举 |
| 节点注册/心跳 | Raft 状态机（`UpsertNodeHeartbeat/RemoveNode`） |
| 任务状态同步 | Raft 状态机（`UpsertStatusEntry/Counter`） |
| 跨节点任务分发 | QueueManager + MqBackend |
| cron 触发协同 | 由当前 leader 本地调度触发（`LeadershipGate`） |
| 元数据查询 | 本地已 apply 读 / Raft 线性写入 |

---

## 11. 数据模型

### 11.1 核心模型

> **注**：以下为目标态概念模型。当前代码实际使用 `TaskEvent`（`src/common/model/message.rs`）、`Request`、`Response`，并仍保留 `MetaData` 这一兼容层；目标态会把运行时上下文拆分为强类型元数据和 typed payload。

```rust
pub struct RoutingMeta {
    pub namespace:         String,
    pub account:           String,
    pub platform:          String,
    pub module:            String,
    pub node_key:          String,
    pub run_id:            uuid::Uuid,
    pub request_id:        uuid::Uuid,
    pub parent_request_id: Option<uuid::Uuid>,
    pub priority:          Priority,
}

pub struct ExecutionMeta {
    pub retry_count:      u32,
    pub task_retry_count: u32,
    pub profile_version:  u64,
    pub trace_id:         Option<String>,
    pub fence_token:      Option<u64>,
    pub created_at_ms:    i64,
    pub updated_at_ms:    i64,
}

pub enum PayloadCodec {
    MsgPack,
    Json,   // 仅兼容 API / 调试边界
}

pub struct TypedEnvelope {
    pub schema_id:      String,
    pub schema_version: u16,
    pub codec:          PayloadCodec,
    pub bytes:          bytes::Bytes,
}

pub struct ResolvedCommonConfig {
    pub timeout_secs:            u64,
    pub rate_limit:              Option<f32>,
    pub priority:                PriorityLevel,
    pub proxy_pool:              Option<String>,
    pub downloader:              String,
    pub enable_session:          bool,
    pub serial_execution:        bool,
    pub rate_limit_group:        Option<String>,
    pub response_cache_enabled:  bool,
    pub response_cache_ttl_secs: Option<u64>,
}

pub struct ResolvedNodeConfig {
    pub profile_key:     String,
    pub profile_version: u64,
    pub common:          ResolvedCommonConfig,
    pub node_config:     TypedEnvelope,
}

pub struct NodeInput {
    pub source_node: Option<String>,
    pub target_node: String,
    pub payload:     TypedEnvelope,
}

pub struct TaskModel {
    pub routing: RoutingMeta,
    pub exec:    ExecutionMeta,
    pub input:   NodeInput,
}

pub struct RequestModel {
    pub routing:    RoutingMeta,
    pub exec:       ExecutionMeta,
    pub input:      NodeInput,
    pub url:        String,
    pub method:     HttpMethod,
    pub headers:    HashMap<String, String>,
    pub body:       Option<Bytes>,
    pub timeout_ms: Option<u64>,
    pub proxy:      Option<String>,
}

pub struct ResponseModel {
    pub routing:    RoutingMeta,
    pub exec:       ExecutionMeta,
    pub request_id: uuid::Uuid,
    pub status:     u16,
    pub headers:    HashMap<String, String>,
    pub body:       Bytes,
    pub latency_ms: u64,
}

pub struct NodeDispatch {
    pub target_node: String,
    pub input:       NodeInput,
}

pub struct NodeParseOutput {
    pub next:     Vec<NodeDispatch>,
    pub data:     Vec<ParsedData>,
    pub finished: bool,
}

pub struct ErrorTaskModel {
    pub routing: RoutingMeta,
    pub exec:    ExecutionMeta,
    pub stage:   PipelineStage,
    pub error:   String,
}
```

当前代码中的 parser/error legacy carrier 类型本体已删除；兼容层现主要收缩到 `MetaData`、`Map<String, Value>` 型 `params` 以及部分 request/response transport 过渡面。目标方向是不再把业务参数和控制面配置继续堆进 `HashMap<String, Value>` / `serde_json::Value`。

### 11.2 ParserTask.parser_task 语义

`ParserTaskModel.parser_task` 是 `Vec<ParserTask>`，**不是** `Option`。

操作模式：
- `append`：parser 追加新发现的子任务
- `iter`：遍历执行所有已注册 task
- `drain`：执行后清空，防止重复执行

---

## 12. 模块与中间件

### 12.1 ModuleTrait 完整接口

```rust
#[async_trait]
pub trait ModuleTrait: Send + Sync {
    // 必实现
    fn name(&self) -> &str;
    async fn generate(&self, state: &State) -> Result<Vec<TaskModel>>;
    async fn parser(&self, response: ResponseModel, state: &State) -> Result<ParserTaskModel>;

    // 可选：DAG 定义（优先于 add_step）
    fn dag_definition(&self) -> Option<DagDefinition> { None }

    // 可选：线性步骤（兼容旧接口）
    fn add_step(&self, builder: &mut PipelineBuilder) {}

    // 可选：请求构造（默认直接透传 TaskModel.url）
    async fn build_request(&self, task: &TaskModel, state: &State) -> Result<RequestModel> {
        Ok(RequestModel::from_task(task))
    }

    // 可选：错误处理钩子
    async fn on_error(&self, error: &ErrorTaskModel, state: &State) -> Result<()> {
        Ok(())
    }

    // 可选：模块级 Policy 覆盖
    fn policy(&self) -> Option<Policy> { None }

    // 可选：ORM/DB 配置覆盖
    fn db_config(&self) -> Option<DbConfig> { None }
}
```

#### 12.1.1 当前代码中的职责拆分

上面这段接口更接近架构层抽象；当前代码实际把模块职责拆成两层：

| Trait / 类型 | 当前职责 |
|-------------|----------|
| `ModuleTrait` | 提供 `name/version/default_arc`、`dag_definition()`、`add_step()`、`pre_process()`、`post_process()`、`cron()` 等模块级钩子 |
| `ModuleNodeTrait` | 真正承载单个 DAG 节点的 `generate()` / `parser()` 逻辑 |
| `NodeParseOutput` | 当前节点 parser 的输出载体；旧的 `TaskOutputEvent` 兼容壳已删除，主路径收口为 `NodeParseOutput / NodeDispatch` |
| `MetaData` | 当前实现中的兼容元数据包；目标态会拆成 `RoutingMeta + ExecutionMeta + NodeInput/ResolvedNodeConfig` |

也就是说，**当前代码里的 generate/parser 主要发生在 `ModuleNodeTrait` 层，`ModuleTrait` 更像模块外壳与 DAG 入口**。v2 文档之所以仍保留更高层接口，是为了描述未来统一配置与工作流接口的收敛方向。

### 12.2 数据落地：中间件是固定方式

**数据落地固定由 `DataStoreMiddleware` 实现，不引入独立 Sink 节点或 Sink 抽象。**

实现示例（写入 PostgreSQL）：

```rust
pub struct PgStoreMiddleware {
    pool: Arc<PgPool>,
}

#[async_trait]
impl DataStoreMiddleware for PgStoreMiddleware {
    async fn before_store(&self, data: &mut ParsedData) -> Result<()> {
        // 字段校验、去 null、类型转换
        data.normalize()?;
        Ok(())
    }

    async fn store_data(&self, data: &ParsedData) -> Result<StoreReceipt> {
        let id = sqlx::query!(
            "INSERT INTO items (url, content, scraped_at) VALUES ($1, $2, $3) RETURNING id",
            data.url, data.content, data.scraped_at
        )
        .fetch_one(&*self.pool).await?.id;
        Ok(StoreReceipt { id: id.to_string() })
    }

    async fn after_store(&self, data: &ParsedData, receipt: StoreReceipt) -> Result<()> {
        // 更新缓存、发通知、触发下游
        Ok(())
    }
}
```

注册：

```rust
engine.middleware_chain.lock().await.add(
    Arc::new(Mutex::new(Box::new(PgStoreMiddleware::new(pool))))
);
```

### 12.3 中间件权重排序

中间件按 `weight()` 升序执行（值越小越早执行）：

| weight | 典型用途 |
|--------|---------|
| 0–99   | 认证、请求签名 |
| 100–199 | 限流、熔断 |
| 200–299 | 缓存读取 |
| 300–399 | 数据转换、清洗 |
| 400–499 | 存储落地（DataStoreMiddleware） |
| 500+   | 审计日志、通知 |

---

## 13. API 与控制面

> **完整 API 设计详见独立文档：[api-architecture.md](./api-architecture.md)**  
> 本节记录当前已实现的接口，新架构（Raft+API）的完整接口规范在上述文档中维护。

### 13.1 当前已实现路由

`src/engine/api/router.rs` 当前关键路由如下（仅列控制面主入口；`/config/*`、`/debug/*` 细分路由未在此表中展开）：

| 方法 | 路径 | 中间件 | 说明 |
|------|------|--------|------|
| `GET` | `/health` | 无 | 节点健康检查（不需要认证，适合负载均衡探针）|
| `GET` | `/metrics` | 限速 | Prometheus 格式指标输出（无需 API key）|
| `POST` | `/tasks/dispatch` | 认证 + 限速 | 手动下发一次 `TaskEvent`，写入 typed ingress queue |
| `GET` | `/cluster/nodes` | 认证 + 限速 | 列出集群所有活跃节点（来自 control-plane store）|
| `GET` | `/cluster/leader` | 认证 + 限速 | 查看当前 control-plane leader；Raft 选举期间返回 `503` |
| `GET` | `/dlq/messages` | 认证 + 限速 | 查看死信队列消息列表 |
| `POST` | `/control/pause` | 认证 + 限速 | 暂停所有 `ProcessorRunner` |
| `POST` | `/control/resume` | 认证 + 限速 | 恢复所有 `ProcessorRunner` |

**中间件层次：**

```
请求
  │
  ├─ /health ──────────────────────────────▶ health_check（无中间件）
  │
  └─ 其他路由
       │
       ├─ rate_limit_middleware（api.rate_limit req/s）
       │     │
       │     ├─ /metrics ──────────────────▶ metrics_handler
       │     │
       │     └─ auth_middleware（校验 X-API-Key: api.api_key）
       │           │
    │           ├─ POST /tasks/dispatch ─▶ dispatch_task(Json<TaskEvent>)
    │           ├─ GET  /cluster/nodes ──▶ get_nodes
    │           ├─ GET  /cluster/leader ─▶ get_cluster_leader
    │           ├─ GET  /dlq/messages ───▶ get_dlq_messages
       │           ├─ POST /control/pause ──▶ pause_engine
       │           └─ POST /control/resume ─▶ resume_engine
```

`State`（`ApiState`）通过 axum 的 `State` extractor 注入所有 handler。

**当前 router 代码还有两个实现细节值得明确：**

1. `/metrics` 只经过限速中间件，不要求 API key。
2. `POST /tasks/dispatch` 的 handler 本质上只是把 `TaskEvent` 包装成 typed dispatch envelope 后写入 `queue_manager.get_task_push_channel()`；任务最终走本地还是远端，由 `QueueManager` 决定。

**另外几个当前实现细节也很重要：**

3. `/health` 目前检查 `cache_service.ping()` 和 `db.ping()`；返回体带 `components.cache / components.db` 与整体 `up / degraded` 状态，但 handler 直接返回 JSON，因此当前实现里即使 degraded 也仍是 HTTP 200。
4. `rate_limit_middleware` 会优先使用 `X-API-Key`，其次使用 `Authorization: Bearer ...`，都没有时退化为 `"anonymous"`；因此 `/metrics` 和所有未认证请求共享同一个 anonymous 限流桶。
5. `GET /cluster/leader` 直接读取 `RaftRuntime` metrics 暴露当前 leader 视图；如果当前节点启用了 Raft 但选举尚未收敛，则接口返回 `503 Service Unavailable`。

6. `/control/pause` / `/control/resume` 现在通过 `ProfileControlPlaneStore` 持久化 pause 状态，并由其本地 `watch` 通道直接驱动 `Engine` 的 pause 开关；processor 不再依赖 5 秒一次的轮询同步。
6. `/nodes` 和 `/dlq` 当前都属于 fail-open 风格：后端读取失败时直接返回空数组；其中 `/dlq` 还会先尝试把 payload 转成 UTF-8，失败则返回固定占位字符串。
7. `NodeRegistry` 的活跃节点视图来自“TTL JSON record + `registry:nodes_index` ZSET 索引”；读取 `/nodes` 时会做 lazy cleanup，而不是由专门的 registry cleaner 后台任务维护。

**`/start_work` 请求体示例：**

```json
{
  "account": "my_account",
  "platform": "web",
  "module": ["my_module"],
  "priority": "normal",
  "run_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 13.2 指标与可观测体系

当前代码已经具备 Prometheus 暴露能力：`Engine::new()` 会安装 recorder，`GET /metrics` 直接渲染 `PrometheusHandle`；同时 `src\common\metrics.rs` 已提供 `node_up / component_health / resource_usage / backlog / inflight / throughput / latency / errors / policy_decisions` 等统一封装。  
但当前实现仍处于“部分统一 + 大量散点直打点”状态：queue、cache、dedup、scheduler、DAG、logger、backpressure 等模块都定义了各自的指标族，文档中的旧指标列表已经不足以表达真实系统。

当前指标体系的主要问题：

1. 指标语义不统一：既有 `mocra_throughput_total` 这类统一指标，也有 `mocra_ptm_commit_total`、`mocra_dag_remote_*` 等专用指标，难以做全链路聚合。
2. 标签不统一：有的指标带 `module`，有的只带 `result`，有的没有 backend/stage 维度；分布式下 `namespace` 与 `node_id` 也未严格分离。
3. 单位不统一：当前代码同时出现 `_us`、`_ms`、`_seconds`，不利于统一 dashboard 和告警阈值。
4. 覆盖不完整：API、control plane、config hot update、profile version 漂移、DagRunState 恢复等关键路径缺少一等指标。
5. 后端不对称：Kafka / in-memory / `Raft + RocksDB` 路径的指标覆盖仍可继续补齐。

#### 13.2.1 分层目标

目标态指标体系分三层：

| 层级 | 目标 | 说明 |
|------|------|------|
| L1 核心流水线指标 | 统一回答“吞吐、延迟、错误、积压、并发” | 所有主链路阶段必须接入，作为默认 dashboard 与 SLO 入口 |
| L2 子系统指标 | 统一回答“哪一层坏了” | queue / dag / scheduler / downloader / api / config / coordination / cache / logger |
| L3 调试与兼容指标 | 统一回答“为什么坏” | 保留 `ptm_commit`、`dag_remote_*`、Lua action、细粒度 backpressure 等诊断指标 |

目标不是删除全部专用指标，而是建立一套**先看 L1，再落到 L2，最后用 L3 排障**的观测顺序。

#### 13.2.2 标签模型与基数控制

所有新指标都必须从 typed runtime context 取标签，而不是临时从 `serde_json::Value` 或字符串拼接推断。

| 标签 | 来源 | 级别 | 说明 |
|------|------|------|------|
| `namespace` | `config.name` | 必带 | 命名空间根键；所有分布式指标必须显式区分 namespace |
| `node_id` | `crawler.node_id` 或运行时生成值 | 必带 | 真实执行节点；**不能**再复用 `namespace` |
| `component` | 固定枚举 | 必带 | `engine / queue / dag / api / scheduler / downloader / config / cache / logger / sync` |
| `deployment_mode` | `single / distributed` | 必带 | 方便单节点与分布式看板共用 |
| `pipeline` | 固定枚举 | 按需 | `task / request / download / response / parse / parser_task / error / dag / api / control` |
| `stage` | 固定枚举 | 按需 | `ingress / generate / build_request / download / parse / dispatch / ack / nack / dlq / commit / recover` |
| `backend` | 固定枚举 | 按需 | `memory / kafka / raft / rocksdb / http / wss` |
| `result` | 固定枚举 | 按需 | `success / error / retry / timeout / dropped / rejected / dlq / skipped / canceled` |
| `error_class` | 固定枚举 | 错误类 | `network / parse / policy / backend / timeout / validation / coordination / panic` |
| `error_code` | 受控枚举 | 错误类 | 禁止直接使用原始异常文本 |
| `module` | 模块名 | 受控 | 只在模块数可控的业务指标上允许使用 |
| `workflow` / `node_name` | DAG 上下文 | 受控 | 只在 DAG 相关指标上使用 |

基数控制规则：

1. 禁止把 `account`、`request_id`、`task_id`、`run_id`、原始 URL、原始错误消息作为标签。
2. `module`、`workflow`、`node_name` 只允许出现在真正需要按业务拓扑定位的指标上。
3. `backend` 必须覆盖 `memory / kafka / raft / rocksdb` 当前路径。
4. 所有分布式指标必须同时带 `namespace + node_id`，防止多节点聚合后失真。

#### 13.2.3 核心流水线指标（L1）

以下 5 类指标是所有处理阶段的统一骨架，覆盖 `task -> request -> download -> response -> parse -> parser_task -> error`，以及目标态 DAG dispatch / recover 路径：

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `mocra_stage_events_total` | Counter | `namespace,node_id,component,pipeline,stage,action,result,module?` | 统一吞吐计数 |
| `mocra_stage_duration_seconds` | Histogram | `namespace,node_id,component,pipeline,stage,action,result,module?` | 统一阶段耗时 |
| `mocra_stage_errors_total` | Counter | `namespace,node_id,component,pipeline,stage,error_class,error_code,module?` | 统一错误计数 |
| `mocra_stage_inflight` | Gauge | `namespace,node_id,component,pipeline,stage` | 当前在途工作量 |
| `mocra_stage_backlog` | Gauge | `namespace,node_id,component,pipeline,queue,priority,backend` | 当前积压深度 |

要求：

1. 每个主链路阶段都必须至少接入这 5 类指标中的适用子集。
2. `retry / nack / dlq / dropped / skipped` 不能只写日志，必须进入 `result` 维度。
3. 统一使用 `_seconds` 作为耗时单位；旧 `_ms / _us` 指标只允许在兼容窗口内保留。

#### 13.2.4 子系统指标族（L2）

##### Queue / MQ

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_queue_messages_total` | Counter | `backend,topic,priority,operation,result` | publish / consume / ack / nack / retry / dlq 总量 |
| `mocra_queue_message_bytes_total` | Counter | `backend,topic,operation` | 流量体积 |
| `mocra_queue_operation_duration_seconds` | Histogram | `backend,topic,operation,result` | 编码、投递、消费、提交耗时 |
| `mocra_queue_backlog` | Gauge | `backend,topic,priority` | topic / queue 深度 |
| `mocra_queue_consumer_lag` | Gauge | `backend,topic,consumer_group` | consumer lag |
| `mocra_queue_redelivery_total` | Counter | `backend,topic,reason` | reclaim / retry / replay 次数 |
| `mocra_queue_dlq_messages_total` | Counter | `backend,topic,reason` | 死信写入次数 |
| `mocra_queue_codec_total` | Counter | `backend,codec,operation,result` | encode / decode / blob offload 结果 |

##### Scheduler / Cron

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_scheduler_ticks_total` | Counter | `result` | tick 次数 |
| `mocra_scheduler_tick_duration_seconds` | Histogram | `result` | 单次 tick 耗时 |
| `mocra_scheduler_lock_total` | Counter | `result` | cron 触发锁竞争结果 |
| `mocra_scheduler_lock_duration_seconds` | Histogram | `result` | 锁获取耗时 |
| `mocra_scheduler_triggers_total` | Counter | `module,result` | 触发任务次数 |
| `mocra_scheduler_misfire_total` | Counter | `module,result` | misfire 补偿与放弃次数 |
| `mocra_scheduler_refresh_total` | Counter | `result` | cache/config refresh 次数 |
| `mocra_scheduler_oldest_unprocessed_tick_age_seconds` | Gauge | `module?` | 最老未处理 tick 年龄 |

##### DAG

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_dag_runs_total` | Counter | `workflow,result` | DAG run 总量 |
| `mocra_dag_run_duration_seconds` | Histogram | `workflow,result` | 单个 run 耗时 |
| `mocra_dag_nodes_total` | Counter | `workflow,node_name,result` | 节点执行次数 |
| `mocra_dag_node_duration_seconds` | Histogram | `workflow,node_name,result` | 节点耗时 |
| `mocra_dag_dispatch_total` | Counter | `backend,placement,result` | 本地/远程派发 |
| `mocra_dag_ready_nodes` | Gauge | `workflow` | ready queue 当前长度 |
| `mocra_dag_oldest_incomplete_run_age_seconds` | Gauge | `workflow` | 最老未完成 run 年龄 |
| `mocra_dag_run_guard_total` | Counter | `operation,result` | acquire / renew / release |
| `mocra_dag_run_guard_duration_seconds` | Histogram | `operation,result` | run guard 耗时 |
| `mocra_dag_fencing_total` | Counter | `result` | fencing 提交接受 / 拒绝次数 |
| `mocra_dag_recovery_total` | Counter | `reason,result` | run resume / replay / recovery 次数 |
| `mocra_dag_state_store_total` | Counter | `operation,result` | run state save / load / clear |

`mocra_dag_remote_*`、`mocra_ptm_commit_total` 等现有指标可作为 L3 调试指标；目标态 L2 应以 backend/placement 标签统一。

##### Downloader / Proxy / 限流

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_http_requests_total` | Counter | `module,host,method,status_class,result,proxy_group?` | 请求总量 |
| `mocra_http_request_duration_seconds` | Histogram | `module,host,method,status_class,result` | 请求耗时 |
| `mocra_http_response_bytes_total` | Counter | `module,host,status_class` | 响应体体积 |
| `mocra_http_phase_duration_seconds` | Histogram | `phase` | dns / connect / tls / ttfb / body |
| `mocra_proxy_acquire_total` | Counter | `proxy_group,result` | 代理获取与失败 |
| `mocra_proxy_pool_size` | Gauge | `proxy_group,state` | ready / cooling / bad |
| `mocra_proxy_health` | Gauge | `proxy_group` | 代理组健康 |
| `mocra_rate_limit_actions_total` | Counter | `action,result` | suspend / decrease / restore / reject |

##### Parser / Data / Store

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_generate_tasks_total` | Counter | `module,result` | generate 输出任务数 |
| `mocra_parse_outputs_total` | Counter | `module,node_name,output_type,result` | next / data / finished |
| `mocra_parse_duration_seconds` | Histogram | `module,node_name,result` | parser 耗时 |
| `mocra_parsed_records_total` | Counter | `module,entity` | 解析记录数 |
| `mocra_parse_empty_total` | Counter | `module,reason` | 空输出 / 空数据推进次数 |
| `mocra_data_store_writes_total` | Counter | `store,entity,result` | 数据落库次数 |
| `mocra_data_store_duration_seconds` | Histogram | `store,entity,result` | 落库耗时 |
| `mocra_data_quality_total` | Counter | `module,rule,result` | 去重、字段校验、丢弃等质量事件 |

##### API / Control / Config

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_api_requests_total` | Counter | `route,method,status_class` | API 请求量 |
| `mocra_api_request_duration_seconds` | Histogram | `route,method,status_class` | API 耗时 |
| `mocra_api_auth_total` | Counter | `route,result` | 认证成功/失败 |
| `mocra_api_rate_limit_total` | Counter | `route,result` | API 限速结果 |
| `mocra_control_actions_total` | Counter | `action,result` | pause / resume / start_work / dlq_read / nodes_query |
| `mocra_config_updates_total` | Counter | `scope,result` | profile/config 更新次数 |
| `mocra_config_apply_duration_seconds` | Histogram | `scope,result` | apply 耗时 |
| `mocra_config_applied_version` | Gauge | `node_id,module` | 节点已应用版本 |
| `mocra_config_version_skew_nodes` | Gauge | `module` | 配置版本漂移节点数 |
| `mocra_pause_state` | Gauge | `namespace` | 暂停状态 |

##### Coordination / Cluster

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_cluster_nodes_active` | Gauge | `namespace` | 活跃节点数 |
| `mocra_cluster_heartbeat_age_seconds` | Gauge | `node_id` | 距离最近 heartbeat 的年龄 |
| `mocra_leader_changes_total` | Counter | `domain,result` | leader 切换 |
| `mocra_coordination_ops_total` | Counter | `backend,operation,result` | 状态机、锁、协调操作 |
| `mocra_lock_acquire_total` | Counter | `scope,result` | 分布式锁获取 |
| `mocra_lock_wait_duration_seconds` | Histogram | `scope,result` | 分布式锁等待耗时 |
| `mocra_sync_messages_total` | Counter | `topic,codec,result` | distributed sync 消息 |
| `mocra_sync_message_duration_seconds` | Histogram | `topic,operation,result` | sync encode/decode/apply 耗时 |

##### Cache / Dedup / Logger

| 指标名 | 类型 | 关键标签 | 说明 |
|--------|------|----------|------|
| `mocra_cache_ops_total` | Counter | `backend,operation,result` | cache get/set/mget |
| `mocra_cache_duration_seconds` | Histogram | `backend,operation,result` | cache 耗时 |
| `mocra_cache_evictions_total` | Counter | `backend,reason` | cache 淘汰 |
| `mocra_dedup_checks_total` | Counter | `layer,result` | bloom / l1 / l2 去重 |
| `mocra_dedup_duration_seconds` | Histogram | `path,result` | dedup 耗时 |
| `mocra_log_events_total` | Counter | `sink,level` | 日志量 |
| `mocra_log_dropped_total` | Counter | `sink,reason` | 日志丢弃 |
| `mocra_log_queue_lag` | Gauge | `sink` | log pipeline 滞后 |

#### 13.2.5 命名与单位规则

统一规则：

1. Counter 必须以 `_total` 结尾。
2. Histogram 必须以 `_duration_seconds` 或 `_bytes` 结尾。
3. Gauge 只表达“当前值”，不表达累计量。
4. 新指标不再引入 `_ms` / `_us` 命名；旧指标仅在兼容期保留。
5. 错误标签只能使用受控 `error_class / error_code`，不能直接使用异常文本。

迁移映射建议：

| 当前指标族 | 目标指标族 | 说明 |
|------------|------------|------|
| `mocra_throughput_total` | `mocra_stage_events_total` | 统一吞吐语义 |
| `mocra_latency_seconds` | `mocra_stage_duration_seconds` | 统一阶段耗时 |
| `mocra_errors_total` | `mocra_stage_errors_total` | 统一错误模型 |
| `mocra_backlog_depth` | `mocra_queue_backlog` / `mocra_stage_backlog` | 区分 queue backlog 与 stage backlog |
| `mocra_resource_usage` | 保留为主机资源 gauge，业务指标不再混入 | 避免把 host 与业务观测混在一族里 |
| `mocra_policy_decisions_total` | 兼容期保留为 L3；长期并入各域指标 | 作为策略调试指标逐步收口 |

#### 13.2.6 Dashboard 与告警

推荐默认 dashboard 分组：

| 看板 | 核心内容 | 主要回答的问题 |
|------|----------|----------------|
| Cluster Overview | `node_up`、活跃节点、CPU/内存、leader 状态 | 集群是否还活着 |
| Pipeline SLA | 各阶段吞吐、错误率、P95/P99、inflight/backlog | 哪个 stage 成了瓶颈 |
| Queue & Backpressure | backlog、lag、ack/nack/retry/dlq、queue_full | 堵在 MQ 还是本地 channel |
| DAG Execution | run 成功率、node 成功率、run_guard/fencing、oldest incomplete run | DAG 是否稳定推进 |
| Downloader & Proxy | 请求量、状态码、耗时、network error、proxy 健康、限流动作 | 下载链路是否健康 |
| Control Plane & Config | API QPS、配置 apply、version skew、pause/resume | 控制面是否一致 |

推荐告警分组：

1. 节点不可达：`mocra_node_up == 0` 或 `/metrics` 抓取中断。
2. 队列堆积：`queue_backlog` / `consumer_lag` 持续上升。
3. 主链路劣化：`stage_errors_total` 或 `stage_duration_seconds` 的错误率、P99 异常升高。
4. DAG 卡死：`oldest_incomplete_run_age_seconds`、`run_guard` 失败或 `fencing` 拒绝异常升高。
5. 控制面漂移：`config_version_skew_nodes > 0` 持续不收敛。
6. 下载异常：timeout / 429 / 5xx / proxy acquire failure 激增。

#### 13.2.7 迁移规则

指标重构顺序建议固定为：

1. 先修基础标签：拆分 `namespace` 与 `node_id`，重建 `MetricsScope`。
2. 再接入核心流水线 5 件套：统一 task/request/download/response/parse/error。
3. 再对齐 queue backend：Kafka、in-memory 使用同一指标族。
4. 再补 DAG / scheduler / control plane / config hot update。
5. 最后补 downloader / parser / cache / dedup / logger 的子系统指标。
6. 兼容窗口内同时保留旧指标名与新指标名；当 dashboard 和告警完成切换后，再删除 legacy 指标。

---

## 14. 部署拓扑

### 14.1 单节点部署

- SQLite / PostgreSQL
- 本地 Raft 状态机副本 + RocksDB
- 本地 Tokio queue
- 单进程承载全部能力（generate / download / parse / scheduler）

### 14.2 单命名空间分布式部署

同一个 `config.name`，多个节点横向扩展：

- PostgreSQL
- Kafka（或其他 MqBackend）
- Raft + RocksDB（缓存、限流、锁、协调、状态）
- 多实例统一二进制，所有节点共享同一 `config.name`
- Leader 选举决定谁触发 cron 调度
- 所有节点均可执行 generate / download / parse

### 14.3 多命名空间联邦部署（Namespace Federation）

多个任务（不同 `config.name`）共享同一套基础设施，组成联邦网络：
不同的config.name 代表不同的命名空间（Namespace），每个命名空间独立选举 leader，负责触发调度和管理本 namespace 的状态；但所有 namespace 可以共享同一套消息队列、数据库以及跨 namespace 的 Download Pool。

```text
┌─────────────────────────────────────────────────────────────────┐
│              共享基础设施层（MQ / DB / Download Pool）             │
├──────────────────────────┬──────────────────────────────────────┤
│  Namespace A             │  Namespace B                        │
│  Leader(A) → 触发调度     │  Leader(B) → 触发调度               │
│  nodes(A): generate/parse│  nodes(B): generate/parse           │
├──────────────────────────┴──────────────────────────────────────┤
│           共享 Download Pool（任意联邦节点均可处理）               │
│           topic: shared-download / 或各自 namespace download     │
└─────────────────────────────────────────────────────────────────┘
```

**联邦部署要点：**

| 能力 | 归属 |
| --- | --- |
| cron 调度触发 | 各命名空间 Leader 独立负责 |
| `generate()` | 拥有模块代码的命名空间节点 |
| `download` | 联邦共享 Download Pool（跨命名空间） |
| `parser()` | 拥有模块代码的命名空间节点（`Response` 按 namespace 标记路由回） |
| 状态/元数据 | 严格按 `config.name` 隔离，互不干扰 |


---

## 15. 渐进式演进路径

### 15.1 阶段划分

| 阶段 | 目标 | 交付边界 |
|------|------|----------|
| **Phase 1** | 收口 `generate()` 的 remote placement 调度链 | 当前 target node 的 generate 在本地/远程 placement 下都走统一 scheduler bridge |
| **Phase 2** | 建立 parser scheduler bridge | `ModuleNodeDagAdapter` 能消费 parser 所需 runtime input，scheduler 可执行真实 parser |
| **Phase 3** | 切换 parser/error ingress 到 scheduler | parser/error 主链路不再依赖本地快路径推断，兼容层只保留明确回退边界 |
| **Phase 4** | Pack 6-8 平台化收口 | 控制面 API、可观测性、bootstrap/legacy removal 进入主线 |

### 15.2 Phase 1：Generate Remote Placement 收口

**阶段目标**

- 把 `Module::generate()` 当前已打通的“单节点、本地 scheduler bridge”扩展到 remote placement。
- 保持 `ModuleDagProcessor` 仍然作为 parser 路径与 fail-closed 兜底，不在这一阶段强行切 parser。

**历史交付清单**

- [x] 为 `Module::generate()` 注入可配置的 `DagNodeDispatcher` 构造来源，而不是固定使用 `LocalNodeDispatcher`
- [x] 打通 `RedisRemoteDispatcher` 或等价 dispatcher 的装配链，让 scheduler bridge 能按 `NodePlacement::Remote` 真正派发
- [x] 保持 remote placement 失败时的显式错误语义，避免重新退回静默本地执行
- [x] 为本地/远程两种 placement 各补一条 focused regression test
- [x] 补一条端到端验证：当前 target node 的 generate 在 remote placement 下可返回 request batch，并保持 `ExecutionMark` / `run_id` / `prefix_request` 一致

### 15.3 Phase 2：Parser Runtime Bridge

**阶段目标**

- 为 parser 建立与 generate 对称的 scheduler runtime bridge。
- 让 scheduler 可以在拿到 response 运行态后执行真实 `ModuleNodeTrait::parser()`，而不只是 generate。

**历史交付清单**

- [x] 定义 parser runtime input 的 payload schema，覆盖 `Response`、`RoutingMeta`、`ExecutionMeta`、`ResolvedNodeConfig` 等 parser 所需上下文
- [x] 在 `ModuleNodeDagAdapter` 中补 parser 路径的 decode / invoke / encode 逻辑
- [x] 评估并明确 parser 输出到 `NodeDispatch` / error envelope / stop signal 的桥接边界
- [x] 当前边界：scheduler parser bridge 已可编码 `NodeDispatch + data + finished`；error envelope 仍留在 Phase 3 ingress cutover 统一收口
- [x] 已补齐回归覆盖：local placement 下 generate/parser 在“无 dispatcher”场景都可由 scheduler 直接本地执行；parser 产出的 `NodeDispatch` 与 `data` 现都可经 scheduler bridge 本地/远程 round-trip，而一旦 scheduler bridge 执行失败，generate/parser 仍均 fail-closed，remote/no-dispatcher 继续显式 fail-closed
- [x] 增加 parser runtime payload 的本地 round-trip 测试与 remote wire compatibility 测试
- [x] 增加 scheduler 执行真实 parser 的 focused regression test

### 15.4 Phase 3：Parser/Error Ingress Scheduler Cutover

**阶段目标**

- 把 parser/error ingress 从当前本地快路径正式切到 scheduler 入口。
- 收口剩余兼容层推断，让 runtime routing hint 和 typed context 成为主路径真相。

**历史交付清单**

- [x] 在 parser/error ingress 上建立明确的 scheduler 执行入口，而不是继续由本地 `ModuleDagProcessor` 隐式推进
- [x] 把 parser/error envelope 中的 runtime routing hint 与 typed context 显式映射到 scheduler runtime overrides / runtime_input
- [x] `TaskModelProcessor` 的 parser/error scheduler ingress 已改为显式透传模块 `DagNodeDispatcher`，并在 runtime hint 指向 remote placement 且 dispatcher 缺失时 fail-closed（不再依赖后置隐式报错）
- [x] parser/error ingress 在 typed context 缺失时可从 transport 字段恢复 `node_id/module` 继续尝试 scheduler 主路径，并把缺失 transport 目标字段的情形收口为 fail-closed，而不是继续回退 legacy chain
- [x] parser/error ingress 已切换为 typed-first 目标恢复：优先使用 transport 字段（`routing/module/node_key`、`NodeDispatch.target_node`、typed metadata/prefix）驱动 scheduler 主路径，仅在缺失时才读取 legacy seed 兜底
- [x] 已补齐 parser/error metadata 与 prefix_request 的 typed-first 优先级回归：typed context > transport parent/request fields > legacy seed
- [x] 清理仅为过渡期保留的 fallback 推断分支，并为保留项写明退出条件
- [x] 为 cutover 增加 shadow compare / warmup gate / failure gate 的校验测试
- [x] 更新运行文档，明确 parser/error 主路径、fallback 边界与 gate/block 策略

**重构进度评估（2026-04-15）**

- Phase 3 完成度：5/5（含 fallback 推断分支清理与最小保留项边界）
- 代码健康检查（当时）：`cargo test --lib` 通过（`337 passed, 0 failed, 1 ignored`），`cargo test --all-targets` 通过；当前分支最新全量 `cargo test --lib` 结果见 21.1 当前验证基线
- gate 覆盖现状：failure/warmup/shadow 三类 gate 均有可执行单测并通过（`engine::task::task_manager::tests::*`）
- 当前主要风险：显式 fallback 已主要收敛到 gray/failure gate 这类 rollout 控制分支，后续重点转向目标环境 gate 阈值、告警与 release block 证据闭环

**Phase 3 保留项与退出条件（fallback）**

- 保留项范围：parser/error ingress 不再保留“typed context / transport 目标字段缺失时回退 legacy chain”的兼容分支；当前仅保留 gray/failure gate 未放行时的显式 rollout fallback
- 退出条件：
    1. shadow compare 与 warmup gate 在目标模块满足上线阈值并持续稳定
    2. 目标环境中的 alert gate / probe whitelist / release block 证据闭环
    3. 线上观测窗口内 `gray_gate_blocked` / `failure_gate_blocked` fallback 命中持续收敛至 0（或满足约定阈值）
    4. 具备可验证的按模块粒度阻断与恢复验证结果

### 15.4.1 重构深度报告（基于当前代码，2026-04-16）

本报告基于当前仓库已落地代码进行反向核对，目标是回答三件事：

1. 这轮重构到底“把什么从旧模型迁到了新模型”；
2. 迁移后的真实运行边界在哪里（哪些是主路径、哪些仍是兼容保留）；
3. 下一阶段若继续推进，需要优先清理哪些技术债。

#### A. 执行链路：从 legacy event 向 typed envelope-first 收口

**已完成的结构性变化**

- `task/request/response/parser/error` 五类入口都具备了 typed envelope 适配器，且主路径在 worker 入站侧优先读取 envelope 的 routing/exec/context 信息。
- 适配层不再只是“序列化壳”，而是把路由语义（`RoutingMeta`）、执行语义（`ExecutionMeta`）和节点输入语义（`NodeInput` / typed payload）显式分层。

**关键代码证据**

- `src/engine/task/task_dispatch_adapter.rs`：`TaskEvent -> TaskDispatchEnvelope`，并把 retry/created_at 回灌到 `ProcessorContext`。
- `src/engine/task/request_response_adapter.rs`：`Request/Response` 双向 envelope 化，`ResponseDispatchEnvelope` 支持 payload offload/reload。
- `src/engine/task/parser_error_adapter.rs`：parser/error typed context 与 legacy seed 的统一桥接（包含 runtime node hint 注入与回读）。

**架构意义**

- 运行时不再依赖“某个阶段隐式约定字段必须存在”；
- 代码层面已经具备按 transport 契约跨进程/跨节点传输的稳定基础。

#### B. Scheduler 收口：parser/error ingress 已进入 typed-first 真实主路径

**已完成的行为收口**

- `UnifiedTaskIngressChain` 的 parser/error scheduler ingress 已切到 typed-first：
    - node/module：优先 typed context 与 transport 字段；
    - metadata/prefix：优先 typed context，再取 transport parent/request，最后才落到 legacy seed；
    - remote hint 且无 dispatcher：显式 fail-closed，不回退为静默本地执行。

**关键代码证据**

- `src/engine/chain/task_model_chain.rs`：
    - `try_scheduler_parser_dispatch()` / `try_scheduler_error_dispatch()`；
    - `resolve_parser_*` / `resolve_error_*` 系列解析器；
    - `requires_remote_dispatch_hint()` 与 fallback/result/error 指标记录。

**回归证据（focused）**

- 同文件 `scheduler_ingress_tests` 已覆盖：
    - transport 字段可独立驱动解析；
    - typed context 对 metadata/prefix 的优先级；
    - transport 与 seed 的后备优先级链路；
    - remote placement hint 检测分支。

#### C. 节点执行桥：DAG 调度层已经可消费真实 generate/parser 逻辑

**已完成的桥接能力**

- `ModuleNodeDagAdapter` 已可根据 `runtime_input` 类型分别执行：
    - generate：调用 `ModuleNodeTrait::generate()`，编码 request batch；
    - parser：调用 `ModuleNodeTrait::parser()`，编码 parser output payload。
- runtime_input 缺失或 node_key 不匹配时会显式报错，避免 silent corruption。

**关键代码证据**

- `src/engine/task/module_node_dag_adapter.rs`：`start()` 中 generate/parser 双分支与错误语义。
- `src/engine/task/workflow_compiler.rs`：兼容 `dag_definition()` 与 `add_step()`，并在双定义并存时对 legacy 线性步骤做命名空间化合并。
- `src/engine/task/profile_loader.rs`：按 workflow 节点构造 `TaskProfileSnapshot.node_configs`，并把 legacy `ModuleConfig` 映射到 typed node/middleware envelope。

**架构意义**

- 调度器已经不是“只会排图不会跑节点”的空壳；
- 运行期“图定义、节点配置、节点执行”的三层边界在代码中已可追踪。

#### D. 控制面：从“只读配置”升级到可写一致性面

**已完成的控制面增量**

- API 层已具备 `/config/*`、`/debug/*`、`/cluster/*`、`/control/*`、`/tasks/dispatch` 的完整路由面。
- `ProfileControlPlaneStore` 在本地 RocksDB 上承载 profile/default/middleware/status/pause 等状态，并预留与 Raft runtime 的挂接。
- `control`/`config`/`debug` handler 已统一纳入 throughput/latency/error 指标口径。

**关键代码证据**

- `src/engine/api/router.rs`：控制面路由总装配与鉴权/限流分层。
- `src/engine/api/profile_store.rs`：控制面状态落盘模型、patch/upsert 入口、raft runtime 绑定。
- `src/engine/api/control.rs`、`src/engine/api/config.rs`、`src/engine/api/debug.rs`：leader 视图、配置 CRUD、调试剖面与状态查询。

#### E. 数据面：Queue 契约显式化与 codec 策略固化

**已完成的数据面收口**

- 引入 `QueueRouteContract`，把 topic、批处理策略、并行度与 backpressure scope 从散落逻辑收敛为统一契约。
- queue codec 由配置驱动并在进程启动时固化（`OnceCell`），与当前“滚动重启切 codec”运维约束一致。

**关键代码证据**

- `src/queue/contract.rs`：route/batch/priority/backpressure 的统一建模。
- `src/queue/manager.rs`：按 route 合并 forward/subscribe 主流程。
- `src/queue/codec.rs`：json/msgpack 双 codec、typed `QueueEnvelope` 编解码与配置注入。

#### F. 一致性层：Raft runtime 已形成可落地控制面闭环

**已完成的一致性能力**

- 已具备 `RaftRuntimeConfig`（节点、peer、超时、snapshot、data_dir）到运行态的构造路径。
- 已定义 raft RPC 路径（append/vote/snapshot/join/client-write）并具备 leader 视图抽象。
- 本地无 raft 配置时可回落本地 leadership gate，便于单机部署与测试环境平滑运行。

**关键代码证据**

- `src/sync/raft.rs`：openraft 类型声明、运行时配置、RPC 路径常量、存储骨架。
- `src/sync/tests/test_local_leadership_gate.rs`：本地 gate 回退行为验证。

#### G. 当前风险与下一步建议（按优先级）

1. **rollout gate 仍是唯一显式 fallback 分支**：parser/error ingress 对缺失 `module/node` transport 已改为 fail-closed，剩余需要持续观测的是 gray/failure gate 命中与其下线时间窗。
2. **codec 热更新不可在线切换**：当前 `OnceCell` 固化是合理约束，但需要在运维手册中明确“变更即滚动重启”。
3. **控制面与执行面版本绑定仍需进一步制度化**：`profile_version + DAG 版本 + run_id` 的三元绑定已在代码中具备基础，但还需端到端演练脚本持续压测验证。

#### H. 结论

这轮重构不是“局部重写”，而是把核心运行时从“隐式字段 + 阶段约定”迁移到“typed transport + scheduler bridge + control-plane consistency”的系统性改造。当前代码状态已经具备进入下一阶段（Pack 8 装配边界清理与 fallback 下线窗口验证）的工程条件。

### 15.5 Phase 4：Pack 6-8 平台化收口

Pack 6-8 当前已完成，不再保留旧版分批回滚清单：

- Pack 6 控制面 API 已接入 `ApiState`、`ProfileControlPlaneStore` 与 leader/follower 写路径。
- Pack 7 metrics facade、dashboard baseline、alert gate 已落地，失败处理以 release block / gate block 为准。
- Pack 8 已完成 State/Engine 装配边界收口，Redis/Lua/rollback/legacy runtime builder 清理完成。
- 旧 `cutover_rehearsal` 脚本、旧 rollback 归档和 `docs/pack8-priority-decision.md` 已删除。

当前执行约束：

- 禁止新增 Redis 后端、Redis 配置字段、Redis 环境变量或 Redis 依赖。
- 禁止新增 rollback 兼容流程；灰度失败只能进入 release block / gate block。
- 禁止新增 `build_legacy_*` runtime builder 和 `"legacy.*"` schema ID。
- 每次触碰 hot path 后必须运行第 21.4 节列出的检查命令。

## 16. 最终架构结论

结合当前代码与联邦设计，mocra 下一阶段最合理的系统架构定义如下：

1. **以现有 `State + Engine + QueueManager + ProcessorRunner + TaskManager` 为稳定内核**
2. **每个任务拥有独立的 `config.toml` 和 `config.name` 命名空间，命名空间是状态与调度的最小隔离单元**
3. **多个命名空间可以共享同一套消息队列 / 数据库 / Download Pool 基础设施，组成联邦分布式网络**
4. **联邦中的职责边界固定**：
   - cron 调度触发：各命名空间 leader 独立负责
   - `generate()` 与 `parser()`：归属命名空间节点（持有模块代码）
   - download：联邦共享 Download Pool，跨命名空间池化
5. **以 `Raft + RocksDB` 建立去中心化元数据控制面，所有元数据通过 Raft 日志复制同步，按 `config.name` 命名空间隔离**
6. **把 `StatusTracker + NodeRegistry + LockService + RateLimiter + CacheService` 收敛为统一控制面能力，全部复用底层 Raft 状态机**
7. **把 ParserTask 上已落地的执行一致性模型（claim / commit-before-ack / fencing）推广为统一执行状态机**
8. **数据落地固定由 `DataStoreMiddleware` 三段式生命周期承担，不引入独立 sink 抽象**

换句话说，优化后的 mocra 架构应是：

- **每个命名空间是自治的调度与执行单元**（独立 config / leader / 状态）
- **多个命名空间在基础设施层联邦协作**（共享 download 能力 + 共用 MQ/DB）
- **数据面保持轻、快、可扩展**
- **控制面以 Raft + RocksDB 提供去中心化强一致元数据同步**
- **执行状态面成为连接控制面与数据面的核心**

---

## 17. 代码地图

### 17.1 核心引擎

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/engine/engine.rs` | `Engine` | 主协调器，负责组装 queue/task/downloader/middleware/api/scheduler/event-bus |
| `src/common/state.rs` | `State` | 组合根，加载配置并初始化 DB/cache/lock/limit/status |
| `src/engine/runner.rs` | `ProcessorRunner` | pause/shutdown aware 的有界并发执行器 |
| `src/engine/engine/runtime.rs` | processor runtime loops | 各 processor 循环与后台 watcher |
| `src/engine/task/task_manager.rs` | `TaskManager`, embedded `DagCutoverStateTracker` | 模块注册、Task 装配、DAG 预编译与切换门控 |
| `src/engine/task/module_dag_orchestrator.rs` | `ModuleDagOrchestrator` | 合并 `dag_definition()` 与 `add_step()` 两套 DAG 定义 |

### 17.2 队列系统

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/queue/manager.rs` | `QueueManager` | 本地 channel 与 MQ backend 桥接；batch/codec/compress/DLQ/NACK |
| `src/queue/channel.rs` | `Channel` | 11 组 Tokio mpsc channel |
| `src/queue/lib.rs` | `MqBackend`, `QueuedItem`, `Message`, `NackPolicy` | ACK/NACK 抽象与队列协议面 |
| `src/queue/batcher.rs` | `Batcher` | 出站/入站统一批次窗口执行器 |
| `src/queue/kafka.rs` | `KafkaQueue` | Kafka backend |

### 17.3 分布式协调

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/common/registry.rs` | `NodeRegistry` | 当前 cache/ZSET 节点注册表；目标态迁移到 Raft 状态机 |
| `src/common/status_tracker.rs` | `StatusTracker` | 当前错误/状态跟踪器；目标态迁移到统一状态机 |
| `src/cacheable/cache_service/service.rs` | `CacheService` | KV / TTL / ZSET / L1 facade |
| `src/cacheable/cache_service/raft_backend.rs` | `RaftRocksDbCacheBackend` | Raft/RocksDB cache backend |
| `src/cacheable/cache_service/local_backend.rs` | `LocalBackend` | 单机内存 backend |
| `src/utils/distributed_lock.rs` | `DistributedLockManager`, `RaftLockBackend` | 分布式锁 facade 与 Raft 后端 |
| `src/utils/distributed_rate_limit.rs` | `DistributedSlidingWindowRateLimiter`, `RaftRateLimitBackend` | 分布式限流 facade 与 Raft 后端 |

### 17.4 执行一致性

| 文件 | 主要类型/函数 | 用途 |
|------|---------------|------|
| `src/common/processors/processor.rs` | `ProcessorResult`, `RetryPolicy`, `ProcessorContext` | 责任链执行输出契约 |
| `src/engine/engine.rs` | `handle_policy_failure`, `handle_policy_retry` | 成功/重试/DLQ 的队列侧效执行 |
| `src/queue/lib.rs` | `AckFn`, `NackFn`, `NackDisposition` | ACK/NACK 语义与重试决策 |
| `src/engine/task/module_dag_processor.rs` | `ModuleDagProcessor` | DAG 节点执行与上下文推进 |
| `src/schedule/dag/scheduler.rs` | `DagScheduler` | 目标态分布式 DAG 调度层：run guard、resume、fencing、dispatch |
| `src/schedule/dag/types.rs` | `DagNodeDispatcher`, `DagRunGuard`, `DagRunStateStore`, `DagFencingStore`, `NodePlacement` | DAG 分布式执行契约与节点放置抽象 |

### 17.5 模型定义

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/common/model/config.rs` | `Config` | 完整配置树（含 cache backend、queue、api、raft 配置） |
| `src/common/model/message.rs` | `TaskEvent`, `UnifiedTaskInput` | 当前任务载体与 ingress 兼容输入 |
| `src/common/model/request.rs` | `Request` | 下载请求模型 |
| `src/common/model/response.rs` | `Response` | 下载响应模型 |
| `src/common/model/meta.rs` | `MetaData` | 当前实现的 Value-heavy 元数据兼容层；目标态会拆为强类型 metadata + envelope |
| `src/common/model/priority.rs` | `Priority`, `Prioritizable` | 优先级抽象 |

### 17.6 调度与事件

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/engine/scheduler.rs` | `CronScheduler` | cron 触发、context refresh、leader guard |
| `src/engine/events/event_bus.rs` | `EventBus` | 进程内 fan-out 事件总线 |
| `src/engine/events/handlers/*.rs` | 各类 handler | console / MQ 等事件输出 |

### 17.7 中间件

| 文件 | 主要 Trait/类型 | 说明 |
|------|---------------|------|
| `src/common/interface/middleware.rs` | `DownloadMiddleware`, `DataMiddleware`, `DataStoreMiddleware` | 中间件接口与 weight 排序 |
| `src/common/interface/data_store.rs` | `StoreTrait` | 数据落地事件构造辅助 |

### 17.8 API

| 文件 | 说明 |
|------|------|
| `src/engine/api/router.rs` | axum 路由注册、`start_work`、`metrics_handler` |
| `src/engine/api/auth.rs` | API key 鉴权中间件 |
| `src/engine/api/limit.rs` | API 限速中间件 |
| `src/engine/api/control.rs` | `get_nodes` / `pause_engine` / `resume_engine` |
| `src/engine/api/dlq.rs` | `get_dlq` |
| `src/engine/api/health.rs` | `health_check` |

### 17.9 下载器

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/downloader/downloader_manager.rs` | `DownloaderManager` | 下载器注册表 + 实例生命周期管理 |
| `src/downloader/request_downloader.rs` | `RequestDownloader` | 主 HTTP 下载器（Session + Response Cache + 限速 + 锁）|
| `src/downloader/websocket_downloader.rs` | `WebSocketDownloader` | WebSocket 长连接下载器 |
| `src/downloader/lib.rs` | `Downloader` trait | 下载器抽象接口（`download`, `health_check`, `close`）|

### 17.10 Response Cache：分布式文件缓存

Response Cache 被定义为一个**分布式文件存储能力**：

> **当前代码说明：** 现有实现已经具备 `Request.enable_response_cache(bool)` / `enable_response_cache_with(&T)`、`Response::sync(hash, cache_service)` 的缓存命中路径，以及 downloader 成功返回后的 cache 写回路径；当前缓存仍是把完整 `Response` 放进当前 `CacheService` backend，并在 metadata 中保留 `response_cache_owner_namespace`、可选 `response_cache_owner_node_id` 与 `response_cache_lookup_key`。parser 侧的历史 cache refresh 现已把回流响应显式落成本 namespace 的本地缓存副本，并同步写入 namespace-local 的 `cache_key -> owner` 索引，避免 foreign namespace ownership 直接作为 parser 侧本地缓存锚点。与此同时，节点心跳已开始发布可选 `api_port`，受保护 API 也能按 cache key 读取本地 cached response；当前同 namespace 的 downloader 节点已能通过 control-plane 中的 `cache_key -> owner_node_id` 索引与 `/debug/cache/response/{cache_key}` 回读远端缓存并回填本地 cache，而且回填成功后的本地副本会立即重写为当前 downloader 的本地 owner 并覆盖 owner 索引，使 warmed copy 自身变成可发现的同 namespace fallback anchor。foreign namespace 场景下，当前实现新增了一个最小显式入口：通过 `channel_config.federation_response_cache_api_endpoints` 把 owner namespace 映射到受保护 API base URL，再走同一条 `/debug/cache/response/{cache_key}` 回源；此外，owner 索引现在已经具备首批安全失效路径，会在“self-owned 本地 miss”“同 namespace owner 已不在活跃心跳集”或“远端 owner 返回 404”这类可判定为当前记录失真的负信号上条件清理旧记录，避免索引无限累积陈旧 owner。这仍不是目标态的 shared-download/service-discovery 方案。v2 目标态将把“大 body 存储”从一致性层剥离出来，改为“本地文件 + Raft 索引 + gRPC 回源”。

| 层 | 存储内容 | 说明 |
|----|----------|------|
| `Request` | `enable_response_cache`、`cache_key`、`ttl_secs` | 决定当前请求是否参与 Response Cache |
| 下载节点本地磁盘 | `status_code`、`headers`、`cookies`、`content` | 实际缓存数据，仅保存在实际执行下载的节点 |
| Raft 状态机 | `cache_key -> owner_node_id + expires_at` | 全局一致的索引元数据，不存大 body |
| gRPC (`NodeCacheService`) | `GetCachedResponse(cache_key)` | 其他节点按索引回源到 owner 节点拉取缓存 |

#### 17.10.1 Request API 设计

```rust
impl Request {
    /// 启用 Response Cache；ttl_secs=None 时使用 download_config.cache_ttl（默认 86400）
    pub fn enable_response_cache(mut self, ttl_secs: Option<u64>) -> Self {
        self.enable_response_cache = true;
        self.response_cache_ttl_secs = ttl_secs;
        self
    }

    /// 使用显式业务 key 启用 Response Cache；适合跨运行复用缓存
    pub fn enable_response_cache_with<T>(mut self, hash_able: &T, ttl_secs: Option<u64>) -> Self
    where
        T: Serialize,
    {
        if let Ok(cache_key) = serde_json::to_string(hash_able) {
            self.enable_response_cache = true;
            self.response_cache_ttl_secs = ttl_secs;
            self.hash_str = Some(md5(cache_key.as_bytes()).to_string());
        }
        self
    }
}
```

- `ttl_secs = None`：使用 `download_config.cache_ttl`
- `download_config.cache_ttl` 默认值：`86400` 秒（24 小时）
- `enable_response_cache()`：使用**稳定的请求摘要**作为 key，**不应包含 `run_id` 等运行期字段**
- `enable_response_cache_with()`：允许用户显式指定业务稳定 key（如商品 ID、分页参数、查询条件）

#### 17.10.2 缓存写入流程

```text
Request(enable_response_cache=true)
    │
    ├─ 正常执行 HTTP 下载
    │
    ├─ 下载完成后，在实际下载节点本地磁盘写入缓存文件
    │      路径示例：{data_dir}/response_cache/{namespace}/{cache_key[0..2]}/{cache_key}.bin
    │
    ├─ 计算 expires_at = now + ttl_secs（未指定则取 86400 秒默认值）
    │
    └─ 向 Raft 提交：
           UpsertResponseCacheIndex {
               cache_key,
               owner_node_id = current_node_id,
               expires_at,
           }
```

**关键约束：先写本地文件，再写 Raft 索引。**  
这样即使节点在索引提交前崩溃，最多损失一次缓存写入，不会产生“Raft 里有索引但磁盘无文件”的成功假象。

#### 17.10.3 缓存读取流程

```text
Request(enable_response_cache=true)
    │
    ├─ 1. 本地读取已 apply 的 Raft 状态机：lookup(cache_key)
    │
    ├─ 2. miss / expired
    │      └─▶ 直接走正常下载流程
    │
    ├─ 3. hit 且 owner_node_id == current_node_id
    │      └─▶ 直接从本地磁盘读取缓存文件
    │
    └─ 4. hit 且 owner_node_id != current_node_id
           └─▶ gRPC: GetCachedResponse(namespace, cache_key)
                    ├─ success 且未过期 → 返回缓存内容
                    └─ timeout / 连接失败 / found=false / 文件过期
                           └─▶ 降级到正常下载流程
```

下载器的降级原则：

1. **缓存命中失败不能阻塞主流程**
2. **gRPC 远程读取失败等价于 cache miss**
3. **重新下载成功后，以当前下载节点为新的 owner 覆盖 Raft 索引**

#### 17.10.4 过期与清理

Response Cache 采用**显式 TTL + 惰性失效 + 后台清理**三层机制：

| 机制 | 行为 |
|------|------|
| 显式 TTL | 每个缓存条目都带 `expires_at`；默认 24h，可由 Request 单独覆盖 |
| 惰性失效 | 读取时若 `expires_at <= now`，直接视为 miss，不返回旧内容 |
| 后台清理 | owner 节点周期性扫描本地磁盘，删除过期文件，并提交 `DeleteResponseCacheIndex` |

即使后台清理尚未执行，只要 `expires_at` 已过，所有节点都会把该条目视为失效，不会继续复用旧数据。

#### 17.10.5 与命名空间联邦的关系

- Response Cache 索引 key 仍按 `config.name` 隔离，推荐格式：`{namespace}:response_cache:{cache_key}`
- 实际 `owner_node_id` 可以是联邦共享 Download Pool 中的任意节点
- `parser()` 不感知缓存来源；无论是 HTTP 下载还是远程缓存读取，最终都产出同样的 `Response`
- 跨命名空间即使 URL 相同，也不默认共享缓存，避免不同任务的 Cookie / Header / 代理语义互相污染

---

---

## 18. Cookie 与 Headers：数据结构与手动管理

### 18.1 设计原则

框架不再自动补全或管理 Cookie 信息。Cookie 的获取、更新和传递由用户在 `ModuleTrait` 中自行实现；框架只负责提供数据结构，并将 `cookies()` / `headers()` 的返回值附加到该模块生成的所有请求上。

| 职责 | 归属 |
|------|------|
| Cookie / Header 数据结构定义 | 框架提供 |
| Cookie / Header 内容的获取和更新 | 用户在 `ModuleTrait` 中实现 |
| 将模块级 Cookie / Header 附加到 Request | 框架在 `Module::generate()` 中自动执行 |
| 单个 Request 的 Cookie / Header 覆盖 | 用户在 `ModuleNodeTrait::generate()` 中按需设置 |

### 18.2 Cookies 数据结构

```rust
pub struct CookieItem {
    pub name:      String,
    pub value:     String,
    pub domain:    String,
    pub path:      String,
    pub expires:   Option<u64>,    // Unix 时间戳（秒）
    pub max_age:   Option<u64>,    // 秒数；0 = 显式删除信号
    pub secure:    bool,
    pub http_only: Option<bool>,
}

pub struct Cookies {
    pub cookies: Vec<CookieItem>,
}

impl Cookies {
    pub fn new() -> Self;
    pub fn add(mut self, name: &str, value: &str, domain: &str) -> Self;
    pub fn cookie_header_for_url(&self, url: &Url) -> Option<String>;
    pub fn str_by_domain(&self, domain: &[impl AsRef<str>]) -> String;
    pub fn merge(&mut self, other: &Cookies);
    pub fn merge_if_absent(&mut self, other: &Cookies);  // 不覆盖已有同名 Cookie
}
```

### 18.3 Headers 数据结构

```rust
pub struct HeaderItem {
    pub key:   String,
    pub value: String,
}

pub struct Headers {
    pub headers: Vec<HeaderItem>,
}

impl Headers {
    pub fn new() -> Self;
    pub fn add(mut self, key: &str, value: &str) -> Self;
    pub fn get(&self, key: impl AsRef<str>) -> Option<String>;
    pub fn merge(&mut self, other: &Headers);
    pub fn merge_if_absent(&mut self, other: &Headers);  // 不覆盖已有同名 Header
}
```

### 18.4 ModuleTrait 中的使用

`ModuleTrait` 提供两个异步方法，返回值会被框架自动附加到该模块生成的所有 Request：

```rust
#[async_trait]
pub trait ModuleTrait: Send + Sync {
    /// 返回该模块所有请求共用的 Cookie。
    /// 默认返回空，由用户按需实现（例如从文件、数据库、外部服务读取登录态）。
    async fn cookies(&self) -> Cookies {
        Cookies::default()
    }

    /// 返回该模块所有请求共用的 Headers（如 User-Agent、Accept-Language 等）。
    async fn headers(&self) -> Headers {
        Headers::default()
    }
    // ...
}
```

#### 使用示例

```rust
struct ShopeeSearchModule {
    // 登录态由用户自行维护，可以是文件路径、数据库连接、内存缓存等
    cookie_store: Arc<CookieStore>,
}

#[async_trait]
impl ModuleTrait for ShopeeSearchModule {
    fn name(&self) -> &'static str { "search" }
    fn version(&self) -> i32 { 1 }
    fn default_arc() -> Arc<dyn ModuleTrait> {
        Arc::new(Self { cookie_store: Arc::new(CookieStore::load()) })
    }

    async fn cookies(&self) -> Cookies {
        // 从用户自管理的 cookie store 读取
        self.cookie_store.get_cookies("shopee.sg")
    }

    async fn headers(&self) -> Headers {
        Headers::new()
            .add("User-Agent", "Mozilla/5.0 ...")
            .add("Accept-Language", "zh-CN,zh;q=0.9")
    }
}
```

### 18.5 Request 级别的 Cookie / Header 覆盖

模块级的 `cookies()` / `headers()` 是所有请求的基础值；在 `ModuleNodeTrait::generate()` 中可以为单个请求追加或覆盖：

```rust
async fn generate(&self, ctx: NodeGenerateContext<'_>)
    -> Result<SyncBoxStream<'static, Request>>
{
    vec![
        // 大多数请求：继承模块级 cookies/headers
        Request::new("https://shopee.sg/search"),

        // 特殊请求：额外追加本次请求独有的 header/cookie
        Request::new("https://shopee.sg/api/submit")
            .with_headers(Headers::new().add("X-CSRF-Token", &csrf_token))
            .with_cookies(Cookies::new().add("cart_session", &cart_id, "shopee.sg")),
    ].into_stream_ok()
}
```

框架在 `Module::generate()` 中的合并策略：

```
最终 Request.cookies = 节点生成的 cookies（高优先级，不被模块级覆盖）
                       ∪ ModuleTrait::cookies()（补充，merge_if_absent）
最终 Request.headers = 节点生成的 headers（高优先级，不被模块级覆盖）
                       ∪ ModuleTrait::headers()（补充，merge_if_absent）
```

### 18.6 登录态完全由 ModuleTrait 实现

当前方案中，框架**不再处理登录态**，也不再提供独立的框架级登录态注入参数。  
所有登录 Cookie、认证 Header、User-Agent 轮换、token 刷新前后的上下文，都由用户在 `ModuleTrait` 内自行维护。

推荐做法：

- 在模块 struct 内持有自定义的登录态来源（本地文件、数据库、外部 HTTP 服务、内存缓存等）
- 在 `cookies()` / `headers()` 中读取并返回当前应附加到请求的值
- 如需刷新登录态，由模块自己的逻辑负责更新其内部状态

### 18.7 enable_session 与手动 Cookie 的协作

`enable_session`（定义在 `ModuleTrait`，见 §20）控制的是**下载器的自动响应头处理**，与 §18.2–18.5 的手动 Cookie 管理是互补的两层机制：

| 机制 | 触发条件 | 作用 |
|------|----------|------|
| `ModuleTrait::cookies()` / `headers()` | 每个 Request 发出前 | 将模块级基础 Cookie/Header 附加到请求（由用户实现，主动控制） |
| `enable_session = true` | 每次 HTTP 响应返回后 | 下载器自动提取响应的 `Set-Cookie` 及指定响应头，注入该模块后续请求（纯内存，被动自动） |

两者不互斥：

- 用户通过 `cookies()` 提供初始登录 Cookie；
- 开启 `enable_session` 后，服务端每次刷新 Cookie（如 CSRF token 轮转、滑动过期的 session）会被自动捕获并延续到下一个请求，无需用户手动处理。

**此机制纯内存运行，仅在当前模块实例的运行期内有效。**  
若用户需要跨节点共享登录态，应由其自行选择外部持久化方案；框架本身不介入。

### 18.8 config.toml Cookie 相关配置移除

框架不再提供任何 Cookie/登录态专用配置节。

如需持久化存储 Cookie 登录态，由用户自行选择存储方案（本地文件、数据库、外部服务等），完全与框架解耦。


## 19. Task Profile：配置路由与 Typed Metadata 的统一数据模型

### 19.1 现状与问题

当前 `(account, platform, module)` 三元组唯一标识一个采集任务，模块与中间件关联存储在 5 张关系表中，由 `TaskFactory` 在运行时加载：

```
TaskEvent { account, platform }
    └─ create_task_with_modules()
            ├─ DB Query 1: account entity
            ├─ DB Query 2: platform entity
            ├─ DB Query 3: rel_account_platform
            ├─ DB Query 4: modules WHERE platform=X AND account=Y
            ├─ DB Query 5: rel_module_data_middleware
            ├─ DB Query 6: rel_module_download_middleware
            └─ DB Query 7: bulk load middleware entities
```

进程内 `DashMap` TTL = 30 秒，无跨节点失效机制。

除此之外，当前元数据模型还有一个更深层的问题：**控制面配置、运行时参数、调试附加信息大量堆在 `serde_json::Value` / `Map<String, Value>` 中**。

| 问题 | 影响 |
|------|------|
| 每次冷加载触发 7 次 DB 查询 | 高并发下 DB 成为瓶颈 |
| 30s 缓存无跨节点失效 | 多节点配置更新不一致，最长延迟 30s |
| 中间件绑定在 module 级 | 无法针对不同 account+platform 组合差异化中间件 |
| `ModuleConfig` 以 JSON blob + 字符串 key merge | 无类型安全，排查最终配置值困难 |
| `ModuleNodeTrait::generate(config, params)` 同时承载控制面配置和数据面输入 | 节点接口语义耦合，难以做版本管理 |
| `MetaData` 将 task/login/module/trait 状态混装在同一 Value 容器 | 审计、灰度、调试、热更新边界不清 |
| 热更新缺少 run 级 `profile_version` 绑定 | 同一 run 中途可能读到不同版本配置 |
| `account.modules` 与关系表冗余 | 两个真相来源，存在冲突风险 |

### 19.2 数据分类

| 对象 | 类型 | Canonical 形态 | 存储 / 传播 |
|------|------|----------------|-------------|
| `TaskProfileSnapshot` | **控制面配置快照** | 强类型 struct + per-node `TypedEnvelope` | Raft 状态机 + RocksDB |
| `ResolvedNodeConfig` | **运行时节点配置切片** | 当前 node 的不可变配置快照 | `TaskFactory` L1/L2 派生缓存 |
| `NodeInput` / `NodeDispatch` | **数据面参数传递** | `TypedEnvelope` | task/request/response/parser 事件体 |
| `RoutingMeta` | **路由元数据** | 强类型字段 | 全管道事件 |
| `ExecutionMeta` | **执行元数据** | 强类型字段 | 全管道事件 |
| Cache KV / Lease / Counter | **运行时状态** | 强一致 KV / 状态机记录 | Raft 状态机 + RocksDB |
| ResponseCacheIndex（`cache_key -> owner_node_id, expires_at`） | **运行时索引 / 派生元数据** | 强类型索引记录 | Raft 状态机 |
| 调试层投影（`debug_layers_json`） | **调试数据** | `Option<serde_json::Value>` | API / 调试输出边界 |
| 任务执行历史 / 错误日志 | **操作数据** | DB 记录 | DB（保持现状） |
| 任务状态（Pending/Running/Done） | **运行时状态** | 状态机记录 | Raft 状态机 |

原则上，**运行时热路径、Raft 命令、队列消息不再直接携带 `serde_json::Value`**；JSON 仅允许出现在 API DTO 和调试投影边界。

### 19.3 目标架构：Raft 状态机 + API 驱动 + Typed Envelope

**所有配置变更通过 API 写入，经 Raft 共识后同步到所有节点。节点读取配置从本地状态机读取，0 网络 RTT。**

```
管理员 / 外部系统
    │  HTTP API（JSON DTO；见 api-architecture.md §3）
    ▼
任意节点接收写请求
    │  DTO 校验 / schema 解析 / 默认值填充
    ▼
TaskProfileSnapshot / TypedEnvelope
    │  若当前节点不是 Leader，自动转发给 Leader
    ▼
Raft Leader：写入日志 → 多数派 ACK → commit
    │
    ▼
所有节点 apply 日志
    ├─ 更新本地 RocksDB（task_profile / typed defaults / schema cache）
    └─ 清除本地 DashMap L1 缓存（对应 profile 键）

TaskFactory 读取路径（热路径，无网络）：
    L1 DashMap 命中 ──▶ 直接返回 `TaskProfileSnapshot`
    未命中 ──▶ 本地 RocksDB 读 ──▶ 写入 DashMap ──▶ 返回
    命中后按 node_key 切片 ──▶ `ResolvedNodeConfig`

ModuleDagProcessor：
    新 run 绑定 `profile_version`
    进行中的 run 始终使用同一版本
```

### 19.4 `TaskProfileSnapshot` 标准模型

`task_profile` 不再以单个 `config: serde_json::Value` 作为运行时真相，而是升级为**已解析、可直接执行的快照对象**：

```rust
pub struct ResolvedCommonConfig {
    pub timeout_secs:            u64,
    pub rate_limit:              Option<f32>,
    pub priority:                PriorityLevel,
    pub proxy_pool:              Option<String>,
    pub downloader:              String,
    pub enable_session:          bool,
    pub serial_execution:        bool,
    pub rate_limit_group:        Option<String>,
    pub response_cache_enabled:  bool,
    pub response_cache_ttl_secs: Option<u64>,
}

pub enum PayloadCodec {
    MsgPack,
    Json,   // 仅 API / debug 边界兼容
}

pub struct TypedEnvelope {
    pub schema_id:      String,
    pub schema_version: u16,
    pub codec:          PayloadCodec,
    pub bytes:          bytes::Bytes,
}

impl TypedEnvelope {
    pub fn encode<T: serde::Serialize>(
        schema_id: impl Into<String>,
        schema_version: u16,
        value: &T,
    ) -> Result<Self>;

    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T>;
}

pub struct MiddlewareBinding {
    pub name:            String,
    pub middleware_type: MiddlewareType,
    pub weight:          i32,
}

pub struct TaskProfileSnapshot {
    pub namespace:           String,
    pub account:             String,
    pub platform:            String,
    pub module:              String,
    pub version:             u64,
    pub enabled:             bool,
    pub common:              ResolvedCommonConfig,
    pub node_configs:        BTreeMap<String, TypedEnvelope>,
    pub download_middleware: Vec<MiddlewareBinding>,
    pub data_middleware:     Vec<MiddlewareBinding>,
    pub middleware_configs:  BTreeMap<String, TypedEnvelope>,
    pub debug_layers_json:   Option<serde_json::Value>, // 仅调试 / explain 使用
    pub updated_at:          i64,
    pub updated_by:          String,
}
```

其中：

- `common` 是服务端在 API 写入时就计算好的**最终通用配置**，不再在热路径做字符串 key merge；
- `node_configs` 按 `node_key` 存储每个 DAG 节点的业务配置，载体是 `TypedEnvelope`；
- `middleware_configs` 用于中间件的参数快照，同样走 typed envelope；
- `debug_layers_json` 仅用于说明“这个最终值来自哪一层”，**不能**参与运行时热路径。

**Raft 状态机键：** `{namespace}:profile:{account}:{platform}:{module}`

如果需要关系型审计投影，可将 `common` 拆成显式列、将 `node_configs / middleware_configs` 以 MsgPack BLOB 落盘；不再以整块 JSONB 作为运行时真相。

### 19.5 配置 Raft Command

```rust
pub enum ControlPlaneRaftCommand {
    // ... §9 中已有命令 ...

    UpsertTaskProfile {
        snapshot: TaskProfileSnapshot,
    },
    DisableTaskProfile {
        identity: TaskProfileIdentity,
    },
    BatchUpsertTaskProfiles {
        snapshots: Vec<TaskProfileSnapshot>,
    },
    UpsertAccountDefault  { default: DefaultConfigUpsert },
    UpsertPlatformDefault { default: DefaultConfigUpsert },
    UpsertModuleDefault   { default: DefaultConfigUpsert },
    UpsertMiddleware { middleware: MiddlewareUpsert },
}
```

Raft 日志中不再直接携带 `serde_json::Value`。API 仍然可以接受 JSON body，但**必须先完成 schema 校验和 typed envelope 编码，再构造 `ControlPlaneRaftCommand`**。

### 19.6 运行时解析：`ResolvedNodeConfig` 替代 `ModuleConfig`

```rust
pub struct ResolvedNodeConfig {
    pub profile_key:     String,
    pub profile_version: u64,
    pub common:          ResolvedCommonConfig,
    pub node_config:     TypedEnvelope,
}

pub struct NodeInput {
    pub source_node: Option<String>,
    pub target_node: String,
    pub payload:     TypedEnvelope,
}

pub struct NodeGenerateContext<'a> {
    pub routing:    &'a RoutingMeta,
    pub exec:       &'a ExecutionMeta,
    pub config:     &'a ResolvedNodeConfig,
    pub input:      &'a NodeInput,
    pub login_info: Option<&'a LoginInfo>,
}

pub struct NodeDispatch {
    pub target_node: String,
    pub input:       NodeInput,
}

pub struct NodeParseOutput {
    pub next:     Vec<NodeDispatch>,
    pub data:     Vec<ParsedData>,
    pub finished: bool,
}
```

运行时约束：

- `config` 是控制面快照，来自 API 热更新后的 `TaskProfileSnapshot`，但在 `generate()` 里只暴露当前 node 的 `ResolvedNodeConfig`；
- `params` 被替换为 `NodeInput.payload`，它来自上一个节点/任务的输出；
- `generate()` 和 `parser()` 不再直接接收整个 `ModuleConfig` 或 `Map<String, Value>`；
- 若按本轮开发路线实施，可以直接以 `NodeParseOutput / NodeDispatch` 替换 parser 输出；不再把 `TaskOutputEvent` 作为必须保留的兼容壳。

### 19.7 热更新能力

| 热更新场景 | 实现方式 | 生效延迟 |
|-----------|---------|---------|
| 修改 account/module/platform 默认配置 | API 解析为 typed envelope → 重算受影响 `TaskProfileSnapshot` → `UpsertTaskProfile` | < 50ms（Raft 写入） |
| 修改某 node 的业务配置 | API 更新对应 `node_configs[node_key]` 的 envelope，生成新 `version` | < 50ms |
| 修改中间件参数 | API → `UpsertMiddleware` + 重算关联 `TaskProfileSnapshot` | < 50ms |
| 为某组合新增 module | 生成新 `TaskProfileSnapshot` 并写入 Raft | < 50ms |
| 禁用某 account 的所有采集 | 批量 `DisableTaskProfile` | < 50ms |
| 节点重启 | 从本地 RocksDB 恢复，无需联系 Leader | 启动即恢复 |
| 强制切换进行中的 run | 显式 drain / cutover，禁止隐式中途切换 | 运维控制 |

所有节点通过 Raft 日志复制保证**强一致**：每个节点 apply 新版本时清除 DashMap 对应条目；`TaskFactory` 下次读取时从本地 RocksDB 加载新快照。  
同时，`ExecutionMeta.profile_version` 会在 run 启动时固定下来：

- **新 run**：读取最新 committed `profile_version`
- **进行中的 run**：始终使用启动时绑定的版本
- **如需中途切换**：必须通过显式 cutover / drain，而不是让同一 run 隐式穿透到新配置

这里要明确一条边界：  
本节讨论的“热更新能力”是 **TaskProfile / middleware / node business config** 级别的热更新，不等同于“任意基础设施参数都可以在线切换”。即使在目标态，也应把下列配置默认视为 **restart / rolling-rebind scope**：

- queue backend 类型切换（in-memory / Kafka）
- queue codec 切换（JSON / MsgPack）
- cache backend / cookie backend 拓扑切换
- API listener 端口变更

原因很直接：当前代码基线里，这些对象都不是通过 watcher 在线重建的；如果未来要支持无损切换，也必须额外引入显式 rebind 协议，而不是把它们混进 `TaskProfileSnapshot` 的“业务配置热更新”语义里。

### 19.8 演进路线（直切）

本轮若按开发文档执行，可以不把“保留历史 API / 历史消息格式 / 运行时兼容层”作为前提。  
推荐按以下步骤直接切到目标模块边界：

| 阶段 | 当前状态 | 操作 | 切换边界 |
|------|----------|------|----------|
| Step 1 | ✅ 已完成 | 冻结目标模块：`control_plane::profile`、`workflow::definition`、`runtime::context`、`scheduler::dag`、`transport::queue`、`pipeline::processors`、`cluster::control`、`observability`、`bootstrap` | 文档先行，无运行时切换 |
| Step 2 | ✅ 已完成 | 建立 `TaskProfileSnapshot / WorkflowDefinition / RoutingMeta / ExecutionMeta / NodeInput / NodeParseOutput / QueueEnvelope` 等 canonical model | 模型完成后不再继续扩展旧 `ModuleConfig / MetaData` |
| Step 3 | ✅ 已完成 | 直接替换 `ModuleNodeTrait`、`TaskFactory`、workflow compile path，让节点只接受 typed context | 节点接口与工厂切换在同一阶段完成 |
| Step 4 | ✅ 已完成（核心调度层） | 建立 `DagScheduler / DagRunStateStore / DagRunGuard / DagFencingStore / NodePlacement`，并把 run state 从 processor 中剥离 | scheduler 就位后再切 processor 主链路 |
| Step 5 | ✅ 已完成 | 重做 queue schema、DLQ、blob、topic，统一到目标 envelope | codec/topic/batch/DLQ 已完成；`task/request/response/parser/error` topic 均已切到 typed envelope |
| Step 6 | ✅ 已完成 | 切换 `/config/*`、`/tasks/dispatch`、`/cluster/*`、`/debug/*`、`/control/*` 等目标 API，并同步更新脚本与控制面入口 | 目标 API 路由已在主 `router()` 生效，旧 route 不再是主路径 |
| Step 7 | ✅ 已完成 | 用目标模块重组 `State/Engine`，删除 legacy 路由、旧类型和 Redis 时代主路径依赖 | 装配单入口已就位；Redis/Lua/rollback/legacy runtime builder 已清理 |

> 当前代码已经完成执行流水线的 transport 层切口：`task/request/response/parser/error` 五段 topic 都已切到 typed envelope，parser/error ingress chain、`TaskManager` / `TaskFactory`、`TaskModelProcessor` 也已直接接收 envelope。生产代码中不再保留 `build_legacy_*` runtime builder 或 `"legacy.*"` schema ID。

## 20. ModuleTrait / ModuleNodeTrait 配置与上下文重构

### 20.1 当前问题

当前配置和参数在实现上混杂了**控制面默认值**、**节点业务配置**、**单次请求覆盖**、**上游节点输入**四个层次：

```
config.toml [download]                 ← 基础设施全局默认
    ↑ ModuleTrait 方法                 ← 模块代码默认值
        ↑ TaskProfileSnapshot.common   ← API / Raft 热更新后的最终通用配置
            ↑ Request 字段             ← 单次请求覆盖

node_config (per node)                 ← 节点业务配置
params / 上游输出                      ← 下一个节点的输入
```

问题：

- `ModuleConfig` 和 `params` 都通过字符串 key / `Value` 访问，编译期无法校验；
- 节点拿到的是整个 `ModuleConfig`，而不是“当前 node 真正需要的配置切片”；
- `config`（控制面）和 `params`（数据面）被塞进同一个接口，语义耦合；
- `MetaData`、`login_info`、`trait_meta` 等附加上下文界限不清；
- 用户无法在 IDE 中直接看到“这个 node 能拿到什么配置 / 这个 payload 的 schema 是什么”。

### 20.2 设计目标

1. **控制面配置与数据面输入彻底分层**：`config` 是 `ResolvedNodeConfig`，`params` 是 `NodeInput.payload`
2. **框架通用配置保持强类型、可发现**：超时、限速、代理、优先级等由 `ModuleTrait` 和 `ResolvedCommonConfig` 描述
3. **节点业务配置与上游输入统一走 `TypedEnvelope`**：支持动态模块，但不再依赖 `Map<String, Value>`
4. **热更新不穿透进行中的 run**：`profile_version` 在 run 级固定
5. **Request 仍保留最高优先级的单次覆盖能力**，但只覆盖基础设施字段，不覆盖 node 业务配置

### 20.3 重构后的 ModuleNodeTrait

```rust
pub struct NodeGenerateContext<'a> {
    pub routing:    &'a RoutingMeta,
    pub exec:       &'a ExecutionMeta,
    pub config:     &'a ResolvedNodeConfig,
    pub input:      &'a NodeInput,
    pub login_info: Option<&'a LoginInfo>,
}

pub struct NodeParseContext<'a> {
    pub routing:    &'a RoutingMeta,
    pub exec:       &'a ExecutionMeta,
    pub config:     &'a ResolvedNodeConfig,
    pub login_info: Option<&'a LoginInfo>,
}

#[async_trait]
pub trait ModuleNodeTrait: Send + Sync {
    async fn generate(
        &self,
        ctx: NodeGenerateContext<'_>,
    ) -> Result<SyncBoxStream<'static, Request>>;

    async fn parser(
        &self,
        response: Response,
        ctx: NodeParseContext<'_>,
    ) -> Result<NodeParseOutput>;
}
```

当前代码中的 `Map<String, Value>` 型 `params`、`MetaData` 仍可作为兼容适配层；但对外暴露的设计目标是**节点只面向强类型上下文和 typed envelope**。

### 20.4 重构后的 ModuleTrait

```rust
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum PriorityLevel {
    #[serde(rename = "height")]
    Height,
    #[default]
    #[serde(rename = "middle")]
    Middle,
    #[serde(rename = "low")]
    Low,
}

impl PriorityLevel {
    pub fn to_priority(self) -> Priority {
        match self {
            PriorityLevel::Height => Priority::High,
            PriorityLevel::Middle => Priority::Normal,
            PriorityLevel::Low => Priority::Low,
        }
    }
}

#[async_trait]
pub trait ModuleTrait: Send + Sync {
    fn name(&self) -> &'static str;
    fn version(&self) -> i32;

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized;

    // 通用配置默认值：用于生成 TaskProfileSnapshot.common
    fn rate_limit(&self) -> Option<f32> { None }
    fn proxy_pool(&self) -> Option<&str> { None }
    fn timeout_secs(&self) -> Option<u64> { None }
    fn priority_level(&self) -> PriorityLevel { PriorityLevel::Middle }
    fn serial_execution(&self) -> bool { false }
    fn enable_session(&self) -> bool { false }
    fn downloader(&self) -> &str { "request_downloader" }
    fn rate_limit_group(&self) -> Option<&str> { None }
    fn should_login(&self) -> bool { true }

    async fn cookies(&self) -> Cookies { Cookies::default() }
    async fn headers(&self) -> Headers { Headers::default() }

    async fn dag_definition(&self) -> Option<ModuleDagDefinition> { None }
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> { vec![] }

    async fn pre_process(&self, _profile: Option<Arc<TaskProfileSnapshot>>) -> Result<()> { Ok(()) }
    async fn post_process(&self, _profile: Option<Arc<TaskProfileSnapshot>>) -> Result<()> { Ok(()) }

    fn cron(&self) -> Option<CronConfig> { None }
}
```

`ModuleTrait` 在这里表达的是**模块代码默认值**和模块生命周期；真正传给 node 的运行时配置由 `ResolvedNodeConfig` 承载，避免 node 再去做全局 merge。

### 20.5 用户侧使用方式

#### 最简模块（无任何额外默认值）

```rust
struct SearchModule;

impl ModuleTrait for SearchModule {
    fn name(&self) -> &'static str { "search" }
    fn version(&self) -> i32 { 1 }
    fn default_arc() -> Arc<dyn ModuleTrait> { Arc::new(Self) }
}
```

#### 节点内读取 typed config / typed params

```rust
#[derive(Serialize, Deserialize)]
struct SearchListConfig {
    page_size: u32,
    max_pages: u32,
}

#[derive(Serialize, Deserialize)]
struct SearchSeed {
    keyword: String,
    page:    u32,
}

#[derive(Serialize)]
struct SearchCacheKey<'a> {
    keyword: &'a str,
    page:    u32,
}

async fn generate(&self, ctx: NodeGenerateContext<'_>)
    -> Result<SyncBoxStream<'static, Request>>
{
    let cfg: SearchListConfig = ctx.config.node_config.decode()?;
    let input: SearchSeed = ctx.input.payload.decode()?;

    vec![
        Request::new(format!(
            "https://example.com/search?q={}&page={}&size={}",
            input.keyword, input.page, cfg.page_size
        ))
        .enable_response_cache_with(
            &SearchCacheKey {
                keyword: &input.keyword,
                page: input.page,
            },
            ctx.config.common.response_cache_ttl_secs,
        ),
    ].into_stream_ok()
}
```

上面这段代码里：

- `ctx.config.node_config` 是**当前节点的业务配置**；
- `ctx.input.payload` 是**上游节点传来的数据面输入**；
- 两者都通过 `decode::<T>()` 得到强类型 struct，不再通过字符串 key 去读 `serde_json::Value`。

#### parser 向下游节点派发 typed input

```rust
async fn parser(
    &self,
    response: Response,
    ctx: NodeParseContext<'_>,
) -> Result<NodeParseOutput> {
    let ids = extract_ids(response)?;

    let next = ids
        .into_iter()
        .map(|id| {
            Ok(NodeDispatch {
                target_node: "detail".to_string(),
                input: NodeInput {
                    source_node: Some(ctx.routing.node_key.clone()),
                    target_node: "detail".to_string(),
                    payload: TypedEnvelope::encode(
                        "search.detail.input",
                        1,
                        &DetailInput { id },
                    )?,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(NodeParseOutput {
        next,
        data: vec![],
        finished: true,
    })
}
```

### 20.6 Request 级别的覆盖

`Request` 仍保留 builder API，作为**最高优先级的单次请求覆盖**，但它只覆盖基础设施字段，不覆盖 `node_config` 和 `input.payload`：

```rust
impl Request {
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn with_proxy(mut self, proxy: ProxyEnum) -> Self {
        self.proxy = Some(proxy);
        self
    }

    pub fn with_rate_limit_group(mut self, group: impl Into<String>) -> Self {
        self.limit_id = group.into();
        self
    }

    pub fn with_timeout(mut self, secs: u64) -> Self {
        self.timeout = Some(secs);
        self
    }

    pub fn enable_response_cache(mut self, ttl_secs: Option<u64>) -> Self {
        self.enable_response_cache = true;
        self.response_cache_ttl_secs = ttl_secs;
        self
    }

    pub fn enable_response_cache_with<T>(mut self, hash_able: &T, ttl_secs: Option<u64>) -> Self
    where
        T: Serialize,
    {
        if let Ok(cache_key) = serde_json::to_string(hash_able) {
            self.enable_response_cache = true;
            self.response_cache_ttl_secs = ttl_secs;
            self.hash_str = Some(md5(cache_key.as_bytes()).to_string());
        }
        self
    }
}
```

在 `ModuleNodeTrait::generate()` 中按需覆盖：

```rust
#[derive(Serialize)]
struct HeavyPageCacheKey<'a> {
    page: &'a str,
    lang: &'a str,
}

async fn generate(&self, ctx: NodeGenerateContext<'_>)
    -> Result<SyncBoxStream<'static, Request>>
{
    vec![
        Request::new("https://example.com/list").into_stream_ok(),
        Request::new("https://example.com/heavy-page")
            .with_priority(Priority::High)
            .with_timeout(60)
            .with_proxy(ProxyEnum::Tunnel(premium_tunnel))
            .enable_response_cache_with(
                &HeavyPageCacheKey { page: "heavy-page", lang: "zh-CN" },
                Some(3600),
            )
            .with_rate_limit_group("heavy_requests"),
    ].to_stream().into_stream_ok()
}
```

### 20.7 配置分层覆盖规则

配置解析分成两个阶段：

#### 阶段 A：控制面生成快照（高 → 低）

```
① task_profile / API 覆盖
② ModuleTrait 方法（代码默认值）
③ config.toml [download]（基础设施兜底）

结果：TaskProfileSnapshot.common + node_configs
```

也就是说，**通用配置的 merge 在写入/重算 `TaskProfileSnapshot` 时完成**，而不是在 `generate()` 热路径里继续用字符串 key 查值。

#### 阶段 B：运行时单次请求覆盖（高 → 低）

```
① Request 字段（单次请求覆盖）
② ResolvedNodeConfig.common（控制面已解析结果）
```

其中：

- `enable_session / serial_execution` 是 module/profile 级控制，不在单个 `Request` 上覆盖；
- `ctx.config.node_config` 和 `ctx.input.payload` **不参与优先级链**，它们是显式 decode 的 typed payload，不再做字符串 key 级 merge。

运行时包装请求时，只需要处理 `Request` 对 `ResolvedNodeConfig.common` 的覆盖：

```rust
let timeout = request
    .timeout
    .unwrap_or(ctx.config.common.timeout_secs);

let priority = request
    .priority
    .unwrap_or_else(|| ctx.config.common.priority.to_priority());

let proxy = request
    .proxy
    .clone()
    .or_else(|| ctx.config.common.proxy_pool.as_ref().and_then(|name| proxy_manager.get_pool(name)));
```

**热更新场景举例：**

```
场景：模块代码默认 `rate_limit() = Some(10.0)`，运营通过 API 临时降速到 2.0 rps

PATCH /config/profiles/account_a/shopee/search
{ "common": { "rate_limit": 2.0 } }

→ API 解析并重算 `TaskProfileSnapshot.common`
→ Raft 同步到所有节点（< 50ms）
→ 所有节点 apply 后清除该 profile 的 DashMap 缓存
→ 新启动的 run 绑定新的 `profile_version`
→ 进行中的 run 继续按旧版本 10.0 执行，直到自然结束或显式 cutover
```

### 20.8 config.toml 的职责收缩

重构后 `config.toml` 只保留**基础设施级**全局默认，不再承载模块行为配置：

```toml
[download]
# 控制面在生成 TaskProfileSnapshot.common 时的兜底值
# serial_execution / node business config / DAG params 不在此处提供
timeout           = 30         # 秒
rate_limit        = 0.0        # 0 = 不限速（全局不限速，各模块按需开启）
downloader        = "request_downloader"
pool_size         = 200
max_response_size = 10485760   # 10MB
```

移除或禁止承载的内容：

- `enable_rate_limit`（由 `rate_limit: Option<f32>` 统一语义）
- `enable_locker`（提升为 `serial_execution`，进入 profile / trait）
- 任何 node 业务配置
- 任何 DAG `params` 或上游 payload
- 以 JSON blob 形式堆叠的 module 运行时配置

### 20.9 接口替换路线

| 阶段 | 操作 |
|------|------|
| Step 1 | 直接定义 `TaskProfileSnapshot / ResolvedNodeConfig / NodeGenerateContext / NodeParseContext / NodeParseOutput` 作为唯一目标接口 |
| Step 2 | `TaskFactory` 只产出 profile、workflow 和 node config，不再让 runtime 主路径依赖 `ModuleConfig` |
| Step 3 | `ModuleNodeTrait::generate/parser` 直接改为 typed context 签名，节点实现与调用方同时替换 |
| Step 4 | parser 输出直接改成 `NodeParseOutput / NodeDispatchEnvelope`，不再保留 `TaskOutputEvent` 主路径 |
| Step 5 | 删除 `Map<String, Value>` 型 `params`、大块 `MetaData` 字段和热路径中的 JSON-heavy config 读取 |

---

## 21. 当前代码重构进度与遗留检查结论

### 21.1 当前验证基线（2026-05-20）

本节记录当前代码实际验证结果。旧版 T21-N01 至 T21-N06 任务卡已全部完成并移除，不再作为待办或兼容清单保留。

已验证通过：

```bash
cargo check
cargo check --tests
cargo check --manifest-path tests\Cargo.toml --bins
powershell -ExecutionPolicy Bypass -File scripts\check_typed_hot_path.ps1
```

验证结论：

- 主 crate、测试目标和 `tests` 独立 crate bin 均可编译。
- `check_typed_hot_path.ps1` 通过；生产代码中没有 `"legacy.*"` schema ID，也没有 `build_legacy_*` runtime builder 调用。
- 非本地临时/归档路径中没有 Redis 后端、Redis 配置、Redis 环境变量、`redis_lock`、rollback/cutover rehearsal 脚本引用。

### 21.2 已完成的重构面

1. Redis 生产支持已移除：`redis` / `deadpool-redis` 依赖、Redis cache backend、Redis queue、Redis event handler、Redis sync、Lua 脚本路径和 Redis DAG remote 均已删除。
2. `Config` 已移除 Redis/cookie/compensator/rollback 兼容字段；`[cache].backend` 只保留 `local` 与 `raft_rocksdb`。
3. `State` 在 `raft_rocksdb` 模式下注入 `RaftLockBackend` 与 `RaftRateLimitBackend`，cache/lock/rate-limit/status 主状态写入 `ProfileControlPlaneStore`。
4. `QueueManager` 默认使用 `QueueNativeCompensator`，不再保留 Redis compensator 路径。
5. `redis_lock.rs` 已重命名为 `distributed_lock.rs`，调用点均迁移到 `crate::utils::distributed_lock`。
6. 旧 `check_legacy_hot_path` 脚本已重命名为 `check_typed_hot_path`，CI evidence gate 不再运行 Redis 可选测试。
7. cutover rollback rehearsal 脚本和 2026-04-17 rollback 归档证据已删除。

### 21.3 当前仍需注意的非代码残留

- `tests/tmp/engine_e2e_*.toml` 是未被 git 跟踪的本地生成临时配置，其中仍可能包含旧 Redis / rollback 字段；它们不参与编译和仓库状态，可按需清空本地 tmp 目录。
- 本文档早期章节仍包含部分架构演进背景中的“Redis / legacy / rollback”历史词汇；这些段落只作为迁移背景，不应作为当前代码能力或兼容承诺解读。
- 若后续新增任务卡，应只记录真实未完成代码工作；已完成项不要再保留为 TODO。

### 21.4 后续检查命令

每次清理历史遗留后，至少执行：

```bash
cargo check
cargo check --tests
cargo check --manifest-path tests\Cargo.toml --bins
powershell -ExecutionPolicy Bypass -File scripts\check_typed_hot_path.ps1
rg -n "deadpool_redis|deadpool-redis|\bredis\b|\bRedis\b|RedisConfig|allow_rollback|rollback|cutover_rehearsal|check_legacy_hot_path|redis_lock|build_legacy_|\"legacy\."" src Cargo.toml tests scripts docs --glob "!tests/tmp/**"
```

Go / No-Go 口径：上述命令通过，且新增代码不重新引入 Redis、rollback、legacy runtime builder 或旧 schema ID。

*文档最后更新：2026-05-20。当前结论：重构主路径已完成；本轮清理后未发现仍参与编译或 CI 的历史兼容代码。*
