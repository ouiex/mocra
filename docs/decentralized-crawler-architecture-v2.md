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
allow_rollback   = true    # 是否允许版本回滚
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

1. `Config::is_single_node_deployment()` 以 `cache.redis` 是否配置为派生判断：未配置时 deployment label 为单节点；已配置时为当前分布式基线。`is_single_node_mode()` 仍作为兼容别名保留。
2. `[raft]` 表示目标态控制面的配置入口，不应再额外引入 `runtime_mode` 之类手动切模式字段。
3. 当前 `Config` 仍保留多组 Redis 兼容字段（如 `cache.redis`、`channel_config.redis`、`channel_config.compensator`、`cookie`）；v2 目标态是逐步把这些能力收敛到 `Raft + RocksDB`，而不是要求一次性删空兼容字段。

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

`State`（`src/common/state.rs`）仍是整个运行时的组合根；即使未来把控制面后端从 Redis 迁到 `Raft + RocksDB`，这一职责边界也不变。

`State::try_new_with_provider()` 的当前装配顺序大致如下：

1. 读取配置，并通过 `Config::is_single_node_deployment()` 派生当前 deployment label。
2. 初始化数据库连接（当前代码使用 **SeaORM** `DatabaseConnection`，而不是单独的 SQLx pool 抽象）。
3. 按各自配置独立创建 `cache_pool` / `cookie_pool`，并仅对 `cache_pool` 做 `PING` 探活；single-node deployment label 仍保留为部署标识，但不再作为 `cookie` 后端的总开关。
4. 复用 `cache_pool` 构造 `DistributedLockManager`、下载限流器以及可选 API 限流器。
5. 构造 `CacheService` / `cookie_service`：
   - 当前后端是 `LocalBackend`、`RedisBackend` 或 `TwoLevelCacheBackend(Redis + L1)`。
   - v2 目标态是把这个后端替换为 `RaftCacheBackend + RocksDB`，而不改变上层调用方式。
    - `Engine` 的 Lua preload 与 PTM claim/commit 热路径现在直接根据 `State` 已装配的 cache Redis backend 判定，而不再重复读取 `Config::is_single_node_deployment()`。
    - task ingress 的 threshold/terminate Lua 决策也同样显式绑定到 `State` 的 cache Redis capability；仅仅“持有一个 `LuaScriptRegistry`”不再意味着允许走 Lua 原子路径。
6. 基于 `CacheService + DistributedLockManager` 初始化 `StatusTracker`。
7. 启动配置 watcher；当配置热更新时，动态刷新内存中的 `Config` 和限流阈值。

需要特别注意：当前 `State` 的 watcher 还不是“全量运行时重绑定”。  
它目前只会：

- 更新 `Arc<RwLock<Config>>` 中的配置快照；
- 调用下载限流器和 API 限流器的 `set_all_limit(...)` 推送新的阈值。

它**不会**在运行中重建：

- `QueueManager`
- `CacheService` / `cookie_service`
- `DistributedLockManager`
- Redis pool / MQ backend
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
- `LuaScriptRegistry`：当前分布式基线下的原子脚本注册表
- `shutdown_tx` / `pause_tx` / `prometheus_handle` / `inflight_counter`：运行时控制与观测组件

其中 `pause_tx` 现在由控制面 `ProfileControlPlaneStore` 的本地 `watch::channel<bool>` 驱动：  
`/control/pause`、`/control/resume` 或 Raft 状态机 apply 一旦更新 pause 状态，就会直接把变更推送到 `Engine`，因此当前 pause/resume 已经不是 5 秒轮询，而是**事件驱动的本地传播**。

#### 3.2.1 `Engine::start()` 的实际启动顺序

当前代码中的启动不是单个 `start_work()` 调用，而是 `State::new()` → `Engine::new()` → `engine.start()` 的多阶段流程：

1. 若当前处于分布式基线模式，则预加载 Lua 脚本。
2. 若配置了 `[api]`，启动 axum API。
3. 若启用 `EventBus`，注册 console / MQ / Redis 等 handler 并启动事件总线。
4. 启动下载器后台清理器、Ctrl+C 监听、zombie cleaner、system monitor、idle-stop watcher。
5. 启动 `CronScheduler`。
6. 构造 `UnifiedTaskIngressChain`、download chain、parser chain 等责任链。
7. 并发拉起 `TaskProcessor / DownloadProcessor / ParserProcessor / ErrorProcessor / HealthMonitor`。

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

当前 consumer group 的具体形态依 backend 而不同：

- **Kafka**：`{namespace}-crawler_group`
- **Redis Stream**：`{namespace}:crawler_group`

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

#### 4.3.7 当前代码中的近似实现

当前代码尚未落地 `comp_begin:* / comp_done` 事件流，但已经有一版**补偿雏形**：

- `QueueManager` 在订阅侧反序列化成功后，可调用 `Compensator::add_task(topic, id, payload)` 记录“进入处理”。
- 各 processor 在成功完成后，会调用 `comp.remove_task(topic, id)` 删除补偿记录。
- 当前默认实现仍是 `RedisCompensator`，因此它更接近“Redis 时代的补偿基线”，尚未演进成文档中的 queue-native replayer。

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
   - 若达到 circuit breaker / rollback 阈值，则触发 `DagCutoverStateTracker` 或 run 级失败终止

4. **节点恢复**
   - 调度器从 `DagRunStateStore` 读取 `DagRunResumeState`
   - 已成功节点直接视为完成，仅重建剩余前驱计数和 ready queue
   - fencing token 必须保证“旧 worker 晚到的结果不能提交覆盖新 worker”

这也是 `src/schedule/dag/scheduler.rs` 中 `DagSchedulerOptions.max_in_flight`、`cancel_inflight_on_failure`、`DagRunStateStore`、`DagFencingStore` 存在的根本原因：  
**DAG 调度器不仅要决定“接下来跑谁”，还要保证“恢复后仍然只跑一次、只推进一次、只提交最新一次”。**

### 5.3 DAG 蓝绿切换

`DagCutoverStateTracker`（当前嵌入 `src/engine/task/task_manager.rs`）管理 DAG 版本热切换：

- **warmup 计数器**：新版本 DAG 需执行 N 次成功才完成预热
- **failure_streak**：连续失败次数超阈值时自动回滚到旧版本
- **原子状态机**：`Warming → Active → (Rollback)` 状态迁移

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
    warmup_target:   u32,           // 预热目标次数
    warmup_count:    AtomicU32,     // 当前预热计数
    failure_streak:  AtomicU32,     // 连续失败次数
    rollback_thresh: u32,           // 触发回滚的失败阈值
    state:           AtomicU8,      // 0=Warming, 1=Active, 2=RolledBack
}
```

状态迁移：
- `Warming` + 成功执行 → `warmup_count++`；达到 `warmup_target` → `Active`
- `Active` + 连续失败 → `failure_streak++`；达到 `rollback_thresh` → `RolledBack`
- `RolledBack` → 触发 `EventBus::emit(DagRollback)` 通知 `Engine` 切换回旧 DAG

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

因此当前 `EventBus` 更接近“轻量异步事件分发器”，而不是依赖 `broadcast` 的全局总线；Redis/MQ 等外部输出能力是挂在 handler 层，而不是 EventBus 本体。

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

> 以下状态表示**代码已实际落地**的部分，不等同于目标态已经完成最终 cutover。

| 重构包 | 状态 | 已落地范围 | 当前说明 |
|------|------|----------|----------|
| Pack 1：Canonical Model（A01-A04） | ✅ 已完成 | `TaskProfileSnapshot`、`WorkflowDefinition`、`RoutingMeta / ExecutionMeta / NodeInput / NodeParseOutput`、`QueueEnvelope / TaskDispatchEnvelope / NodeDispatchEnvelope / NodeErrorEnvelope / DeadLetterEnvelope`、控制面写入模型 | canonical model 已建立；新设计不再继续向主路径扩展旧 `ModuleConfig / MetaData` |
| Pack 2：Module Runtime（B01-B04） | ✅ 已完成 | `ModuleTrait` 静态声明、`ModuleNodeTrait` typed context、legacy adapter、`WorkflowCompiler`、`TaskFactory` profile/workflow 加载 | 节点运行时契约和工厂装配已切到 typed profile/runtime contract |
| Pack 3：Scheduler Core（C01-C05） | ✅ 已完成 | `DagRunState`、run guard / fencing、scheduler persistence / recovery、placement contract、stop signal convergence | DAG 调度核心已经具备恢复、放置、停止收敛与持久化快照语义 |
| Pack 4：Transport Layer（D01-D03） | ✅ 已完成 | queue codec boundary、topic / batch / backpressure contract、structured DLQ | `QueueManager` 内散落的 codec/topic 策略已收口，DLQ 读取已改成结构化 envelope 视图 |
| Pack 5：Execution Pipeline（E01-E04） | 🚧 进行中 | **E01 / E02 已完成**：`task/request/response` topic 已切到 typed dispatch；**E03 已继续内推**：`parser_task/error_task` topic、worker 入口、parser/error ingress chain、`TaskManager`、`TaskFactory`、`TaskModelProcessor` 主执行路径均已切到 envelope-first，且新消息主路径已优先使用 envelope 自带 typed parser/error context；**E04 已进一步收口**：`ResponseParserProcessor` 的隐式 `local_generate` 分支已移除，`ModuleDagProcessor` 会把目标节点的 placement/policy 显式保留为 runtime routing hint，并透传进 parser/error envelope typed context；`DagScheduler` 已支持按 `node_id` 直接应用 runtime override，`NodeExecutionContext` / remote wire protocol 已支持 opaque `runtime_input`，`ModuleNodeDagAdapter` 在拿到该输入时已可执行真实 `generate()` 并返回 request batch payload；`ModuleDagProcessor` 已补齐 generate/parser 双侧单节点 DAG 编译能力（含 parser stale node_id→step_idx 回退）；`Module::generate()` 对本地 placement 的当前 target node 已经通过 `ModuleDagOrchestrator::execute_dag_with_generate_runtime_input(...)` 走 scheduler bridge 执行真实 generate，`Module::parser()` 也已接入 `execute_dag_with_parser_runtime_input_and_dispatcher(...)` 并复用处理器路由语义；parser scheduler bridge 对本地 placement 的执行失败现已 fail-closed，不再回退到本地 parse 路径；scheduler ingress 在 bridge 已接管后若预编译 DAG 缺失、重建 task 与目标模块/节点不一致、scheduler bridge 自身未挂载，或 parser/error transport 已缺失 `module/node` 目标字段，都会直接 retryable fail-closed；同时 parser 本地快路径已补齐 remote placement fail-closed 保护，避免 remote parser 节点误落本地执行 | request/response replay 本地证据已闭环；parser/error ingress 的执行期 fallback 已进一步收敛到 gray/failure gate 未放行这类显式 rollout 边界；`Module::generate()` 的 remote dispatcher 仍需进一步与 scheduler cutover 的最终策略统一；整张 DAG 的 parser/error scheduler cutover 仍在推进中 |
| Pack 6：Control Plane API（F01-F04） | ✅ 已完成 | `/config/*`、`/tasks/dispatch`、`/cluster/*`、`/debug/*`、`/control/*` | `ApiState` + `ProfileControlPlaneStore` 已接入主链路，follower 写可自动转发 leader，接口已覆盖配置、状态、调试与控制面常用路径 |
| Pack 7：Observability（G01-G03） | ✅ 已完成 | metrics facade、统一标签、dashboard/alert 基线 | `control_plane` 与 `scheduler_ingress` 主路径已统一接入吞吐/时延/错误指标，告警规则已落地；`scheduler_ingress` 的 gray/failure gate fallback 现单独保留 cutover 指标口径，不再重复计入 stage error |
| Pack 8：Bootstrap / Legacy Removal（H01-H03） | 🚧 进行中 | `State/Engine` 装配边界收口、legacy 兼容层分批清理、cutover rehearsal 脚本 | 已完成装配单入口与 rehearsal 脚本；P8-A 已完成，P8-C/P8-D 已完成，P8-E 已推进到 batch-66，P8-B 已推进到 batch-37，当前剩余主线已收敛到 parser/error 整张 DAG cutover 与目标环境证据闭环 |

**最近一次定向代码验证状态：**

- `cargo test --lib -- --nocapture` 已通过（2026-04-24；`485 passed, 0 failed, 1 ignored`；过程中仅保留既有 warning，未出现新的失败）
- `cargo test module_dag_processor --lib -- --nocapture` 已通过（2026-04-21）
- `cargo test module_processor_with_chain --lib -- --nocapture` 已通过（2026-04-21）
- `cargo test node_context_adapter --lib -- --nocapture` 已通过（2026-04-21）
- `cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\tests\Cargo.toml --bin tests_debug -- --nocapture` 已通过（2026-04-21；本地无 Redis 时相关 case 按预期输出 skip）
- `cargo test engine::events --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test message --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test parser_error_adapter --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test scheduler_ingress --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test module --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test message --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test request_response_adapter --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test queue --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test retryable_failure_policy_ --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test dlq_message --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test dlq_ --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test queue_manager_stored_ --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test parser_fails_closed_when_scheduler_bridge_fails_for_local_node --lib -- --nocapture` 已通过（2026-04-22）
- `cargo test scheduler_ingress --lib -- --nocapture` 已通过（2026-04-23）

**当前下一步：**

- 继续推进 **Pack 8**：按冻结清单完成 P8-B/P8-E 的剩余兼容层收敛，直到主路径不再依赖 legacy schema / shared runtime bridge
- 完成 E04 最后一段“整张 DAG parser/error scheduler cutover”，把 parser/error 主路径从 compatibility carrier 切到 typed dispatch / typed error envelope 直连
- 在目标环境执行 cutover rehearsal、alert gate、probe whitelist 与回滚演练，补齐首份生产验收证据

### 7.2 已实现（稳定）

| 组件 | 状态 | 说明 |
|------|------|------|
| `State` | ✅ 稳定 | 组合根，资源初始化完整 |
| `Engine` | ✅ 稳定 | 主协调器，字段完整 |
| `QueueManager` | ✅ 稳定 | 远程消息队列抽象 + 优先级 topic |
| `ProcessorRunner` | ✅ 稳定 | 有界并发、暂停/关闭完整 |
| `TaskManager` | ✅ 稳定 | 任务 CRUD + 状态管理 |
| `CacheService` | ✅ 稳定基线 | 当前提供通用 KV / TTL / ZSET / script / L1，后端仍以 Local/Redis 为主 |
| `CronScheduler` | ✅ 稳定 | cron 触发 + leader guard；当前已切到 `LeadershipGate` 抽象，单节点 Raft 配置可直接复用 Raft Leader |
| `LeaderElector` | ✅ 稳定基线 | 当前是 Redis lock 软 leader，目标态由 Raft Leader 替代 |
| `NodeRegistry` | ✅ 稳定基线 | 当前为 `CacheService + ZSET` 节点心跳索引，目标态迁移到 Raft 状态机 |
| `StatusTracker` | ✅ 稳定基线 | 当前为 `CacheService + lock` 的错误/状态跟踪器，目标态迁移到统一状态机 |
| `EventBus` | ✅ 稳定 | broadcast，进程内 |
| `DagCutoverStateTracker` | ✅ 稳定 | 当前嵌入 `TaskManager` 内部，负责 warmup/failure gate |
| `DataStoreMiddleware` | ✅ 稳定 | 三段式落地 |
| `RaftRuntime` | ✅ 初始落地 | 已接入 `openraft 0.10.0-alpha.17 + RocksDB`，支持单节点 bootstrap、metrics/leader 读取、RocksDB log/state machine scaffold |

### 7.2.1 当前代码仍存在的 Redis 时代兼容层

这些组件不应再被误读为 v2 目标态的最终控制面，而应视为**当前实现基线/迁移前状态**：

| 文件 | 当前作用 | 目标态去向 |
|------|----------|-----------|
| `src/sync/leader.rs` | 基于分布式锁的 soft leader | 收敛到 openraft 领导权 |
| `src/common/registry.rs` | 基于 cache + ZSET 的节点注册/心跳 | 收敛到 Raft 状态机 |
| `src/common/status_tracker.rs` | 错误计数、终止判定、模块锁联动 | 收敛到统一执行状态机 |
| `src/engine/engine.rs` 中 cron leader 选择 | 已通过统一 `LeadershipGate` 装配收口 | 启用 `[raft]` 时走 `RaftLeadershipGate`；未启用 `[raft]` 时才回落到 soft leader / 本地 gate |
| `src/cacheable/cache_service/redis_backend.rs` | 当前 Redis cache backend | 被 `raft_rocksdb` 后端替代 |
| `src/queue/redis.rs` | Redis Stream 队列 backend | 保留为可选 MQ backend，不再承担控制面职责 |
| `src/queue/compensation/*` | Redis 补偿记录 | 迁移到 QueueManager 原生补偿 topic |
| `src/engine/events/redis_event_handler.rs` | 可选 Redis 事件输出 | 保留为观测性 sink，不属于控制面核心 |

### 7.3 部分实现（需补全）

| 组件 | 状态 | 缺失部分 |
|------|------|----------|
| `CompensationReplayer` | ⚠️ 设计已定 | 需要把 Begin/Done 事件流接入 QueueManager |
| `Response Cache` | ⚠️ 设计已定 | 需要补齐本地文件索引与 gRPC 远程读取 |
| `Policy / CircuitBreaker` | ⚠️ 框架存在 | 断路器逻辑待完善 |
| `BlobStorage` | ⚠️ 可选启用 | 仅本地文件系统实现 |
| `Metrics` | ⚠️ Prometheus 格式 | 部分指标标签缺失 |
| `Raft 控制面接线` | ⚠️ 已进入主路径 | `State` 已能解析并启动 `RaftRuntime`，scheduler 在启用 `[raft]` 时直接以 Raft Leader 为准；`ProfileControlPlaneStore` 写路径已通过 Raft proposal 提交，并由状态机 apply 回填控制面 RocksDB 读模型，follower 上的控制面写请求会自动 forward 到当前 leader |

### 7.4 待实现

| 组件 | 说明 |
|------|------|
| 统一 `CacheBackend` 落地 | openraft + RocksDB，统一缓存/锁/限流/状态 |
| `LeaderElector` 迁移 | 对未启用 `[raft]` 的旧路径保留 soft leader；Raft 主路径已切到 `RaftLeadershipGate` |
| 联邦 Download Pool | 跨命名空间共享 HTTP 下载能力 |
| `Response.namespace` 路由 | 下载完成后路由回正确命名空间解析器 |
| 跨节点通信能力补齐 | Raft 复制与 join/forward 已走 axum+HTTP(JSON)；Response Cache 跨节点读取接口待补齐（可选 gRPC） |

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

3. `/health` 目前只检查 `cache_service.ping()` 和 `db.ping()`；返回体带 `components.redis / components.db` 与整体 `up / degraded` 状态，但 handler 直接返回 JSON，因此当前实现里即使 degraded 也仍是 HTTP 200。
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
5. 后端不对称：Redis 兼容路径指标较多，Kafka / in-memory / 未来 `Raft + RocksDB` 路径指标不完整。

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
| `backend` | 固定枚举 | 按需 | `memory / redis / kafka / raft / rocksdb / http / wss` |
| `result` | 固定枚举 | 按需 | `success / error / retry / timeout / dropped / rejected / dlq / skipped / canceled` |
| `error_class` | 固定枚举 | 错误类 | `network / parse / policy / backend / timeout / validation / coordination / panic` |
| `error_code` | 受控枚举 | 错误类 | 禁止直接使用原始异常文本 |
| `module` | 模块名 | 受控 | 只在模块数可控的业务指标上允许使用 |
| `workflow` / `node_name` | DAG 上下文 | 受控 | 只在 DAG 相关指标上使用 |

基数控制规则：

1. 禁止把 `account`、`request_id`、`task_id`、`run_id`、原始 URL、原始错误消息作为标签。
2. `module`、`workflow`、`node_name` 只允许出现在真正需要按业务拓扑定位的指标上。
3. `backend` 必须覆盖 `memory / redis / kafka` 当前路径，并为未来 `raft / rocksdb` 预留。
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

`mocra_dag_remote_*`、`mocra_ptm_commit_total` 等现有指标可在兼容期保留为 L3 调试指标，但目标态 L2 应以 backend/placement 标签统一，不再把 Redis 原型路径单独发展成第二套命名。

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
3. 再对齐 queue backend：Redis、Kafka、in-memory 使用同一指标族。
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
- [x] 更新运行文档，明确 parser/error 主路径、fallback 边界与回滚策略

**重构进度评估（2026-04-15）**

- Phase 3 完成度：5/5（含 fallback 推断分支清理与最小保留项边界）
- 代码健康检查（当时）：`cargo test --lib` 通过（`337 passed, 0 failed, 1 ignored`），`cargo test --all-targets` 通过；当前分支最新全量 `cargo test --lib` 结果见 21.1 当前验证基线
- gate 覆盖现状：failure/warmup/shadow 三类 gate 均有可执行单测并通过（`engine::task::task_manager::tests::*`）
- 当前主要风险：显式 fallback 已主要收敛到 gray/failure gate 这类 rollout 控制分支，后续重点转向目标环境 gate 阈值、告警与回滚演练证据闭环

**Phase 3 保留项与退出条件（fallback）**

- 保留项范围：parser/error ingress 不再保留“typed context / transport 目标字段缺失时回退 legacy chain”的兼容分支；当前仅保留 gray/failure gate 未放行时的显式 rollout fallback
- 退出条件：
    1. shadow compare 与 warmup gate 在目标模块满足上线阈值并持续稳定
    2. 目标环境中的 alert gate / probe whitelist / rollback rehearsal 证据闭环
    3. 线上观测窗口内 `gray_gate_blocked` / `failure_gate_blocked` fallback 命中持续收敛至 0（或满足约定阈值）
    4. 具备可验证的按模块粒度回滚演练结果

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

**阶段目标**

- 在 Execution Pipeline 收口后，再推进 Control Plane API、Observability、Bootstrap/Legacy Removal。
- 把当前“文档已冻结但代码未切”的包拆成可执行任务，而不是继续保留大而泛的待办。

**历史交付清单**

- [x] Pack 6：按 `/config/*`、`/tasks/dispatch`、`/cluster/*`、`/debug/*` 分批落控制面 API，并补 leader/follower 写转发验证
- [x] Pack 7：把指标 facade、统一标签、dashboard/alert 基线接到主链路与 scheduler 路径
- [x] Pack 8：重组 `State/Engine` 装配边界，清理 legacy compatibility 层并补 cutover rehearsal 脚本
- [x] 为 Pack 6-8 分别补 acceptance checklist，避免“代码有了但不可切换”
- [x] 在 Pack 6-8 启动前重新审视联邦 Download Pool、QueueManager 原生补偿、统一 CacheService 收口的优先级

**Pack 6-8 Acceptance Checklist**

**Pack 6（Control Plane API）**

- [x] `router.rs` 暴露 `/config/*`、`/tasks/dispatch`、`/cluster/*`、`/debug/*` 路由并可启动
- [x] `/cluster/leader` 在本地模式与 raft 模式均可返回一致语义（含 leader_id / is_local_leader）
- [x] follower 写请求可转发至 leader（`sync::raft::tests::follower_client_write_forwards_to_leader`）
- [x] 控制面关键 handler 覆盖至少一组回归测试（`engine::api::control::tests::*`）

**Pack 7（Observability）**

- [x] control-plane/config/debug/cluster 关键接口都接入统一 throughput/latency 指标
- [x] scheduler ingress fallback 命中按 `path+reason` 可观测
- [x] dashboard baseline 覆盖：吞吐、延迟、错误、fallback 命中、raft 转发
- [x] alerts baseline 覆盖：leader 变更异常、fallback 持续高位、cutover gate 持续阻断

**Pack 7 当前落地资产（第一批）**

- [x] Scheduler ingress 结果接入统一 metrics facade：`mocra_stage_events_total` / `mocra_stage_errors_total`（pipeline=`engine`, stage=`scheduler_ingress`）
- [x] Prometheus 规则：`monitoring/prometheus/rules/mocra-pack7-alerts.yml`
- [x] Grafana baseline dashboard：`docs/dashboards/mocra-pack7-baseline.json`

**Pack 7 当前落地资产（第二批）**

- [x] `/tasks/dispatch` 接入统一 throughput/latency/error 指标（pipeline=`control_plane`, stage=`dispatch`）
- [x] `config/debug/cluster` 接口与 `dispatch` 路径统一纳入 control-plane 指标口径

**Pack 7 结论（2026-04-15）**

- 指标口径统一、观测资产（dashboard+alerts）和关键控制面路径已完成收口，可作为 Pack 8 的基线进入装配边界与 legacy 清理阶段。

**Pack 8（Bootstrap/Legacy Removal）**

- [x] `State/Engine` 装配边界文档化，控制面依赖注入路径固定
- [x] legacy compatibility 清单冻结并分批删除（含 owner、回滚点、完成时间）
- [x] cutover rehearsal 脚本可复现：切入 scheduler -> 观测 -> 回滚 -> 再切入
- [x] rehearsal 输出包含成功判定标准与失败处置手册

**Pack 8 当前落地资产（第一批）**

- [x] API 控制面装配路径固定为单入口：`ApiState::new(queue_manager, prometheus_handle, state)`，`profile_store` 由 `State` 内部注入，避免运行时分散拼装
- [x] rehearsal 脚本：`scripts/cutover_rehearsal.ps1`、`scripts/cutover_rehearsal.sh`
- [x] rehearsal 报告输出统一：`docs/dashboards/rehearsal/cutover_rehearsal_<release_tag>.md`

**Pack 8 当前落地资产（第二批 / P8-A 最小风险删除）**

- [x] 删除 `TaskModelProcessor<ParserTaskModel>` 与 `TaskModelProcessor<ErrorTaskModel>` 中仅用于兼容提示的 debug 文案（不涉及执行路径与分支条件变更）
- [x] 清理 scheduler ingress 注释中的 legacy 语义描述（仅文字调整，不涉及分支与数据流）
- [x] 回归验证：`cargo test --lib scheduler_ingress -- --nocapture` 通过（9/9）
- [x] 回归验证（当时）：`cargo test --lib` 通过（337 passed, 0 failed, 1 ignored）；当前分支最新全量结果见 21.1 当前验证基线

**Pack 8 Rehearsal 使用示例**

PowerShell:

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/cutover_rehearsal.ps1 `
    -PromUrl http://127.0.0.1:9090 `
    -Profile dev `
    -CutInCommand "curl.exe -X POST http://127.0.0.1:8080/cluster/pause" `
    -RollbackCommand "curl.exe -X POST http://127.0.0.1:8080/cluster/resume"
```

Bash:

```bash
bash scripts/cutover_rehearsal.sh \
    "http://127.0.0.1:9090" \
    "curl -sS -X POST http://127.0.0.1:8080/cluster/pause" \
    "curl -sS -X POST http://127.0.0.1:8080/cluster/resume" \
    "dev"
```

**Pack 8 Legacy Compatibility 清单（冻结版）**

| 批次 | 兼容层范围 | 当前锚点 | Owner | 回滚点 | 目标完成时间 |
|------|------------|----------|-------|--------|--------------|
| P8-A | `add_step()` 线性兼容 DAG 合并路径（已完成，2026-04-21） | 已删除；custom DAG 与 `add_step()` 不再走 hybrid merge，`add_step()` 仅作为无 custom DAG 时的线性 fallback | `engine/task` | 回滚需恢复 hybrid merge 语义、显式 linear-compat API 与 `merged_with_linear_compat` 标记 | 2026-04-21 |
| P8-B | parser/error 兼容封装与 legacy schema (`legacy.*`) | `task_model_chain.rs`、`parser_error_adapter.rs`、`request_response_adapter.rs` | `engine/chain` | 恢复 `compatibility wrapper -> ParserDispatch/ErrorEnvelope` 日志链路 | 2026-04-24 |
| P8-C | legacy execution key 双写/兼容读取（已完成，2026-04-20） | 已删除；主链路只保留 canonical `chain:*` key | `common/model` | 不再保留 runtime rollback anchor；旧 key 仅通过 cutover drain 自然淘汰 | 2026-04-20 |
| P8-D | 远端执行 wire 兼容 JSON 字段兜底（已完成，2026-04-20） | 已删除；remote wire 现要求显式携带 `runtime_input_envelope` 字段（可为 `null`） | `schedule/dag` | 不再保留缺字段兼容；回滚需恢复 `#[serde(default)]` 与 legacy 缺字段测试 | 2026-04-20 |
| P8-E | legacy runtime 输入构造桥接 | `module_node_runtime_bridge.rs` + `module_dag_processor.rs` / `module_processor_with_chain.rs` 中 residual legacy parser output adapter | `engine/task` | 恢复 `node_context_adapter.rs` 对 legacy parser output/metadata helper 的集中导出 | 2026-04-28 |

冻结策略：本表进入变更控制后，新增兼容层一律禁止；仅允许“替换或删除”，且必须附带回滚点与 gate 结果。

已执行批次记录：

- 2026-04-21 / P8-A batch-1：`module_dag_orchestrator.rs` 已删除显式 `compile_linear_compat()` API，`task_manager.rs` 已删除 `compile_module_dag_linear_compat()` / `execute_module_dag_linear_compat()` / `execute_module_dag_linear_compat_with_compare()` 三个 linear-compat 入口，`workflow_compiler.rs` 与 `module_dag_orchestrator.rs` 也已移除 `merged_with_linear_compat` 元数据标记；当前只保留 `compile_module()` / `build_definition()` 目标路径与 `legacy_` 命名空间 merge 本身。回滚点为恢复显式 linear-compat API 和 `merged_with_linear_compat` 标记；回归记录：`cargo test module_dag_orchestrator --lib -- --nocapture`、`cargo test workflow_compiler --lib -- --nocapture`、`cargo test task_manager --lib -- --nocapture`
- 2026-04-21 / P8-A batch-2：`module_dag_orchestrator.rs` 与 `workflow_compiler.rs` 已删除 custom DAG 与 `add_step()` 同时存在时的 hybrid merge 语义，当前统一为 `dag_definition()` 优先，`add_step()` 只在没有 custom DAG 时生效；`ModuleTrait` 注释与相关测试已同步到 custom-first 行为。至此 P8-A 完成。回滚点为恢复 `legacy_` 命名空间 merge 与 hybrid workflow/profile 测试；回归记录：`cargo test module_dag_orchestrator --lib -- --nocapture`、`cargo test workflow_compiler --lib -- --nocapture`
- 2026-04-17 / P8-B batch-1：`task_model_chain.rs` 的 parser/error scheduler ingress 已改为 `profile.resolve_node_config(node_id)` 优先构造 typed runtime input，仅在缺失 profile node config 时回退 `build_legacy_generate_runtime_input`；回滚点保持为 `compatibility wrapper -> ParserDispatch/ErrorEnvelope` 与 legacy helper；回归记录：`cargo test --lib engine::chain::task_model_chain::scheduler_ingress_tests -- --nocapture` -> `24 passed; 0 failed`
- 2026-04-20 / P8-B batch-2：`parser_error_adapter.rs` 的 `extract_parser_dispatch_seed` / `extract_error_envelope_seed` 已不再回退解码 legacy payload，当前 parser/error seed 提取要求 envelope 显式携带 `parser_context` / `error_context`；回滚点为恢复 legacy payload decode fallback 与缺失 typed context 的兼容行为；回归记录：`cargo test parser_error_adapter --lib -- --nocapture`
- 2026-04-20 / P8-B batch-3：`task_model_chain.rs` 的 scheduler ingress 已删除 parser/error seed-only fallback，node/module/metadata/prefix 解析现在只走 `typed context -> transport fields`；回滚点为恢复 `parser_seed_fallback` / `error_seed_fallback` 及对应 seed 解析分支；回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-21 / P8-B batch-4：`parser_error_adapter.rs` 已删除 `legacy.parser_task` / `legacy.error_task` schema 的主路径使用与 error detail payload 依赖；`build_parser_task_dispatch()` 现仅发送 transport placeholder payload，`build_error_task_envelope()` 不再附带 legacy detail，`decode_parser_task_dispatch()` / `decode_error_task_envelope()` 统一改为从 transport fields 与 typed context 重建事件语义。回滚点为恢复 `legacy.parser_task` / `legacy.error_task` schema 与 detail payload decode；回归记录：`cargo test parser_error_adapter --lib -- --nocapture`、`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-21 / P8-B batch-5：`request_response_adapter.rs` 已把 request/response dispatch 的 schema id 从 `legacy.request` / `legacy.response` 切换为 `transport.request` / `transport.response`，去掉这两类 legacy schema 名称继续作为主路径事实标准；`transport_envelope.rs` 与 adapter 自身测试已同步更新。该批次不改变 request/response payload 语义，只收口 schema 命名。回滚点为恢复 `legacy.request` / `legacy.response` schema id。回归记录：`cargo test request_response_adapter --lib -- --nocapture`、`cargo test transport_envelope --lib -- --nocapture`
- 2026-04-21 / P8-B batch-6：`parser_error_adapter.rs` 与 `request_response_adapter.rs` 已继续清理内部 fallback node / helper / test 命名中的 `legacy_*` 语义，默认 node key 现统一改为 `transport_parser` / `transport_error` / `transport_request` / `transport_response`，避免 transport-only 主路径仍以 legacy 名称承载默认 routing 语义。该批次不改变编码/解码行为，只继续收口主路径命名。回滚点为恢复两处 adapter 内部 `legacy_*` fallback node 与 helper/test 命名。回归记录：`cargo test parser_error_adapter --lib -- --nocapture`、`cargo test request_response_adapter --lib -- --nocapture`
- 2026-04-21 / P8-B batch-7：`parser_chain.rs` 与 `task_model_chain.rs` 的内部 error 入队路径已不再临时物化 `TaskErrorEvent` 再转 envelope，改为直接构造 `ErrorEnvelopeSeed` 并生成 `NodeErrorEnvelope`；该批次不改变外部 processor trait 的兼容入口，只继续压缩主链路内部的 legacy carrier 组装。回滚点为恢复两处链路内部 `TaskErrorEvent` 临时对象与 `build_error_task_envelope()` 组装。回归记录：`cargo test parser_chain --lib -- --nocapture`、`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-21 / P8-B batch-8：`task_model_chain.rs` 中 `ProcessorTrait<TaskParserEvent, Task>`、`ProcessorTrait<TaskErrorEvent, Task>` 以及 `UnifiedTaskInput::ParserTask/ErrorTask` 的兼容入口已统一改成通过 `ParserDispatchSeed` / `ErrorEnvelopeSeed` 走 shared seed builder 生成 transport envelope，不再在这些入口直接调用 legacy model -> envelope 的专用 builder。该批次不移除外部兼容 trait，只进一步统一主链路内部的 envelope 构造方式。回滚点为恢复这些兼容入口对 `build_parser_task_dispatch()` / `build_error_task_envelope()` 的直接调用。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`、`cargo test parser_error_adapter --lib -- --nocapture`
- 2026-04-21 / P8-B batch-9：`UnifiedTaskIngressChain::execute()` 已把 `UnifiedTaskInput::ParserTask/ErrorTask` 与 `ParserDispatch/ErrorEnvelope` 对齐为同一套 scheduler cutover 判定顺序；兼容输入在转成 transport envelope 后不再绕过 `try_scheduler_parser_dispatch()` / `try_scheduler_error_dispatch()`，避免同一语义输入因载体不同而走出不同的 local/remote 分叉。该批次不移除兼容输入类型，只统一 cutover gate。回滚点为恢复 `UnifiedTaskIngressChain::execute()` 中 `ParserTask/ErrorTask` 分支直接执行 `parser_chain/error_chain` 的旧路径。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-21 / P8-B batch-10：`parser_error_adapter.rs` 已删除仅面向 legacy model wrapper 的 `build_parser_task_dispatch()` / `build_error_task_envelope()` 两个薄封装，主代码与单测现统一直接使用 `build_parser_dispatch_from_seed()` / `build_error_envelope_from_seed()`；这意味着 parser/error transport adapter 的公共构造路径只再保留 seed-based 接口。回滚点为恢复两个 legacy-model wrapper builder 并让调用点重新回到 `TaskParserEvent` / `TaskErrorEvent` 薄封装入口。回归记录：`cargo test parser_error_adapter --lib -- --nocapture`、`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-21 / P8-B batch-11：`task_model_chain.rs` 的 `scheduler_ingress_tests` 已新增 execute-level 兼容输入回归，直接验证 `UnifiedTaskIngressChain::execute()` 在接收 `UnifiedTaskInput::ParserTask/ErrorTask` 时，会先 canonical 化为 `NodeDispatchEnvelope` / `NodeErrorEnvelope` 再进入 parser/error 链，而不是继续把 legacy carrier 当作下游事实标准。该批次不引入真实 scheduler bridge，只把“兼容输入已统一 transport contract”从间接断言提升为直接测试证据。回滚点为删除两条 execute-level 回归并恢复仅靠 `try_scheduler_*`/resolver 单测证明 cutover 的状态。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-21 / P8-B batch-12：`task_model_chain.rs` 已删除 `TaskModelProcessor` 上不再被任何 ingress chain 消费的 `ProcessorTrait/EventProcessorTrait<TaskParserEvent, Task>` 与 `ProcessorTrait/EventProcessorTrait<TaskErrorEvent, Task>` 两组实现，避免 `TaskParserEvent/TaskErrorEvent` 继续伪装成可直接驱动 `TaskModelProcessor` 的主路径输入；当前兼容输入只再通过 `UnifiedTaskIngressChain::execute()` 先 canonical 化为 transport envelope 后进入 parser/error 链。回滚点为恢复这两组 compatibility impl。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-21 / P8-B batch-13：`factory.rs` 已删除 `load_parser_model()` / `load_error_model()`，`task_manager.rs` 已删除对应的 `load_parser()` / `load_error()` wrapper；这两层在代码中已无任何真实调用，继续保留只会让 `TaskParserEvent/TaskErrorEvent` 看起来像仍是 task 恢复的正式入口。当前 parser/error 恢复路径只再保留 `ParserDispatchSeed/ErrorEnvelopeSeed -> NodeDispatchEnvelope/NodeErrorEnvelope -> load_*_dispatch/envelope`。回滚点为恢复这四个 legacy wrapper。回归记录：`cargo test task_manager --lib -- --nocapture`
- 2026-04-21 / P8-B batch-14：`queue/compensation.rs` 已删除 `TaskParserEvent` / `TaskErrorEvent` 的 `Identifiable` 兼容实现。当前 `Channel` 与 `QueueManager` 的 parser/error topic 已统一使用 `NodeDispatchEnvelope` / `NodeErrorEnvelope`，继续保留旧 carrier 的 queue 身份接口没有运行时价值，只会扩大“旧 carrier 仍可直接入队”的误导面。回滚点为恢复这两条 `Identifiable` impl。回归记录：`cargo test queue --lib -- --nocapture`
- 2026-04-21 / P8-B batch-15：`common/model/message.rs` 已删除一批 workspace 内零调用的 legacy carrier helper surface：`TaskParserEvent::start_other_module*()`、`UnifiedTaskInput` 针对 `TaskParserEvent/TaskErrorEvent` 的直接 `From` 转换、`From<&Response> for TaskErrorEvent`，以及 `TaskOutputEvent` 上未再被消费的 `with_tasks/with_error/with_stop` builder。该批次不移除 `TaskParserEvent` / `TaskErrorEvent` / `TaskOutputEvent` 类型本体，也不触碰仍由 trait 与测试基面承载的 `TaskOutputEvent::with_task()` / `TaskParserEvent::from(&Response)`；目标只是先压缩 `message.rs` 的零调用 helper 面。回滚点为恢复上述 helper/builder/conversion impl。回归记录：`cargo test message --lib -- --nocapture`
- 2026-04-21 / P8-B batch-16：`common/model/message.rs` 已继续删除两处 workspace 内零调用的 legacy helper：旧的 topic 名称拼装器 `TopicType::get_name()` 与 `UnifiedTaskInput::run_id()` accessor。它们都不再参与当前 queue contract 或 ingress 主路径，只会继续扩大 `message.rs` 的遗留表面积。该批次不改变任何 carrier 类型与队列路由语义，只进一步压缩零调用 API 面。回滚点为恢复这两个 helper。回归记录：`cargo test message --lib -- --nocapture`
- 2026-04-21 / P8-B batch-17：`prelude.rs` 已删除对 `TaskOutputEvent` / `TaskParserEvent` 的公共再导出，workspace 内剩余消费者改为显式从 `common::model::message` 导入。该批次不改变 `ModuleNodeTrait` 的 parser 返回签名，也不移除 `TaskOutputEvent` / `TaskParserEvent` 类型本体；目标是先把 legacy carrier 从默认公共入口面撤掉，避免继续把它们包装成推荐用户路径。回滚点为恢复 `prelude` 再导出并撤销对应调用点导入调整。回归记录：`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\Cargo.toml message --lib -- --nocapture`、`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\tests\Cargo.toml --bin dag_module_trait_e2e -- --nocapture`
- 2026-04-21 / P8-B batch-18：`common/model/message.rs` 已删除 `From<&Response> for TaskParserEvent` 这一仅剩测试语义使用的 legacy conversion helper，`module_dag_processor.rs` 与 `message.rs` 的相关回归已同步改成显式构造 `TaskParserEvent`。该批次不改变 parser 主路径、`TaskParserEvent` 类型本体或 `TaskOutputEvent` 兼容载体；目标只是继续压缩 `message.rs` 中不再被生产代码依赖的隐式 legacy builder/conversion 面。回滚点为恢复该 `From<&Response>` impl 及对应测试。回归记录：`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\Cargo.toml message --lib -- --nocapture`、`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\Cargo.toml module_dag_processor --lib -- --nocapture`
- 2026-04-21 / P8-B batch-19：`common/model/message.rs` 已删除 `TaskOutputEvent::with_task()` helper，仓库内最后两个残余调用点（`simple/module_node_trait_dag.rs` 与 `tests/src/modules/test/add_chain_api.rs`）已同步改成显式构造 `TaskParserEvent` 并直接 `push` 到 `parser_task`。该批次不改变 `TaskOutputEvent` 类型本体或旧 trait 示例的整体接口，只继续删除把 legacy parser carrier 包装成默认 builder 风格的表面积。回滚点为恢复 `with_task()` helper 及两个示例中的链式调用。回归记录：`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\Cargo.toml message --lib -- --nocapture`
- 2026-04-21 / P8-B batch-20：`engine/events/events.rs` 已删除 `ParserTaskModelEvent` 对 `TaskParserEvent` / `TaskErrorEvent` 的直接 `From` 兼容映射，相关回归改为只验证 `NodeDispatchEnvelope` / `NodeErrorEnvelope` 这两条当前主路径输入。该批次不改变事件名、event bus 契约或 parser/error processor 逻辑；目标是让事件层也停止把 legacy parser/error carrier 当作一等生产输入模型。回滚点为恢复两条 `From<&TaskParserEvent|&TaskErrorEvent>` impl 及对应测试。回归记录：`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\Cargo.toml events --lib -- --nocapture`
- 2026-04-21 / P8-B batch-21：`common/model/message.rs` 已删除 `TaskOutputEvent::with_data()` 这一最后残留的 output builder helper，剩余三个旧测试模块调用点（`tests/src/modules/test/add_chain_api.rs`、`tests/src/modules/test/moc_dev.rs`、`tests/src/modules/test/portal_live_trend.rs`）已改成显式 `output.data.push(...)`。该批次不改变 `TaskOutputEvent` 类型本体、旧 trait 签名或测试模块业务逻辑；目标只是把 `TaskOutputEvent` 从 builder 风格 helper 收口到纯结构体载体。回滚点为恢复 `with_data()` helper 及三处旧写法。回归记录：`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\Cargo.toml message --lib -- --nocapture`
- 2026-04-21 / P8-B batch-22：`simple/module_node_trait_dag.rs` 已迁移到当前 `NodeGenerateContext<'_>` / `NodeParseContext<'_>` / `NodeParseOutput` 接口，并补齐稳定 node key 与 typed `NodeDispatch` 示例；`README.md` / `README.zh.md` 的 quickstart 片段也已同步改成 typed parser 签名，`common/interface/module.rs` 的 trait 注释则从旧的 `Request -> Response -> ParserData -> Request` 更新为 `Request -> Response -> NodeParseOutput -> NodeDispatch`。该批次不改变生产调度逻辑或 `TaskOutputEvent` 类型本体；目标是收掉仓库对外可见的旧示例/trait 签名口径，避免继续传播过时 API。回滚点为恢复 `simple/module_node_trait_dag.rs` 和中英文 README 的旧签名示例。回归记录：`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\Cargo.toml common::interface::module --lib -- --nocapture`
- 2026-04-21 / P8-B batch-23：`tests/src/modules/test/add_chain_api.rs`、`moc_dev.rs`、`mock_dev.rs`、`portal_live_trend.rs` 已迁移到当前 `NodeGenerateContext<'_>` / `NodeParseContext<'_>` / `NodeParseOutput` 接口，并统一改为通过 `DataEvent` / `NodeDispatch` 组装 typed parser 结果；同时 `tests/src/middleware/object_store.rs` 已切到 `DataEvent`，`tests/src/main.rs` 也已对齐当前 `Engine::new(...).await -> Result<Engine>` 与 `RequestDownloader::new(...)` 签名，并把 Redis 不可达的 session harness case 收口为显式 skip。该批次使 `tests/src` 范围内残留的 `ParserData` / `ParserTaskModel` 命名兼容面清零，并把 `tests_debug` 恢复到当前代码可执行状态。回滚点为恢复这几处 tests harness 的旧 parser 签名、`Data` 旧类型与旧构造函数调用方式。回归记录：`cargo test --manifest-path C:\Users\eason\RustroverProjects\mocra\tests\Cargo.toml --bin tests_debug -- --nocapture`
- 2026-04-22 / P8-B batch-24：`engine/events/events.rs` 中仅用于 event bus 的紧凑载体 `ParserTaskModelEvent` 已重命名为 `ParserDispatchEvent`，对应 `EventType::ParserTaskModel` 也已收口为 `EventType::ParserDispatch`；`engine.rs`、`engine/engine/processors.rs` 与 `engine/events/redis_event_monitor.rs` 的内部事件键/订阅字符串同步从 `parser_task_model` 切到 `parser_dispatch`。该批次不触碰 `common/model/message.rs` 中 `TaskParserEvent` / `TaskOutputEvent` 类型本体，也不改变 parser/error 运行时 carrier；目标只是先清掉事件层残留的旧命名，让 event bus 口径和当前 envelope-first 主路径一致。回滚点为恢复 `ParserTaskModelEvent` / `EventType::ParserTaskModel` 及其事件键字符串。回归记录：`cargo test engine::events --lib -- --nocapture`、`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-22 / P8-B batch-25：`engine/task/module.rs` 中残留的测试夹具命名 `ParserDataTestNode` / `ParserDataTestModule` 已收口为 `ParseOutputTestNode` / `ParseOutputTestModule`，避免 typed parser output 主路径旁边继续保留旧 `ParserData` 术语；同时 `common/model/message.rs` 已删除两处 workspace 内零调用的 `get_context()` helper，并把 `stay_current_step()` 注释从旧 `ParserTaskModel` 术语改成当前 dispatch 语义。该批次不触碰 `TaskParserEvent` / `TaskErrorEvent` / `TaskOutputEvent` 类型本体，也不改变 parser/error 主链路行为；目标只是继续清理剩余内部旧命名与零调用 helper 表面积。回滚点为恢复两处测试夹具命名、`get_context()` helper 和 `stay_current_step()` 旧注释。回归记录：`cargo test module --lib -- --nocapture`、`cargo test message --lib -- --nocapture`
- 2026-04-22 / P8-B batch-26：`common/model/message.rs` 已继续删除 `TaskParserEvent::with_context()`、`stay_current_step()`、`with_prefix_request()` 这三处只被 `message.rs` 自测消费的 legacy-style builder，并把相关回归改成显式字段更新，避免 `TaskParserEvent` 继续暴露 builder 风格的兼容入口。该批次不触碰 `TaskParserEvent` / `TaskErrorEvent` / `TaskOutputEvent` 类型本体，也不改变 parser/error 生产主路径；目标只是进一步压缩 `message.rs` 中仅剩的零调用 helper 表面积。回滚点为恢复这三个 helper 与 `message.rs` 对应测试中的链式写法。回归记录：`cargo test message --lib -- --nocapture`
- 2026-04-22 / P8-B batch-27：`common/model/message.rs` 已继续删除 `TaskErrorEvent::with_meta()`、`add_meta()` 两处 workspace 内零调用 helper，以及 `UnifiedTaskInput` / `TaskOutputEvent` 两个空的无行为 `impl` 块；同时 `module_dag_processor.rs` 中仅测试使用的 `TaskParserEvent::with_meta()` / `add_meta()` 调用已改成显式 metadata 更新。该批次不触碰 `UnifiedTaskInput::ParserTask/ErrorTask` 兼容变体或 `TaskOutputEvent` / `TaskParserEvent` / `TaskErrorEvent` 类型本体，因为这些仍被 scheduler ingress 与兼容测试基面实际消费；目标只是把 `message.rs` 中最后一层“零调用 helper / 空壳 impl” 表面积先收干净。回滚点为恢复上述 helper、空 `impl` 块与 `module_dag_processor.rs` 测试中的链式 metadata 写法。回归记录：`cargo test add_meta_appends_with_meta_replaces --lib -- --nocapture`、`cargo test message --lib -- --nocapture`
- 2026-04-22 / P8-B batch-28：`common/model/message.rs` 已删除 workspace 内零引用的 `TaskOutputEvent` 兼容类型本体，并同步清理其遗留 `DataEvent` 导入；这意味着 `message.rs` 中与 parser output 相关的“旧兼容壳”现在只剩 `TaskParserEvent` / `TaskErrorEvent` 以及 `UnifiedTaskInput::ParserTask/ErrorTask` 这组 ingress 兼容输入。该批次不触碰 scheduler ingress 对这两类兼容变体的实际消费，也不改变 parser/error 主链路；目标只是继续把 `message.rs` 中已完全失去运行时和测试引用的 legacy carrier 彻底移除。回滚点为恢复 `TaskOutputEvent` 类型定义与对应导入。回归记录：`cargo test message --lib -- --nocapture`
- 2026-04-22 / P8-B batch-29：`common/model/message.rs` 已删除 `TaskParserEvent` / `TaskErrorEvent` 上不再被任何当前 queue 路径消费的 `Offloadable` / `Prioritizable` 兼容实现；`parser_error_adapter.rs` 中仅被本地测试消费的 `decode_parser_task_dispatch()` / `decode_error_task_envelope()` 也已移除，相关回归改为直接断言 `extract_*_seed()` 结果。该批次不触碰 `UnifiedTaskInput::ParserTask/ErrorTask` 兼容变体或 `ParserDispatchSeed` / `ErrorEnvelopeSeed` 的生产主路径，只继续清理“legacy carrier 仍像 queue item / decode endpoint 一样存在”的残留表面积。回滚点为恢复这四个 trait impl、两条 decode wrapper 以及对应测试中的旧断言方式。回归记录：`cargo test message --lib -- --nocapture`、`cargo test parser_error_adapter --lib -- --nocapture`
- 2026-04-22 / P8-B batch-30：`common/model/message.rs` 已删除 `UnifiedTaskInput::ParserTask/ErrorTask` 两条仅剩的 ingress 兼容变体；`task_model_chain.rs` 中对应的 execute-level canonicalization 分支与只服务这些分支的本地测试辅助也已同步移除。该批次不触碰 `TaskParserEvent` / `TaskErrorEvent` 类型本体或 `ParserDispatchSeed` / `ErrorEnvelopeSeed` 的生产转换逻辑，只结束 scheduler ingress 继续接受 legacy parser/error carrier 作为统一入口这一兼容面。回滚点为恢复 `UnifiedTaskInput` 的两条兼容变体、`UnifiedTaskIngressChain::execute()` 中对应分支以及相关 execute-level 回归。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-22 / P8-B batch-31：`common/model/message.rs` 已删除只剩测试语义的 `TaskParserEvent` 类型本体；`parser_error_adapter.rs` 中对应的 `From<&TaskParserEvent> for ParserDispatchSeed` 生产转换也已移除，相关回归统一改为直接构造 `ParserDispatchSeed`。`module_dag_processor.rs` 与 `task_model_chain.rs` 中残留的 `TaskParserEvent` 注释/测试也已同步收口到当前 dispatch-seed 语义。该批次不触碰仍被生产错误路径实际消费的 `TaskErrorEvent -> ErrorEnvelopeSeed` 转换，只继续压缩 parser 侧已经完全退出运行时的 legacy carrier 表面积。回滚点为恢复 `TaskParserEvent` 类型定义、`ParserDispatchSeed` 上的 legacy `From` 转换，以及对应测试中的旧 carrier 构造。回归记录：`cargo test parser_error_adapter --lib -- --nocapture`、`cargo test message --lib -- --nocapture`、`cargo test module_dag_processor --lib -- --nocapture`
- 2026-04-22 / P8-B batch-32：`module_dag_processor.rs` 与 `module_processor_with_chain.rs` 的 parser 失败路径已不再临时物化 `TaskErrorEvent`，统一改为直接构造 `ErrorEnvelopeSeed`；`common/model/message.rs` 中只剩测试语义的 `TaskErrorEvent` 类型本体，以及 `parser_error_adapter.rs` 上对应的 `From<&TaskErrorEvent> for ErrorEnvelopeSeed` 兼容转换也已删除，adapter 回归统一改为直接构造 `ErrorEnvelopeSeed`。该批次结束了 parser/error 运行时继续依赖 `message.rs` 中 legacy carrier 类型本体的最后一条代码路径；剩余 T21-03 重点转向 request/response transport contract 收口与整张 DAG cutover。回滚点为恢复两处 processor 中的 `TaskErrorEvent` 临时对象、`TaskErrorEvent` 类型定义以及 adapter 上对应 `From` 转换。回归记录：`cargo test parser_error_adapter --lib -- --nocapture`、`cargo test message --lib -- --nocapture`、`cargo test module_dag_processor --lib -- --nocapture`、`cargo test module_processor_with_chain --lib -- --nocapture`
- 2026-04-22 / P8-B batch-33：`request_response_adapter.rs` 的 `decode_request_dispatch()` / `decode_response_dispatch()` 已从“MsgPack/Json 双解码”收口为只接受当前 transport 主路径实际发送的 `PayloadCodec::MsgPack`；两条新增回归明确断言 Json codec 的 request/response dispatch 会被拒绝，从而结束 request/response transport decode 继续为历史 Json dispatch 保留兼容壳的状态。该批次不移除 decode wrapper 本身，也不改变 queue/worker 对 request/response dispatch envelope 的运行时入口，只把 transport contract 明确收口到当前实际生产格式。回滚点为恢复 decode 中的 Json fallback 分支以及对应失败断言回归。回归记录：`cargo test request_response_adapter --lib -- --nocapture`、`cargo test queue --lib -- --nocapture`
- 2026-04-22 / P8-B batch-34：`engine/processors.rs` 的 request/response worker 在 retry/fatal 进入 DLQ 时，已不再把解码后的裸 `Request` / `Response` 写入 DLQ，而是保留原始 `RequestDispatchEnvelope` / `ResponseDispatchEnvelope` 作为 replay 载荷。这样 `QueueManager::requeue_dlq_message()` 回放到 `request` / `response` topic 时，payload 形态与 worker 订阅契约保持一致，避免 replay 再次落回“业务模型字节直接投递到 dispatch topic”的兼容壳。该批次不改变 `QueueManager::send_to_dlq()` 的泛型接口，也不修改 DLQ API 面，只收口 request/response worker 失败路径的保存载荷。回滚点为恢复 processor 中 request/response 失败分支传入 `handle_policy_retry()` / `handle_policy_failure()` 的裸业务模型，以及对应断言 DLQ payload 形态的单测。回归记录：`cargo test retryable_failure_policy_ --lib -- --nocapture`
- 2026-04-22 / P8-B batch-35：`engine/api/dlq.rs` 已补上 request/response replay 的 API 级契约证据：`GET /dlq/messages` 现在有直接回归断言 request DLQ 记录会暴露 `queue.request` / `msgpack` 这类当前 queue wrapper contract 元数据；`POST /dlq/messages/{id}/requeue` 也有回归断言 response dispatch 会按 backend record id 精确回放，并继续以 `ResponseDispatchEnvelope` 形态重新发布到原始 source topic。该批次不改变 DLQ handler 的对外接口，也不引入新的 payload 解析字段，只把 API 层当前已经承诺的 replay contract 固定成测试证据。回滚点为移除 `dlq.rs` 中这两条 handler 级单测。回归记录：`cargo test dlq_message --lib -- --nocapture`
- 2026-04-22 / P8-B batch-36：`queue/tests.rs` 中原先仍以裸 `Request` 模型为基线的 DLQ record-id replay 回归，已改为直接使用 `RequestDispatchEnvelope`，并补上一条对称的 `ResponseDispatchEnvelope` replay 用例。当前 queue slice 已明确锁定两点：request/response 进入 DLQ 后保留的是 dispatch envelope payload，且 record-id replay 会把同一 envelope 形态重新发布回 `request` / `response` source topic，而不是回落到裸业务模型字节。该批次不改变 `QueueManager::send_to_dlq()` / `requeue_dlq_message()` 的运行时逻辑，只修正 queue 层测试基线到当前契约。回滚点为恢复 request replay 测试中的裸 `Request` 断言，以及删除新增的 response replay 用例。回归记录：`cargo test dlq_ --lib -- --nocapture`
- 2026-04-22 / P8-B batch-37：`engine/api/dlq.rs` 已进一步补上 request/response replay 的真实整链证据：新增两条回归直接走 `QueueManager::send_to_dlq()` 的真实存储路径，再通过同一 backend 上的 `POST /dlq/messages/{id}/requeue` 完成 record-id replay，确认 request/response 两侧都会把原始 `RequestDispatchEnvelope` / `ResponseDispatchEnvelope` 重新发布回 source topic。至此 request/response 的 worker 失败入 DLQ、queue 存储、DLQ API 检视与 record-id replay 四段局部证据已经闭环，P8-B 中 request/response replay 的本地整链缺口已收口。该批次不改变运行时逻辑，只把最后一段“真实 queue 存储 -> API replay”链路固定成测试。回滚点为移除 `dlq.rs` 中两条 `queue_manager_stored_*_dlq_record_can_be_replayed_via_api` 集成单测。回归记录：`cargo test queue_manager_stored_ --lib -- --nocapture`
- 2026-04-22 / P8-E batch-38：`module.rs` 中 parser scheduler bridge 对本地 placement 的失败分支已不再回退到 `execute_parse()` 本地快路径，而是与 remote placement 一样 fail-closed 返回错误；对应回归也已从“本地成功回退”改为显式断言 bridge 失败会直接冒泡。该批次结束了 parser scheduler bridge 在执行期因 dispatcher 失败而静默落回旧本地 parse 语义的兼容壳，进一步把 E04 的 cutover 边界收紧到“无 dispatcher 时允许显式本地路径、bridge 已接管时失败即失败”。回滚点为恢复 `Module::parser()` 中 `Err(err) if !requires_remote_dispatch` 分支的本地 parse 回退，以及对应测试中的成功断言。回归记录：`cargo test parser_fails_closed_when_scheduler_bridge_fails_for_local_node --lib -- --nocapture`
- 2026-04-23 / P8-E batch-39：`task_model_chain.rs` 中 parser/error scheduler ingress 在 bridge 已接管后，如果 `load_*` 重建出的 task 无法包含当前解析出的目标模块/节点，已不再回退到旧 parser/error ingress chain，而是与其他 scheduler 执行期异常一样直接返回 retryable failure。该批次继续把 E04 的执行期兼容边界从“桥接失败时可能掉回旧链”收紧到“只有入口不具备接管资格时才 fallback；一旦 bridge 已接管，task 重建不一致也 fail-closed”。回滚点为恢复 parser/error 两侧 `module_not_found -> record_scheduler_ingress_fallback(...); return None` 分支，以及对应 fail-closed 回归。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-23 / P8-E batch-40：`task_model_chain.rs` 中 parser/error scheduler ingress 在 bridge 已接管后，如果目标模块的预编译 DAG 缓存缺失，已不再把该情况记作 fallback 后回退到旧 parser/error ingress chain，而是与其它 scheduler 执行期不一致一样直接返回 retryable failure。该批次继续把 E04 的 fallback 边界压缩到“bridge 缺失 / gate 未放行 / typed context 不完整”等未接管前提，避免模块 DAG 编译/装配漂移在 bridge 已接管后静默落回旧链。回滚点为恢复 parser/error 两侧 `dag_missing -> record_scheduler_ingress_fallback(...); return None` 分支，以及对应 fail-closed 回归。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-23 / P8-E batch-41：`task_model_chain.rs` 中 parser/error scheduler ingress 的 `bridge_missing` 已不再被视为可回退到旧 parser/error ingress chain 的合法边界；当前生产装配只通过 `create_unified_task_ingress_chain(...).with_scheduler_bridge(...)` 构造统一 ingress，因此一旦 bridge 本身未挂载，就与其它 bridge 已接管后的执行期不一致一样直接返回 retryable failure。该批次同时把 scheduler ingress 的资格判定顺序调整为“先判 node/module 等接管资格，再判 bridge 是否缺失”，避免 `bridge_missing` 短路这些资格不足检查；这些资格不足分支已在后续 batch-43 继续收口为 fail-closed。回滚点为恢复 parser/error 两侧 `bridge_missing -> record_scheduler_ingress_fallback(...); return None` 分支，以及对应测试中的 `returns_none` 断言。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-23 / P8-E batch-42：`task_model_chain.rs` 中 scheduler ingress 的 `record_scheduler_ingress_fallback()` 已把 gray/failure gate blocked 从 `scheduler_ingress` stage error 口径里剥离出来，保留 `mocra_scheduler_ingress_total{result="fallback",reason=...}` 这条 cutover 指标，但不再额外把这两类有意 rollout fallback 计入 `mocra_stage_errors_total{pipeline="engine",stage="scheduler_ingress"}`。这样 gate blocking 继续作为独立 cutover 信号存在，而 stage error 更聚焦于真正的 malformed envelope / bridge execution / runtime mismatch 类异常。回滚点为恢复 `record_scheduler_ingress_fallback()` 对所有 fallback reason 一律调用 `record_scheduler_ingress_error()`，以及对应 helper 单测。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-23 / P8-E batch-43：`task_model_chain.rs` 中 parser/error scheduler ingress 对 `node_id_missing` / `modules_missing` 已不再把它们视为“资格不足可回退 legacy chain”的边界；当前主路径的 parser/error envelope 构造会补齐 transport `module/node` 真相，因此一旦 bridge 已接管却仍缺失这些目标字段，就与其它 malformed transport 一样直接返回 retryable failure。该批次同时把原先依赖 `returns_none` 的 typed-context-missing 回归改成 fail-closed 断言，确认 parser/error ingress 的显式 fallback 已进一步收敛到 gray/failure gate blocked 这类 rollout 控制分支。回滚点为恢复 parser/error 两侧 `node_id_missing/modules_missing -> record_scheduler_ingress_fallback(...); return None` 分支，以及对应测试中的 fail-closed 断言。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-23 / P8-E batch-44：`module.rs` 中 generate scheduler bridge 对本地 placement 的执行失败已不再回退到旧 `execute_generate()` 本地路径，而是与 parser 侧保持一致，一旦 bridge 已接管却执行失败就直接 fail-closed 返回错误。该批次结束了 generate/parser 在 local placement 下对 bridge failure 语义的不对称状态，把单节点 scheduler bridge 的异常合同统一为“无 dispatcher 时可本地执行，bridge 执行失败时 generate/parser 均 fail-closed”。回滚点为恢复 `Module::generate()` 中 `Err(err) if !requires_remote_dispatch` 分支的本地 generate 回退，以及对应测试中的成功回退断言。回归记录：`cargo test scheduler_bridge_fails_for_local_node --lib -- --nocapture`
- 2026-04-23 / P8-E batch-45：`module.rs` 中一条过期回归曾把 local/no-dispatcher parser 误记成“回退旧本地 parse 路径”；当前代码真实语义已核对为：local/no-dispatcher 仍走 scheduler 本地执行，只有 bridge-supported 的 `NodeDispatch + finished` parser 输出会成功，若 parser 产出 `data` 则因 `module_node_runtime_bridge.rs` 尚未支持该 payload 而显式 fail-closed。该批次把回归拆成一条成功样例和一条 data-output fail-closed 样例，用测试固定这条现有合同，并把文档同步到当前代码。回滚点为恢复旧的成功回退断言，或为 parser runtime bridge 增加 `NodeParseOutput.data` payload 支持后改写这两条回归。回归记录：`cargo test module --lib -- --nocapture`
- 2026-04-23 / P8-E batch-46：`module_node_runtime_bridge.rs` 已为 parser output 增加 `NodeParseOutput.data` 的 payload wrapper，当前会把 `DataEvent` 按 file/dataframe 两类可序列化 transport 形态编码进 scheduler parser output，并在 decode 时重建为 `ParsedData`；`data.rs` 同步补了 `DataFrameStore` 的最小重建 builder。这样 local/no-dispatcher parser 不再因为产出 `data` 而 fail-closed，而是与 `NodeDispatch` 一样可通过 scheduler 本地执行链路返回给 `Module::parser()`。该批次同时把对应 module 回归从失败断言改成成功断言，并补齐 parser output payload round-trip 测试。回滚点为移除 parser output 的 `data` transport wrapper、恢复 `SchedulerNodeParserRuntimeOutput::from_node_parse_output()` 对非空 `data` 的拒绝，以及将 local parser data 回归改回 fail-closed 断言。回归记录：`cargo test parser_output_round_trips_through_task_payload --lib -- --nocapture`；`cargo test parser_succeeds_without_dispatcher_for_local_node_when_output_contains_data --lib -- --nocapture`
- 2026-04-23 / P8-E batch-47：`module.rs` 中 parser scheduler cutover 在“当前 response 已无法解析出有效 target node”时，已不再静默回落到 `ModuleDagProcessor::execute_parse()` 并返回空输出，而是直接 fail-closed 返回错误。该批次把这条 unresolved-target 分支从兼容性空吞行为收口成显式 cutover mismatch，继续与前面几批“bridge 已接管后 malformed target / missing target 必须失败”的方向保持一致。回滚点为恢复 `Module::parser()` 在 `compile_parse_node_dag()` 返回 `None` 时对 `execute_parse()` 的本地回退，以及对应回归中的成功空返回预期。回归记录：`cargo test parser_fails_closed_when_scheduler_cutover_cannot_resolve_target_node --lib -- --nocapture`
- 2026-04-23 / P8-E batch-48：`factory.rs` 中 parser dispatch 与 error envelope 在 task 重建后的 runtime refresh 已重新对齐。此前 `load_parser_seed()` 会从缓存重载 `ModuleConfig` 并刷新 `profile/workflow`，而 `load_error_seed()` 仍停留在旧状态，导致 parser/error fast-path 对 scheduler-ready runtime 状态的准备不一致。当前已把两侧统一收口到同一个 refresh helper，让 error path 在加载 task 后也会补齐 `config/profile/workflow` 刷新并重新绑定 execution metadata。该批次不改变 gray/failure gate fallback 边界，只消除 parser/error 在 factory 装载层面的一个实际不对称点。回滚点为恢复 `load_error_seed()` 的旧实现，移除共用 refresh helper，仅保留 parser dispatch 的 runtime refresh。回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-23 / P8-E batch-49：`module.rs` 中 generate scheduler cutover 在“已有 pending_ctx 但当前 target 已无法解析成有效 scheduler node”时，已不再静默回落到 `ModuleDagProcessor::execute_generate()`。当前这条 unresolved-target 分支会和 parser 侧一样直接 fail-closed 返回错误，避免 bridge 已接管的 generate 路径在 target mismatch 时又掉回旧本地 generate 语义。该批次只收口“已有 pending target 但解析失败”这一类兼容性回退，不改变无 pending_ctx 的初始 entry-node 主路径。回滚点为恢复 `Module::generate()` 在 `compile_generate_node_dag()` 返回 `None` 且 `pending_ctx.is_some()` 时对 `execute_generate()` 的本地回退，以及对应回归中的失败断言。回归记录：`cargo test generate_fails_closed_when_scheduler_cutover_cannot_resolve_target_node --lib -- --nocapture`
- 2026-04-23 / P8-E batch-50：`factory.rs` 中 response-based module loading 已进一步对齐到同一个 runtime refresh helper。此前 `load_module_with_response()` 只修正 `run_id`，不会像 parser/error envelope reconstruction 那样刷新 `config/profile/workflow`，这意味着 parser response 主链在命中 factory cache 时仍可能拿到旧的 scheduler-ready runtime 视图。当前已改为在 response-path 上也复用同一 refresh helper，让 parser response 装载与 parser/error envelope 重建在 runtime 准备语义上保持一致。回滚点为恢复 `load_module_with_response()` 里仅调用 `bind_module_execution()` 的旧实现，移除 response-path 的 refresh helper 调用。回归记录：`cargo test module --lib -- --nocapture`
- 2026-04-24 / P8-E batch-51：scheduler parser bridge 下“节点 `parser()` 自身返回错误”的语义已重新对齐旧本地 `execute_parse()`。此前无论 local 还是 remote placement，只要 bridge 内节点 parser 失败，scheduler 最终都会把它包装成 `RetryExhausted/NodeExecutionFailed` 冒泡给 `Module::parser()`，从而整体 fail-closed；当前 `module_node_dag_adapter.rs` 会为真实 parser 节点错误打上专用标记，`module.rs` 会在 `RetryExhausted` 包装后识别这类错误，并回落到 `ModuleDagProcessor` 统一生成 `stay_current_step=true` 的 same-node error seed。这样 bridge 接管后的 parser 节点错误合同重新与旧本地 parse 对齐，而 dispatcher/transport/bridge 自身失败仍保持 fail-closed。回滚点为移除 parser node error 标记识别、恢复 `Module::parser()` 对这类 bridge 错误的整体失败返回，以及删掉两条 parser-error bridge 回归。回归记录：`cargo test parser_node_error_yields_same_node_error_seed_through_ --lib -- --nocapture`
- 2026-04-24 / P8-E batch-52：补上了一条直接挂 `Module::parser()` scheduler bridge 的多后继 fan-out 回归。当前在自定义多节点 DAG 上，当 parser 节点经 bridge 成功返回空 `NodeParseOutput` 时，`execute_parse_with_preparsed()` 仍会沿用旧处理器语义，为所有 successor 合成 placeholder dispatch，而不会在 bridge 成功后再掉回任何额外兼容路径。该批次主要是把 “fan-out placeholder routing 在 bridge 接管后仍成立” 这条证据上移到 module 级真实入口，而不只停留在 `ModuleDagProcessor` 单元测试。回滚点为删除该 module 级回归，或在后续整张 DAG cutover 重写入口后改写断言。回归记录：`cargo test parser_bridge_preserves_fanout_placeholder_routing --lib -- --nocapture`
- 2026-04-24 / P8-E batch-53：补上了一条 module 级 parser stop-signal 回归。当前当 bridged parser 节点返回 `NodeParseOutput::finish()` 时，`Module::parser()` 仍会把 `finished` 透传成 `TypedParserOutput.stop=true`，说明 scheduler bridge 成功路径下 stop signal 没有在入口层被吞掉。该批次继续把“整张 DAG parser cutover”剩余面的验证证据前移到真实 module 入口，而不只停留在处理器内部。回滚点为删除该 module 级 stop-signal 回归，或在后续入口改造时同步调整断言。回归记录：`cargo test parser_bridge_preserves_stop_signal --lib -- --nocapture`
- 2026-04-24 / P8-E batch-54：补上了一条 module 级 parser advance-gate 回归。当前同一个 `Module` 实例在 scheduler bridge 成功返回空 parser 输出后，若再次对同一节点执行 parse，第二次不会重新合成 placeholder dispatch，说明 `try_mark_node_advanced_once()` 的 per-run gate 没有在 module 入口或 bridge 成功路径中被重置。该批次把“advance gate 仍是一锤子事实”从 `ModuleDagProcessor` 内部验证上移到了真实 `Module::parser()` 入口。回滚点为删除该 module 级 repeated-parse 回归，或在后续调整 gate 语义时同步改写断言。回归记录：`cargo test parser_bridge_preserves_advance_gate_across_repeated_parse_calls --lib -- --nocapture`
- 2026-04-24 / P8-E batch-55：补上了一条 module 级多节点 parser error-recovery 回归。当前当带多个 successor 的 parser 节点在 scheduler bridge 下直接报错时，`Module::parser()` 会返回 same-node retry error seed，且不会误合成任何 successor placeholder dispatch，说明 bridge 的 error-recovery 分支没有旁路掉进 advance gate。该批次把“多节点 DAG 出错时不应继续推进”这条合同也上移到真实 module 入口。回滚点为删除该 module 级 fan-out error-recovery 回归，或在后续调整 error-recovery 语义时同步改写断言。回归记录：`cargo test parser_bridge_error_recovery_does_not_advance_fanout_successors --lib -- --nocapture`
- 2026-04-24 / P8-E batch-56：补上了 remote 多节点 parser bridge 的 module 级证据。当前当入口 parser 节点本身是 remote placement 时，bridge 成功返回空输出后仍会在 `Module::parser()` 入口保留 fan-out placeholder routing；若 remote parser 节点直接报错，则同样返回 same-node retry error seed，且不会误推进 successor。该批次把此前只在本地 placement 上成立的多节点 success/error-recovery 合同，补齐到了 remote placement 组合面，并用 `RecordingRemoteDispatcher` 计数确认这些用例确实经过 remote dispatcher。回滚点为删除这两条 remote 多节点回归，或在后续 remote recovery 语义调整时同步改写断言。回归记录：`cargo test parser_remote_bridge_ --lib -- --nocapture`
- 2026-04-24 / P8-E batch-57：补上了 remote 多节点 parser repeated-parse 的 advance-gate 回归。当前同一个 `Module` 实例若连续两次对 remote placement 的 fan-out parser 节点执行 parse，第二次不会重新合成 placeholder dispatch，但 remote dispatcher 计数会正常递增到 2，说明 remote bridge 成功路径并没有重置 per-run advance gate，只是再次执行了远端节点本身。该批次把此前已在本地 placement 上成立的 repeated-parse gate 合同补齐到了 remote placement 组合面。回滚点为删除这条 remote repeated-parse 回归，或在后续 gate 语义调整时同步改写断言。回归记录：`cargo test parser_remote_bridge_preserves_advance_gate_across_repeated_parse_calls --lib -- --nocapture`
- 2026-04-24 / P8-E batch-58：补上了 remote parser stop-signal 的 module 级证据。当前当 remote placement 的 parser 节点返回 `NodeParseOutput::finish()` 时，`Module::parser()` 仍会透传 `TypedParserOutput.stop=true`，而且 `RecordingRemoteDispatcher` 计数为 1，说明 stop 语义在 remote bridge 成功路径下同样没有被入口层吞掉。该批次把此前仅在本地 placement 上验证过的 stop-signal 合同补齐到了 remote placement 组合面。回滚点为删除这条 remote stop 回归，或在后续 stop/recovery 语义调整时同步改写断言。回归记录：`cargo test parser_remote_bridge_preserves_stop_signal --lib -- --nocapture`
- 2026-04-24 / P8-E batch-59：补上了 run-state resume 对 `profile_version + dag_version + run_id` 三元绑定的恢复侧证据。当前 `ModuleDagProcessor::compile_generate_node_dag()` / `compile_parse_node_dag()` 会把 execution binding 中的 `profile_version` 与 `dag_version` 写入单节点 runtime DAG metadata；`DagRunState` / `DagRunResumeState` 也会持久化这组 runtime identity，`DagScheduler::execute_parallel()` 在 recover 前会将当前 runtime DAG 的 identity 与快照 identity 对比，不一致时直接清掉旧快照并从当前 DAG 重新执行。这样 run 恢复不再会继续复用旧 profile/workflow 下的成功输出，而 identity 匹配时仍可继续 resume。回滚点为移除 runtime DAG metadata 中的 binding 注入、恢复 `DagRunState`/`DagRunResumeState` 仅持久化 outputs 的旧模型，或删掉 recover identity mismatch 的清快照逻辑。回归记录：`cargo test execute_parallel_discards_resume_snapshot_when_runtime_identity_changes --lib -- --nocapture`、`cargo test execute_parallel_resumes_from_run_state_store_snapshot --lib -- --nocapture`、`cargo test compile_generate_node_dag_preserves_target_runtime_settings --lib -- --nocapture`、`cargo test compile_parse_node_dag_preserves_target_runtime_settings --lib -- --nocapture`、`cargo test execute_parse_rejects_mismatched_execution_binding --lib -- --nocapture`
- 2026-04-24 / P8-E batch-60：`engine/api/dlq.rs` 已补上 parser/error 两侧的 DLQ replay 合同回归，并新增一条 parser dispatch 的 delete-by-record-id 回归。当前 `QueueManager::send_to_dlq()` 写入 `NodeDispatchEnvelope` / `NodeErrorEnvelope` 后，`POST /dlq/messages/{id}/requeue` 会继续按原始 source topic 把同一 typed envelope 重新发布；`DELETE /dlq/messages/{id}` 也能在新 envelope 契约下按 backend record id 正确删除。该批次不改动 DLQ handler 或 `QueueManager` 运行时逻辑，只把 parser/error 这两类此前缺失的 replay/delete 证据补齐到与 request/response 对称。回滚点为移除 `dlq.rs` 中新增的 parser/error replay 与 parser delete 三条回归。回归记录：`cargo test dlq_record_can_be_ --lib -- --nocapture`
- 2026-04-24 / P8-E batch-61：`task_model_chain.rs` 已补上两条 attached-bridge 的 scheduler ingress gate-hit 回归：一条固定 parser dispatch 在 gray gate blocked 时会回退到 parser ingress recorder chain，另一条固定 error envelope 在 failure gate blocked 时会回退到 error ingress recorder chain。这样 execute-level 的 scheduler ingress 证据不再只覆盖 `no_bridge` / malformed transport / fallback reason 计量，而是进一步证明“bridge 已挂载但 rollout gate 未放行”时不会误走 scheduler fast-path。该批次仍未尝试补真实 fast-path success 或 remote failure 组合面，因为那一侧会进入 `TaskFactory::create_task_with_modules()` 的 DB 装配链，需要单独的更重夹具。回滚点为移除 `task_model_chain.rs` 中新增的 attached-bridge gate-hit 两条回归。回归记录：`cargo test attached_bridge_falls_back --lib -- --nocapture`
- 2026-04-24 / P8-E batch-62：`task_model_chain.rs` 已补上 attached-bridge + gate-miss 的 scheduler ingress fast-path success execute-level 证据。当前通过 seeded SQLite state 与最小 workflow/module harness，parser dispatch 与 error envelope 在 bridge 已挂载且 gate 放行时，都会经真实 `TaskFactory::create_task_with_modules()` / `ModuleDagOrchestrator` fast-path 发布 `RequestDispatchEnvelope` 到 request queue，而不是回退到 parser/error recorder chain。这样 T21-06 现已同时覆盖 attached-bridge 的 gate-hit fallback 与 gate-miss success 两个 execute-level 面，剩余缺口主要收敛到 remote placement / dispatcher-failed / network-failed 这类真实远端失败矩阵。回滚点为移除 `task_model_chain.rs` 中新增的 seeded SQLite harness、`FastPathSuccessModule` 以及两条 `fast_path_publishes_request_when_gate_allows` 回归。回归记录：`cargo test fast_path_publishes_request_when_gate_allows --lib -- --nocapture`
- 2026-04-24 / P8-E batch-63：`task_model_chain.rs` 已把两条错误的 execute-level “no dispatcher” 远端断言修正为真实的 dispatcher backend failure 回归。当前在 attached bridge + gate 放行的 seeded SQLite harness 下，只要给 parser dispatch / error envelope 注入 remote placement `runtime_node` hint，scheduler ingress 就会经 `TaskFactory::create_task_with_modules()` 取到真实 `RedisRemoteDispatcher`；由于测试 state 只挂本地 `CacheService`、不提供 Redis backend，remote dispatch 会在 `enqueue remote request` 的 Lua 入队阶段显式失败，并由 ingress 统一返回 retryable failure，而不会回退到 parser/error recorder chain，也不会误发布 request。这样 T21-06 的 execute-level 远端失败矩阵已先补齐一类真实 `dispatcher-failed` 面，剩余缺口主要收敛到更接近目标环境的 network/timeout 类远端失败。回滚点为恢复这两条回归对“无 dispatcher”的旧错误假设，或删除 `task_model_chain.rs` 中新增的 `dispatcher_backend_failure_is_retryable_and_fail_closed` 断言。回归记录：`cargo test dispatcher_backend_failure_is_retryable_and_fail_closed --lib -- --nocapture`
- 2026-04-24 / P8-E batch-64：`task_model_chain.rs` 已继续把 T21-06 的 execute-level 远端失败矩阵向前推进到真实 `network-failed` 面。当前在 attached bridge + gate 放行的 seeded SQLite harness 下，测试夹具会给 `AppState.cache_service` 挂一个指向本机关闭端口的 Redis-backed `CacheService`；此时 parser dispatch / error envelope 一旦注入 remote placement `runtime_node` hint，scheduler ingress 仍会走真实 `TaskFactory::create_task_with_modules()` + `RedisRemoteDispatcher` 路径，但会在 `enqueue remote request` 时因不可达 Redis endpoint 显式失败，并统一返回 retryable failure，而不会回退到 parser/error recorder chain，也不会误发布 request。这样 execute-level 基线现在已经分别覆盖了 fallback gate、fast-path success、backend 缺失型 dispatcher failure 与不可达 endpoint 型 network failure，剩余缺口主要收敛到 response-timeout / live-worker 参与的更深远端失败组合面。回滚点为移除 `scheduler_ingress_tests` 中新增的 unreachable-Redis cache helper、seeded harness 变体以及两条 `network_failure_is_retryable_and_fail_closed` 回归。回归记录：`cargo test network_failure_is_retryable_and_fail_closed --lib -- --nocapture`
- 2026-04-25 / P8-E batch-65：`module.rs` 与 `task_model_chain.rs` 已进一步收掉 scheduler bridge 在 missing-profile 场景下的 legacy runtime input fallback。当前不论是 `Module::generate()/parser()` 还是 parser/error scheduler ingress，只要 bridge 已接管但缺少 loaded profile，就会直接 fail-closed 返回错误，而不会再偷偷退回 `build_legacy_generate_runtime_input_with_common(...)` / `build_legacy_parse_runtime_input_with_common(...)` 这类 compatibility builder。为保持 bridge 自测继续走真实 typed path，本批次同时给 `module.rs` 的测试夹具补了最小 profile/workflow 默认值。这样 P8-E 的 cutover 主功能又少掉一层旧 runtime-input 兼容壳。回滚点为恢复 `module.rs` / `task_model_chain.rs` 在 profile 缺失时的 legacy runtime builder fallback，以及恢复 `module.rs` 测试夹具的 `profile=None` 默认状态。回归记录：`cargo test scheduler_generate_runtime_input_errors_when_profile_missing --lib -- --nocapture`、`cargo test scheduler_parse_runtime_input_errors_when_profile_missing --lib -- --nocapture`、`cargo test uses_remote_dispatcher_for_remote_placement --lib -- --nocapture`、`cargo test parser_remote_bridge_preserves_fanout_placeholder_routing --lib -- --nocapture`
- 2026-04-27 / P8-E batch-66：`module.rs` 已继续收掉 `Module::generate()` 在无 pending context 时掉回 `ModuleDagProcessor::execute_generate()` 的旧本地生成路径。当前 entry node 仍由 `compile_generate_node_dag(None)` 解析后走 scheduler bridge；若模块没有可编译 DAG 节点，则直接返回空请求流，不再借 legacy generate context builder 兜底。这样初始 generate 入口也与已接管节点的 fail-closed/typed bridge 语义对齐。回滚点为恢复 `Module::generate()` 中对 `self.processor.execute_generate(...)` 的 fallback 分支。回归记录：`cargo test generate_with_empty_dag_does_not_use_legacy_generate_path --lib -- --nocapture`、`cargo test generate_returns_requests_for_single_node_module --lib -- --nocapture`
- 2026-04-17 / P8-E batch-1：`module.rs` 的 generate/parser scheduler bridge 已改为 profile-first typed runtime input，仅在缺失 profile node config 时回退 `build_legacy_generate_runtime_input` / `build_legacy_parse_runtime_input`；回滚点保持为 `module_node_runtime_bridge.rs` 内 legacy builder；回归记录：`cargo test --lib engine::task::module::tests -- --nocapture` -> `16 passed; 0 failed`
- 2026-04-20 / P8-E batch-2：`module.rs` 的 scheduler runtime input builder 已改为“无 profile 才允许 legacy builder fallback”；一旦 profile 已加载但缺失目标 node config，generate/parser 现在 fail-closed 返回错误，避免 profile/workflow 漂移静默落回 legacy bridge。回滚点为恢复 `build_scheduler_generate_runtime_input` / `build_scheduler_parse_runtime_input` 在 profile 存在时的 legacy fallback；回归记录：`cargo test engine::task::module::tests --lib -- --nocapture`
- 2026-04-20 / P8-E batch-3：`task_model_chain.rs` 的 scheduler ingress runtime input 构造已与 `module.rs` 对齐为 fail-closed 语义；当 task 已加载 profile 但缺失目标 node config 时，parser/error fast-path 现在直接返回 retryable failure，而不是静默退回 legacy runtime builder。回滚点为恢复 `build_scheduler_generate_runtime_input` 在 profile 存在时的 legacy fallback 以及 parser/error 分支中的 `runtime_input_invalid` 处理；回归记录：`cargo test scheduler_ingress --lib -- --nocapture`
- 2026-04-20 / P8-E batch-4：`module_node_runtime_bridge.rs` 已删除不显式传入 common config 的 legacy runtime input wrapper，剩余调用点统一改为 `*_with_common(..., ResolvedCommonConfig::default(), ...)`，把默认 common config 的选择从隐式 helper 收口到调用端。该批次不改变运行时语义，只压缩 bridge API 面和兼容锚点。回滚点为恢复 `build_legacy_generate_runtime_input` / `build_legacy_parse_runtime_input` 两个 wrapper；回归记录：`cargo test module_node_runtime_bridge --lib -- --nocapture`、`cargo test scheduler_ingress --lib -- --nocapture`、`cargo test remote_redis --lib -- --nocapture`
- 2026-04-20 / P8-E batch-5：`node_context_adapter.rs` 已删除不显式传入 common config 的 legacy context wrapper，`module_processor_with_chain.rs` 改为统一调用 `build_legacy_generate_context_with_common` / `build_legacy_parse_context_with_common` 并显式传 `ResolvedCommonConfig::default()`。该批次不改变链式 processor 的运行时语义，只继续压缩 legacy context bridge 的 API 面。回滚点为恢复 `build_legacy_generate_context` / `build_legacy_parse_context` 两个 wrapper；回归记录：`cargo test module_processor_with_chain --lib -- --nocapture`、`cargo test node_context_adapter --lib -- --nocapture`
- 2026-04-20 / P8-E batch-6：`parse_legacy_step_target` 已从 `node_context_adapter.rs` 移出并本地化到 `module_processor_with_chain.rs`，`node_context_adapter` 只再保留仍被多处消费的 legacy parser output/metadata 适配逻辑。该批次不改变链式 parser 的推进语义，只继续缩小 legacy adapter 的导出面。回滚点为恢复 `node_context_adapter.rs` 对 `parse_legacy_step_target` 的导出；回归记录：`cargo test module_processor_with_chain --lib -- --nocapture`、`cargo test node_context_adapter --lib -- --nocapture`
- 2026-04-21 / P8-E batch-7：`legacy_output_from_node_parse_output` 与 `decode_payload_to_metadata` 已从 `node_context_adapter.rs` 移除，并分别本地化到 `module_dag_processor.rs` 与 `module_processor_with_chain.rs` 两个真实消费点；`node_context_adapter` 现在只保留 legacy context/common override 构造职责。该批次不改变 DAG/chain parser 的运行时语义，只继续压缩 shared legacy adapter 的导出面。回滚点为恢复 `node_context_adapter.rs` 对 parser output/metadata helper 的集中导出；回归记录：`cargo test module_dag_processor --lib -- --nocapture`、`cargo test module_processor_with_chain --lib -- --nocapture`、`cargo test node_context_adapter --lib -- --nocapture`
- 2026-04-21 / P8-E batch-8：`module_dag_processor.rs`、`module_processor_with_chain.rs` 与 `module.rs` 已删除本地 parser fast-path 对 `TaskOutputEvent` / `TaskParserEvent` 兼容模型的直接依赖，统一改为返回 typed parser result；`parser_chain.rs` 改为从 typed seed 直接构造 `NodeDispatchEnvelope` / `NodeErrorEnvelope`，把本地 `NodeParseOutput -> 下游 dispatch/error` 的桥接收口到 typed dispatch 语义。该批次保留现有 metadata 兼容回放语义，但结束了本地 DAG/chain parser 运行时继续物化 legacy parser task 的路径。回滚点为恢复 `Module::parser`、`ModuleDagProcessor::execute_parse*`、`ModuleProcessorWithChain::execute_parser` 返回 `TaskOutputEvent`，以及 `parser_chain.rs` 对 `TaskParserEvent` / `TaskErrorEvent` 的直接消费。回归记录：`cargo test module_dag_processor --lib -- --nocapture`、`cargo test module_processor_with_chain --lib -- --nocapture`、`cargo test module --lib -- --nocapture`、`cargo test parser_chain --lib -- --nocapture`、`cargo test parser_error_adapter --lib -- --nocapture`
- 2026-04-20 / P8-C batch-1：`common/model/chain_key.rs` 与 `engine/task/module_processor_with_chain.rs` 已删除 legacy execution key / gate 双轨读写，链式 processor 的 stop / advance / fallback gate 只再使用 canonical `chain:exec:*`、`chain:gate:*` key；回滚策略调整为不再保留 runtime fallback anchor，而是在 cutover 窗口内通过 drain 清理旧 key；回归记录：`cargo test --lib common::model::chain_key::tests -- --nocapture`、`cargo test --lib engine::task::module_processor_with_chain::tests -- --nocapture`
- 2026-04-20 / P8-D batch-1：`schedule/dag/remote_redis.rs` 已删除远端执行 wire 对“缺失 `runtime_input_envelope` 字段”的 legacy JSON 兜底；当前 remote wire 协议要求显式携带该字段，但允许 `null` 表示当前节点无 runtime input。回滚点为恢复 `WireNodeExecutionContext.runtime_input_envelope` 上的 `#[serde(default)]` 与缺字段兼容测试；回归记录：`cargo test --lib wire_context -- --nocapture`

### 15.5.1 后续执行清单（2026-04-16）

目标：在不引入新增兼容层的前提下，完成 Pack 8 收口、压低 fallback 风险，并把 typed runtime model 切换到可灰度、可回滚、可观测的稳定状态。

**Week 1（P0：先关风险口）**

- [x] W1-01 建立 parser/error ingress fallback 观测窗口（按模块输出命中率趋势）
- [x] W1-02 给 fallback 下线定义阈值与开关（含灰度比例和回滚触发条件）
- [x] W1-03 落地 parser/error 最终 typed runtime model cutover 方案文档（含边界与异常语义）
- [x] W1-04 补齐 remote placement + dispatcher 异常矩阵测试（no-dispatcher / dispatcher-failed / network-failed）
- [x] W1-05 固化 `profile_version + dag_version + run_id` 三元绑定校验点（创建、推进、恢复三阶段）

**Week 1 验收标准**

- [x] A1-01 fallback 指标可按 `path + reason + module` 分组查看，并形成日报
- [x] A1-02 parser/error scheduler ingress 回归集在当前分支持续通过
- [x] A1-03 三元绑定校验规则进入实现或至少进入强约束设计评审

当前验收证据：

- 日报脚本 `scripts/dag_alerts_baseline.py` / `scripts/dag_alerts_baseline.ps1` / `scripts/dag_alerts_baseline.sh` 已输出 `Scheduler Ingress Fallback Summary` 与 `Scheduler Ingress Fallback Hotspots By Path/Reason/Module`，并可落地到 `docs/dashboards/dag_alerts_baseline_latest.md` 与 `docs/dashboards/dag_alerts_baseline_latest.json`
- `cargo test --lib engine::chain::task_model_chain::scheduler_ingress_tests -- --nocapture` 已于 2026-04-17 连续执行两次，结果均为 `22 passed; 0 failed`

**Week 2（P1：收口兼容层）**

- [x] W2-01 按 P8-B/P8-E 批次删除 legacy schema/runtime bridge 冗余分支（仅删已观测稳定部分）
- [x] W2-02 保留最小回滚锚点并更新回滚手册（每一批删除都要有对应恢复点）
- [x] W2-03 完成 queue codec 运维手册（明确“变更即滚动重启”的标准流程）
- [x] W2-04 对齐联邦 Download Pool、Queue 原生补偿、统一 CacheService 的优先级决议

**Week 2 验收标准**

- [x] A2-01 legacy 删除批次有清单、有 owner、有回滚点、有测试记录
- [x] A2-02 观测面可区分“cutover 问题”与“业务失败”两类告警
- [x] A2-03 运维手册已覆盖灰度切换与回滚演练步骤

当前验收证据：

- `docs/alerts/dag_alerts_calibration_runbook.md` 已覆盖日报、灰度前 gate 检查、批次 rollback anchor、`scripts/cutover_rehearsal.ps1` / `scripts/cutover_rehearsal.sh` 的执行方式，以及失败后的回滚步骤
- `docs/queue-codec-operations.md` 已明确 `channel_config.queue_codec` 由 `OnceCell` 在启动期固化，变更流程为 `pause -> rolling restart -> resume`
- `cargo test --lib queue::codec::tests -- --nocapture` 已于 2026-04-17 通过，结果为 `2 passed; 0 failed`
- `docs/pack8-priority-decision.md` 已冻结 Pack 8 三项延后能力的排序：`QueueManager native compensation -> Unified CacheService -> Federation Download Pool`，并明确 compensation 先入主 backlog、CacheService 单独成批、Download Pool 暂不进入 Pack 8 关键路径
- `monitoring/prometheus/rules/mocra-pack7-alerts.yml` 已为现有 scheduler/cutover 告警补齐 `category=cutover`，并新增 `category=business_failure` 的 critical/non-retryable 告警；`scripts/dag_alerts_baseline.py` 与 `docs/alerts/dag_alerts_calibration_runbook.md` 已输出并说明 `Cutover Signals` / `Business Failure Signals` 两类分箱
- `src/queue/compensation.rs` 与 `src/queue/manager.rs` 已切换到 queue native compensation 默认热路径：远端 queue backend 默认挂载 `QueueNativeCompensator`，`RedisCompensator` 仅保留为回滚锚点；focused regression：`cargo test --lib queue_native_compensator_tracks_add_and_remove -- --nocapture`、`cargo test --lib remote_queue_defaults_to_native_compensator -- --nocapture`、`cargo test --lib local_queue_without_rollback_anchor_keeps_compensator_disabled -- --nocapture` 均通过
- `src/common/state.rs` 已完成 `Unified CacheService convergence` 的第一批收口：`CacheService`、cookie cache、distributed lock、download/API limiter 与 `StatusTracker` 统一通过内部 `CoordinationServices::build(...)` 装配，不再在 `State::try_new_with_provider()` 内分散拼装；focused regression：`cargo test --lib common::state::tests -- --nocapture` -> `5 passed; 0 failed`
- `src/engine/task/request_response_adapter.rs` 已完成 Federation Download Pool 的第一批 transport 前置：`RequestDispatchEnvelope` / `ResponseDispatchEnvelope` round-trip 时会保留 origin namespace，不再在 response 回投时只能依赖下载节点本地 namespace；focused regression：`cargo test --lib request_response_adapter -- --nocapture` -> `3 passed; 0 failed`
- `src/queue/contract.rs`、`src/queue/kafka.rs`、`src/queue/redis.rs` 已补齐 Federation Download Pool 的第二批底座前置：queue backend 现已支持显式 namespace-qualified topic（如 `origin::response-normal`），Kafka/Redis 的 publish、subscribe、DLQ 解析都会优先使用显式 namespace，但默认本地 namespace 行为保持不变；focused regression：`cargo test --lib explicit_namespace_topic -- --nocapture` -> `3 passed; 0 failed`
- `src/queue/lib.rs`、`src/queue/manager.rs`、`src/engine/chain/download_chain.rs`、`src/engine/chain/stream_chain.rs` 已把上述能力接到 response 出口：`QueuedItem` 可携带 namespace override，`QueueManager` 会按 `(priority, namespace_override)` 分批并向 backend 发出 `origin::response-*`，远端 namespace 响应不会再误走本地 response fast path；focused regression：`cargo test --lib namespace -- --nocapture` -> `8 passed; 0 failed`
- `src/common/model/config.rs`、`src/queue/manager.rs` 已补齐 Federation Download Pool 的 request-side 最小配置入口：`channel_config.federation_request_namespaces` 可显式声明下载池节点应订阅的远端 request namespace，`QueueManager` 会在保留本地 `request-*` 订阅的同时额外订阅 `origin::request-*`；focused regression：`cargo test --lib queue_manager_resolves_remote_request_namespace_subscriptions -- --nocapture`、`cargo test --lib federated_download_pool_subscribes_remote_request_topics -- --nocapture` -> 均 `1 passed; 0 failed`
- `src/downloader/request_downloader.rs` 与 `src/downloader/downloader_manager.rs` 已补齐 response cache 的当前实现缺口：fresh download 现在会把 `Response` 写回当前 `CacheService`，并在 metadata 中显式保留 `response_cache_owner_namespace`、可选 `response_cache_owner_node_id` 与 `response_cache_lookup_key`；cache hit 路径也不再丢失缓存内已有 metadata，且修正了 `prefix_request` 回填错误；focused regression：`cargo test --lib response_cache_write_stores_ownership_metadata -- --nocapture`、`cargo test --lib cache_hit_preserves_cached_response_metadata -- --nocapture`、`cargo test --lib request_downloader -- --nocapture` -> 均通过
- `src/engine/chain/parser_chain.rs` 已把现有 parser-side cache refresh 收口为显式本地 rollback copy：响应回到 parser 命名空间后，会以本 namespace 的 `response_cache_owner_namespace`（以及可选本地 `response_cache_owner_node_id`）重写本地缓存副本，避免把 foreign namespace ownership 直接留在 parser 侧回滚锚点里；focused regression：`cargo test --lib parser_side_cache_refresh -- --nocapture` -> `2 passed; 0 failed`
- `src/common/registry.rs`、`src/engine/engine/processors.rs`、`src/engine/api/debug.rs` 已补齐 remote cache retrieval 的服务端基础：节点心跳会带上可选 `api_port`，受保护 API 已提供 `GET /debug/cache/response/{cache_key}` 本地读取缓存响应体的接口。
- `src/common/model/control_plane_profile.rs`、`src/engine/api/profile_store.rs`、`src/downloader/request_downloader.rs`、`src/downloader/downloader_manager.rs` 已补齐当前批次的 caller-side owner lookup / fallback：downloader 现在会把 `cache_key -> owner_node_id` 轻量索引写入 control-plane store，本 namespace 内的其他节点在 local miss 时会基于 heartbeat 里的 `api_port` 回读 owner 节点的 `/debug/cache/response/{cache_key}`，并把结果回填到本地 cache 后再继续主链路。focused regression：`cargo test --lib response_cache_write_stores_ownership_metadata -- --nocapture`、`cargo test --lib remote_cache_hit_fetches_owner_response_and_warms_local_cache -- --nocapture`、`cargo test --lib remote_cache_lookup_skips_foreign_namespace_owner -- --nocapture` -> 均通过
- 当前仍未进入真正的联邦下载池完整切换：本批次只覆盖 same-namespace owner lookup / remote fallback；foreign namespace owner 仍显式跳过并降级到 live download，且文档里的 `shared-download` 单 topic 方案也尚未实现

**Week 3（P1/P2：演练闭环）**

- [x] W3-01 执行一轮完整 cutover rehearsal（切入 -> 观测 -> 回滚 -> 再切入）
- [x] W3-02 输出版本化 rehearsal 报告（含指标截图、阈值结果、失败处置）
- [x] W3-03 复盘并锁定“可下线 fallback 的模块白名单”
- [x] W3-04 更新架构文档中的“保留项与退出条件”为最新状态

**Week 3 验收标准**

- [x] A3-01 rehearsal 结论可复现，且回滚路径在时限内可完成
- [x] A3-02 至少一批模块满足 fallback 下线条件并可灰度发布
- [x] A3-03 文档、脚本、指标、告警四者口径一致

当前验收证据：

- `target\debug\metrics_node.exe` 已于 2026-04-17 在本地监控配置上验证可启动；为避开 Windows 保留端口区间，本地监控端口已从 `8905` 调整为 `8805`，并同步到 `monitoring/local_engine.toml` 与 `monitoring/prometheus/prometheus.yml`
- `scripts/cutover_rehearsal.ps1` 已于 2026-04-17 以 `ReleaseTag=20260417_local` 完整执行：`pause -> gate -> resume -> rollback gate`，两次 gate 结果均为 `PASSED`
- 版本化演练报告：`docs/dashboards/rehearsal/cutover_rehearsal_20260417_local.md`
- 版本化 gate 摘要：`docs/dashboards/rehearsal/cutover_gate_20260417_local.json`
- 归档基线：`docs/dashboards/archive/dag_alerts_baseline_20260417_local_20260417_025050.md`、`docs/dashboards/archive/dag_alerts_baseline_20260417_local_rollback_20260417_025052.md`
- `target\debug\metrics_node.exe` 现已自带本地 SQLite `base` 任务目录初始化，并注册 `local_probe_module` 单跳候选模块用于本地 fallback-off 白名单验证
- `scripts/local_probe_whitelist_check.ps1` 与 `scripts/local_probe_whitelist_check.sh` 已把候选批次的观测口径改为“dispatch 前后指标增量对比”：要求 dispatch success、DAG scheduler success、downloader success 的增量均至少一次，且 `mocra_scheduler_ingress_total` 与 `mocra_stage_errors_total` 的增量均为 0
- `docs/dashboards/rehearsal/fallback_whitelist_20260417_local.md` 已更新为通过结论：`local_probe_module` 的 `single-hop-baseline` 可作为首批本地 fallback-off whitelist batch
- `docs/alerts/dag_alerts_calibration_runbook.md` 已同步 fallback-off baseline 口径，并保留 multi-hop parser/error 模块单独验证的退出条件

**执行约束（本清单生效期间）**

- [ ] C-01 禁止新增任何 legacy compatibility 分支
- [ ] C-02 所有 cutover 相关改动必须附带 focused regression 或 e2e rehearsal 证据
- [ ] C-03 若出现连续异常，优先触发模块级回滚，不做全局硬切

### 15.5.2 Week 1 开发任务单（文件/函数级）

以下任务单对应 15.5.1 的 Week 1（W1-01 ~ W1-05），用于直接开工实施。

#### W1-01 观测：fallback 命中趋势（按模块）

- [x] 在 `src/engine/chain/task_model_chain.rs` 的 `record_scheduler_ingress_fallback()` 增加 `module` 维度透传（调用点在 parser/error 两条 ingress 分支）
- [x] 对 `try_scheduler_parser_dispatch()` / `try_scheduler_error_dispatch()` 中各类 fallback reason 统一枚举，避免 reason 漂移导致看板碎片化
- [x] 在 `monitoring/prometheus/rules/mocra-pack7-alerts.yml` 增加按 `path + reason + module` 的 fallback 高位告警

建议回归：

- `cargo test --lib engine::chain::task_model_chain::scheduler_ingress_tests -- --nocapture`

#### W1-02 策略：fallback 下线阈值与开关

- [x] 在 `src/engine/task/task_manager.rs`（cutover gate 逻辑）补一个模块级 fallback gate（高位时阻断切入或触发回滚）
- [x] 在 `src/common/model/config.rs` 增加 gate 配置项（阈值、观察窗口、灰度比例）
- [x] 在 `src/engine/api/control.rs`（或 `src/engine/api/debug.rs`）补一个只读接口，输出当前模块 fallback gate 状态

建议回归：

- `cargo test --lib engine::task::task_manager::tests -- --nocapture`
- `cargo test --lib engine::api::control::tests -- --nocapture`

#### W1-03 方案：parser/error 最终 typed runtime model cutover

- [x] 在 `src/engine/chain/task_model_chain.rs` 的 `resolve_parser_*` / `resolve_error_*` 标注并收敛“仅保留最小 seed 回退”的退出条件
- [x] 在 `src/engine/task/parser_error_adapter.rs` 识别并隔离 legacy seed decode 入口（为后续删除做 feature gate）
- [x] 在 `src/engine/task/module_node_runtime_bridge.rs` 明确 generate/parser runtime input 的最终 schema 边界（避免继续扩大 legacy 输入面）

当前收口结论：

- parser/error ingress 的字段解析顺序固定为 `typed context -> transport routing -> minimal seed fallback`，不再允许为了“补全 typed 路径”而主动放大 legacy seed 读取面。
- legacy seed decode 仅作为 cutover 兜底出口保留；当 typed/transport 已经足够定位 `node_id`、`module`、`metadata`、`prefix_request` 时，不再触发 legacy payload 解码。
- scheduler runtime bridge 只接受四类显式 schema：`mocra.scheduler.generate_input`、`mocra.scheduler.parser_input`、`mocra.scheduler.parser_output`、`mocra.scheduler.request_batch`，并要求 `schema + version + content_type` 同时匹配。
- parser runtime output 仍只承载 `next dispatches + finished`；`parsed data` 未进入 bridge schema，若出现该类输出，继续按 invalid payload 处理而不是隐式扩展 schema。
- 异常语义保持 fail-closed：typed bridge schema 不匹配、runtime input 解码失败、remote dispatcher 执行失败，都不通过 legacy 输入面静默兜回旧路径。

建议回归：

- `cargo test --lib engine::chain::task_model_chain::scheduler_ingress_tests -- --nocapture`
- `cargo test --lib engine::task::module_node_runtime_bridge::tests -- --nocapture`

#### W1-04 测试：remote placement + dispatcher 异常矩阵

- [x] 在 `src/engine/task/module.rs` 扩展现有 no-dispatcher 用例，补齐 dispatcher 执行失败与网络失败路径
- [x] 在 `src/engine/chain/task_model_chain.rs` 增加 parser/error ingress 层的 dispatcher-failed 断言（确保 fail-closed）
- [x] 在 `src/schedule/dag/remote_redis.rs`（若已有测试模块）补 wire 层错误映射测试，保证错误可观测且不被吞掉

建议回归：

- `cargo test --lib engine::task::module::tests -- --nocapture`
- `cargo test --lib engine::chain::task_model_chain::scheduler_ingress_tests -- --nocapture`

#### W1-05 一致性：`profile_version + dag_version + run_id` 三元绑定

- [x] 在 `src/engine/task/factory.rs` 和 `src/engine/task/profile_loader.rs` 明确 profile_version 进入 Task/Module 的绑定点
- [x] 在 `src/engine/task/module_dag_orchestrator.rs` 给 DAG 编译结果引入可追踪版本标识（至少是 hash/version 字段）
- [x] 在 `src/engine/task/module_dag_processor.rs` 的恢复/推进关键路径增加 run_id 与版本一致性校验

建议回归：

- `cargo test --lib engine::task::factory::tests -- --nocapture`
- `cargo test --lib engine::task::module_dag_orchestrator::tests -- --nocapture`
- `cargo test --lib engine::task::module_dag_processor::tests -- --nocapture`

#### Week 1 完成判定（执行口径）

- [x] 至少一个 dashboard 面板可按 `path/reason/module` 直接定位 fallback 热点
- [x] parser/error ingress focused 回归在连续两次提交中稳定通过
- [x] 三元绑定规则已有代码约束（不仅停留在文档）
- [x] cutover 与回滚触发条件已能通过接口或日志明确读取

---

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
| `src/queue/redis.rs` | `RedisQueue` | Redis Stream backend（兼容实现） |

### 17.3 分布式协调

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/common/registry.rs` | `NodeRegistry` | 当前 cache/ZSET 节点注册表；目标态迁移到 Raft 状态机 |
| `src/common/status_tracker.rs` | `StatusTracker` | 当前错误/状态跟踪器；目标态迁移到统一状态机 |
| `src/cacheable/cache_service/service.rs` | `CacheService` | KV / TTL / ZSET / script facade |
| `src/cacheable/cache_service/redis_backend.rs` | `RedisBackend` | 当前 Redis cache backend |
| `src/cacheable/cache_service/local_backend.rs` | `LocalBackend` | 单机内存 backend |
| `src/cacheable/cache_service/two_level_backend.rs` | `TwoLevelCacheBackend` | Redis + L1 本地缓存 |
| `src/sync/leader.rs` | `LeaderElector` | 当前 Redis lock soft leader |

### 17.4 执行一致性

| 文件 | 主要类型/函数 | 用途 |
|------|---------------|------|
| `src/common/processors/processor.rs` | `ProcessorResult`, `RetryPolicy`, `ProcessorContext` | 责任链执行输出契约 |
| `src/engine/engine.rs` | `handle_policy_failure`, `handle_policy_retry` | 成功/重试/DLQ 的队列侧效执行 |
| `src/queue/lib.rs` | `AckFn`, `NackFn`, `NackDisposition` | ACK/NACK 语义与重试决策 |
| `src/engine/task/module_dag_processor.rs` | `ModuleDagProcessor` | DAG 节点执行与上下文推进 |
| `src/schedule/dag/scheduler.rs` | `DagScheduler` | 目标态分布式 DAG 调度层：run guard、resume、fencing、dispatch |
| `src/schedule/dag/types.rs` | `DagNodeDispatcher`, `DagRunGuard`, `DagRunStateStore`, `DagFencingStore`, `NodePlacement` | DAG 分布式执行契约与节点放置抽象 |
| `src/schedule/dag/remote_redis.rs` | `RedisRemoteDispatcher` | 现有远程 dispatcher 原型；目标态应替换为 MQ/gRPC/Raft-aware dispatcher |

### 17.5 模型定义

| 文件 | 主要类型 | 说明 |
|------|---------|------|
| `src/common/model/config.rs` | `Config` | 完整配置树（含 Redis 兼容字段与 `RaftConfig`） |
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

> **当前代码说明：** 现有实现已经具备 `Request.enable_response_cache(bool)` / `enable_response_cache_with(&T)`、`Response::sync(hash, cache_service)` 的缓存命中路径，以及 downloader 成功返回后的 cache 写回路径；当前缓存仍是把完整 `Response` 放进当前 `CacheService` backend，并在 metadata 中保留 `response_cache_owner_namespace`、可选 `response_cache_owner_node_id` 与 `response_cache_lookup_key`。parser 侧的历史 cache refresh 现已把回流响应显式落成本 namespace 的 rollback copy，并同步写入 namespace-local 的 `cache_key -> owner` 索引，避免 foreign namespace ownership 直接作为 parser 侧本地缓存锚点。与此同时，节点心跳已开始发布可选 `api_port`，受保护 API 也能按 cache key 读取本地 cached response；当前同 namespace 的 downloader 节点已能通过 control-plane 中的 `cache_key -> owner_node_id` 索引与 `/debug/cache/response/{cache_key}` 回读远端缓存并回填本地 cache，而且回填成功后的本地副本会立即重写为当前 downloader 的本地 owner 并覆盖 owner 索引，使 warmed copy 自身变成可发现的同 namespace fallback anchor。foreign namespace 场景下，当前实现新增了一个最小显式入口：通过 `channel_config.federation_response_cache_api_endpoints` 把 owner namespace 映射到受保护 API base URL，再走同一条 `/debug/cache/response/{cache_key}` 回源；此外，owner 索引现在已经具备首批安全失效路径，会在“self-owned 本地 miss”“同 namespace owner 已不在活跃心跳集”或“远端 owner 返回 404”这类可判定为当前记录失真的负信号上条件清理旧记录，避免索引无限累积陈旧 owner。这仍不是目标态的 shared-download/service-discovery 方案。v2 目标态将把“大 body 存储”从一致性层剥离出来，改为“本地文件 + Raft 索引 + gRPC 回源”。

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

- queue backend 类型切换（in-memory / Redis / Kafka）
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
| Step 5 | 🚧 进行中 | 重做 queue schema、DLQ、blob、topic，统一到目标 envelope；切换前先 drain 或重建旧 topic | codec/topic/batch/DLQ 已完成；`task/request/response/parser/error` topic 均已切到 typed envelope，其中 parser/error 仍处于 bridge 形态，链内兼容 payload 还未完全删除 |
| Step 6 | ✅ 已完成 | 切换 `/config/*`、`/tasks/dispatch`、`/cluster/*`、`/debug/*`、`/control/*` 等目标 API，并同步更新脚本与控制面入口 | 目标 API 路由已在主 `router()` 生效，旧 route 不再是主路径 |
| Step 7 | 🚧 进行中 | 用目标模块重组 `State/Engine`，删除 legacy 路由、旧类型和 Redis 时代主路径依赖 | 装配单入口与 rehearsal 脚本已就位；剩余工作集中在 P8-A/P8-B/P8-E 与生产演练闭环 |

> 当前代码已经完成执行流水线的 transport 层切口：`task/request/response/parser/error` 五段 topic 都已切到 typed envelope，parser/error ingress chain、`TaskManager` / `TaskFactory`、`TaskModelProcessor` 也已直接接收 envelope，并在主路径内部先收口到 seed。  
> 但这还不意味着 `pipeline::processors` 已完成整包切换，因为 parser/error 主链路内部仍在用 legacy event 作为兼容载体。

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

## 21. 当前代码重构进度与 AI 可执行 TODO List

### 21.1 当前验证基线（2026-05-19）

本节只记录**当前代码真实状态**和**仍需交给 AI 实现的剩余代码任务**。旧版 R01-R08 任务卡已删除，不再把已完成项作为待办展开。

已验证通过：

```bash
cargo test --lib fail_closed -- --nocapture
# 22 passed, 0 failed

cargo test cacheable::cache_service --lib -- --nocapture
# 16 passed, 0 failed

cargo test raft_rocksdb --lib -- --nocapture
# 2 passed, 0 failed

cargo test raft_rate_limit --lib -- --nocapture
# 3 passed, 0 failed

cargo test --lib
# 544 passed, 0 failed, 1 ignored

powershell -ExecutionPolicy Bypass -File scripts\check_legacy_hot_path.ps1
# PASS
```

已验证失败：

```bash
powershell -ExecutionPolicy Bypass -File scripts\ci_contract_tests.ps1
# FAIL
```

失败原因不是单项 Rust 测试失败，而是证据脚本自身仍有问题：

1. `scripts/ci_contract_tests.ps1` 在 Windows 上仍调用 `bash scripts/check_legacy_hot_path.sh`，本机 WSL/Hyper-V 未启用时必然失败。
2. `$ErrorActionPreference = "Stop"` 与 `cargo` warning stderr 组合后，PowerShell 把 warning 包装成 `NativeCommandError`，导致 `cargo test --lib` 和 `cargo test raft_rocksdb --lib -- --nocapture` 在 evidence runner 中被记录为失败。
3. `target/mocra-evidence/latest.json` 因上述脚本问题写出 `overall = "fail"`，不能作为当前代码质量通过证据。

### 21.2 当前已完成的重构面

1. `ModuleTrait / ModuleNodeTrait` 已切到 typed context。
2. `TaskProfileSnapshot / ResolvedNodeConfig / TypedEnvelope` 已进入运行时配置模型。
3. `task / request / response / parser_task / error_task` 已具备 typed transport envelope 与 DLQ replay 契约。
4. `Module::generate()` 与 `Module::parser()` 主路径已接 scheduler bridge。
5. parser/error ingress 已有 scheduler fast-path，默认不再回退旧 chain；旧 fallback 只能通过 `legacy_scheduler_ingress_fallback_enabled` 显式开启。
6. `ModuleConfig` 已从 download/parser/stream chain 与 middleware 热路径移出，剩余使用集中在 profile building、legacy adapter 或测试。
7. Response Cache 已具备 owner index、远端 HTTP client 抽象、跨 namespace fallback、owner 清理与本地 warm copy。
8. Queue-native compensator 已有 pending scan / drain 与 crash/replay 幂等测试。
9. `ModuleProcessorWithChain` 已从生产主路径隔离。
10. Raft runtime、ProfileControlPlaneStore、leader view、profile/default/middleware/status/response-cache-owner 读写模型已落地。
11. `ControlPlaneRaftCommand` / `ControlPlaneApply` 已增加 cache KV、TTL、NX、counter、zset、lock 命令模型。
12. `RaftRocksDbCacheBackend` 已实现并接入 `CacheService::new_with_backend_kind()`。
13. `[cache].backend = "raft_rocksdb"` 已进入 `Config` 与 `State` 装配，cache 与 lock 在该模式下不依赖 Redis。
14. `DistributedLockManager` 已支持 `LockBackend`，并有 `RaftLockBackend`。
15. `StatusTrackerThresholdDecisionService` 已按 Redis capability 决定是否走 Lua，避免仅因存在 `LuaScriptRegistry` 就启用 Lua path。
16. `scripts/ci_contract_tests.ps1` / `.sh` 已加入 `raft_rocksdb_backend` evidence 项，但 PowerShell 版仍需修复。

### 21.3 当前真实剩余缺口

1. `RaftRateLimitBackend` 已存在并有单元测试，但没有接入 `DistributedSlidingWindowRateLimiter` 主路径；`State` 在 `raft_rocksdb` 模式下仍调用 `DistributedSlidingWindowRateLimiter::new(None, ...)`，实际落到本地内存限流。
2. cache/lock 的 NX、counter、CAS 返回值当前由 `ProfileControlPlaneStore` 在 submit 前本地读取再提交；单机测试可过，但多节点 Raft 场景下不是强线性化 apply-result 语义。
3. `RaftRocksDbCacheBackend::zremrangebyscore()` 当前提交命令后固定返回 `0`，没有返回真实删除数量。
4. no-redis `raft_rocksdb` 现有测试主要是 State/service smoke，没有覆盖真实 Engine 最小任务执行，也没有证明 rate-limit 主路径写入 Raft store。
5. `scripts/ci_contract_tests.ps1` 不能作为 Windows 原生门禁；它仍调用 bash，且会把 cargo warning 当失败。
6. 当前 `cargo test --lib` 仍有 warnings：`unused_braces`、`unused import: Instant`、`supports_raft_control_plane` dead code、若干 dead field。它们不阻塞测试，但会触发当前 PowerShell evidence runner 的误判。

### 21.4 TODO 任务卡格式

以下每个 TODO 都可以直接交给 AI 编码。执行时应把单张任务卡整体作为 prompt，并要求 AI 先读卡片中的**主要文件**，再实施代码变更。

每张任务卡包含：

- **AI 任务**：直接实施目标。
- **设计目标**：本任务要解决的架构缺口。
- **主要文件**：建议优先修改或新增的文件。
- **实现要求**：必须满足的行为约束。
- **实现步骤**：建议按顺序执行的代码改造步骤。
- **测试设计**：需要新增或扩展的测试场景。
- **验收命令**：完成后必须通过的命令。
- **完成判定**：除测试外的静态或行为检查。
- **可交给 AI 的实现提示**：可直接复制给 AI worker 的任务描述。

---

### 21.5 P0：补齐 Raft-only 主路径正确性

#### T21-N01 接入 `RaftRateLimitBackend` 到 `DistributedSlidingWindowRateLimiter` 主路径

- [x] **AI 任务**：把 `RaftRateLimitBackend` 从孤立测试 backend 接入 `DistributedSlidingWindowRateLimiter` 与 `State` 装配；`[cache].backend = "raft_rocksdb"` 时限流状态必须写入 `ProfileControlPlaneStore`，不能落到本地 `DashMap`。
- **设计目标**：
  - 让 no-redis Raft 模式真正具备跨进程限流状态，而不是单进程本地限流。
  - 保持 Redis/local 旧行为兼容。
  - 让 `raft_rocksdb_cache_lock_rate_limit_status_smoke` 能证明 rate-limit 使用 Raft read model。
- **主要文件**：
  - `src/utils/distributed_rate_limit.rs`
  - `src/common/state.rs`
  - `src/engine/api/profile_store.rs`
- **实现要求**：
  - 新增 `RateLimitBackend` trait 或等价 backend enum，至少覆盖 `verify` / `wait` / `cleanup` 当前主路径需要的状态读写。
  - `DistributedSlidingWindowRateLimiter` 保留现有 public API，但内部在 Redis/local/Raft 三种 backend 间分派。
  - `State::CoordinationServices::build()` 在 `Some(CacheBackendKind::RaftRocksdb)` 时必须构造并注入 `RaftRateLimitBackend`。
  - `api_limiter` 与主 `limiter` 都要按 backend kind 装配，不能只改 downloader limiter。
  - Raft backend key 必须包含 namespace、limiter scope、identifier/bucket，避免主限流与 API 限流互相污染。
- **实现步骤**：
  1. 阅读 `DistributedSlidingWindowRateLimiter` 的 `verify`、`wait`、`cleanup`、`set_all_limit`、`set_key_configs` 当前逻辑，列出哪些状态必须进入 backend。
  2. 提取最小 backend trait。若一次性迁移配置 API 风险过大，第一步至少把 request hit/last-request/window 语义迁到 backend，并保留配置本地缓存。
  3. 将现有 Redis 分支封装为 `RedisRateLimitBackend`，本地分支封装为 `LocalRateLimitBackend`。
  4. 将现有 `RaftRateLimitBackend::hit()` 接入主 limiter，补齐 `cleanup` 与当前 limiter 需要的返回值。
  5. 修改 `State` 装配，`raft_rocksdb` 时注入 Raft backend，Redis pool 必须为 `None`。
  6. 在 State smoke 测试里断言 rate-limit bucket/counter 可从 `ProfileControlPlaneStore` 读到。
- **测试设计**：
  - `cargo test raft_rate_limit --lib -- --nocapture`：保留现有 backend 单测。
  - 新增 `raft_rocksdb_state_uses_raft_rate_limit_backend`：构建 `CoordinationServices` 后调用 limiter，断言 `profile_store.read_cache_counter(...)` 有 Raft bucket。
  - 新增 `raft_rocksdb_api_limiter_uses_raft_backend`：配置 `api.rate_limit` 后验证 API limiter 也写 Raft store。
  - 保留 `distributed_rate_limit` 现有 Redis/local 测试不退化。
- **验收命令**：
  ```bash
  cargo test distributed_rate_limit --lib -- --nocapture
  cargo test raft_rate_limit --lib -- --nocapture
  cargo test raft_rocksdb_state --lib -- --nocapture
  cargo test --lib
  ```
- **完成判定**：
  - `rg "RaftRateLimitBackend" src/common src/utils` 能看到 State 装配和 limiter 主路径使用，不只是在测试里出现。
  - `raft_rocksdb` 模式下 limiter 不写 `local_last_request` 作为主状态。
- **可交给 AI 的实现提示**：
  > 请把 `RaftRateLimitBackend` 接入 `DistributedSlidingWindowRateLimiter` 主路径和 `State` 装配。`raft_rocksdb` 模式下主 limiter 与 api_limiter 都必须写 `ProfileControlPlaneStore`，不能退回本地 DashMap。补充 State 级测试，断言限流 hit 后 Raft/RocksDB read model 中存在对应 counter/bucket。

#### T21-N02 将 cache/lock 的 NX、counter、CAS 返回语义收口到 Raft apply 结果

- [x] **AI 任务**：修复 `submit_cache_set_nx`、`submit_cache_incr_by`、`submit_lock_acquire`、`submit_lock_renew`、`submit_lock_release` 的 submit-before-read / read-before-submit 弱语义，让返回值来自 Raft 串行 apply 后的真实状态。
- **设计目标**：
  - 避免多节点并发时两个 caller 都在 submit 前读到旧状态，从而错误返回成功。
  - 让 counter 返回值代表 Raft log apply 后的新值，而不是本地读出的预测值。
  - 让 lock acquire/renew/release 的 owner 校验在状态机串行执行中完成。
- **主要文件**：
  - `src/common/model/control_plane_profile.rs`
  - `src/engine/api/profile_store.rs`
  - `src/sync/raft.rs`
  - `src/utils/redis_lock.rs`
  - `src/cacheable/cache_service/raft_backend.rs`
- **实现要求**：
  - `CacheSetNx` 的“key 不存在或已过期才写入”必须在 apply 侧判断，不允许只在 submit 前判断。
  - `CacheIncrBy` command 字段应表达 delta，而不是把 submit 前计算出的 new value 塞进 `delta`。
  - lock acquire/renew/release 的 owner 与 TTL 校验必须在 apply 侧判断。
  - `ProfileControlPlaneStore` 需要提供可读取 apply outcome 的 helper。可以先用 request-id outcome record，例如 `{namespace}:control:outcome:{request_id}`，再由 helper submit 后读取。
  - follower 转发场景下 helper 不得返回本地预测成功；无法确认 outcome 时应返回明确 error 或 retryable 状态。
- **实现步骤**：
  1. 为需要返回值的 command 增加 `request_id`，并新增 `ControlPlaneApply::CommandOutcomeUpsert` 或等价 outcome read model。
  2. 在 `apply_model_to_batch()` 内执行条件判断，并写 outcome：`applied=true/false`、`value`、`reason`。
  3. 修改 `submit_cache_set_nx()`、`submit_cache_incr_by()`、`submit_lock_*()`：submit 后读取 outcome，而不是 submit 前直接返回预测值。
  4. 调整 `ControlPlaneRaftCommand::CacheIncrBy` 语义，确保 apply 侧用当前 counter + delta 得到新值。
  5. 补并发测试，至少用 `tokio::join!` 在同一 store 上模拟两个 `set_nx` / `lock_acquire` 并发调用。
  6. 保持现有 local apply 单测通过。
- **测试设计**：
  - `cache_set_nx_concurrent_only_one_success`
  - `cache_incr_by_returns_apply_ordered_values`
  - `raft_lock_concurrent_acquire_only_one_owner`
  - `raft_lock_renew_and_release_wrong_owner_return_false_from_apply`
  - `follower_or_forwarding_without_outcome_is_retryable_or_error`
- **验收命令**：
  ```bash
  cargo test engine::api::profile_store --lib -- --nocapture
  cargo test raft_lock --lib -- --nocapture
  cargo test raft_rocksdb_cache --lib -- --nocapture
  cargo test sync::raft --lib -- --nocapture
  cargo test --lib
  ```
- **完成判定**：
  - `submit_cache_set_nx`、`submit_cache_incr_by`、`submit_lock_*` 中不再以 submit 前本地 read 作为最终成功判定。
  - 新增 tests 能证明并发冲突只有一个成功。
- **可交给 AI 的实现提示**：
  > 请把 Raft cache/lock 的条件写入和返回值改成 apply-result 语义。新增 command outcome read model，`set_nx`、`incr_by`、`lock_acquire/renew/release` 都要在状态机 apply 侧判断并写 outcome，helper submit 后读取 outcome 返回。补充并发冲突测试，确保多 caller 下只有一个 NX/lock acquire 成功。

#### T21-N03 修复 Raft zset 删除返回值与索引一致性

- [x] **AI 任务**：修复 `RaftRocksDbCacheBackend::zremrangebyscore()` 固定返回 `0` 的问题，并确保 `CacheZRem` / `CacheZRemRangeByScore` 同步清理 zset entry 与 member index。
- **设计目标**：
  - 让 Raft backend 的 zset 行为与 Redis backend 的返回语义一致。
  - 避免删除 range 后 `zset_idx` 或 reverse index 残留，影响后续 `zrem(member)` 或重复 `zadd`。
- **主要文件**：
  - `src/common/model/control_plane_profile.rs`
  - `src/engine/api/profile_store.rs`
  - `src/cacheable/cache_service/raft_backend.rs`
- **实现要求**：
  - `zremrangebyscore()` 返回实际删除 entry 数。
  - range delete 必须同时删除 score entry 和 member index。
  - `zadd` 同一 member 新 score 时，旧 score entry 必须被删除或覆盖，不得出现同一 member 多个 score entry。
  - 若继续使用 `hex(member)` 作为 index key，应补二进制 member 测试。
- **实现步骤**：
  1. 增加 zset member index read helper：由 `{namespace}:cache:zset_idx:{zset_key}:{member_hex}` 找到旧 score key。
  2. 修改 `CacheZAdd` apply：写新 score 前删除同 member 的旧 score entry。
  3. 修改 `CacheZRemRangeByScore` apply：统计删除数量，并删除对应 member index。
  4. 通过 T21-N02 的 outcome 机制或新增 delete-count helper 让 backend 返回真实删除数。
  5. 补 `zadd_same_member_updates_score`、`zremrangebyscore_returns_count_and_cleans_index` 测试。
- **测试设计**：
  - 同 member 先以 score=1 写入，再以 score=10 写入，`zrangebyscore(0, 5)` 不应返回该 member。
  - range delete 删除两个 member，backend 返回 `2`。
  - range delete 后再 `zadd` 同 member，不受旧 index 影响。
  - member 使用非 UTF-8 bytes。
- **验收命令**：
  ```bash
  cargo test raft_backend_zset --lib -- --nocapture
  cargo test cache_zset --lib -- --nocapture
  cargo test cacheable::cache_service --lib -- --nocapture
  ```
- **完成判定**：
  - `RaftRocksDbCacheBackend::zremrangebyscore()` 不再 `.map(|_| 0)`。
  - 删除后 RocksDB 中不存在对应 zset entry 和 index 残留。
- **可交给 AI 的实现提示**：
  > 请修复 Raft zset 删除语义：`zremrangebyscore` 返回真实删除数量，range delete 同步清理 member index，`zadd` 同一 member 新 score 时删除旧 score entry。补二进制 member、重复 zadd、删除数量和索引清理测试。

---

### 21.6 P1：集成验收与证据门禁

#### T21-N04 增加 no-redis `raft_rocksdb` Engine 最小执行集成测试

- [x] **AI 任务**：在现有 State smoke 基础上增加真实 Engine 最小任务执行测试，证明 no-redis `raft_rocksdb` 模式不只服务可构建，而且能跑通最小 generate/parser 主路径。
- **设计目标**：
  - 让生产前验收覆盖 Engine 层，而不是只覆盖 `CoordinationServices::build()`。
  - 同时证明 cache、lock、rate-limit、status tracker 在同一个 Engine 运行上下文中可用。
- **主要文件**：
  - `src/common/state.rs`
  - `src/engine/engine.rs`
  - `src/engine/engine/tests.rs` 或现有 engine test module
  - `src/engine/task/module.rs`
- **实现要求**：
  - 测试配置不得包含 Redis。
  - 使用 `[cache].backend = "raft_rocksdb"`。
  - 使用临时 data dir / 单节点 Raft 或 local apply store，不能依赖固定端口。
  - 注册最小 module / node，至少执行一次 generate 或 parser scheduler bridge。
  - 执行后断言 control-plane store 中存在 cache、lock、rate-limit、status 中至少三类状态；rate-limit 必须是 Raft backend 写入，不是本地 `DashMap`。
- **实现步骤**：
  1. 复用 `common::state::tests::sample_config()` 或现有 engine fixture。
  2. 构造 no-redis `State`，启用 `CacheBackendKind::RaftRocksdb`。
  3. 初始化 Engine，并注册最小 workflow/module。
  4. 触发最小 generate/parser 路径。
  5. 断言无 Redis pool，且关键状态进入 `ProfileControlPlaneStore`。
- **测试设计**：
  - `raft_rocksdb_engine_minimal_generate_smoke`
  - `raft_rocksdb_engine_minimal_parser_smoke`
  - `raft_rocksdb_engine_no_redis_pool_and_raft_rate_limit_state`
- **验收命令**：
  ```bash
  cargo test raft_rocksdb_engine --lib -- --nocapture
  cargo test raft_rocksdb --lib -- --nocapture
  cargo test --lib
  ```
- **完成判定**：
  - `cargo test raft_rocksdb --lib -- --nocapture` 不再只跑 State 级测试。
  - 测试断言 rate-limit 状态来自 Raft/RocksDB。
- **可交给 AI 的实现提示**：
  > 请新增 no-redis `raft_rocksdb` Engine 最小执行测试。复用现有 State/Engine fixture，配置 `CacheBackendKind::RaftRocksdb` 且无 Redis，跑通最小 generate/parser 路径，并断言 cache/lock/rate-limit/status 状态写入 `ProfileControlPlaneStore`。

#### T21-N05 修复 Windows PowerShell CI evidence gate

- [x] **AI 任务**：修复 `scripts/ci_contract_tests.ps1`，让 Windows 原生 evidence gate 不依赖 bash，且不会因为 cargo warning stderr 把通过的测试误判为失败。
- **设计目标**：
  - `powershell -ExecutionPolicy Bypass -File scripts\ci_contract_tests.ps1` 必须在 Windows 上可作为真实门禁。
  - `target/mocra-evidence/latest.json` 的 `overall` 必须可靠反映命令 exit code。
- **主要文件**：
  - `scripts/ci_contract_tests.ps1`
  - `scripts/check_legacy_hot_path.ps1`
  - `scripts/ci_contract_tests.sh`
- **实现要求**：
  - PowerShell 版 legacy check 必须调用 `scripts/check_legacy_hot_path.ps1`，不能调用 `.sh` 或 `bash`。
  - evidence runner 必须捕获 stdout/stderr 到文件，但只以 native process exit code 判定成功失败。
  - `cargo` warning 可以写入 evidence txt，但不能让 `Invoke-EvidenceTest` 进入 catch 并误判。
  - `summary.json` 需要包含每个 evidence item 的名称、命令、状态、exit code、日志路径，而不只是 passed/failed 总数。
  - Bash 版继续用于 Linux/macOS，不影响 PowerShell 原生路径。
- **实现步骤**：
  1. 重写 `Invoke-EvidenceTest`，使用 `Start-Process` 或 `&` 调用后显式读取 `$LASTEXITCODE`，避免 `$ErrorActionPreference=Stop` 捕获 native stderr。
  2. 将 `check_legacy` command 改为 `powershell -ExecutionPolicy Bypass -File scripts\check_legacy_hot_path.ps1`。
  3. 写 structured items 到 `summary.json`。
  4. 保留 `raft_rocksdb_backend` item。
  5. 运行脚本并检查 `latest.json`。
- **测试设计**：
  - 正常运行时 overall 为 pass。
  - 临时用不存在的 cargo filter 或 dry-run mock 验证失败命令会让 overall fail。
  - 确认 evidence txt 包含 warning 但 passFile 为 true。
- **验收命令**：
  ```bash
  powershell -ExecutionPolicy Bypass -File scripts\ci_contract_tests.ps1
  powershell -ExecutionPolicy Bypass -File scripts\check_legacy_hot_path.ps1
  ```
- **完成判定**：
  - Windows 无 WSL 环境下 `ci_contract_tests.ps1` 成功。
  - `target/mocra-evidence/latest.json` 包含 `raft_rocksdb_backend` 且 `overall = "pass"`。
- **可交给 AI 的实现提示**：
  > 请修复 `scripts/ci_contract_tests.ps1`：不要调用 bash，legacy check 使用 PowerShell 脚本；重写 evidence runner，使 cargo warning stderr 不触发误判，只按 exit code 判定。summary.json 输出每个 evidence item 的 name/command/status/exit_code/log_path，并验证 latest.json overall pass。

#### T21-N06 清理当前 lib test warnings，避免 evidence runner 与 CI 噪音

- [x] **AI 任务**：清理 `cargo test --lib` 当前 warnings，尤其是会污染 evidence 的 native stderr 输出。
- **设计目标**：
  - 让 `cargo test --lib` 输出更稳定，降低 CI 脚本误判概率。
  - 让新增 capability API 要么被使用，要么移除死代码。
- **主要文件**：
  - `src/common/model/cookies.rs`
  - `src/engine/chain/parser_chain.rs`
  - `src/cacheable/cache_service/backend.rs`
  - `src/cacheable/cache_service/service.rs`
  - `src/common/state.rs`
  - `src/engine/chain/stream_chain.rs`
  - `src/engine/task/module_dag_processor.rs`
- **实现要求**：
  - 移除 `cookies.rs` 中 unnecessary braces。
  - 移除 `parser_chain.rs` 未使用的 `Instant` import。
  - `supports_raft_control_plane` 如果保留，必须通过 `CacheService` / `State` 暴露并在测试中使用；否则从 trait 删除。
  - 对确实暂时保留的 dead fields，使用更局部的 allow 或补真实读取，不能全 crate 粗暴关闭 warning。
- **实现步骤**：
  1. 跑 `cargo test --lib` 记录 warning 列表。
  2. 逐项做最小修改。
  3. 对 capability helper 补测试或删掉。
  4. 再跑 `cargo test --lib`，确认没有上述 warnings。
- **测试设计**：
  - 不需要新增业务测试，除非保留 `supports_raft_control_plane` 时需要补 capability helper 测试。
- **验收命令**：
  ```bash
  cargo test --lib
  powershell -ExecutionPolicy Bypass -File scripts\ci_contract_tests.ps1
  ```
- **完成判定**：
  - `cargo test --lib` 不再输出本节列出的 warnings。
  - PowerShell evidence gate 不再因为 warning stderr 失败。
- **可交给 AI 的实现提示**：
  > 请清理当前 `cargo test --lib` warnings：去掉 `cookies.rs` 多余 braces、`parser_chain.rs` 未用 import，并处理 `supports_raft_control_plane` dead code。只做最小修改，不改业务行为。完成后 `cargo test --lib` 应无这些 warnings。

### 21.7 Go/No-Go 代码验收门槛

进入生产灰度前，必须全部满足：

- [x] `T21-N01` 至 `T21-N06` 全部完成（N01 ✅ N02 ✅ N03 ✅ N04 ✅ N05 ✅ N06 ✅），551 passed, 0 failed, 1 ignored。
- [x] `raft_rocksdb` 模式下 cache、lock、rate-limit、status tracker 都通过 Raft/RocksDB 主路径，不依赖 Redis，也不退回本地内存作为分布式状态。
- [x] cache/lock 的 NX、counter、CAS 返回语义来自 Raft apply outcome，而不是 submit 前本地预测。
- [x] `RaftRocksDbCacheBackend` zset 删除返回值与索引清理行为和 Redis backend 对齐。
- [x] no-redis `raft_rocksdb` Engine 最小执行测试通过。
- [x] 固定证据命令通过：
  ```bash
  cargo test fail_closed --lib -- --nocapture
  cargo test cacheable::cache_service --lib -- --nocapture
  cargo test raft_rocksdb --lib -- --nocapture
  cargo test raft_rate_limit --lib -- --nocapture
  cargo test --lib
  powershell -ExecutionPolicy Bypass -File scripts\check_legacy_hot_path.ps1
  powershell -ExecutionPolicy Bypass -File scripts\ci_contract_tests.ps1
  ```

*文档最后更新：2026-05-20。Section 21 全部完成。T21-N01-N06 ✅，Section 21.7 Go/No-Go ✅。551 passed, 0 failed, 1 ignored。所有分布式状态（cache/lock/rate-limit/suspension/status）均通过 Raft/RocksDB 主路径，config 配置本地缓存。*
