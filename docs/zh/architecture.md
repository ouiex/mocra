# 系统架构

本文档描述 mocra 的内部架构。

## 概述

mocra 是一个面向 Rust 的**分布式、事件驱动的爬取与数据采集框架**。它将数据采集建模为由队列驱动的多阶段流水线，并通过 DAG 引擎编排模块执行。

```
┌─────────────────────────────────────────────────────────────┐
│                           引擎                              │
│                                                             │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌─────────┐ │
│  │  任务     │──▶│  请求    │──▶│  响应    │──▶│  解析   │ │
│  │  队列     │   │  队列    │   │  队列    │   │  队列   │ │
│  └──────────┘   └──────────┘   └──────────┘   └────┬────┘ │
│       ▲                                             │      │
│       │         ┌──────────┐                        │      │
│       └─────────│ 解析任务 │◀───────────────────────┘      │
│                 │ 队列     │                               │
│                 └──────────┘                               │
│                 ┌──────────┐                               │
│                 │ 错误任务 │◀── (失败时)                    │
│                 │ 队列     │                               │
│                 └──────────┘                               │
└─────────────────────────────────────────────────────────────┘
```

## 核心组件

### State（状态）

`State` 是**运行时组合根**。它加载 TOML 配置并初始化所有共享服务：

- **数据库** 连接（PostgreSQL / SQLite，基于 SeaORM）
- **CacheService** 缓存服务（内存或 Redis）
- **DistributedLockManager** 分布式锁管理器
- **QueueManager** 队列管理器
- **StatusTracker** 状态追踪器（错误阈值管理）
- **ProxyPool** 代理池

运行模式**自动检测** — 无需手动切换：
- 配置了 `cache.redis` → **分布式模式**
- 未配置 `cache.redis` → **单节点模式**

### Engine（引擎）

`Engine` 是主编排器，负责将以下组件组装在一起：

- **TaskManager** — 模块注册、任务创建、DAG 编译
- **QueueManager** — 队列订阅与发布
- **ProcessorRunner** — 有界并发工作循环
- **CronScheduler** — 基于 cron 的周期性任务调度
- **DownloaderManager** — HTTP/WebSocket 客户端池
- **MiddlewareManager** — 下载、数据、存储中间件链
- **API 服务器** — 基于 Axum 的控制面板
- **Metrics** — Prometheus 指标导出器

### 处理流水线

流水线完全由队列驱动，每个阶段通过消息队列解耦：

```
TaskEvent ──▶ [任务队列]
                  │
                  ▼
            Module.generate()  ──▶  Stream<Request>
                                        │
                                        ▼
                                  [请求队列]
                                        │
                                        ▼
                              Downloader.download()  ──▶  Response
                                                            │
                                                            ▼
                                                      [响应队列]
                                                            │
                                                            ▼
                                                    Module.parser()  ──▶  TaskOutputEvent
                                                                              │
                                              ┌───────────────┬───────────────┤
                                              ▼               ▼               ▼
                                        [数据存储]    [解析任务队列]    [错误任务队列]
                                                            │               │
                                                            ▼               ▼
                                                      (下一节点)     (重试 / DLQ)
```

**阶段详情：**

| 阶段 | 输入 | 输出 | 说明 |
|---|---|---|---|
| **任务** | `TaskEvent` | `Request` 流 | 解析模块，调用当前 DAG 节点的 `generate()` |
| **下载** | `Request` | `Response` | HTTP/WebSocket 抓取，支持代理、限速、会话同步 |
| **解析** | `Response` | `TaskOutputEvent` | 调用 `parser()`，将结果路由到数据存储、下游任务或错误队列 |
| **错误** | `TaskErrorEvent` | 重试或终止 | 基于阈值的重试与退避，模块/任务级终止 |

### ProcessorRunner（处理器运行器）

每个流水线阶段以 `ProcessorRunner` 运行 — 一个带有以下特性的工作循环：

- **有界并发** — 基于信号量（Semaphore）
- **暂停/恢复** — 通过 `watch` 通道
- **优雅关闭** — 通过 `broadcast` 通道
- **批量接收** — 每次迭代最多 100 条消息以提升吞吐

### QueueManager（队列管理器）

`QueueManager` 将本地 Tokio 通道桥接到可选的远程后端：

| 后端 | 使用场景 | 说明 |
|---|---|---|
| **本地（Tokio mpsc）** | 始终可用 | 进程内通道，适用于单节点 |
| **Redis Streams** | 配置 `channel_config.redis` | 分布式队列，支持消费者组、分片、认领恢复 |
| **Kafka** | 配置 `channel_config.kafka` | Kafka 主题，支持消费者组 |

特性：
- **按优先级分主题** — 不同优先级使用独立通道
- **DLQ / NACK** — 可配置重试次数和退避策略，超过后进入死信队列
- **压缩** — 超过阈值时使用 zstd 压缩
- **Blob 卸载** — 大载荷卸载到本地 blob 存储
- **编解码** — MsgPack（默认）或 JSON 用于远程队列编码

### 模块系统

模块是面向用户的抽象层。模块定义**抓取什么**以及**如何解析**：

```
ModuleTrait                          ModuleNodeTrait
┌───────────────────┐                ┌───────────────────┐
│ name()            │                │ generate()        │
│ version()         │                │   → Stream<Req>   │
│ should_login()    │                │                   │
│ add_step()        │──── 返回 ────▶ │ parser()          │
│ dag_definition()  │   Vec<Node>    │   → TaskOutput    │
│ pre_process()     │                │                   │
│ post_process()    │                │ retryable()       │
│ cron()            │                └───────────────────┘
└───────────────────┘
```

每个 `ModuleNodeTrait` 代表 DAG 中的一个节点，包含两个操作：
- **`generate()`** — 产生 HTTP 请求流
- **`parser()`** — 处理响应并返回数据 + 下游任务

### DAG 执行

每个模块在注册时被编译为 DAG：

- **`add_step()`** 返回线性链：`step_0 → step_1 → step_2`
- **`dag_definition()`** 返回自定义图，支持扇出/汇合
- 两者同时存在时会被**合并**（线性节点加 `legacy_` 前缀）

DAG 拓扑是**静态的**（初始化时确定）。每个节点的执行次数是**动态的** — 由流经队列的 `TaskParserEvent` 数量驱动。

详见 [DAG 执行指南](dag-guide.md)。

### 中间件管道

三层中间件，按 `weight()` 排序（值越小越先执行）：

| 层级 | Trait | 钩子点 |
|---|---|---|
| **下载** | `DownloadMiddleware` | `before_request()`, `after_response()` |
| **数据** | `DataMiddleware` | `handle_data()` |
| **存储** | `DataStoreMiddleware` | `before_store()`, `store_data()`, `after_store()` |

中间件实例以 `Arc<Mutex<Box<dyn ...>>>` 形式共享。

详见 [中间件指南](middleware-guide.md)。

### 错误处理

错误在三个层级处理：

| 层级 | 机制 | 说明 |
|---|---|---|
| **生成失败** | 一次性缓存回退 | 非入口节点通过 Redis 缓存的 `prefix_request` 回退到上一次请求 |
| **解析失败** | `TaskErrorEvent` + 错误队列 | 发出 `stay_current_step: true` 错误事件，在同一节点重试 |
| **阈值超限** | `StatusTracker` | 追踪每个请求/模块/任务的错误计数，决定重试、跳过或终止 |

### Cron 调度

模块可通过 `ModuleTrait::cron()` 声明 cron 调度。`CronScheduler`：
- 按可配置的刷新间隔评估 cron 表达式
- 支持误触发容忍
- 触发时向任务队列注入 `TaskEvent`

### 控制面板 API

内置的 Axum HTTP 服务器（通过 `[api]` 配置）：

| 路由 | 方法 | 认证 | 说明 |
|---|---|---|---|
| `/health` | GET | 否 | 健康检查 |
| `/metrics` | GET | 限速 | Prometheus 指标 |
| `/start_work` | POST | API Key | 手动注入任务 |
| `/nodes` | GET | API Key | 列出活跃集群节点 |
| `/dlq` | GET | API Key | 查看死信队列 |
| `/control/pause` | POST | API Key | 暂停全局引擎 |
| `/control/resume` | POST | API Key | 恢复全局引擎 |
