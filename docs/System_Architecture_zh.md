> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Mocra System Architecture

## 1. 架构概览 (High-Level Architecture)

Mocra 是一个**分布式、事件驱动的爬虫、采集与数据处理系统**，旨在高效、弹性地处理大规模数据抓取和处理任务。系统采用 **Rust** 编写，核心设计理念是**模块化**、**状态分离**和**分布式**。

从宏观上看，系统由多个无状态的 Worker 节点组成，这些节点通过消息队列（Redis/Kafka）进行通信，并共享外部存储（Redis, Postgres）。

*   **控制面 (Control Plane)**: 通过 API 管理节点、暂停/恢复任务、查看指标。
*   **计算面 (Data Plane)**: Worker 节点运行 Engine，通过 Pipeline 模式处理任务。
*   **存储面 (Storage Plane)**:
    *   **Postgres**: 持久化任务状态、配置、业务数据。
    *   **Redis**: 重度依赖组件，用于队列、缓存、分布式锁、选主、节点注册、去重等。
    *   **Kafka (可选)**: 用于高吞吐量的消息传递。

---

## 2. 详细组件设计 (Component Deep Dive)

系统采用 Rust Workspace 组织，包含多个功能独立的 Crate：

### 2.1 Engine (核心引擎) `engine`
**Engine 是系统的大脑**，负责编排整个爬取和处理流程。它不直接执行 HTTP 请求或数据库操作，而是协调各个 Processor 工作。

*   **核心 Pipeline**: 采用经典的 Scrapy 风格的 Pipeline 设计，但完全异步且分布式。
*   **关键组件**:
    *   **EventBus**: 集中处理系统事件（启动、健康检查、指标等），支持日志和监控插件。
    *   **Scheduler**: 分布式调度器 (`RunLoopProcessor`)，支持 Cron 定时任务，利用 Leader Election 保证只在单一节点触发。
    *   **MiddlewareManager**: 管理下载中间件（签名、代理）、数据中间件（清洗）和存储中间件。
    *   **Zombie Killer**: 监控并清理卡死的任务。

### 2.2 Common (公共基础) `common`
**Common 是系统的骨架**，定义了所有模块必须遵循的契约。

*   **Global State (`State`)**: 单例上下文对象，持有 DB 连接池、Redis 连接、配置对象、分布式限流器等，通过依赖注入传递给所有组件。
*   **Registry**: 服务注册与发现。每个节点启动时都会在 Redis 中注册（附带 UUID 和元数据），并定期发送心跳。

### 2.3 Queue (消息队列抽象) `queue`
**Queue 是系统的神经网络**，负责组件间的异步通信。支持 Redis Streams 和 Kafka，提供统一的 `MqBackend` 接口和各业务 Topic (TopicType: Task, Request, Response, ParserTask, Error)。

### 2.4 Downloader (下载器) `downloader`
**Downloader 是系统的触手**，负责实际的网络交互。支持 HTTP/1.1, H2 和 WebSocket。内置了连接复用、Redis 缓存（Gzip压缩）、分布式限流。

### 2.5 JS-V8 (动态执行环境) `js-v8`
**JS-V8 是系统的扩展插件**，通过 Worker Pool 模型在多线程 Actor 中运行 V8 Isolate，用于执行复杂的 JS 逻辑（如反爬签名）。

### 2.6 Sync (分布式协同) `sync`
**Sync 是系统的协调者**，解决分布式一致性问题。提供 Leader Election 和 DistributedSync（基于 CAS 和 Pub/Sub 的共享状态同步）。

### 2.7 Cacheable (统一缓存) `cacheable`
**Cacheable 是系统的记忆**。提供 DashMap（本地）和 Redis（分布式）的统一接口。

---

## 3. 核心数据结构与生命周期 (Core Data Structures & Lifecycle)

### 3.1 TaskModel (`common/src/model/message.rs`)
**定义**: 整个抓取任务的顶层描述，由调度器或 API 生成。
*   **关键字段**: `account`, `platform`, `module` (可选列表), `priority`, `run_id`.
*   **生命周期**:
    1.  **Creation**: `CronScheduler` 触发或 用户 API `POST /task`。
    2.  **Queue**: 推送到 `Topic: Task`。
    3.  **Consumption**: `TaskModelProcessor` 消费。
    4.  **Transformation**: 加载对应 Module，调用 `Module.generate()` 生成 `Request`。

### 3.2 Request (`common/src/model/request.rs`)
**定义**: 表示一个具体的 HTTP/WebSocket 请求。
*   **关键字段**: `url`, `method`, `headers`, `cookies`, `meta`, `limit_id`, `context` (执行标记), `run_id`.
*   **生命周期**:
    1.  **Creation**: 由 `TaskModelProcessor` (调用 `Module.generate`) 或 `ParserProcessor` (翻页/子请求) 生成。
    2.  **Middleware**: 经过 `RequestMiddleware` 链（签名、Cookie注入等）。
    3.  **Queue**: 推送到 `Topic: Request`。
    4.  **Consumption**: `DownloadProcessor` 消费。
    5.  **Execution**: 转换为网络流，发送至目标服务器。

### 3.3 Response (`common/src/model/response.rs`)
**定义**: 网络请求的原始返回结果，尚未解析。
*   **关键字段**: `status_code`, `content` (Bytes), `headers`, `context`, `run_id`, `request_hash`.
*   **生命周期**:
    1.  **Creation**: 由 `DownloadProcessor` 在网络请求完成后生成。
    2.  **Queue**: 推送到 `Topic: Response`。
    3.  **Consumption**: `ResponseModuleProcessor` 消费。
    4.  **Parsing**: 传递给 `Module.parser()` 进行解析。

### 3.4 ParserTaskModel (`common/src/model/message.rs`)
**定义**: 解析过程中产生的需要“递归”处理或状态保持的中间态任务。通常用于多步流程（如 Step 1 -> Step 2）或复杂的分页。
*   **关键字段**: `account_task`, `metadata` (携带上下文数据), `context` (`ExecutionMark`), `prefix_request`.
*   **生命周期**:
    1.  **Creation**: `Module.parser()` 返回 `ExecuteResult` 中包含 `parser_task`。
    2.  **Queue**: 推送到 `Topic: ParserTask`。
    3.  **Consumption**: `TaskModelProcessor` (或专门的 `ParserTaskProcessor`) 消费，通常作为带有特定 Context 的新 `Task` 处理，重新触发生成逻辑或特定步骤逻辑。

### 3.5 Data (`common/src/model/data.rs`)
**定义**: 最终提取的业务数据封装。
*   **关键字段**: `request_id`, `platform`, `account`, `module`, `meta` (元数据), `data` (`DataType` 枚举), `data_middleware`.
*   **支持数据类型 (`DataType`)**:
    *   `DataFrame`: 结构化数据（如 Polars DateFrame）。
    *   `File`: 二进制文件数据（如图片、PDF）。
    *   `Json`: 非结构化 JSON 数据。
*   **生命周期**:
    1.  **Creation**: `Module.parser()` 解析成功后生成。
    2.  **Processing**: `DataMiddleware` 链式处理（如数据清洗、格式转换）。
    3.  **Persistence**: `DataStoreMiddleware` 将其写入最终存储（Postgres, S3, ES 等）。

---

## 4. 关键接口与契约 (Key Interfaces & Contracts)

### 4.1 ModuleTrait (`common/src/interface/module.rs`)
核心业务逻辑接口，每个具体的 Crawler 必须实现。
*   `generate()`: 根据配置和参数生成 `Request`流。
*   `parser()`: 接收 `Response`，返回 `ParserData`（包含数据 `Data`、新请求 `Request`、新任务 `ParserTask`）。
*   `add_step()`: 定义多步执行流（DAG）。
*   `cron()`: 可选的定时调度配置。

### 4.2 DownloadMiddleware (`common/src/interface/middleware.rs`)
拦截和处理网络请求/响应的钩子。
*   `before_request(request)`: 请求发送前调用（签名、加 Header）。
*   `after_response(response)`: 响应接收后调用（验证状态码、解密）。

### 4.3 DataMiddleware & DataStoreMiddleware (`common/src/interface/middleware.rs`)
处理和存储提取出的数据。
*   `handle_data(data)`: 数据清洗、去重、转换。
*   `store_data(data)`: 数据持久化接口。

---

## 5. 关键机制与功能 (Key Mechanisms & Features)

### 5.1 错误处理与重试 (Retry & Error Handling)
基于 `status_tracker` crate 实现了多层级错误追踪策略，防止局部故障级联扩散。
*   **层级控制**:
    *   **Request 级**: 单个 URL 失败重试（默认 3 次）。
    *   **Module 级**: 如果 Module 内错误率过高（默认连续 3 次），暂停该 Module 的后续处理。
    *   **Task 级**: 如果 Task（账号+平台）整体错误过多（默认 10 次），终止整个 Task 的运行。
*   **策略**:
    *   **指数退避**: 重试间隔采用指数级增加，防止重试风暴。
    *   **错误分类**: 区分 `Download` (网络), `Parse` (代码逻辑), `Auth` (401/403), `RateLimit` (429) 等不同错误类型采取不同策略。
    *   **死信队列 (DLQ)**: 超过最大重试次数的消息将被移入 DLQ 供人工排查。

### 5.2 分布式限流 (Distributed Rate Limiting)
基于 `DistributedSlidingWindowRateLimiter` 实现，确保对目标站点的友好性。
*   **机制**: 
    *   使用 Redis 存储滑动窗口计数。
    *   **平滑限流**: 计算请求间隔时间，避免窗口边界突发流量。
    *   **动态调整**: 支持通过 API 运行时调整 `rate` (QPS)。
    *   **时钟同步**: 使用 Redis Time 作为基准，避免节点间时钟偏移导致的不准。
    *   **配置热更新**: `download_config.rate_limit` 与 `api.rate_limit` 更新后，限流器会自动同步到新值。

### 5.3 动态配置 (Dynamic Configuration)
支持热重载的配置管理系统。
*   **Provider**: 支持 `FileConfigProvider` (本地文件监控) 和 `RedisConfigProvider` (Redis Pub/Sub 订阅)。
*   **分发**: 配置变更后通过 `tokio::sync::watch` 通道广播至所有活跃组件，无需重启服务即可调整日志级别、队列容量、限流阈值等。

### 5.4 调度系统 (Scheduler)
`CronScheduler` 结合 **Leader Election** 实现分布式定时任务。
*   **选主**: 仅 Leader 节点执行调度逻辑，防止多节点重复触发。
*   **缓存**: 调度规则与上下文缓存在内存中，每分钟与 Redis/DB 同步一次。
*   **容错**: `misfire_tolerance` 机制处理调度延迟。

### 5.5 事件驱动系统 (Event System)
系统通过 `EventBus` 实现了细粒度的可观测性。所有的关键状态变更都会生成事件，这些事件可以被 Log、Metric 甚至外部系统消费。
*   **事件模型**: 统一使用 `EventEnvelope`，包含 `domain`/`event_type`/`phase`/`payload`/`error`，并通过 `event_key` 路由。
*   **事件分类**:
    *   `EventType::TaskModel`/`ParserTaskModel`：任务接收、开始、完成、失败、重试。
    *   `EventType::RequestPublish`/`RequestMiddleware`：请求生成、发送、前置中间件。
    *   `EventType::Download`：下载开始/结束/失败/重试。
    *   `EventType::ResponseMiddleware`/`ResponsePublish`：响应中间件与发布。
    *   `EventType::ModuleGenerate`/`Parser`/`MiddlewareBefore`/`DataStore`：解析与数据链路。
    *   `EventType::SystemError`/`SystemHealth`：系统错误与健康状态。
*   **用途**:
    *   **结构化日志**: 所有操作流水线化，便于追踪 ID (Trace ID) 关联。
    *   **实时监控**: Prometheus Exporter 订阅事件总线生成 Metrics。
    *   **持久化审计**: 可选将事件写入 Postgres/ElasticSearch 用于审计。

### 5.6 代理管理 (Proxy Manager)
`ProxyManager` 提供了对 IP 代理和隧道代理的统一管理与调度。
*   **策略**:
    *   **IP Pool**: 维护一个动态的 IP 池，支持定期健康检查剔除失效 IP。
    *   **Tunnel**: 支持设置多个隧道代理提供商 (如 SmartProxy, BrightData)。
    *   **Quality Scoring**: 根据历史成功率和响应时间对代理评分，优先使用高质量代理。
*   **接口**: `get_proxy(provider)` 支持指定特定的供应商或策略。

---

## 6. Python 版本架构规划 (Python Implementation Plan)

Python 版本的目标是复刻 Rust 版本的高性能设计，同时利用 Python 丰富的生态系统（特别是数据分析和 ML 领域）来降低用户编写爬虫逻辑的门槛。Rust 版本作为高性能内核，Python 版本作为快速开发和原型验证的工具，两者共享相同的设计哲学和数据结构契约，便于未来互操作。

### 6.1 核心设计理念
*   **兼容性**: 保持与 Rust 版本一致的消息契约 (`TaskModel`, `Request`, `Response`)，理论上允许 Python Engine 和 Rust Engine 共用同一个 Redis/Kafka 集群进行混合部署。
*   **异步优先**: 全面拥抱 Python `asyncio`，IO 密集型组件（Downloader, Redis, DB）全部异步化。
*   **类型安全**: 使用 Pydantic 进行严格的数据验证，确保数据在系统流转中的一致性。

### 6.2 技术栈选型
| 组件 | 选型 | 理由 |
| :--- | :--- | :--- |
| **Runtime** | `Python 3.13` + `uvloop` | 利用最新的 GIL 改进和 uvloop 的高性能事件循环。 |
| **Web Framework** | `FastAPI` | 基于 ASGI，原生支持异步和 Pydantic，用于 Control Plane API。 |
| **Data Validation** | `Pydantic v2` | 核心 Rust 实现，性能极高，用于 Request/Response 序列化。 |
| **HTTP Client** | `httpx` | 支持 HTTP/2，全异步 API，接口设计现代。 |
| **Redis** | `redis-py` (async) | 标准 Redis 异步客户端。 |
| **ORM** | `SQLAlchemy` (async) | 支持异步数据库操作，类型提示友好。 |
| **Dependency Injection** | `FastAPI Depends` / Custom | 简单的依赖注入容器，管理 Singletons (Config, State)。 |

### 6.3 模块架构

#### 6.3.1 目录结构
```text
mocra-py/
  ├── common/
  │   ├── models/       # Pydantic 模型 (TaskModel, Request, ParserData...)
  │   ├── interfaces/   # 抽象基类 (BaseModule, Middleware)
  │   ├── config.py     # 配置管理 (Pydantic Settings)
  │   └── state.py      # 全局状态单例
  ├── engine/
  │   ├── core/         # Pipeline, EventBus 实现
  │   ├── chains/       # 处理链 (TaskChain, DownloadChain...)
  │   ├── processors/   # 具体的 Processor 实现
  │   ├── scheduler.py  # APScheduler 集成
  │   └── event_bus.py  # 简单的内存/Redis 事件总线
  ├── downloader/       # httpx 封装, Client Pool, RateLimit
  ├── queue/            # Redis Stream / Kafka Consumer 封装
  ├── modules/          # 用户自定义爬虫脚本 (动态加载)
  └── main.py           # 程序入口
```

#### 6.3.2 关键类与转换
*   **Pipeline 模式**: Python 版将实现一个轻量级的 `Pipeline` 类，支持中间件风格的 `process(input, context) -> output` 调用链。
*   **Module 加载**: 利用 Python 的动态特性，支持从文件系统动态加载 `BaseModule` 的子类，无需重新编译。
*   **Rate Limiting**: 使用 `redis-py` 实现与 Rust 版本兼容的 Lua 脚本限流算法。

### 6.4 交互与扩展
*   **混合部署**: Python 编写的 Parser 可以消费由 Rust Engine 下载好的 Response (通过 Topic 隔离)，利用 Python 强大的 `BeautifulSoup`, `lxml` 或 `pandas` 进行复杂数据提取。
*   **快速迭代**: 开发者可以先用 Python 快速实现 `Module` 逻辑，验证通过后，如果性能成为瓶颈，再迁移至 Rust 版本（因为接口一致）。

---

## 7. 处理链详解 (Processor Chains)

Engine 通过一系列 `Chain` 串联处理逻辑，每个 Chain 处理特定类型的 Queue 消息，并可能产生新的 Queue 消息。

### 4.1 TaskModelChain
负责将抽象的“任务”转化为具体的“请求”。
*   **Input**: `TaskModel`
*   **Process**:
    1.  **Status Check**: 检查任务/模块是否被终止/暂停。
    2.  **Load Module**: 动态加载对应的 Rust/JS 模块。
    3.  **Generate**: 调用 `module.generate(task)`。
    4.  **Sign/Pre-process**: 应用 `RequestMiddleware`。
    5.  **Publish**: 将生成的 `Vec<Request>` 发送到 Request Queue。

### 4.2 DownloadChain
负责执行网络 I/O。
*   **Input**: `Request`
*   **Process**:
    1.  **Pre-check**: 再次检查任务状态。
    2.  **Middleware (Pre)**: `DownloadMiddleware::before_request`。
    3.  **Download**: 选择合适的 `Downloader` (Request / WebSocket) 执行请求。
    4.  **Middleware (Post)**: `DownloadMiddleware::after_response`。
    5.  **Publish**: 将 `Response` 发送到 Response Queue。若启用缓存，可能直接从 Redis 返回 Response。

### 4.3 ParserChain
负责业务逻辑解析。此链包含两个子处理器：
1.  **ResponseModuleProcessor**:
    *   **Input**: `Response`
    *   **Action**: 加载 Module、Config 和 LoginInfo。确保解析所需环境就绪。
    *   **Output**: `(Response, Module, Config, LoginInfo)`
2.  **ResponseParserProcessor**:
    *   **Input**: `(Response, Module, Config, LoginInfo)`
    *   **Action**: 调用 `module.parser(response)`.
    *   **Result Handling**:
        *   `Data`: 存入数据库 (`DataStoreMiddleware`)。
        *   `Request`: 发送回 Request Queue (如下一页)。
        *   `Task`: 发送回 Task Queue (如生成新任务)。
        *   `ParserTask`: 发送回 ParserTask Queue (用于状态流转)。
        *   `Error`: 发送回 Error Queue。

---

## 5. 数据流转图 (Data Flow)

```mermaid
graph TD
    A[Scheduler/API] -->|TaskModel| Q_Task[Queue: Task]
    Q_Task --> P_Task[TaskModelProcessor]
    P_Task -->|Load Module| M[Module Logic]
    M -->|Generate Requests| Q_Req[Queue: Request]
    
    Q_Req --> P_Down[DownloadProcessor]
    P_Down -->|HTTP/WS| Net[Internet]
    Net -->|Response| P_Down
    P_Down -->|Response| Q_Resp[Queue: Response]
    
    Q_Resp --> P_Parse[ResponseProcessor]
    P_Parse -->|Parse Logic| M
    
    M -->|Extracted Data| DB[(Database)]
    M -->|New Requests (Pagination)| Q_Req
    M -->|Next Step (ParserTask)| Q_PT[Queue: ParserTask]
    Q_PT --> P_Task
    
    M -->|Error| Q_Err[Queue: Error]
```

## 6. 目录结构说明

```text
mocra/
  ├── cacheable/      # 缓存抽象层
  ├── common/         # 共享类型、State、Config、接口定义
  ├── downloader/     # HTTP/WS 客户端实现
  ├── engine/         # 核心业务编排、Processor、EventBus
  ├── errors/         # 统一错误类型定义
  ├── js-v8/          # 基于 Rusty_v8 的 JS 运行时池
  ├── queue/          # Redis/Kafka 队列封装
  ├── sync/           # 分布式锁、状态同步、选主
  ├── proxy/          # 代理池管理
  ├── utils/          # 工具库（加密、日期处理、算法）
  ├── docs/           # 详细的设计文档
  └── src/            # Workspace 根，统一导出
```
