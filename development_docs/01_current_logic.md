# 现有逻辑总览

## 1. 文档范围

本文档描述当前 `src\` 代码中的真实运行逻辑，重点覆盖：

- 运行时装配
- 任务/请求/响应/解析链路
- 配置与元数据传播
- DAG 的当前执行方式
- 队列、缓存、协调与 API 现状
- 当前实现对重构的主要阻碍

目标是给重构提供一个**真实基线**，避免只根据目标架构文档做不符合当前代码的假设。

## 2. 运行时装配入口

### 2.1 `State`

关键文件：`src\common\state.rs`

当前 `State` 是系统的组合根，负责：

| 资源 | 当前实现 |
|------|----------|
| DB | `sea_orm::DatabaseConnection` |
| 配置 | `Arc<RwLock<Config>>` |
| 通用缓存 | `CacheService`（本地或 Redis/L1 两级） |
| Cookie 存储 | 可选独立 `CacheService` |
| 分布式锁 | `DistributedLockManager` |
| 分布式限流 | `DistributedSlidingWindowRateLimiter` |
| API 限流 | 基于同一限流器另建 namespace |
| 状态跟踪 | `StatusTracker` |
| Redis 连接池 | 暴露给某些特殊组件使用 |

几个容易被忽略的实现细节：

1. `cache_pool` 会被缓存、分布式锁、下载限流器和 API 限流器复用；`cookie_pool` 是可选的独立池。
2. `StatusTracker` 初始化时默认开启 `success_decay`，但 `time_window` 逻辑默认关闭。
3. 单节点 / 分布式模式仍由 `Config::is_single_node_mode()` 推断，本质上还是“看 `cache.redis` 是否存在”。

当前问题：

- 运行模式仍由 `Config::is_single_node_mode()` 和 `cache.redis` 推断。
- 分布式锁、限流、leader 等路径仍有明显 Redis 基线。
- `State` 同时承载控制面、数据面、兼容层，职责过重。
- 当前 config watcher 只会更新内存中的 `Config`，并把新阈值推送给 `limiter / api_limiter`；它不会在线重建 `QueueManager`、`CacheService`、`cookie_service`、`DistributedLockManager`、Redis pool 或 API server。

### 2.2 `Engine`

关键文件：`src\engine\engine.rs`

`Engine` 当前负责装配：

- `QueueManager`
- `DownloaderManager`
- `TaskManager`
- `MiddlewareManager`
- `EventBus`
- `NodeRegistry`
- `CronScheduler`
- `LuaScriptRegistry`
- `ProcessorRunner`

当前还有两个重要的运行时控制细节：

1. pause/resume 并不是即时广播，而是 `Engine::new()` 中的后台 poller 每 5 秒读取 `{namespace}:engine:pause`，再通过 `pause_tx` 通知各 `ProcessorRunner`。
2. `start_health_monitor()` 每 30 秒执行一次限流器过期键清理，并向 `EventBus` 发布 `SystemHealth` 事件；它与 HTTP `/health` 路由的探活逻辑是分离的。

当前处理管道：

```text
TaskEvent / TaskParserEvent / TaskErrorEvent
    -> TaskModelChain
    -> Module.generate()
    -> Request
    -> DownloadChain
    -> Response
    -> ParserChain
    -> TaskOutputEvent
    -> parser_task / error_task / data store
```

## 3. 当前任务装配逻辑

### 3.1 `TaskFactory`

关键文件：`src\engine\task\factory.rs`

当前 `TaskFactory` 的真实行为：

1. 根据 `(account, platform)` 读取任务模块集合。
2. 冷加载时触发多次 DB 查询：
   - account
   - platform
   - account-platform relation
   - modules
   - module-data-middleware relations
   - module-download-middleware relations
   - middleware entity bulk load
3. 使用 `ConfigAssembler::assemble_module_config(...)` 生成 `ModuleConfig`。
4. 为每个模块创建 `Module` 运行时实例，并在内部初始化 `ModuleDagProcessor`。
5. 使用 30 秒 `DashMap` 做任务级内存缓存。

当前问题：

- 冷加载 DB 查询多，且缓存无跨节点失效。
- `ModuleConfig` 在工厂阶段就被组装为 JSON-heavy 结构。
- `ModuleDagProcessor.run_id` 随 factory cache 复用时可能与新 run 不完全同步。

### 3.2 `Module`

关键文件：`src\engine\task\module.rs`

`Module` 运行时对象当前同时持有：

- `config: Arc<ModuleConfig>`
- account/platform/module 元信息
- 中间件清单
- `ModuleDagProcessor`
- `pending_ctx`
- `prefix_request`
- `run_id`
- 绑定后的 task meta / login info

这说明当前 `Module` 同时承担：

1. 模块静态定义外壳
2. 模块运行时状态容器
3. DAG 当前推进上下文

这也是后续需要把运行态 context 拆走的重要原因。

## 4. 当前配置与元数据逻辑

### 4.1 `ModuleConfig`

关键文件：`src\common\model\model_config.rs`

当前 `ModuleConfig` 的特点：

| 特征 | 当前实现 |
|------|----------|
| 基本形态 | 多个 `serde_json::Value` + 多个 `HashMap<String, Value>` |
| 合并层次 | account / platform / middleware / relation / module 共 9+ 层 |
| 读取方式 | `get_config_value("key")` / `get_config<T>("key")` |
| 运行时语义 | 控制面配置、中间件配置、关系配置全部混在一个对象里 |

当前问题：

- 强依赖字符串 key；
- merge 优先级隐式；
- 节点拿到的是整个对象，而不是当前 node 需要的配置切片；
- 调试和运行时真相混在一起。

### 4.2 `MetaData`

关键文件：`src\common\model\meta.rs`

当前 `MetaData` 有四个主要槽位：

- `task`
- `login_info`
- `module_config`
- `trait_meta`

这些槽位都以 `serde_json::Value` 存储，并在 `Request -> Response -> Parser -> TaskParserEvent` 之间传递。

当前问题：

- 控制面配置、业务输入、登录上下文、调试数据没有边界；
- 每次读取都依赖 `serde_json::from_value`；
- 类型变化和 schema 变化无法在编译期暴露。

### 4.3 `ModuleNodeTrait`

关键文件：`src\common\interface\module.rs`

当前签名：

```rust
async fn generate(
    &self,
    _config: Arc<ModuleConfig>,
    _params: Map<String, serde_json::Value>,
    _login_info: Option<LoginInfo>,
) -> Result<SyncBoxStream<'static, Request>>;

async fn parser(
    &self,
    response: Response,
    _config: Option<Arc<ModuleConfig>>,
) -> Result<TaskOutputEvent>;
```

当前问题：

- `config` 和 `params` 是两类完全不同的数据，却被放进同一组参数；
- `params` 实际上是“上一个节点传来的数据面输入”，不该再是 map；
- `parser` 输出 `TaskOutputEvent` 仍是当前兼容壳，不适合作为最终目标接口。

## 5. 当前 DAG 执行逻辑

### 5.1 图编译

关键文件：

- `src\engine\task\module_dag_orchestrator.rs`
- `src\engine\task\task_manager.rs`

当前逻辑：

1. `ModuleTrait::dag_definition()` 定义自定义 DAG。
2. `ModuleTrait::add_step()` 提供线性步骤兼容模式。
3. `ModuleDagOrchestrator` 合并两者。
4. `TaskManager` 负责预编译 DAG 和蓝绿切换门控。

### 5.2 单次 run 推进

关键文件：`src\engine\task\module_dag_processor.rs`

当前 `ModuleDagProcessor` 已具备以下能力：

| 能力 | 当前实现 |
|------|----------|
| 当前节点定位 | `ExecutionMark.node_id` 优先，缺失时 fallback 到 `step_idx` |
| entry node 选择 | `entry_nodes` 或自动推导 |
| successor 路由 | parser 未显式指定时自动推进 |
| fan-out | 多后继时自动复制 task |
| 空输出推进 | `advance gate` 合成占位 `TaskParserEvent` |
| 停止信号 | `DagStopSignal` |
| 同节点重试 | `stay_current_step=true` |

当前 caveat：

- `advance gate` 和 `stop signal` 仍基于 `CacheService`；
- run 状态没有独立的 `DagRunStateStore`；
- `processor.run_id` 与 factory cache 的复用存在陈旧风险；
- 分布式调度抽象存在于 `src\schedule\dag\*`，但主执行链尚未真正接入。

### 5.3 本地快路径

关键文件：`src\engine\chain\parser_chain.rs`

当前 parser 后的推进不是总回到 `parser_task` 队列：

- 若下一跳仍然是**同 module、同 account/platform**，则会尝试直接本地调用 `generate()`；
- 若本地生成失败，或目标 module/context 不同，则回退到 `parser_task` 队列。

这意味着当前系统已经有“局部 DAG 就地推进”的优化，只是没有被抽象成显式的 placement 策略。

## 6. 当前队列与消息逻辑

### 6.1 `QueueManager`

关键文件：`src\queue\manager.rs`

当前 `QueueManager` 特点：

| 能力 | 当前实现 |
|------|----------|
| backend | In-memory / Redis / Kafka |
| codec | MsgPack 默认，支持 JSON |
| 压缩 | 按阈值压缩 payload |
| blob 卸载 | 支持大 payload 文件系统卸载 |
| compensator | 支持补偿记录与回放 |
| topic | task / request / response / parser_task / error / log |

当前问题：

- 消息 schema 仍然偏运行时对象直传，而非显式 schema/version；
- parser_task / error_task 的上下文仍带 legacy metadata；
- 兼容 Redis Stream 的逻辑还存在于主路径与原型路径中。

补充几个对迁移很重要的实现细节：

1. `queue_codec` 通过进程级 `OnceCell` 在第一次 `QueueManager::from_config(...)` 时确定；后续 config watcher 不会在线切换 JSON / MsgPack。
2. 出站 forwarder 当前按 **500 条或 5ms** 刷批；入站订阅按 **50 条或 5ms** 解批。
3. 当批量达到 32 条以上，或总 payload 接近 64KB 时，会切到 `spawn_blocking` 做序列化/反序列化。
4. 反序列化失败的 poison message 会立即 NACK 为 `"Deserialization failed"`，不会伪装成成功消费。
5. `Compensator` 当前仍是 `RedisCompensator`；它记录的是“已进入处理但尚未 ACK 的原始 payload”，还不是文档目标态里的 queue-native begin/done 事件流。
6. `QueueManager` 已经存在局部本地快路径，例如 `try_send_local_response()` 可以在本机消费场景下绕过远端 MQ。

## 7. 当前 API 与控制面逻辑

### 7.1 已实现 API

关键文件：`src\engine\api\router.rs`

当前实际路由只有：

- `GET /metrics`
- `GET /health`
- `POST /start_work`
- `GET /nodes`
- `GET /dlq`
- `POST /control/pause`
- `POST /control/resume`

### 7.2 规划 API 与现状差距

`docs\api-architecture.md` 已定义 `/config/*`、`/tasks/*`、`/cluster/*`、`/debug/*` 等路径，但当前代码仍未真正落地这些控制面接口。

当前 API 还有几条很具体的行为约束：

1. `/health` 只检查 `cache_service.ping()` 和 `db.ping()`；虽然响应体会返回 `up / degraded`，但当前 handler 直接返回 `Json<HealthResponse>`，所以 degraded 仍是 HTTP 200。
2. `/metrics` 虽然不要求 API key，但仍会经过 `rate_limit_middleware`；限流 key 依次取 `X-API-Key`、`Authorization: Bearer`，都没有时退化为 `"anonymous"`。
3. `/control/pause` / `/control/resume` 只是在 cache 里写一个 namespaced key；真正停机要等 `Engine` 侧 5 秒轮询任务观察到变化。
4. `/nodes` 和 `/dlq` 都是 fail-open：后端查询失败时记录日志并返回空数组。
5. `/dlq` 返回体把 payload 当作 UTF-8 尝试解码；非 UTF-8 消息会显示成 `"Invalid UTF-8 payload"`，所以当前接口更偏“人工巡检”而不是“结构化回放”。
6. `NodeRegistry` 当前使用“TTL JSON record + ZSET 索引 + 读取时 lazy cleanup”的模式维护活跃节点列表。

## 8. 当前指标与可观测逻辑

### 8.1 当前 Prometheus 出口

关键文件：

- `src\engine\engine.rs`
- `src\engine\api\router.rs`
- `src\common\metrics.rs`
- `src\engine\monitor.rs`
- `src\engine\runner.rs`

当前代码已经具备 Prometheus 基础能力：

- `Engine::new()` 会安装 `PrometheusBuilder`；
- `GET /metrics` 直接调用 `PrometheusHandle::render()` 输出文本格式指标；
- `SystemMonitor` 会写入 CPU、内存、swap 等资源指标；
- `ProcessorRunner` 会在 processor 执行前后维护 inflight gauge。
- `/metrics` 虽然不要求 API key，但仍会经过 `rate_limit_middleware`，未携带身份信息时进入 `"anonymous"` 限流桶；
- `start_health_monitor()` 发布的 `SystemHealth` 事件与 `/health` HTTP 探活并不是同一条信号链路。

### 8.2 当前指标分布

当前代码里的指标并不算少，但主要是分散存在：

| 域 | 当前情况 | 典型指标 |
|----|----------|----------|
| 基础封装 | `src\common\metrics.rs` 提供少量统一族 | `mocra_node_up`、`mocra_component_health`、`mocra_resource_usage`、`mocra_backlog_depth`、`mocra_inflight`、`mocra_throughput_total`、`mocra_latency_seconds`、`mocra_errors_total` |
| Queue / Backpressure | Redis、Kafka、chain backpressure 各自打点 | `mocra_queue_encode_errors_total`、`mocra_request_publish_backpressure_total`、`mocra_download_response_backpressure_total`、`mocra_parser_chain_backpressure_total` |
| Scheduler / DAG | scheduler、run_guard、fencing、remote dispatcher 各自维护 | `mocra_scheduler_tick_duration_seconds`、`mocra_scheduled_tasks_total`、`mocra_dag_run_guard_*`、`mocra_dag_fencing_commit_total`、`mocra_dag_remote_*` |
| Cache / Dedup | 有命中率和延迟，但命名与单位不统一 | `mocra_cache_hits`、`mocra_cache_get_latency_us`、`mocra_dedup_l1_hits`、`mocra_dedup_check_latency_us` |
| Logger | sink 级别指标独立存在 | `mocra_log_events_total`、`mocra_log_dropped_total`、`mocra_log_queue_lag` |
| 其他调试路径 | PTM、Lua action、policy decision 等 | `mocra_ptm_commit_total`、`mocra_error_task_lua_action_total`、`mocra_policy_decisions_total` |

### 8.3 当前问题

1. 文档中原有“指标列表”与代码里的真实指标集合已经不一致。
2. `common::metrics` 只覆盖了部分路径，很多模块仍直接调用 `counter!` / `histogram!` / `gauge!`。
3. 标签模型不统一：有的指标带 `module`，有的只带 `result`，有的甚至缺失 backend/stage。
4. 单位不统一：当前同时存在 `_us`、`_ms`、`_seconds`。
5. API / control plane / config hot update / version skew 等关键路径缺少系统性指标。
6. 当前 `init_metrics()` 的调用点把 `config.name` 传成了 node 维度，分布式下会混淆 `namespace` 与 `node_id`。

## 9. 当前实现对重构的主要阻碍

| 阻碍 | 影响 | 首要处理方向 |
|------|------|-------------|
| `ModuleConfig` / `MetaData` 过度 JSON 化 | 接口不清晰，迁移难度大 | 先引入 typed model 与适配层 |
| `TaskFactory` 冷加载 + 30 秒缓存 | 多节点下配置一致性差 | 引入 `TaskProfileSnapshot` 和 versioned invalidation |
| `ModuleDagProcessor` 运行态与模块实例耦合 | run 级恢复和调度困难 | 把 run state 外提到 scheduler/state store |
| `schedule/dag` 尚未接入主执行链 | 目标态和当前主链路割裂 | 设计渐进式接入点 |
| 当前 API 过少 | 无法支撑配置热更新与集群控制 | 分阶段补齐控制面 |
| Redis 兼容层仍广泛存在 | 目标态控制面尚未收敛 | 分层迁移，不一次性拔除 |
| 指标体系分散且标签不一致 | 无法稳定回答“哪里堵、为什么堵、哪层坏了” | 先统一 `namespace + node_id + stage` 维度，再补齐核心流水线指标 |
| 当前热更新范围被高估 | watcher 只会在线更新内存配置和限流阈值 | 明确区分“业务配置热更新”与“基础设施配置滚动重启” |
| 当前 API 语义较粗糙 | `/health`、`/nodes`、`/dlq`、pause 控制都偏运维雏形 | 在控制面重构时补齐 readiness、错误返回和传播语义 |
