# 修改后目标逻辑

## 1. 目标定义

本次重构的目标不是“把现有代码整理一下”，而是把 mocra 收敛成一套**控制面清晰、数据面稳定、DAG 可恢复、配置可热更新、接口可演进**的分布式采集框架。

目标态必须满足以下原则：

1. 控制面统一落到 `Raft + RocksDB`。
2. 运行时热路径不再依赖 `serde_json::Value` 作为主表达。
3. DAG 调度具备 run guard、resume、fencing、placement 和单次推进语义。
4. API 成为配置热更新和运维控制的唯一入口。
5. 重构路线允许直接替换历史 API 与运行时模型，但切换边界必须可验证。

## 1.1 目标模块边界

目标态按以下模块收口：

| 模块 | 职责 |
|------|------|
| `control_plane::profile` | profile 写入、版本管理、配置真相 |
| `workflow::definition` | module 静态声明、workflow 编译、节点定义 |
| `runtime::context` | typed metadata、node context、request/response context |
| `scheduler::dag` | run state、guard、fencing、placement、recovery |
| `transport::queue` | queue envelope、topic、codec、blob、DLQ |
| `pipeline::processors` | task/request/download/parse/error 主链路 |
| `cluster::control` | health、pause/resume、node registry、cluster view |
| `observability` | metrics、health signal、dashboard/alert contract |
| `bootstrap` | `State/Engine` 装配、生命周期与 runtime wiring |

## 2. 目标控制面

### 2.1 统一控制面对象

| 对象 | 目标形态 | 说明 |
|------|----------|------|
| `TaskProfileSnapshot` | 配置快照 | Account/Platform/Module/Middleware 合成后的最终真相 |
| `DagRunState` | 运行状态 | run_id、已成功节点、resume outputs、run token |
| `DagRunGuard` | 运行锁 | 防止同一 run 被多个调度器重复执行 |
| `DagFencingStore` | 提交 token | 防止旧 worker 晚到结果覆盖新结果 |
| `StatusTracker` | 执行状态 | 任务、请求、解析、错误的状态写入统一状态机 |
| `ResponseCacheIndex` | 缓存索引 | 用于响应体定位与过期管理 |

### 2.2 控制面读写路径

```text
API DTO -> validator -> assembler
    -> TaskProfileSnapshot / TypedEnvelope
    -> Raft command
    -> RocksDB apply
    -> DashMap/L1 invalidation
    -> runtime local read
```

约束：

- 所有配置写入必须经过 API；
- 所有运行态读优先走本地已 apply 状态；
- 运行中的 run 必须绑定 `profile_version`，不允许被新版本隐式穿透。
- `TaskProfileSnapshot` 只承载业务配置与节点配置切片；queue backend / queue codec / cache topology / API 监听端口等基础设施配置，默认仍属于 restart 或显式 rebind 范围，不能和业务配置热更新语义混在一起。

## 3. 目标元数据模型

### 3.1 核心类型

| 类型 | 用途 |
|------|------|
| `RoutingMeta` | namespace、account、platform、module、node_key、run_id、request_id 等 |
| `ExecutionMeta` | retry_count、task_retry_count、profile_version、trace_id、fence_token 等 |
| `TypedEnvelope` | schema_id、schema_version、codec、bytes |
| `NodeInput` | 上游节点传给下游节点的输入 |
| `ResolvedCommonConfig` | timeout、rate_limit、priority、proxy_pool 等通用配置 |
| `ResolvedNodeConfig` | 当前节点实际可用的配置切片 |

### 3.2 元数据边界

目标边界如下：

| 边界 | 是否允许 JSON |
|------|---------------|
| API 入参/出参 | 允许 |
| 调试视图 | 允许 |
| 运行时热路径 | 不允许作为主表达 |
| DAG 节点上下文 | 不允许作为主表达 |
| MQ 消息 schema | 只允许 envelope，不允许裸 `Value` |

## 4. 目标模块接口

### 4.1 `ModuleTrait`

`ModuleTrait` 保留模块的**静态声明**和**默认配置方法**：

- `name()`
- `version()`
- `dag_definition()`
- `add_step()`
- `rate_limit()`
- `proxy_pool()`
- `timeout_secs()`
- `priority_level()`
- `serial_execution()`
- `enable_session()`
- `downloader()`
- `rate_limit_group()`
- `pre_process()`
- `post_process()`
- `cron()`

### 4.2 `ModuleNodeTrait`

目标是改成上下文式接口：

```rust
async fn generate(&self, ctx: NodeGenerateContext<'_>) -> Result<SyncBoxStream<'static, Request>>;
async fn parser(&self, response: Response, ctx: NodeParseContext<'_>) -> Result<NodeParseOutput>;
```

节点不再直接面对：

- 整个 `ModuleConfig`
- `Map<String, Value>` 型 params
- 隐式 metadata slot

而只面对：

- 当前 node 的配置切片
- 当前 run 的路由/执行上下文
- 上游节点传来的 typed input

## 5. 目标 DAG 执行逻辑

### 5.1 图定义层

仍由 `ModuleDagOrchestrator` 作为统一入口：

- `dag_definition()` 负责自定义图；
- `add_step()` 负责线性 DSL 写法；
- 两者合并成统一 DAG definition。

### 5.2 调度层

目标态接入 `DagScheduler`：

| 构件 | 作用 |
|------|------|
| `DagRunGuard` | 同一 run 单 owner |
| `DagRunStateStore` | 恢复点保存 |
| `DagFencingStore` | 节点提交有序性 |
| `DagNodeDispatcher` | 本地/远程执行派发 |
| `NodePlacement` | 节点放置 |
| `DagNodeExecutionPolicy` | timeout/retry/idempotency/circuit breaker |

### 5.3 推进层

`ModuleDagProcessor` 或其后继实现负责：

- 当前节点解析
- successor 路由
- fan-out
- stay_current_step 重试
- 空输出推进
- stop signal

### 5.4 本地快路径

保留当前 parser -> local generate 的优化，但升级为显式策略：

- 当 `NodePlacement=Local` 且目标 node 与当前执行上下文一致时，允许直接本地推进；
- 不再把这个行为写成 parser chain 里的隐式特殊判断；
- 由调度层统一决定是否绕过 `parser_task` topic。

## 6. 目标队列与消息模型

### 6.1 队列职责

`QueueManager` 在目标态保留以下职责：

- topic 管理
- codec 选择
- payload 压缩
- blob 卸载
- ACK/NACK/DLQ
- backpressure

### 6.2 新消息 schema 原则

| 消息 | 当前 | 目标 |
|------|------|------|
| task | `TaskEvent` | `TaskDispatchEnvelope` |
| parser_task | `TaskParserEvent` | `NodeDispatchEnvelope` |
| request | `Request` | 明确携带 `routing / exec / node_config_ref` 的目标 request model |
| response | `Response` | 明确携带 `routing / exec / request_id` 的目标 response model |
| error | `TaskErrorEvent` | `NodeErrorEnvelope` |

### 6.3 切换原则

切换期间遵循：

- 旧 worker 与新 worker 不共同消费同一 topic；
- 旧 topic 在切换前先 drain、重建或离线迁移；
- DLQ / blob 若需保留，走离线迁移或定点回放，而不是运行时双解码；
- queue runtime 默认以 typed envelope 为唯一真相。

## 7. 目标 API 逻辑

### 7.1 API 分层

目标 API 分为：

- `/config/*`
- `/tasks/*`
- `/cluster/*`
- `/dlq/*`
- `/control/*`
- `/debug/*`
- `/metrics`
- `/health`

### 7.2 API 写路径

所有写 API：

1. 收到 JSON DTO
2. 校验字段与 schema
3. 组装 typed snapshot / envelope
4. 写入 Raft
5. 本地 apply 后失效缓存
6. 由运行时下次读取时获得新版本

## 8. 目标指标与可观测逻辑

详细指标清单、命名规则和迁移策略见 `development_docs\07_metrics_system.md`。本文件只保留目标态必须满足的结构性要求。

### 8.1 指标分层

目标态指标体系分三层：

1. **L1 核心流水线指标**：统一回答吞吐、延迟、错误、积压、并发。
2. **L2 子系统指标**：定位 queue / dag / scheduler / downloader / api / config / coordination / cache / logger 中的具体故障域。
3. **L3 调试指标**：保留 PTM、Lua action、remote dispatcher、细粒度 backpressure 等定位细节。

### 8.2 标签与上下文

目标态所有指标都必须从 typed runtime context 派生标签，最小公共维度为：

- `namespace`
- `node_id`
- `component`
- `deployment_mode`

再按域追加：

- `pipeline`
- `stage`
- `backend`
- `result`
- `error_class / error_code`
- `module` / `workflow` / `node_name`（受控使用）

明确禁止：

- `account`
- `request_id / task_id / run_id`
- 原始 URL
- 原始异常文本

### 8.3 核心指标骨架

所有主链路阶段都必须优先接入以下统一指标族：

- `mocra_stage_events_total`
- `mocra_stage_duration_seconds`
- `mocra_stage_errors_total`
- `mocra_stage_inflight`
- `mocra_stage_backlog`

其余 queue / DAG / API / config / coordination 指标都建立在这套骨架之上，而不是绕开骨架各自定义一套不可聚合的语义。

### 8.4 目标看板与告警

目标默认看板至少包括：

1. Cluster Overview
2. Pipeline SLA
3. Queue & Backpressure
4. DAG Execution
5. Downloader & Proxy
6. Control Plane & Config

目标默认告警至少覆盖：

- 节点不可达
- queue backlog / consumer lag 持续升高
- stage error rate / P99 latency 异常
- DAG run 卡死或 run_guard/fencing 失败
- config version skew 不收敛
- timeout / 429 / proxy acquire failure 激增

## 9. 目标开发约束

1. 不先改调度器再改模型；先把 typed model 引入，再切 scheduler 和 processor。
2. 不保留 `ModuleConfig / MetaData / TaskOutputEvent` 双轨；新链路 ready 后直接替换。
3. 不允许让同一 run 在切换中跨两套 profile version 继续执行。
4. 不允许旧 schema topic 与新 schema worker 混跑。
5. 不依赖 `tests\Cargo.toml` 下当前已知不稳定的 harness 作为主验收路径。
6. 不允许在新指标体系里继续混用 `namespace` 与 `node_id`。
7. 不允许把高基数字段（例如 request_id、account、原始异常文本）直接写进指标标签。
