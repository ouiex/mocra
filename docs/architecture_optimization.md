
Please activate [Research Expert Mode] only when the conversation involves academic research, deep learning, paper writing, or technical discussions.
Core Principles of Research Expert Mode:
Critical Thinking: Do not default to agreeing with my ideas. Stress-test my concepts against SOTA facts and logic.
Simulate a Reviewer: Think like "Reviewer #2." Actively look for loopholes, flaws in assumptions, or logical fallacies. 

# 架构优化方案（Reviewer #2 模式）

日期：2026-02-09

## 范围与依据

本文档结合 TODO 方向与代码证据，主要来源如下：

- 引擎生命周期与编排：[src/engine/engine.rs](src/engine/engine.rs)
- Cron 调度与缓存刷新：[src/engine/scheduler.rs](src/engine/scheduler.rs)
- 队列语义与 Ack/Nack：[src/queue/lib.rs](src/queue/lib.rs)
- 队列管理、编解码、压缩与批处理：[src/queue/manager.rs](src/queue/manager.rs)
- 分布式同步服务：[src/sync/distributed.rs](src/sync/distributed.rs)
- 错误模型与分类：[src/errors/error.rs](src/errors/error.rs)

目标是从“描述性架构”升级为“可验证、契约驱动”的行为体系。

## 关键风险（Reviewer #2）

1) 关键路径契约不清晰
- Ack/Nack 与 DLQ 已在队列层定义，但系统级保证（至少一次/至多一次）未明确。
- 风险：行为漂移与难以定位的正确性回归。

2) 一致性模型未形式化
- SyncService 混合流式更新与定期 KV 刷新，可能出现短时回滚或读到陈旧值。
- 风险：读己之写不稳定，语义不确定。

3) 调度正确性缺少可验证 SLO
- Cron 缓存刷新依赖 Redis 版本与模块 hash，但未定义传播延迟或恢复目标。
- 风险：调度遗漏或重复，缺乏可量化保证。

4) 错误策略隐式化
- ErrorKind 已存在，但未建立统一的重试/退避/DLQ 策略。
- 风险：不可预测的重试风暴、静默丢失或告警噪音。

5) 可观测性未标准化
- 有 Prometheus，但缺少关键路径的必备指标与日志字段规范。
- 风险：排障成本高，依赖临时经验。

## 优化目标（可量化）

- 明确 queue 与 sync 的一致性语义并约束陈旧窗口。
- 为 engine/queue/sync 定义最小契约与契约测试。
- 给出延迟、吞吐、可用性与恢复时间的 SLO。
- 建立错误到策略的映射（重试、DLQ、告警）。
- 关键路径日志与指标标准化。

## 计划方案

### 1) 一致性模型与读写语义

交付物：
- 输出“读写语义矩阵”，逐条描述关键路径：
  - 强一致 / 最终一致
  - 读己之写支持
  - 最大陈旧窗口

目标示例：
- 调度传播：P95 <= 2s，最大 10s
- Sync 状态刷新：最大 30s 陈旧（若流式更新更快则取更小）

验收标准：
- 语义文档化，并有测试或监控阈值支撑。

### 2) 最小契约集合（Queue/Sync/Engine）

定义契约 trait 并配套契约测试，锁定语义：
- Queue：
  - Ack/Nack 行为
  - DLQ 重试失败策略
  - 顺序/优先级期望
- Sync：
  - 更新单调性或明确允许回滚
  - 快照与流式更新的顺序关系
- Engine：
  - 任务生命周期不变式（入队 -> 处理 -> ack/nack）

验收标准：
- 覆盖重复消息、乱序与 nack -> DLQ 的契约测试。

### 3) 可靠性目标与 SLO

为关键链路定义量化目标：
- 端到端延迟的 P95/P99
- 单节点与集群吞吐
- 可用性与恢复时间

验收标准：
- tests/ 下新增可复现的基准或压测脚本。

### 4) 统一错误策略矩阵

将 ErrorKind 映射为处理策略：
- 是否重试
- 退避策略
- DLQ 路由
- 告警等级

验收标准：
- 统一策略表并覆盖关键错误的测试用例。

### 5) 可观测性标准化

统一：
- 日志字段：trace_id、task_id、node_id、queue_topic
- 指标命名与基数约束
- Trace 采样策略

验收标准：
- 核心仪表盘定义与告警阈值。

### 6) 依赖拓扑与边界收敛

动作：
- 生成 crate 依赖图，识别环与高耦合节点。
- 低变动模块合并或下沉至 common。

验收标准：
- 公共接口数量减少，并验证编译时间下降。

## 证据链接

- 引擎编排与事件接入：[src/engine/engine.rs](src/engine/engine.rs)
- Cron 调度缓存刷新：[src/engine/scheduler.rs](src/engine/scheduler.rs)
- 队列 ack/nack 与 DLQ 接口：[src/queue/lib.rs](src/queue/lib.rs)
- 队列批处理、编解码与压缩：[src/queue/manager.rs](src/queue/manager.rs)
- Sync 流与 KV 刷新：[src/sync/distributed.rs](src/sync/distributed.rs)
- 错误类型分类：[src/errors/error.rs](src/errors/error.rs)

## 待确认问题

- 各队列 topic（task/request/response/parser/error）的投递保证是什么？
- SyncService 是否需要强制单调更新，还是允许回滚并记录？
- 调度正确性是严格一次，还是 best-effort？

## 下一步（建议）

1) 评审一致性矩阵模板与目标陈旧窗口。
2) 定义 Queue 与 Sync 的契约测试用例。
3) 确认 SLO 目标并补充基准脚本。

## 优先级规划（5 个优化点）

收益最大、应从“契约与一致性语义落地”开始，因为它直接决定正确性与回归成本，并为测试、SLO、可观测性打基础。

### P0: 一致性语义矩阵 + 最小契约

目标：明确 queue/sync/engine 的投递保证、幂等、顺序性与读写语义，并形成可测试契约。

#### 一致性语义矩阵（初稿）

| 路径/能力 | 读写语义 | 投递保证 | 顺序性 | 幂等要求 | 允许陈旧 | 证据/备注 |
| --- | --- | --- | --- | --- | --- | --- |
| Queue 生产到消费 | 最终一致 | 至少一次 | 仅同 topic 内尽力 | 必须 | <= 5s | 通过 Ack/Nack + DLQ 约束重放，需明确重试次数与退避策略 |
| DLQ 读取 | 最终一致 | 至少一次 | 不保证 | 必须 | <= 60s | DLQ 语义需明确“人工/自动回放”策略 |
| SyncService 流更新 | 最终一致 | 至少一次 | 不保证 | 必须 | <= 30s | 流更新优先于定期 KV 刷新，可能出现短时回滚 |
| SyncService KV 刷新 | 最终一致 | 至少一次 | 不保证 | 必须 | <= 30s | 作为补偿刷新，可能覆盖更旧值，需要冲突策略 |
| Cron 配置传播 | 最终一致 | 至少一次 | 不保证 | 必须 | <= 10s | 依赖 Redis 版本与模块 hash，需明确刷新窗口 |

注：以上为基于现有实现的最小语义建议，若目标是更强保证（例如“严格一次”或“有序”），需在实现层补充事务或去重窗口。

#### 最小契约（必须项）

Queue 契约：
- Ack 必须表示“此消息不会再次投递”。
- Nack 必须导致重试或进入 DLQ，且重试次数与退避策略可配置。
- 同一消息重复投递必须可被幂等逻辑安全处理。

Sync 契约：
- 明确是否允许回滚（收到旧值覆盖新值）。
- 若允许回滚，需有版本号或时间戳比较策略。
  - 现状默认值（未使用 SyncOptions 时）：允许回滚=true，envelope_enabled=false。

Engine 契约：
- 任务生命周期必须满足：入队 -> 处理 -> Ack/Nack 终结。
- 处理失败必须可定位到明确的重试/降级/DLQ 行为。

#### 详细优化方案（落地版）

目标：在不改变核心业务逻辑的前提下，先固化语义与边界，再以测试与度量作为回归防线。

实施顺序（与一致性矩阵对齐）：
- 先落地 Queue 语义（至少一次 + 重试 + DLQ + 幂等前置），并补齐最小契约测试。
- 再落地 Sync 语义（回滚/版本策略），确保 KV 刷新与流更新冲突可解释。
- 最后补齐 Cron 配置传播的可验证窗口与监控指标。

阶段 1：语义冻结与范围收敛（1-2 周）
- 定义 3 类关键路径清单：队列投递、同步状态、调度配置。
- 为每条路径填写语义矩阵字段：读写语义、投递保证、顺序性、幂等要求、最大陈旧窗口。
- 明确“允许回滚/不允许回滚”的策略选项，并形成决策记录。

阶段 2：最小契约落地（1 周）
- Queue 契约：明示 Ack/Nack 的最终性，重试次数与退避策略的默认值与上限。
- Sync 契约：定义版本号或时间戳对比规则，描述回滚的判定条件。
- Engine 契约：明确任务生命周期必须覆盖 ack/nack 终结态，禁止“悬空任务”。

阶段 3：可验证性与风险防线（1-2 周）
- 新增契约测试最小集：重复投递、乱序、nack -> DLQ、同步回滚边界、调度刷新窗口。
- 明确每条契约的“可观察指标”，用于线上验证（例如重复率、回滚率、延迟分位）。

交付物清单（必须）
- 语义矩阵完整表（覆盖所有关键路径）。
- 契约定义文档（Queue/Sync/Engine）。
- 契约测试用例列表与预期结果。

验收标准
- 每条路径都给出明确语义与最大陈旧窗口。
- 契约测试通过率 100%，且覆盖最小故障场景。
- 关键语义均可映射到可观测指标。

风险与反证点（Reviewer #2）
- 若无法量化“允许陈旧窗口”，则语义矩阵不成立。
- 若回滚策略不明确，则 Sync 一致性结论不可验证。
- 若 Ack/Nack 语义不具备最终性定义，则契约测试无法构建。

#### P0 当前实现与验证（已完成）

实现要点：
- Queue：落地 Nack 重试/DLQ 策略与投递头部，明确重试判定与 DLQ 路由。[src/queue/lib.rs](./src/queue/lib.rs) [src/queue/manager.rs](.././src/queue/manager.rs) [src/queue/redis.rs](./src/queue/redis.rs) [src/queue/kafka.rs](./src/queue/kafka.rs)
- Sync：引入 SyncOptions 与 envelope 语义，支持回滚策略与配置化接入。[src/sync/distributed.rs](src/sync/distributed.rs) [src/common/model/config.rs](src/common/model/config.rs)
- Cron：补齐配置传播的刷新间隔与最大陈旧窗口，并增加强制刷新逻辑。[src/engine/scheduler.rs](src/engine/scheduler.rs)
- Engine：处理失败落地 Nack 语义，确保生命周期终结态可追踪。[src/engine/engine.rs](src/engine/engine.rs)

测试与结果：
- Queue 契约与 Redis/Kafka 行为测试通过。[src/queue/tests.rs](src/queue/tests.rs)
- Sync 回滚边界测试通过。[src/sync/tests/test_sync_rollback.rs](src/sync/tests/test_sync_rollback.rs)
- Cron 陈旧窗口强制刷新测试通过。[src/engine/scheduler.rs](src/engine/scheduler.rs)
- 最近验证命令：`cargo test`，`cargo test`，`cargo test`

### P1: 契约测试与关键失败场景回归

覆盖重复、乱序、nack -> DLQ、宕机恢复等关键路径。

#### P1 分析：关键失败场景缺口

当前 P0 已固化语义与最小契约，但“失败场景回归”仍是薄弱环节：
- 失败窗口未被系统化枚举，导致回归仅覆盖“正常路径”。
- Redis/Kafka 语义差异可能导致同一契约在不同 backend 上出现漂移。
- 进程崩溃、网络抖动、重复投递等场景缺乏可重复、可比较的回归基线。

#### P1 实现要点（如何落地）

1) 建立“失败场景矩阵”并绑定到契约测试
- 将失败场景定义为固定输入与预期输出，做到“可复现 + 可验证”。
- 每个场景必须映射到至少一个契约断言（Queue/Sync/Engine）。

2) 统一测试基建与接口抽象
- 复用现有契约测试框架扩展场景集。[src/queue/tests.rs](src/queue/tests.rs) [src/sync/tests/test_sync_rollback.rs](src/sync/tests/test_sync_rollback.rs)
- 为 Redis/Kafka 构建相同语义的测试入口，确保“同一测试、不同后端”。[src/queue/redis.rs](src/queue/redis.rs) [src/queue/kafka.rs](src/queue/kafka.rs)

3) 引入可控故障与时间控制
- 在测试中引入“可控失败点”（例如重试次数耗尽、强制延迟、模拟重复投递）。
- 对 Cron/Sync 使用时间窗口驱动的断言，避免不稳定的 sleep 依赖。[src/engine/scheduler.rs](src/engine/scheduler.rs) [src/sync/distributed.rs](src/sync/distributed.rs)

#### 关键失败场景矩阵（最小集）

| 场景 | 触发方式 | 预期行为 | 契约归属 | 覆盖文件 |
| --- | --- | --- | --- | --- |
| 重复投递 | producer 重试 / broker 重发 | 幂等处理，Ack 后不再投递 | Queue | [src/queue/tests.rs](src/queue/tests.rs) |
| 乱序投递 | 多分区或并发消费 | 允许乱序但不违反幂等 | Queue | [src/queue/tests.rs](src/queue/tests.rs) |
| 重试耗尽 -> DLQ | 连续 Nack | 进入 DLQ，且携带重试头 | Queue | [src/queue/tests.rs](src/queue/tests.rs) |
| 毒消息 | 解码失败或业务异常 | 快速失败 + DLQ + 记录原因 | Queue/Engine | [src/queue/tests.rs](src/queue/tests.rs) [src/engine/engine.rs](src/engine/engine.rs) |
| 消费者崩溃 | Ack 前崩溃 | 消息可重投递，最终有终结态 | Queue/Engine | [src/queue/tests.rs](src/queue/tests.rs) [src/engine/engine.rs](src/engine/engine.rs) |
| Sync 回滚冲突 | 旧版本写入 | 按策略拒绝或记录回滚 | Sync | [src/sync/distributed.rs](src/sync/distributed.rs) [src/sync/tests/test_sync_rollback.rs](src/sync/tests/test_sync_rollback.rs) |
| Cron 陈旧窗口 | 过期缓存 | 强制刷新并更新时间戳 | Engine | [src/engine/scheduler.rs](src/engine/scheduler.rs) |

#### 优化路径（与 P0 格式一致）

阶段 1：场景枚举与契约映射（1 周）
- 产出“失败场景矩阵”，明确触发方式与预期行为。
- 为每个场景绑定契约断言与观测指标。

阶段 2：测试实现与跨后端对齐（1-2 周）
- Queue：同一测试在 Redis/Kafka 后端均通过，确保语义一致。
- Sync：补齐回滚、旧版本覆盖、快照/流式交错顺序的测试。
- Engine：补齐“处理失败 -> Nack/DLQ”的闭环测试。

阶段 3：稳定性与回归门禁（1 周）
- 将失败场景测试加入 CI 阶段门禁。
- 为每个场景设置最小通过阈值与回归报警。

交付物清单（必须）
- 失败场景矩阵（表格化 + 预期行为）。
- 契约测试用例清单与覆盖报告。
- Redis/Kafka 后端一致性报告。

验收标准
- 每个场景均有自动化测试，且在两种队列后端表现一致。
- 关键失败路径的告警与指标能够量化验证（例如 DLQ 率、重试耗尽率）。

风险与反证点（Reviewer #2）
- 若失败场景无法稳定复现，则测试价值下降并引入噪音。
- 若不同后端表现不一致且无法解释，则契约不成立。

#### P1 当前实现与验证（进行中）

新增覆盖：
- Queue：新增“消费者崩溃重投递”回归，并补齐重试路径的 Nack 原因头部断言。[src/queue/tests.rs](src/queue/tests.rs)
- Queue：补齐 Kafka 语义对齐回归（重试与乱序 Ack）。[src/queue/tests.rs](src/queue/tests.rs)
- Queue：补齐 Redis 并发/乱序 Ack 回归。[src/queue/tests.rs](src/queue/tests.rs)
- Sync：新增“毒消息流更新被忽略”回归，避免异常字节污染状态。[src/sync/tests/test_sync_rollback.rs](src/sync/tests/test_sync_rollback.rs)
- Engine：新增“毒消息触发 Nack”处理闭环回归。[src/engine/runner.rs](src/engine/runner.rs)
- CI：补齐契约测试门禁脚本与流水线配置。[scripts/ci_contract_tests.ps1](scripts/ci_contract_tests.ps1) [scripts/ci_contract_tests.sh](scripts/ci_contract_tests.sh) [.github/workflows/contract-tests.yml](.github/workflows/contract-tests.yml)
- 覆盖清单：输出 P1 契约测试覆盖报告。[docs/contract_test_coverage.md](docs/contract_test_coverage.md)

测试与结果：
- Queue 新增失败场景测试通过（11/11，含 Redis 并发回归与 Kafka 对齐用例）。[src/queue/tests.rs](src/queue/tests.rs)
- Sync 新增失败场景测试通过（3/3）。[src/sync/tests/test_sync_rollback.rs](src/sync/tests/test_sync_rollback.rs)
- Kafka 本地验证通过：`cargo test -- tests::test_kafka_retry_then_ack tests::test_kafka_out_of_order_ack`
- Redis 并发回归验证通过：`cargo test -- tests::test_redis_concurrent_out_of_order_ack`
- Engine 毒消息回归验证通过：`cargo test -- tests::test_poison_message_triggers_nack`
- 最近验证命令：`cargo test`，`cargo test`

#### P1 未完成项

- Redis 多消费者/多分区争抢的更深覆盖（目前为单消费者并发 + 乱序 Ack）。
- 真实链路的“毒消息/解码失败 -> 业务链路 -> Nack/DLQ”端到端回归（当前为 runner 级别闭环）。
- 覆盖报告产物发布到 CI（当前为静态文档，无流水线产物上传与校验）。

### P2: 统一错误与事件策略矩阵

错误来源限定为 Engine 链路错误事件 + System 错误事件。

#### P2 策略矩阵模板（事件驱动）

| 事件域 | 事件类型 | 典型错误来源 | ErrorKind | 处理策略 | 备注 |
| --- | --- | --- | --- | --- | --- |
| Engine | TaskModelReceived/Started/Completed/Failed/Retry | ModuleNotFound, TaskMaxError, 配置加载失败 | Task, Module, CacheService | 失败进入 ErrorTaskModel 或 DLQ | [src/engine/chain/task_model_chain.rs](src/engine/chain/task_model_chain.rs) [src/engine/events/events.rs](src/engine/events/events.rs) |
| Engine | ParserTaskModelReceived/Started/Completed/Failed/Retry | 解析失败, 模块锁定 | Parser, ProcessorChain | 可重试或转入错误队列 | 同上 |
| Engine | RequestPublishPrepared/Send/Completed/Failed/Retry | 队列写入失败 | Queue | 可重试, 超阈值 DLQ | 同上 |
| Engine | RequestMiddlewarePrepared/Started/Completed/Failed/Retry | 参数非法, 中间件异常 | Request, ProcessorChain | 失败进入 DLQ | [src/engine/chain/download_chain.rs](src/engine/chain/download_chain.rs) |
| Engine | DownloaderCreate/DownloadStarted/Completed/Failed/Retry | 网络超时, 连接失败, 限流/封禁 | Download, Proxy, RateLimit | 可重试, 超阈值 DLQ | 同上 |
| Engine | ResponseMiddlewarePrepared/Started/Completed/Failed/Retry | 响应解析异常 | Response, ProcessorChain | 失败进入 DLQ | 同上 |
| Engine | ResponsePublishPrepared/Send/Completed/Failed/Retry | 队列写入失败 | Queue | 可重试, 超阈值 DLQ | 同上 |
| Engine | ModuleGenerate/Started/Completed/Failed/Retry | ModuleMaxError, TaskMaxError, 配置缺失 | Module, Task | 可重试或终止 | [src/engine/chain/parser_chain.rs](src/engine/chain/parser_chain.rs) |
| Engine | ParserReceived/Started/Completed/Failed/Retry | 反序列化失败, 业务逻辑异常 | Parser | 失败进入 DLQ | 同上 |
| Engine | MiddlewareBefore/Started/Completed/Failed/Retry | 数据清洗异常 | DataMiddleware | 可重试或丢弃 | 同上 |
| Engine | StoreBefore/Started/Completed/Failed/Retry | DB 超时, 约束冲突 | DataStore, Orm | 可重试, 幂等冲突不重试 | 同上 |
| System | ErrorOccurred/ErrorHandled | 运行时异常, 依赖异常 | Service, Command, DynamicLibrary, CacheService | 记录 + 告警, 不进入 DLQ | [src/engine/events/events.rs](src/engine/events/events.rs) |
| System | SystemStarted/SystemShutdown | 启停流程异常 | Service | 记录 + 告警 | 同上 |
| System | ComponentHealthCheck | 组件健康异常 | Service, CacheService, Queue | 记录 + 告警 | 同上 |

#### P2 策略矩阵细化（按阶段）

说明：只对 Failed/Retry 阶段做策略决策；Started/Completed 阶段仅记录指标与审计，不触发重试/DLQ。

| 事件域 | 事件类型 | 阶段 | ErrorKind | 重试 | 退避 | DLQ | 告警 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Engine | TaskModel | Failed | Task, Module | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 失败进入 ErrorTaskModel 或 DLQ |
| Engine | TaskModel | Retry | Task, Module | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 复用失败策略 |
| Engine | ParserTaskModel | Failed | Parser, ProcessorChain | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 解析/链路失败可重试 |
| Engine | ParserTaskModel | Retry | Parser, ProcessorChain | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 复用失败策略 |
| Engine | RequestPublish | Failed | Queue | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 发送失败优先重试 |
| Engine | RequestPublish | Retry | Queue | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 复用失败策略 |
| Engine | RequestMiddleware | Failed | Request, ProcessorChain | 否 | None | Always | Error | 参数非法/中间件错误不重试 |
| Engine | Download | Failed | Download, Proxy, RateLimit | 是 | Exponential(500ms-60s) | OnExhausted | Warn | 限流/封禁可延长退避 |
| Engine | Download | Retry | Download, Proxy, RateLimit | 是 | Exponential(500ms-60s) | OnExhausted | Warn | 复用失败策略 |
| Engine | ResponseMiddleware | Failed | Response, ProcessorChain | 否 | None | Always | Error | 响应解析异常直接 DLQ |
| Engine | ResponsePublish | Failed | Queue | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 发送失败优先重试 |
| Engine | ResponsePublish | Retry | Queue | 是 | Exponential(500ms-30s) | OnExhausted | Warn | 复用失败策略 |
| Engine | ModuleGenerate | Failed | Module, Task | 条件 | Exponential(500ms-30s) | OnExhausted | Error | 模块缺失/上限可转终止 |
| Engine | ModuleGenerate | Retry | Module, Task | 条件 | Exponential(500ms-30s) | OnExhausted | Error | 复用失败策略 |
| Engine | Parser | Failed | Parser | 否 | None | Always | Error | 解析类失败直接 DLQ |
| Engine | Parser | Retry | Parser | 否 | None | Always | Error | 复用失败策略 |
| Engine | MiddlewareBefore | Failed | DataMiddleware | 条件 | Linear(200ms-10s) | OnExhausted | Warn | 清洗失败按策略降级 |
| Engine | DataStore | Failed | DataStore, Orm | 条件 | Linear(200ms-10s) | OnExhausted | Error | 幂等冲突不重试 |
| System | SystemError | Failed | Service, Command, DynamicLibrary | 否 | None | Never | Error | 不进入 DLQ，仅告警 |
| System | SystemHealth | Completed | Service, CacheService, Queue | 否 | None | Never | Warn | 健康检查异常告警 |

#### P2 条件策略判定（用于“条件”）

- DataStore：若为幂等冲突/唯一键冲突 -> 不重试，不入 DLQ；若为超时/连接失败 -> 按重试策略。
- ModuleGenerate：若为 TaskMaxError/ModuleMaxError/配置缺失 -> 不重试，进入终止或 DLQ；若为加载超时/依赖异常 -> 按重试策略。
- MiddlewareBefore：若为用户输入/格式错误 -> 不重试；若为临时资源不足/依赖超时 -> 按重试策略。

#### P2 策略优先级与回退规则

策略选择优先级（从高到低）：
1) (EventDomain, EventType, EventPhase, ErrorKind)
2) (EventDomain, EventType, ErrorKind)
3) (ErrorKind)
4) DefaultPolicy

回退策略必须满足：
- 不允许降低告警等级（只能保持或更高）。
- 只允许增加退避或转入 DLQ，禁止从 DLQ 回退到重试。

#### P2 策略测试矩阵（最小集）

- Download/Failed + ErrorKind::Download -> 重试次数用尽后进入 DLQ。
- Parser/Failed + ErrorKind::Parser -> 不重试，直接 DLQ。
- SystemError/Failed -> 不重试、不进入 DLQ，必须告警。
- DataStore/Failed + 幂等冲突 -> 不重试，记录并继续。
- RequestPublish/Failed -> 重试 + 退避生效，达到上限后 DLQ。

#### P2 抽象概念与 Rust 风格伪代码

核心抽象（概念）：
- `EventDomain`：事件域（Engine/System）。
- `EventType`：具体事件类型（如 `TaskModelFailed`、`DownloadRetry`、`ErrorOccurred`）。
- `EventPhase`：事件阶段（Started/Completed/Failed/Retry），作为策略分流主键。
- `ErrorKind`：统一错误类型枚举（来自 `src/errors/error.rs`）。
- `Policy`：错误处理策略（重试、退避、DLQ、告警）。
- `Decision`：策略决策结果（用于执行 Ack/Nack/DLQ/告警）。
- `StrategyResolver`：事件 + ErrorKind -> Policy 的解析器。

Rust 风格伪代码（概念示意）：

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EventDomain {
  Engine,
  System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EventPhase {
  Started,
  Completed,
  Failed,
  Retry,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EventType {
  TaskModel,
  ParserTaskModel,
  RequestPublish,
  RequestMiddleware,
  Download,
  ResponseMiddleware,
  ResponsePublish,
  ModuleGenerate,
  Parser,
  MiddlewareBefore,
  DataStore,
  SystemError,
  SystemHealth,
}

#[derive(Debug, Clone, Copy)]
enum BackoffPolicy {
  None,
  Linear { base_ms: u64, max_ms: u64 },
  Exponential { base_ms: u64, max_ms: u64 },
}

#[derive(Debug, Clone, Copy)]
enum DlqPolicy {
  Never,
  OnExhausted,
  Always,
}

#[derive(Debug, Clone, Copy)]
enum AlertLevel {
  Info,
  Warn,
  Error,
  Critical,
}

#[derive(Debug, Clone, Copy)]
struct Policy {
  retryable: bool,
  backoff: BackoffPolicy,
  dlq: DlqPolicy,
  alert: AlertLevel,
}

#[derive(Debug, Clone)]
struct Decision {
  policy: Policy,
  reason: String,
}

trait EventKey {
  fn domain(&self) -> EventDomain;
  fn event_type(&self) -> EventType;
  fn phase(&self) -> EventPhase;
}

trait PhaseGuarded {
  fn allow_phase(&self, phase: EventPhase) -> bool;
}

struct DefaultPhaseGuard;

impl PhaseGuarded for DefaultPhaseGuard {
  fn allow_phase(&self, phase: EventPhase) -> bool {
    matches!(phase, EventPhase::Failed | EventPhase::Retry)
  }
}

// 明确哪些事件不产生 Retry 阶段
fn should_emit_retry(event: &EventType) -> bool {
  !matches!(
    event,
    EventType::RequestMiddleware
      | EventType::ResponseMiddleware
      | EventType::Parser
      | EventType::SystemError
      | EventType::SystemHealth
  )
}

trait StrategyResolver {
  fn resolve(domain: EventDomain, event: &EventType, phase: EventPhase, err: &Error) -> Decision;
}

struct DefaultResolver;

impl StrategyResolver for DefaultResolver {
  fn resolve(domain: EventDomain, event: &EventType, phase: EventPhase, err: &Error) -> Decision {
    let kind = err.kind();
    let policy = match (domain, event, phase, kind) {
      (EventDomain::Engine, EventType::TaskModel, EventPhase::Failed | EventPhase::Retry, ErrorKind::Task)
      | (EventDomain::Engine, EventType::TaskModel, EventPhase::Failed | EventPhase::Retry, ErrorKind::Module) => Policy {
        retryable: true,
        backoff: BackoffPolicy::Exponential { base_ms: 500, max_ms: 30_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Warn,
      },
      (EventDomain::Engine, EventType::ParserTaskModel, EventPhase::Failed | EventPhase::Retry, ErrorKind::Parser)
      | (EventDomain::Engine, EventType::ParserTaskModel, EventPhase::Failed | EventPhase::Retry, ErrorKind::ProcessorChain) => Policy {
        retryable: true,
        backoff: BackoffPolicy::Exponential { base_ms: 500, max_ms: 30_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Warn,
      },
      (EventDomain::Engine, EventType::RequestPublish, EventPhase::Failed | EventPhase::Retry, ErrorKind::Queue) => Policy {
        retryable: true,
        backoff: BackoffPolicy::Exponential { base_ms: 500, max_ms: 30_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Warn,
      },
      (EventDomain::Engine, EventType::RequestMiddleware, EventPhase::Failed, ErrorKind::Request)
      | (EventDomain::Engine, EventType::RequestMiddleware, EventPhase::Failed, ErrorKind::ProcessorChain) => Policy {
        retryable: false,
        backoff: BackoffPolicy::None,
        dlq: DlqPolicy::Always,
        alert: AlertLevel::Error,
      },
      (EventDomain::Engine, EventType::Download, EventPhase::Failed | EventPhase::Retry, ErrorKind::Download)
      | (EventDomain::Engine, EventType::Download, EventPhase::Failed | EventPhase::Retry, ErrorKind::Proxy)
      | (EventDomain::Engine, EventType::Download, EventPhase::Failed | EventPhase::Retry, ErrorKind::RateLimit) => Policy {
        retryable: true,
        backoff: BackoffPolicy::Exponential { base_ms: 500, max_ms: 60_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Warn,
      },
      (EventDomain::Engine, EventType::ResponseMiddleware, EventPhase::Failed, ErrorKind::Response)
      | (EventDomain::Engine, EventType::ResponseMiddleware, EventPhase::Failed, ErrorKind::ProcessorChain) => Policy {
        retryable: false,
        backoff: BackoffPolicy::None,
        dlq: DlqPolicy::Always,
        alert: AlertLevel::Error,
      },
      (EventDomain::Engine, EventType::ResponsePublish, EventPhase::Failed | EventPhase::Retry, ErrorKind::Queue) => Policy {
        retryable: true,
        backoff: BackoffPolicy::Exponential { base_ms: 500, max_ms: 30_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Warn,
      },
      (EventDomain::Engine, EventType::ModuleGenerate, EventPhase::Failed | EventPhase::Retry, ErrorKind::Module)
      | (EventDomain::Engine, EventType::ModuleGenerate, EventPhase::Failed | EventPhase::Retry, ErrorKind::Task) => Policy {
        retryable: err.is_timeout() || err.is_connect(),
        backoff: BackoffPolicy::Exponential { base_ms: 500, max_ms: 30_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Error,
      },
      (EventDomain::Engine, EventType::Parser, EventPhase::Failed | EventPhase::Retry, ErrorKind::Parser) => Policy {
        retryable: false,
        backoff: BackoffPolicy::None,
        dlq: DlqPolicy::Always,
        alert: AlertLevel::Error,
      },
      (EventDomain::Engine, EventType::MiddlewareBefore, EventPhase::Failed, ErrorKind::DataMiddleware) => Policy {
        retryable: err.is_timeout() || err.is_connect(),
        backoff: BackoffPolicy::Linear { base_ms: 200, max_ms: 10_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Warn,
      },
      (EventDomain::Engine, EventType::DataStore, EventPhase::Failed, ErrorKind::DataStore)
      | (EventDomain::Engine, EventType::DataStore, EventPhase::Failed, ErrorKind::Orm) => Policy {
        retryable: err.is_timeout() || err.is_connect(),
        backoff: BackoffPolicy::Linear { base_ms: 200, max_ms: 10_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Error,
      },
      (EventDomain::System, EventType::SystemError, EventPhase::Failed, _) => Policy {
        retryable: false,
        backoff: BackoffPolicy::None,
        dlq: DlqPolicy::Never,
        alert: AlertLevel::Error,
      },
      _ => Policy {
        retryable: true,
        backoff: BackoffPolicy::Exponential { base_ms: 500, max_ms: 30_000 },
        dlq: DlqPolicy::OnExhausted,
        alert: AlertLevel::Warn,
      },
    };

    Decision {
      policy,
      reason: format!("{:?}/{:?}/{:?}/{:?}", domain, event, phase, kind),
    }
  }
}

// 统一入口：事件处理器调用，按 phase 约束策略决策
fn decide_policy<E: EventKey>(event: &E, err: &Error) -> Option<Decision> {
  let guard = DefaultPhaseGuard;
  if !guard.allow_phase(event.phase()) {
    return None;
  }
  Some(DefaultResolver::resolve(
    event.domain(),
    &event.event_type(),
    event.phase(),
    err,
  ))
}
```

核心数据结构（Rust 风格伪代码）：

```rust
#[derive(Debug, Clone)]
struct EventEnvelope<T> {
  domain: EventDomain,
  event: EventType,
  phase: EventPhase,
  data: T,
  error: Option<Error>,
  timestamp_ms: u64,
  trace_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StrategyKey {
  domain: EventDomain,
  event: EventType,
  phase: EventPhase,
  kind: ErrorKind,
}

#[derive(Debug, Clone)]
struct ErrorContext {
  task_id: Option<String>,
  module_id: Option<String>,
  request_id: Option<String>,
  queue_topic: Option<String>,
  extra: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy)]
struct RetryState {
  current: u32,
  max: u32,
}

#[derive(Debug, Clone)]
struct DlqRecord {
  reason: String,
  payload: Vec<u8>,
  headers: std::collections::BTreeMap<String, String>,
}
```

事件类型的 trait 实现示例（Rust 风格伪代码）：

```rust
enum EngineEvent {
  TaskModelReceived,
  TaskModelStarted,
  TaskModelCompleted,
  TaskModelFailed,
  TaskModelRetry,
  ParserTaskModelFailed,
  ParserTaskModelRetry,
  RequestPublishFailed,
  RequestPublishRetry,
  RequestMiddlewareFailed,
  DownloadStarted,
  DownloadCompleted,
  DownloadFailed,
  DownloadRetry,
  ResponseMiddlewareFailed,
  ResponsePublishFailed,
  ResponsePublishRetry,
  ModuleGenerateFailed,
  ModuleGenerateRetry,
  ParserFailed,
  ParserRetry,
  MiddlewareBeforeFailed,
  DataStoreFailed,
  DataStoreRetry,
}

impl EventKey for EngineEvent {
  fn domain(&self) -> EventDomain { EventDomain::Engine }

  fn event_type(&self) -> EventType {
    match self {
      EngineEvent::TaskModelReceived
      | EngineEvent::TaskModelStarted
      | EngineEvent::TaskModelCompleted
      | EngineEvent::TaskModelFailed
      | EngineEvent::TaskModelRetry => EventType::TaskModel,
      EngineEvent::ParserTaskModelFailed
      | EngineEvent::ParserTaskModelRetry => EventType::ParserTaskModel,
      EngineEvent::RequestPublishFailed
      | EngineEvent::RequestPublishRetry => EventType::RequestPublish,
      EngineEvent::RequestMiddlewareFailed => EventType::RequestMiddleware,
      EngineEvent::DownloadStarted
      | EngineEvent::DownloadCompleted
      | EngineEvent::DownloadFailed
      | EngineEvent::DownloadRetry => EventType::Download,
      EngineEvent::ResponseMiddlewareFailed => EventType::ResponseMiddleware,
      EngineEvent::ResponsePublishFailed
      | EngineEvent::ResponsePublishRetry => EventType::ResponsePublish,
      EngineEvent::ModuleGenerateFailed
      | EngineEvent::ModuleGenerateRetry => EventType::ModuleGenerate,
      EngineEvent::ParserFailed
      | EngineEvent::ParserRetry => EventType::Parser,
      EngineEvent::MiddlewareBeforeFailed => EventType::MiddlewareBefore,
      EngineEvent::DataStoreFailed
      | EngineEvent::DataStoreRetry => EventType::DataStore,
    }
  }

  fn phase(&self) -> EventPhase {
    match self {
      EngineEvent::TaskModelStarted
      | EngineEvent::DownloadStarted => EventPhase::Started,
      EngineEvent::TaskModelCompleted
      | EngineEvent::DownloadCompleted => EventPhase::Completed,
      EngineEvent::TaskModelFailed
      | EngineEvent::DownloadFailed
      | EngineEvent::ParserTaskModelFailed
      | EngineEvent::RequestPublishFailed
      | EngineEvent::RequestMiddlewareFailed
      | EngineEvent::ResponseMiddlewareFailed
      | EngineEvent::ResponsePublishFailed
      | EngineEvent::ModuleGenerateFailed
      | EngineEvent::ParserFailed
      | EngineEvent::MiddlewareBeforeFailed
      | EngineEvent::DataStoreFailed => EventPhase::Failed,
      EngineEvent::TaskModelRetry
      | EngineEvent::DownloadRetry
      | EngineEvent::ParserTaskModelRetry
      | EngineEvent::RequestPublishRetry
      | EngineEvent::ResponsePublishRetry
      | EngineEvent::ModuleGenerateRetry
      | EngineEvent::ParserRetry
      | EngineEvent::DataStoreRetry => EventPhase::Retry,
      EngineEvent::TaskModelReceived => EventPhase::Started,
    }
  }
}

enum SystemEventKind {
  ErrorOccurred,
  ErrorHandled,
  SystemStarted,
  SystemShutdown,
  ComponentHealthCheck,
}

impl SystemEventKind {
  fn domain(&self) -> EventDomain { EventDomain::System }

  fn event_type(&self) -> EventType {
    match self {
      SystemEventKind::ErrorOccurred | SystemEventKind::ErrorHandled => EventType::SystemError,
      SystemEventKind::ComponentHealthCheck => EventType::SystemHealth,
      SystemEventKind::SystemStarted | SystemEventKind::SystemShutdown => EventType::SystemHealth,
    }
  }

  fn phase(&self) -> EventPhase {
    match self {
      SystemEventKind::ErrorOccurred => EventPhase::Failed,
      SystemEventKind::ErrorHandled => EventPhase::Completed,
      SystemEventKind::SystemStarted => EventPhase::Started,
      SystemEventKind::SystemShutdown => EventPhase::Completed,
      SystemEventKind::ComponentHealthCheck => EventPhase::Completed,
    }
  }
}
```

#### P2 分析：风险与缺口

- ErrorKind 已存在但未绑定处理策略，导致重试风暴、静默丢失与告警噪音不可控。[src/errors/error.rs](src/errors/error.rs)
- Queue/Engine/Sync 的失败行为依赖局部实现，缺少统一的“错误 -> 动作”契约。
- 缺少“策略变更可验证”的测试与指标闭环，线上回归难追踪。

#### P2 落地方案（与 P0/P1 格式一致）

阶段 1：策略矩阵与分层标准（1 周）
- 定义错误分层：用户输入、外部依赖、系统资源、内部逻辑。
- 统一策略维度：是否重试、退避策略、DLQ 路由、告警等级、是否降级。

阶段 2：策略配置化与集中决策（1-2 周）
- 在 errors 或 common 中新增“策略映射表”结构，支持默认值与覆盖配置。
- Queue/Engine/Sync 在失败处理时只调用策略决策，不再散落硬编码逻辑。

阶段 3：契约测试与可观测闭环（1 周）
- 为关键 ErrorKind 增加策略契约测试（重试次数、DLQ 路由、告警等级）。
- 统一指标：重试率、DLQ 率、告警率，支持按 ErrorKind 聚合。

#### P2 当前实现与验证（进展更新）

已完成：
- 统一事件模型：`EventEnvelope` + `EventDomain`/`EventType`/`EventPhase`，并统一 `event_key` 路由。
- 事件链路落地：Engine 各链路与 EventBus/Handler 统一消费 `EventEnvelope`。
- 错误类型清理：`ErrorKind` 移除 Cookie/Header/Sync，并保持统一语义；`EventError` 可序列化。
- 策略映射与集中决策：新增 `PolicyResolver` 与默认策略映射，支持覆盖配置。[src/common/policy.rs](src/common/policy.rs) [src/common/model/config.rs](src/common/model/config.rs)
- Queue/Engine/Sync 落地统一决策入口：Queue 的 Nack 策略、Engine 的失败处理、Sync 的编解码异常均走策略决策。[src/queue/manager.rs](src/queue/manager.rs) [src/engine/engine.rs](src/engine/engine.rs) [src/sync/distributed.rs](src/sync/distributed.rs)
- 指标闭环雏形：新增 `policy_decisions_total` 统计重试/DLQ/丢弃等决策路径。[src/queue/redis.rs](src/queue/redis.rs) [src/queue/kafka.rs](src/queue/kafka.rs) [src/engine/engine.rs](src/engine/engine.rs) [src/sync/distributed.rs](src/sync/distributed.rs)
- 策略契约测试最小集：Queue/Engine/Sync 统一入口新增最小覆盖，验证策略变更可追踪。[src/queue/tests.rs](src/queue/tests.rs) [src/engine/engine.rs](src/engine/engine.rs) [src/sync/distributed.rs](src/sync/distributed.rs)
- 告警规则与聚合查询模板：基于 `policy_decisions_total` 输出 Prometheus 规则与聚合维度示例。[docs/alerts/policy_alerts.yml](docs/alerts/policy_alerts.yml)
- RetryableFailure 策略覆盖：RetryableFailure 进入统一策略决策，支持 DLQ/ACK 分流与指标统计。[src/engine/engine.rs](src/engine/engine.rs)
- 基础回归验证：`cargo test` 全量通过，确认事件链路、策略与指标接入不破坏编译与基础测试。

进行中：
- 无。

下一步（P2 剩余工作）：
- 无，进入 P3 可观测性标准化。

交付物清单（必须）
- 错误策略矩阵表（ErrorKind -> 重试/退避/DLQ/告警）。
- 策略决策代码与配置默认值。
- 契约测试与指标定义文档。

验收标准
- 所有关键 ErrorKind 均有明确策略与默认配置。
- 策略变更具备自动化测试与指标验证。
- 线上行为不再依赖局部实现的“隐式策略”。

风险与反证点（Reviewer #2）
- 若策略无法覆盖主要 ErrorKind，则矩阵无效。
- 若无指标闭环，策略无法验证或持续优化。

### P3: 可观测性标准化

目标：在 P2 指标与错误策略基础上，统一日志与指标出口，支持 console + 文本 + prometheus + 消息队列，移除数据库日志，并以统一 trait 管理。

#### P3 落地方案（日志系统）

范围与约束：
- 支持输出：console / file / prometheus / mq，删除 db sink。
- 统一入口：业务侧只依赖 Logger API，不直接依赖具体 sink。
- 配置驱动：格式、队列地址与缓存大小、prometheus 开关与端口可配置。
- 保障：队列 backpressure 不阻塞业务；支持丢弃与统计。

统一接口设计：
- LogRecord：结构化字段 + 业务上下文。
- LogSink trait：标准化 emit/flush 接口。
- Logger：负责过滤、格式化、缓冲、批量 flush、多 sink 分发。

```rust
pub trait LogSink: Send + Sync {
  fn name(&self) -> &'static str;
  fn enabled(&self) -> bool { true }
  fn emit(&self, record: LogRecord) -> Result<(), LogError>;
  fn flush(&self) -> Result<(), LogError> { Ok(()) }
}
```

统一字段（最小集）：
- time, level, module, message
- event_type, phase, error_kind
- trace_id, task_id, request_id, queue_topic
- policy.action, policy.reason, retry.count

配置结构（示例）：
```toml
[logger]
enabled = true
level = "INFO"
format = "text" # console/file only
include = ["time", "level", "module", "event_type", "error_kind", "trace_id", "task_id", "request_id"]
buffer = 10000
flush_interval_ms = 500

[[logger.outputs]]
type = "console"

[[logger.outputs]]
type = "file"
path = "logs/app.log"
rotate = "daily" # daily | size
max_size_mb = 256
max_files = 14

[[logger.outputs]]
type = "mq"
backend = "kafka" # kafka | redis
brokers = "localhost:9095"
topic = "mocra-logs"
format = "json" # mq only
buffer = 10000
batch_size = 200
compression = "lz4"

[logger.prometheus]
enabled = true
port = 9102
path = "/metrics"
```

输出实现要点：
- ConsoleSink：仅 text 格式（stdout/stderr 分流）。
- FileSink：仅 text 格式，滚动策略 daily/size，支持最大文件数。
- MqSink：仅 json 格式，Kafka/Redis 统一入口，支持 buffer + batch + drop 策略。
- PrometheusSink：同时支持指标与日志统计（日志以指标化方式呈现，如 log_emits_total/log_dropped_total），不输出原始文本日志。

指标扩展：
- policy_decisions_total（已有）。
- log_emits_total / log_dropped_total / log_queue_lag / log_batch_size（作为 Prometheus 日志支持入口）。

迁移步骤：
1) 删除 db sink 与相关配置。
2) 引入 LogSink trait 与 Logger 统一入口。
3) 替换各模块直接输出为 Logger API。
4) 更新配置与文档，并提供默认值。

#### P3 当前实现与验证（进展更新）

已完成：
- Logger 统一入口落地：新增 `LogSink`/`LogRecord`/分发器与配置映射，支持 console/file/mq/prometheus 四类输出。[src/utils/logger.rs](src/utils/logger.rs)
- 配置结构落地：新增 P3 logger 配置模型（console/file/mq/prometheus）。[src/common/model/logger_config.rs](src/common/model/logger_config.rs)
- 引擎接入：根据配置初始化 Logger + MQ 发送桥，EventBus 输出改为 MQ/Console/文件接入，移除 DB handler 注册。[src/engine/engine.rs](src/engine/engine.rs)
- MQ 日志 topic 可配置：QueueManager 支持自定义 log topic（默认 `log`）。[src/queue/manager.rs](src/queue/manager.rs)
- Prometheus 日志指标扩展：补齐 `log_queue_lag` 与 `log_batch_size` 统计。[src/utils/logger.rs](src/utils/logger.rs)
- 文档更新：配置示例与约束（MQ json-only / console/file text-only）。[docs/configuration.md](docs/configuration.md)
- 清理 DB 日志 sink：移除 `db_event_handler` 模块与引用。[src/engine/events/mod.rs](src/engine/events/mod.rs)
- Prometheus 仪表盘与告警建议：输出日志指标面板与告警规则示例。[docs/dashboards/logging_dashboard.md](docs/dashboards/logging_dashboard.md) [docs/alerts/logging_alerts.yml](docs/alerts/logging_alerts.yml)
- 压测脚本与基线：提供日志链路压测入口与基线阈值说明。[scripts/logging_load_test.ps1](scripts/logging_load_test.ps1) [scripts/logging_load_test.sh](scripts/logging_load_test.sh) [tests/src/log_stress.rs](tests/src/log_stress.rs)
- 回归验证：`cargo test` 全部通过。
- 最近验证：`cargo test`，`cargo run -p tests --bin log_stress -- --total 200000 --concurrency 4 --buffer 20000`。

进行中：
- 无。

下一步（P3 剩余工作）：
- 无。

验收标准：
- console/file/mq/prometheus 均可按配置启停。
- 业务侧不依赖具体 sink，统一入口可替换。
- 队列拥塞时可控丢弃，且有指标可观察。

#### P3 生产可用性检查清单（当前状态：未满足）

必须通过：
- 日志链路压测：在峰值负载下验证 MQ 队列滞后、丢弃比例与吞吐稳定性。
- 观测闭环：Prometheus 日志指标的仪表盘与告警阈值可用，并经过验证。
- 配置与回滚：生产配置样例、变更流程与回滚策略明确（含 json-only/text-only 约束）。
- 集成冒烟：删除 db sink 后的完整链路冒烟与一次灰度演练。

#### P3 生产配置与回滚（补齐项）

生产配置示例（仅日志相关）：
```toml
[logger]
enabled = true
level = "INFO"
format = "text" # console/file only
buffer = 20000
flush_interval_ms = 500

[[logger.outputs]]
type = "console"
level = "INFO"

[[logger.outputs]]
type = "file"
path = "logs/app.log"
rotation = "daily"

[[logger.outputs]]
type = "mq"
backend = "kafka"
topic = "mocra-logs"
format = "json" # mq only
buffer = 20000
kafka = { brokers = "localhost:9095" }

[logger.prometheus]
enabled = true
```

回滚策略（最小集）：
- 关闭 MQ 输出：移除 `logger.outputs` 中 `type = "mq"` 段落，或将 `enabled = false`。
- 降级为本地：仅保留 `console/file` 输出，保持 `format = "text"`。
- 快速止损：设置 `DISABLE_LOGS=true` 或 `MOCRA_DISABLE_LOGS=true` 临时关闭日志。

#### P3 压测与基线（补齐项）

压测入口：
- PowerShell: `scripts/logging_load_test.ps1`
- Bash: `scripts/logging_load_test.sh`

基线阈值（起始建议，可按机器规格调整）：
- 吞吐 >= 20000 logs/sec
- 丢弃率 < 0.5%
- `log_queue_lag` 持续 < 5000

最近压测结果：
- `cargo run -p tests --bin log_stress -- --total 200000 --concurrency 4 --buffer 20000`
- 输出：`total=200000 consumed=20258 dropped=179742 drop_rate=0.8987 max_lag=20000 elapsed_ms=636 rate=314152/s`
- 结论：吞吐达标，但丢弃率与 `log_queue_lag` 明显超出基线阈值，需要定位与优化。

#### P3 仪表盘与告警（补齐项）

PromQL 面板示例：
- [docs/dashboards/logging_dashboard.md](docs/dashboards/logging_dashboard.md)

告警规则示例：
- [docs/alerts/logging_alerts.yml](docs/alerts/logging_alerts.yml)

### P4: SLO 与基准体系

明确 P95/P99 延迟、吞吐、恢复目标，并提供可复现基准脚本。
