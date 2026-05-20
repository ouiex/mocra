# 重构推进方案

## 1. 目标

本方案用于定义本轮重构的**直接切换路线**。  
目标不是维持历史 API 或运行时兼容，而是在明确的 cutover 窗口内把系统切到目标架构。

本轮重构必须满足：

1. 模块边界先冻结，再进入实现。
2. 同一阶段的输出直接服务最终架构，不为兼容层额外设计。
3. 不允许同一 run 跨两套 profile/schema/runtime 继续执行。
4. 不允许旧 worker 与新 worker 混跑同一套 topic。

## 2. 切换前提

本方案基于以下前提：

1. 允许在 cutover 窗口内执行 **drain + 停机/重启 + 重新上线**。
2. 历史 API route、历史 DTO、历史消息格式不作为必须保留的能力。
3. 若存在历史 queue / DLQ / blob 数据，优先采用离线迁移、清队或定点回放，而不是在新 runtime 中继续扩展兼容逻辑。
4. Dashboard、运维脚本和调用方 SDK 在相邻阶段同步切换，不依赖运行时双写。

## 3. 推进阶段

### Phase 0：冻结目标边界

**目标**：先把目标模块边界、目标路由、目标 schema 一次定清楚。

内容：

- 冻结 `control_plane / workflow / runtime / scheduler / transport / pipeline / cluster / observability / bootstrap` 九大模块边界；
- 冻结目标 API 路由树；
- 冻结 queue envelope、DLQ envelope、metrics contract；
- 输出 cutover checklist。

阶段出口：

- 主架构文档与 `development_docs` 一致；
- `04_task_breakdown.md` 与 `development_path.md` 可直接驱动实施。

### Phase 1：建立 canonical model

**目标**：先把目标领域模型建完整。

内容：

- `TaskProfileSnapshot / ResolvedNodeConfig`
- `WorkflowDefinition / NodeSpec / EdgeSpec`
- `RoutingMeta / ExecutionMeta / NodeInput / NodeParseOutput`
- `QueueEnvelope / TaskDispatchEnvelope / NodeDispatchEnvelope / DeadLetterEnvelope`

阶段出口：

- 新模型足以表达目标态运行时；
- 不再需要为未来路径继续设计 `ModuleConfig / MetaData` 新字段。

### Phase 2：切换模块接口与 workflow 编译

**目标**：让模块侧直接说目标接口语言。

内容：

- 重写 `ModuleTrait` 静态 DSL；
- 重写 `ModuleNodeTrait` 为 context 接口；
- 实现 `WorkflowCompiler`；
- 重写 `TaskFactory` 为 `ProfileLoader + WorkflowCompiler + NodeConfigResolver`。

阶段出口：

- 节点只接收 typed context；
- workflow 编译和 node config 切片已经是主路径。

### Phase 3：建立 DAG scheduler 核心

**目标**：先让 run state、guard、placement 成型，再接主链路。

内容：

- 实现 `DagRunStateStore`
- 实现 `DagRunGuard`
- 实现 `DagFencingStore`
- 实现 `DagScheduler`
- 实现 `DagNodeDispatcher + NodePlacement`
- 固化 retry / stop / recovery / fan-out 语义

阶段出口：

- run state 不再依附于 `ModuleDagProcessor`；
- 调度器可以独立回答“谁拥有 run、下一步该派发什么、如何恢复”。

### Phase 4：切换 transport 与 queue schema

**目标**：让消息总线只传目标 envelope。

内容：

- 重写 `QueueManager` 编码层；
- 重做 topic registry、batch policy、backpressure contract；
- 重做 blob、DLQ、requeue contract；
- 将 task/parser/error topic 全部切到新 schema。

切换边界：

- 新 consumer 启动前，旧 topic 先 drain 或重建；
- 不允许 mixed-version queue consumer。

阶段出口：

- 所有 processor 只读写目标 envelope；
- DLQ 与 blob 路径使用结构化 payload。

### Phase 5：切换执行流水线

**目标**：把执行主链路完全换到 scheduler + transport + typed context 上。

内容：

- 重写 task ingress processor；
- 重写 request/download/response processor；
- 重写 parser/error processor；
- 把 local fast path 收口为 scheduler policy。

阶段出口：

- 主链路不再依赖旧 parser output、旧 metadata 结构；
- request/response/parse/error 全部通过新 runtime contract 驱动。

### Phase 6：切换控制面 API

**目标**：控制面一次切到目标路由树。

内容：

- 落地 `/config/*`
- 落地 `/tasks/dispatch`
- 落地 `/cluster/*`
- 落地 `/control/*`
- 落地 `/debug/*`
- 落地 `/dlq/messages*`

切换边界：

- 调用方脚本与 SDK 在本阶段同步切换；
- 历史路由直接下线，不做 alias。

阶段出口：

- 所有配置写入都走 profile write path；
- 所有集群与死信管理都走目标态接口。

### Phase 7：切换指标与运维契约

**目标**：运维面与 runtime 一起切到新观测模型。

内容：

- 建立 `MetricsScope`
- 建立 `MetricsEmitter`
- 主链路接入 L1 五件套
- queue / DAG / API / config / cluster 接入 L2/L3 指标
- 更新 dashboard、alert、scrape contract

切换边界：

- Dashboard 与 alert 在同阶段切换到新指标查询；
- 不要求 runtime 双写旧指标名。

阶段出口：

- `/metrics` 只以目标态指标族为核心输出；
- 运维面可直接基于新指标排障。

### Phase 8：收口启动装配并完成切换

**目标**：用目标模块重组 `State/Engine`，删除 legacy 主路径。

内容：

- 重写 bootstrap / lifecycle / runtime wiring；
- 把 Redis 时代兼容路径从热路径中移除；
- 删除历史 route、历史类型和无效 bridge；
- 执行 cutover rehearsal 与上线检查。

阶段出口：

- `State/Engine` 只装配目标模块；
- 历史 API 与历史运行时模型不再留在主仓库主路径。

## 4. 关键风险

| 风险 | 说明 | 控制方式 |
|------|------|----------|
| mixed-version runtime | 旧 worker 与新 worker 共同消费导致语义撕裂 | cutover 前统一 drain、停机、重启 |
| 历史 queue / DLQ 残留 | 新 schema 上线后残留旧消息不可解释 | 先离线迁移、重建 topic 或清队 |
| 调用方仍访问旧路由 | API 切换后脚本/SDK 直接失败 | 在 Phase 6 前冻结新路由并同步客户端 |
| dashboard 查询失效 | 指标切换后看板与告警失真 | 在 Phase 7 同步更新查询，不依赖双写 |
| run 跨版本续跑 | 进行中的 run 在切换后继续消费旧上下文 | cutover 前 drain run，必要时显式终止旧 run |
| Redis 时代装配残留 | 新模块已完成但 bootstrap 仍把旧组件挂在热路径 | H01/H02 阶段集中收口启动装配 |

## 5. 阶段门槛

每个阶段进入下一阶段前，至少满足：

1. `cargo build`
2. `cargo test --lib`
3. 本阶段新增测试通过
4. 对应 cutover 演练或 smoke case 通过
5. 文档同步更新

## 6. 失败处理原则

本方案不依赖运行时兼容层兜底。若某阶段实现失败：

1. **在 cutover 前失败**：继续在开发分支修正，不对线上作兼容性承诺。
2. **在 cutover rehearsal 失败**：停止上线，修正文档、脚本和 schema 后重新演练。
3. **在正式切换后失败**：回退到上一个稳定部署快照，而不是在新代码里补历史兼容层。
