# DAG 两阶段分布式改造开发文档

## 1. 文档目的

本文档用于把设计方案转化为可执行研发计划，覆盖以下内容：

1. 目标范围与阶段边界。
2. 代码改造点与接口变更。
3. 开发任务拆分与交付标准。
4. 测试、灰度、回滚与上线清单。

适用对象：架构、后端开发、测试、运维、值班负责人。

---

## 2. 关联设计文档

基线方案：

1. docs/design/module_node_dag_refactor_plan.md

本文默认遵循该方案中的两阶段路径：

1. 阶段1：Module 级别分布式，DAG 仅在单节点内运行。
2. 阶段2：ModuleNode 级别分布式，跨节点分布式 DAG。

---

## 3. 开发总原则

1. 兼容优先：新能力必须可开关、可回滚。
2. 双轨运行：阶段1稳定后再进入阶段2灰度。
3. 幂等先行：重试与重分发必须先定义幂等边界。
4. 观测先行：每个里程碑必须有可观测与告警。
5. 变更最小化：优先复用现有链路，不做无必要重构。
6. Rust 优先：DAG 内核必须优先实现 Rust 版本，不依赖外部图执行引擎。
7. 模型统一：ModuleNode 必须直接作为 DAG 节点实体，不新增平行节点抽象。

---

## 4. 现有代码映射

以下为本次改造的核心代码落点（按职责）：

1. 执行入口与任务模型
   - src/engine/task/task.rs
   - src/engine/task/task_manager.rs
2. Module 运行与装配
   - src/engine/task/module.rs
   - src/engine/task/assembler.rs
3. Module 内链路执行器
   - src/engine/task/module_processor_with_chain.rs
4. 解析链与队列投递
   - src/engine/chain/parser_chain.rs
   - src/queue/manager.rs
5. 上下文与消息契约
   - src/common/model/context.rs
   - src/common/model/message.rs
6. 分布式键空间
   - src/common/model/chain_key.rs

说明：阶段1以复用以上链路为主；阶段2在上述文件基础上增量扩展 node 级能力。

---

## 5. 阶段1开发方案（Module 级别分布式）

## 5.1 阶段目标

1. 固化 Module 作为分布式调度单元。
2. 保持 DAG 在单执行节点内运行。
3. 保证现网行为不劣化。
4. 完成 Rust 版 DAG 内核首版落地，并以 ModuleNode 作为节点。

## 5.2 功能清单

1. 调度语义收敛
   - 明确 Module 是唯一跨节点迁移单位。
   - 禁止 ModuleNode 被单独投递到远端执行节点。
2. Rust DAG 内核落地
   - 在 Rust 代码中实现 DAG 数据结构、拓扑校验、节点路由。
   - 节点对象直接复用 ModuleNodeTrait 实现体。
   - 图节点 ID 与 ModuleNode 运行身份一一对应。
2. 上下文兼容
   - 保留 step 字段兼容逻辑。
   - 保留 node 字段预埋但不启用跨节点路由。
3. 错误链确认
   - generate/parser 错误继续走 ErrorTask 路径。
   - 阈值判定保持模块级优先、任务级兜底。
4. 可观测补齐
   - 增加 Module 调度、重试、终止核心指标。

## 5.3 接口与配置

新增配置项：

1. dag_phase1_enabled
2. dag_phase1_module_runtime_mode（可选，local_only 或 compat）

配置来源建议：

1. 全局配置文件。
2. 环境变量覆盖。
3. 运行时管理接口（若已有配置热更新通道）。

## 5.4 代码改造任务

任务 P1-00：Rust DAG 内核优先实现

1. 文件：src/engine/task/module_processor_with_chain.rs
2. 文件：src/common/interface/module.rs
3. 动作：
   - 在执行器内新增 Rust DAG 结构与拓扑检查流程。
   - 将 ModuleNodeTrait 实例直接映射为 DAG 节点，不引入额外节点包装层。
   - 支持线性 step 到 DAG 的兼容构图（node_i -> node_{i+1}）。
4. 验收：
   - 启动时可完成图校验（含环检测）。
   - 旧模块无需改业务代码即可运行在 Rust DAG 内核上。

任务 P1-01：调度语义收敛

1. 文件：src/engine/task/task_manager.rs
2. 动作：统一 Module 维度投递路径，清理潜在 node 级跨节点分派分支。
3. 验收：任务流中不存在 ModuleNode 级远程投递日志。

任务 P1-02：Module 执行边界加固

1. 文件：src/engine/task/module.rs
2. 动作：在 Module 生命周期中显式标注单节点内 DAG 执行边界。
3. 验收：日志可定位每个 Module 的执行节点与 run_id。

任务 P1-03：执行器兼容保障

1. 文件：src/engine/task/module_processor_with_chain.rs
2. 动作：保留 step 兼容行为，禁用任何跨节点 node 调度假设，并通过 Rust DAG 内核统一执行路径。
3. 验收：回归用例全通过，链路无行为漂移。

任务 P1-04：指标与告警

1. 文件：src/engine/chain/parser_chain.rs
2. 文件：src/common/status_tracker.rs 或对应指标模块
3. 动作：增加 Module 分发成功率、重试率、终止率指标。
4. 验收：监控看板能分 run_id/module_id 展示。

---

## 6. 阶段2开发方案（ModuleNode 级别分布式）

## 6.1 阶段目标

1. 升级分布式调度单元到 ModuleNode。
2. 实现跨节点 DAG 路由、分叉、汇聚与恢复。
3. 引入 Spark 参考机制优化聚合重分发。
4. 复用阶段1已落地的 Rust DAG 内核，仅扩展分布式调度与状态同步能力。

## 6.2 阶段拆分

阶段2-A：基础路由层

1. node_id 主路由上线（灰度）。
2. 节点级上下文与幂等框架上线。
3. Rust DAG 节点调度器接入远端执行通道。

阶段2-B：分叉汇聚层

1. fan-out 和 fan-in 能力上线。
2. barrier 释放一致性上线。

阶段2-C：聚合增强层（Spark 参考）

1. 两阶段聚合上线。
2. 重分区、重分发、skew 治理上线。

## 6.3 上下文与消息契约变更

目标字段：

1. run_id
2. module_id
3. node_id
4. branch_token
5. prefix_request
6. attempt
7. epoch（可选）

相关文件：

1. src/common/model/context.rs
2. src/common/model/message.rs

开发要求：

1. 新字段必须向后兼容。
2. 反序列化必须允许旧消息缺失新字段。
3. 关键字段缺失时必须降级到安全路径。
4. node_id 必须与 ModuleNode 节点标识保持一致，不允许二次映射。

## 6.4 键空间变更

相关文件：

1. src/common/model/chain_key.rs

新增键函数建议：

1. dag_node_state_key
2. dag_edge_gate_key
3. dag_barrier_arrive_key
4. dag_barrier_release_key
5. dag_dedup_key

开发要求：

1. 键格式稳定，不得在灰度期变更。
2. 统一 TTL 策略，避免状态泄漏。
3. 若存在旧键，迁移期支持新旧并读。

## 6.5 分叉与汇聚实现

实现目标：

1. fan-out：一个 node 可生成多个下游 node 任务。
2. fan-in：汇聚节点仅在满足条件时触发。

关键点：

1. barrier 到达记录必须可幂等。
2. barrier 释放必须使用原子条件（NX 或 Lua）。
3. 汇聚超时需定义兜底策略（重试、跳过或终止）。

## 6.6 Spark 参考机制落地

组件建议：

1. LocalCombiner
   - 本地 partial 聚合。
2. PartitionRouter
   - 按 key 分区路由。
3. FinalAggregator
   - 分区 final 聚合与落盘。
4. SkewController
   - 热 key 识别与 salting。

关键算法：

1. 分区函数：partition = hash(key) mod N。
2. 重分发：失败任务按同分区重试，attempt 递增。
3. 去重：event_id 级幂等键。

---

## 7. 详细任务分解（可入迭代）

## 7.1 阶段1任务

1. P1-T0 Rust DAG 内核实现
   - 输出：Rust 原生 DAG 执行骨架、拓扑校验、ModuleNode 节点映射。
2. P1-T1 调度路径收敛
   - 输出：Module 级分发逻辑固定。
3. P1-T2 执行边界日志
   - 输出：run_id/module_id/worker_id 贯通日志。
4. P1-T3 配置开关接入
   - 输出：dag_phase1_enabled 生效。
5. P1-T4 回归测试
   - 输出：现有关键链路回归报告。

## 7.2 阶段2任务

1. P2-T1 上下文字段扩展
2. P2-T2 node 路由主干
3. P2-T3 fan-out/fan-in 与 barrier
4. P2-T4 dedup 和幂等写入
5. P2-T5 Spark 参考聚合重分发
6. P2-T6 skew 治理
7. P2-T7 灰度与回滚演练
8. P2-T8 收口与文档归档

每个任务必须包含：

1. PRD 说明
2. 技术方案
3. 回滚方案
4. 测试报告
5. 监控截图或指标结果

---

## 8. 测试计划

## 8.1 单元测试

1. 键函数稳定性。
2. 上下文兼容序列化。
3. barrier 原子释放。
4. dedup 去重命中。

## 8.2 集成测试

1. 阶段1全链路回归。
2. 阶段2跨节点路由。
3. fan-out/fan-in 全流程。
4. 聚合重分发失败恢复。

## 8.3 压测测试

1. 热 key 压测。
2. 分区倾斜压测。
3. 重试风暴压测。
4. 长稳压测（至少 24 小时）。

## 8.4 验收指标

1. 丢任务率接近 0。
2. 重复处理率低于阈值。
3. p95、p99 延迟满足 SLO。
4. 重试成功率满足目标。

---

## 9. 上线策略

## 9.1 发布顺序

1. 全量启用阶段1。
2. 阶段2白名单灰度。
3. 阶段2聚合重分发子能力灰度。

## 9.2 开关清单

1. dag_phase1_enabled
2. dag_phase2_enabled
3. dag_phase2_modules
4. dag_aggregation_redispatch_enabled

## 9.3 回滚顺序

1. 关闭聚合重分发。
2. 关闭阶段2。
3. 回退到阶段1稳定模式。

## 9.4 回滚触发条件

1. 错误率明显高于基线。
2. 重复处理率异常升高。
3. 队列积压持续恶化。
4. barrier 超时持续增长。

---

## 10. 运维与值班手册要求

需要补充到运维文档或 runbook：

1. 开关切换 SOP。
2. 异常分流与降级策略。
3. 快速回滚步骤。
4. 关键指标阈值与告警级别。
5. 值班排障流程与责任人。

建议新增 runbook 文档：

1. DAG_phase1_runbook.md
2. DAG_phase2_runbook.md
3. aggregation_redispatch_runbook.md

---

## 11. 里程碑与交付件

里程碑 M1（阶段1完成）：

1. 代码交付。
2. 测试报告。
3. 监控看板。
4. 灰度和回滚演练记录。
5. Rust DAG 内核设计与实现说明（含 ModuleNode 节点建模说明）。

里程碑 M2（阶段2基础完成）：

1. node 路由、barrier、dedup。
2. 白名单灰度报告。
3. 线上稳定性报告。

里程碑 M3（阶段2增强完成）：

1. Spark 参考聚合重分发上线。
2. skew 治理报告。
3. 性能收益评估报告。

---

## 12. 风险清单

1. 阶段切换期间语义不一致。
2. 分布式 DAG 重复执行。
3. 聚合热点导致局部雪崩。
4. 回滚流程不熟导致恢复超时。

风险缓解：

1. 强制开关分层。
2. 强制幂等与 dedup。
3. 强制演练后上线。
4. 强制里程碑验收门禁。

---

## 13. 验收清单

上线前必须全部满足：

1. 设计评审通过。
2. 安全评审通过。
3. 测试报告完整。
4. 监控和告警已接入。
5. 回滚演练完成。
6. 值班手册更新完成。
7. 负责人签字确认。
8. Rust DAG 内核验证通过（图校验、节点映射、兼容路径）。

---

## 14. 版本维护建议

1. 每次里程碑完成后更新本文档版本号。
2. 关键字段变更必须记录兼容策略。
3. 所有开关变更必须记录默认值与生效范围。
4. 所有异常复盘结论应沉淀到 runbook。

建议版本号格式：

1. v1.0 阶段1开发版
2. v2.0 阶段2基础版
3. v2.1 阶段2增强版

---

## 15. Rust DAG 内核最小接口草案

本节用于提供可直接编码的最小接口设计，确保 P1-T0 能快速落地。

## 15.1 设计目标

1. 用 Rust 原生结构实现 DAG 执行内核。
2. ModuleNodeTrait 实例直接作为 DAG 节点处理器。
3. 支持从线性 step 自动构图为 DAG。
4. 预留阶段2分布式扩展点，不影响阶段1上线。

## 15.2 建议模块划分

建议在 src/engine/task 下新增或收敛以下内部模块：

1. dag/graph.rs
   - 图结构、拓扑校验、出入边查询。
2. dag/runtime.rs
   - 节点调度、推进、终止判断。
3. dag/context.rs
   - DAG 执行上下文模型。
4. dag/errors.rs
   - DAG 错误码与错误类型。

若当前仓库倾向减少文件，可先合并在 module_processor_with_chain.rs，后续再拆分。

## 15.3 最小 trait 草案

1. DagNodeId
   - 目标：统一节点 ID 类型。
   - 建议：type DagNodeId = String。

2. DagNodeExecutor
   - 目标：抽象节点执行能力。
   - 要求：
     - generate 对应 ModuleNodeTrait.generate。
     - parse 对应 ModuleNodeTrait.parser。
   - 兼容：直接由 Arc<dyn ModuleNodeTrait> 适配。

3. DagGraphBuilder
   - 目标：构图接口。
   - 能力：
     - add_node(node_id, executor)
     - add_edge(from, to)
     - set_entry(node_id)
     - build_and_validate()

4. DagScheduler
   - 目标：运行时调度接口。
   - 能力：
     - route(context) -> current node
     - next_nodes(result, context) -> Vec<node_id>
     - is_terminal(node_id)

## 15.4 核心结构体草案

1. DagNode
   - node_id: String
   - executor: Arc<dyn ModuleNodeTrait>
   - retryable: bool

2. DagEdge
   - from: String
   - to: String
   - priority: u8（可选）
   - condition_tag: Option<String>（阶段1可空）

3. DagGraph
   - nodes: HashMap<String, DagNode>
   - out_edges: HashMap<String, Vec<DagEdge>>
   - in_degree: HashMap<String, usize>
   - entry_nodes: Vec<String>

4. DagExecutionContext
   - run_id
   - module_id
   - node_id
   - step_idx（兼容字段）
   - prefix_request
   - attempt
   - branch_token（阶段1可空）

5. DagExecutionResult
   - generated_requests
   - parser_tasks
   - error_task
   - next_node_ids

## 15.5 状态机草案

建议最小状态机：

1. Ready
2. Generating
3. Parsing
4. Advancing
5. WaitingBarrier（阶段2使用）
6. Completed
7. Failed

状态转移（阶段1）：

1. Ready -> Generating
2. Generating -> Parsing
3. Parsing -> Advancing
4. Advancing -> Generating（有后继节点）
5. Advancing -> Completed（无后继节点）
6. 任意状态 -> Failed（不可恢复错误）

## 15.6 错误码草案

建议错误分类：

1. DagBuildError
   - NodeNotFound
   - DuplicateNodeId
   - EdgeInvalid
   - EmptyEntry
2. DagValidationError
   - CycleDetected
   - UnreachableNode
3. DagRuntimeError
   - RouteMiss
   - NodeExecuteFailed
   - ParserFailed
   - AdvanceConflict

要求：错误码必须可观测，日志中输出 run_id/module_id/node_id。

## 15.7 线性兼容构图规则

用于老模块平滑迁移：

1. 输入：Vec<Arc<dyn ModuleNodeTrait>>
2. 构图：
   - node_0, node_1 ... node_n
   - edge(node_i -> node_{i+1})
   - entry = node_0
3. context.step_idx 与 node_id 双写：
   - node_i 对应 step_idx = i

## 15.8 关键实现顺序

建议编码顺序：

1. 先实现图结构与校验（含环检测）。
2. 再实现线性兼容构图。
3. 再将现有执行器切换到 DagScheduler。
4. 最后接入指标与错误码。

每一步都必须有单测覆盖。

## 15.9 最小单测清单

1. 构图校验
   - 空图报错。
   - 重复 node_id 报错。
   - 环检测命中。
2. 兼容构图
   - 线性节点自动生成正确边。
   - step_idx 与 node_id 对齐。
3. 运行时路由
   - 有后继时推进。
   - 无后继时结束。
4. 错误路径
   - generate 失败进入失败态。
   - parser 失败进入错误任务路径。

## 15.10 阶段2扩展保留点

阶段1实现时必须预留：

1. branch_token 字段。
2. barrier 状态接口。
3. node 级 dedup 键拼接入口。
4. 分区重分发 hook（供 Spark 参考策略接入）。

若以上保留点缺失，阶段2将出现较大返工风险。
