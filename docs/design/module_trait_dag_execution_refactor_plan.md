# ModuleTrait 返回 DAG 的改造方案（增强版）

## 1. 背景与目标

当前链路以 `ModuleTrait.add_step() -> Vec<ModuleNodeTrait>` 为主，执行器在 `module_processor_with_chain` 内按 step 推进。该模式在表达分叉/汇聚、跨节点调度、一致性治理方面成本较高。

本方案目标：

1. `ModuleNodeTrait` 成为 DAG 的业务执行节点。
2. `ModuleTrait` 直接返回一个 DAG 定义，而不是返回线性 step 列表。
3. 上层（TaskManager/Engine）统一管理并执行 DAG（含 run guard、fencing、resume、remote dispatch）。
4. 保持对历史 `add_step()` 模块的兼容迁移能力。

非目标（本阶段不做）：

1. 不重写 `schedule::dag` 核心调度器，仅做模块层接入与编排收敛。
2. 不在第一阶段改变 `ModuleNodeTrait.generate/parser` 的业务语义。
3. 不一次性移除 `module_processor_with_chain`，仅做兼容桥接与逐步收敛。

---

## 2. 设计结论

1. 保留 `ModuleNodeTrait` 作为业务节点接口，不新增平行业务节点抽象。
2. 在 `ModuleTrait` 增加 `build_dag(...)` 接口，返回模块 DAG 定义。
3. 在上层新增 `ModuleDagOrchestrator`（可并入现有 task manager），负责：
   - DAG 构建
   - `DagScheduler` 配置注入
   - 执行与结果回传
4. 提供适配器：`ModuleNodeDagAdapter`，把 `ModuleNodeTrait` 挂接到调度层 `DagNodeTrait`。
5. 迁移分两阶段：
   - 阶段 A：兼容模式，`add_step()` 自动转线性 DAG。
   - 阶段 B：模块显式实现 `build_dag()`，逐步淘汰 `add_step()`。

补充结论（关键治理点）：

1. DAG 拓扑错误前置到构建期失败，不进入运行期。
2. 上层统一定义失败策略（fail-fast、部分容错、重试预算）。
3. 所有节点执行必须带统一执行上下文（`run_id/module_id/node_id/attempt`）。
4. 兼容路径和新路径共享同一套 `DagScheduler` 能力，不允许并行维护两套调度语义。

---

## 3. 当前痛点

1. 线性 step 为主，表达分叉/汇聚依赖隐式 metadata 约定，维护成本高。
2. 执行与调度耦合在 `module_processor_with_chain`，远程执行与一致性能力难复用。
3. 业务模块无法显式声明图结构，拓扑错误较晚暴露。
4. 上层无法统一治理模块执行策略（重试、超时、run guard、fencing）。

---

## 4. 目标架构

```text
ModuleTrait::build_dag(ctx)
    -> ModuleDagDefinition (nodes/edges/policies)
    -> ModuleDagOrchestrator
        -> build schedule::dag::Dag
        -> inject dispatcher/run_guard/fencing/resume
        -> DagScheduler::execute_parallel()
        -> map outputs/events to existing task pipeline
```

分层职责：

1. 模块层：声明 DAG 结构与节点实现（业务语义）。
2. 调度层：并发执行、重试、超时、singleflight、远程分发（一致性语义）。
3. 编排层：组装上下文、注入运行时依赖、执行结果落地（平台语义）。

分布式补充原则：

1. 一致性优先于吞吐：涉及重复执行风险时默认保守。
2. 调度无状态化：编排器实例可横向扩缩容，不依赖本地内存状态。
3. 状态外置与可恢复：运行态必须可持久化并可从任意实例恢复。
4. 显式失败域：节点失败、实例失败、机房失败分别建模并隔离影响面。

建议的调用链（与现有代码结构对齐）：

```text
TaskManager / TaskFactory
  -> ModuleRuntime::prepare()
  -> ModuleDagOrchestrator::execute(module, runtime_ctx)
   -> module.build_dag(build_ctx)
   -> ModuleDagCompiler::compile(def)
   -> DagScheduler::new(dag)
      .with_dispatcher(...)
      .with_run_guard(...)
      .with_fencing_store(...)
      .with_run_state_store(...)
      .execute_parallel(...)
   -> EventMapper::to_task_events(report)
```

---

## 5. 接口改造方案

## 5.1 ModuleTrait 增强

在 `src/common/interface/module.rs` 中新增 DAG 构建入口（示意）：

```rust
#[async_trait]
pub trait ModuleTrait: Send + Sync {
    // 现有接口保留
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> { vec![] }

    // 新增：返回模块 DAG 定义
    async fn build_dag(&self, ctx: ModuleDagBuildContext) -> Result<ModuleDagDefinition> {
        // 默认兼容：把 add_step() 转线性 DAG
        ModuleDagDefinition::from_linear_steps(self.add_step().await)
    }
}
```

说明：

1. `build_dag` 为主接口。
2. `add_step` 在兼容期保留，最终标记 deprecated。

建议补充的上下文结构（示意）：

```rust
pub struct ModuleDagBuildContext {
   pub module_id: String,
   pub run_id: uuid::Uuid,
   pub config: std::sync::Arc<ModuleConfig>,
   pub task_meta: serde_json::Map<String, serde_json::Value>,
   pub login_info: Option<LoginInfo>,
   pub force_linear_compat: bool,
}
```

约束：

1. `build_dag()` 必须幂等，不依赖可变全局状态。
2. `build_dag()` 不执行 I/O（仅构建定义）；实际 I/O 在节点执行时进行。
3. 节点 ID 必须在模块内唯一，且不得使用保留 ID：`__start__`、`__end__`。

## 5.2 ModuleNodeTrait 作为 DAG 节点

`ModuleNodeTrait` 不改业务语义，仍负责 `generate/parser`。

通过适配器将其映射到 `schedule::dag::DagNodeTrait`：

```text
ModuleNodeDagAdapter(ModuleNodeTrait)
  input : NodeExecutionContext + ModuleRuntimeContext
  run   : generate -> downloader/parser chain -> TaskOutputEvent
  output: TaskPayload(标准封装)
```

适配器负责：

1. 从 `TaskPayload` 反序列化上游输出。
2. 组装 `ExecutionMark`（module_id/node_id/attempt）。
3. 执行节点业务并封装统一输出。

适配器补充职责：

1. `TaskPayload` 编解码（含 envelope 版本校验）。
2. 错误归类映射（业务错误/重试耗尽/超时/调度异常）。
3. 指标埋点透传（节点时延、重试次数、dispatch 位置）。
4. 对 `prefix_request` 做兼容回填，避免旧链路字段缺失。

## 5.3 DAG 定义结构

建议新增轻量定义对象（位于 module 接口层）：

1. `ModuleDagDefinition`
   - `nodes: Vec<ModuleDagNodeDef>`
   - `edges: Vec<ModuleDagEdgeDef>`
   - `entry_nodes: Vec<String>`
2. `ModuleDagNodeDef`
   - `node_id: String`
   - `node: Arc<dyn ModuleNodeTrait>`
   - `placement/policy`（可选覆盖）
3. `ModuleDagEdgeDef`
   - `from: String`
   - `to: String`

建议扩展字段（降低后续破坏性改动）：

1. `ModuleDagNodeDef.policy_override: Option<DagNodeExecutionPolicy>`
2. `ModuleDagNodeDef.placement_override: Option<NodePlacement>`
3. `ModuleDagNodeDef.tags: Vec<String>`（用于灰度、观测、路由）
4. `ModuleDagDefinition.default_policy: Option<DagNodeExecutionPolicy>`
5. `ModuleDagDefinition.metadata: HashMap<String, String>`

编译期校验清单：

1. 非空图校验（至少 1 个业务节点）。
2. 节点唯一性校验。
3. 边引用存在性校验。
4. 无环校验。
5. 入度可达性校验（所有节点可从入口到达）。
6. 控制节点冲突校验（禁止业务节点使用保留名称）。

---

## 6. 上层编排与执行

上层新增统一编排步骤：

1. 从 `ModuleTrait.build_dag()` 获取 DAG 定义。
2. 转换为 `schedule::dag::Dag`：
   - 注册节点
   - 注册边
   - 写入 node placement / execution policy
3. 创建 `DagScheduler` 并注入：
   - dispatcher（本地或 remote redis）
   - run guard
   - fencing store
   - run state store
4. 执行 `execute_parallel()`，将结果映射回现有事件模型。

建议新增组件：

1. `ModuleDagOrchestrator`
2. `ModuleDagCompiler`（定义 -> 运行时 Dag）
3. `ModuleNodeDagAdapter`（业务节点适配）

执行语义建议（统一口径）：

1. 默认 fail-fast：任一关键节点失败则中止 DAG。
2. 可配置 fail-continue：仅对声明为可降级节点启用。
3. 重试策略优先级：节点覆盖 > 模块默认 > 调度器默认。
4. timeout 语义：节点 timeout 触发节点失败；run timeout 触发全局中止。
5. resume 语义：仅复用已成功节点输出，失败和运行中节点重新调度。
6. cancel 语义：失败中止时终止 in-flight 任务并释放 run guard。

分布式执行语义补充：

1. 至少一次执行（At-Least-Once）作为默认交付语义，业务侧通过幂等键实现效果一次。
2. 调度器重启后可从 run state 恢复，保证不丢已成功节点输出。
3. 网络分区期间禁止双主推进：run guard 续租失败即快速失败并让渡执行权。
4. 远程分发失败不直接丢任务，进入受控重试与退避队列。
5. 对 side-effect 节点强制要求幂等声明，否则禁止启用 fail-continue。

建议补充伪代码：

```text
def execute_module_dag(module, runtime_ctx):
   defn = module.build_dag(build_ctx)
   dag = compiler.compile(defn)  # 含拓扑校验
   scheduler = DagScheduler::new(dag)
      .with_dispatcher(resolve_dispatcher(runtime_ctx))
      .with_run_guard(resolve_run_guard(runtime_ctx))
      .with_fencing_store(resolve_fencing_store(runtime_ctx))
      .with_run_state_store(resolve_run_state_store(runtime_ctx))
      .with_options(resolve_options(module, runtime_ctx))

   report = scheduler.execute_parallel()
   return map_report_to_task_events(report)
```

---

## 7. 数据与上下文约定

统一上下文键（最低要求）：

1. `run_id`
2. `module_id`
3. `node_id`
4. `attempt`
5. `prefix_request`（兼容链式回溯）

分布式关键键建议：

1. `idempotency_key`：跨重试与跨实例去重主键。
2. `fencing_token`：写路径防并发覆盖。
3. `dispatcher_epoch`：标识调度器代际，便于排障。
4. `region` / `zone`：跨地域路由与隔离。
5. `causal_parent`：跨节点因果链追踪。

统一输出封装：

1. 节点间传输使用 `TaskPayload`。
2. 业务事件（`TaskOutputEvent`）按统一编码写入 payload metadata/body。
3. 上层编排在 DAG 结束后做事件解包与投递。

建议增加标准 metadata：

1. `dag_run_id`
2. `dag_node_id`
3. `dag_attempt`
4. `dag_layer`
5. `dag_placement`
6. `trace_id` / `span_id`

序列化约束：

1. payload 优先使用稳定 envelope（带版本号）。
2. metadata key 仅允许 ASCII 小写+下划线，避免跨系统兼容问题。
3. 大 payload 建议外置存储，仅在 metadata 中传引用。

去重与幂等约束：

1. `idempotency_key` 生成规则需稳定：建议由 `module_id + node_id + business_pk + logical_window` 组成。
2. 幂等记录 TTL 必须覆盖最长重试窗口与最大恢复窗口。
3. 外部副作用（DB 写入/回调）必须在同一幂等域内提交。

---

## 8. 兼容与迁移策略

## 8.1 阶段 A（兼容期）

1. 所有旧模块无需改代码：
   - `build_dag` 默认走 `add_step` 线性转换。
2. 引入开关：
   - `module_dag_enabled`
   - `module_dag_force_linear_compat`
3. 线上双轨：
   - 关键模块先灰度 `build_dag` 显式实现。

建议新增开关矩阵：

1. `module_dag_enabled`：总开关。
2. `module_dag_force_linear_compat`：强制走 `add_step -> linear dag`。
3. `module_dag_whitelist`：按模块名灰度启用显式 DAG。
4. `module_dag_remote_dispatch_enabled`：是否允许远程分发。
5. `module_dag_cross_region_enabled`：是否允许跨地域调度。
6. `module_dag_strict_fencing`：是否启用严格 fencing 拒写。

建议决策顺序：

1. 若 `module_dag_enabled=false`，走旧链路。
2. 若 `module_dag_force_linear_compat=true`，走兼容线性 DAG。
3. 若模块在 `module_dag_whitelist`，走显式 `build_dag()`。
4. 其他模块走兼容线性 DAG。
5. 若跨地域关闭，则强制本地域优先 placement。
6. 若严格 fencing 开启，则任何 token 校验失败立即失败。

## 8.2 阶段 B（收敛期）

1. 新模块必须实现 `build_dag`。
2. `add_step` 标记 deprecated。
3. 逐步清理 `module_processor_with_chain` 的链式专有逻辑。

退场标准：

1. Top N 核心模块全部切换显式 DAG。
2. 兼容路径 2 个发布周期无回退。
3. `module_processor_with_chain` 仅保留最小适配能力后下线。

---

## 9. 代码落点建议

1. `src/common/interface/module.rs`
   - 增加 `build_dag` 与 DAG 定义结构。
2. `src/engine/task/module.rs`
   - Module 生命周期改为 DAG 编排驱动。
3. `src/engine/task/task_manager.rs`
   - 接入 `ModuleDagOrchestrator`。
4. `src/engine/task/module_processor_with_chain.rs`
   - 兼容期保留；逐步缩减到适配层。
5. `src/schedule/dag/*`
   - 复用现有调度能力，不复制实现。

建议新增文件：

1. `src/engine/task/module_dag_orchestrator.rs`
2. `src/engine/task/module_dag_compiler.rs`
3. `src/engine/task/module_node_dag_adapter.rs`
4. `src/common/interface/module_dag.rs`（可选：若不希望污染 `module.rs`）

建议改造点细化：

1. `src/engine/task/module.rs`：`add_step()` 改为 `prepare_execution_graph()`。
2. `src/engine/task/task_manager.rs`：增加 DAG 执行入口，统一调用。
3. `src/engine/task/module_processor_with_chain.rs`：保留兼容桥，不再新增能力。

分布式落点补充：

1. `src/schedule/dag/remote_redis.rs`：补齐幂等去重键和退避重试策略。
2. `src/schedule/dag/fencing.rs`：补齐 token 校验失败的统一错误码与指标。
3. `src/schedule/dag/scheduler.rs`：补齐 lease 续租抖动、分区快速失败和恢复路径日志。

---

## 10. 测试与验收

必测项：

1. 兼容模式：`add_step -> 线性 DAG` 行为与旧链路一致。
2. 显式 DAG：分叉/汇聚节点正确执行。
3. 远程分发：`NodePlacement::Remote` 端到端通过。
4. 一致性：run guard / fencing / resume 场景通过。
5. 回归：错误链（generate/parser error）行为一致。

补充测试矩阵（建议最小集）：

1. 拓扑校验：重复节点、非法边、环路、不可达节点。
2. 调度并发：ready queue 去重、最大并发限制、in-flight 取消。
3. 超时：节点 timeout、run timeout、timeout 后恢复行为。
4. 一致性：run guard acquire/renew/release 失败路径。
5. fencing：token 缺失/拒绝路径。
6. resume：成功输出复用与失败节点重跑。
7. 兼容：`add_step` 与显式 `build_dag` 输出一致性（golden case）。
8. 远程分发：本地/远程混合 placement。
9. 分区容错：run guard 续租丢失后单实例快速失败且无双写。
10. 调度器崩溃恢复：中途 kill 进程后 resume 一致。
11. 时钟漂移：模拟节点时钟偏移下 lease 行为稳定。
12. 幂等回归：重复投递与重复回放不产生重复副作用。

建议命令：

1. `cargo test schedule::dag`
2. `cargo test module_processor_with_chain -- --nocapture`

补充建议命令：

1. `cargo test schedule::dag::tests::execute_parallel_run_guard_renew_lost_fails_fast -- --exact`
2. `cargo test schedule::dag::tests::execute_parallel_run_timeout_interrupts_wait_loop_quickly -- --exact`
3. `cargo test schedule::dag::tests::execute_parallel_ready_queue_dedup_avoids_duplicate_dispatch -- --exact`

验收标准：

1. 关键模块迁移后行为零回归。
2. DAG 能力成为模块执行默认入口。
3. 灰度期间可一键回退兼容模式。

量化门槛（建议）：

1. DAG 路径成功率不低于旧链路。
2. P95 端到端耗时回归不超过 5%。
3. run guard/fencing 相关错误占比低于约定阈值（例如 < 0.1%）。
4. 重复副作用率低于约定阈值（例如 < 0.01%）。
5. 调度器异常重启后恢复成功率达到约定阈值（例如 > 99.9%）。

---

## 11. 风险与缓解

1. 风险：`ModuleNodeTrait` 语义与 `DagNodeTrait` 语义不完全对齐。
   - 缓解：适配器层显式做输入输出标准化。
2. 风险：旧链路依赖隐式 metadata。
   - 缓解：先定义最小上下文契约，再做模块迁移清单。
3. 风险：大规模切换导致排障复杂。
   - 缓解：模块白名单灰度 + 指标看板 + 回滚开关。

4. 风险：节点 ID 策略不稳定导致 resume 命中率低。
   - 缓解：要求显式稳定 node_id，不允许随机 ID。
5. 风险：payload 体积膨胀造成 Redis/网络压力。
   - 缓解：大对象外置，payload 只传引用与摘要。
6. 风险：兼容路径长期保留导致技术债固化。
   - 缓解：设定退场 SLA 和版本截止线。

7. 风险：跨地域链路抖动导致远程节点误超时。
   - 缓解：按地域配置 timeout 基线并启用自适应退避。
8. 风险：时钟漂移导致 lease 误判。
   - 缓解：采用单调时钟计算租约窗口，避免 wall clock 直接判定。
9. 风险：幂等键设计不当导致误去重。
   - 缓解：引入业务主键评审模板与压测回放验证。

---

## 12. 可观测性与运维要求

最小指标集：

1. `dag_run_total{module,status}`
2. `dag_node_total{module,node,status,placement}`
3. `dag_node_latency_ms{module,node}`（直方图）
4. `dag_retry_total{module,node}`
5. `dag_guard_renew_fail_total{module}`
6. `dag_resume_reused_nodes{module}`

日志字段规范：

1. `run_id`
2. `module_id`
3. `node_id`
4. `attempt`
5. `placement`
6. `error_code`

告警建议：

1. run guard renew fail 短时突增。
2. execute timeout 比例超阈值。
3. resume 复用率异常下降。
4. duplicate_effect_rate 超阈值。
5. fencing_reject_rate 持续升高。

---

## 13. 里程碑

1. M1（接口就绪）
   - `ModuleTrait.build_dag` 上线，兼容转换可用。
2. M2（编排就绪）
   - `ModuleDagOrchestrator` 接入 task manager。
3. M3（模块迁移）
   - 核心模块改为显式 DAG 定义。
4. M4（收敛）
   - `add_step` 进入废弃周期，链式专有逻辑下线。

建议补充每阶段交付物：

1. M1：trait 变更 + 兼容转换 + 单测。
2. M2：orchestrator/compiler/adapter + 集成测试。
3. M3：核心模块迁移清单 + 灰度看板 + 回滚演练。
4. M4：兼容代码清理 + 文档收敛 + 变更公告。

---

## 14. 实施拆分建议（可直接建任务）

1. 任务包 A（接口层）
   - 增加 `ModuleTrait::build_dag` 与定义结构。
   - 增加兼容转换 `from_linear_steps`。
2. 任务包 B（编排层）
   - 引入 `ModuleDagOrchestrator/Compiler/Adapter`。
   - 对接 `TaskManager` 调用路径。
3. 任务包 C（治理能力）
   - 开关矩阵、白名单、回滚策略。
   - 指标日志与告警。
4. 任务包 D（迁移与验收）
   - 先迁移 1~2 个核心模块做样板。
   - 完成兼容对比与性能压测。

---

## 15. 与现有文档关系

本方案是对以下文档的接口层补充与收敛：

1. `docs/design/module_node_dag_refactor_plan.md`
2. `docs/design/module_node_dag_development_guide.md`

差异点：本方案聚焦 `ModuleTrait/ModuleNodeTrait` 接口治理与上层编排职责重划分。

---

## 16. 一页式结论

1. 方向：正确，且与现有 `schedule::dag` 能力天然匹配。
2. 关键成功因素：稳定 node_id、统一上下文、严格灰度与回滚。
3. 落地策略：先兼容、再迁移、后收敛，避免一次性大切换风险。

---

## 17. 分布式场景专项优化

### 17.1 一致性模型与副作用边界

1. 系统级语义：调度层保证至少一次，业务层通过幂等实现效果一次。
2. 副作用节点必须声明 `side_effect=true`，并提供 `idempotency_key` 生成规则。
3. 对外写入统一经过 fencing 校验，拒绝陈旧 token 的写请求。

### 17.2 租约与续租参数建议

1. 设租约时长为 `ttl_ms`，续租间隔为 `heartbeat_ms`。
2. 建议满足：

$$
heartbeat\_ms \le \frac{ttl\_ms}{3}, \quad jitter \in [10\%, 30\%]
$$

3. 若连续续租失败超过阈值（例如 1 次），立刻触发 fail-fast，避免双主推进。

### 17.3 网络分区与故障域

1. 按 `region -> zone -> instance` 建立分层故障域标签。
2. placement 默认同 zone 优先，跨 zone 次之，跨 region 最后。
3. 分区时禁止自动跨 region 抢占，需开关显式放行。

### 17.4 恢复与重放策略

1. run state 定期 checkpoint，最小粒度到节点成功输出。
2. 恢复时只重放非成功节点，并复用成功节点输出快照。
3. 对 side-effect 节点恢复重放前强制幂等检查。

### 17.5 容量与背压策略

1. 以模块维度设置最大并发 `max_in_flight`，防止热点模块拖垮全局。
2. 远程分发队列设置可观测背压阈值（队列长度、等待时延）。
3. 背压触发时优先降级低优先级模块或切换为本地执行。

### 17.6 安全与多租户隔离

1. `module_id` 与租户标识纳入所有锁键和幂等键命名空间。
2. 禁止跨租户共享 run guard/fencing 资源键。
3. 审计日志至少保留 `tenant/module/run/node/token` 五元组。

### 17.7 混沌与演练要求

1. 每个发布周期执行一次 kill-leader 演练。
2. 每个双周执行一次网络分区演练（单 region）。
3. 每月执行一次跨 region 恢复演练，并输出恢复耗时与数据一致性报告。
