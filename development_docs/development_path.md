# development_path

## 1. 目的

本文件定义本轮重构的**推荐推进顺序**。  
顺序设计目标不是“兼容历史实现”，而是让每一阶段都直接逼近目标态模块边界。

## 2. 基本假设

1. 历史 API 不作为设计约束。
2. 历史消息格式不作为运行时兼容约束。
3. 切换采用 **drain -> 停机/重启 -> 启动新 runtime** 的模式。
4. 不允许旧 worker 与新 worker 混跑。
5. JSON 只保留在 API ingress、debug 和离线导入导出边界。

## 3. 总体顺序

推荐固定顺序如下：

1. **目标模块边界**
2. **领域模型与 runtime context**
3. **模块接口与 workflow 编译**
4. **DAG scheduler 核心**
5. **transport 与 queue schema**
6. **执行流水线 processors**
7. **控制面 API 与 cluster control**
8. **指标与运维契约**
9. **bootstrap 收口与 cutover**

这个顺序背后的原则是：

- 先定模块和数据结构，再改执行接口；
- 先定 scheduler 和 transport，再改 processor 主链路；
- 先切控制面，再切运维面；
- 最后才收口 `State/Engine` 和历史路径。

## 4. 分阶段路径

### Stage 0：冻结目标模块边界

**先动：**

- `docs\decentralized-crawler-architecture-v2.md`
- `development_docs\02_target_logic.md`
- `development_docs\03_interface_change_list.md`
- `development_docs\04_task_breakdown.md`
- `development_docs\05_migration_plan.md`

**输出：**

- 九大目标模块边界
- 目标 API 路由树
- 目标 queue envelope 与 metrics contract

**退出条件：**

- 文档之间不再互相保留“兼容旧 API/旧消息”的要求

---

### Stage 1：领域模型与 runtime context

**顺序：**

1. `src\common\model\*`
2. 新增/重构 profile、workflow、runtime context 模块
3. `src\common\model\request.rs`
4. `src\common\model\response.rs`

**目标：**

- `TaskProfileSnapshot / ResolvedNodeConfig`
- `WorkflowDefinition / NodeSpec / EdgeSpec`
- `RoutingMeta / ExecutionMeta / NodeInput / NodeParseOutput`
- `RequestContextRef`

**不要先动：**

- `QueueManager`
- `ModuleDagProcessor`
- `engine\api\*`

因为后续所有模块都依赖这里的 canonical model。

---

### Stage 2：模块接口与 workflow 编译

**顺序：**

1. `src\common\interface\module.rs`
2. `src\engine\task\module.rs`
3. `src\engine\task\factory.rs`
4. `src\engine\task\module_dag_orchestrator.rs` 或其后继 compiler

**目标：**

- `ModuleTrait` 只负责静态声明与默认值；
- `ModuleNodeTrait` 直接接受 typed context；
- `WorkflowCompiler` 成为 workflow 编译入口；
- `TaskFactory` 只做 profile load、workflow compile、node config resolve。

**退出条件：**

- 节点实现不再读取 `ModuleConfig` 和 `Map<String, Value>`

---

### Stage 3：DAG scheduler 核心

**顺序：**

1. `src\schedule\dag\types.rs`
2. `src\schedule\dag\scheduler.rs`
3. `src\engine\task\module_dag_processor.rs`
4. `src\engine\task\task_manager.rs`

**目标：**

- `DagRunStateStore`
- `DagRunGuard`
- `DagFencingStore`
- `DagScheduler`
- `DagNodeDispatcher`
- `NodePlacement`

**必须一起考虑：**

- advance gate
- stop signal
- retry / recovery
- local fast path 收口

---

### Stage 4：transport 与 queue schema

**顺序：**

1. `src\queue\manager.rs`
2. `src\queue\channel.rs`
3. `src\queue\compensation.rs`
4. `src\engine\api\dlq.rs`

**目标：**

- 全部 topic 改成目标 envelope；
- topic registry、batch policy、backpressure 统一；
- blob 与 DLQ 变成结构化 contract。

**退出条件：**

- 新 consumer 启动前，旧 topic 已 drain 或重建完成

---

### Stage 5：执行流水线 processors

**顺序：**

1. `src\engine\engine\processors.rs`
2. `src\engine\chain\task_model_chain.rs`
3. `src\engine\chain\parser_chain.rs`
4. `src\downloader\*.rs`

**目标：**

- task -> request -> download -> response -> parse -> error 全链路切到新 contract；
- parser 输出直接走 `NodeParseOutput / NodeDispatchEnvelope`；
- local fast path 变成 scheduler policy，而不是链路内特殊判断。

**退出条件：**

- 主链路不再使用 `TaskOutputEvent`、旧 parser_task payload、旧 metadata 主表达

---

### Stage 6：控制面 API 与 cluster control

**顺序：**

1. `src\engine\api\router.rs`
2. `src\engine\api\state.rs`
3. `src\engine\api\control\*`
4. 新增 `/config/*`、`/tasks/*`、`/cluster/*`、`/debug/*` handler
5. `src\common\registry.rs`

**目标：**

- `/tasks/dispatch`
- `/config/*`
- `/cluster/*`
- `/control/*`
- `/debug/*`
- `/dlq/messages*`

**退出条件：**

- 历史 route 不再是上线前提

---

### Stage 7：指标与运维契约

**顺序：**

1. `src\common\metrics.rs`
2. `src\engine\runner.rs`
3. `src\engine\api\*.rs`
4. `src\queue\*.rs`
5. `src\schedule\dag\*.rs`

**目标：**

- `MetricsScope`
- `MetricsEmitter`
- L1/L2/L3 指标族
- dashboard / alert / scrape contract

**退出条件：**

- 运维面已切换到新指标查询，不再依赖旧指标名

---

### Stage 8：bootstrap 收口与 cutover

**顺序：**

1. `src\common\state.rs`
2. `src\engine\engine.rs`
3. `src\engine\engine\runtime.rs`
4. `src\sync\*`
5. `src\cacheable\cache_service\*`

**目标：**

- `State/Engine` 只装配目标模块；
- Redis 时代兼容路径退出主热路径；
- 启动、停机、pause/resume、health 都切到目标控制面；
- 完成 cutover rehearsal。

**退出条件：**

- 历史 route、历史运行时类型、历史主路径已删除

## 5. 不建议的顺序

1. **不要先做 API，再做 profile/store model**  
   API 会没有稳定写模型。

2. **不要先做 processor，再做 scheduler/transport**  
   主链路会因为 contract 未冻结而反复返工。

3. **不要先切 metrics，再切 pipeline**  
   指标标签和阶段语义会不断漂移。

4. **不要让旧 worker 与新 worker 混跑**  
   这会把所有 cutover 风险扩散到运行时。

## 6. 每阶段退出条件

| 阶段 | 必须满足 |
|------|----------|
| Stage 0 | 目标模块边界、路由树、schema contract 已冻结 |
| Stage 1 | canonical model 已齐备，后续模块无需再发明新旧桥接类型 |
| Stage 2 | 节点接口与 workflow compiler 已成为主路径 |
| Stage 3 | run state、guard、fencing、placement 语义闭环 |
| Stage 4 | queue topic 与 envelope 已统一到目标 schema |
| Stage 5 | 主链路 processor 已全部切到目标 contract |
| Stage 6 | 目标 API 全部可用，历史 route 不再保留 |
| Stage 7 | 运维面已切到新 metrics contract |
| Stage 8 | bootstrap 收口完成，cutover rehearsal 通过 |
