# 接口替换清单

## 1. 说明

本文档列出本轮重构需要**直接替换**的接口。  
当前接口只用于识别改造范围，不构成兼容承诺。

本轮执行约束：

- 不保留历史 API alias。
- 不保留 `ModuleConfig / MetaData / TaskOutputEvent` 的运行时双轨。
- 不允许旧 worker 与新 worker 混跑同一套 topic。
- 仅在 API ingress、debug/export 边界保留 JSON；运行时热路径以 typed model 为唯一真相。
- 若存在历史消息、DLQ 或脚本迁移需求，使用 **drain + 停机切换 + 离线迁移** 处理，而不是继续扩展兼容层。

## 2. 关键运行时接口

| 领域 | 当前接口 | 目标接口 | 变动级别 | 替换方式 | 主要文件 |
|------|----------|----------|----------|----------|----------|
| Node generate | `ModuleNodeTrait::generate(config: Arc<ModuleConfig>, params: Map<String, Value>, login_info)` | `ModuleNodeTrait::generate(ctx: NodeGenerateContext<'_>)` | 破坏性 | 同一阶段替换 trait、节点实现与调用方 | `src\common\interface\module.rs` |
| Node parser | `ModuleNodeTrait::parser(response, Option<Arc<ModuleConfig>>) -> TaskOutputEvent` | `ModuleNodeTrait::parser(response, ctx: NodeParseContext<'_>) -> NodeParseOutput` | 破坏性 | 直接替换输出模型，不保留 `TaskOutputEvent` 主路径 | `src\common\interface\module.rs` |
| Module pre/post | `pre_process/post_process(Option<Arc<ModuleConfig>>)` | `pre_process/post_process(profile: &TaskProfileSnapshot)` | 中等 | 与 profile cutover 同阶段替换 | `src\common\interface\module.rs` |
| Module config | `ModuleConfig` | `TaskProfileSnapshot + ResolvedNodeConfig` | 核心变更 | `TaskFactory` 与运行时只读新模型 | `src\common\model\model_config.rs`, `src\engine\task\factory.rs` |
| Runtime metadata | `MetaData` | `RoutingMeta + ExecutionMeta + NodeInput` | 核心变更 | 直接替换运行时上下文，不再镜像回旧槽位 | `src\common\model\meta.rs` |
| Parser output | `TaskOutputEvent` | `NodeParseOutput` | 核心变更 | parser、scheduler、queue 一次切换 | `src\common\model\message.rs` |
| Next-hop payload | `TaskParserEvent.metadata + context` | `NodeDispatchEnvelope` | 核心变更 | 已完成 queue transport bridge：`parser_task` topic 先承载 `NodeDispatchEnvelope { routing, exec, dispatch(input=legacy.parser_task) }`，processor 内部仍解码回 `TaskParserEvent`；后续再切掉 legacy event | `src\common\model\message.rs`, `src\queue\manager.rs`, `src\engine\task\parser_error_adapter.rs` |
| Request config bind | `with_module_config` / `with_task_config` / metadata 注入 | `RequestContextRef { routing, exec, node_config_ref }` | 中等 | 已完成 queue transport bridge：`RequestDispatchEnvelope / ResponseDispatchEnvelope` 先承载 `routing / exec + legacy payload`；`RequestContextRef` 显式化继续在后续 processor/model cutover 完成 | `src\common\model\request.rs`, `src\common\model\response.rs`, `src\engine\task\request_response_adapter.rs` |

## 3. 调度与 DAG 相关接口

| 领域 | 当前接口 | 目标接口 | 变动级别 | 替换方式 | 主要文件 |
|------|----------|----------|----------|----------|----------|
| DAG 编译 | `ModuleDagOrchestrator::build_definition/compile_module` | `WorkflowCompiler::compile(profile, module) -> WorkflowDefinition` | 高 | 直接抽出 workflow compiler，停止让 orchestrator 承担运行时逻辑 | `src\engine\task\module_dag_orchestrator.rs`, `src\schedule\dag\*.rs` |
| DAG 运行 | `ModuleDagProcessor` 持有 run_id、gate、stop signal | `DagScheduler + DagExecutor + DagRunStateStore` | 高 | 直接把 run state 外提，processor 只做节点执行 | `src\engine\task\module_dag_processor.rs`, `src\schedule\dag\*.rs` |
| Run ownership | 隐式依赖 queue 顺序与局部锁 | `DagRunGuard + DagFencingStore` | 高 | 调度器切换时一并引入 | `src\schedule\dag\*.rs` |
| Node placement | parser chain 隐式本地快路径 | `NodePlacement::Local/Remote` + `DagNodeDispatcher` | 中等 | 由 scheduler 显式控制 placement | `src\schedule\dag\types.rs`, `src\engine\chain\parser_chain.rs` |

## 4. 队列与消息接口

| 领域 | 当前接口 | 目标接口 | 变动级别 | 替换方式 | 主要文件 |
|------|----------|----------|----------|----------|----------|
| Task ingress payload | `TaskEvent` | `TaskDispatchEnvelope` | 高 | dispatcher 与 ingress processor 同阶段切换 | `src\common\model\message.rs`, `src\engine\api\*.rs` |
| ParserTask payload | `TaskParserEvent` | `NodeDispatchEnvelope` | 高 | parser_task topic 改成新 schema，不保留旧 payload 解码 | `src\common\model\message.rs`, `src\queue\manager.rs` |
| ErrorTask payload | `TaskErrorEvent` | `NodeErrorEnvelope` | 中等 | 已完成 queue transport bridge：error topic 先承载 `NodeErrorEnvelope { routing, exec, stage, error_message, detail=legacy.error_task }`，worker 入口再解码回 `TaskErrorEvent`；后续再切掉 legacy event | `src\common\model\message.rs`, `src\engine\engine\processors.rs`, `src\engine\task\parser_error_adapter.rs` |
| DLQ payload | 原始 bytes + 文本巡检 | `DeadLetterEnvelope` | 高 | DLQ API 改成结构化读写，不再依赖 UTF-8 假设 | `src\queue\manager.rs`, `src\engine\api\dlq.rs` |
| Queue envelope | 运行时对象直传 | `QueueEnvelope { schema_id, schema_version, codec, payload }` | 核心变更 | transport 层统一编码，processor 不再直接序列化模型 | `src\queue\manager.rs` |
| Queue codec | JSON / MsgPack | 默认 MsgPack，JSON 只保留给 API/debug/import-export 工具 | 中等 | 切换窗口内统一重建 topic 与 consumer | `src\queue\manager.rs` |

## 5. API 接口变动

| 当前路由 | 目标路由 | 说明 | 切换方式 |
|----------|----------|------|----------|
| `POST /start_work` | `POST /tasks/dispatch` | 统一任务分发入口 | 旧路由直接下线 |
| `GET /nodes` | `GET /cluster/nodes` | 集群视图归类 | 旧路由直接下线 |
| `GET /dlq` | `GET /dlq/messages` | 死信资源列表化 | 旧路由直接下线 |
| 无 | `POST /dlq/messages/{id}/requeue` | 死信回放 | 新增 |
| 无 | `DELETE /dlq/messages/{id}` | 死信删除 | 新增 |
| `POST /control/pause` | `POST /control/pause` | 保持语义，但实现改成目标控制面状态写入 | 原路由保留 |
| `POST /control/resume` | `POST /control/resume` | 保持语义，但实现改成目标控制面状态写入 | 原路由保留 |
| `GET /health` | `GET /health` | 保持路由，但契约改为明确的 HTTP 状态 + 组件健康 | 直接替换 handler 契约 |
| 无 | `/config/accounts/*` | Account 配置控制面 | 新增 |
| 无 | `/config/platforms/*` | Platform 配置控制面 | 新增 |
| 无 | `/config/modules/*` | Module 配置控制面 | 新增 |
| 无 | `/config/middlewares/*` | Middleware 配置控制面 | 新增 |
| 无 | `/config/profiles/*` | Profile 读写入口 | 新增 |
| 无 | `/debug/config/*` | 配置 explain | 新增 |
| 无 | `/debug/profile/*` | profile explain | 新增 |

## 6. 指标与可观测接口变动

| 领域 | 当前接口 | 目标接口 | 变动级别 | 替换方式 | 主要文件 |
|------|----------|----------|----------|----------|----------|
| Metrics bootstrap | `init_metrics(node_id: &str)`，当前调用点实传 `config.name` | `init_metrics(scope: MetricsScope)` | 核心变更 | 一次替换所有调用点，不保留旧签名 | `src\common\metrics.rs`, `src\engine\engine.rs` |
| 通用 stage 打点 | `inc_throughput / observe_latency / inc_error` 与散点 `counter!` 混用 | `StageMetricContext + MetricsEmitter` | 高 | 主链路与 helper 同阶段切换 | `src\common\metrics.rs`, `src\engine\runner.rs`, `src\engine\chain\*.rs` |
| Queue / DAG / API 子系统打点 | 各模块直接拼标签、命名不一致 | queue / dag / scheduler / api / config / coordination 统一标签模型 | 高 | 子系统逐域替换，完成后删除 direct 宏调用 | `src\queue\*.rs`, `src\schedule\dag\*.rs`, `src\engine\api\*.rs`, `src\sync\*.rs` |
| `/metrics` 语义 | 路由已存在，但输出集合杂糅 | 路由保持不变，输出统一 L1/L2/L3 指标体系 | 低 | 与 dashboard/alert 同阶段切换，不要求双写 | `src\engine\api\router.rs` |
| Config / control 观测 | 几乎没有 profile apply、version skew、control action 指标 | `config_updates`、`config_apply_duration`、`config_applied_version`、`control_actions` 等 | 新增 | API 写路径与 apply 路径一并接入 | `src\engine\api\*.rs`, `src\common\state.rs`, `src\engine\task\factory.rs` |

## 7. 配置接口变动

| 领域 | 当前 | 目标 | 说明 |
|------|------|------|------|
| 全局下载配置 | `enable_rate_limit`、`enable_locker`、`enable_session` 等在 `DownloadConfig` | 全局只保留基础设施默认值 | 业务配置全部进入 profile / node config |
| 任务配置来源 | 关系表 + `ModuleConfig` merge | `TaskProfileSnapshot` | 控制面唯一真相 |
| Cookie/登录态 | 部分运行期元数据注入 | 模块自行维护，框架只提供 typed context | 不再在 metadata 里堆隐式状态 |
| 基础设施运行时配置 | file watcher 只改内存 `Config` | 明确区分控制面配置与启动参数 | queue backend、codec、cache 拓扑、API port 属于 cutover/rebind 范围 |

## 8. 直接替换原则

1. 不保留历史路由 alias 或双签名 trait。
2. 不保留 `ModuleConfig`、`MetaData`、`TaskOutputEvent` 的运行时双轨。
3. 不允许旧 worker 与新 worker 共同消费同一套 topic。
4. 仅在 API ingress、debug/export、离线导入导出工具保留 JSON。
5. 需要迁移的历史消息、DLQ、脚本与 dashboard，统一在 cutover 窗口内离线处理。
