# 架构拆分与单任务清单

## 1. 说明

本清单把本轮重构拆成**可独立推进、直接面向目标态**的单任务。  
本轮路线不以保留历史 API、历史消息格式或运行时兼容层为约束。

任务编号采用：

- `A*`：领域模型与 schema
- `B*`：模块接口与 workflow 编译
- `C*`：DAG 调度与 run state
- `D*`：队列传输与 envelope
- `E*`：执行流水线 processor
- `F*`：控制面 API
- `G*`：指标与可观测
- `H*`：启动装配、切换与验收

执行约束：

1. 不保留 `ModuleConfig / MetaData / TaskOutputEvent` 双轨。
2. 不保留 `/start_work`、`/nodes` 这类历史路由 alias。
3. 不允许旧 worker 和新 worker 混跑同一套 topic。
4. 切换以 **drain -> 停机/重启 -> 启动新 schema** 为基本模式。

## 2. 目标模块拆分

| 模块 | 目标职责 | 主要组件 | 主要替换来源 |
|------|----------|----------|--------------|
| `control_plane::profile` | 配置真相、profile 写入、版本管理 | `TaskProfileSnapshot`、`ResolvedNodeConfig`、profile command | `ModuleConfig` merge、关系表拼装逻辑 |
| `workflow::definition` | 模块静态声明、图编译、节点定义 | `WorkflowDefinition`、`NodeSpec`、`EdgeSpec`、`WorkflowCompiler` | `ModuleDagOrchestrator`、`ModuleTrait::add_step()` 的运行时耦合 |
| `runtime::context` | 运行时 typed 上下文 | `RoutingMeta`、`ExecutionMeta`、`NodeInput`、`NodeGenerateContext`、`NodeParseContext` | `MetaData`、`Map<String, Value>` |
| `scheduler::dag` | run state、guard、fencing、placement、恢复 | `DagScheduler`、`DagRunStateStore`、`DagRunGuard`、`DagFencingStore`、`DagNodeDispatcher` | `ModuleDagProcessor` 持有的 run state、局部推进逻辑 |
| `transport::queue` | 消息 envelope、codec、topic、blob、DLQ | `QueueEnvelope`、`TaskDispatchEnvelope`、`NodeDispatchEnvelope`、`DeadLetterEnvelope` | 运行时对象直传、当前 DLQ 巡检接口 |
| `pipeline::processors` | task/request/download/parse/error 主链路 | ingress processor、request processor、response processor、error processor | `engine\engine\processors.rs`、chain 内隐式推进 |
| `cluster::control` | 集群状态、pause/resume、health、node registry | `ClusterControlState`、`NodeRegistryView`、health aggregator | cache key 轮询、NodeRegistry TTL/ZSET 逻辑 |
| `observability` | 指标、健康、看板与告警 | `MetricsScope`、`MetricsEmitter`、dashboard contract | 分散 `counter! / gauge! / histogram!` 与不统一标签 |
| `bootstrap` | `State/Engine` 启动装配与生命周期 | runtime builder、processor wiring、API wiring | 当前 `State` 与 `Engine` 的重耦合装配 |

## 3. 任务总表

| ID | 任务 | 主要输出 | 依赖 |
|----|------|----------|------|
| A01 | 定义 workflow/profile 领域模型 | `WorkflowDefinition / NodeSpec / EdgeSpec / TaskProfileSnapshot / ResolvedNodeConfig` | 无 |
| A02 | 定义 runtime context 模型 | `RoutingMeta / ExecutionMeta / NodeInput / NodeParseOutput / RequestContextRef` | A01 |
| A03 | 定义 transport envelope 模型 | `QueueEnvelope / TaskDispatchEnvelope / NodeDispatchEnvelope / NodeErrorEnvelope / DeadLetterEnvelope` | A02 |
| A04 | 定义控制面写模型 | profile DTO、validator、Raft command、apply model | A01 |
| B01 | 重写 `ModuleTrait` 静态声明接口 | 面向 `WorkflowDefinition` 的模块 DSL | A01 |
| B02 | 重写 `ModuleNodeTrait` 上下文接口 | `generate(ctx)` / `parser(response, ctx)` | A02 |
| B03 | 实现 `WorkflowCompiler` | profile + module -> workflow compile path | A01, B01 |
| B04 | 重写 `TaskFactory` | `ProfileLoader + WorkflowCompiler + NodeConfigResolver` | A01, B03 |
| C01 | 定义 `DagRunState` 存储模型 | run snapshot、ready set、completed set、stop signal | A02 |
| C02 | 实现 `DagRunGuard` 与 `DagFencingStore` | 单 run owner 与有序提交 | C01 |
| C03 | 实现 `DagScheduler` 主循环 | ready queue、advance gate、retry/recover 调度 | C01, C02, B03 |
| C04 | 实现 `DagNodeDispatcher` 与 placement | `NodePlacement::Local/Remote`、dispatcher contract | C03, A03 |
| C05 | 实现 stop/retry/recovery 规则 | 同节点重试、resume、fan-out、stop 收敛 | C03, C04 |
| D01 | 重写 `QueueManager` 编码层 | envelope encode/decode、schema registry、codec policy | A03 |
| D02 | 重写 topic/batch/backpressure 策略 | topic registry、batch policy、consumer contract | D01 |
| D03 | 重写 blob/DLQ 路径 | blob reload、结构化 DLQ、requeue/delete contract | D01 |
| E01 | 重写 task ingress/request processor | task dispatch -> request publish 主链路 | B02, B04, D01 |
| E02 | 重写 download/response processor | request/response queue transport 已切到 `RequestDispatchEnvelope / ResponseDispatchEnvelope` bridge，processor 内部仍解码回 legacy `Request / Response`；后续继续显式化 `RequestContextRef` 与 response typed model | E01 |
| E03 | 重写 parser/error processor | parser/error queue transport、worker 入口、统一 ingress、`TaskManager` / `TaskFactory`、`TaskModelProcessor` 主执行路径已切到 `NodeDispatchEnvelope / NodeErrorEnvelope`；新消息主路径优先使用 envelope 自带 typed parser/error context，legacy `TaskParserEvent / TaskErrorEvent` 仅保留兼容入口与旧消息 fallback decode，后续继续推进最终 typed runtime model | C05, D03, E02 |
| E04 | 收口本地快路径 | `ResponseParserProcessor` 的隐式 `local_generate` 分支已移除，parser 结果统一回到 `parser_task` 队列；`ModuleDagProcessor` 已把目标节点 placement/policy 显式编码为 runtime routing hint 并透传到 parser/error envelope，`DagScheduler` 也已支持按 `node_id` 应用 runtime override；后续把这些 hint 接入真实执行入口并继续压缩兼容层 | C04, E03 |
| F01 | 实现 `/config/*` | Account/Platform/Module/Middleware/Profile 控制面 | A04, B04 |
| F02 | 实现 `/tasks/dispatch` | 统一任务分发入口 | A03, E01 |
| F03 | 实现 `/cluster/*`、`/control/*`、`/health` | 集群状态、pause/resume、健康聚合 | C02, C03 |
| F04 | 实现 `/debug/*` 与 `/dlq/messages*` | explain、死信读取、死信回放 | D03, F01 |
| G01 | 实现 `MetricsScope` 与 `MetricsEmitter` | 统一标签模型与打点入口 | A02 |
| G02 | 接入 pipeline/scheduler/transport 指标 | L1 核心流水线 + queue/DAG 指标 | G01, C03, D01, E03 |
| G03 | 接入 control-plane/cluster 指标 | config apply、control action、health、cluster 指标 | G01, F01, F03 |
| H01 | 重写 `State/Engine` 装配 | bootstrap、runtime wiring、lifecycle | D01, E03, F03, G03 |
| H02 | 删除 legacy 路径 | 历史路由、旧类型、Redis 时代主路径依赖 | H01 |
| H03 | 完成验收与 cutover rehearsal | 构建、测试、演练、上线检查单 | H02 |

## 4. 执行包

### Pack 1：Canonical Model（A01-A04）

先冻结真正的领域模型：

- `TaskProfileSnapshot / ResolvedNodeConfig`
- `WorkflowDefinition`
- `RoutingMeta / ExecutionMeta / NodeInput`
- `QueueEnvelope` 与控制面 DTO

这一包结束前，不进入 scheduler、processor 和 API 实现。

### Pack 2：Module Runtime（B01-B04）

把模块声明和运行时上下文重构成目标接口：

- `ModuleTrait` 只负责静态声明和默认值；
- `ModuleNodeTrait` 只接受 typed context；
- `TaskFactory` 只加载 profile、编译 workflow、切 node config。

这一包结束后，`ModuleConfig / MetaData` 不再被当成未来主路径。

### Pack 3：Scheduler Core（C01-C05）

建立真正的 DAG 调度核心：

- run state 外提；
- guard / fencing / placement 就位；
- retry / stop / recovery 语义一次成型。

这一包结束后，局部 parser 快路径不再是隐式主逻辑。

### Pack 4：Transport Layer（D01-D03）

把 transport 独立成明确边界：

- 所有 topic 统一 envelope；
- batch / codec / backpressure 收口到 `QueueManager`；
- DLQ 与 blob 进入结构化 contract。

这一包结束后，不再让 processor 直接携带旧运行时对象进出 MQ。

### Pack 5：Execution Pipeline（E01-E04）

重搭真正的执行流水线：

- task ingress
- request publish / consume
- download / response attach
- parse / next-hop dispatch / error dispatch

这一包结束后，主链路完全受 scheduler + transport + typed context 驱动。

### Pack 6：Control Plane API（F01-F04）

控制面直接切到目标态：

- `/config/*`
- `/tasks/dispatch`
- `/cluster/*`
- `/control/*`
- `/debug/*`
- `/dlq/messages*`

这一包结束后，历史路由不再是实施前提。

### Pack 7：Observability（G01-G03）

统一指标层：

- `MetricsScope`
- `MetricsEmitter`
- L1/L2/L3 指标族
- dashboard / alert contract

这一包结束后，旧散点指标不再是默认观测入口。

### Pack 8：Bootstrap & Cutover（H01-H03）

最后收口：

- `State/Engine` 用新模块重新装配；
- 删除 legacy route/type/composition；
- 执行 cutover rehearsal、构建、测试和上线检查。

## 5. 并行度建议

可以并行的任务组：

- `A01 + A04`
- `A02 + G01`
- `B01 + B02`
- `D01 + F02`（接口已冻结后）
- `F03 + G03`
- `H03` 的测试准备可从 Pack 3 开始提前编写

不建议并行的任务组：

- `B04` 之前不要启动 `E01`
- `C03` 之前不要启动 `E04`
- `D01` 之前不要做 `F04`
- `H02` 必须在 `H01` 之后

## 6. 完成定义

本清单完成时，应满足：

1. 主链路只依赖 typed model、workflow compiler、scheduler 和 queue envelope。
2. 控制面只暴露目标路由，不再暴露历史 alias。
3. 运行时不再把 `serde_json::Value` 当成主表达。
4. `State/Engine` 只装配目标模块，不再把兼容层放在热路径。
