# ModuleTrait / ModuleNodeTrait / DAG 兼容层时序图

下面时序图覆盖两条主路径：

1. 线性兼容路径：`ModuleTrait.add_step -> ModuleDagDefinition::from_linear_steps -> compile -> execute`
2. 节点执行与数据契约路径：`Request -> Response -> TaskOutputEvent -> TaskParserEvent`

```mermaid
sequenceDiagram
    autonumber
    participant Biz as 业务模块实现<br/>impl ModuleTrait
    participant Engine as Engine/TaskManager
    participant Orch as ModuleDagOrchestrator
    participant Comp as ModuleDagCompiler
    participant Def as ModuleDagDefinition
    participant Adapter as ModuleNodeDagAdapter
    participant Sched as DagScheduler
    participant Node as DagNodeTrait.start
    participant Ctx as NodeExecutionContext
    participant Payload as TaskPayload
    participant MNode as ModuleNodeTrait<br/>(generate/parser)
    participant Req as Request
    participant Resp as Response
    participant Out as TaskOutputEvent
    participant PTask as TaskParserEvent

    rect rgb(235, 245, 255)
    Note over Biz,Engine: A. 模块注册与线性兼容 DAG 编译
    Biz->>Engine: register_module(Arc<dyn ModuleTrait>)
    Engine->>Orch: compile_linear_compat(module)
    Orch->>Biz: add_step() -> Vec<Arc<dyn ModuleNodeTrait>>
    Orch->>Def: from_linear_steps(steps)
    Def-->>Orch: nodes/edges/entry_nodes/default_policy/metadata
    Orch->>Comp: compile(definition)
    Comp->>Comp: 校验: 空图/重复节点/边端点/入口/环路
    Comp->>Adapter: 为每个 ModuleNodeTrait 包装 ModuleNodeDagAdapter
    Comp-->>Orch: Dag
    Orch-->>Engine: Dag
    end

    rect rgb(235, 255, 235)
    Note over Engine,Sched: B. DAG 运行时调度
    Engine->>Sched: execute_dag(dag)
    Sched->>Sched: with_options(DagSchedulerOptions)
    Sched->>Node: start(context)
    Node->>Ctx: 读取 run_id/node_id/attempt/upstream_outputs
    Node->>Payload: 构造或继承上游 TaskPayload
    Node-->>Sched: Result<TaskPayload, DagError>
    Sched-->>Engine: DagExecutionReport
    end

    rect rgb(255, 245, 235)
    Note over MNode,PTask: C. 业务节点 I/O 契约（当前主链路）
    Engine->>MNode: generate(config, params, login_info)
    MNode->>Req: 产生 SyncBoxStream<Request>
    Req-->>Engine: Request(url/method/headers/cookies/context/run_id/...)

    Engine->>MNode: parser(response, config)
    Resp-->>MNode: Response(status_code/content/metadata/context/run_id/...)
    MNode->>Out: 构造 TaskOutputEvent
    Out->>PTask: parser_task.push(TaskParserEvent)
    PTask-->>Engine: 下游任务(account_task/metadata/context/run_id/prefix_request)
    end

    rect rgb(245, 235, 255)
    Note over Orch,Comp: D. 显式多链路 DAG（可选）
    Engine->>Orch: compile_definition(custom ModuleDagDefinition)
    Orch->>Comp: compile(definition)
    Comp-->>Engine: Dag(支持分叉/汇合)
    end
```

## 备注

- 兼容层核心是把 `ModuleTrait.add_step()` 返回的线性节点转为 `ModuleDagDefinition`，再编译成 `Dag`。
- `ModuleNodeDagAdapter` 是 `ModuleNodeTrait -> DagNodeTrait` 的桥接器。
- 在当前阶段，业务 I/O 仍保持 `Request/Response/TaskOutputEvent/TaskParserEvent` 契约不变。
