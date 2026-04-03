# DAG 执行指南

mocra 将每个模块编译为有向无环图（DAG）。本指南说明 DAG 的定义、编译和执行方式。

## 概述

```
定义 ──▶ 编译 ──▶ 执行
(用户代码)  (初始化时)  (队列驱动，运行时)
```

- **定义** — 通过 `ModuleTrait` 声明节点和边
- **编译** — 引擎在模块注册时构建 `Dag`
- **执行** — `ModuleDagProcessor` 通过队列流水线在节点间路由消息

## 定义 DAG

### 方式一：线性链（add_step）

返回有序的节点向量，引擎自动连接为链式结构：

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![Arc::new(NodeA), Arc::new(NodeB), Arc::new(NodeC)]
}
// 结果：step_0(NodeA) → step_1(NodeB) → step_2(NodeC)
```

### 方式二：自定义图（dag_definition）

用于非线性拓扑，返回 `ModuleDagDefinition`：

```rust
async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
    Some(ModuleDagDefinition {
        nodes: vec![
            ModuleDagNodeDef { node_id: "fetch".into(), node: Arc::new(FetchNode), .. },
            ModuleDagNodeDef { node_id: "parse_a".into(), node: Arc::new(ParseA), .. },
            ModuleDagNodeDef { node_id: "parse_b".into(), node: Arc::new(ParseB), .. },
            ModuleDagNodeDef { node_id: "save".into(), node: Arc::new(SaveNode), .. },
        ],
        edges: vec![
            ModuleDagEdgeDef { from: "fetch".into(), to: "parse_a".into() },
            ModuleDagEdgeDef { from: "fetch".into(), to: "parse_b".into() },
            ModuleDagEdgeDef { from: "parse_a".into(), to: "save".into() },
            ModuleDagEdgeDef { from: "parse_b".into(), to: "save".into() },
        ],
        entry_nodes: vec!["fetch".into()],
        default_policy: None,
        metadata: Default::default(),
    })
}
```

### ModuleDagNodeDef 字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `node_id` | `String` | 模块内唯一标识 |
| `node` | `Arc<dyn ModuleNodeTrait>` | 节点实现 |
| `placement_override` | `Option<PlacementConstraint>` | 分布式模式下的节点放置提示 |
| `policy_override` | `Option<NodePolicy>` | 自定义重试/超时策略 |
| `tags` | `Vec<String>` | 元数据标签 |

### ModuleDagEdgeDef 字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `from` | `String` | 源节点 ID |
| `to` | `String` | 目标节点 ID |

## 编译

当你调用 `engine.register_module(module)` 时，引擎：

1. 调用 `module.add_step()` 和 `module.dag_definition()`
2. 如果 `add_step()` 返回了节点，将它们转换为 `legacy_step_0`、`legacy_step_1` 等
3. 合并两种定义（如果都存在）
4. 验证图是 DAG（无环）
5. 构建带拓扑排序的编译后 `Dag`

编译后的 DAG 可通过 `engine.get_module_dag("module_name")` 访问。

## 执行模型

DAG 通过**队列流水线**执行，而非内存中直接调用。每个节点作为标准流水线迭代运行：

```
TaskEvent（含 node_id）→ generate() → [请求队列] → 下载 → [响应队列] → parser() → 路由
```

### 静态拓扑，动态执行

- **拓扑是静态的**：图结构在初始化时确定，运行时不能添加/删除边。
- **执行次数是动态的**：每个节点的运行次数取决于传入消息的数量。

### 路由机制

`parser()` 返回 `TaskOutputEvent` 后：

1. **如果 `parser_task` 非空** — 每个 `TaskParserEvent` 被独立路由到后继节点。如果当前节点有 N 个后继，每个任务会被**克隆**发送到每个后继（扇出）。

2. **如果 `parser_task` 为空** — `DagNodeAdvanceGate`（Redis SETNX 一次性门控）确保 DAG 恰好推进到下一节点**一次**，无论当前节点完成了多少个响应。

### 扇出示例

```
        ┌── branch_a ──┐
start ──┤               ├── merge
        └── branch_b ──┘
```

当 `start` 完成时：
- 如果 `start.parser()` 返回 3 个 `TaskParserEvent`，每个都会被克隆发送到 `branch_a` 和 `branch_b`。
- 结果：`branch_a` 收到 3 个任务，`branch_b` 收到 3 个任务。

### 汇合（Fan-In）

当多个父节点汇入单个节点（如上面的 `merge`）时：
- 每个父节点独立将任务路由到 `merge`。
- `merge` 对来自**任意**父节点的每个传入任务触发一次。
- 没有内置的屏障或 join 机制 — 合并节点对每条传入消息都会运行。

## 推进门控（Advance Gate）

推进门控防止空 `parser_task` 时的重复推进：

- 使用 `Redis SETNX`（不存在则设置），键为：`{config.name}:dag_advance:{module}:{node_id}:{task_key}`
- 第一个完成的响应获胜 — 触发向后继节点的推进
- 后续完成的响应为空操作

这对于产生 N 个请求但只需推进一次的节点至关重要（例如"抓取所有分页"节点，任意一页完成即触发下一阶段）。

## 回退门控（Fallback Gate）

对于非入口节点，如果 `generate()` 失败：

- `DagNodeFallbackGate`（Redis 一次性门控）触发
- 从缓存键 `{config.name}:prefix_request:{...}` 加载上一次 `Request`
- 重新注入缓存的请求以重试该节点

这为瞬态故障提供了无需用户代码的自动重试。

## 停止信号

当 DAG 分支的最终节点完成（无后继）时，`ModuleDagProcessor` 通过专用的 stop 通道发出**停止信号**。引擎使用此信号检测任务完成。

## 最佳实践

1. **保持节点专注** — 每个节点只做一件事（抓取列表、解析详情、保存数据）。
2. **使用元数据传递状态** — 通过 `TaskParserEvent::add_meta()` 在节点间传递状态，不要使用共享可变状态。
3. **设计 N:1 汇合** — 合并节点应能处理被调用任意次数的情况。
4. **使用 `parser_task` 进行显式路由** — 当你需要精确控制下一节点接收的内容时。
5. **返回空 `parser_task` 进行隐式推进** — 当你只需要 DAG 向前推进一次时。

## 完整示例

参见 [`simple/module_node_trait_dag.rs`](../../simple/module_node_trait_dag.rs) 获取包含扇出和合并的完整可运行示例。
