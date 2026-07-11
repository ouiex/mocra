# DAG 执行指南(进阶)

> 大多数用户应从[门面快速上手](getting-started.md)开始 —— 单节点、无需 DB。本指南介绍进阶的 ModuleTrait/DAG 路径,面向多阶段、多节点或数据库驱动的流水线。

一个 `ModuleTrait` 模块会以 `ModuleNodeTrait` 节点组成的有向无环图(DAG)运行。本指南说明该图如何定义、组装与执行。trait 本身见[模块开发](module-development.md)。

## 概述

```
定义 ──▶ build_definition ──▶ ModuleDagProcessor
(你的代码)   (每次运行、初始化时)   (队列驱动的执行)
```

- **定义** —— 通过 `ModuleTrait::dag_definition()` / `add_step()` 声明节点和边。
- **组装** —— 运行启动时,`ModuleDagOrchestrator::build_definition` 把这些钩子汇成单个 `ModuleDagDefinition`(不在注册时预编译)。
- **执行** —— 队列驱动的 `ModuleDagProcessor` 构建后继 / 入口节点拓扑,并按 `ExecutionMark.node_id` 在节点间路由消息。

## 定义 DAG

### 方式一:线性链(add_step)

返回有序的节点向量;它们被连成一条链,第一个为入口节点:

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![Arc::new(NodeA), Arc::new(NodeB), Arc::new(NodeC)]
}
// 结果:NodeA → NodeB → NodeC
```

每个节点的 id 取自其 `stable_node_key()`(为空时回退为生成的 UUID)。

### 方式二:自定义图(dag_definition)

对非线性拓扑,返回一个 `ModuleDagDefinition`。构建器会从你声明的边中收集节点,并自动推导入口节点(任何没有入边的节点):

```rust
use mocra::common::model::module_dag::{ModuleDagDefinition, ModuleDagNodeDef};

async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
    let fetch   = ModuleDagNodeDef::new(Arc::new(FetchNode)).with_id("fetch");
    let parse_a = ModuleDagNodeDef::new(Arc::new(ParseA)).with_id("parse_a");
    let parse_b = ModuleDagNodeDef::new(Arc::new(ParseB)).with_id("parse_b");
    let save    = ModuleDagNodeDef::new(Arc::new(SaveNode)).with_id("save");

    Some(
        ModuleDagDefinition::builder()
            .edge(&fetch, &parse_a)
            .edge(&fetch, &parse_b)
            .edge(&parse_a, &save)
            .edge(&parse_b, &save)
            .build(),
    )
}
```

优先使用构建器。若改用结构体字面量,请注意 `ModuleDagNodeDef` 实现了 `Clone` 但**没有** `Default` —— 每个节点用 `ModuleDagNodeDef::new(node)` 构造(而非 `..Default::default()`),并显式设置 `ModuleDagDefinition` 的所有字段。

### ModuleDagNodeDef 字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `node_id` | `String` | 模块内唯一标识 |
| `node` | `Arc<dyn ModuleNodeTrait>` | 节点实现 |
| `placement_override` | `Option<NodePlacement>` | 分布式模式下的节点放置提示(来自 `mocra_dag`) |
| `policy_override` | `Option<DagNodeExecutionPolicy>` | 单节点的重试 / 超时 / 熔断策略(来自 `mocra_dag`) |
| `tags` | `Vec<String>` | 元数据标签 |

构造方式:`ModuleDagNodeDef::new(node)` 从节点的 `stable_node_key()` 推导 `node_id`(为空则用 UUID);`.with_id("..")` 显式覆盖(当你需要同一节点类型在一个 DAG 中出现两次时用它)。

### ModuleDagEdgeDef 字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `from` | `String` | 源节点 id |
| `to` | `String` | 目标节点 id |

`ModuleDagEdgeDef::new(&from_node, &to_node)` 可由两个节点定义构造一条边。

### ModuleDagDefinition 字段

| 字段 | 类型 | 说明 |
|---|---|---|
| `nodes` | `Vec<ModuleDagNodeDef>` | 所有节点 |
| `edges` | `Vec<ModuleDagEdgeDef>` | 有向边 |
| `entry_nodes` | `Vec<String>` | 无前驱的节点 id(运行的起点) |
| `default_policy` | `Option<DagNodeExecutionPolicy>` | 未被覆盖时应用于每个节点的策略 |
| `metadata` | `HashMap<String, String>` | 自由格式元数据 |

辅助方法:`ModuleDagDefinition::builder()`(流式、自动推导入口节点)与 `ModuleDagDefinition::from_linear_steps(steps)`(线性链形态)。

## DAG 组装

模块的 DAG 在**运行启动时惰性组装** —— 并非在注册时预编译。`ModuleDagOrchestrator::build_definition(module)`:

1. 调用 `module.dag_definition()` 与 `module.add_step()`。
2. 通过 `from_linear_steps` 从 `add_step()` 构建一个线性兼容定义。
3. 两者都存在时合并 —— 线性节点的 id 会加上 `legacy_` 前缀,以避免与自定义图的 id 冲突。
4. 把合并后的 `ModuleDagDefinition` 交给 `ModuleDagProcessor::init_from_definition`,由它构建后继邻接表与入口节点集合,用于路由执行。

## 执行模型

DAG 通过**队列流水线**运行,而非在内存中直接调用。每个节点作为标准流水线迭代运行:

```
TaskEvent(node_id) → generate() → [请求队列] → 下载 → [响应队列] → parser() → 路由
```

路由依据每个 Request/Response 上携带的 `ExecutionMark.node_id`;当它未设置(新任务)时,入口节点运行。

### 静态拓扑,动态执行

- **拓扑是静态的** —— 图在初始化时固定;运行时不能添加 / 删除边。
- **执行次数是动态的** —— 每个节点每收到一条消息就运行一次。

### 路由机制

`parser()` 返回 `TaskOutputEvent` 后:

1. **`parser_task` 非空** —— 每个 `TaskParserEvent` 被路由到后继节点。若 parser 未指定具体目标节点,而当前节点有 N 个后继,则任务被**克隆**到每个后继(扇出)。标记了 `stay_current_step()` 的任务会重新进入**当前**节点而非推进。位于**叶子**节点(无后继)的任务会被丢弃。

2. **`parser_task` 为空** —— 对每个后继,一次性**推进门控**合成单个占位任务,使 DAG 恰好推进到该后继**一次**,无论当前节点完成了多少个响应。

### 扇出示例

```
        ┌── branch_a ──┐
start ──┤               ├── merge
        └── branch_b ──┘
```

当 `start` 完成且 `start.parser()` 返回 3 个未路由的 `TaskParserEvent` 时,每个都会被克隆到 `branch_a` 与 `branch_b` —— 因此 `branch_a` 收到 3 个任务,`branch_b` 收到 3 个任务。

### 汇合(Fan-In)

当多个父节点汇入一个节点(上面的 `merge`)时:

- 每个父节点独立将任务路由到 `merge`。
- `merge` 对来自**任意**父节点的每个传入任务触发一次。
- 没有内置的屏障或 join —— 合并节点对每条传入消息都会运行,因此要把它设计成可被调用任意次数。

## 推进门控(Advance Gate)

当节点产出空 `parser_task` 但有后继时,推进门控防止重复推进:

- 一个按 `(run, module, node, successor)` 的一次性门控,存放在共享缓存中(单机为内存;分布式为内嵌 Raft KV),键为 `dag:gate:advance:{run_id}:{module_id}:{node_id}:{successor_id}`。
- 第一个赢得门控的响应向该后继合成占位任务;后续完成为空操作。

这对产生 N 个请求但只需推进一次的节点很重要(例如「抓取所有分页」节点,任意一页完成即应触发下一阶段)。

## 回退门控(Fallback Gate)

对于 `generate()` 失败的**非入口**节点:

- 触发一个按 `(run, module, node, prefix_request)` 的一次性回退门控 —— 键为 `dag:gate:fallback:{run_id}:{module_id}:{node_id}:{prefix_request}`。
- 它依据 `prefix_request` 追踪前驱请求(请求以 `request.id` 持久化)并重新注入一次;基于索引的回退使用 `response.context.step_idx`,即使某个 UUID 已失效,重试也能正确路由。

这为瞬态故障提供了无需用户代码的自动重试,一次性门控则防止无限回退循环。

## 停止信号

节点的 `parser()` 可通过返回带 `with_stop(true)` 的 `TaskOutputEvent` 结束模块。处理器会在共享缓存中记录一个分布式 `DagStopSignal`,键为 `dag:exec:stop:{run_id}:{module_id}`;后续迭代会检查它并停止本次运行。

## 最佳实践

1. **保持节点专注** —— 每个节点只做一件事(抓取列表、解析详情、保存数据)。
2. **用元数据传递状态** —— 通过 `TaskParserEvent::add_meta()` 在节点间传递状态,不要用共享可变状态。
3. **为 N:1 汇合而设计** —— 合并节点应能处理被调用任意次数。
4. **给节点一个 `stable_node_key()`** —— 稳定的 id 让错误重试路由在 DAG 重建后仍正确。
5. **用 `parser_task` 做显式路由**;当你只需要 DAG 推进一次时,返回空的 `parser_task`。

## 运行

把模块注册到 `Engine` 并启动 —— 见[模块开发 → 注册与运行](module-development.md#注册与运行)。
