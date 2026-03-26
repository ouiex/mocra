# DAG API Reference / DAG API 参考

## Scope / 范围

EN:
This document is a bilingual API-facing reference for the DAG runtime implemented in `src/schedule/dag/`.

ZH:
本文是 DAG 运行时（`src/schedule/dag/`）的双语 API 参考文档，面向开发者与测试同学。

## Exports / 对外导出

EN:
Main exports are from `src/schedule/dag/mod.rs` and `src/schedule/lib.rs`.

ZH:
主要导出入口在 `src/schedule/dag/mod.rs` 与 `src/schedule/lib.rs`。

### Graph & Scheduler / 图与调度器

- `Dag`
- `DagChainBuilder`
- `DagNodePtr`
- `DagScheduler`
- `DagSchedulerOptions`
- `DagExecutionReport`
- `NodeExecutionResult`

### Types / 类型

- `DagError`
- `DagErrorCode`
- `DagErrorClass`
- `DagNodeStatus`
- `TaskPayload`
- `NodeExecutionContext`
- `NodePlacement`
- `DagNodeExecutionPolicy`
- `DagNodeRetryMode`

### Traits / 可扩展接口

- `DagNodeTrait`
- `DagNodeDispatcher`
- `DagRunGuard`
- `DagFencingStore`
- `DagRunStateStore`

### Built-ins / 内置实现

- `LocalNodeDispatcher`
- `RedisRemoteDispatcher`
- `RedisDagWorker`
- `RedisDagFencingStore`

## Core Build API / 核心构图 API

### `Dag::new()`

EN:
Creates a DAG with two control nodes inserted automatically: `__start__`, `__end__`.

ZH:
创建 DAG 时会自动插入两个控制节点：`__start__`、`__end__`。

### `add_node(...)` / `add_node_with_id(...)`

EN:
Adds a business node. If predecessors are omitted, node is linked from `__start__`.

ZH:
添加业务节点。若前驱为空，则自动从 `__start__` 连接。

### `add_chain_node(...)` / `add_chain_node_with_id(...)`

EN:
Fluent chain builder for linear graph construction.

ZH:
用于线性 DAG 的链式构建。

### `topological_sort()`

EN:
Validates topology and detects cycles.

ZH:
拓扑校验并检测环。

## Execution API / 执行 API

### `DagScheduler::new(dag)`

EN:
Creates scheduler with default local dispatcher and default options.

ZH:
创建调度器，默认本地调度器 + 默认选项。

### `with_options(DagSchedulerOptions)`

Fields / 字段:

- `max_in_flight`: max concurrent running node tasks / 最大并发节点任务数
- `cancel_inflight_on_failure`: abort in-flight tasks on final failure / 终态失败是否中止在途任务
- `run_timeout_ms`: overall run timeout / 全局运行超时

### `with_dispatcher(...)`

EN:
Inject custom dispatcher (for remote workers, custom routing, etc.).

ZH:
注入自定义调度器（远程执行、路由策略等）。

### `with_run_guard(...)`

EN:
Enable distributed run lock and optional heartbeat renewal.

ZH:
启用分布式运行锁，并可结合心跳续约。

### `with_fencing_store(...)`

EN:
Enable fencing commit for stale-writer protection.

ZH:
启用 fencing 提交，防止旧运行覆盖新运行。

### `with_run_state_store(...)`

EN:
Enable resume snapshot storage for recovery.

ZH:
启用运行快照持久化，实现恢复续跑。

### `execute_parallel().await`

EN:
Runs DAG and returns `DagExecutionReport` on success.

ZH:
并行执行 DAG，成功返回 `DagExecutionReport`。

## Node Contract / 节点契约

### `DagNodeTrait`

```rust
#[async_trait]
pub trait DagNodeTrait: Send + Sync {
    async fn start(&self, context: NodeExecutionContext) -> Result<TaskPayload, DagError>;
}
```

EN:
Node code must be deterministic and idempotent when retry or singleflight is enabled.

ZH:
节点实现在启用重试或 singleflight 时必须具备确定性与幂等性。

### `NodeExecutionContext`

Important fields / 关键字段:

- `run_id`
- `run_fencing_token`
- `node_id`
- `attempt`
- `upstream_nodes`
- `upstream_outputs`
- `layer_index`

## Payload API / 负载 API

### `TaskPayload`

EN:
Payload envelope supports bytes + metadata + versioned codec (`to_envelope_bytes` / `from_envelope_bytes`).

ZH:
负载支持 bytes + metadata + 版本化封装（`to_envelope_bytes` / `from_envelope_bytes`）。

## Status Machine / 状态机

Statuses / 状态:

- `Pending`
- `Ready`
- `Running`
- `Succeeded`
- `Failed`

Valid transitions / 合法迁移:

- `Pending -> Ready`
- `Ready -> Running`
- `Running -> Pending`
- `Running -> Succeeded`
- `Running -> Failed`

## Error Model / 错误模型

Representative errors / 典型错误:

- `CycleDetected`
- `MissingNodeCompute`
- `RetryExhausted`
- `RunAlreadyInProgress`
- `MissingRunFencingToken`
- `FencingTokenRejected`
- `ExecutionTimeout`
- `ExecutionIncomplete`
- `InvalidStateTransition`

EN:
Use `DagErrorCode` and `DagErrorClass` for metric labels and sync events.

ZH:
建议使用 `DagErrorCode` 与 `DagErrorClass` 作为指标和同步事件标签。

## API Usage Examples / API 使用示例

### 1) Local DAG / 本地 DAG

```rust
let mut dag = Dag::new();
let a = dag.add_node_with_id(None, "A", node_a)?;
let _b = dag.add_node_with_id(Some(&[a]), "B", node_b)?;

let report = DagScheduler::new(dag).execute_parallel().await?;
```

### 2) Retry Policy / 重试策略

```rust
dag.set_node_execution_policy(
    &node,
    DagNodeExecutionPolicy {
        max_retries: 2,
        timeout_ms: Some(500),
        retry_backoff_ms: 100,
        idempotency_key: None,
        retry_mode: DagNodeRetryMode::RetryableOnly,
        circuit_breaker_failure_threshold: Some(3),
        circuit_breaker_open_ms: 1_000,
    },
)?;
```

### 3) Remote Placement / 远程执行

```rust
dag.set_node_placement(
    &node,
    NodePlacement::Remote { worker_group: "wg-a".to_string() },
)?;

let scheduler = DagScheduler::new(dag).with_dispatcher(remote_dispatcher);
```

## Compatibility Notes / 兼容性说明

EN:
Current tests and behavior are aligned with `src/schedule/dag/tests.rs`.

ZH:
当前行为与测试基线以 `src/schedule/dag/tests.rs` 为准。