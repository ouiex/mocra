# DAG Execution Guide

mocra compiles every module into a Directed Acyclic Graph (DAG). This guide explains how DAGs are defined, compiled, and executed.

## Overview

```
Definition ──▶ Compilation ──▶ Execution
(user code)     (init time)     (queue-driven, runtime)
```

- **Definition** — you declare nodes and edges via `ModuleTrait`
- **Compilation** — the engine builds a `Dag` at module registration
- **Execution** — the `ModuleDagProcessor` routes messages between nodes through the queue pipeline

## Defining a DAG

### Option 1: Linear Chain (add_step)

Return an ordered vector of nodes. The engine automatically wires them as a chain:

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![Arc::new(NodeA), Arc::new(NodeB), Arc::new(NodeC)]
}
// Result: step_0(NodeA) → step_1(NodeB) → step_2(NodeC)
```

### Option 2: Custom Graph (dag_definition)

For non-linear topologies, return a `ModuleDagDefinition`:

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

### ModuleDagNodeDef fields

| Field | Type | Description |
|---|---|---|
| `node_id` | `String` | Unique identifier within the module |
| `node` | `Arc<dyn ModuleNodeTrait>` | The node implementation |
| `placement_override` | `Option<PlacementConstraint>` | Node placement hint for distributed mode |
| `policy_override` | `Option<NodePolicy>` | Custom retry/timeout policy |
| `tags` | `Vec<String>` | Metadata tags |

### ModuleDagEdgeDef fields

| Field | Type | Description |
|---|---|---|
| `from` | `String` | Source node ID |
| `to` | `String` | Target node ID |

## Compilation

When you call `engine.register_module(module)`, the engine:

1. Calls `module.add_step()` and `module.dag_definition()`
2. If `add_step()` returns nodes, converts them to `legacy_step_0`, `legacy_step_1`, etc.
3. Merges both definitions (if present)
4. Validates the graph is a DAG (no cycles)
5. Builds a compiled `Dag` with topological ordering

The compiled DAG is accessible via `engine.get_module_dag("module_name")`.

## Execution Model

The DAG is executed through the **queue pipeline**, not in-memory. Each node runs as a standard pipeline iteration:

```
TaskEvent (with node_id) → generate() → [Request Queue] → download → [Response Queue] → parser() → route
```

### Static Topology, Dynamic Execution

- **Topology is static**: the graph structure is fixed at init time; you cannot add/remove edges at runtime.
- **Execution count is dynamic**: each node runs as many times as there are incoming messages.

### How Routing Works

After `parser()` returns a `TaskOutputEvent`:

1. **If `parser_task` is non-empty** — each `TaskParserEvent` is routed independently to the successor node(s). If the current node has N successors, each task is **cloned** for each successor (fan-out).

2. **If `parser_task` is empty** — the `DagNodeAdvanceGate` (a Redis SETNX one-shot gate) ensures the DAG advances to the next node exactly **once**, regardless of how many responses completed for the current node.

### Fan-Out Example

```
        ┌── branch_a ──┐
start ──┤               ├── merge
        └── branch_b ──┘
```

When `start` completes:
- If `start.parser()` returns 3 `TaskParserEvent` items, each is cloned for `branch_a` and `branch_b`.
- Result: `branch_a` receives 3 tasks, `branch_b` receives 3 tasks.

### Fan-In / Merge

When multiple parents feed into a single node (like `merge` above):
- Each parent independently routes tasks to `merge`.
- `merge` fires once per incoming task from **either** parent.
- There is no built-in barrier or join — the merge node runs for every incoming message.

## Advance Gate

The advance gate prevents duplicate advancement for nodes that produce empty `parser_task`:

- Uses `Redis SETNX` (set-if-not-exists) with key: `{config.name}:dag_advance:{module}:{node_id}:{task_key}`
- First response to complete wins — triggers advance to successor(s)
- Subsequent completions are no-ops

This is critical for nodes that produce N requests but only need to advance once (e.g., a "fetch all pages" node where completion of any page triggers the next stage).

## Fallback Gate

For non-entry nodes, if `generate()` fails:

- `DagNodeFallbackGate` (Redis one-shot gate) fires
- Loads the previous `Request` from cache key `{config.name}:prefix_request:{...}`
- Re-injects the cached request to retry the node

This provides automatic retry for transient failures without user code.

## Stop Signal

When the final node in a DAG branch completes (no successors), the `ModuleDagProcessor` emits a **stop signal** through a dedicated stop channel. The engine uses this to detect task completion.

## Best Practices

1. **Keep nodes focused** — each node should do one thing (fetch a list, parse details, save data).
2. **Use metadata for state** — pass state between nodes via `TaskParserEvent::add_meta()`, not via shared mutable state.
3. **Design for N:1 fan-in** — merge nodes should handle being called an arbitrary number of times.
4. **Use `parser_task` for explicit routing** — when you need precise control over what the next node receives.
5. **Return empty `parser_task` for implicit advance** — when you just need the DAG to move forward once.

## Complete Example

See [`simple/module_node_trait_dag.rs`](../simple/module_node_trait_dag.rs) for a full working example with fan-out and merge.
