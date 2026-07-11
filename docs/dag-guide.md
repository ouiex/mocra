# DAG Execution Guide (Advanced)

> Most users should start with the [facade quickstart](getting-started.md) — single-node, no DB. This guide covers the advanced ModuleTrait/DAG path for multi-stage, multi-node, or DB-driven pipelines.

A `ModuleTrait` module runs as a Directed Acyclic Graph (DAG) of `ModuleNodeTrait` nodes. This
guide explains how the graph is defined, assembled, and executed. For the traits themselves see
[Module Development](module-development.md).

## Overview

```
Definition ──▶ build_definition ──▶ ModuleDagProcessor
(your code)     (per run, at init)   (queue-driven execution)
```

- **Definition** — you declare nodes and edges via `ModuleTrait::dag_definition()` /
  `add_step()`.
- **Assembly** — `ModuleDagOrchestrator::build_definition` turns those hooks into one
  `ModuleDagDefinition` when a run starts (not precompiled at registration).
- **Execution** — the queue-backed `ModuleDagProcessor` builds the successor / entry-node
  topology and routes messages between nodes by `ExecutionMark.node_id`.

## Defining a DAG

### Option 1: Linear Chain (add_step)

Return an ordered vector of nodes; they are wired as a chain and the first is the entry node:

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![Arc::new(NodeA), Arc::new(NodeB), Arc::new(NodeC)]
}
// Result: NodeA → NodeB → NodeC
```

Each node's id comes from its `stable_node_key()` (or a generated UUID when that is empty).

### Option 2: Custom Graph (dag_definition)

For non-linear topologies, return a `ModuleDagDefinition`. The builder collects nodes from the
edges you declare and derives the entry nodes (any node with no incoming edge) automatically:

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

Prefer the builder. If you construct the struct literally instead, note that `ModuleDagNodeDef`
implements `Clone` but **not** `Default` — build each node with `ModuleDagNodeDef::new(node)`
(not `..Default::default()`), and set all `ModuleDagDefinition` fields explicitly.

### ModuleDagNodeDef fields

| Field | Type | Description |
|---|---|---|
| `node_id` | `String` | Unique identifier within the module |
| `node` | `Arc<dyn ModuleNodeTrait>` | The node implementation |
| `placement_override` | `Option<NodePlacement>` | Node placement hint for distributed mode (from `mocra_dag`) |
| `policy_override` | `Option<DagNodeExecutionPolicy>` | Per-node retry / timeout / circuit-breaker policy (from `mocra_dag`) |
| `tags` | `Vec<String>` | Metadata tags |

Constructors: `ModuleDagNodeDef::new(node)` derives `node_id` from the node's
`stable_node_key()` (or a UUID); `.with_id("..")` overrides it explicitly (use this when you need
two instances of the same node type in one DAG).

### ModuleDagEdgeDef fields

| Field | Type | Description |
|---|---|---|
| `from` | `String` | Source node id |
| `to` | `String` | Target node id |

`ModuleDagEdgeDef::new(&from_node, &to_node)` builds one from two node defs.

### ModuleDagDefinition fields

| Field | Type | Description |
|---|---|---|
| `nodes` | `Vec<ModuleDagNodeDef>` | All nodes |
| `edges` | `Vec<ModuleDagEdgeDef>` | Directed edges |
| `entry_nodes` | `Vec<String>` | Node ids with no predecessor (where a run starts) |
| `default_policy` | `Option<DagNodeExecutionPolicy>` | Policy applied to every node unless overridden |
| `metadata` | `HashMap<String, String>` | Free-form metadata |

Helpers: `ModuleDagDefinition::builder()` (fluent, entry nodes auto-derived) and
`ModuleDagDefinition::from_linear_steps(steps)` (the linear-chain form).

## DAG assembly

The module's DAG is assembled **lazily when a run starts** — it is *not* precompiled at
registration. `ModuleDagOrchestrator::build_definition(module)`:

1. Calls `module.dag_definition()` and `module.add_step()`.
2. Builds a linear-compat definition from `add_step()` via `from_linear_steps`.
3. Merges both when both are present — the linear nodes are namespaced with a `legacy_` prefix to
   avoid id collisions with the custom graph.
4. Hands the merged `ModuleDagDefinition` to `ModuleDagProcessor::init_from_definition`, which
   builds the successor adjacency list and entry-node set used to route execution.

## Execution Model

The DAG runs through the **queue pipeline**, not in-memory. Each node runs as a standard pipeline
iteration:

```
TaskEvent (node_id) → generate() → [Request Q] → download → [Response Q] → parser() → route
```

Routing is by `ExecutionMark.node_id` carried on each Request/Response; when it is unset (a fresh
task), the entry nodes run.

### Static Topology, Dynamic Execution

- **Topology is static** — the graph is fixed at init; you cannot add/remove edges at runtime.
- **Execution count is dynamic** — each node runs once per incoming message.

### How Routing Works

After `parser()` returns a `TaskOutputEvent`:

1. **`parser_task` is non-empty** — each `TaskParserEvent` is routed to the successor node(s). If
   the parser did not target a specific node and the current node has N successors, the task is
   **cloned** to each successor (fan-out). A task marked `stay_current_step()` re-enters the
   **current** node instead of advancing. A task on a **leaf** node (no successors) is discarded.

2. **`parser_task` is empty** — for each successor, a one-shot **advance gate** synthesizes a
   single placeholder task, so the DAG advances to that successor exactly once regardless of how
   many responses completed for the current node.

### Fan-Out Example

```
        ┌── branch_a ──┐
start ──┤               ├── merge
        └── branch_b ──┘
```

When `start` completes and `start.parser()` returns 3 unrouted `TaskParserEvent`s, each is cloned
for `branch_a` and `branch_b` — so `branch_a` receives 3 tasks and `branch_b` receives 3 tasks.

### Fan-In / Merge

When multiple parents feed one node (`merge` above):

- Each parent independently routes tasks to `merge`.
- `merge` fires once per incoming task from **either** parent.
- There is no built-in barrier or join — the merge node runs for every incoming message, so
  design it to be called an arbitrary number of times.

## Advance Gate

The advance gate prevents duplicate advancement when a node produces an empty `parser_task` but
has successors:

- A one-shot gate per `(run, module, node, successor)`, stored in the shared cache (in-memory
  single-node; the embedded Raft KV in distributed mode) under key
  `dag:gate:advance:{run_id}:{module_id}:{node_id}:{successor_id}`.
- The first response to win the gate synthesizes the placeholder task to that successor;
  subsequent completions are no-ops.

This matters for nodes that emit N requests but only need to advance once (e.g. a "fetch all
pages" node where any page completing should trigger the next stage).

## Fallback Gate

For a **non-entry** node whose `generate()` fails:

- A one-shot fallback gate per `(run, module, node, prefix_request)` fires — key
  `dag:gate:fallback:{run_id}:{module_id}:{node_id}:{prefix_request}`.
- It traces the predecessor request via `prefix_request` (requests are persisted by
  `request.id`) and re-injects it once; the index-based fallback uses `response.context.step_idx`
  so retries route correctly even if a UUID is stale.

This gives automatic retry for transient failures without user code, while the one-shot gate
prevents infinite fallback loops.

## Stop Signal

A node's `parser()` can end the module by returning `TaskOutputEvent` with `with_stop(true)`.
The processor records a distributed `DagStopSignal` in the shared cache under key
`dag:exec:stop:{run_id}:{module_id}`; subsequent iterations check it and halt the run.

## Best Practices

1. **Keep nodes focused** — each node should do one thing (fetch a list, parse details, save
   data).
2. **Use metadata for state** — pass state between nodes via `TaskParserEvent::add_meta()`, not
   shared mutable state.
3. **Design for N:1 fan-in** — merge nodes should handle being called an arbitrary number of
   times.
4. **Give nodes a `stable_node_key()`** — stable ids keep error-retry routing correct across DAG
   rebuilds.
5. **Use `parser_task` for explicit routing**; return an empty `parser_task` when you just need
   the DAG to advance once.

## Running

Register the module on the `Engine` and start it — see
[Module Development → Registration and Running](module-development.md#registration-and-running).
