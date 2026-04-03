# Examples

This directory contains runnable examples for mocra.

## Files

| File | Description |
|---|---|
| `simple.rs` | Basic linear module — single node, fetch and parse |
| `module_node_trait_dag.rs` | DAG module — fan-out/fan-in with multiple branches |

## module_node_trait_dag.rs

Demonstrates:

1. **Linear pipeline** via `add_step()` — `step_0(StartNode) → step_1(FollowNode)`
2. **Custom DAG** via `dag_definition()` — fan-out and merge:

```
       ┌── branch_a ──┐
start ─┤               ├── merge
       └── branch_b ──┘
```

3. **Mixed mode** — when both are present, the engine merges them into a single DAG

## Running

```bash
# DAG example (uses tests/config.mock.pure.engine.toml)
cargo run --example module_node_trait_dag

# Or directly
cargo run --bin module_node_trait_dag
```

> **Note:** Examples use a test config file. For production, replace with your own `config.toml`.

## Learn More

- [Getting Started](../docs/getting-started.md) — Installation and first module
- [Module Development](../docs/module-development.md) — Full ModuleTrait and ModuleNodeTrait guide
- [DAG Guide](../docs/dag-guide.md) — DAG definition, fan-out/fan-in, routing
