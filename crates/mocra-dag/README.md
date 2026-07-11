# mocra-dag

Generic **distributed DAG execution engine** extracted from the
[mocra](https://github.com/ouiex/mocra) crawler framework — **zero crawler coupling**.

## Features

- Build DAGs (`Dag` / `DagChainBuilder`): nodes + dependency edges, topological validation,
  cycle detection.
- `DagScheduler`: layered concurrent execution, retry policies, fencing-guarded distributed
  run guards.
- Node dispatch via the `DagNodeDispatcher` trait (built-in `LocalNodeDispatcher`; hosts can plug in custom dispatchers).
- Runtime dependencies are injected via traits (`DagStore` for a distributed KV/atomic
  backend, `DagEventSink` for node-state events), so the crate does **not** depend on the
  host — the host adapts its cache / pub-sub services to those traits.

## Example

```rust,ignore
use mocra_dag::{Dag, DagScheduler};

let dag = Dag::builder()./* add nodes + edges */.build()?;
let report = DagScheduler::new(dag).run().await?;   // layered concurrent execution
```

Part of the [mocra](https://github.com/ouiex/mocra) workspace.

## License

Licensed under either of MIT or Apache-2.0 at your option.
