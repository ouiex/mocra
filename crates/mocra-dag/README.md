# mocra-dag

Generic **distributed DAG execution engine** extracted from the
[mocra](https://github.com/ouiex/mocra) crawler framework — **zero crawler coupling**.

## Features

- Build DAGs (`Dag` / `DagChainBuilder`): nodes + dependency edges, topological validation,
  cycle detection.
- `DagScheduler`: layered concurrent execution, retry policies, fencing-guarded distributed
  run guards. It waits for node completion, run timeout, or run guard renewal loss without
  a polling timer.
- Optional run-state storage saves a full checkpoint after every 16 successful nodes by
  default. `with_run_state_checkpoint_interval(n)` changes the interval (minimum 1).
  Failure, timeout, and renewal loss also save the current state. If a process is cancelled
  between checkpoints, recovery may repeat up to `n - 1` completed nodes; use idempotent
  nodes or interval 1 when that is unacceptable.
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
