# Runtime, extension points, and validation

Use this reference for queues, DAGs, modules, clusters, configuration, performance, or documentation changes. Locate the stage in `docs/architecture.md`, then inspect its source and tests. The `mocra` root crate is a facade; most runtime code is in `crates/mocra-core`. `mocra-proxy`, `mocra-dag`, `mocra-cluster`, and `mocra-store` are reusable subsystem crates.

## Choose the API layer

| Need | Entry point | Read further |
| --- | --- | --- |
| One crawl and typed output | `Spider` + `DataSink` | `src/facade.rs`, `docs/getting-started.md` |
| Multiple processing nodes, login, or a custom DAG | `ModuleTrait` / `ModuleNodeTrait` | `docs/module-development.md`, `docs/dag-guide.md` |
| Intercept downloads, transformed data, or storage | Middleware traits | `docs/middleware-guide.md` |
| Configuration or distributed deployment | `MocraBuilder::from_toml` and feature flags | `docs/configuration.md`, `docs/deployment.md` |
| Metrics and control API | `dashboard` feature | `docs/api-reference.md`, `examples/dashboard.rs` |

A task passes through task/generate, request/download, and response/parse stages before output, follow-up work, or error handling. When changing fields across stages, trace event types, serialization, queue codecs, retries, and persistence; check that older task formats still decode when applicable. A `Spider` adapts to a single-node module. Do not create a separate download or proxy path in the facade that diverges from the core runtime.

## Features and deployment boundaries

- The default build needs no database or external message queue. `store` enables the database task model; `dashboard` enables management and observability APIs; `cluster-embedded` enables the Raft + redb control plane; `queue-kafka` and `queue-nats` enable cross-process data queues.
- `.cluster(…)` provides elections, locks, and membership. With an in-memory task queue, processes do not share crawl tasks; a cross-node data plane needs Kafka or NATS. See `examples/cluster_quickstart.rs` and `docs/deployment.md`.
- `.from_toml(path)` loads engine configuration. Cargo features gate config-driven functionality; the presence of a TOML section alone does not mean its runtime code was compiled or enabled.

## Validation matrix

| Change | Useful checks |
| --- | --- |
| `Spider` or facade | `cargo test -p mocra --lib`; `cargo check --examples`; run the relevant offline example |
| `mocra-core` events, download, or parse chain | `cargo test -p mocra-core --lib`; cover affected request fields and failure paths |
| Proxy pool | `cargo test -p mocra-proxy --lib`; `cargo run --example proxy_pool` |
| Dashboard or cluster | `cargo check --examples --features dashboard,cluster-embedded`; run the relevant example if needed |
| Docs or examples | `cargo fmt --all -- --check`, `git diff --check`; execute key documented commands |

Use the smallest check that resolves the actual risk. Report missing system dependencies or external services when they prevent a feature build. English guides in `docs/` have corresponding Chinese guides in `docs/zh/`; update both when changing behavior or example entry points.
