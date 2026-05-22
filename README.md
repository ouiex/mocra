# mocra

`mocra` is a Rust crawler runtime library for building module-based crawlers. A module describes what to request, how to parse responses, and how parsed output moves to the next node in a workflow.

The current runtime supports:

- single-node local execution;
- Kafka-backed queue execution;
- Raft/RocksDB-backed coordination and cache state;
- DAG and linear module workflows;
- response caching, middleware, cron scheduling, metrics, health checks, and DLQ operations.

Redis is not supported by the current codebase.

## Install

```toml
[dependencies]
mocra = "0.3.0"
```

Optional features:

```toml
mocra = { version = "0.3.0", features = ["js-v8", "polars"] }
```

The crate currently targets Rust `1.85` and edition `2024`.

## Minimal Runtime

```rust
use std::sync::Arc;

use mocra::common::state::State;
use mocra::engine::Engine;

#[tokio::main]
async fn main() -> mocra::errors::Result<()> {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(Arc::clone(&state), None).await?;

    // engine.register_module(MyModule::default_arc()).await;

    engine.start().await;
    Ok(())
}
```

## Documentation

- [Architecture](docs/en/architecture.md)
- [Design](docs/en/design.md)
- [Development Guide](docs/en/development.md)
- [Configuration Reference](docs/en/configuration.md)
- [HTTP API](docs/en/api.md)
- [Operations](docs/en/operations.md)

Chinese documentation:

- [架构文档](docs/zh/architecture.md)
- [设计文档](docs/zh/design.md)
- [开发文档](docs/zh/development.md)
- [配置参考](docs/zh/configuration.md)
- [HTTP API](docs/zh/api.md)
- [运维文档](docs/zh/operations.md)
