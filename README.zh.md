# mocra

`mocra` 是一个 Rust 爬虫运行时库，用于构建基于模块的爬虫。模块负责描述请求如何生成、响应如何解析，以及解析结果如何流转到工作流中的后续节点。

当前运行时支持：

- 单节点本地执行；
- Kafka 队列执行；
- Raft/RocksDB 协调与缓存状态；
- DAG 和线性模块工作流；
- 响应缓存、中间件、Cron 调度、指标、健康检查和 DLQ 操作。

当前代码库不再支持 Redis。

## 安装

```toml
[dependencies]
mocra = "0.3.0"
```

可选 features：

```toml
mocra = { version = "0.3.0", features = ["js-v8", "polars"] }
```

当前 crate 使用 Rust `1.85` 和 edition `2024`。

## 最小运行入口

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

## 文档

- [架构文档](docs/zh/architecture.md)
- [设计文档](docs/zh/design.md)
- [开发文档](docs/zh/development.md)
- [配置参考](docs/zh/configuration.md)
- [HTTP API](docs/zh/api.md)
- [运维文档](docs/zh/operations.md)

English documentation:

- [Architecture](docs/en/architecture.md)
- [Design](docs/en/design.md)
- [Development Guide](docs/en/development.md)
- [Configuration Reference](docs/en/configuration.md)
- [HTTP API](docs/en/api.md)
- [Operations](docs/en/operations.md)
