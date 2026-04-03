# Getting Started

This guide walks you through installing mocra, writing your first module, and running the engine.

## Prerequisites

- **Rust 1.85+** (edition 2024)
- **PostgreSQL** or **SQLite** for metadata storage
- **Redis** (optional — enables distributed mode, caching, and distributed queues)
- **Kafka** (optional — alternative queue backend)

## Installation

Add mocra to your `Cargo.toml`:

```toml
[dependencies]
mocra = "0.2"
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
futures = "0.3"
```

### Optional Features

| Feature | Description |
|---|---|
| `mimalloc` | Use mimalloc allocator (enabled by default) |
| `js-v8` | Embed V8 JavaScript engine for scripting |
| `polars` | Re-export Polars for data processing |

## Minimal Example

Below is the simplest possible module — a single node that fetches a URL and parses the response:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use futures::stream;
use serde_json::{Map, Value};

use mocra::prelude::*;
use mocra::common::model::login_info::LoginInfo;
use mocra::common::state::State;
use mocra::engine::engine::Engine;

// 1. Define a node
struct FetchNode;

#[async_trait]
impl ModuleNodeTrait for FetchNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new(
            "https://httpbin.org/get",
            RequestMethod::Get.as_ref(),
        );
        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        println!("Status: {}", response.status);
        println!("Body length: {}", response.body.len());
        Ok(TaskOutputEvent::default())
    }
}

// 2. Define a module
struct MyModule;

#[async_trait]
impl ModuleTrait for MyModule {
    fn name(&self) -> String { "my_module".into() }
    fn version(&self) -> i32 { 1 }
    fn should_login(&self) -> bool { false }

    fn default_arc() -> Arc<dyn ModuleTrait> { Arc::new(Self) }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(FetchNode)]
    }
}

// 3. Run the engine
#[tokio::main]
async fn main() {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(state, None).await;
    engine.register_module(MyModule::default_arc()).await;
    engine.run().await;
}
```

## Minimal Configuration

Create a `config.toml`:

```toml
name = "my_crawler"

[db]
url = "sqlite://data/crawler.db?mode=rwc"

[cache]
ttl = 60

[download_config]
timeout = 30
downloader_expire = 3600
rate_limit = 10
enable_session = false
enable_locker = false
enable_rate_limit = true
cache_ttl = 60
wss_timeout = 30

[crawler]
request_max_retries = 3
task_max_errors = 100
module_max_errors = 10
module_locker_ttl = 60

[channel_config]
minid_time = 12
capacity = 5000
```

## Running

```bash
cargo run
```

The engine will:
1. Load configuration from `config.toml`
2. Initialize database, cache, and queue services
3. Compile the DAG for each registered module
4. Start the processing pipeline
5. Expose a control API on the configured port (if `[api]` is set)

## Next Steps

- [Architecture](architecture.md) — Understand the processing pipeline
- [Module Development](module-development.md) — Multi-node modules, pagination, login
- [DAG Execution](dag-guide.md) — Fan-out, branching, and merge patterns
- [Configuration](configuration.md) — Full configuration reference
