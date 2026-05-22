# Development Guide

This guide shows how to build a crawler module with the current public API.

## Minimal Module

```rust
use std::sync::Arc;

use async_trait::async_trait;
use mocra::common::interface::module::ToSyncBoxStream;
use mocra::common::interface::{ModuleNodeTrait, ModuleTrait, SyncBoxStream};
use mocra::common::model::{NodeParseOutput, Request, Response};
use mocra::common::interface::module::{NodeGenerateContext, NodeParseContext};
use mocra::errors::Result;

struct MyModule;

#[async_trait]
impl ModuleTrait for MyModule {
    fn name(&self) -> &'static str {
        "my_module"
    }

    fn version(&self) -> i32 {
        1
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(Self)
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(ListNode)]
    }
}
```

## Minimal Node

```rust
struct ListNode;

#[async_trait]
impl ModuleNodeTrait for ListNode {
    async fn generate(&self, _ctx: NodeGenerateContext<'_>) -> Result<SyncBoxStream<'static, Request>> {
        vec![Request::new("https://example.com", "GET")].into_stream_ok()
    }

    async fn parser(&self, _response: Response, _ctx: NodeParseContext<'_>) -> Result<NodeParseOutput> {
        Ok(NodeParseOutput::default().finish())
    }

    fn stable_node_key(&self) -> &'static str {
        "list"
    }
}
```

## Running the Engine

```rust
use std::sync::Arc;

use mocra::common::state::State;
use mocra::engine::Engine;

#[tokio::main]
async fn main() -> mocra::errors::Result<()> {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(Arc::clone(&state), None).await?;

    engine.register_module(MyModule::default_arc()).await;
    engine.start().await;

    Ok(())
}
```

Register modules and middleware before calling `start()`.

## Request Generation

Return a `SyncBoxStream<'static, Request>` from `generate()`. For small request batches, use `Vec<Request>::into_stream_ok()` from the prelude.

```rust
let request = Request::new("https://example.com/api", "POST")
    .with_json(&serde_json::json!({ "page": 1 }))
    .enable_response_cache(true);

vec![request].into_stream_ok()
```

Use request metadata for pagination cursors, business IDs, or parser routing hints.

## Response Parsing

A parser can emit data, continue to another node, or finish:

```rust
let input = build_next_node_input();

Ok(NodeParseOutput::default()
    .with_next(NodeDispatch::new("detail", input))
    .finish())
```

Use `with_next(...)` when downstream nodes need typed input. Use `finish()` when the current branch is complete.

## DAG Nodes

Always implement `stable_node_key()` for nodes used in production workflows:

```rust
fn stable_node_key(&self) -> &'static str {
    "detail"
}
```

The key must be unique within the module DAG.

## Module Options

Use module methods to define common behavior:

```rust
fn rate_limit(&self) -> Option<f32> {
    Some(2.0)
}

fn response_cache_enabled(&self) -> bool {
    true
}

fn response_cache_ttl_secs(&self) -> Option<u64> {
    Some(3600)
}
```

## Cron Scheduling

Return a `CronConfig` from `cron()` to enable scheduled startup. For ad-hoc execution, dispatch tasks through the HTTP API or queue path configured by the application.

## Testing Modules

Recommended test coverage:

- compile the module DAG;
- run `generate()` and assert produced requests;
- run `parser()` with a fixture response and assert `NodeParseOutput`;
- verify `stable_node_key()` values are unique;
- run `cargo check` for API compatibility.
