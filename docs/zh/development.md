# 开发文档

本文展示如何使用当前公共 API 开发爬虫模块。

## 最小模块

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

## 最小节点

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

## 运行 Engine

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

请在调用 `start()` 前注册模块和中间件。

## 请求生成

`generate()` 返回 `SyncBoxStream<'static, Request>`。如果请求数量较小，可以使用 prelude 中的 `Vec<Request>::into_stream_ok()`。

```rust
let request = Request::new("https://example.com/api", "POST")
    .with_json(&serde_json::json!({ "page": 1 }))
    .enable_response_cache(true);

vec![request].into_stream_ok()
```

分页游标、业务 ID 或 parser 路由提示可以放入 request metadata。

## 响应解析

parser 可以输出数据、继续到另一个节点或结束：

```rust
let input = build_next_node_input();

Ok(NodeParseOutput::default()
    .with_next(NodeDispatch::new("detail", input))
    .finish())
```

下游节点需要类型化输入时使用 `with_next(...)`。当前分支完成时使用 `finish()`。

## DAG 节点

生产工作流中的节点应实现 `stable_node_key()`：

```rust
fn stable_node_key(&self) -> &'static str {
    "detail"
}
```

该 key 必须在同一个模块 DAG 中唯一。

## 模块选项

通过模块方法定义通用行为：

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

## Cron 调度

从 `cron()` 返回 `CronConfig` 可以启用定时启动。临时执行可以通过 HTTP API 或应用配置的队列路径分发任务。

## 模块测试

推荐覆盖：

- 编译模块 DAG；
- 运行 `generate()` 并断言请求；
- 使用 fixture response 运行 `parser()` 并断言 `NodeParseOutput`；
- 验证 `stable_node_key()` 在模块内唯一；
- 执行 `cargo check` 确认 API 兼容。
