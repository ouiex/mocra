# Design

This document explains the user-facing concepts behind `mocra`.

## Module Design

A crawler is packaged as a `ModuleTrait` implementation.

Required methods:

```rust
fn name(&self) -> &'static str;
fn version(&self) -> i32;
fn default_arc() -> Arc<dyn ModuleTrait>
where
    Self: Sized;
```

Common optional methods:

- `rate_limit()` sets per-module request rate.
- `proxy_pool()` selects a proxy pool.
- `timeout_secs()` overrides request timeout.
- `priority()` sets request priority.
- `serial_execution()` requests serial module execution.
- `module_locker()` enables a module-level lock.
- `enable_session()` enables session handling.
- `response_cache_enabled()` enables response caching.
- `response_cache_ttl_secs()` overrides response cache TTL.
- `cron()` enables scheduled execution.
- `pre_process()` and `post_process()` add module lifecycle hooks.

## Node Design

Each node implements `ModuleNodeTrait`:

```rust
async fn generate(&self, ctx: NodeGenerateContext<'_>) -> Result<SyncBoxStream<'static, Request>>;

async fn parser(&self, response: Response, ctx: NodeParseContext<'_>) -> Result<NodeParseOutput>;
```

Nodes should keep mutable progression state in request metadata, node input, or profile/config data instead of relying on mutable node fields.

Override `stable_node_key()` for every production DAG node. A stable key keeps retry and parser dispatch routing valid when a module is rebuilt.

## Linear Workflow

For simple workflows, implement `add_step()`:

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![Arc::new(ListNode), Arc::new(DetailNode)]
}
```

The runtime builds a linear DAG from the returned node list.

## Explicit DAG

For branching or joining workflows, implement `dag_definition()`. It returns a `ModuleDagDefinition` with nodes and edges.

Use an explicit DAG when:

- one response can route to different node types;
- nodes have multiple downstream targets;
- the workflow has joins or non-linear execution;
- node placement or execution policy must be controlled.

`NodePlacement::Local` executes a node locally. `NodePlacement::Remote { worker_group }` is an advanced routing hint and requires dispatcher support in the runtime deployment.

## Parser Output

`NodeParseOutput` is the parser contract:

```rust
NodeParseOutput::default()
    .with_next(NodeDispatch::new("detail", input))
    .with_data(parsed)
    .finish()
```

Use `with_next(...)` to continue the workflow, `with_data(...)` to emit parsed data, and `finish()` to mark completion.

## Request and Response

Create requests with:

```rust
Request::new("https://example.com", "GET")
```

Useful request builders include:

- `with_params(...)`
- `with_headers(...)`
- `with_cookies(...)`
- `with_json(...)`
- `with_body(...)`
- `with_form(...)`
- `add_meta(...)`
- `with_sleep(...)`
- `enable_session(...)`
- `enable_response_cache(...)`

`Response` carries the downloaded body, execution context, and metadata needed by parser nodes.

## Middleware

The runtime supports download, data, and store middleware. Register middleware on `Engine` before `start()`:

```rust
engine.register_download_middleware(middleware).await;
engine.register_data_middleware(middleware).await;
engine.register_store_middleware(middleware).await;
```

Middleware is intended for cross-cutting behavior such as request rewriting, response normalization, data transformation, and storage handling.

## Current Boundaries

The current codebase does not provide Redis compatibility. Do not configure Redis for cache, queue, locks, events, or rate limiting.

Rollback compatibility for old distributed crawler paths is not a user-facing feature. New applications should use the current typed DAG, queue, cache, and Raft/RocksDB paths.

