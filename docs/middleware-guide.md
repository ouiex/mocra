# Middleware Guide

> **Start with the facade.** For most collection tasks you never need middleware — the [Getting Started](getting-started.md) quickstart (`Mocra::builder().spider(...).run()`) plus a typed `DataSink` / `on_item` sink already gives you request generation, parsing, and storage. Middleware is an **advanced, engine-level** extension point for intercepting the download and data pipeline directly. Reach for it when you need cross-cutting behaviour the facade doesn't cover.

mocra provides three middleware layers that intercept different stages of the processing pipeline. Middleware enables cross-cutting concerns such as header injection, transformation, filtering, and custom storage.

Middleware is registered on the `Engine` (not on the `Mocra` facade builder). For the common "persist typed items" case, prefer a `DataSink` / `on_item` sink over a `DataStoreMiddleware`.

## Middleware Layers

```
Request ─── DownloadMiddleware ───▶ Download ─── DownloadMiddleware ───▶ Response
              before_request()                     after_response()
              (weight ascending)                   (weight descending)

DataEvent ─── DataMiddleware ───▶ transformed DataEvent
                handle_data()
                (weight ascending)

DataEvent ─── DataStoreMiddleware ───▶ persisted
                before_store() → store_data() → after_store()
                (all matching store middleware run concurrently)
```

## How middleware is selected

Middleware does **not** run against every request or data item. Each `Request` carries the names of the middleware to run, and those names propagate through the pipeline:

- `Request.download_middleware: Vec<String>` selects which **download** middleware run for that request.
- `Request.data_middleware: Vec<String>` propagates to the `Response` and then to the `DataEvent`, selecting which **data** and **store** middleware run for the data produced from that request.

A registered middleware only executes when its `name()` appears in the relevant list. You attach names when you build a `Request` in a node's `generate()`, or on a `DataEvent` via `with_middleware("name")` / `with_middlewares(vec![...])`.

Effective ordering weight is resolved per invocation: a `ModuleConfig` override (`get_middleware_weight(name)`) wins when present, otherwise the middleware's own `weight()` is used. Lower weight executes earlier.

## DownloadMiddleware

Intercepts the HTTP download stage. Both hooks take ownership of the value and return an `Option` — returning `None` drops the request/response from the rest of the pipeline.

```rust
use mocra::prelude::*;
use mocra::common::interface::DownloadMiddlewareHandle;
use async_trait::async_trait;

#[async_trait]
pub trait DownloadMiddleware: Send + Sync {
    /// Unique name of this middleware instance.
    fn name(&self) -> String;

    /// Execution weight. Lower weight executes earlier. Default: 0.
    fn weight(&self) -> u32 { 0 }

    /// Runs before the request is sent. Return `None` to skip the request.
    async fn before_request(
        &mut self,
        request: Request,
        config: &Option<ModuleConfig>,
    ) -> Option<Request> {
        Some(request)
    }

    /// Runs after the response is received. Return `None` to skip the
    /// remaining response middleware and publishing.
    async fn after_response(
        &mut self,
        response: Response,
        config: &Option<ModuleConfig>,
    ) -> Option<Response> {
        Some(response)
    }

    /// A ready-made shared handle for this middleware.
    fn default_arc() -> DownloadMiddlewareHandle
    where
        Self: Sized;
}
```

### Example: Auth header middleware

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use mocra::prelude::*;
use mocra::common::interface::DownloadMiddlewareHandle;
use async_trait::async_trait;

struct AuthHeaderMiddleware {
    token: String,
}

#[async_trait]
impl DownloadMiddleware for AuthHeaderMiddleware {
    fn name(&self) -> String { "auth_header".into() }
    fn weight(&self) -> u32 { 10 } // low weight → runs early

    async fn before_request(
        &mut self,
        mut request: Request,
        _config: &Option<ModuleConfig>,
    ) -> Option<Request> {
        // `Headers::add` is a case-insensitive upsert builder.
        request.headers = request
            .headers
            .add("Authorization", format!("Bearer {}", self.token));
        Some(request)
    }

    fn default_arc() -> DownloadMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self { token: String::new() })))
    }
}
```

## DataMiddleware

Intercepts a parsed `DataEvent` for transformation or filtering. `handle_data` takes the event by value and returns an `Option<DataEvent>` — returning `None` skips the remaining data middleware and storage.

```rust
use mocra::prelude::*;
use mocra::common::interface::DataMiddlewareHandle;
use async_trait::async_trait;

#[async_trait]
pub trait DataMiddleware: Send + Sync {
    fn name(&self) -> String;
    fn weight(&self) -> u32 { 0 }

    /// Clean, transform, or enrich the data item before storage.
    /// Return `None` to drop the item.
    async fn handle_data(
        &mut self,
        data: DataEvent,
        config: &Option<ModuleConfig>,
    ) -> Option<DataEvent> {
        Some(data)
    }

    fn default_arc() -> DataMiddlewareHandle
    where
        Self: Sized;
}
```

The payload lives in `DataEvent.data`, a `DataType` enum (`Empty`, `File(..)`, and `DataFrame(..)` under the `polars` feature); metadata such as `module`, `account`, `platform`, and `meta` is available on the event itself.

### Example: Drop empty payloads

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use mocra::prelude::*;
use mocra::common::interface::DataMiddlewareHandle;
use async_trait::async_trait;

struct DropEmptyMiddleware;

#[async_trait]
impl DataMiddleware for DropEmptyMiddleware {
    fn name(&self) -> String { "drop_empty".into() }

    async fn handle_data(
        &mut self,
        data: DataEvent,
        _config: &Option<ModuleConfig>,
    ) -> Option<DataEvent> {
        if data.size() == 0 {
            None // filtered out — storage is skipped
        } else {
            Some(data)
        }
    }

    fn default_arc() -> DataMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self)))
    }
}
```

## DataStoreMiddleware

Extends `DataMiddleware` with a three-phase storage lifecycle. `store_data` is required; `before_store` / `after_store` default to no-ops. All three return `Result<()>`.

```rust
use mocra::prelude::*;
use mocra::common::interface::DataStoreMiddlewareHandle;
use async_trait::async_trait;

#[async_trait]
pub trait DataStoreMiddleware: DataMiddleware {
    /// Runs once before persisting (open a connection, ensure a table, …).
    async fn before_store(&mut self, config: &Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }

    /// Persist the data item. Required.
    async fn store_data(
        &mut self,
        data: DataEvent,
        config: &Option<ModuleConfig>,
    ) -> Result<()>;

    /// Runs after persisting (flush, emit metrics, …).
    async fn after_store(&mut self, config: &Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }

    fn default_arc() -> DataStoreMiddlewareHandle
    where
        Self: Sized;
}
```

Because `DataStoreMiddleware` extends `DataMiddleware`, a store type implements **both** traits. The `DataMiddleware::handle_data` hook can stay a pass-through — the actual persistence happens in `store_data`. Note that each trait declares its own `default_arc()` (returning a different handle type), so implement both.

### Example: File store

```rust
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use mocra::prelude::*;
use mocra::common::interface::{DataMiddlewareHandle, DataStoreMiddlewareHandle};
use mocra::common::model::data::DataType;
use async_trait::async_trait;

struct FileStoreMiddleware {
    dir: PathBuf,
}

// Store middleware must also satisfy DataMiddleware; keep the data hook a pass-through.
#[async_trait]
impl DataMiddleware for FileStoreMiddleware {
    fn name(&self) -> String { "file_store".into() }
    fn weight(&self) -> u32 { 200 }

    async fn handle_data(
        &mut self,
        data: DataEvent,
        _config: &Option<ModuleConfig>,
    ) -> Option<DataEvent> {
        Some(data)
    }

    fn default_arc() -> DataMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self { dir: "out".into() })))
    }
}

#[async_trait]
impl DataStoreMiddleware for FileStoreMiddleware {
    async fn before_store(&mut self, _config: &Option<ModuleConfig>) -> Result<()> {
        tokio::fs::create_dir_all(&self.dir).await.ok();
        Ok(())
    }

    async fn store_data(
        &mut self,
        data: DataEvent,
        _config: &Option<ModuleConfig>,
    ) -> Result<()> {
        if let DataType::File(file) = data.data {
            let path = self.dir.join(&file.file_name);
            tokio::fs::write(path, &file.content).await.ok();
        }
        Ok(())
    }

    async fn after_store(&mut self, _config: &Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }

    fn default_arc() -> DataStoreMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self { dir: "out".into() })))
    }
}
```

## Registering middleware

Middleware is registered on the `Engine` **before it starts**. Dynamic registration at runtime is not supported. Each handle is an `Arc<Mutex<Box<dyn ...>>>` (`tokio::sync::Mutex`):

```rust
use std::sync::Arc;
use tokio::sync::Mutex;

let engine = Engine::new(state, None).await?;

// Download middleware
engine
    .register_download_middleware(Arc::new(Mutex::new(Box::new(
        AuthHeaderMiddleware { token: "secret".into() },
    ))))
    .await;

// Data middleware — `default_arc()` is a convenient constructor when no fields are needed
engine
    .register_data_middleware(DropEmptyMiddleware::default_arc())
    .await;

// Store middleware
engine
    .register_store_middleware(Arc::new(Mutex::new(Box::new(
        FileStoreMiddleware { dir: "out".into() },
    ))))
    .await;
```

The type aliases used above are:

```rust
type DownloadMiddlewareHandle  = Arc<Mutex<Box<dyn DownloadMiddleware>>>;
type DataMiddlewareHandle      = Arc<Mutex<Box<dyn DataMiddleware>>>;
type DataStoreMiddlewareHandle = Arc<Mutex<Box<dyn DataStoreMiddleware>>>;
```

## Execution order

Only middleware named on the request/data event runs (see [How middleware is selected](#how-middleware-is-selected)). Within the selected set:

| Stage | Hook | Ordering |
|---|---|---|
| Before download | `before_request()` | weight **ascending** (0 → 10 → 100) |
| After download | `after_response()` | weight **descending** (unwinds the stack) |
| Data transform | `handle_data()` | weight **ascending** |
| Storage | `before_store()` → `store_data()` → `after_store()` | **all matching store middleware run concurrently**; the three phases are sequential per middleware |

`after_response()` deliberately runs in the reverse order of `before_request()`, so wrapping-style middleware (the first to touch a request is the last to touch its response) compose predictably. Store middleware are **not** weight-ordered — every matching store middleware runs concurrently, and per-middleware storage failures are collected and reported individually.

## Best practices

1. **Use low weight for authentication** — header injection, cookie management, signing.
2. **Use medium weight for transformation** — normalization, enrichment, filtering.
3. **Use high weight for late download-stage work** — response validation, logging.
4. **Prefer `DataSink` / `on_item` over store middleware** for the common typed-item case — implement `DataStoreMiddleware` only when you need custom persistence semantics.
5. **Keep middleware cheap under lock** — each is shared as `Arc<Mutex<...>>`; minimize work while holding the lock to reduce contention.
6. **Validate in `before_store()`** — reject bad data before it reaches your storage backend.
7. **Return `None` to short-circuit** — dropping a request/response/data item skips the rest of that stage.

## See also

- [Getting Started](getting-started.md) — the facade quickstart (primary path)
- [Module Development](module-development.md) — building modules and nodes that emit requests
- [DAG Execution](dag-guide.md) — how requests and data flow through a module
- [API Reference](api-reference.md) — the observability / admin HTTP API
- [Configuration](configuration.md) — full TOML reference, including per-module middleware weight overrides
