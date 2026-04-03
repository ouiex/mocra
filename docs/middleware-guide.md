# Middleware Guide

mocra provides three middleware layers that intercept different stages of the processing pipeline. Middleware enables cross-cutting concerns like logging, transformation, rate-limiting, and custom storage.

## Middleware Layers

```
Request ─── DownloadMiddleware ───▶ Download ─── DownloadMiddleware ───▶ Response
                before_request()                    after_response()

Data ─── DataMiddleware ───▶ Transformed Data
           handle_data()

Data ─── DataStoreMiddleware ───▶ Stored
           before_store() → store_data() → after_store()
```

## DownloadMiddleware

Intercepts the HTTP download stage:

```rust
#[async_trait]
pub trait DownloadMiddleware: Send + Sync {
    /// Unique name for this middleware.
    fn name(&self) -> &str;

    /// Ordering weight. Lower values run earlier. Default: 100.
    fn weight(&self) -> i32 { 100 }

    /// Called before the HTTP request is sent.
    /// Modify the request (add headers, change URL, etc).
    async fn before_request(
        &mut self,
        request: &mut Request,
        config: Arc<ModuleConfig>,
    );

    /// Called after the HTTP response is received.
    /// Inspect or modify the response.
    async fn after_response(
        &mut self,
        response: &mut Response,
        config: Arc<ModuleConfig>,
    );
}
```

### Example: Custom Header Middleware

```rust
struct AuthHeaderMiddleware {
    token: String,
}

#[async_trait]
impl DownloadMiddleware for AuthHeaderMiddleware {
    fn name(&self) -> &str { "auth_header" }
    fn weight(&self) -> i32 { 10 } // runs early

    async fn before_request(&mut self, request: &mut Request, _config: Arc<ModuleConfig>) {
        request.headers.insert(
            "Authorization".to_string(),
            format!("Bearer {}", self.token),
        );
    }

    async fn after_response(&mut self, _response: &mut Response, _config: Arc<ModuleConfig>) {
        // no-op
    }
}
```

## DataMiddleware

Intercepts parsed data for transformation:

```rust
#[async_trait]
pub trait DataMiddleware: Send + Sync {
    fn name(&self) -> &str;
    fn weight(&self) -> i32 { 100 }

    /// Transform or filter data items before storage.
    async fn handle_data(
        &mut self,
        data: &mut Vec<Value>,
        config: Arc<ModuleConfig>,
    );
}
```

### Example: Data Filtering

```rust
struct FilterEmptyMiddleware;

#[async_trait]
impl DataMiddleware for FilterEmptyMiddleware {
    fn name(&self) -> &str { "filter_empty" }

    async fn handle_data(&mut self, data: &mut Vec<Value>, _config: Arc<ModuleConfig>) {
        data.retain(|item| !item.is_null());
    }
}
```

## DataStoreMiddleware

Extends `DataMiddleware` with a three-phase storage lifecycle:

```rust
#[async_trait]
pub trait DataStoreMiddleware: DataMiddleware {
    /// Called before storage begins.
    async fn before_store(
        &mut self,
        data: &mut Vec<Value>,
        config: Arc<ModuleConfig>,
    );

    /// Perform the actual storage operation.
    async fn store_data(
        &mut self,
        data: &Vec<Value>,
        config: Arc<ModuleConfig>,
    );

    /// Called after storage completes.
    async fn after_store(
        &mut self,
        data: &Vec<Value>,
        config: Arc<ModuleConfig>,
    );
}
```

### Example: Database Store

```rust
struct PostgresStoreMiddleware {
    pool: PgPool,
}

#[async_trait]
impl DataMiddleware for PostgresStoreMiddleware {
    fn name(&self) -> &str { "postgres_store" }
    fn weight(&self) -> i32 { 200 } // runs after transformations

    async fn handle_data(&mut self, _data: &mut Vec<Value>, _config: Arc<ModuleConfig>) {
        // no-op — actual storage in store_data()
    }
}

#[async_trait]
impl DataStoreMiddleware for PostgresStoreMiddleware {
    async fn before_store(&mut self, data: &mut Vec<Value>, _config: Arc<ModuleConfig>) {
        // validate or deduplicate before insert
    }

    async fn store_data(&mut self, data: &Vec<Value>, _config: Arc<ModuleConfig>) {
        for item in data {
            // insert into database
        }
    }

    async fn after_store(&mut self, _data: &Vec<Value>, _config: Arc<ModuleConfig>) {
        // log or emit metrics
    }
}
```

## Registering Middleware

Middleware is registered on the engine before running:

```rust
let engine = Engine::new(state, None).await;

// Download middleware
engine.add_download_middleware(Arc::new(Mutex::new(Box::new(AuthHeaderMiddleware {
    token: "secret".into(),
})))).await;

// Data middleware
engine.add_data_middleware(Arc::new(Mutex::new(Box::new(FilterEmptyMiddleware)))).await;

// Data store middleware
engine.add_data_store_middleware(Arc::new(Mutex::new(Box::new(PostgresStoreMiddleware {
    pool: pg_pool,
})))).await;
```

All middleware instances are wrapped as `Arc<Mutex<Box<dyn ...>>>`.

## Execution Order

Middleware instances are sorted by `weight()` at registration time. **Lower weight values execute first.**

For download middleware, the order is:
1. `before_request()` — weight ascending (10 → 50 → 100)
2. HTTP download
3. `after_response()` — weight ascending

For data store middleware:
1. `handle_data()` — all DataMiddleware, weight ascending
2. `before_store()` — weight ascending
3. `store_data()` — weight ascending
4. `after_store()` — weight ascending

## Best Practices

1. **Use low weight for authentication** — header injection, cookie management, etc.
2. **Use medium weight for transformation** — data normalization, enrichment
3. **Use high weight for storage** — database writes, file output
4. **Keep middleware stateless when possible** — they are shared via `Arc<Mutex<...>>`; minimize lock contention
5. **Use `before_store()` for validation** — reject bad data before it hits your database
