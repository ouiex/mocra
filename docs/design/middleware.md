> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Middleware Design

## Overview
Middleware allows injecting custom logic into the crawling pipeline without modifying the core engine.
There are three types of middleware:
1. **DownloadMiddleware**: Intercepts requests and responses.
2. **DataMiddleware**: Processes extracted data.
3. **DataStoreMiddleware**: Handles data persistence.

## Architecture

### Middleware Traits
Refactored to use strong-typed traits with explicit hooks.

#### DownloadMiddleware
```rust
#[async_trait]
pub trait DownloadMiddleware: Send + Sync {
    fn name(&self) -> String;
    // Executed before the request is sent to the downloader
    // Return None to skip this request
    async fn before_request(&self, req: Request, cfg: &Option<ModuleConfig>) -> Option<Request> { Some(req) }
    // Return None to skip subsequent response middleware and publishing
    async fn after_response(&self, res: Response, cfg: &Option<ModuleConfig>) -> Option<Response> { Some(res) }
}
```

#### DataMiddleware
```rust
#[async_trait]
pub trait DataMiddleware: Send + Sync {
    fn name(&self) -> String;
    // Process data after parsing
    // Return None to skip subsequent data middleware and storage
    async fn handle_data(&self, data: Data, cfg: &Option<ModuleConfig>) -> Option<Data> { Some(data) }
}
```

### MiddlewareManager
The `MiddlewareManager` manages the lifecycle and execution of all registered middleware.
It sorts middleware by weight and executes them sequentially.

## Usage
Middleware is registered in the `Engine` during initialization.
Functional modules can configure which middleware to use via `ModuleConfig`.
