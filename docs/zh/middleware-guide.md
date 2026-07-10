# 中间件指南

> **从门面开始。** 绝大多数采集任务用不到中间件 —— [快速开始](getting-started.md) 的门面用法（`Mocra::builder().spider(...).run()`）配合类型化的 `DataSink` / `on_item` sink，已经覆盖请求生成、解析与存储。中间件是**进阶的、引擎级**扩展点，用于直接拦截下载与数据流水线。当门面无法满足某些横切需求时再使用它。

mocra 提供三层中间件，分别拦截处理流水线的不同阶段。中间件用于实现请求头注入、数据转换、过滤和自定义存储等横切关注点。

中间件注册在 `Engine` 上（而非 `Mocra` 门面构建器）。对于常见的「持久化类型化数据」场景，请优先使用 `DataSink` / `on_item` sink，而不是 `DataStoreMiddleware`。

## 中间件层级

```
Request ─── DownloadMiddleware ───▶ 下载 ─── DownloadMiddleware ───▶ Response
              before_request()                  after_response()
              (权重升序)                         (权重降序)

DataEvent ─── DataMiddleware ───▶ 转换后的 DataEvent
                handle_data()
                (权重升序)

DataEvent ─── DataStoreMiddleware ───▶ 已持久化
                before_store() → store_data() → after_store()
                (所有匹配的存储中间件并发执行)
```

## 中间件如何被选中

中间件**不会**对每一个请求或数据项都执行。每个 `Request` 携带要执行的中间件名称，这些名称在流水线中向后传播：

- `Request.download_middleware: Vec<String>` 决定该请求执行哪些**下载**中间件。
- `Request.data_middleware: Vec<String>` 传播到 `Response`，再到 `DataEvent`，决定由该请求产出的数据执行哪些**数据**与**存储**中间件。

只有当已注册中间件的 `name()` 出现在相应列表中时，它才会执行。你在节点的 `generate()` 中构建 `Request` 时附加名称，或在 `DataEvent` 上通过 `with_middleware("name")` / `with_middlewares(vec![...])` 附加。

有效排序权重按每次调用解析:存在 `ModuleConfig` 覆盖（`get_middleware_weight(name)`)时以覆盖值为准，否则使用中间件自身的 `weight()`。权重越小越先执行。

## DownloadMiddleware（下载中间件）

拦截 HTTP 下载阶段。两个钩子都按值接管入参并返回 `Option` —— 返回 `None` 会把该请求/响应从后续流水线中丢弃。

```rust
use mocra::prelude::*;
use mocra::common::interface::DownloadMiddlewareHandle;
use async_trait::async_trait;

#[async_trait]
pub trait DownloadMiddleware: Send + Sync {
    /// 该中间件实例的唯一名称。
    fn name(&self) -> String;

    /// 执行权重。权重越小越先执行。默认: 0。
    fn weight(&self) -> u32 { 0 }

    /// 在请求发送前执行。返回 `None` 可跳过该请求。
    async fn before_request(
        &mut self,
        request: Request,
        config: &Option<ModuleConfig>,
    ) -> Option<Request> {
        Some(request)
    }

    /// 在响应接收后执行。返回 `None` 可跳过后续响应中间件与发布。
    async fn after_response(
        &mut self,
        response: Response,
        config: &Option<ModuleConfig>,
    ) -> Option<Response> {
        Some(response)
    }

    /// 该中间件的现成共享句柄。
    fn default_arc() -> DownloadMiddlewareHandle
    where
        Self: Sized;
}
```

### 示例：认证请求头中间件

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
    fn weight(&self) -> u32 { 10 } // 权重低 → 优先执行

    async fn before_request(
        &mut self,
        mut request: Request,
        _config: &Option<ModuleConfig>,
    ) -> Option<Request> {
        // `Headers::add` 是大小写不敏感的 upsert 构建器。
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

## DataMiddleware（数据中间件）

拦截解析后的 `DataEvent` 进行转换或过滤。`handle_data` 按值接管事件并返回 `Option<DataEvent>` —— 返回 `None` 会跳过后续数据中间件与存储。

```rust
use mocra::prelude::*;
use mocra::common::interface::DataMiddlewareHandle;
use async_trait::async_trait;

#[async_trait]
pub trait DataMiddleware: Send + Sync {
    fn name(&self) -> String;
    fn weight(&self) -> u32 { 0 }

    /// 在存储前清洗、转换或富化数据项。返回 `None` 可丢弃该项。
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

数据载荷位于 `DataEvent.data`,是一个 `DataType` 枚举（`Empty`、`File(..)`,以及 `polars` 特性下的 `DataFrame(..)`);`module`、`account`、`platform`、`meta` 等元数据则挂在事件本身上。

### 示例：丢弃空载荷

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
            None // 被过滤 —— 跳过存储
        } else {
            Some(data)
        }
    }

    fn default_arc() -> DataMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self)))
    }
}
```

## DataStoreMiddleware（存储中间件）

扩展 `DataMiddleware`,提供三阶段存储生命周期。`store_data` 必须实现;`before_store` / `after_store` 默认为空操作。三者均返回 `Result<()>`。

```rust
use mocra::prelude::*;
use mocra::common::interface::DataStoreMiddlewareHandle;
use async_trait::async_trait;

#[async_trait]
pub trait DataStoreMiddleware: DataMiddleware {
    /// 持久化前执行一次（建立连接、确保表存在……）。
    async fn before_store(&mut self, config: &Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }

    /// 持久化数据项。必须实现。
    async fn store_data(
        &mut self,
        data: DataEvent,
        config: &Option<ModuleConfig>,
    ) -> Result<()>;

    /// 持久化后执行（flush、上报指标……）。
    async fn after_store(&mut self, config: &Option<ModuleConfig>) -> Result<()> {
        Ok(())
    }

    fn default_arc() -> DataStoreMiddlewareHandle
    where
        Self: Sized;
}
```

由于 `DataStoreMiddleware` 扩展了 `DataMiddleware`,存储类型需**同时**实现两个 trait。`DataMiddleware::handle_data` 钩子可保持透传 —— 真正的持久化发生在 `store_data`。注意每个 trait 各自声明了 `default_arc()`（返回不同的句柄类型),两者都要实现。

### 示例：文件存储

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

// 存储中间件同样要满足 DataMiddleware;数据钩子保持透传即可。
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

## 注册中间件

中间件在 `Engine` **启动前**注册,不支持运行时动态注册。每个句柄都是 `Arc<Mutex<Box<dyn ...>>>`（`tokio::sync::Mutex`）:

```rust
use std::sync::Arc;
use tokio::sync::Mutex;

let engine = Engine::new(state, None).await?;

// 下载中间件
engine
    .register_download_middleware(Arc::new(Mutex::new(Box::new(
        AuthHeaderMiddleware { token: "secret".into() },
    ))))
    .await;

// 数据中间件 —— 无需字段时 `default_arc()` 是便捷构造器
engine
    .register_data_middleware(DropEmptyMiddleware::default_arc())
    .await;

// 存储中间件
engine
    .register_store_middleware(Arc::new(Mutex::new(Box::new(
        FileStoreMiddleware { dir: "out".into() },
    ))))
    .await;
```

上文用到的类型别名:

```rust
type DownloadMiddlewareHandle  = Arc<Mutex<Box<dyn DownloadMiddleware>>>;
type DataMiddlewareHandle      = Arc<Mutex<Box<dyn DataMiddleware>>>;
type DataStoreMiddlewareHandle = Arc<Mutex<Box<dyn DataStoreMiddleware>>>;
```

## 执行顺序

只有在请求/数据事件上被命名的中间件才会执行（见 [中间件如何被选中](#中间件如何被选中)）。在被选中的集合内:

| 阶段 | 钩子 | 顺序 |
|---|---|---|
| 下载前 | `before_request()` | 权重**升序**（0 → 10 → 100） |
| 下载后 | `after_response()` | 权重**降序**（栈式回退） |
| 数据转换 | `handle_data()` | 权重**升序** |
| 存储 | `before_store()` → `store_data()` → `after_store()` | **所有匹配的存储中间件并发执行**;三阶段在单个中间件内串行 |

`after_response()` 刻意以 `before_request()` 的逆序执行,因此包裹式中间件（最先接触请求者最后接触其响应）能可预测地组合。存储中间件**不**按权重排序 —— 每个匹配的存储中间件并发运行,各中间件的存储失败被单独收集并上报。

## 最佳实践

1. **低权重用于认证** —— 请求头注入、Cookie 管理、签名。
2. **中等权重用于转换** —— 标准化、富化、过滤。
3. **高权重用于下载后期处理** —— 响应校验、日志。
4. **常见类型化数据场景优先用 `DataSink` / `on_item`** —— 仅当需要自定义持久化语义时才实现 `DataStoreMiddleware`。
5. **持锁期间保持轻量** —— 每个中间件以 `Arc<Mutex<...>>` 共享;持锁时尽量少做事以减少竞争。
6. **在 `before_store()` 中校验** —— 让无效数据在到达存储后端前被拒绝。
7. **返回 `None` 以短路** —— 丢弃某个请求/响应/数据项会跳过该阶段的其余处理。

## 参见

- [快速开始](getting-started.md) —— 门面快速上手（主路径）
- [模块开发](module-development.md) —— 构建产出请求的模块与节点
- [DAG 执行](dag-guide.md) —— 请求与数据在模块内的流转
- [API 参考](api-reference.md) —— 可观测 / 后台管理 HTTP API
- [配置](configuration.md) —— 完整 TOML 参考,含按模块的中间件权重覆盖
