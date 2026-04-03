# 中间件指南

mocra 提供三层中间件，分别拦截处理流水线的不同阶段。中间件用于实现日志、数据转换、限速和自定义存储等横切关注点。

## 中间件层级

```
Request ─── DownloadMiddleware ───▶ 下载 ─── DownloadMiddleware ───▶ Response
                before_request()                  after_response()

Data ─── DataMiddleware ───▶ 转换后的数据
           handle_data()

Data ─── DataStoreMiddleware ───▶ 已存储
           before_store() → store_data() → after_store()
```

## DownloadMiddleware（下载中间件）

拦截 HTTP 下载阶段：

```rust
#[async_trait]
pub trait DownloadMiddleware: Send + Sync {
    /// 中间件的唯一名称。
    fn name(&self) -> &str;

    /// 排序权重。值越小越先执行。默认: 100。
    fn weight(&self) -> i32 { 100 }

    /// 在 HTTP 请求发送前调用。
    /// 修改请求（添加请求头、修改 URL 等）。
    async fn before_request(
        &mut self,
        request: &mut Request,
        config: Arc<ModuleConfig>,
    );

    /// 在 HTTP 响应接收后调用。
    /// 检查或修改响应。
    async fn after_response(
        &mut self,
        response: &mut Response,
        config: Arc<ModuleConfig>,
    );
}
```

### 示例：自定义请求头中间件

```rust
struct AuthHeaderMiddleware {
    token: String,
}

#[async_trait]
impl DownloadMiddleware for AuthHeaderMiddleware {
    fn name(&self) -> &str { "auth_header" }
    fn weight(&self) -> i32 { 10 } // 优先执行

    async fn before_request(&mut self, request: &mut Request, _config: Arc<ModuleConfig>) {
        request.headers.insert(
            "Authorization".to_string(),
            format!("Bearer {}", self.token),
        );
    }

    async fn after_response(&mut self, _response: &mut Response, _config: Arc<ModuleConfig>) {
        // 无操作
    }
}
```

## DataMiddleware（数据中间件）

拦截解析后的数据进行转换：

```rust
#[async_trait]
pub trait DataMiddleware: Send + Sync {
    fn name(&self) -> &str;
    fn weight(&self) -> i32 { 100 }

    /// 在存储前转换或过滤数据项。
    async fn handle_data(
        &mut self,
        data: &mut Vec<Value>,
        config: Arc<ModuleConfig>,
    );
}
```

### 示例：数据过滤

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

## DataStoreMiddleware（存储中间件）

扩展 `DataMiddleware`，提供三阶段存储生命周期：

```rust
#[async_trait]
pub trait DataStoreMiddleware: DataMiddleware {
    /// 在存储开始前调用。
    async fn before_store(
        &mut self,
        data: &mut Vec<Value>,
        config: Arc<ModuleConfig>,
    );

    /// 执行实际的存储操作。
    async fn store_data(
        &mut self,
        data: &Vec<Value>,
        config: Arc<ModuleConfig>,
    );

    /// 在存储完成后调用。
    async fn after_store(
        &mut self,
        data: &Vec<Value>,
        config: Arc<ModuleConfig>,
    );
}
```

### 示例：数据库存储

```rust
struct PostgresStoreMiddleware {
    pool: PgPool,
}

#[async_trait]
impl DataMiddleware for PostgresStoreMiddleware {
    fn name(&self) -> &str { "postgres_store" }
    fn weight(&self) -> i32 { 200 } // 在转换之后执行

    async fn handle_data(&mut self, _data: &mut Vec<Value>, _config: Arc<ModuleConfig>) {
        // 无操作 — 实际存储在 store_data() 中
    }
}

#[async_trait]
impl DataStoreMiddleware for PostgresStoreMiddleware {
    async fn before_store(&mut self, data: &mut Vec<Value>, _config: Arc<ModuleConfig>) {
        // 插入前验证或去重
    }

    async fn store_data(&mut self, data: &Vec<Value>, _config: Arc<ModuleConfig>) {
        for item in data {
            // 插入数据库
        }
    }

    async fn after_store(&mut self, _data: &Vec<Value>, _config: Arc<ModuleConfig>) {
        // 记录日志或发送指标
    }
}
```

## 注册中间件

在运行前将中间件注册到引擎：

```rust
let engine = Engine::new(state, None).await;

// 下载中间件
engine.add_download_middleware(Arc::new(Mutex::new(Box::new(AuthHeaderMiddleware {
    token: "secret".into(),
})))).await;

// 数据中间件
engine.add_data_middleware(Arc::new(Mutex::new(Box::new(FilterEmptyMiddleware)))).await;

// 存储中间件
engine.add_data_store_middleware(Arc::new(Mutex::new(Box::new(PostgresStoreMiddleware {
    pool: pg_pool,
})))).await;
```

所有中间件实例以 `Arc<Mutex<Box<dyn ...>>>` 形式包装。

## 执行顺序

中间件实例在注册时按 `weight()` 排序。**权重值越小越先执行。**

下载中间件的执行顺序：
1. `before_request()` — 权重升序（10 → 50 → 100）
2. HTTP 下载
3. `after_response()` — 权重升序

存储中间件的执行顺序：
1. `handle_data()` — 所有 DataMiddleware，权重升序
2. `before_store()` — 权重升序
3. `store_data()` — 权重升序
4. `after_store()` — 权重升序

## 最佳实践

1. **低权重用于认证** — 请求头注入、Cookie 管理等
2. **中等权重用于转换** — 数据标准化、富化
3. **高权重用于存储** — 数据库写入、文件输出
4. **尽量保持中间件无状态** — 它们通过 `Arc<Mutex<...>>` 共享；减少锁竞争
5. **使用 `before_store()` 进行验证** — 在数据写入数据库之前拒绝无效数据
