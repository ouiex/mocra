# 设计文档

本文解释 `mocra` 面向使用者的核心概念。

## 模块设计

一个爬虫以 `ModuleTrait` 实现的形式封装。

必须实现的方法：

```rust
fn name(&self) -> &'static str;
fn version(&self) -> i32;
fn default_arc() -> Arc<dyn ModuleTrait>
where
    Self: Sized;
```

常用可选方法：

- `rate_limit()` 设置模块请求速率；
- `proxy_pool()` 选择代理池；
- `timeout_secs()` 覆盖请求超时时间；
- `priority()` 设置请求优先级；
- `serial_execution()` 请求模块串行执行；
- `module_locker()` 启用模块级锁；
- `enable_session()` 启用会话；
- `response_cache_enabled()` 启用响应缓存；
- `response_cache_ttl_secs()` 覆盖响应缓存 TTL；
- `cron()` 启用定时调度；
- `pre_process()` 和 `post_process()` 添加模块生命周期钩子。

## 节点设计

每个节点实现 `ModuleNodeTrait`：

```rust
async fn generate(&self, ctx: NodeGenerateContext<'_>) -> Result<SyncBoxStream<'static, Request>>;

async fn parser(&self, response: Response, ctx: NodeParseContext<'_>) -> Result<NodeParseOutput>;
```

节点应尽量保持执行期间不可变。分页游标、业务 ID、路由信息等可变进度状态应放在请求 metadata、节点输入或 profile/config 数据中。

生产 DAG 节点应覆盖 `stable_node_key()`。稳定 key 可以保证模块重建后，重试和 parser dispatch 仍能路由到正确节点。

## 线性工作流

简单工作流可以实现 `add_step()`：

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![Arc::new(ListNode), Arc::new(DetailNode)]
}
```

运行时会根据返回的节点列表构建线性 DAG。

## 显式 DAG

分支、汇聚或非线性流程应实现 `dag_definition()`。它返回包含节点和边的 `ModuleDagDefinition`。

以下场景适合使用显式 DAG：

- 一个响应会路由到不同类型的节点；
- 节点有多个下游目标；
- 工作流存在 join 或非线性执行；
- 需要控制节点 placement 或执行策略。

`NodePlacement::Local` 表示本地执行。`NodePlacement::Remote { worker_group }` 是高级路由提示，需要运行时部署提供 dispatcher 支持。

## 解析输出

`NodeParseOutput` 是 parser 的输出契约：

```rust
NodeParseOutput::default()
    .with_next(NodeDispatch::new("detail", input))
    .with_data(parsed)
    .finish()
```

使用 `with_next(...)` 继续工作流，使用 `with_data(...)` 输出解析数据，使用 `finish()` 标记完成。

## Request 和 Response

创建请求：

```rust
Request::new("https://example.com", "GET")
```

常用 request builder：

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

`Response` 携带下载后的响应体、执行上下文和 parser 所需 metadata。

## 中间件

运行时支持 download、data 和 store 三类中间件。在 `start()` 前注册：

```rust
engine.register_download_middleware(middleware).await;
engine.register_data_middleware(middleware).await;
engine.register_store_middleware(middleware).await;
```

中间件适合实现请求改写、响应规范化、数据转换和存储处理等横切逻辑。

## 当前边界

当前代码库不提供 Redis 兼容能力。不要为缓存、队列、锁、同步、事件或限流配置 Redis。

旧分布式爬虫路径的回滚兼容不是面向使用者的能力。新应用应使用当前 typed DAG、队列、缓存和 Raft/RocksDB 路径。

