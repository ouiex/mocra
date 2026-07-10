# 模块开发(进阶)

> 大多数用户应从[门面快速上手](getting-started.md)开始 —— 单节点、无需 DB/Redis。本指南介绍进阶的 ModuleTrait/DAG 路径,面向多阶段、多节点或数据库驱动的流水线。

`Spider` 门面已能覆盖绝大多数单节点采集。当你需要**多阶段**(扇出 / 汇合)、**登录 / 会话**流程、跨节点的游标**翻页**,或**数据库驱动的多租户**流水线时,再下沉到底层的 `ModuleTrait` / `ModuleNodeTrait` API。它跑在与门面完全相同的引擎上 —— 只是把图结构与请求 / 解析生命周期交到了你手里。

安装(进阶特性按需开启):

```toml
[dependencies]
mocra = { version = "0.4", features = ["store"] }   # `store` 解锁下文的 DB 支撑模型
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

## 概念

| 概念 | 说明 |
|---|---|
| **模块**(`ModuleTrait`) | 具名工作单元 —— 声明登录 / 版本,以及节点如何连接 |
| **节点**(`ModuleNodeTrait`) | 单个阶段 —— 生成请求(`generate`)并解析响应(`parser`) |
| **DAG** | 模块内节点的执行图(线性链或自定义图) |

一个模块包含一个或多个节点,连接成 DAG:要么是线性链(`add_step()`),要么是自定义图(`dag_definition()`)。图拓扑、路由,以及推进 / 回退门控详见 [DAG 指南](dag-guide.md)。

## 门面如何映射到这套 API

门面就是这套 API 之上的一层薄适配。`Mocra::builder().spider(spider, sink)` 会把你的 `Spider` 包装成一个**单节点模块**:

- `SpiderModule` 实现 `ModuleTrait`,其 `add_step()` 返回单个节点。
- `SpiderNode` 实现 `ModuleNodeTrait`(`stable_node_key() == "spider"`):其 `generate()` 从 `Spider::start` 播种请求,其 `parser()` 调用 `Spider::parse`,把产出项转交给你的 `DataSink`,并将 `Ctx::follow` 变为后续请求。

所以**一个 `Spider` 恰好就是一个单节点模块。** 下文所述的一切,都是在「一个节点不够用」时才需要的。

> `Mocra::builder()` 门面只注册 `Spider`。手写的 `ModuleTrait` 模块需直接注册到 `Engine` 上 —— 见[注册与运行](#注册与运行)。

## ModuleNodeTrait

每个节点实现两个操作 —— `generate`(产生请求)与 `parser`(处理响应):

```rust
#[async_trait]
pub trait ModuleNodeTrait: Send + Sync {
    /// 为该阶段产生一个 HTTP 请求流。
    async fn generate(
        &self,
        config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>>;

    /// 将单个已下载的响应解析为数据 + 下游任务。
    async fn parser(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent>;

    /// 失败时是否重试该节点(默认:true)。
    fn retryable(&self) -> bool { true }

    /// 模块 DAG 内稳定且唯一的节点 id(默认 `""` → 随机 UUID)。
    /// 返回一个短常量,使节点 id 在 DAG 重建时保持不变(错误重试路由需要它)。详见 DAG 指南。
    fn stable_node_key(&self) -> &'static str { "" }
}
```

### generate()

`generate()` 返回 `SyncBoxStream<'static, Request>` —— 一个装箱、定住的 `Stream`。扩展 trait `ToSyncBoxStream` 可把 `Vec<Request>` 转成它;你也可以用 `futures::stream::iter` 手工构造:

```rust
use mocra::prelude::*;
use mocra::prelude::common::ToSyncBoxStream;  // .into_stream_ok() / .to_stream()

async fn generate(
    &self,
    _config: Arc<ModuleConfig>,
    params: Map<String, Value>,
    _login: Option<LoginInfo>,
) -> Result<SyncBoxStream<'static, Request>> {
    let page: u64 = params.get("page").and_then(|v| v.as_u64()).unwrap_or(1);

    let req = Request::new(
        format!("https://api.example.com/items?page={page}"),
        RequestMethod::Get,
    );
    vec![req].into_stream_ok()   // 或:Ok(Box::pin(futures::stream::iter(vec![req])))
}
```

`Request::new(url, method)` 两个参数都接受任意 `impl AsRef<str>`,因此 `RequestMethod::Get`(或 `"GET"`)都可用。

**可设置的 Request 字段**(拿到 `&mut Request`,或构造后再改):

| 字段 | 说明 |
|---|---|
| `url` | 目标 URL |
| `method` | HTTP 方法 |
| `headers` | 自定义请求头(`Headers::new().add(k, v)`) |
| `body` / `json` / `form` | 请求体(用于 POST/PUT) |
| `account` / `platform` | 租户标识(通常从任务继承) |
| `meta` | 任意 JSON 元数据(透传到响应) |
| `timeout` | 单请求超时(秒) |
| `downloader` | 路由到具名的自定义下载器 |
| `priority` | 调度优先级 |

### parser()

`parser()` 接收一个 `Response`,返回一个 `TaskOutputEvent`:

```rust
pub struct TaskOutputEvent {
    pub data: Vec<DataEvent>,              // 送往数据存储流水线的解析数据
    pub parser_task: Vec<TaskParserEvent>, // 路由到下一节点的任务
    pub error_task: Option<TaskErrorEvent>,
    pub stop: Option<bool>,                // 发出模块级停止信号
}
```

用链式辅助方法构建 —— `with_data`、`with_task` / `with_tasks`、`with_error`、`with_stop`:

```rust
async fn parser(
    &self,
    response: Response,
    _config: Option<Arc<ModuleConfig>>,
) -> Result<TaskOutputEvent> {
    let body: serde_json::Value = response.json()?;

    // 产出数据(见下文「产出数据」)。
    let record = DataEvent::from(&response)
        .with_file(response.content.clone())
        .with_name("items.json")
        .with_path("./data/example");

    let mut out = TaskOutputEvent::default().with_data(vec![record]);

    // 路由一个携带下一游标的后续任务到后继节点。
    if body["has_next"].as_bool().unwrap_or(false) {
        let next = TaskParserEvent::from(&response)
            .add_meta("page", body["next_page"].as_i64().unwrap_or(2));
        out = out.with_task(next);
    }

    Ok(out)
}
```

### 产出数据

`TaskOutputEvent.data` 是 `Vec<DataEvent>`,而非原始 JSON。用 `StoreTrait` 构建器产出 `DataEvent`:

- **文件 / 原始字节** —— `DataEvent::from(&response).with_file(bytes).with_name(..).with_path(..)` 得到一个 `FileStore`(它实现了 `StoreTrait`)。
- **DataFrame**(`polars` 特性)—— `DataEvent::from(&response).with_df(df)` 得到一个 `DataFrameStore`。
- **自定义记录** —— 在你自己的类型上实现 `StoreTrait`(`fn build(&self) -> DataEvent`)。

把其中任意一种传给 `with_data(vec![..])`。此后由引擎的数据存储流水线与 `DataStoreMiddleware` 接手。(与门面对照:门面里 `cx.emit(item)` 会把类型化记录直接送到你的 `DataSink`,无需 `StoreTrait`。)

### 在节点间传递数据

用 `TaskParserEvent::add_meta(key, value)` 向前传递状态。下游节点在 `generate()` 的 `params` 中读取:

```rust
// 在节点 A 的 parser 中:
let next = TaskParserEvent::from(&response)
    .add_meta("user_id", "12345")
    .add_meta("cursor", "abc");
out.with_task(next)

// 在节点 B 的 generate 中:
let user_id = params.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
```

### 翻页

- **跨节点** —— 列表节点为每页 / 每个游标路由一个 `TaskParserEvent` 到详情节点,后者从自身 `params` 读取游标(如上)。
- **在同一节点上循环** —— 返回带 `stay_current_step()` 的任务,使其携带更新后的 params **重新进入当前节点**的 `generate()`,而非推进到后继:

  ```rust
  if let Some(cursor) = next_cursor {
      let next = TaskParserEvent::from(&response)
          .add_meta("cursor", cursor)
          .stay_current_step();   // 重跑「本」节点,而非后继
      out = out.with_task(next);
  }
  // 没有下一页时不返回任务 → 该节点停止循环。
  ```

## ModuleTrait

模块把节点连接起来,并声明模块级行为:

```rust
#[async_trait]
pub trait ModuleTrait: Send + Sync {
    fn name(&self) -> String;
    fn version(&self) -> i32;

    /// 引擎是否先执行登录流程。注意:默认为 `true`。
    fn should_login(&self) -> bool { true }

    /// 应用于本模块的静态请求头 / cookie(可选)。
    async fn headers(&self) -> Headers { Headers::default() }
    async fn cookies(&self) -> Cookies { Cookies::default() }

    /// 构造一个装箱实例(必需 —— 供模块工厂 / 错误重试重建使用)。
    fn default_arc() -> Arc<dyn ModuleTrait> where Self: Sized;

    /// 自定义 DAG 图(可选)。
    async fn dag_definition(&self) -> Option<ModuleDagDefinition> { None }

    /// 线性节点流水线(可选)。
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> { vec![] }

    /// 一次运行前后的钩子(可选)。
    async fn pre_process(&self, _config: Option<Arc<ModuleConfig>>) -> Result<()> { Ok(()) }
    async fn post_process(&self, _config: Option<Arc<ModuleConfig>>) -> Result<()> { Ok(()) }

    /// Cron 调度(可选;`None` = 不做定时启动)。
    fn cron(&self) -> Option<CronConfig> { None }
}
```

> `should_login()` 在 `ModuleTrait` 上默认为 **`true`**。无需登录的模块要覆写它返回 `false`。(门面的 `Spider::should_login()` 默认为 `false`。)

### 线性流水线(add_step)

最简单的连接方式 —— 节点顺序执行:

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![
        Arc::new(ListNode),    // 抓取列表页
        Arc::new(DetailNode),  // 抓取详情页
        Arc::new(SaveNode),    // 最终处理
    ]
}
```

节点按顺序串联(`ListNode → DetailNode → SaveNode`),第一个为入口节点。每个节点的 id 取自其 `stable_node_key()`(为空时回退为生成的 UUID)。

### 自定义 DAG(dag_definition)

对扇出、汇合或非线性流水线,返回一个 `ModuleDagDefinition`。最顺手的方式是用构建器 —— 你描述 `ModuleDagNodeDef` 之间的边,入口节点与节点列表会被自动推导:

```rust
use mocra::common::model::module_dag::{ModuleDagDefinition, ModuleDagNodeDef};

async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
    let start    = ModuleDagNodeDef::new(Arc::new(StartNode)).with_id("start");
    let branch_a = ModuleDagNodeDef::new(Arc::new(BranchA)).with_id("branch_a");
    let branch_b = ModuleDagNodeDef::new(Arc::new(BranchB)).with_id("branch_b");
    let merge    = ModuleDagNodeDef::new(Arc::new(MergeNode)).with_id("merge");

    Some(
        ModuleDagDefinition::builder()
            .edge(&start, &branch_a)
            .edge(&start, &branch_b)
            .edge(&branch_a, &merge)
            .edge(&branch_b, &merge)
            .build(),
    )
}
```

```
       ┌─── branch_a ───┐
start ─┤                 ├── merge
       └─── branch_b ───┘
```

`ModuleDagNodeDef` 实现了 `Clone`,但**没有** `Default`,因此用 `ModuleDagNodeDef::new(node)`(可选 `.with_id("..")`)构造节点,而非结构体更新语法。完整字段说明与路由语义见 [DAG 指南](dag-guide.md)。

### 混合(add_step + dag_definition)

若一个模块**两者都**返回,mocra 会把它们合并成一个 DAG;线性链的节点 id 会加上 `legacy_` 前缀,以避免与自定义图冲突。

## 登录(should_login + store)

当 `should_login()` 返回 `true` 时,引擎会在 `generate()` 之前执行模块的登录流程,并把得到的 `LoginInfo` 交给每一次 `generate()` 调用:

```rust
async fn generate(
    &self,
    _config: Arc<ModuleConfig>,
    _params: Map<String, Value>,
    login_info: Option<LoginInfo>,
) -> Result<SyncBoxStream<'static, Request>> {
    if let Some(info) = &login_info {
        // info.cookies: Vec<CookieItem>,  info.useragent: String
        log::info!("已登录:{} 个 cookie,ua={}", info.cookies.len(), info.useragent);
    }
    // ……构造已鉴权的请求……
    Vec::<Request>::new().into_stream_ok()
}
```

**`store` 特性**解锁 DB 支撑的**账号 × 平台 × 模块**模型(来自 `mocra-store` crate 的 sea-orm 实体),登录与多租户模块用它来持久化凭据与各租户关系。它需要带数据库 URL 的 TOML 配置(`.from_toml(..)` / `State::try_new(..)`);见[配置](configuration.md)。

## Cron 调度

返回一个 cron 配置以周期性运行模块:

```rust
fn cron(&self) -> Option<CronConfig> {
    Some(CronConfig::new("0 */30 * * * *")) // 每 30 分钟
}
```

## 注册与运行

手写的 `ModuleTrait` 模块直接跑在 `Engine` 上(门面构建器只接受 `Spider`):

```rust
use std::sync::Arc;
use mocra::prelude::*;
use mocra::prelude::engine::Engine;
use mocra::common::state::State;

#[tokio::main]
async fn main() {
    // 进阶模块需要配置(DB / Redis / 队列)—— 见「配置」。
    let state = Arc::new(State::try_new("config.toml").await.expect("init state"));
    let engine = Engine::new(state, None).await.expect("init engine");

    engine.register_module(MyModule::default_arc()).await;

    // 运行引擎(驱动完整的下载 / 解析流水线;阻塞直至关闭)。
    engine.start().await;
}
```

## 节点中的错误处理

- 若**非入口**节点的 `generate()` 返回 `Err`,DAG 的一次性回退门控会依据 `prefix_request` 重新注入上一个请求一次,让瞬态失败无需用户代码即可重试。
- 若 `parser()` 返回 `Err`,会向错误队列发出一个 `TaskErrorEvent`,并重试该节点。
- 在某节点上把 `retryable()` 设为 `false` 可跳过其重试。

具体机制见 [DAG 指南 → 推进 / 回退门控](dag-guide.md#推进门控advance-gate)。

## 示例

- 可运行的门面示例:[`examples/spider_quickstart.rs`](../../examples/spider_quickstart.rs)(单节点、无 DB/Redis)—— 即上述一切的单节点形态。
- 多节点图、扇出 / 汇合,以及路由模型,继续阅读 [DAG 指南](dag-guide.md)。
