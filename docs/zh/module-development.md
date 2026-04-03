# 模块开发指南

本指南介绍如何为 mocra 构建模块和节点。

## 概念

| 概念 | 说明 |
|---|---|
| **模块**（`ModuleTrait`） | 命名的工作单元 — 定义抓取目标和节点的连接方式 |
| **节点**（`ModuleNodeTrait`） | 单个处理阶段 — 生成请求并解析响应 |
| **DAG** | 模块内节点的执行图 |

一个模块包含一个或多个节点。节点通过线性链（`add_step()`）或自定义图（`dag_definition()`）连接成 DAG。

## ModuleNodeTrait

每个节点实现两个操作：

```rust
#[async_trait]
pub trait ModuleNodeTrait: Send + Sync {
    /// 为当前阶段生成 HTTP 请求流。
    async fn generate(
        &self,
        config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>>;

    /// 解析单个已下载的响应。
    /// 返回要存储的数据和/或下游节点的任务。
    async fn parser(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent>;

    /// 该节点失败时是否应重试（默认: true）。
    fn retryable(&self) -> bool { true }
}
```

### generate()

`generate()` 返回 `SyncBoxStream<'static, Request>` — 一个 boxed、pinned 的 `Stream`。
使用 `futures::stream::iter(vec![...])` 构造固定集合：

```rust
async fn generate(&self, config: Arc<ModuleConfig>, params: Map<String, Value>, _login: Option<LoginInfo>) -> Result<SyncBoxStream<'static, Request>> {
    let page: u32 = params.get("page")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;

    let req = Request::new(
        &format!("https://api.example.com/items?page={page}"),
        RequestMethod::Get.as_ref(),
    );
    Ok(Box::pin(stream::iter(vec![req])))
}
```

**可设置的 Request 字段：**

| 字段 | 说明 |
|---|---|
| `url` | 目标 URL |
| `method` | GET、POST 等 |
| `headers` | 自定义请求头 |
| `body` | 请求体（用于 POST/PUT） |
| `account` | 账号标识 |
| `platform` | 平台标识 |
| `module` | 模块名（通常自动设置） |
| `meta` | 任意 JSON 元数据（传递到解析器） |

### parser()

`parser()` 接收 `Response` 并返回 `TaskOutputEvent`，其中包含：

- **`data`** — 用于存储的解析数据项（`Vec`）
- **`parser_task`** — 下游节点的 `Vec<TaskParserEvent>`

```rust
async fn parser(&self, response: Response, _config: Option<Arc<ModuleConfig>>) -> Result<TaskOutputEvent> {
    let body: Value = serde_json::from_str(&response.body)?;
    let items = body["results"].as_array().unwrap_or(&vec![]).clone();

    let mut output = TaskOutputEvent::default();

    // 存储数据
    for item in &items {
        output.data.push(item.clone());
    }

    // 生成下游任务并携带元数据
    if body["has_next"].as_bool().unwrap_or(false) {
        let next = TaskParserEvent::from(&response)
            .add_meta("page", body["next_page"].as_i64().unwrap_or(2));
        output = output.with_task(next);
    }

    Ok(output)
}
```

### 节点间数据传递

使用 `TaskParserEvent::add_meta(key, value)` 向下游传递数据。下游节点在 `generate()` 的 `params` 参数中接收：

```rust
// 在节点 A 的 parser 中：
let next = TaskParserEvent::from(&response)
    .add_meta("user_id", "12345")
    .add_meta("cursor", "abc");
output.with_task(next)

// 在节点 B 的 generate 中：
let user_id = params.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
```

## ModuleTrait

模块将节点组织在一起：

```rust
#[async_trait]
pub trait ModuleTrait: Send + Sync {
    fn name(&self) -> String;
    fn version(&self) -> i32;
    fn should_login(&self) -> bool;
    fn default_arc() -> Arc<dyn ModuleTrait> where Self: Sized;

    /// 线性流水线：step_0 → step_1 → step_2
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![]
    }

    /// 自定义 DAG 图（可选）。
    async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
        None
    }

    /// 模块启动前的预处理（可选）。
    async fn pre_process(&self, _state: Arc<State>) {}

    /// 模块完成后的后处理（可选）。
    async fn post_process(&self, _state: Arc<State>) {}

    /// Cron 调度表达式（可选）。
    fn cron(&self) -> Option<String> { None }
}
```

### 线性流水线（add_step）

最简单的连接方式 — 节点按顺序执行：

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![
        Arc::new(ListNode),    // step_0: 抓取列表页
        Arc::new(DetailNode),  // step_1: 抓取详情页
        Arc::new(SaveNode),    // step_2: 最终处理
    ]
}
```

生成拓扑：`step_0 → step_1 → step_2`。

### 自定义 DAG（dag_definition）

用于扇出、汇合或非线性流水线：

```rust
async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
    Some(ModuleDagDefinition {
        nodes: vec![
            ModuleDagNodeDef {
                node_id: "start".into(),
                node: Arc::new(StartNode),
                placement_override: None,
                policy_override: None,
                tags: vec!["entry".into()],
            },
            ModuleDagNodeDef {
                node_id: "branch_a".into(),
                node: Arc::new(BranchANode),
                ..Default::default()
            },
            ModuleDagNodeDef {
                node_id: "branch_b".into(),
                node: Arc::new(BranchBNode),
                ..Default::default()
            },
            ModuleDagNodeDef {
                node_id: "merge".into(),
                node: Arc::new(MergeNode),
                ..Default::default()
            },
        ],
        edges: vec![
            ModuleDagEdgeDef { from: "start".into(), to: "branch_a".into() },
            ModuleDagEdgeDef { from: "start".into(), to: "branch_b".into() },
            ModuleDagEdgeDef { from: "branch_a".into(), to: "merge".into() },
            ModuleDagEdgeDef { from: "branch_b".into(), to: "merge".into() },
        ],
        entry_nodes: vec!["start".into()],
        default_policy: None,
        metadata: Default::default(),
    })
}
```

生成拓扑：
```
       ┌─── branch_a ───┐
start ─┤                 ├── merge
       └─── branch_b ───┘
```

### 混合模式（add_step + dag_definition）

两者同时存在时，mocra 会将它们合并。线性步骤会加上 `legacy_` 前缀：

- `add_step()` → `legacy_step_0 → legacy_step_1`
- `dag_definition()` → 自定义图

两者合并为一个 DAG。

## 注册与运行

```rust
#[tokio::main]
async fn main() {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(state, None).await;

    // 注册模块
    engine.register_module(MyModule::default_arc()).await;

    // 运行引擎（阻塞直到关闭）
    engine.run().await;
}
```

## 登录支持

如果 `should_login()` 返回 `true`，引擎会在 `generate()` 之前调用模块的登录流程。`LoginInfo` 会传递给每次 `generate()` 调用：

```rust
fn should_login(&self) -> bool { true }
```

## Cron 调度

返回 cron 表达式以周期性运行模块：

```rust
fn cron(&self) -> Option<String> {
    Some("0 */30 * * * *".into()) // 每 30 分钟
}
```

## 节点中的错误处理

- `generate()` 返回 `Err` 时，框架对非入口节点使用缓存的上一次请求进行一次性回退。
- `parser()` 返回 `Err` 时，发出 `TaskErrorEvent` 到错误队列，`stay_current_step: true` 触发同节点重试。
- 将 `retryable()` 设为 `false` 可跳过特定节点的重试。

## 示例

完整示例参见 [`simple/`](../../simple/) 目录：

- [`simple/module_node_trait_dag.rs`](../../simple/module_node_trait_dag.rs) — 扇出/汇合 DAG 模块
