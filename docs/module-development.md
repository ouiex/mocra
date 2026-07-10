# Module Development (Advanced)

> Most users should start with the [facade quickstart](getting-started.md) — single-node, no DB/Redis. This guide covers the advanced ModuleTrait/DAG path for multi-stage, multi-node, or DB-driven pipelines.

The `Spider` facade covers most single-node scrapes. When you need **multiple stages**
(fan-out / fan-in), **login / session** flows, cursor-based **pagination** across nodes, or a
**DB-driven multi-tenant** pipeline, drop down to the lower-level `ModuleTrait` /
`ModuleNodeTrait` API. This is the same engine the facade runs on — just with the graph and the
request/parse lifecycle in your hands.

Install (advanced features are opt-in):

```toml
[dependencies]
mocra = { version = "0.4", features = ["store"] }   # `store` unlocks the DB-backed model (below)
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

## Concepts

| Concept | Description |
|---|---|
| **Module** (`ModuleTrait`) | A named unit of work — declares login/version and how its nodes are wired |
| **Node** (`ModuleNodeTrait`) | A single stage — generates requests (`generate`) and parses responses (`parser`) |
| **DAG** | The execution graph of nodes within a module (linear chain or custom graph) |

A module contains one or more nodes wired into a DAG, either as a linear chain (`add_step()`) or
a custom graph (`dag_definition()`). See the [DAG Guide](dag-guide.md) for graph topology,
routing, and the advance/fallback gates.

## How the facade maps onto this

The facade is a thin adapter over exactly this API. `Mocra::builder().spider(spider, sink)` wraps
your `Spider` into a **single-node module**:

- `SpiderModule` implements `ModuleTrait`; its `add_step()` returns one node.
- `SpiderNode` implements `ModuleNodeTrait` (with `stable_node_key() == "spider"`): its
  `generate()` seeds the requests from `Spider::start`, and its `parser()` calls `Spider::parse`,
  forwarding emitted items to your `DataSink` and turning `Ctx::follow` into follow-up requests.

So **a `Spider` is exactly a one-node module.** Everything below is what you reach for when one
node is not enough.

> The `Mocra::builder()` facade only registers `Spider`s. Hand-written `ModuleTrait` modules are
> registered directly on the `Engine` — see [Registration and Running](#registration-and-running).

## ModuleNodeTrait

Each node implements two operations — `generate` (produce requests) and `parser` (handle
responses):

```rust
#[async_trait]
pub trait ModuleNodeTrait: Send + Sync {
    /// Produce a stream of HTTP requests for this stage.
    async fn generate(
        &self,
        config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>>;

    /// Parse a single downloaded response into data + downstream tasks.
    async fn parser(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent>;

    /// Whether this node should be retried on failure (default: true).
    fn retryable(&self) -> bool { true }

    /// Stable, unique node id within the module's DAG (default: `""` → a random UUID).
    /// Return a short constant so node ids survive DAG rebuilds (needed for correct
    /// error-retry routing). See the DAG Guide.
    fn stable_node_key(&self) -> &'static str { "" }
}
```

### generate()

`generate()` returns a `SyncBoxStream<'static, Request>` — a boxed, pinned `Stream`. The
`ToSyncBoxStream` extension trait turns a `Vec<Request>` into one; you can also build a stream
by hand with `futures::stream::iter`:

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
    vec![req].into_stream_ok()   // or: Ok(Box::pin(futures::stream::iter(vec![req])))
}
```

`Request::new(url, method)` takes any `impl AsRef<str>` for both arguments, so
`RequestMethod::Get` (or `"GET"`) both work.

**Request fields you can set** (get a `&mut Request` back, or build one and mutate it):

| Field | Description |
|---|---|
| `url` | Target URL |
| `method` | HTTP method |
| `headers` | Custom headers (`Headers::new().add(k, v)`) |
| `body` / `json` / `form` | Request payload (for POST/PUT) |
| `account` / `platform` | Tenant identifiers (usually inherited from the task) |
| `meta` | Arbitrary JSON metadata (carried through to the response) |
| `timeout` | Per-request timeout (seconds) |
| `downloader` | Route to a named custom downloader |
| `priority` | Scheduling priority |

### parser()

`parser()` receives a `Response` and returns a `TaskOutputEvent`:

```rust
pub struct TaskOutputEvent {
    pub data: Vec<DataEvent>,             // parsed data destined for the data-store pipeline
    pub parser_task: Vec<TaskParserEvent>, // tasks routed to the next node(s)
    pub error_task: Option<TaskErrorEvent>,
    pub stop: Option<bool>,               // signal module-level stop
}
```

Build it with the chainable helpers — `with_data`, `with_task` / `with_tasks`, `with_error`,
`with_stop`:

```rust
async fn parser(
    &self,
    response: Response,
    _config: Option<Arc<ModuleConfig>>,
) -> Result<TaskOutputEvent> {
    let body: serde_json::Value = response.json()?;

    // Emit data (see "Emitting data" below).
    let record = DataEvent::from(&response)
        .with_file(response.content.clone())
        .with_name("items.json")
        .with_path("./data/example");

    let mut out = TaskOutputEvent::default().with_data(vec![record]);

    // Route a follow-up task carrying the next cursor to the successor node(s).
    if body["has_next"].as_bool().unwrap_or(false) {
        let next = TaskParserEvent::from(&response)
            .add_meta("page", body["next_page"].as_i64().unwrap_or(2));
        out = out.with_task(next);
    }

    Ok(out)
}
```

### Emitting data

`TaskOutputEvent.data` is a `Vec<DataEvent>`, not raw JSON. Produce a `DataEvent` with the
`StoreTrait` builders:

- **Files / raw bytes** — `DataEvent::from(&response).with_file(bytes).with_name(..).with_path(..)`
  yields a `FileStore` (which implements `StoreTrait`).
- **DataFrames** (feature `polars`) — `DataEvent::from(&response).with_df(df)` yields a
  `DataFrameStore`.
- **Custom records** — implement `StoreTrait` (`fn build(&self) -> DataEvent`) on your own type.

Pass any of these to `with_data(vec![..])`. From there the engine's data-store pipeline and
`DataStoreMiddleware` take over. (Contrast with the facade, where `cx.emit(item)` sends a typed
record straight to your `DataSink` — no `StoreTrait` needed.)

### Passing data between nodes

Use `TaskParserEvent::add_meta(key, value)` to forward state. The downstream node reads it from
`generate()`'s `params`:

```rust
// In node A's parser:
let next = TaskParserEvent::from(&response)
    .add_meta("user_id", "12345")
    .add_meta("cursor", "abc");
out.with_task(next)

// In node B's generate:
let user_id = params.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
```

### Pagination

- **Across nodes** — a list node routes one `TaskParserEvent` per page/cursor to a detail node,
  which reads the cursor from its `params` (as above).
- **Looping on the same node** — return a task with `stay_current_step()` so it re-enters the
  current node's `generate()` with updated params instead of advancing:

  ```rust
  if let Some(cursor) = next_cursor {
      let next = TaskParserEvent::from(&response)
          .add_meta("cursor", cursor)
          .stay_current_step();   // re-run THIS node, not a successor
      out = out.with_task(next);
  }
  // Return no task when there is no next page → the node stops looping.
  ```

## ModuleTrait

A module ties nodes together and declares module-level behavior:

```rust
#[async_trait]
pub trait ModuleTrait: Send + Sync {
    fn name(&self) -> String;
    fn version(&self) -> i32;

    /// Whether the engine runs a login flow first. NOTE: defaults to `true`.
    fn should_login(&self) -> bool { true }

    /// Static request headers / cookies applied to this module (optional).
    async fn headers(&self) -> Headers { Headers::default() }
    async fn cookies(&self) -> Cookies { Cookies::default() }

    /// Constructs a boxed instance (required — used by the module factory / error-retry rebuilds).
    fn default_arc() -> Arc<dyn ModuleTrait> where Self: Sized;

    /// Custom DAG graph (optional).
    async fn dag_definition(&self) -> Option<ModuleDagDefinition> { None }

    /// Linear pipeline of nodes (optional).
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> { vec![] }

    /// Hooks around a run (optional).
    async fn pre_process(&self, _config: Option<Arc<ModuleConfig>>) -> Result<()> { Ok(()) }
    async fn post_process(&self, _config: Option<Arc<ModuleConfig>>) -> Result<()> { Ok(()) }

    /// Cron schedule (optional; `None` = no scheduled startup).
    fn cron(&self) -> Option<CronConfig> { None }
}
```

> `should_login()` defaults to **`true`** on `ModuleTrait`. Override it to return `false` for
> modules that need no login. (The facade's `Spider::should_login()` defaults to `false`.)

### Linear Pipeline (add_step)

The simplest wiring — nodes execute in sequence:

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![
        Arc::new(ListNode),    // fetch listing pages
        Arc::new(DetailNode),  // fetch detail pages
        Arc::new(SaveNode),    // final processing
    ]
}
```

The nodes are chained in order (`ListNode → DetailNode → SaveNode`); the first is the entry
node. Each node's id comes from its `stable_node_key()` (falling back to a generated UUID).

### Custom DAG (dag_definition)

For fan-out, fan-in, or non-linear pipelines, return a `ModuleDagDefinition`. The ergonomic way
is the builder — you describe edges between `ModuleDagNodeDef`s and the entry nodes and node list
are derived automatically:

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

`ModuleDagNodeDef` implements `Clone` but **not** `Default`, so construct nodes with
`ModuleDagNodeDef::new(node)` (optionally `.with_id("..")`) rather than struct-update syntax. See
the [DAG Guide](dag-guide.md) for the full field reference and routing semantics.

### Mixed (add_step + dag_definition)

If a module returns **both**, mocra merges them into one DAG; the linear-chain node ids are
namespaced with a `legacy_` prefix to avoid colliding with the custom graph.

## Login (should_login + store)

When `should_login()` returns `true`, the engine runs the module's login flow before `generate()`
and hands the resulting `LoginInfo` to every `generate()` call:

```rust
async fn generate(
    &self,
    _config: Arc<ModuleConfig>,
    _params: Map<String, Value>,
    login_info: Option<LoginInfo>,
) -> Result<SyncBoxStream<'static, Request>> {
    if let Some(info) = &login_info {
        // info.cookies: Vec<CookieItem>,  info.useragent: String
        log::info!("logged in: {} cookies, ua={}", info.cookies.len(), info.useragent);
    }
    // ...build authenticated requests...
    Vec::<Request>::new().into_stream_ok()
}
```

The **`store` feature** unlocks the DB-backed **account × platform × module** model (sea-orm
entities from the `mocra-store` crate) that login and multi-tenant modules use to persist
credentials and per-tenant relations. It requires a TOML config with a database URL
(`.from_toml(..)` / `State::try_new(..)`); see [Configuration](configuration.md).

## Cron Scheduling

Return a cron config to run the module periodically:

```rust
fn cron(&self) -> Option<CronConfig> {
    Some(CronConfig::new("0 */30 * * * *")) // every 30 minutes
}
```

## Registration and Running

Hand-written `ModuleTrait` modules run on the `Engine` directly (the facade builder only takes
`Spider`s):

```rust
use std::sync::Arc;
use mocra::prelude::*;
use mocra::prelude::engine::Engine;
use mocra::common::state::State;

#[tokio::main]
async fn main() {
    // Advanced modules need a config (DB / Redis / queues) — see Configuration.
    let state = Arc::new(State::try_new("config.toml").await.expect("init state"));
    let engine = Engine::new(state, None).await.expect("init engine");

    engine.register_module(MyModule::default_arc()).await;

    // Run the engine (drives the full download / parse pipeline; blocks until shutdown).
    engine.start().await;
}
```

## Error Handling in Nodes

- If `generate()` returns `Err` on a **non-entry** node, the DAG's one-shot fallback gate
  re-injects the previous request (traced via `prefix_request`) once, so a transient failure
  retries without user code.
- If `parser()` returns `Err`, a `TaskErrorEvent` is emitted to the error queue and the node is
  retried.
- Set `retryable()` to `false` on a node to skip retries for it.

See [DAG Guide → Advance / Fallback gates](dag-guide.md#advance-gate) for the exact mechanics.

## Examples

- Runnable facade example: [`examples/spider_quickstart.rs`](../examples/spider_quickstart.rs)
  (single node, no DB/Redis) — the one-node case of everything above.
- For multi-node graphs, fan-out/fan-in, and the routing model, continue to the
  [DAG Guide](dag-guide.md).
