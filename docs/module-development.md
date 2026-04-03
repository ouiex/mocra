# Module Development

This guide covers how to build modules and nodes for mocra.

## Concepts

| Concept | Description |
|---|---|
| **Module** (`ModuleTrait`) | A named unit of work — defines what to crawl and how nodes are wired |
| **Node** (`ModuleNodeTrait`) | A single stage — generates requests and parses responses |
| **DAG** | The execution graph of nodes within a module |

A module contains one or more nodes. Nodes are connected into a DAG either via a linear chain (`add_step()`) or a custom graph (`dag_definition()`).

## ModuleNodeTrait

Each node implements two operations:

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

    /// Parse a single downloaded response.
    /// Returns data to store and/or tasks for downstream nodes.
    async fn parser(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent>;

    /// Whether this node should be retried on failure (default: true).
    fn retryable(&self) -> bool { true }
}
```

### generate()

`generate()` returns a `SyncBoxStream<'static, Request>` — a boxed, pinned `Stream`.
Use `futures::stream::iter(vec![...])` for a fixed set:

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

**Request fields you can set:**

| Field | Description |
|---|---|
| `url` | Target URL |
| `method` | GET, POST, etc. |
| `headers` | Custom headers |
| `body` | Request body (for POST/PUT) |
| `account` | Account identifier |
| `platform` | Platform identifier |
| `module` | Module name (usually auto-set) |
| `meta` | Arbitrary JSON metadata (passed through to parser) |

### parser()

`parser()` receives a `Response` and returns `TaskOutputEvent`, which contains:

- **`data`** — parsed data items for storage (a `Vec`)
- **`parser_task`** — `Vec<TaskParserEvent>` for downstream node(s)

```rust
async fn parser(&self, response: Response, _config: Option<Arc<ModuleConfig>>) -> Result<TaskOutputEvent> {
    let body: Value = serde_json::from_str(&response.body)?;
    let items = body["results"].as_array().unwrap_or(&vec![]).clone();

    let mut output = TaskOutputEvent::default();

    // Store data
    for item in &items {
        output.data.push(item.clone());
    }

    // Spawn downstream task with metadata
    if body["has_next"].as_bool().unwrap_or(false) {
        let next = TaskParserEvent::from(&response)
            .add_meta("page", body["next_page"].as_i64().unwrap_or(2));
        output = output.with_task(next);
    }

    Ok(output)
}
```

### Passing data between nodes

Use `TaskParserEvent::add_meta(key, value)` to pass data forward. The downstream node receives it in `generate()`'s `params` argument:

```rust
// In node A's parser:
let next = TaskParserEvent::from(&response)
    .add_meta("user_id", "12345")
    .add_meta("cursor", "abc");
output.with_task(next)

// In node B's generate:
let user_id = params.get("user_id").and_then(|v| v.as_str()).unwrap_or("");
```

## ModuleTrait

A module ties nodes together:

```rust
#[async_trait]
pub trait ModuleTrait: Send + Sync {
    fn name(&self) -> String;
    fn version(&self) -> i32;
    fn should_login(&self) -> bool;
    fn default_arc() -> Arc<dyn ModuleTrait> where Self: Sized;

    /// Linear pipeline: step_0 → step_1 → step_2
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![]
    }

    /// Custom DAG graph (optional).
    async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
        None
    }

    /// Pre-processing before the module starts (optional).
    async fn pre_process(&self, _state: Arc<State>) {}

    /// Post-processing after the module completes (optional).
    async fn post_process(&self, _state: Arc<State>) {}

    /// Cron schedule expression (optional).
    fn cron(&self) -> Option<String> { None }
}
```

### Linear Pipeline (add_step)

The simplest wiring — nodes execute in sequence:

```rust
async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
    vec![
        Arc::new(ListNode),    // step_0: fetch listing pages
        Arc::new(DetailNode),  // step_1: fetch detail pages
        Arc::new(SaveNode),    // step_2: final processing
    ]
}
```

This produces: `step_0 → step_1 → step_2`.

### Custom DAG (dag_definition)

For fan-out, fan-in, or non-linear pipelines:

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

This produces:
```
       ┌─── branch_a ───┐
start ─┤                 ├── merge
       └─── branch_b ───┘
```

### Mixed (add_step + dag_definition)

If both are present, mocra merges them. Linear steps are prefixed with `legacy_`:

- `add_step()` → `legacy_step_0 → legacy_step_1`
- `dag_definition()` → custom graph

Both are combined into a single DAG.

## Registration and Running

```rust
#[tokio::main]
async fn main() {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(state, None).await;

    // Register modules
    engine.register_module(MyModule::default_arc()).await;

    // Run the engine (blocks until shutdown)
    engine.run().await;
}
```

## Login Support

If `should_login()` returns `true`, the engine calls the module's login flow before `generate()`. The `LoginInfo` is passed to each `generate()` call:

```rust
fn should_login(&self) -> bool { true }
```

## Cron Scheduling

Return a cron expression to run the module periodically:

```rust
fn cron(&self) -> Option<String> {
    Some("0 */30 * * * *".into()) // every 30 minutes
}
```

## Error Handling in Nodes

- If `generate()` returns `Err`, the framework applies a one-shot fallback (for non-entry nodes) using the cached previous request.
- If `parser()` returns `Err`, a `TaskErrorEvent` is emitted to the error queue with `stay_current_step: true`, triggering retry on the same node.
- Set `retryable()` to `false` to skip retries for a specific node.

## Examples

See the complete examples in the [`simple/`](../simple/) directory:

- [`simple/module_node_trait_dag.rs`](../simple/module_node_trait_dag.rs) — Fan-out/fan-in DAG module
- [`simple/module_node_trait_dag.rs`](../simple/module_node_trait_dag.rs) — DAG module with fan-out/fan-in
