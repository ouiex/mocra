# mocra API reference (facade)

Everything below is re-exported from `mocra::prelude`. Start every file with `use mocra::prelude::*;`.

## Cargo setup

```toml
[dependencies]
mocra = "0.2"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
scraper = "0.20"          # optional: HTML CSS selectors (mocra returns the raw Response)
```

Optional mocra features — enable like `mocra = { version = "0.2", features = ["dashboard"] }`:

| Feature | Unlocks |
|---|---|
| `dashboard` | Observability HTTP API + built-in web UI (`.dashboard(port)`) |
| `cluster-embedded` | Embedded Raft + redb control plane (`.cluster(ClusterConfig)`) |
| `store` | DB-backed account × platform × module model (sea-orm) |
| `queue-kafka` / `queue-nats` | Kafka / NATS JetStream data-plane queue backends |
| `polars` / `excel` | DataFrame / Excel data handling |
| `js-v8` | Embedded V8 JS runtime for parse scripts |

## `Spider` trait

```rust
#[async_trait]
pub trait Spider: Send + Sync + 'static {
    type Item: Send + 'static;                  // your output record

    fn name(&self) -> &str;                      // unique module name
    async fn start(&self, seeds: &mut Seeds);    // seed URLs
    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()>;

    fn should_login(&self) -> bool { false }     // override for login flows (needs `store`)
    fn version(&self) -> i32 { 1 }
}
```

## `Seeds` — used inside `start`

- `s.get(url) -> &mut Request` — enqueue a GET; the returned `&mut Request` lets you set fields.
- `s.add(Request) -> &mut Request` — enqueue any method (build a `Request` first).

## `Ctx<Item>` — used inside `parse`

- `cx.emit(item)` — produce one typed item (delivered to your `DataSink`).
- `cx.follow_get(url)` — enqueue a follow-up GET; its response re-enters `parse`.
- `cx.follow(Request)` — enqueue a follow-up with any method / headers.

## `Response`

| Member | Type | Notes |
|---|---|---|
| `res.status_code` | `u16` | HTTP status |
| `res.text()` | `Result<&str>` | UTF-8 body; **errors** on invalid UTF-8 |
| `res.text_lossy()` | `Cow<str>` | lossy UTF-8 — prefer for wild HTML |
| `res.json::<T>()` | `Result<T>` | deserialize a JSON body (`T: DeserializeOwned`) |
| `res.content` | `Vec<u8>` | raw bytes |
| `res.headers` / `res.cookies` | | response headers / cookies |
| `res.module_id()` | `String` | the source module id (handy as a record key) |

## `Request`

Construct with `Request::new(url, RequestMethod::Get)`, or get a `&mut Request` from
`s.get` / `s.add` and set public fields directly:

```rust
let req = s.get("https://api.example.com/items");
req.headers = Headers::new()
    .add("User-Agent", "my-bot/1.0")
    .add("Accept", "application/json");
req.timeout = 30;                 // seconds
req.downloader = "browser".into(); // route to a named custom downloader (see recipes)
```

Common fields: `headers: Headers`, `cookies`, `timeout: u64`, `params`, `json`, `body`, `form`,
`proxy`, `downloader: String`, `priority`. `RequestMethod` is in the prelude
(`RequestMethod::{Get, Post, …}`). `Headers::new().add(k, v)` chains (builder style).

## Sinks — where emitted items go

- `on_item(|item| async move { … })` — closure sink; do anything async (print, DB insert, send).
- `ChannelSink::new(tx)` — push each item into a `tokio::sync::mpsc::Sender<Item>`.
- Custom — implement the trait:
  ```rust
  #[async_trait]
  impl DataSink<Item> for MySink {
      async fn write(&self, item: Item) -> Result<()> { /* … */ Ok(()) }
  }
  ```

## `Mocra::builder()`

| Method | Effect |
|---|---|
| `.spider(spider, sink)` | Register a spider + its sink (call repeatedly for several spiders) |
| `.from_toml(path)` | Load a TOML config (DB / Redis / api). Omit → single-node, no DB/Redis |
| `.default_downloader(d)` | Replace the default reqwest downloader (browser render, proxy, …) |
| `.downloader(d)` | Register a named downloader; routed when `Request.downloader == d.name()` |
| `.dashboard(port)` | *(feature `dashboard`)* observability API + web UI at `http://host:port/` |
| `.cluster(cfg)` | *(feature `cluster-embedded`)* embedded Raft control plane |
| `.run().await` | Build + start the engine; blocks until shutdown |

Without `.from_toml`, a single-node in-memory engine **auto-seeds each spider and exits when
idle** — perfect for one-shot scrapes.
