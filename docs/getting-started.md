# Getting Started

This guide gets you from an empty project to a running crawler in a few minutes.
You implement a **`Spider`**, hand it to `Mocra::builder()`, and call `.run()` — single-node,
in-memory, **no database**.

## Prerequisites

- **Rust 1.89+** (edition 2024).

That's it for the quickstart. mocra's default build is single-node with no external services.

> A **database** (PostgreSQL / SQLite) is *optional*. It is needed for the DB-backed
> account × platform × module task model (`store` feature), but not for this guide,
> TOML configuration by itself, or the embedded `cluster-embedded` control plane.

## Installation

Add mocra to your `Cargo.toml`:

```toml
[dependencies]
mocra = "0.5"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }

# Optional: mocra returns the raw HTTP Response — bring your own HTML parser.
scraper = "0.27"   # CSS selectors, for HTML targets
```

mocra hands you the raw `Response`; you choose how to parse it (`scraper` for HTML,
`res.json()` for JSON APIs — no parser is bundled).

## Your first Spider

A `Spider` answers two questions: **what to fetch** (`start`) and **how to parse each response**
(`parse`). Emitted items flow to a **sink** you provide. Here is a complete program that fetches
a URL and prints one typed record per response:

```rust
use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Page {
    module_id: String,
    status: u16,
}

struct Httpbin;

#[async_trait]
impl Spider for Httpbin {
    type Item = Page;

    fn name(&self) -> &str {
        "httpbin"
    }

    // Seed the initial request(s).
    async fn start(&self, s: &mut Seeds) {
        s.get("https://httpbin.org/get");
    }

    // Parse one downloaded response; `cx.emit` produces a typed item.
    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit(Page {
            module_id: res.module_id(),
            status: res.status_code,
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(Httpbin, on_item(|p: Page| async move {
            println!("[item] module={} status={}", p.module_id, p.status);
        }))
        .run()
        .await
}
```

## Run it

```bash
cargo run
```

No configuration file, no services to start. Without a TOML config, `.run()` spins up a
single-node, in-memory engine, **auto-seeds each registered spider**, and **exits once the
queues go idle** — ideal for one-shot scrapes.

## What just happened

- **`name()`** — a unique id used for queue topics, metric labels, and dedup.
- **`start(&mut Seeds)`** — `s.get(url)` enqueues a GET (returns `&mut Request` if you want to
  set headers, timeout, etc.); `s.add(Request)` enqueues any method.
- **`parse(Response, &mut Ctx)`** — `cx.emit(item)` produces one typed item;
  `cx.follow_get(url)` / `cx.follow(Request)` enqueue follow-up requests that re-enter `parse`
  (pagination, detail pages). `follow(Request)` preserves the request method, headers, body,
  cookies, and explicit proxy. Add a stop condition so following terminates.
- **`res.module_id()`** identifies the account/platform/module. It is not the fetched URL;
  use request metadata or response content when you need to identify a source page.
- **The sink** — `on_item(|item| async move { … })` runs your async closure for every emitted
  item (print, write a file, insert into a DB, send downstream).

## Parsing responses

mocra returns the raw `Response`. Common accessors:

| Accessor | Returns | Use for |
|---|---|---|
| `res.status_code` | `u16` | HTTP status |
| `res.text()` | `Result<&str>` | UTF-8 body (errors on invalid UTF-8) |
| `res.text_lossy()` | `Cow<str>` | lossy UTF-8 — prefer for wild HTML |
| `res.json::<T>()` | `Result<T>` | deserialize a JSON body |
| `res.content` | `Vec<u8>` | raw bytes |

HTML with `scraper`:

```rust
async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
    let html = res.text_lossy();
    let doc = scraper::Html::parse_document(&html);
    let sel = scraper::Selector::parse("h2 a").unwrap();
    for a in doc.select(&sel) {
        cx.emit(Item {
            title: a.text().collect::<String>().trim().to_string(),
            url: a.value().attr("href").unwrap_or_default().to_string(),
        });
    }
    Ok(())
}
```

JSON APIs — deserialize straight into your types:

```rust
#[derive(serde::Deserialize)]
struct ApiPage { results: Vec<Row> }
#[derive(serde::Deserialize)]
struct Row { id: u64, name: String }

async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
    let page: ApiPage = res.json()?;
    for r in page.results {
        cx.emit(Item { id: r.id, name: r.name });
    }
    Ok(())
}
```

## Getting items out

`on_item` is the quick path. To pull results out of the run instead of handling them inline, use
a `ChannelSink`:

```rust
let (tx, mut rx) = tokio::sync::mpsc::channel::<Item>(256);
let engine = tokio::spawn(async move {
    Mocra::builder().spider(MySpider, ChannelSink::new(tx)).run().await
});

let mut all = Vec::new();
while let Some(item) = rx.recv().await { all.push(item); }  // ends when the run finishes
engine.await.ok();
```

Or implement `DataSink<Item>` yourself for full control.

## Optional features

All are off by default; enable per need, e.g.
`mocra = { version = "0.5", features = ["dashboard"] }`.

| Feature | Unlocks |
|---|---|
| `dashboard` | Read-only observability HTTP API + built-in web UI (`.dashboard(port)`) |
| `cluster-embedded` | Embedded Raft + redb control plane (`.cluster(…)`) — no external coordinator |
| `store` | DB-backed account × platform × module model (sea-orm) |
| `queue-kafka` / `queue-nats` | Kafka / NATS JetStream data-plane queue backends |
| `polars` / `excel` | DataFrame / Excel data handling |
| `js-v8` | Embedded V8 JS runtime for parse scripts |
| `mimalloc` | mimalloc global allocator (**on by default**) |

## Next steps

- [Architecture](architecture.md) — how the queue-driven pipeline and DAG engine fit together.
- [Module Development](module-development.md) — the **advanced** path: implement `ModuleTrait` /
  `ModuleNodeTrait` for multi-stage pipelines, login flows, and custom middleware.
- [DAG Guide](dag-guide.md) — fan-out / fan-in graphs and advance gates.
- [Configuration](configuration.md) — the full TOML reference (DB, queues, API).
- [Follow-up Requests](follow-up-requests.md) — preserve POST, headers, cookies, metadata, and proxy through a follow-up.
- [Proxies and Downloaders](proxies-and-downloaders.md) — fixed vs managed proxies, rotation, and custom downloaders.
- [Runtime Tuning](runtime-tuning.md) — queue bounds, proxy Client cache, and DAG checkpoints.
- Runnable examples in [`../examples/`](../examples/):
  - [`spider_quickstart.rs`](../examples/spider_quickstart.rs) — the minimal `Spider` above (no DB).
  - [`custom_downloader.rs`](../examples/custom_downloader.rs) — implement the `Downloader` trait and inject it with `.default_downloader()`.
  - [`follow_request.rs`](../examples/follow_request.rs) — preserve a POST request through `Ctx::follow` (offline).
  - [`proxy_pool.rs`](../examples/proxy_pool.rs) — select and report proxy attempts (offline simulation).
  - [`explicit_proxy.rs`](../examples/explicit_proxy.rs) — use a reachable proxy for a `Spider` request.
  - [`quotes_scraper.rs`](../examples/quotes_scraper.rs) — pagination and JSONL output from a real site.
  - [`dashboard.rs`](../examples/dashboard.rs) — the built-in observability dashboard (`--features dashboard`).
  - [`cluster_quickstart.rs`](../examples/cluster_quickstart.rs) — a self-organizing embedded cluster (`--features cluster-embedded`).
