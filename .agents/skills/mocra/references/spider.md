# Spider and request flow

Use this reference when building a crawler, fixing request fields, parsing responses, or emitting data. Check `src/facade.rs`, `src/prelude.rs`, `docs/getting-started.md`, and `examples/` for the current public API.

## Smallest runnable path

In a consumer `Cargo.toml`, add `mocra = "0.4"`, `async-trait = "0.1"`, and `tokio` with macros and a runtime. Add `scraper` for HTML parsing: mocra provides response bytes but no HTML selector library. Add `serde` when the output format needs serialization. Within this repository, `examples/spider_quickstart.rs` is a starting point.

```rust
use async_trait::async_trait;
use mocra::prelude::*;

struct BodyLength;

#[async_trait]
impl Spider for BodyLength {
    type Item = usize;

    fn name(&self) -> &str { "body-length" }

    async fn start(&self, seeds: &mut Seeds) {
        seeds.get("https://example.org/");
    }

    async fn parse(&self, response: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit(response.content.len());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(BodyLength, on_item(|bytes: usize| async move { println!("{bytes} bytes"); }))
        .run()
        .await
}
```

`Spider::Item` requires `Send + 'static`; serialization is a sink requirement, not a `Spider` trait requirement. `name()` contributes to the module identity and deduplication namespace, so give each spider a distinct name.

## Seeds, follow-ups, and parsing

| Operation | Effect | Example |
| --- | --- | --- |
| `seeds.get(url)` | Queue a GET and return a mutable `&mut Request` | `examples/spider_quickstart.rs` |
| `seeds.add(req)` | Queue a constructed request with any method | `src/facade.rs` |
| `cx.emit(item)` | Send a typed item to the sink | `examples/quotes_scraper.rs` |
| `cx.follow_get(url)` | Create a follow-up GET; its response enters the same `parse` | `examples/quotes_scraper.rs` |
| `cx.follow(req)` | Queue a complete `Request`, including POST, headers, body, and cookies | `examples/follow_request.rs` |

For a follow-up POST:

```rust
let mut next = Request::new("https://example.org/api/next", RequestMethod::Post)
    .with_body(b"page=2".to_vec());
next.headers = Headers::new().add("content-type", "application/x-www-form-urlencoded");
next.timeout = 20;
cx.follow(next);
```

`Ctx::follow` carries the whole request through the task queue. When changing that path, inspect `SpiderNode::parser` and `SpiderNode::generate` in `src/facade.rs` and the regression tests. Checking only the URL would miss the earlier lost-fields bug. Pagination needs a termination condition; account for duplicate list or detail URLs.

`Response::text()` errors on invalid UTF-8; `text_lossy()` replaces invalid bytes. `json::<T>()` deserializes JSON, and `content` holds the raw bytes. `Response::get_meta` reads trait metadata carried by the request. Inspect a real page or API response before writing selectors or field names; a saved response can validate parsing offline.

## Output and execution

- `on_item` works for printing or an async output function. `ChannelSink` lets the caller collect items. A custom `DataSink` can route records to files or a database; see the JSONL sink in `examples/quotes_scraper.rs`.
- Without a config file, `Mocra::builder().run()` seeds each spider automatically and exits after an idle period (currently 30 seconds by default). `.dashboard(port)` keeps the unconfigured single-process engine running.
- Run `cargo run --example follow_request` for an offline check of follow-up request fields. `cargo run --example quotes_scraper` requires an external site and writes to `data/quotes/`.
