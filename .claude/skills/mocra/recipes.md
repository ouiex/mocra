# mocra recipes

Copy-paste patterns. All assume `use mocra::prelude::*;` and an `impl Spider`. mocra returns the
raw `Response` — parsing is yours (`scraper` for HTML, `res.json()` for JSON).

## 1. HTML with `scraper` (CSS selectors)

```rust
async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
    let html = res.text_lossy();
    let doc = scraper::Html::parse_document(&html);
    let row = scraper::Selector::parse("article.item").unwrap();
    let title = scraper::Selector::parse("h2 a").unwrap();
    for item in doc.select(&row) {
        if let Some(a) = item.select(&title).next() {
            cx.emit(Item {
                title: a.text().collect::<String>().trim().to_string(),
                url: a.value().attr("href").unwrap_or_default().to_string(),
            });
        }
    }
    Ok(())
}
```

`Selector::parse` returns a `Result`; `.unwrap()` is fine for static selectors you control.

## 2. JSON API

```rust
#[derive(serde::Deserialize)]
struct ApiPage { results: Vec<ApiRow> }
#[derive(serde::Deserialize)]
struct ApiRow { id: u64, name: String }

async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
    let page: ApiPage = res.json()?;          // deserialize the body
    for r in page.results {
        cx.emit(Item { id: r.id, name: r.name });
    }
    Ok(())
}
```

## 3. Pagination / following links

```rust
async fn start(&self, s: &mut Seeds) {
    s.get("https://example.com/list?page=1");
}

async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
    // … emit items from this page …

    // enqueue the next page (its response comes back into parse)
    if let Some(next) = find_next_url(&res.text_lossy()) {
        cx.follow_get(next);
    }
    Ok(())
}
```

`cx.follow` / `cx.follow_get` re-enter this same spider's `parse`. Add a stop condition (max
pages, empty result set) so it terminates.

## 4. Custom headers / User-Agent / timeout

```rust
async fn start(&self, s: &mut Seeds) {
    let req = s.get("https://api.example.com/v1/items");
    req.headers = Headers::new()
        .add("User-Agent", "acme-collector/1.0 (+https://acme.example)")
        .add("Authorization", "Bearer <token>");
    req.timeout = 30;   // seconds
}
```

## 5. Collect results instead of printing

Use a `ChannelSink` to pull items out of the run:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Item>(256);
    let engine = tokio::spawn(async move {
        Mocra::builder().spider(MySpider, ChannelSink::new(tx)).run().await
    });

    let mut all = Vec::new();
    while let Some(item) = rx.recv().await { all.push(item); }  // ends when the run finishes
    engine.await.ok();

    println!("collected {} items", all.len());
    // e.g. std::fs::write("out.json", serde_json::to_vec_pretty(&all).unwrap()).unwrap();
    Ok(())
}
```

Or write to a file / DB directly inside `on_item(|item| async move { … })`.

## 6. Observability dashboard (`--features dashboard`)

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async {}))
    .dashboard(8080)         // GET / → web UI;  /metrics,  /observability/{engine,cluster,system,logs}
    .run().await?;
// open http://127.0.0.1:8080  — metrics / logs / tasks / performance, no frontend build
```

The dashboard keeps the engine running (no idle exit) so the panels stay live.

## 7. Custom downloader (e.g. a headless browser)

When a target needs JS rendering or a special client, implement `Downloader` and inject it.
Replace the default globally, or register it by name and select it per request. Needs
`async-trait` and `semver` as extra deps. The implementor must be `Clone` (`Box<dyn Downloader>`
is cloned internally).

```rust
use mocra::prelude::downloader::{Downloader, DownloadConfig};

#[derive(Clone)]
struct BrowserDownloader { /* your headless-browser handle */ }

#[async_trait]
impl Downloader for BrowserDownloader {
    fn name(&self) -> String { "browser".into() }
    fn version(&self) -> semver::Version { semver::Version::new(1, 0, 0) }
    async fn download(&self, request: Request) -> Result<Response> {
        // render request.url in a browser, build and return a Response …
        todo!()
    }
    async fn set_config(&self, _id: &str, _config: DownloadConfig) {}
    async fn set_limit(&self, _id: &str, _limit: f32) {}
    async fn health_check(&self) -> Result<()> { Ok(()) }
    // fn close(&self) has a default impl — override only if you hold resources.
}

Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async {}))
    .default_downloader(BrowserDownloader { /* … */ })   // swap reqwest everywhere
    // .downloader(BrowserDownloader { … })              // …or register by name; then set
    //                                                   // req.downloader = "browser" per request
    .run().await?;
```

Building a `Response` inside `download` means copying the correlation fields (`id`, `run_id`,
`module`, `context`, …) from the incoming `Request` — see the runnable
`examples/custom_downloader.rs` in the mocra repo for the full, compiling pattern.

## 8. Embedded cluster (`--features cluster-embedded`, no Redis)

```rust
// first node bootstraps; others join a seed address
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async {}))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;

Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async {}))
    .cluster(ClusterConfig::join(2, "127.0.0.1:7002", "./data/n2", "127.0.0.1:7001"))
    .run().await?;
```

Leader election / locks / membership run on embedded Raft + redb; the data-plane queue is
chosen independently (in-memory, Redis Streams, Kafka, NATS).
