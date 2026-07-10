---
name: mocra
description: >-
  Turn a plain-language scraping target ("collect X from Y") into a runnable mocra project.
  mocra is a distributed, event-driven crawler / data-collection framework for Rust. Use this
  whenever the user wants to build a scraper, crawler, or data collector with mocra, write a
  mocra Spider, or scaffold a mocra project from a collection goal.
---

# Building a mocra data-collection project

mocra is a Rust framework for crawlers / data-collection pipelines. Its high-level facade lets
a user go from "collect story titles from Hacker News" to a running project by implementing one
`Spider` trait — no database, no Redis, a single binary.

Your job: turn the user's natural-language target into a **correct, runnable** mocra project.

## Workflow

1. **Pin the target.** Infer from the request and state your assumptions; only ask if genuinely
   blocked. You need: start URL(s), the fields to extract, whether it paginates, whether it
   needs login.
2. **Look at the real page before writing any selector — never guess the HTML/JSON.**
   Fetch it (`curl -sL <url>` or WebFetch) and read the actual markup or JSON payload.
   - JSON endpoint → plan `res.json::<T>()` with a `#[derive(Deserialize)]` struct.
   - HTML page → plan CSS selectors with the `scraper` crate. **mocra hands you the raw
     `Response`; it does not bundle an HTML parser.**
3. **Scaffold.** Run `scaffold.sh <dir> [SpiderName]` (bundled here) for a compiling skeleton,
   then fill in `start` (seed URLs) and `parse` (extract → `cx.emit`).
4. **Verify.** `cargo check`, then `cargo run` on a small seed set and confirm items print.
   Fix and re-run — do not hand back code you did not compile.

## Minimal API — the whole 80% path

```rust
use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Item { title: String, url: String }      // your output record; Send + 'static

struct MySpider;

#[async_trait]
impl Spider for MySpider {
    type Item = Item;
    fn name(&self) -> &str { "my_spider" }

    async fn start(&self, s: &mut Seeds) {       // seed the crawl
        s.get("https://example.com/page/1");     // s.get(url) -> &mut Request (set headers/meta)
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        // res.status_code:u16 · res.text()? · res.text_lossy() · res.json::<T>()? · res.content:Vec<u8>
        cx.emit(Item { title: "…".into(), url: res.module_id() });   // produce a typed item
        cx.follow_get("https://example.com/page/2");                 // enqueue a follow-up request
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(MySpider, on_item(|it: Item| async move { println!("{it:?}"); }))
        .run()
        .await
}
```

For anything past this, read the bundled files (load on demand — don't dump them at the user):

- **`reference.md`** — full API: `Spider` / `Seeds` / `Ctx` / `Request` / `Response` / sinks
  (`on_item`, `ChannelSink`, custom `DataSink`) / builder options (`.dashboard`, `.cluster`,
  `.default_downloader`, `.from_toml`) / feature flags.
- **`recipes.md`** — copy-paste patterns: HTML via `scraper`, JSON APIs, pagination, custom
  headers / User-Agent, collecting into a `Vec`, the dashboard, a custom (browser) downloader,
  the embedded cluster.
- **`scaffold.sh`** — `scaffold.sh <project-dir> [SpiderName]` writes a compiling project
  (`Cargo.toml` + `src/main.rs` skeleton with both the HTML and JSON paths stubbed).

## Rules that keep the output correct

- **Never invent a selector or a field.** Fetch the page and read the real DOM/JSON first.
- mocra does **not** parse HTML — add `scraper` for CSS selectors, or use `res.json()` for JSON.
- `type Item` must be `Send + 'static`; derive `serde::Serialize` when the sink serializes it.
- A default single-node run **auto-seeds and exits when idle** — ideal for one-shot scrapes.
  A long-running service (e.g. `.dashboard(port)`) keeps the engine up.
- `res.text()` borrows `&self` and errors on invalid UTF-8; prefer `res.text_lossy()` for HTML
  from the wild.
- Be a good citizen: set a real `User-Agent`, keep concurrency reasonable, and respect the
  target's Terms of Service / robots.txt. Do not scaffold anything that hammers a site, evades
  rate limits or bot protection, or scrapes behind a login the user isn't authorized to use.
