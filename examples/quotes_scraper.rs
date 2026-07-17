//! **quotes.toscrape.com end-to-end example: fetch → parse → save.**
//!
//! A complete crawler running against a real site, stringing together as much of the facade layer
//! as it reasonably can:
//!
//! | Capability | How this example uses it |
//! |---|---|
//! | Seeds + per-request tweaks | `Seeds::get()` in `start()` returns `&mut Request` for a custom UA / timeout |
//! | Pagination follow | Parse `li.next a` → `cx.follow_get()`, paging through all 10 pages |
//! | Detail-page fan-out | Each quote's author link → `cx.follow_get()` to fetch the author bio |
//! | Cross-page deduplication | A `HashSet` remembers queued authors, so one is never fetched twice |
//! | Content-based routing | `follow` passes only the URL (no custom meta), so page features tell listing / author pages apart |
//! | Typed emission | An `Item` enum (quote / author); `#[serde(tag = "kind")]` tags each saved row |
//! | Custom persistence | Implement `DataSink` to split the two kinds into two JSONL files |
//! | Observability (optional) | With `--features dashboard`, `.dashboard(8080)` starts the panel |
//!
//! Run it:
//!
//! ```bash
//! cargo run --example quotes_scraper
//! # Output: ./data/quotes/quotes.jsonl and ./data/quotes/authors.jsonl
//!
//! # With the monitoring panel (open http://127.0.0.1:8080 in a browser):
//! cargo run --example quotes_scraper --features dashboard
//! ```
//!
//! Exit behavior: without the dashboard, the engine exits automatically once the queue has been
//! idle for 30s; with the dashboard enabled it stays resident (the panel has to remain reachable),
//! so use Ctrl+C to stop it.

use async_trait::async_trait;
use mocra::prelude::*;
use scraper::{ElementRef, Html, Selector};
use serde::Serialize;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex as SyncMutex};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

const BASE: &str = "https://quotes.toscrape.com";
const OUT_DIR: &str = "./data/quotes";
const UA: &str = "mocra-example/0.4 (+https://github.com/ouiex/mocra)";

// Compile the CSS selectors once (parsing is a hot path — every page uses them).
static SEL_QUOTE: LazyLock<Selector> = LazyLock::new(|| Selector::parse("div.quote").unwrap());
static SEL_TEXT: LazyLock<Selector> = LazyLock::new(|| Selector::parse("span.text").unwrap());
static SEL_AUTHOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("small.author").unwrap());
static SEL_AUTHOR_LINK: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse(r#"a[href^="/author/"]"#).unwrap());
static SEL_TAG: LazyLock<Selector> = LazyLock::new(|| Selector::parse("div.tags a.tag").unwrap());
static SEL_NEXT: LazyLock<Selector> = LazyLock::new(|| Selector::parse("li.next a").unwrap());
static SEL_AUTHOR_DETAILS: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("div.author-details").unwrap());
static SEL_AUTHOR_TITLE: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("h3.author-title").unwrap());
static SEL_BORN_DATE: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("span.author-born-date").unwrap());
static SEL_BORN_LOCATION: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("span.author-born-location").unwrap());
static SEL_BIO: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("div.author-description").unwrap());

// ---------- Emitted data models ----------

#[derive(Debug, Serialize)]
struct Quote {
    text: String,
    author: String,
    author_url: String,
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
struct Author {
    name: String,
    born_date: String,
    born_location: String,
    bio: String,
}

/// A spider has exactly one `Item` type — an enum carries both kinds of emission, and
/// `tag = "kind"` gives every JSON row a type tag so downstream consumers can split them apart.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Item {
    Quote(Quote),
    Author(Author),
}

fn text_of(el: ElementRef<'_>) -> String {
    el.text().collect::<String>().trim().to_string()
}

fn abs(href: &str) -> String {
    format!("{BASE}{href}")
}

// ---------- Spider ----------

#[derive(Default)]
struct QuotesSpider {
    /// Author pages already queued, so authors recurring across the 10 listing pages are not
    /// fetched over and over.
    seen_authors: SyncMutex<HashSet<String>>,
}

#[async_trait]
impl Spider for QuotesSpider {
    type Item = Item;

    fn name(&self) -> &str {
        "quotes.toscrape"
    }

    fn version(&self) -> i32 {
        1
    }

    async fn start(&self, s: &mut Seeds) {
        // `get` returns `&mut Request` — you can go on to customize the request (UA, timeout,
        // headers, and so on).
        let req = s.get(format!("{BASE}/"));
        req.headers = Headers::new()
            .add("user-agent", UA)
            .add("accept-language", "en-US,en;q=0.9");
        req.timeout = 20;
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        if res.status_code != 200 {
            eprintln!(
                "[quotes] non-200 response, skipping: status={}",
                res.status_code
            );
            return Ok(());
        }

        let html = res.text_lossy();
        let doc = Html::parse_document(&html);

        // The facade's `follow` only passes the URL through, without custom meta (see
        // SpiderNode::generate), so we tell the page types apart by their features rather than by
        // a marker attached to the request.
        if doc.select(&SEL_AUTHOR_DETAILS).next().is_some() {
            self.parse_author(&doc, cx);
        } else {
            self.parse_listing(&doc, cx);
        }
        Ok(())
    }
}

impl QuotesSpider {
    /// Listing page: extract quotes → emit; author links → fan out (deduplicated); `next` → page
    /// on.
    fn parse_listing(&self, doc: &Html, cx: &mut Ctx<Item>) {
        let mut found = 0usize;

        for q in doc.select(&SEL_QUOTE) {
            let text = q.select(&SEL_TEXT).next().map(text_of).unwrap_or_default();
            let author = q
                .select(&SEL_AUTHOR)
                .next()
                .map(text_of)
                .unwrap_or_default();
            let tags: Vec<String> = q.select(&SEL_TAG).map(text_of).collect();
            let href = q
                .select(&SEL_AUTHOR_LINK)
                .next()
                .and_then(|a| a.value().attr("href"))
                .unwrap_or_default()
                .to_string();

            cx.emit(Item::Quote(Quote {
                text,
                author,
                author_url: abs(&href),
                tags,
            }));
            found += 1;

            // Author detail fan-out — each author is fetched only once.
            if !href.is_empty() {
                let is_new = {
                    // The lock is held only within this sync block (this function has no await, so
                    // it never spans an await point).
                    let mut seen = self.seen_authors.lock().expect("seen_authors poisoned");
                    seen.insert(href.clone())
                };
                if is_new {
                    cx.follow_get(abs(&href));
                }
            }
        }

        // Pagination: keep going while there is a next link; the last page has none, so this
        // converges naturally.
        if let Some(href) = doc
            .select(&SEL_NEXT)
            .next()
            .and_then(|a| a.value().attr("href"))
        {
            println!("[page] parsed {found} quotes, following next page {href}");
            cx.follow_get(abs(href));
        } else {
            println!("[page] parsed {found} quotes (this is the last page)");
        }
    }

    /// Author detail page: extract name / birth date / birthplace / bio.
    fn parse_author(&self, doc: &Html, cx: &mut Ctx<Item>) {
        let name = doc
            .select(&SEL_AUTHOR_TITLE)
            .next()
            .map(text_of)
            .unwrap_or_default();
        let born_date = doc
            .select(&SEL_BORN_DATE)
            .next()
            .map(text_of)
            .unwrap_or_default();
        let born_location = doc
            .select(&SEL_BORN_LOCATION)
            .next()
            .map(text_of)
            .unwrap_or_default();
        let bio = doc.select(&SEL_BIO).next().map(text_of).unwrap_or_default();

        println!("[author] {name}");
        cx.emit(Item::Author(Author {
            name,
            born_date,
            born_location,
            bio,
        }));
    }
}

// ---------- Persistence: a custom DataSink that splits JSONL output by type ----------

struct JsonlSink {
    quotes: Mutex<BufWriter<tokio::fs::File>>,
    authors: Mutex<BufWriter<tokio::fs::File>>,
    n_quotes: AtomicUsize,
    n_authors: AtomicUsize,
}

impl JsonlSink {
    async fn create(dir: &str) -> std::io::Result<Self> {
        tokio::fs::create_dir_all(dir).await?;
        let quotes = tokio::fs::File::create(format!("{dir}/quotes.jsonl")).await?;
        let authors = tokio::fs::File::create(format!("{dir}/authors.jsonl")).await?;
        Ok(Self {
            quotes: Mutex::new(BufWriter::new(quotes)),
            authors: Mutex::new(BufWriter::new(authors)),
            n_quotes: AtomicUsize::new(0),
            n_authors: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl DataSink<Item> for JsonlSink {
    async fn write(&self, item: Item) -> Result<()> {
        let line =
            serde_json::to_string(&item).map_err(|e| Error::new(ErrorKind::DataStore, Some(e)))?;

        let (file, counter, label) = match &item {
            Item::Quote(_) => (&self.quotes, &self.n_quotes, "quote"),
            Item::Author(_) => (&self.authors, &self.n_authors, "author"),
        };

        {
            let mut w = file.lock().await;
            w.write_all(line.as_bytes()).await?;
            w.write_all(b"\n").await?;
            // Flush per row: keeps the example easy to follow and loses no data on Ctrl+C;
            // throughput-sensitive setups can switch to batched flushes.
            w.flush().await?;
        }

        let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
        if n % 10 == 0 {
            println!("[saved] {label}: {n} rows written");
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let sink = JsonlSink::create(OUT_DIR)
        .await
        .map_err(|e| Error::new(ErrorKind::DataStore, Some(e)))?;

    println!("starting crawl of {BASE} — output goes to {OUT_DIR}/{{quotes,authors}}.jsonl");

    #[allow(unused_mut)]
    let mut builder = Mocra::builder().spider(QuotesSpider::default(), sink);

    // Optional: with the dashboard feature enabled, also bring up the monitoring panel (this
    // disables idle stop, so Ctrl+C is needed to exit).
    #[cfg(feature = "dashboard")]
    {
        builder = builder.dashboard(8080);
        println!("dashboard: http://127.0.0.1:8080 (Ctrl+C to stop)");
    }

    builder.run().await
}
