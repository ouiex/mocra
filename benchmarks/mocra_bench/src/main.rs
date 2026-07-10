//! mocra benchmark node. Crawls this node's slice [OFFSET, OFFSET+COUNT) of item URLs, parses each
//! (CSS selectors via `scraper`), and self-times the STEADY-STATE crawl (first emit -> last emit,
//! excluding process startup) — printed as NODE_RESULT for the driver to aggregate.
use async_trait::async_trait;
use mocra::prelude::*;
use scraper::{Html, Selector};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

const BASE: &str = "http://127.0.0.1:8899";

struct ItemSpider;

#[async_trait]
impl Spider for ItemSpider {
    type Item = ();
    fn name(&self) -> &str {
        "itembench"
    }
    async fn start(&self, s: &mut Seeds) {
        let env =
            |k: &str, d: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        let (offset, count) = (env("OFFSET", 0), env("COUNT", 5000));
        for i in offset..offset + count {
            s.get(format!("{BASE}/item/{i}"));
        }
    }
    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        let body = res.text_lossy();
        let doc = Html::parse_document(&body);
        let _title = doc
            .select(&Selector::parse("h1.title").unwrap())
            .next()
            .map(|e| e.text().collect::<String>());
        let _price = doc
            .select(&Selector::parse("span.price").unwrap())
            .next()
            .map(|e| e.text().collect::<String>());
        let _desc = doc
            .select(&Selector::parse("p.desc").unwrap())
            .next()
            .map(|e| e.text().collect::<String>());
        cx.emit(());
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let count: usize = std::env::var("COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);

    let done = Arc::new(AtomicUsize::new(0));
    let first: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());

    let sink = on_item(move |_: ()| {
        let done = done.clone();
        let first = first.clone();
        async move {
            let n = done.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                let _ = first.set(Instant::now());
            }
            if n == count {
                let secs = first.get().unwrap().elapsed().as_secs_f64();
                // (count-1) items span the first->last window (startup excluded).
                let rate = (count - 1) as f64 / secs;
                println!("NODE_RESULT count={count} secs={secs:.4} rate={rate:.0}");
                use std::io::Write;
                let _ = std::io::stdout().flush();
                std::process::exit(0);
            }
        }
    });

    Mocra::builder().spider(ItemSpider, sink).run().await
}
