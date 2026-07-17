//! Minimal Spider example (the refactor Phase 1 facade API).
//!
//! Demonstrates the new high-level entry point: implement a `Spider`, register and run it with
//! `Mocra::builder()`, and receive typed data through `on_item` — no need to implement
//! `DataStoreMiddleware`, and no hand-written `ModuleTrait` + `ModuleNodeTrait`.
//!
//! Run it (no database required — single-node in-memory mode, exits once the crawl finishes):
//!
//! ```bash
//! cargo run --example spider_quickstart
//! ```

use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

/// The typed item this spider emits.
#[derive(Debug, Serialize)]
struct Page {
    url: String,
    status: u16,
    bytes: usize,
}

struct Httpbin;

#[async_trait]
impl Spider for Httpbin {
    type Item = Page;

    fn name(&self) -> &str {
        "httpbin"
    }

    async fn start(&self, s: &mut Seeds) {
        s.get("https://httpbin.org/get");
        s.get("https://httpbin.org/uuid");
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit(Page {
            url: res.module_id(),
            status: res.status_code,
            bytes: res.text().map(|t| t.len()).unwrap_or(0),
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // No database: single-node in-memory mode, with a seed task injected for the spider
    // automatically.
    Mocra::builder()
        .spider(
            Httpbin,
            on_item(|page: Page| async move {
                println!(
                    "[item] {} -> {} ({} bytes)",
                    page.url, page.status, page.bytes
                );
            }),
        )
        .run()
        .await
}
