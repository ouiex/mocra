//! Admin dashboard example (the `dashboard` feature).
//!
//! With the `dashboard` feature enabled, the engine serves a read-only observability HTTP API on
//! the given port **as well as** a built-in single-file frontend page — open that port in a browser
//! and you get the metrics / logs / tasks / performance panels, with no frontend build and no
//! endpoint to fill in by hand (the page is same-origin, so it points at this engine
//! automatically).
//!
//! Run it (single-node in-memory mode, no database required):
//!
//! ```bash
//! cargo run --example dashboard --features dashboard
//! # then open http://127.0.0.1:12800 in a browser
//! ```
//!
//! This spider keeps fetching a batch of URLs so the queue / log / metric panels have live data to
//! show.

use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Page {
    url: String,
    status: u16,
    bytes: usize,
}

struct Demo;

#[async_trait]
impl Spider for Demo {
    type Item = Page;

    fn name(&self) -> &str {
        "dashboard-demo"
    }

    async fn start(&self, s: &mut Seeds) {
        // Seed a batch of requests so the task / download / parse queues show live activity.
        for _ in 0..40 {
            s.get("https://httpbin.org/get");
            s.get("https://httpbin.org/uuid");
            s.get("https://httpbin.org/headers");
        }
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
    // The port can be overridden with the PORT environment variable (defaults to 12800).
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(12800);
    println!("dashboard: open http://127.0.0.1:{port} for metrics / logs / tasks / performance");
    Mocra::builder()
        .spider(
            Demo,
            on_item(|page: Page| async move {
                println!(
                    "[item] {} -> {} ({} bytes)",
                    page.url, page.status, page.bytes
                );
            }),
        )
        .dashboard(port)
        .run()
        .await
}
