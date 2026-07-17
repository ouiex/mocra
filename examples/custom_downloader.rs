//! Custom downloader example (the facade's `.default_downloader()` / `.downloader()`).
//!
//! Demonstrates how to implement the [`Downloader`] trait and swap the default reqwest for your own
//! download logic — browser rendering / proxy rotation / cache reads / a different HTTP client /
//! a mock, and so on.
//!
//! This example uses an **offline mock downloader**: it makes no network requests at all, and
//! synthesizes a JSON response from the URL. That keeps the example deterministic and offline, and
//! it plainly demonstrates that the custom downloader really is called by the pipeline — the body
//! the spider parses carries `"downloader": "mock"` rather than the real site's content.
//!
//! Run it:
//! ```bash
//! cargo run --example custom_downloader
//! ```

use async_trait::async_trait;
use mocra::prelude::downloader::{DownloadConfig, Downloader};
use mocra::prelude::*;
use serde::{Deserialize, Serialize};

/// The item this spider emits.
#[derive(Debug, Serialize)]
struct Item {
    url: String,
    title: String,
    served_by: String,
}

/// Shape of the mock response body: the downloader synthesizes it, the spider parses it back.
#[derive(Serialize, Deserialize)]
struct MockBody {
    url: String,
    title: String,
    downloader: String,
}

/// An offline mock downloader: ignores the network and synthesizes a JSON response from the
/// request URL.
///
/// In a real implementation, `download` can obtain its bytes any way you like — driving a headless
/// browser, going through a different HTTP client, reading from disk/cache, or layering on custom
/// retry/proxy logic — as long as it ends up returning a [`Response`]. `Downloader` requires
/// `Clone` (it is cloned internally as a `Box<dyn Downloader>`).
#[derive(Clone)]
struct MockDownloader;

impl MockDownloader {
    /// Build a response from a request: carry the **correlation fields** (id / run_id / module /
    /// context, etc.) over from the request verbatim, and fill in only `status_code` and
    /// `content`. Those correlation fields are what lets the pipeline match a response back to its
    /// task, so they must not be zeroed out with `Default` — a real downloader should likewise
    /// inherit them from the corresponding request.
    fn respond(request: Request, status: u16, body: Vec<u8>) -> Response {
        Response {
            id: request.id,
            platform: request.platform,
            account: request.account,
            module: request.module,
            status_code: status,
            cookies: Cookies { cookies: vec![] },
            content: body,
            storage_path: None,
            headers: vec![("content-type".into(), "application/json".into())],
            task_retry_times: request.task_retry_times,
            metadata: request.meta,
            download_middleware: request.download_middleware,
            data_middleware: request.data_middleware,
            task_finished: request.task_finished,
            context: request.context,
            run_id: request.run_id,
            prefix_request: request.prefix_request,
            request_hash: None,
            priority: request.priority,
        }
    }
}

#[async_trait]
impl Downloader for MockDownloader {
    fn name(&self) -> String {
        "mock".into()
    }

    fn version(&self) -> semver::Version {
        semver::Version::new(1, 0, 0)
    }

    async fn download(&self, request: Request) -> Result<Response> {
        // A real downloader would perform the actual fetch here; this example synthesizes a JSON
        // response body from the URL.
        let body = serde_json::to_vec(&MockBody {
            url: request.url.clone(),
            title: format!("synthetic page for {}", request.url),
            downloader: "mock".into(),
        })
        .unwrap_or_default();
        Ok(Self::respond(request, 200, body))
    }

    async fn set_config(&self, _id: &str, _config: DownloadConfig) {}
    async fn set_limit(&self, _id: &str, _limit: f32) {}
    async fn health_check(&self) -> Result<()> {
        Ok(())
    }
    // `close(&self)` has a default implementation; override it only when you hold resources.
}

struct Demo;

#[async_trait]
impl Spider for Demo {
    type Item = Item;

    fn name(&self) -> &str {
        "custom-downloader-demo"
    }

    async fn start(&self, s: &mut Seeds) {
        s.get("https://example.com/a");
        s.get("https://example.com/b");
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        // Parse as usual — this response came from our mock downloader, not from reqwest.
        let body: MockBody = res.json()?;
        cx.emit(Item {
            url: body.url,
            title: body.title,
            served_by: body.downloader,
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(
            Demo,
            on_item(|item: Item| async move {
                println!(
                    "[item] served_by={} :: {} ({})",
                    item.served_by, item.title, item.url
                );
            }),
        )
        // Replace the default reqwest wholesale.
        // To select per module instead: register with `.downloader(MockDownloader)` and, in
        // `start`, set `req.downloader = "mock".into();` after `s.get(url)` — unmatched requests
        // still go through the default downloader.
        .default_downloader(MockDownloader)
        .run()
        .await
}
