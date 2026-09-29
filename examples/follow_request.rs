//! Offline follow-up example: a GET response schedules a POST with its own headers, body,
//! cookie, and timeout. The mock downloader echoes what it actually receives, so the final
//! printed item shows that those fields survived the parser task queue.
//!
//! Run: `cargo run --example follow_request`
//! The single-node engine exits after its configured idle period (30 seconds by default).

use async_trait::async_trait;
use mocra::prelude::downloader::{DownloadConfig, Downloader};
use mocra::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Echo {
    url: String,
    method: String,
    header: Option<String>,
    body: Option<String>,
    cookie: Option<String>,
    timeout: u64,
}

#[derive(Clone)]
struct EchoDownloader;

#[async_trait]
impl Downloader for EchoDownloader {
    fn name(&self) -> String {
        "echo".into()
    }

    fn version(&self) -> semver::Version {
        semver::Version::new(1, 0, 0)
    }

    async fn set_config(&self, _id: &str, _config: DownloadConfig) {}

    async fn set_limit(&self, _id: &str, _limit: f32) {}

    async fn health_check(&self) -> Result<()> {
        Ok(())
    }

    async fn download(&self, request: Request) -> Result<Response> {
        let echo = Echo {
            url: request.url.clone(),
            method: request.method.clone(),
            header: request.headers.get("x-demo"),
            body: request
                .body
                .as_ref()
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned()),
            cookie: request
                .cookies
                .cookies
                .first()
                .map(|item| item.value.clone()),
            timeout: request.timeout,
        };
        let content = serde_json::to_vec(&echo).expect("Echo contains only serializable fields");
        Ok(Response {
            id: request.id,
            platform: request.platform,
            account: request.account,
            module: request.module,
            status_code: 200,
            cookies: Cookies::default(),
            content,
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
        })
    }
}

struct FollowSpider;

#[async_trait]
impl Spider for FollowSpider {
    type Item = Echo;

    fn name(&self) -> &str {
        "follow-request-example"
    }

    async fn start(&self, seeds: &mut Seeds) {
        // This host is never contacted: EchoDownloader supplies both responses locally.
        seeds.get("https://example.invalid/seed");
    }

    async fn parse(&self, response: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        let echo: Echo = response.json()?;
        if echo.url.ends_with("/seed") {
            let mut next = Request::new("https://example.invalid/next", RequestMethod::Post)
                .with_body(b"hello from follow".to_vec());
            next.headers = Headers::new().add("x-demo", "kept");
            next.cookies
                .add("session", "cookie-kept", "example.invalid");
            next.timeout = 17;
            cx.follow(next);
        } else {
            cx.emit(echo);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(
            FollowSpider,
            on_item(|echo: Echo| async move {
                println!("follow-up downloader received: {echo:?}");
            }),
        )
        .default_downloader(EchoDownloader)
        .run()
        .await
}
