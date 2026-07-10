//! 自定义下载器示例(门面 `.default_downloader()` / `.downloader()`)。
//!
//! 演示如何实现 [`Downloader`] trait,把默认的 reqwest 换成你自己的下载逻辑 ——
//! 浏览器渲染 / 代理轮换 / 读缓存 / 另一个 HTTP 客户端 / mock 等。
//!
//! 本例用一个**离线 mock 下载器**:不发任何网络请求,按 URL 合成一段 JSON 响应。
//! 这样示例可确定性运行、无需联网,并能清楚证明「自定义下载器真的被管线调用了」——
//! spider 解析到的响应体带着 `"downloader": "mock"`,而不是真实站点的内容。
//!
//! 运行:
//! ```bash
//! cargo run --example custom_downloader
//! ```

use async_trait::async_trait;
use mocra::prelude::downloader::{DownloadConfig, Downloader};
use mocra::prelude::*;
use serde::{Deserialize, Serialize};

/// 采集产出项。
#[derive(Debug, Serialize)]
struct Item {
    url: String,
    title: String,
    served_by: String,
}

/// mock 响应体的结构:下载器合成它,spider 再解析它。
#[derive(Serialize, Deserialize)]
struct MockBody {
    url: String,
    title: String,
    downloader: String,
}

/// 一个离线 mock 下载器:忽略网络,按请求 URL 合成 JSON 响应。
///
/// 换成真实实现时,`download` 里可以是任意「拿到字节」的方式 —— 调 headless 浏览器渲染、
/// 走另一个 HTTP 客户端、从磁盘/缓存读取、或叠加自定义重试/代理逻辑 —— 只要最终返回一个
/// [`Response`]。`Downloader` 要求可 `Clone`(内部按 `Box<dyn Downloader>` 克隆)。
#[derive(Clone)]
struct MockDownloader;

impl MockDownloader {
    /// 由请求构造响应:把**关联字段**(id / run_id / module / context 等)从 request 原样
    /// 带过来,只填 `status_code` 与 `content`。这些关联字段是管线把响应对回其任务所必需的,
    /// 因此不能用 `Default` 置零 —— 真实下载器同样应从对应请求继承它们。
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
        // 真实下载器在这里发起真正的抓取;本例按 URL 合成一段 JSON 响应体。
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
    // `close(&self)` 有默认实现,持有资源时才需要覆盖。
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
        // 正常解析 —— 这条响应来自我们的 mock 下载器,而非 reqwest。
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
                println!("[item] served_by={} :: {} ({})", item.served_by, item.title, item.url);
            }),
        )
        // 整体替换默认 reqwest。
        // 若只想按模块选择:改用 `.downloader(MockDownloader)` 注册,并在 `start` 里
        // `s.get(url)` 之后设 `req.downloader = "mock".into();` —— 未匹配的请求仍走默认下载器。
        .default_downloader(MockDownloader)
        .run()
        .await
}
