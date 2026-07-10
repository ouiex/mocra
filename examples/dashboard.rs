//! 后台管理 dashboard 示例(`dashboard` 特性)。
//!
//! 启用 `dashboard` 特性后,引擎在指定端口托管一套只读可观测 HTTP API **以及**一个
//! 内置的单文件前端页面 —— 浏览器打开该端口即见 指标 / 日志 / 任务 / 性能 面板,
//! 无需任何前端构建、无需手填 endpoint(页面同源自动指向本引擎)。
//!
//! 运行(单机内存模式,无需数据库 / Redis):
//!
//! ```bash
//! cargo run --example dashboard --features dashboard
//! # 然后浏览器打开 http://127.0.0.1:8080
//! ```
//!
//! 该 spider 会持续抓取一批 URL,使队列 / 日志 / 指标面板有实时数据可看。

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
        // 播入一批种子,让任务 / 下载 / 解析队列有可观测的实时活动。
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
    // 端口可用 PORT 环境变量覆盖(默认 8080)。
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    println!("dashboard: 打开 http://127.0.0.1:{port} 查看 指标 / 日志 / 任务 / 性能");
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
