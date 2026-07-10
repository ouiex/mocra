//! 最小 Spider 示例(重构 Phase 1 门面 API)。
//!
//! 演示新的高层入口:实现一个 `Spider`,用 `Mocra::builder()` 注册并运行,
//! 通过 `on_item` 拿到类型化数据 —— 无需实现 `DataStoreMiddleware`、无需手写
//! `ModuleTrait` + `ModuleNodeTrait`。
//!
//! 运行(无需数据库、无需 Redis —— 单机内存模式,抓完自动退出):
//!
//! ```bash
//! cargo run --example spider_quickstart
//! ```

use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

/// 类型化产出项。
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
    // 无数据库、无 Redis:单机内存模式,自动为 spider 注入种子任务。
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
