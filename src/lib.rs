//! **mocra** — a distributed, event-driven crawling and data-collection framework
//! that runs as an embeddable Rust library.
//!
//! Implement a [`Spider`](crate::facade::Spider) and run it with
//! [`Mocra::builder`](crate::facade::Mocra::builder) — **no database**
//! required on a single node. Typed output is delivered through
//! [`DataSink`](crate::facade::DataSink) / [`on_item`](crate::facade::on_item).
//!
//! ```no_run
//! use async_trait::async_trait;
//! use mocra::prelude::*;
//! use serde::Serialize;
//!
//! #[derive(Debug, Serialize)]
//! struct Page {
//!     url: String,
//!     status: u16,
//! }
//!
//! struct MySpider;
//!
//! #[async_trait]
//! impl Spider for MySpider {
//!     type Item = Page;
//!
//!     fn name(&self) -> &str {
//!         "my_spider"
//!     }
//!
//!     async fn start(&self, seeds: &mut Seeds) {
//!         seeds.get("https://httpbin.org/get");
//!     }
//!
//!     async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
//!         cx.emit(Page {
//!             url: res.module_id(),
//!             status: res.status_code,
//!         });
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     Mocra::builder()
//!         .spider(
//!             MySpider,
//!             on_item(|page: Page| async move {
//!                 println!("{} -> {}", page.url, page.status);
//!             }),
//!         )
//!         .run()
//!         .await
//! }
//! ```
//!
//! # Beyond a single node
//!
//! - **Cluster** (`cluster-embedded`): a self-organizing Raft + redb control plane via
//!   `Mocra::builder().cluster(..)` — no external ZooKeeper / etcd.
//! - **Dashboard** (`dashboard`): `.dashboard(port)` serves a built-in web dashboard
//!   plus a read-only, CORS-enabled observability API (metrics / logs / tasks / performance).
//!
//! See the [`prelude`] for the curated public surface and [`facade`] for the entry types.

// 结构性 clippy lint —— 现有设计取舍(参数数、类型复杂度、模块同名、error/枚举变体尺寸),
// 非 bug;统一豁免,便于逐步对主 crate 收紧 `-D warnings`。
#![allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::module_inception,
    clippy::result_large_err,
    clippy::large_enum_variant
)]

pub mod prelude;

// 高层 `Spider` 门面(重构 Phase 1)—— 面向 80% 场景的简单入口。
// 模块级文档见 `facade.rs` 顶部的 `//!`(此处用普通注释,避免与其内部
// `//!` 合并后在 crate 根作用域解析、导致 intra-doc 链接失效)。
pub mod facade;

#[path = "cacheable/lib.rs"]
pub mod cacheable;
#[path = "common/lib.rs"]
pub mod common;
#[path = "downloader/lib.rs"]
pub mod downloader;
#[path = "engine/lib.rs"]
pub mod engine;
#[path = "errors/lib.rs"]
pub mod errors;
#[path = "proxy/lib.rs"]
pub mod proxy;
#[path = "queue/lib.rs"]
pub mod queue;
#[path = "schedule/lib.rs"]
pub mod schedule;
#[path = "sync/lib.rs"]
pub mod sync;
#[path = "utils/lib.rs"]
pub mod utils;

#[cfg(feature = "js-v8")]
#[path = "js_v8/lib.rs"]
pub mod js_v8;

#[cfg(feature = "polars")]
pub mod polars {
    pub mod polars {
        pub use ::polars::*;
    }
    pub mod polars_lazy {
        pub use ::polars_lazy::*;
    }
    pub mod polars_ops {
        pub use ::polars_ops::*;
    }
}

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
