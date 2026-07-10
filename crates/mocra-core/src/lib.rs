//! `mocra-core` — shared runtime types for the [mocra](https://github.com/ouiex/mocra)
//! crawler framework.
//!
//! Currently this crate hosts the framework's errors, cache service, utilities and shared
//! domain models / runtime state ([`errors`] / [`cacheable`] / [`utils`] / [`common`]); the
//! rest of the shared runtime (the pipeline) is being migrated here incrementally so the host
//! `mocra` crate can become a thin facade over reusable, independently-compilable crates.

// 结构性 clippy lint —— 现有设计取舍(参数数、类型复杂度、模块同名、error/枚举变体尺寸),
// 非 bug;与主 crate 保持一致的统一豁免(这些模块原在主 crate,依赖同样的豁免)。
#![allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::module_inception,
    clippy::result_large_err,
    clippy::large_enum_variant
)]

pub mod errors;

#[path = "cacheable/lib.rs"]
pub mod cacheable;

#[path = "utils/lib.rs"]
pub mod utils;

#[path = "common/lib.rs"]
pub mod common;

#[path = "downloader/lib.rs"]
pub mod downloader;

#[path = "queue/lib.rs"]
pub mod queue;

#[path = "sync/lib.rs"]
pub mod sync;

#[path = "schedule/lib.rs"]
pub mod schedule;
