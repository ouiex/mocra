//! `mocra-core` — the [mocra](https://github.com/ouiex/mocra) crawler framework's runtime.
//!
//! This crate holds the **entire runtime**: errors, cache service, utilities, shared domain
//! models and runtime state, the downloader, the data-plane queue, coordination / sync, the
//! scheduler, and the crawling engine + observability API ([`errors`] / [`cacheable`] /
//! [`utils`] / [`common`] / [`downloader`] / [`queue`] / [`sync`] / [`schedule`] / [`engine`]).
//! The host `mocra` crate is a thin facade over it.

// Structural clippy lints — deliberate design trade-offs (argument count, type complexity,
// module inception, error / enum variant size), not bugs. Exempted uniformly to stay consistent
// with the host crate (these modules used to live there and relied on the same exemptions).
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

#[path = "engine/lib.rs"]
pub mod engine;
