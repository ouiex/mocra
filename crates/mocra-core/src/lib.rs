//! `mocra-core` — shared runtime types for the [mocra](https://github.com/ouiex/mocra)
//! crawler framework.
//!
//! Currently this crate hosts the framework's error types ([`errors`]); the rest of the
//! shared runtime (models, cache, pipeline) is being migrated here incrementally so the
//! host `mocra` crate can become a thin facade over reusable, independently-compilable crates.

pub mod errors;

#[path = "cacheable/lib.rs"]
pub mod cacheable;
