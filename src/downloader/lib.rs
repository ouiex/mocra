//! The downloader subsystem has been extracted into the `downloader` module of the standalone
//! [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::downloader::*` references keep
//! working (zero-change migration); new code can use `mocra_core::downloader::...` directly.

pub use mocra_core::downloader::*;
