//! The engine (the collection pipeline core) has been extracted into the `engine` module of the
//! standalone [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::engine::*` references keep
//! working (zero-change migration); new code can use `mocra_core::engine::...` directly.

pub use mocra_core::engine::*;
