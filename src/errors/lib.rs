//! The error types have been extracted into the `errors` module of the standalone [`mocra_core`]
//! crate.
//!
//! This module remains as a re-export shim so that existing `crate::errors::*` references keep
//! working (zero-change migration); new code can use `mocra_core::errors::...` directly.

pub use mocra_core::errors::*;
