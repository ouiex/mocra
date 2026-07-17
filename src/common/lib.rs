//! The shared domain models / runtime context have been extracted into the `common` module of the
//! standalone [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::common::*` references keep
//! working (zero-change migration); new code can use `mocra_core::common::...` directly.

pub use mocra_core::common::*;
