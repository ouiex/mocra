//! The cache service has been extracted into the `cacheable` module of the standalone
//! [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::cacheable::*` references keep
//! working (zero-change migration); new code can use `mocra_core::cacheable::...` directly.

pub use mocra_core::cacheable::*;
