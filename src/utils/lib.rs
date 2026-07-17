//! The utility library has been extracted into the `utils` module of the standalone
//! [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::utils::*` references keep
//! working (zero-change migration); new code can use `mocra_core::utils::...` directly.

pub use mocra_core::utils::*;
