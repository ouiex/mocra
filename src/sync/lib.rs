//! The coordination / sync subsystem has been extracted into the `sync` module of the standalone
//! [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::sync::*` references keep
//! working (zero-change migration); new code can use `mocra_core::sync::...` directly.

pub use mocra_core::sync::*;
