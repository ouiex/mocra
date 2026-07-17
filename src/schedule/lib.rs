//! DAG scheduling (the `mocra-dag` shim + host adapter) has been extracted into the `schedule`
//! module of the standalone [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that the existing `crate::schedule::*` and
//! `crate::schedule::dag::*` paths keep working (zero-change migration); new code can use
//! `mocra_core::schedule::...` directly.

pub use mocra_core::schedule::*;

/// Compatibility shim for the old `crate::schedule::dag::*` path.
pub mod dag {
    pub use mocra_core::schedule::dag::*;
}
