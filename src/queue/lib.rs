//! The queue (data-plane MQ) subsystem has been extracted into the `queue` module of the
//! standalone [`mocra_core`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::queue::*` references keep
//! working (zero-change migration); new code can use `mocra_core::queue::...` directly.

pub use mocra_core::queue::*;
