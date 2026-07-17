//! The coordination backend abstraction has moved to [`crate::utils::coordination`]
//! — its main consumers (the distributed lock manager and the rate limiter) both live in `utils`,
//! so putting it there avoids a `common ↔ utils` cycle.
//!
//! This module remains as a re-export shim, keeping existing `crate::common::coordination::*` paths
//! valid.

pub use crate::utils::coordination::*;
