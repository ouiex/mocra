//! The proxy subsystem has been extracted into the standalone [`mocra_proxy`] crate.
//!
//! This module remains as a re-export shim so that existing `crate::proxy::*` references keep
//! working (zero-change migration); new code can use `mocra_proxy::...` directly. `ProxyError`
//! also lives in [`mocra_proxy`]; the main crate's `Error` absorbs it at the boundary via
//! `From<mocra_proxy::ProxyError>`.

pub use mocra_proxy::*;
