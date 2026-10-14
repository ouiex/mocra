//! Coordination backend re-export.
//!
//! The trait and status type now live in [`crate::common::coordination`] (so `common`'s
//! `State` can hold a coordination handle without a `common → sync` cycle). Backend
//! implementations (embedded Raft) stay in `sync`. This shim keeps the historical
//! `crate::sync::backend::{CoordinationBackend, ClusterStatusView}` paths working.

pub use crate::common::coordination::{ClusterStatusView, CoordinationBackend};
