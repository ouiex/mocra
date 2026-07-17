//! mocra-dag: a general-purpose distributed DAG execution engine (extracted from the main mocra
//! crate, with **zero crawler coupling**).
//!
//! - [`Dag`] / [`DagChainBuilder`]: graph construction (nodes + dependency edges, topological
//!   validation, cycle detection).
//! - [`DagScheduler`]: layered concurrent execution, retry policies, and a distributed run guard
//!   protected by fencing.
//! - Node dispatch is injected through the [`DagNodeDispatcher`] abstraction (with a built-in
//!   [`LocalNodeDispatcher`]; the host can supply its own).
//! - Runtime dependencies are injected through traits ([`DagStore`] / [`DagEventSink`] /
//!   [`DagFencingStore`]); the host implements them with the embedded cluster KV / distributed
//!   pub-sub, so this crate does not depend back on the host.

// Existing style carried over from the main crate: the emit_sync_state telemetry function takes
// many arguments (one per state field), and graph has an empty-list guard — the original logic is
// kept (covered by 54 tests), so these two pedantic lints are waived.
#![allow(clippy::too_many_arguments, clippy::redundant_guards)]

mod graph;
mod metrics;
mod scheduler;
pub mod store;
pub mod types;

pub use graph::{Dag, DagChainBuilder, DagNodePtr};
pub use scheduler::{DagExecutionReport, DagScheduler, DagSchedulerOptions, NodeExecutionResult};
pub use store::{DagEventSink, DagStore};
pub use types::{
    DagError, DagErrorClass, DagErrorCode, DagFencingStore, DagNodeDispatcher,
    DagNodeExecutionPolicy, DagNodeRecord, DagNodeRetryMode, DagNodeStatus, DagNodeSyncState,
    DagNodeTrait, DagRunGuard, DagRunGuardAcquireOutcome, DagRunResumeState, DagRunStateStore,
    LocalNodeDispatcher, NodeExecutionContext, NodePlacement, TaskPayload,
};
