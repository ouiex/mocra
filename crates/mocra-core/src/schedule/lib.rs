//! The DAG engine has been extracted into its own crate, [`mocra_dag`].
//!
//! This module remains as a re-export shim (top level + the `dag` submodule, keeping the existing
//! `crate::schedule::*` and `crate::schedule::dag::*` paths working), and provides the host
//! adapters here: adapting the main crate's
//! [`CacheService`](crate::cacheable::CacheService) / [`SyncService`] to dag's
//! [`DagStore`] / [`DagEventSink`].

pub use mocra_dag::*;

/// Compatibility for the old `crate::schedule::dag::*` path.
pub mod dag {
    pub use mocra_dag::*;
}

use async_trait::async_trait;

use crate::sync::{SyncAble, SyncService};

// `impl DagStore for CacheService` moved into `mocra-core` along with `CacheService` (orphan rule:
// both the trait and the type live in external crates, so the impl must sit in a crate that owns
// one of them).

// SyncAble is a local trait and DagNodeSyncState is a foreign type — the orphan rule permits this
// impl here.
impl SyncAble for DagNodeSyncState {
    fn topic() -> String {
        "dag_node_sync_state".to_string()
    }
}

/// Adapts the host's distributed sync service into dag's node status event sink.
pub struct DagSyncSink(pub SyncService);

#[async_trait]
impl DagEventSink for DagSyncSink {
    async fn emit(&self, event: &DagNodeSyncState) -> Result<(), String> {
        self.0.send(event).await
    }
}

#[cfg(test)]
mod dag_tests;
