//! DAG scheduling and adapters for the cache and distributed sync services.

pub mod dag;
pub use dag::*;

use async_trait::async_trait;

use crate::sync::{SyncAble, SyncService};

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
