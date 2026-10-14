//! Abstract traits for the DAG scheduler's runtime dependencies.

use async_trait::async_trait;

use crate::schedule::dag::types::DagNodeSyncState;

/// Distributed KV / atomic-operation backend (the host implements it with the embedded cluster
/// KV: `get`/`del`/`incr`).
///
/// Semantics: `get` reads bytes, `set` writes (with an optional TTL), `del` removes a key, and
/// `incr` atomically increments. Errors are uniformly `String` (the dag side is mostly
/// best-effort, either ignoring them or degrading gracefully).
#[async_trait]
pub trait DagStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    async fn set(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<std::time::Duration>,
    ) -> Result<(), String>;
    async fn del(&self, key: &str) -> Result<(), String>;
    async fn incr(&self, key: &str, delta: i64) -> Result<i64, String>;
}

/// Outlet for DAG node state events (the host implements it with distributed pub/sub). Optional:
/// if not injected, no events are emitted.
#[async_trait]
pub trait DagEventSink: Send + Sync {
    async fn emit(&self, event: &DagNodeSyncState) -> Result<(), String>;
}
