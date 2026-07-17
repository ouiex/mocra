//! Cluster-coordination abstraction shared across the framework.
//!
//! [`CoordinationBackend`] is the trait the engine (leader election, distributed locks,
//! partition ownership, KV/CAS, pub-sub) talks to; implementations live in `sync`
//! (embedded Raft, etc.). It lives in `utils` — alongside its primary consumers, the
//! distributed lock manager and rate limiter — so neither `common` nor `utils` has to
//! depend on `sync`, and `common ↔ utils` stays acyclic. `common::coordination` re-exports
//! it for backward compatibility.

use tokio::sync::mpsc;

/// Cluster status snapshot (serializable, for admin / monitoring APIs to consume).
///
/// Provided by [`CoordinationBackend::cluster_status`]; only meaningful for the embedded
/// Raft backend, single-node mode returns `None`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterStatusView {
    /// This node's id.
    pub node_id: u64,
    /// Whether this node is currently the leader.
    pub is_leader: bool,
    /// The currently known leader (`None` while an election is in progress).
    pub current_leader: Option<u64>,
    /// The current Raft term.
    pub term: u64,
    /// Highest log index applied to the state machine.
    pub last_applied_index: Option<u64>,
    /// Total member count (voters + learners).
    pub member_count: usize,
    /// Number of core voting members.
    pub voter_count: usize,
}

#[async_trait::async_trait]
pub trait CoordinationBackend: Send + Sync {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String>;
    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String>;
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    // Optimistic Lock (CAS)
    async fn cas(&self, key: &str, old_val: Option<&[u8]>, new_val: &[u8]) -> Result<bool, String>;
    // Distributed Lock
    async fn acquire_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String>;
    async fn renew_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String>;
    /// Release the lock: delete it only if it is still held by `value` (CAS-del).
    ///
    /// The default implementation relies on the TTL expiring naturally (returns `Ok(false)`,
    /// meaning it was not actively released); backends with native release semantics (such as
    /// embedded Raft) should override this to support early release.
    async fn release_lock(&self, key: &str, value: &[u8]) -> Result<bool, String> {
        let _ = (key, value);
        Ok(false)
    }

    /// Current cluster member count (used for **approximate** distributed semantics such as
    /// dividing a rate limit across members).
    ///
    /// Defaults to 1 (single-node / backends that cannot observe membership). Membership-aware
    /// backends (embedded Raft) override this with the real member count.
    fn cluster_size(&self) -> usize {
        1
    }

    /// Whether the given partition key (account / session key) is handled by **this node**.
    ///
    /// Used to divide collection work across cluster nodes by partition: each account is
    /// handled only by its owning node, which avoids duplicate crawling and keeps session
    /// affinity. Defaults to `true` (single-node / backends without partitioning, everything
    /// is local); membership-aware backends (embedded Raft) override this with rendezvous
    /// ownership.
    fn owns_partition_key(&self, key: &str) -> bool {
        let _ = key;
        true
    }

    /// Whether this node is currently the cluster leader.
    ///
    /// Used for "exactly one node in the cluster does this once" actions (such as injecting
    /// seed tasks, where injecting from every node would cause N× duplicate crawling).
    /// Defaults to `true` (single-node / backends with no notion of a leader); embedded Raft
    /// overrides it with "this node == the current Raft leader".
    fn is_leader(&self) -> bool {
        true
    }

    /// Cluster status snapshot (node / leader / term / member count). Only cluster-aware
    /// backends return `Some` (embedded Raft); single-node mode returns `None`. Consumed by
    /// the dashboard / monitoring APIs.
    fn cluster_status(&self) -> Option<ClusterStatusView> {
        None
    }

    /// Gracefully shut down this backend (called when the engine stops): release the resources
    /// it holds (such as the embedded Raft redb handle and its background tasks). No-op by
    /// default (stateless backends need no special handling).
    async fn shutdown(&self) {}
}
