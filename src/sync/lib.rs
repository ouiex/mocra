pub mod backend;
pub mod distributed;
pub mod leader;
pub mod raft;

use std::sync::Arc;

pub use backend::CoordinationBackend;
pub use distributed::{DistributedSync, SyncAble, SyncService};
pub use leader::{LeaderElector, LeadershipGate, LocalLeadershipGate};
pub use raft::{RaftLeaderView, RaftLeadershipGate, RaftRuntime, RaftRuntimeConfig};

pub fn build_leadership_gate(
	raft_runtime: Option<Arc<RaftRuntime>>,
	_redis_pool: Option<()>,
	namespace: &str,
	ttl_ms: u64,
) -> Arc<dyn LeadershipGate> {
	if let Some(runtime) = raft_runtime {
		return Arc::new(RaftLeadershipGate::new(runtime));
	}

	let _ = (namespace, ttl_ms);
	Arc::new(LocalLeadershipGate)
}

#[cfg(test)]
mod tests;
