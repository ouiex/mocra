pub mod backend;
pub mod distributed;
pub mod kafka;
pub mod leader;
pub mod raft;
pub mod redis;

use std::sync::Arc;

use deadpool_redis::Pool;

pub use backend::CoordinationBackend;
pub use distributed::{DistributedSync, SyncAble, SyncService};
pub use kafka::KafkaBackend;
pub use leader::{LeaderElector, LeadershipGate, LocalLeadershipGate};
pub use raft::{RaftLeaderView, RaftLeadershipGate, RaftRuntime, RaftRuntimeConfig};
pub use redis::RedisBackend;

pub fn build_leadership_gate(
	raft_runtime: Option<Arc<RaftRuntime>>,
	redis_pool: Option<Pool>,
	namespace: &str,
	ttl_ms: u64,
) -> Arc<dyn LeadershipGate> {
	if let Some(runtime) = raft_runtime {
		return Arc::new(RaftLeadershipGate::new(runtime));
	}

	if let Some(pool) = redis_pool {
		let backend = Arc::new(RedisBackend::new(pool));
		let (elector, _) = LeaderElector::new(Some(backend), format!("{}:leader:cron", namespace), ttl_ms);
		return elector;
	}

	Arc::new(LocalLeadershipGate)
}

#[cfg(test)]
mod tests;
