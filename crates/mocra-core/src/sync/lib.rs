pub mod backend;
pub mod distributed;
#[cfg(feature = "queue-kafka")]
pub mod kafka;
pub mod leader;
#[cfg(feature = "cluster-embedded")]
pub mod raft_backend;
pub mod redis;

pub use backend::{ClusterStatusView, CoordinationBackend};
pub use distributed::{DistributedSync, SyncAble, SyncService};
#[cfg(feature = "queue-kafka")]
pub use kafka::KafkaBackend;
pub use leader::LeaderElector;
#[cfg(feature = "cluster-embedded")]
pub use raft_backend::RaftCoordinationBackend;
pub use redis::RedisBackend;

#[cfg(test)]
mod tests;
