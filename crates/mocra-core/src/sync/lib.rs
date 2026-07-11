pub mod backend;
pub mod distributed;
pub mod leader;
#[cfg(feature = "cluster-embedded")]
pub mod raft_backend;

pub use backend::{ClusterStatusView, CoordinationBackend};
pub use distributed::{DistributedSync, SyncAble, SyncService};
pub use leader::LeaderElector;
#[cfg(feature = "cluster-embedded")]
pub use raft_backend::RaftCoordinationBackend;

#[cfg(test)]
mod tests;
