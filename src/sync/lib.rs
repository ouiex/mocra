pub mod backend;
pub mod distributed;
#[cfg(feature = "queue-kafka")]
pub mod kafka;
pub mod redis;
pub mod leader;

pub use backend::CoordinationBackend;
pub use distributed::{DistributedSync, SyncAble, SyncService};
#[cfg(feature = "queue-kafka")]
pub use kafka::KafkaBackend;
pub use redis::RedisBackend;
pub use leader::LeaderElector;

#[cfg(test)]
mod tests;
