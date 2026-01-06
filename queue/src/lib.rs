pub mod channel;
pub mod compensation;
pub mod kafka;
pub mod manager;
pub mod redis;

pub use crate::channel::Channel;
use async_trait::async_trait;
pub use compensation::{Compensator, Identifiable, RedisCompensator};
use errors::Result;
pub use manager::QueueManager;
pub use redis::RedisQueue;
use tokio::sync::mpsc;

/// Represents a message received from the queue.
/// The user MUST call `ack()` to acknowledge successful processing.
#[derive(Clone)]
pub struct Message {
    pub payload: Vec<u8>,
    ack_tx: mpsc::Sender<()>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("payload", &self.payload)
            .finish()
    }
}

impl Message {
    pub async fn ack(&self) -> Result<()> {
        // Send a signal back to the backend to acknowledge the message
        self.ack_tx.send(()).await.map_err(|_| {
            errors::error::QueueError::OperationFailed(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to send ACK signal",
            )))
            .into()
        })
    }
}

#[async_trait]
pub trait MqBackend: Send + Sync {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()>;
    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> Result<()>;
    async fn clean_storage(&self) -> Result<()>;
}
