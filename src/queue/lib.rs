pub mod channel;
pub mod compensation;
pub mod kafka;
pub mod manager;
pub mod redis;
pub mod batcher;
pub mod compression;

pub use crate::queue::channel::Channel;
use async_trait::async_trait;
pub use compensation::{Compensator, Identifiable, RedisCompensator};
use crate::errors::Result;
pub use manager::QueueManager;
pub use redis::RedisQueue;
use tokio::sync::mpsc;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
/// Action to take on a message: Acknowledge or Negative Acknowledge (Fail)
pub enum AckAction {
    /// Message processed successfully
    Ack,
    /// Message processing failed
    Nack(String, std::sync::Arc<Vec<u8>>, std::sync::Arc<HashMap<String, String>>), // reason, payload, headers
}

use std::collections::HashMap;
use futures::future::BoxFuture;

pub type AckFn = Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send + Sync>;
pub type NackFn = Box<dyn FnOnce(String) -> BoxFuture<'static, Result<()>> + Send + Sync>;

pub const HEADER_ATTEMPT: &str = "x-attempt";
pub const HEADER_CREATED_AT: &str = "x-created-at";
pub const HEADER_NACK_REASON: &str = "x-nack-reason";

#[derive(Debug, Clone, Copy)]
pub struct NackPolicy {
    pub max_retries: u32,
    pub backoff_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NackDisposition {
    Retry { next_attempt: u32 },
    Dlq,
}

impl Default for NackPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            backoff_ms: 0,
        }
    }
}

pub(crate) fn parse_attempt(headers: &HashMap<String, String>) -> u32 {
    headers
        .get(HEADER_ATTEMPT)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
}

pub fn decide_nack(policy: NackPolicy, attempt: u32) -> NackDisposition {
    if policy.max_retries > 0 && attempt < policy.max_retries {
        NackDisposition::Retry {
            next_attempt: attempt.saturating_add(1),
        }
    } else {
        NackDisposition::Dlq
    }
}

/// A wrapper for items that might require acknowledgement
pub struct QueuedItem<T> {
    pub inner: T,
    ack_fn: Option<AckFn>,
    nack_fn: Option<NackFn>,
}

impl<T> QueuedItem<T> {
    pub fn new(inner: T) -> Self {
        Self { inner, ack_fn: None, nack_fn: None }
    }
    
    pub fn with_ack<A, N>(inner: T, ack: A, nack: N) -> Self 
    where
        A: FnOnce() -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
        N: FnOnce(String) -> BoxFuture<'static, Result<()>> + Send + Sync + 'static,
    {
         Self { inner, ack_fn: Some(Box::new(ack)), nack_fn: Some(Box::new(nack)) }
    }

    pub async fn ack(mut self) -> Result<()> {
        if let Some(f) = self.ack_fn.take() {
            f().await
        } else {
            Ok(())
        }
    }
    
    pub async fn nack(mut self, reason: String) -> Result<()> {
        if let Some(f) = self.nack_fn.take() {
            f(reason).await
        } else {
            Ok(())
        }
    }

    pub fn into_parts(self) -> (T, Option<AckFn>, Option<NackFn>) {
        (self.inner, self.ack_fn, self.nack_fn)
    }
}

impl<T> std::ops::Deref for QueuedItem<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for QueuedItem<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueuedItem")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T: Identifiable> Identifiable for QueuedItem<T> {
    fn get_id(&self) -> String {
        self.inner.get_id()
    }
}

/// Represents a message received from the queue.
/// The user MUST call `ack()` to acknowledge successful processing.
#[derive(Clone)]
pub struct Message {
    pub payload: std::sync::Arc<Vec<u8>>,
    pub id: String,
    pub headers: std::sync::Arc<HashMap<String, String>>,
    pub ack_tx: mpsc::Sender<(String, AckAction)>,
}

impl std::fmt::Debug for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("payload", &self.payload)
            .field("headers", &self.headers)
            .finish()
    }
}

impl Message {
    pub async fn ack(&self) -> Result<()> {
        // Send the ID back to the backend to acknowledge the message
        self.ack_tx.send((self.id.clone(), AckAction::Ack)).await.map_err(|_| {
            crate::errors::error::QueueError::OperationFailed(Box::new(std::io::Error::other(
                "Failed to send ACK signal",
            )))
            .into()
        })
    }

    pub async fn nack(&self, reason: impl Into<String>) -> Result<()> {
        let headers = self.headers.clone();
        self.ack_tx
            .send((self.id.clone(), AckAction::Nack(reason.into(), self.payload.clone(), headers)))
            .await
            .map_err(|_| {
            crate::errors::error::QueueError::OperationFailed(Box::new(std::io::Error::other(
                "Failed to send NACK signal",
            )))
            .into()
        })
    }
}

#[async_trait]
pub trait MqBackend: Send + Sync {
    async fn publish(&self, topic: &str, key: Option<&str>, payload: &[u8]) -> Result<()> {
        self.publish_with_headers(topic, key, payload, &HashMap::new()).await
    }
    
    async fn publish_with_headers(&self, topic: &str, key: Option<&str>, payload: &[u8], headers: &HashMap<String, String>) -> Result<()>;

    async fn publish_batch(&self, topic: &str, items: &[(Option<String>, Vec<u8>)]) -> Result<()> {
        for (key, payload) in items {
            self.publish(topic, key.as_deref(), payload).await?;
        }
        Ok(())
    }
    
    async fn publish_batch_with_headers(&self, topic: &str, items: &[(Option<String>, Vec<u8>, HashMap<String, String>)]) -> Result<()> {
        for (key, payload, headers) in items {
            self.publish_with_headers(topic, key.as_deref(), payload, headers).await?;
        }
        Ok(())
    }

    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> Result<()>;
    async fn clean_storage(&self) -> Result<()>;
    /// Send a message to the Dead Letter Queue (DLQ).
    async fn send_to_dlq(&self, topic: &str, id: &str, payload: &[u8], reason: &str) -> Result<()>;

    /// Read messages from the Dead Letter Queue (DLQ).
    /// Returns a list of (id, payload, reason, original_id).
    async fn read_dlq(&self, topic: &str, count: usize) -> Result<Vec<(String, Vec<u8>, String, String)>>;
}
