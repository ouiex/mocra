use crate::common::model::config::RedisConfig;
use crate::common::model::{
    Request, Response,
    message::TaskEvent,
};
use crate::errors::Result;
use crate::errors::error::QueueError;
use crate::utils::logger::LogModel;
use async_trait::async_trait;
use dashmap::DashMap;
use deadpool_redis::redis;
use log::{error, warn};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;

/// Trait for objects that can be uniquely identified for compensation purposes.
pub trait Identifiable {
    fn get_id(&self) -> String;
}

impl Identifiable for LogModel {
    fn get_id(&self) -> String {
        self.request_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| self.task_id.clone())
    }
}

impl Identifiable for Request {
    fn get_id(&self) -> String {
        self.id.to_string()
    }
}

impl Identifiable for Response {
    fn get_id(&self) -> String {
        self.id.to_string()
    }
}

impl Identifiable for TaskEvent {
    fn get_id(&self) -> String {
        self.run_id.to_string()
    }
}

#[async_trait]
pub trait Compensator: Send + Sync {
    /// Add a task to the compensation queue (Redis ZSet + Hash).
    async fn add_task(&self, topic: &str, id: &str, payload: Arc<Vec<u8>>) -> Result<()>;
    /// Remove a task from the compensation queue.
    async fn remove_task(&self, topic: &str, id: &str) -> Result<()>;
    /// Scan for incomplete (pending) compensation records. Used for crash recovery.
    async fn scan_incomplete(&self) -> Result<Vec<CompensationRecord>> {
        Ok(Vec::new())
    }
}

#[derive(Debug, Clone)]
pub struct CompensationRecord {
    pub topic: String,
    pub id: String,
    pub payload: Arc<Vec<u8>>,
    pub created_at_secs: u64,
}

#[derive(Debug, Default)]
pub struct QueueNativeCompensator {
    namespace: String,
    pending: Arc<DashMap<String, CompensationRecord>>,
}

impl QueueNativeCompensator {
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            pending: Arc::new(DashMap::new()),
        }
    }

    fn record_key(&self, topic: &str, id: &str) -> String {
        format!("{}:{topic}:{id}", self.namespace)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn contains(&self, topic: &str, id: &str) -> bool {
        self.pending.contains_key(&self.record_key(topic, id))
    }

    /// Returns all pending records and drains them from the in-memory store.
    /// Used for crash recovery: replay these records through the normal processing pipeline.
    pub fn drain_pending(&self) -> Vec<CompensationRecord> {
        let records: Vec<CompensationRecord> = self.pending.iter().map(|entry| entry.value().clone()).collect();
        self.pending.clear();
        records
    }
}

#[async_trait]
impl Compensator for QueueNativeCompensator {
    async fn add_task(&self, topic: &str, id: &str, payload: Arc<Vec<u8>>) -> Result<()> {
        // Idempotent: overwrites existing record with same key.
        let created_at_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.pending.insert(
            self.record_key(topic, id),
            CompensationRecord {
                topic: topic.to_string(),
                id: id.to_string(),
                payload,
                created_at_secs,
            },
        );
        Ok(())
    }

    async fn remove_task(&self, topic: &str, id: &str) -> Result<()> {
        self.pending.remove(&self.record_key(topic, id));
        Ok(())
    }

    async fn scan_incomplete(&self) -> Result<Vec<CompensationRecord>> {
        let records: Vec<CompensationRecord> = self
            .pending
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        Ok(records)
    }
}

enum CompensationMessage {
    Add {
        topic: String,
        id: String,
        payload: Arc<Vec<u8>>,
    },
    Remove {
        topic: String,
        id: String,
    },
}

pub struct RedisCompensator {
    sender: mpsc::Sender<CompensationMessage>,
}

impl RedisCompensator {
    pub fn new(redis_config: &RedisConfig, namespace: &str) -> Result<Self> {
        let server = format!("{}:{}", redis_config.redis_host, redis_config.redis_port);
        let db = redis_config.redis_db;
        let redis_url = match (&redis_config.redis_username, &redis_config.redis_password) {
            (Some(user), Some(pass)) => {
                format!("redis://{user}:{pass}@{server}/{db}?protocol=resp3")
            }
            (Some(user), None) => format!("redis://{user}@{server}/{db}?protocol=resp3"),
            (None, Some(pass)) => format!("redis://:{pass}@{server}/{db}?protocol=resp3"),
            (None, None) => format!("redis://{server}/{db}?protocol=resp3"),
        };

        let client = redis::Client::open(redis_url).map_err(|_| QueueError::ConnectionFailed)?;
        let (tx, mut rx) = mpsc::channel(10000);
        let namespace = namespace.to_string();

        tokio::spawn(async move {
            // Initial connection attempt with backoff
            let mut conn = loop {
                match client.get_multiplexed_async_connection().await {
                    Ok(c) => break Some(c),
                    Err(e) => {
                        error!(
                            "Failed to connect to Redis in compensator: {}. Retrying in 1s...",
                            e
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            };

            let mut batch: Vec<CompensationMessage> = Vec::with_capacity(100);
            const MAX_BATCH_BACKLOG: usize = 10_000;

            while let Some(msg) = rx.recv().await {
                batch.push(msg);
                // Try to drain more if available, up to limit
                while batch.len() < 100 {
                    match rx.try_recv() {
                        Ok(m) => batch.push(m),
                        Err(_) => break,
                    }
                }

                // Cap batch size to prevent unbounded growth during prolonged outages.
                if batch.len() > MAX_BATCH_BACKLOG {
                    let dropped = batch.len() - MAX_BATCH_BACKLOG;
                    batch.drain(..dropped);
                    warn!(
                        "Compensator batch exceeded {} limit, dropped {} oldest messages",
                        MAX_BATCH_BACKLOG, dropped
                    );
                }

                // Reconnect if connection is lost
                if conn.is_none() {
                    let mut reconnect_attempts = 0u32;
                    loop {
                        match client.get_multiplexed_async_connection().await {
                            Ok(c) => {
                                conn = Some(c);
                                break;
                            }
                            Err(e) => {
                                reconnect_attempts += 1;
                                if reconnect_attempts >= 5 {
                                    error!(
                                        "Compensator failed to reconnect after {} attempts: {}. \
                                         Keeping {} messages for next cycle.",
                                        reconnect_attempts,
                                        e,
                                        batch.len()
                                    );
                                    break;
                                }
                                let backoff = Duration::from_millis(
                                    500 * (1u64 << reconnect_attempts.min(4)),
                                );
                                error!(
                                    "Failed to reconnect to Redis in compensator (attempt {}): {}. Retrying in {:?}...",
                                    reconnect_attempts, e, backoff
                                );
                                tokio::time::sleep(backoff).await;
                            }
                        }
                    }
                    if conn.is_none() {
                        // Keep batch for next recv cycle instead of dropping
                        continue;
                    }
                }

                let active_conn = match conn.as_mut() {
                    Some(c) => c,
                    None => {
                        // Keep batch for next cycle instead of dropping
                        continue;
                    }
                };

                let mut pipe = redis::pipe();

                for msg in batch.drain(..) {
                    match msg {
                        CompensationMessage::Add { topic, id, payload } => {
                            let tag = format!("{{{}:compensation:{}}}", namespace, topic);
                            let zset_key = format!("{}:zset", tag);
                            let hash_key = format!("{}:data", tag);
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();

                            pipe.zadd(&zset_key, &id, now)
                                .hset(&hash_key, &id, payload.as_slice());
                        }
                        CompensationMessage::Remove { topic, id } => {
                            let tag = format!("{{{}:compensation:{}}}", namespace, topic);
                            let zset_key = format!("{}:zset", tag);
                            let hash_key = format!("{}:data", tag);

                            pipe.zrem(&zset_key, &id).hdel(&hash_key, &id);
                        }
                    }
                }

                if let Err(e) = pipe.query_async::<()>(active_conn).await {
                    error!(
                        "Failed to execute batch compensation task in Redis: {}. Resetting connection.",
                        e
                    );
                    conn = None; // Force reconnect
                }
            }
        });

        Ok(Self { sender: tx })
    }
}

#[async_trait]
impl Compensator for RedisCompensator {
    async fn add_task(&self, topic: &str, id: &str, payload: Arc<Vec<u8>>) -> Result<()> {
        self.sender
            .send(CompensationMessage::Add {
                topic: topic.to_string(),
                id: id.to_string(),
                payload,
            })
            .await
            .map_err(|e| QueueError::PushFailed(Box::new(e)))?;
        Ok(())
    }

    async fn remove_task(&self, topic: &str, id: &str) -> Result<()> {
        self.sender
            .send(CompensationMessage::Remove {
                topic: topic.to_string(),
                id: id.to_string(),
            })
            .await
            .map_err(|e| QueueError::PushFailed(Box::new(e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queue_native_compensator_tracks_add_and_remove() {
        let compensator = QueueNativeCompensator::new("ns-a");
        let payload = Arc::new(vec![1_u8, 2, 3]);

        compensator
            .add_task("task", "run-1", payload.clone())
            .await
            .expect("add should succeed");

        assert_eq!(compensator.pending_count(), 1);
        assert!(compensator.contains("task", "run-1"));

        compensator
            .remove_task("task", "run-1")
            .await
            .expect("remove should succeed");

        assert_eq!(compensator.pending_count(), 0);
        assert!(!compensator.contains("task", "run-1"));
    }

    #[tokio::test]
    async fn crash_before_done_replays_on_startup() {
        let compensator = QueueNativeCompensator::new("ns-a");
        let payload = Arc::new(vec![1_u8, 2, 3]);

        compensator
            .add_task("task", "run-1", payload.clone())
            .await
            .expect("add should succeed");

        // Simulate crash: drain pending records before dropping old compensator
        let pending = compensator.drain_pending();
        assert_eq!(pending.len(), 1);
        assert_eq!(compensator.pending_count(), 0);
        assert_eq!(pending[0].topic, "task");
        assert_eq!(pending[0].id, "run-1");
        assert_eq!(pending[0].payload, payload);

        // Simulate restart: create new compensator and re-add drained records
        let new_compensator = QueueNativeCompensator::new("ns-a");
        for record in &pending {
            new_compensator
                .add_task(&record.topic, &record.id, record.payload.clone())
                .await
                .expect("re-add should succeed");
        }

        assert_eq!(new_compensator.pending_count(), 1);
        assert!(new_compensator.contains("task", "run-1"));

        // Complete processing
        new_compensator
            .remove_task("task", "run-1")
            .await
            .expect("remove should succeed");
        assert_eq!(new_compensator.pending_count(), 0);
    }

    #[tokio::test]
    async fn done_before_ack_is_idempotent() {
        let compensator = QueueNativeCompensator::new("ns-a");
        let payload = Arc::new(vec![1_u8, 2, 3]);

        compensator
            .add_task("task", "run-1", payload.clone())
            .await
            .expect("add should succeed");
        assert_eq!(compensator.pending_count(), 1);

        // Simulate done-before-ack: remove then scan shows nothing to replay
        compensator
            .remove_task("task", "run-1")
            .await
            .expect("remove should succeed");
        assert_eq!(compensator.pending_count(), 0);

        let incomplete = compensator.scan_incomplete().await.expect("scan should succeed");
        assert!(incomplete.is_empty());

        // Re-adding the same task is safe (idempotent begin)
        compensator
            .add_task("task", "run-1", payload.clone())
            .await
            .expect("re-add should succeed");
        assert_eq!(compensator.pending_count(), 1);
    }

    #[tokio::test]
    async fn duplicate_begin_is_idempotent() {
        let compensator = QueueNativeCompensator::new("ns-a");
        let payload1 = Arc::new(vec![1_u8, 2, 3]);
        let payload2 = Arc::new(vec![4_u8, 5, 6]);

        compensator
            .add_task("task", "run-1", payload1.clone())
            .await
            .expect("add should succeed");
        assert_eq!(compensator.pending_count(), 1);

        // Duplicate begin with same key overwrites (idempotent)
        compensator
            .add_task("task", "run-1", payload2.clone())
            .await
            .expect("duplicate add should succeed");
        assert_eq!(compensator.pending_count(), 1);

        let incomplete = compensator.scan_incomplete().await.expect("scan should succeed");
        assert_eq!(incomplete.len(), 1);
    }

    #[tokio::test]
    async fn scan_incomplete_returns_all_pending() {
        let compensator = QueueNativeCompensator::new("ns-a");
        let p1 = Arc::new(vec![1_u8]);
        let p2 = Arc::new(vec![2_u8]);

        compensator.add_task("task", "id-1", p1).await.unwrap();
        compensator.add_task("request", "id-2", p2).await.unwrap();
        assert_eq!(compensator.pending_count(), 2);

        let incomplete = compensator.scan_incomplete().await.unwrap();
        assert_eq!(incomplete.len(), 2);

        // scan_incomplete does NOT drain
        assert_eq!(compensator.pending_count(), 2);
    }
}
