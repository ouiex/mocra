use crate::common::model::config::RedisConfig;
use crate::common::model::{
    Request, Response,
    message::{TaskErrorEvent, TaskEvent, TaskParserEvent},
};
use crate::errors::Result;
use crate::errors::error::QueueError;
use crate::utils::logger::LogModel;
use async_trait::async_trait;
use deadpool_redis::redis;
use log::{error, warn};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;

/// Trait for objects that can be uniquely identified for compensation purposes.
pub trait Identifiable {
    fn get_id(&self) -> String;

    /// MQ 分区键:决定消息路由到哪个分区 / 流分片 / 消费者,用于**账号亲和**
    /// (会话粘性 + 集群里同账号任务落同一节点)。
    ///
    /// 默认与 [`get_id`](Self::get_id) 相同(无亲和,随机分布);`TaskEvent` 覆写为
    /// **账号**,使同一账号的任务经 `hash(account)` 稳定落到同一分区,从而复用
    /// Kafka/NATS 消费组分配 / Redis 流分片实现跨节点消费亲和。与去重 / 补偿用的
    /// `get_id` 相互独立。
    fn partition_key(&self) -> String {
        self.get_id()
    }
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

impl Identifiable for TaskParserEvent {
    fn get_id(&self) -> String {
        self.id.to_string()
    }
}

impl Identifiable for TaskErrorEvent {
    fn get_id(&self) -> String {
        self.id.to_string()
    }
}

impl Identifiable for TaskEvent {
    fn get_id(&self) -> String {
        self.run_id.to_string()
    }

    /// 账号亲和:同账号任务落同一分区(会话粘性 + 集群同账号同节点)。
    fn partition_key(&self) -> String {
        self.account.clone()
    }
}

#[async_trait]
pub trait Compensator: Send + Sync {
    /// Add a task to the compensation queue (Redis ZSet + Hash).
    async fn add_task(&self, topic: &str, id: &str, payload: Arc<Vec<u8>>) -> Result<()>;
    /// Remove a task from the compensation queue.
    async fn remove_task(&self, topic: &str, id: &str) -> Result<()>;
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
