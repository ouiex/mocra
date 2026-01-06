use async_trait::async_trait;
use common::model::config::RedisConfig;
use common::model::{
    Request, Response,
    message::{ErrorTaskModel, ParserTaskModel, TaskModel},
};
use errors::Result;
use errors::error::QueueError;
use log::error;
use tokio::sync::mpsc;
use utils::logger::LogModel;

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

impl Identifiable for ParserTaskModel {
    fn get_id(&self) -> String {
        self.id.to_string()
    }
}

impl Identifiable for ErrorTaskModel {
    fn get_id(&self) -> String {
        self.id.to_string()
    }
}

impl Identifiable for TaskModel {
    fn get_id(&self) -> String {
        self.run_id.to_string()
    }
}

#[async_trait]
pub trait Compensator: Send + Sync {
    /// Add a task to the compensation queue (Redis ZSet + Hash).
    async fn add_task(&self, topic: &str, id: &str, payload: &[u8]) -> Result<()>;
    /// Remove a task from the compensation queue.
    async fn remove_task(&self, topic: &str, id: &str) -> Result<()>;
}

enum CompensationMessage {
    Add {
        topic: String,
        id: String,
        payload: Vec<u8>,
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
            while let Some(msg) = rx.recv().await {
                let mut conn = match client.get_multiplexed_async_connection().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to connect to Redis in compensator: {}", e);
                        continue;
                    }
                };

                match msg {
                    CompensationMessage::Add { topic, id, payload } => {
                        let zset_key = format!("{}:compensation:{}:zset", namespace, topic);
                        let hash_key = format!("{}:compensation:{}:data", namespace, topic);
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();

                        let mut pipe = redis::pipe();
                        pipe.zadd(&zset_key, &id, now).hset(&hash_key, &id, payload);

                        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
                            error!("Failed to add compensation task to Redis: {}", e);
                        }
                    }
                    CompensationMessage::Remove { topic, id } => {
                        let zset_key = format!("{}:compensation:{}:zset", namespace, topic);
                        let hash_key = format!("{}:compensation:{}:data", namespace, topic);

                        let mut pipe = redis::pipe();
                        pipe.zrem(&zset_key, &id).hdel(&hash_key, &id);

                        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
                            error!("Failed to remove compensation task from Redis: {}", e);
                        }
                    }
                }
            }
        });

        Ok(Self { sender: tx })
    }
}

#[async_trait]
impl Compensator for RedisCompensator {
    async fn add_task(&self, topic: &str, id: &str, payload: &[u8]) -> Result<()> {
        self.sender
            .send(CompensationMessage::Add {
                topic: topic.to_string(),
                id: id.to_string(),
                payload: payload.to_vec(),
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
