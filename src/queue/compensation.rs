use crate::common::model::{
    Request, Response,
    message::{TaskErrorEvent, TaskEvent, TaskParserEvent},
};
use crate::errors::Result;
use crate::utils::logger::LogModel;
use async_trait::async_trait;
use std::sync::Arc;

/// Trait for objects that can be uniquely identified for compensation purposes.
pub trait Identifiable {
    fn get_id(&self) -> String;

    /// MQ partition key: decides which partition / stream shard / consumer a message is routed
    /// to, used for **account affinity** (session stickiness + tasks for the same account landing
    /// on the same node within the cluster).
    ///
    /// Defaults to the same value as [`get_id`](Self::get_id) (no affinity, random distribution);
    /// `TaskEvent` overrides it with the **account**, so that tasks for one account land on the
    /// same partition consistently via `hash(account)`, reusing Kafka/NATS consumer group
    /// assignment to achieve cross-node consumer affinity. It is independent of `get_id`, which
    /// is used for deduplication / compensation.
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

    /// Account affinity: tasks for one account land on the same partition (session stickiness +
    /// same account on the same node across the cluster).
    fn partition_key(&self) -> String {
        self.account.clone()
    }
}

#[async_trait]
pub trait Compensator: Send + Sync {
    /// Add a task to the compensation queue.
    async fn add_task(&self, topic: &str, id: &str, payload: Arc<Vec<u8>>) -> Result<()>;
    /// Remove a task from the compensation queue.
    async fn remove_task(&self, topic: &str, id: &str) -> Result<()>;
}
