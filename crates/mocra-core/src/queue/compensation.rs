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

    /// MQ 分区键:决定消息路由到哪个分区 / 流分片 / 消费者,用于**账号亲和**
    /// (会话粘性 + 集群里同账号任务落同一节点)。
    ///
    /// 默认与 [`get_id`](Self::get_id) 相同(无亲和,随机分布);`TaskEvent` 覆写为
    /// **账号**,使同一账号的任务经 `hash(account)` 稳定落到同一分区,从而复用
    /// Kafka/NATS 消费组分配 实现跨节点消费亲和。与去重 / 补偿用的
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
    /// Add a task to the compensation queue.
    async fn add_task(&self, topic: &str, id: &str, payload: Arc<Vec<u8>>) -> Result<()>;
    /// Remove a task from the compensation queue.
    async fn remove_task(&self, topic: &str, id: &str) -> Result<()>;
}

