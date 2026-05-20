use crate::common::interface::storage::{BlobStorage, Offloadable};
use crate::common::model::transport_envelope::{NodeDispatchEnvelope, NodeErrorEnvelope};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum TopicType {
    /// Task queue.
    Task,
    /// Request queue.
    Request,
    /// Response queue (raw task output).
    Response,
    /// Parser task queue (processed results).
    ParserTask,
    /// Error queue (error details).
    Error,
}

impl TopicType {
    /// Returns topic suffix.
    #[cfg(test)]
    pub(crate) fn suffix(&self) -> &'static str {
        match self {
            TopicType::Response => "response",
            TopicType::ParserTask => "parser_task",
            TopicType::Error => "error",
            TopicType::Task => "task",
            TopicType::Request => "request",
        }
    }
}

/// Base task model.
///
/// Defines minimal task identity with account, platform, and module information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    /// Account identifier.
    pub account: String,
    /// Platform identifier.
    pub platform: String,
    /// Module list (optional; empty means all modules).
    pub module: Option<Vec<String>>,
    /// Priority.
    #[serde(default)]
    pub priority: crate::common::model::Priority,
    /// Run identifier.
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum UnifiedTaskInput {
    Task(TaskEvent),
    ParserDispatch(NodeDispatchEnvelope),
    ErrorEnvelope(NodeErrorEnvelope),
}

impl From<TaskEvent> for UnifiedTaskInput {
    fn from(value: TaskEvent) -> Self {
        UnifiedTaskInput::Task(value)
    }
}

impl From<NodeDispatchEnvelope> for UnifiedTaskInput {
    fn from(value: NodeDispatchEnvelope) -> Self {
        UnifiedTaskInput::ParserDispatch(value)
    }
}

impl From<NodeErrorEnvelope> for UnifiedTaskInput {
    fn from(value: NodeErrorEnvelope) -> Self {
        UnifiedTaskInput::ErrorEnvelope(value)
    }
}

#[async_trait]
impl Offloadable for TaskEvent {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
}

impl crate::common::model::priority::Prioritizable for TaskEvent {
    fn get_priority(&self) -> crate::common::model::Priority {
        self.priority
    }
}

/// Serde default function to auto-generate a run_id using UUID v7
fn default_run_id() -> Uuid {
    Uuid::now_v7()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_type() {
        assert_eq!(TopicType::Task.suffix(), "task");
        assert_eq!(TopicType::Request.suffix(), "request");
        assert_eq!(TopicType::Response.suffix(), "response");
    }
}
