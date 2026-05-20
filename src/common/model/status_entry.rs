use serde::{Deserialize, Serialize};

use crate::common::model::PipelineStage;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Done,
    Failed,
    Retrying,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
            TaskStatus::Retrying => "retrying",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusEntry {
    pub task_id: String,
    pub stage: PipelineStage,
    pub status: TaskStatus,
    pub retry_count: u32,
    pub node_id: String,
    pub updated_at: i64,
    pub error_msg: Option<String>,
}
