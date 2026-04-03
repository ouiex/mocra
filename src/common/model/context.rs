
use serde::{Deserialize, Serialize};

/// Strongly-typed distributed ExecutionMark for ModuleProcessor.
/// This avoids generic maps and encodes key fields explicitly.

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ExecutionMark {
    pub module_id: Option<String>,
    /// Legacy linear step index (kept for backward compat with in-flight queue messages).
    /// When `node_id` is set, `step_idx` is ignored by `ModuleDagProcessor`.
    pub step_idx: Option<u32>,
    pub epoch: Option<u64>,
    /// Whether to retry on the current node/step without advancing.
    #[serde(default)]
    pub stay_current_step: bool,
    /// DAG node identifier. When set, this is the primary routing key for `ModuleDagProcessor`.
    /// Replaces `step_idx`-based routing in the new queue-backed DAG engine.
    /// IMPORTANT: must be the last field to maintain msgpack array positional compat with old messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

impl ExecutionMark {
    pub fn with_module_id(mut self, mid: impl AsRef<str>) -> Self {
        self.module_id = Some(mid.as_ref().into());
        self
    }
    pub fn with_step_idx(mut self, idx: u32) -> Self {
        self.step_idx = Some(idx);
        self
    }
    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch = Some(epoch);
        self
    }
    pub fn with_stay_current_step(mut self, stay: bool) -> Self {
        self.stay_current_step = stay;
        self
    }
    pub fn with_node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = Some(id.into());
        self
    }
}
