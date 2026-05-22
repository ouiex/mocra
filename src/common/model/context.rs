use serde::{Deserialize, Serialize};

/// Strongly-typed distributed ExecutionMark for ModuleProcessor.
/// This avoids generic maps and encodes key fields explicitly.

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ExecutionMark {
    pub module_id: Option<String>,
    /// Linear step index used only when a DAG node id is not available.
    /// When `node_id` is set, `step_idx` is ignored by `ModuleDagProcessor`.
    pub step_idx: Option<u32>,
    pub epoch: Option<u64>,
    /// Whether to retry on the current node/step without advancing.
    #[serde(default)]
    pub stay_current_step: bool,
    /// DAG node identifier. When set, this is the primary routing key for `ModuleDagProcessor`.
    /// Replaces `step_idx`-based routing in the new queue-backed DAG engine.
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

#[cfg(test)]
mod tests {
    use super::ExecutionMark;
    #[test]
    fn execution_mark_msgpack_round_trips_without_node_id() {
        let mark = ExecutionMark {
            module_id: Some("module-a".to_string()),
            step_idx: Some(2),
            epoch: Some(42),
            stay_current_step: true,
            node_id: None,
        };

        let bytes = rmp_serde::to_vec(&mark).expect("execution mark should serialize");
        let decoded: ExecutionMark =
            rmp_serde::from_slice(&bytes).expect("execution mark should deserialize");

        assert_eq!(decoded, mark);
    }

    #[test]
    fn execution_mark_msgpack_round_trips_with_node_id() {
        let mark = ExecutionMark::default()
            .with_module_id("module-a")
            .with_step_idx(2)
            .with_epoch(42)
            .with_stay_current_step(true)
            .with_node_id("node-a");

        let bytes = rmp_serde::to_vec(&mark).expect("execution mark should serialize");
        let decoded: ExecutionMark =
            rmp_serde::from_slice(&bytes).expect("execution mark should deserialize");

        assert_eq!(decoded, mark);
    }
}
