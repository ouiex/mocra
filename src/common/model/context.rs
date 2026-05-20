use serde::{Deserialize, Serialize};

/// Strongly-typed distributed ExecutionMark for ModuleProcessor.
/// This avoids generic maps and encodes key fields explicitly.

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::ExecutionMark;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct LegacyExecutionMark {
        module_id: Option<String>,
        step_idx: Option<u32>,
        epoch: Option<u64>,
        stay_current_step: bool,
    }

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

    #[test]
    fn execution_mark_deserializes_legacy_msgpack_without_node_id() {
        let legacy = LegacyExecutionMark {
            module_id: Some("module-a".to_string()),
            step_idx: Some(2),
            epoch: Some(42),
            stay_current_step: true,
        };

        let bytes = rmp_serde::to_vec(&legacy).expect("legacy execution mark should serialize");
        let decoded: ExecutionMark =
            rmp_serde::from_slice(&bytes).expect("legacy execution mark should deserialize");

        assert_eq!(decoded.module_id.as_deref(), Some("module-a"));
        assert_eq!(decoded.step_idx, Some(2));
        assert_eq!(decoded.epoch, Some(42));
        assert!(decoded.stay_current_step);
        assert_eq!(decoded.node_id, None);
    }
}
