use serde::{Deserialize, Serialize};

/// Strongly-typed distributed ExecutionMark for ModuleProcessor.
/// This avoids generic maps and encodes key fields explicitly.

#[derive(Clone,Debug,Serialize,Deserialize,Default)]
pub struct ExecutionMark {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_idx: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// Whether to keep ModuleProcessor on current step when pending==0
    #[serde(default)]
    pub stay_current_step: bool,
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
}
