use std::sync::Arc;

use async_trait::async_trait;

use crate::common::interface::ModuleNodeTrait;
use crate::schedule::dag::{DagError, DagNodeTrait, NodeExecutionContext, TaskPayload};

/// Adapter that bridges ModuleNodeTrait into DagNodeTrait.
///
/// Current phase keeps the old chain runtime as primary execution path.
/// The adapter is intentionally passthrough-compatible so orchestrator wiring
/// can be integrated safely before full parser/generate migration.
pub struct ModuleNodeDagAdapter {
    node: Arc<dyn ModuleNodeTrait>,
}

impl ModuleNodeDagAdapter {
    pub fn new(node: Arc<dyn ModuleNodeTrait>) -> Self {
        Self { node }
    }

    pub fn inner(&self) -> Arc<dyn ModuleNodeTrait> {
        self.node.clone()
    }
}

#[async_trait]
impl DagNodeTrait for ModuleNodeDagAdapter {
    async fn start(&self, context: NodeExecutionContext) -> Result<TaskPayload, DagError> {
        // Placeholder behavior for phase-A compatibility.
        // It keeps DAG scheduling path compilable while business execution
        // still goes through ModuleProcessorWithChain.
        let mut payload = TaskPayload::from_bytes(Vec::new())
            .with_content_type("application/x-mocra-module-node-placeholder")
            .with_meta("module_node_id", &context.node_id)
            .with_meta("attempt", context.attempt.to_string());

        if let Some(first) = context
            .upstream_nodes
            .iter()
            .find_map(|id| context.upstream_outputs.get(id))
        {
            payload = first.clone();
        }

        Ok(payload)
    }
}
