use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::model::data::DataEvent;
use crate::common::model::{Priority, ResolvedNodeConfig, TypedEnvelope};

pub type ParsedData = DataEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutingMeta {
    pub namespace: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub node_key: String,
    pub run_id: Uuid,
    pub request_id: Uuid,
    pub parent_request_id: Option<Uuid>,
    pub priority: Priority,
}

impl RoutingMeta {
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.account, self.platform)
    }

    pub fn module_id(&self) -> String {
        format!("{}-{}-{}", self.account, self.platform, self.module)
    }

    pub fn module_runtime_id(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.account, self.platform, self.module, self.run_id
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionMeta {
    pub retry_count: u32,
    pub task_retry_count: u32,
    pub profile_version: u64,
    pub trace_id: Option<String>,
    pub fence_token: Option<u64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl ExecutionMeta {
    pub fn touch(mut self, updated_at_ms: i64) -> Self {
        self.updated_at_ms = updated_at_ms;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeInput {
    pub source_node: Option<String>,
    pub target_node: String,
    pub payload: TypedEnvelope,
}

impl NodeInput {
    pub fn new(target_node: impl Into<String>, payload: TypedEnvelope) -> Self {
        Self {
            source_node: None,
            target_node: target_node.into(),
            payload,
        }
    }

    pub fn from_source(mut self, source_node: impl Into<String>) -> Self {
        self.source_node = Some(source_node.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeDispatch {
    pub target_node: String,
    pub input: NodeInput,
}

impl NodeDispatch {
    pub fn new(target_node: impl Into<String>, input: NodeInput) -> Self {
        Self {
            target_node: target_node.into(),
            input,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct NodeParseOutput {
    pub next: Vec<NodeDispatch>,
    pub data: Vec<ParsedData>,
    pub finished: bool,
}

impl NodeParseOutput {
    pub fn with_next(mut self, dispatch: NodeDispatch) -> Self {
        self.next.push(dispatch);
        self
    }

    pub fn with_data(mut self, data: ParsedData) -> Self {
        self.data.push(data);
        self
    }

    pub fn finish(mut self) -> Self {
        self.finished = true;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestContextRef {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    pub input: NodeInput,
    pub node_config: ResolvedNodeConfig,
}

impl RequestContextRef {
    pub fn profile_key(&self) -> &str {
        &self.node_config.profile_key
    }

    pub fn profile_version(&self) -> u64 {
        self.node_config.profile_version
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::model::{PayloadCodec, ResolvedCommonConfig, TaskProfileSnapshot};

    fn sample_envelope() -> TypedEnvelope {
        TypedEnvelope::new(
            "node.payload",
            1,
            PayloadCodec::MsgPack,
            vec![1_u8, 2_u8, 3_u8],
        )
    }

    fn sample_routing() -> RoutingMeta {
        RoutingMeta {
            namespace: "demo".to_string(),
            account: "account-a".to_string(),
            platform: "platform-x".to_string(),
            module: "catalog".to_string(),
            node_key: "page".to_string(),
            run_id: Uuid::now_v7(),
            request_id: Uuid::now_v7(),
            parent_request_id: None,
            priority: Priority::High,
        }
    }

    #[test]
    fn routing_meta_helpers_match_existing_runtime_naming() {
        let routing = sample_routing();

        assert_eq!(routing.task_id(), "account-a-platform-x");
        assert_eq!(routing.module_id(), "account-a-platform-x-catalog");
        assert!(
            routing
                .module_runtime_id()
                .starts_with("account-a-platform-x-catalog-")
        );
    }

    #[test]
    fn execution_meta_touch_updates_timestamp() {
        let exec = ExecutionMeta {
            created_at_ms: 100,
            updated_at_ms: 100,
            ..ExecutionMeta::default()
        }
        .touch(250);

        assert_eq!(exec.created_at_ms, 100);
        assert_eq!(exec.updated_at_ms, 250);
    }

    #[test]
    fn node_parse_output_collects_dispatches_and_data() {
        let input = NodeInput::new("detail", sample_envelope()).from_source("page");
        let dispatch = NodeDispatch::new("detail", input.clone());
        let output = NodeParseOutput::default()
            .with_next(dispatch.clone())
            .with_data(ParsedData::default())
            .finish();

        assert_eq!(output.next.len(), 1);
        assert_eq!(output.next[0], dispatch);
        assert_eq!(output.data.len(), 1);
        assert!(output.finished);
        assert_eq!(input.source_node.as_deref(), Some("page"));
    }

    #[test]
    fn request_context_ref_exposes_profile_identity() {
        let snapshot = TaskProfileSnapshot {
            namespace: "demo".to_string(),
            account: "account-a".to_string(),
            platform: "platform-x".to_string(),
            module: "catalog".to_string(),
            version: 11,
            common: ResolvedCommonConfig::default(),
            node_configs: BTreeMap::from([("page".to_string(), sample_envelope())]),
            ..TaskProfileSnapshot::default()
        };
        let node_config = snapshot
            .resolve_node_config("page")
            .expect("node config should resolve");
        let context = RequestContextRef {
            routing: sample_routing(),
            exec: ExecutionMeta {
                profile_version: 11,
                ..ExecutionMeta::default()
            },
            input: NodeInput::new("page", sample_envelope()),
            node_config,
        };

        assert_eq!(
            context.profile_key(),
            "demo:profile:account-a:platform-x:catalog"
        );
        assert_eq!(context.profile_version(), 11);
        assert_eq!(context.exec.profile_version, 11);
    }
}
