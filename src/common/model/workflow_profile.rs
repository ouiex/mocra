use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::common::model::Priority;
use crate::schedule::dag::{DagNodeExecutionPolicy, NodePlacement};

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum PayloadCodec {
    #[default]
    #[serde(rename = "msgpack")]
    MsgPack,
    #[serde(rename = "json")]
    Json,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TypedEnvelope {
    pub schema_id: String,
    pub schema_version: u16,
    pub codec: PayloadCodec,
    #[serde(default)]
    pub bytes: Vec<u8>,
}

impl TypedEnvelope {
    pub fn new(
        schema_id: impl Into<String>,
        schema_version: u16,
        codec: PayloadCodec,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            schema_id: schema_id.into(),
            schema_version,
            codec,
            bytes: bytes.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MiddlewareType {
    Data,
    Download,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MiddlewareBinding {
    pub name: String,
    pub middleware_type: MiddlewareType,
    pub weight: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedCommonConfig {
    pub timeout_secs: u64,
    pub rate_limit: Option<f32>,
    pub priority: Priority,
    pub proxy_pool: Option<String>,
    pub downloader: String,
    pub enable_session: bool,
    pub serial_execution: bool,
    pub module_locker: bool,
    pub rate_limit_group: Option<String>,
    pub response_cache_enabled: bool,
    pub response_cache_ttl_secs: Option<u64>,
}

impl Default for ResolvedCommonConfig {
    fn default() -> Self {
        Self {
            timeout_secs: 30,
            rate_limit: None,
            priority: Priority::default(),
            proxy_pool: None,
            downloader: "request_downloader".to_string(),
            enable_session: false,
            serial_execution: false,
            module_locker: false,
            rate_limit_group: None,
            response_cache_enabled: false,
            response_cache_ttl_secs: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedNodeConfig {
    pub profile_key: String,
    pub profile_version: u64,
    pub common: ResolvedCommonConfig,
    pub node_config: TypedEnvelope,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskProfileSnapshot {
    pub namespace: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub version: u64,
    pub enabled: bool,
    pub common: ResolvedCommonConfig,
    pub node_configs: BTreeMap<String, TypedEnvelope>,
    pub download_middleware: Vec<MiddlewareBinding>,
    pub data_middleware: Vec<MiddlewareBinding>,
    pub middleware_configs: BTreeMap<String, TypedEnvelope>,
    pub debug_layers_json: Option<serde_json::Value>,
    pub updated_at: i64,
    pub updated_by: String,
}

impl Default for TaskProfileSnapshot {
    fn default() -> Self {
        Self {
            namespace: String::new(),
            account: String::new(),
            platform: String::new(),
            module: String::new(),
            version: 0,
            enabled: true,
            common: ResolvedCommonConfig::default(),
            node_configs: BTreeMap::new(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            middleware_configs: BTreeMap::new(),
            debug_layers_json: None,
            updated_at: 0,
            updated_by: String::new(),
        }
    }
}

impl TaskProfileSnapshot {
    pub fn profile_key(&self) -> String {
        format!(
            "{}:profile:{}:{}:{}",
            self.namespace, self.account, self.platform, self.module
        )
    }

    pub fn resolve_node_config(&self, node_key: impl AsRef<str>) -> Option<ResolvedNodeConfig> {
        self.node_configs
            .get(node_key.as_ref())
            .cloned()
            .map(|node_config| ResolvedNodeConfig {
                profile_key: self.profile_key(),
                profile_version: self.version,
                common: self.common.clone(),
                node_config,
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeSpec {
    pub node_key: String,
    pub placement: Option<NodePlacement>,
    pub policy: Option<DagNodeExecutionPolicy>,
    pub tags: Vec<String>,
    pub metadata: BTreeMap<String, String>,
}

impl NodeSpec {
    pub fn new(node_key: impl Into<String>) -> Self {
        Self {
            node_key: node_key.into(),
            placement: None,
            policy: None,
            tags: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }
}

pub const RUNTIME_NODE_HINT_METADATA_KEY: &str = "__mocra_runtime_node";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeNodeRoutingHint {
    pub node_key: String,
    #[serde(default)]
    pub placement: Option<NodePlacement>,
    #[serde(default)]
    pub policy: Option<DagNodeExecutionPolicy>,
}

impl RuntimeNodeRoutingHint {
    pub fn new(node_key: impl Into<String>) -> Self {
        Self {
            node_key: node_key.into(),
            placement: None,
            policy: None,
        }
    }

    pub fn with_placement(mut self, placement: NodePlacement) -> Self {
        self.placement = Some(placement);
        self
    }

    pub fn with_policy(mut self, policy: DagNodeExecutionPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
}

pub fn insert_runtime_node_hint(metadata: &mut Map<String, Value>, hint: &RuntimeNodeRoutingHint) {
    if let Ok(value) = serde_json::to_value(hint) {
        metadata.insert(RUNTIME_NODE_HINT_METADATA_KEY.to_string(), value);
    }
}

pub fn extract_runtime_node_hint(metadata: &Map<String, Value>) -> Option<RuntimeNodeRoutingHint> {
    metadata
        .get(RUNTIME_NODE_HINT_METADATA_KEY)
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EdgeSpec {
    pub from: String,
    pub to: String,
}

impl EdgeSpec {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            to: to.into(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct WorkflowDefinition {
    pub nodes: Vec<NodeSpec>,
    pub edges: Vec<EdgeSpec>,
    pub entry_nodes: Vec<String>,
    pub metadata: BTreeMap<String, String>,
}

impl WorkflowDefinition {
    pub fn node(&self, node_key: &str) -> Option<&NodeSpec> {
        self.nodes.iter().find(|node| node.node_key == node_key)
    }

    pub fn derived_entry_nodes(&self) -> Vec<String> {
        if !self.entry_nodes.is_empty() {
            return self.entry_nodes.clone();
        }

        let mut all_nodes: BTreeSet<&str> = self
            .nodes
            .iter()
            .map(|node| node.node_key.as_str())
            .collect();
        let incoming: BTreeSet<&str> = self.edges.iter().map(|edge| edge.to.as_str()).collect();
        all_nodes.retain(|node_key| !incoming.contains(node_key));
        all_nodes.into_iter().map(str::to_string).collect()
    }

    pub fn validate(&self) -> Result<(), WorkflowDefinitionError> {
        if self.nodes.is_empty() {
            return Err(WorkflowDefinitionError::EmptyWorkflow);
        }

        let mut node_keys = BTreeSet::new();
        for node in &self.nodes {
            if node.node_key.is_empty() {
                return Err(WorkflowDefinitionError::EmptyNodeKey);
            }
            if !node_keys.insert(node.node_key.as_str()) {
                return Err(WorkflowDefinitionError::DuplicateNode(
                    node.node_key.clone(),
                ));
            }
        }

        let mut entry_keys = BTreeSet::new();
        for entry in &self.entry_nodes {
            if !node_keys.contains(entry.as_str()) {
                return Err(WorkflowDefinitionError::MissingNode(entry.clone()));
            }
            if !entry_keys.insert(entry.as_str()) {
                return Err(WorkflowDefinitionError::DuplicateEntryNode(entry.clone()));
            }
        }

        for edge in &self.edges {
            if !node_keys.contains(edge.from.as_str()) {
                return Err(WorkflowDefinitionError::MissingNode(edge.from.clone()));
            }
            if !node_keys.contains(edge.to.as_str()) {
                return Err(WorkflowDefinitionError::MissingNode(edge.to.clone()));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkflowDefinitionError {
    #[error("workflow definition has no nodes")]
    EmptyWorkflow,
    #[error("workflow node key must not be empty")]
    EmptyNodeKey,
    #[error("duplicate workflow node: {0}")]
    DuplicateNode(String),
    #[error("duplicate workflow entry node: {0}")]
    DuplicateEntryNode(String),
    #[error("workflow references missing node: {0}")]
    MissingNode(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolved_common_config_defaults_match_current_request_defaults() {
        let config = ResolvedCommonConfig::default();

        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.priority, Priority::Normal);
        assert_eq!(config.downloader, "request_downloader");
        assert!(!config.enable_session);
        assert!(!config.serial_execution);
        assert!(!config.response_cache_enabled);
        assert_eq!(config.rate_limit, None);
    }

    #[test]
    fn task_profile_snapshot_builds_profile_key_and_resolves_node_config() {
        let node_config = TypedEnvelope::new(
            "catalog.page",
            1,
            PayloadCodec::MsgPack,
            vec![1_u8, 2_u8, 3_u8],
        );
        let snapshot = TaskProfileSnapshot {
            namespace: "demo".to_string(),
            account: "account-a".to_string(),
            platform: "platform-x".to_string(),
            module: "catalog".to_string(),
            version: 7,
            common: ResolvedCommonConfig {
                timeout_secs: 45,
                serial_execution: true,
                ..ResolvedCommonConfig::default()
            },
            node_configs: BTreeMap::from([("page".to_string(), node_config.clone())]),
            ..TaskProfileSnapshot::default()
        };

        assert_eq!(
            snapshot.profile_key(),
            "demo:profile:account-a:platform-x:catalog"
        );

        let resolved = snapshot
            .resolve_node_config("page")
            .expect("node config should resolve");
        assert_eq!(resolved.profile_key, snapshot.profile_key());
        assert_eq!(resolved.profile_version, 7);
        assert_eq!(resolved.common.timeout_secs, 45);
        assert!(resolved.common.serial_execution);
        assert_eq!(resolved.node_config, node_config);
        assert!(snapshot.resolve_node_config("missing").is_none());
    }

    #[test]
    fn workflow_definition_can_derive_entry_nodes_and_validate_references() {
        let workflow = WorkflowDefinition {
            nodes: vec![
                NodeSpec::new("login"),
                NodeSpec::new("catalog"),
                NodeSpec::new("detail"),
            ],
            edges: vec![
                EdgeSpec::new("login", "catalog"),
                EdgeSpec::new("catalog", "detail"),
            ],
            entry_nodes: Vec::new(),
            metadata: BTreeMap::new(),
        };

        assert_eq!(workflow.derived_entry_nodes(), vec!["login".to_string()]);
        assert!(workflow.validate().is_ok());
        assert_eq!(
            workflow.node("catalog").map(|node| node.node_key.as_str()),
            Some("catalog")
        );
    }

    #[test]
    fn workflow_definition_rejects_unknown_nodes_in_edges_or_entries() {
        let missing_edge = WorkflowDefinition {
            nodes: vec![NodeSpec::new("login")],
            edges: vec![EdgeSpec::new("login", "detail")],
            entry_nodes: Vec::new(),
            metadata: BTreeMap::new(),
        };
        assert_eq!(
            missing_edge.validate(),
            Err(WorkflowDefinitionError::MissingNode("detail".to_string()))
        );

        let missing_entry = WorkflowDefinition {
            nodes: vec![NodeSpec::new("login")],
            edges: Vec::new(),
            entry_nodes: vec!["detail".to_string()],
            metadata: BTreeMap::new(),
        };
        assert_eq!(
            missing_entry.validate(),
            Err(WorkflowDefinitionError::MissingNode("detail".to_string()))
        );
    }

    #[test]
    fn runtime_node_hint_round_trips_through_metadata_map() {
        let hint = RuntimeNodeRoutingHint::new("detail")
            .with_placement(NodePlacement::remote("wg-parser"))
            .with_policy(DagNodeExecutionPolicy {
                max_retries: 2,
                timeout_ms: Some(1500),
                retry_backoff_ms: 300,
                idempotency_key: Some("detail-key".to_string()),
                retry_mode: crate::schedule::dag::DagNodeRetryMode::RetryableOnly,
                circuit_breaker_failure_threshold: Some(5),
                circuit_breaker_open_ms: 2000,
            });
        let mut metadata = Map::new();

        insert_runtime_node_hint(&mut metadata, &hint);

        assert_eq!(extract_runtime_node_hint(&metadata), Some(hint));
    }
}
