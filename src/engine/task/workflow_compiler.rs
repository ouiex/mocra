use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use thiserror::Error;

use crate::common::interface::ModuleTrait;
use crate::common::model::{
    EdgeSpec, NodeSpec, TaskProfileSnapshot, WorkflowDefinition, WorkflowDefinitionError,
};
use crate::engine::task::module_dag_compiler::{
    ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef,
};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkflowCompileError {
    #[error(transparent)]
    InvalidWorkflow(#[from] WorkflowDefinitionError),
    #[error("profile node config `{0}` is not declared in workflow")]
    UnknownProfileNode(String),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct WorkflowCompiler;

impl WorkflowCompiler {
    pub async fn compile(
        &self,
        profile: &TaskProfileSnapshot,
        module: Arc<dyn ModuleTrait>,
    ) -> Result<WorkflowDefinition, WorkflowCompileError> {
        let definition = self.build_module_definition(module).await;
        self.compile_definition(profile, definition)
    }

    pub async fn build_module_definition(
        &self,
        module: Arc<dyn ModuleTrait>,
    ) -> ModuleDagDefinition {
        let custom_definition = module.dag_definition().await;
        let linear_definition = self.build_linear_definition(module.clone()).await;

        let has_custom = custom_definition
            .as_ref()
            .map(|definition| !definition.nodes.is_empty())
            .unwrap_or(false);
        let has_linear = !linear_definition.nodes.is_empty();

        match (has_custom, has_linear) {
            (true, true) => custom_definition.expect("checked custom definition exists"),
            (true, false) => custom_definition.expect("checked custom definition exists"),
            (false, true) => linear_definition,
            (false, false) => ModuleDagDefinition::default(),
        }
    }

    pub fn compile_definition(
        &self,
        profile: &TaskProfileSnapshot,
        definition: ModuleDagDefinition,
    ) -> Result<WorkflowDefinition, WorkflowCompileError> {
        let declared_nodes: BTreeSet<&str> = definition
            .nodes
            .iter()
            .map(|node| node.node_id.as_str())
            .collect();
        for node_key in profile.node_configs.keys() {
            if !declared_nodes.contains(node_key.as_str()) {
                return Err(WorkflowCompileError::UnknownProfileNode(node_key.clone()));
            }
        }

        let default_policy = definition.default_policy.clone();
        let mut metadata = BTreeMap::new();
        metadata.insert("profile_key".to_string(), profile.profile_key());
        metadata.insert("profile_version".to_string(), profile.version.to_string());
        metadata.insert("module_name".to_string(), profile.module.clone());
        for (key, value) in definition.metadata {
            metadata.insert(key, value);
        }

        let workflow = WorkflowDefinition {
            nodes: definition
                .nodes
                .into_iter()
                .map(|node| {
                    let mut node_metadata = BTreeMap::new();
                    if let Some(config) = profile.node_configs.get(&node.node_id) {
                        node_metadata
                            .insert("config_schema_id".to_string(), config.schema_id.clone());
                        node_metadata.insert(
                            "config_schema_version".to_string(),
                            config.schema_version.to_string(),
                        );
                    }
                    NodeSpec {
                        node_key: node.node_id,
                        placement: node.placement_override,
                        policy: node.policy_override.or_else(|| default_policy.clone()),
                        tags: node.tags,
                        metadata: node_metadata,
                    }
                })
                .collect(),
            edges: definition
                .edges
                .into_iter()
                .map(|edge| EdgeSpec::new(edge.from, edge.to))
                .collect(),
            entry_nodes: definition.entry_nodes,
            metadata,
        };
        workflow.validate()?;
        Ok(workflow)
    }

    async fn build_linear_definition(&self, module: Arc<dyn ModuleTrait>) -> ModuleDagDefinition {
        let steps = module.add_step().await;
        if steps.is_empty() {
            return ModuleDagDefinition::default();
        }

        let nodes: Vec<ModuleDagNodeDef> = steps
            .into_iter()
            .enumerate()
            .map(|(idx, node)| {
                let stable_key = node.stable_node_key();
                let node_id = if stable_key.is_empty() {
                    format!("step_{idx}")
                } else {
                    stable_key.to_string()
                };
                ModuleDagNodeDef {
                    node_id,
                    node,
                    placement_override: None,
                    policy_override: None,
                    tags: Vec::new(),
                }
            })
            .collect();

        let edges: Vec<ModuleDagEdgeDef> = nodes
            .windows(2)
            .map(|pair| ModuleDagEdgeDef {
                from: pair[0].node_id.clone(),
                to: pair[1].node_id.clone(),
            })
            .collect();

        ModuleDagDefinition {
            entry_nodes: vec![nodes[0].node_id.clone()],
            nodes,
            edges,
            default_policy: None,
            metadata: std::collections::HashMap::from([(
                "source".to_string(),
                "linear_steps".to_string(),
            )]),
        }
    }

}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::common::interface::{
        ModuleNodeTrait, NodeGenerateContext, NodeParseContext, SyncBoxStream, ToSyncBoxStream,
    };
    use crate::common::model::{
        MiddlewareBinding, MiddlewareType, NodeParseOutput, PayloadCodec, Request, Response,
        TypedEnvelope,
    };
    use crate::schedule::dag::{DagNodeExecutionPolicy, DagNodeRetryMode, NodePlacement};

    struct DummyNode {
        stable_key: &'static str,
    }

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> crate::errors::Result<SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> crate::errors::Result<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }

        fn stable_node_key(&self) -> &'static str {
            self.stable_key
        }
    }

    struct LinearModule;
    struct HybridModule;

    #[async_trait]
    impl ModuleTrait for LinearModule {
        fn name(&self) -> &'static str {
            "linear_module"
        }

        fn version(&self) -> i32 {
            1
        }

        fn default_arc() -> Arc<dyn ModuleTrait>
        where
            Self: Sized,
        {
            Arc::new(Self)
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![
                Arc::new(DummyNode { stable_key: "" }),
                Arc::new(DummyNode {
                    stable_key: "detail",
                }),
            ]
        }
    }

    #[async_trait]
    impl ModuleTrait for HybridModule {
        fn name(&self) -> &'static str {
            "hybrid_module"
        }

        fn version(&self) -> i32 {
            2
        }

        fn default_arc() -> Arc<dyn ModuleTrait>
        where
            Self: Sized,
        {
            Arc::new(Self)
        }

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![
                    ModuleDagNodeDef {
                        node_id: "root".to_string(),
                        node: Arc::new(DummyNode { stable_key: "root" }),
                        placement_override: Some(NodePlacement::Remote {
                            worker_group: "wg-a".to_string(),
                        }),
                        policy_override: Some(DagNodeExecutionPolicy {
                            max_retries: 2,
                            timeout_ms: Some(2_000),
                            retry_backoff_ms: 100,
                            idempotency_key: Some("root".to_string()),
                            retry_mode: DagNodeRetryMode::RetryableOnly,
                            circuit_breaker_failure_threshold: Some(5),
                            circuit_breaker_open_ms: 1_000,
                        }),
                        tags: vec!["entry".to_string()],
                    },
                    ModuleDagNodeDef {
                        node_id: "detail".to_string(),
                        node: Arc::new(DummyNode {
                            stable_key: "detail",
                        }),
                        placement_override: None,
                        policy_override: None,
                        tags: vec!["detail".to_string()],
                    },
                ],
                edges: vec![ModuleDagEdgeDef {
                    from: "root".to_string(),
                    to: "detail".to_string(),
                }],
                entry_nodes: vec!["root".to_string()],
                default_policy: None,
                metadata: std::collections::HashMap::new(),
            })
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![Arc::new(DummyNode {
                stable_key: "linear_seed",
            })]
        }
    }

    fn profile(module: &str, node_keys: &[&str]) -> TaskProfileSnapshot {
        TaskProfileSnapshot {
            namespace: "demo".to_string(),
            account: "account-a".to_string(),
            platform: "platform-x".to_string(),
            module: module.to_string(),
            version: 9,
            node_configs: node_keys
                .iter()
                .map(|key| {
                    (
                        (*key).to_string(),
                        TypedEnvelope::new(
                            format!("{key}.config"),
                            1,
                            PayloadCodec::Json,
                            vec![1_u8, 2_u8],
                        ),
                    )
                })
                .collect(),
            download_middleware: vec![MiddlewareBinding {
                name: "download-cache".to_string(),
                middleware_type: MiddlewareType::Download,
                weight: 5,
            }],
            ..TaskProfileSnapshot::default()
        }
    }

    #[tokio::test]
    async fn compile_linear_module_uses_deterministic_step_ids() {
        let compiler = WorkflowCompiler;
        let workflow = compiler
            .compile(
                &profile("linear_module", &["step_0", "detail"]),
                Arc::new(LinearModule),
            )
            .await
            .expect("workflow should compile");

        let node_keys: Vec<_> = workflow
            .nodes
            .iter()
            .map(|node| node.node_key.as_str())
            .collect();
        assert_eq!(node_keys, vec!["step_0", "detail"]);
        assert_eq!(workflow.entry_nodes, vec!["step_0"]);
        assert_eq!(
            workflow
                .metadata
                .get("profile_version")
                .map(std::string::String::as_str),
            Some("9")
        );
    }

    #[tokio::test]
    async fn compile_hybrid_module_prefers_custom_definition() {
        let compiler = WorkflowCompiler;
        let workflow = compiler
            .compile(&profile("hybrid_module", &["root", "detail"]), Arc::new(HybridModule))
            .await
            .expect("workflow should compile");

        assert!(workflow.node("root").is_some());
        assert!(workflow.node("detail").is_some());
        assert!(workflow.node("linear_seed").is_none());
    }

    #[tokio::test]
    async fn compile_definition_preserves_node_policy_and_schema_metadata() {
        let compiler = WorkflowCompiler;
        let workflow = compiler
            .compile(&profile("hybrid_module", &["root", "detail"]), Arc::new(HybridModule))
            .await
            .expect("workflow should compile");
        let root = workflow.node("root").expect("root node should exist");

        assert_eq!(root.tags, vec!["entry"]);
        assert_eq!(
            root.metadata
                .get("config_schema_id")
                .map(std::string::String::as_str),
            Some("root.config")
        );
        assert_eq!(
            root.metadata
                .get("config_schema_version")
                .map(std::string::String::as_str),
            Some("1")
        );
        assert_eq!(
            root.policy,
            Some(DagNodeExecutionPolicy {
                max_retries: 2,
                timeout_ms: Some(2_000),
                retry_backoff_ms: 100,
                idempotency_key: Some("root".to_string()),
                retry_mode: DagNodeRetryMode::RetryableOnly,
                circuit_breaker_failure_threshold: Some(5),
                circuit_breaker_open_ms: 1_000,
            })
        );
    }

    #[test]
    fn compile_definition_rejects_unknown_profile_nodes() {
        let compiler = WorkflowCompiler;
        let definition = ModuleDagDefinition {
            nodes: vec![ModuleDagNodeDef {
                node_id: "root".to_string(),
                node: Arc::new(DummyNode { stable_key: "root" }),
                placement_override: None,
                policy_override: None,
                tags: Vec::new(),
            }],
            edges: Vec::new(),
            entry_nodes: vec!["root".to_string()],
            default_policy: None,
            metadata: std::collections::HashMap::new(),
        };
        let profile = profile("hybrid_module", &["root", "missing"]);

        let error = compiler
            .compile_definition(&profile, definition)
            .expect_err("unknown node config should fail");

        assert_eq!(
            error,
            WorkflowCompileError::UnknownProfileNode("missing".to_string())
        );
    }
}
