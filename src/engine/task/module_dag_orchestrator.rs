use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::interface::ModuleTrait;
use crate::common::model::RuntimeNodeRoutingHint;
use crate::engine::task::module_dag_compiler::{
    ModuleDagCompiler, ModuleDagDefinition,
};
use crate::engine::task::module_node_runtime_bridge::{
    SchedulerNodeGenerateRuntimeInput, SchedulerNodeParserRuntimeInput,
    encode_generate_runtime_input, encode_parser_runtime_input,
};
use crate::schedule::dag::{
    Dag, DagError, DagExecutionReport, DagNodeDispatcher, DagNodeRuntimeOverride,
    DagScheduler, DagSchedulerOptions,
};

#[derive(Debug, Clone, Default)]
pub struct ModuleDagOrchestratorOptions {
    pub scheduler_options: DagSchedulerOptions,
}

pub struct ModuleDagOrchestrator {
    options: ModuleDagOrchestratorOptions,
}

impl Default for ModuleDagOrchestrator {
    fn default() -> Self {
        Self {
            options: ModuleDagOrchestratorOptions::default(),
        }
    }
}

impl ModuleDagOrchestrator {
    const DAG_VERSION_METADATA_KEY: &'static str = "dag_version";

    pub fn routing_hints_to_runtime_overrides(
        hints: impl IntoIterator<Item = RuntimeNodeRoutingHint>,
    ) -> Vec<DagNodeRuntimeOverride> {
        let mut merged = std::collections::HashMap::<String, DagNodeRuntimeOverride>::new();

        for hint in hints {
            let entry = merged
                .entry(hint.node_key.clone())
                .or_insert_with(|| DagNodeRuntimeOverride::new(hint.node_key.clone()));
            if let Some(placement) = hint.placement {
                entry.placement = Some(placement);
            }
            if let Some(policy) = hint.policy {
                entry.execution_policy = Some(policy);
            }
        }

        let mut runtime_overrides: Vec<DagNodeRuntimeOverride> = merged
            .into_values()
            .filter(|runtime_override| !runtime_override.is_empty())
            .collect();
        runtime_overrides.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        runtime_overrides
    }

    pub fn new(options: ModuleDagOrchestratorOptions) -> Self {
        Self { options }
    }

    fn dag_version_for_definition(definition: &ModuleDagDefinition) -> String {
        #[derive(serde::Serialize)]
        struct NodeFingerprint<'a> {
            node_id: &'a str,
            placement_override: &'a Option<crate::schedule::dag::NodePlacement>,
            policy_override: &'a Option<crate::schedule::dag::DagNodeExecutionPolicy>,
            tags: &'a [String],
        }

        #[derive(serde::Serialize)]
        struct EdgeFingerprint<'a> {
            from: &'a str,
            to: &'a str,
        }

        #[derive(serde::Serialize)]
        struct DagFingerprint<'a> {
            nodes: Vec<NodeFingerprint<'a>>,
            edges: Vec<EdgeFingerprint<'a>>,
            entry_nodes: &'a [String],
            default_policy: &'a Option<crate::schedule::dag::DagNodeExecutionPolicy>,
            metadata: BTreeMap<String, String>,
        }

        let metadata = definition
            .metadata
            .iter()
            .filter(|(key, _)| key.as_str() != Self::DAG_VERSION_METADATA_KEY)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let normalized_node_ids: std::collections::HashMap<String, String> = definition
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let stable_key = node.node.stable_node_key();
                let fingerprint_id = if stable_key.is_empty() {
                    format!("index_{index}")
                } else {
                    stable_key.to_string()
                };
                (node.node_id.clone(), fingerprint_id)
            })
            .collect();
        let fingerprint = DagFingerprint {
            nodes: definition
                .nodes
                .iter()
                .map(|node| NodeFingerprint {
                    node_id: normalized_node_ids
                        .get(&node.node_id)
                        .map(String::as_str)
                        .unwrap_or(node.node_id.as_str()),
                    placement_override: &node.placement_override,
                    policy_override: &node.policy_override,
                    tags: &node.tags,
                })
                .collect(),
            edges: definition
                .edges
                .iter()
                .map(|edge| EdgeFingerprint {
                    from: normalized_node_ids
                        .get(&edge.from)
                        .map(String::as_str)
                        .unwrap_or(edge.from.as_str()),
                    to: normalized_node_ids
                        .get(&edge.to)
                        .map(String::as_str)
                        .unwrap_or(edge.to.as_str()),
                })
                .collect(),
            entry_nodes: &definition
                .entry_nodes
                .iter()
                .map(|entry| {
                    normalized_node_ids
                        .get(entry)
                        .cloned()
                        .unwrap_or_else(|| entry.clone())
                })
                .collect::<Vec<_>>(),
            default_policy: &definition.default_policy,
            metadata,
        };

        let bytes = serde_json::to_vec(&fingerprint).unwrap_or_default();
        format!("{:x}", md5::compute(bytes))
    }

    fn stamp_definition_with_dag_version(
        mut definition: ModuleDagDefinition,
    ) -> ModuleDagDefinition {
        let dag_version = Self::dag_version_for_definition(&definition);
        definition
            .metadata
            .insert(Self::DAG_VERSION_METADATA_KEY.to_string(), dag_version);
        definition
    }

    /// Compiles a DAG from explicit module definition.
    pub fn compile_definition(&self, definition: ModuleDagDefinition) -> Result<Dag, DagError> {
        ModuleDagCompiler::compile(Self::stamp_definition_with_dag_version(definition))
    }

    /// Compiles DAG from module hooks.
    ///
    /// Behavior:
    /// - only `dag_definition()`: compile custom DAG.
    /// - only `add_step()`: compile linear-compat DAG.
    /// - both present: merge into one multi-route DAG and compile.
    pub async fn compile_module(&self, module: Arc<dyn ModuleTrait>) -> Result<Dag, DagError> {
        self.compile_definition(self.build_definition(module).await)
    }

    /// Builds a merged `ModuleDagDefinition` from a module's hooks without compiling.
    ///
    /// Same selection semantics as `compile_module`, but returns the definition so it can be
    /// fed directly into `ModuleDagProcessor::init_from_definition`.
    pub async fn build_definition(&self, module: Arc<dyn ModuleTrait>) -> ModuleDagDefinition {
        let custom_definition = module.dag_definition().await;
        let linear_definition = ModuleDagDefinition::from_linear_steps(module.add_step().await);

        let has_custom = custom_definition
            .as_ref()
            .map(|d| !d.nodes.is_empty())
            .unwrap_or(false);
        let has_linear = !linear_definition.nodes.is_empty();

        let definition = match (has_custom, has_linear) {
            (true, true) => custom_definition.expect("checked custom definition exists"),
            (true, false) => custom_definition.expect("checked custom definition exists"),
            (false, true) => linear_definition,
            (false, false) => ModuleDagDefinition::default(),
        };

        Self::stamp_definition_with_dag_version(definition)
    }

    /// Executes a precompiled DAG with DagScheduler defaults from orchestrator options.
    pub async fn execute_dag(&self, dag: Dag) -> Result<DagExecutionReport, DagError> {
        self.execute_dag_with_runtime_overrides(dag, Vec::new()).await
    }

    pub async fn execute_dag_with_runtime_overrides(
        &self,
        dag: Dag,
        runtime_overrides: Vec<DagNodeRuntimeOverride>,
    ) -> Result<DagExecutionReport, DagError> {
        self.execute_dag_with_runtime_overrides_and_dispatcher(dag, runtime_overrides, None)
            .await
    }

    pub async fn execute_dag_with_runtime_overrides_and_dispatcher(
        &self,
        dag: Dag,
        runtime_overrides: Vec<DagNodeRuntimeOverride>,
        dispatcher: Option<Arc<dyn DagNodeDispatcher>>,
    ) -> Result<DagExecutionReport, DagError> {
        let scheduler = DagScheduler::new(dag).with_runtime_overrides(runtime_overrides);
        let scheduler = if let Some(dispatcher) = dispatcher {
            scheduler.with_dispatcher(dispatcher)
        } else {
            scheduler
        };
        scheduler
            .with_options(self.options.scheduler_options.clone())
            .execute_parallel()
            .await
    }

    pub async fn execute_dag_with_routing_hints(
        &self,
        dag: Dag,
        hints: impl IntoIterator<Item = RuntimeNodeRoutingHint>,
    ) -> Result<DagExecutionReport, DagError> {
        self.execute_dag_with_runtime_overrides(dag, Self::routing_hints_to_runtime_overrides(hints))
            .await
    }

    pub async fn execute_dag_with_generate_runtime_input(
        &self,
        dag: Dag,
        target_node_id: impl AsRef<str>,
        input: &SchedulerNodeGenerateRuntimeInput,
        hints: impl IntoIterator<Item = RuntimeNodeRoutingHint>,
    ) -> Result<DagExecutionReport, DagError> {
        self.execute_dag_with_generate_runtime_input_and_dispatcher(
            dag,
            target_node_id,
            input,
            hints,
            None,
        )
        .await
    }

    pub async fn execute_dag_with_generate_runtime_input_and_dispatcher(
        &self,
        dag: Dag,
        target_node_id: impl AsRef<str>,
        input: &SchedulerNodeGenerateRuntimeInput,
        hints: impl IntoIterator<Item = RuntimeNodeRoutingHint>,
        dispatcher: Option<Arc<dyn DagNodeDispatcher>>,
    ) -> Result<DagExecutionReport, DagError> {
        let mut runtime_overrides = Self::routing_hints_to_runtime_overrides(hints);
        let target_node_id = target_node_id.as_ref().to_string();
        let runtime_input = encode_generate_runtime_input(input)?;

        match runtime_overrides
            .iter_mut()
            .find(|runtime_override| runtime_override.node_id == target_node_id)
        {
            Some(runtime_override) => {
                runtime_override.runtime_input = Some(runtime_input);
            }
            None => runtime_overrides.push(
                DagNodeRuntimeOverride::new(target_node_id).with_runtime_input(runtime_input),
            ),
        }

        runtime_overrides.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        self.execute_dag_with_runtime_overrides_and_dispatcher(dag, runtime_overrides, dispatcher)
            .await
    }

    pub async fn execute_dag_with_parser_runtime_input(
        &self,
        dag: Dag,
        target_node_id: impl AsRef<str>,
        input: &SchedulerNodeParserRuntimeInput,
        hints: impl IntoIterator<Item = RuntimeNodeRoutingHint>,
    ) -> Result<DagExecutionReport, DagError> {
        self.execute_dag_with_parser_runtime_input_and_dispatcher(
            dag,
            target_node_id,
            input,
            hints,
            None,
        )
        .await
    }

    pub async fn execute_dag_with_parser_runtime_input_and_dispatcher(
        &self,
        dag: Dag,
        target_node_id: impl AsRef<str>,
        input: &SchedulerNodeParserRuntimeInput,
        hints: impl IntoIterator<Item = RuntimeNodeRoutingHint>,
        dispatcher: Option<Arc<dyn DagNodeDispatcher>>,
    ) -> Result<DagExecutionReport, DagError> {
        let mut runtime_overrides = Self::routing_hints_to_runtime_overrides(hints);
        let target_node_id = target_node_id.as_ref().to_string();
        let runtime_input = encode_parser_runtime_input(input)?;

        match runtime_overrides
            .iter_mut()
            .find(|runtime_override| runtime_override.node_id == target_node_id)
        {
            Some(runtime_override) => {
                runtime_override.runtime_input = Some(runtime_input);
            }
            None => runtime_overrides.push(
                DagNodeRuntimeOverride::new(target_node_id).with_runtime_input(runtime_input),
            ),
        }

        runtime_overrides.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        self.execute_dag_with_runtime_overrides_and_dispatcher(dag, runtime_overrides, dispatcher)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::common::interface::{
        ModuleNodeTrait, ModuleTrait, NodeGenerateContext, NodeParseContext, SyncBoxStream,
        ToSyncBoxStream,
    };
    use crate::common::model::{
        ExecutionMark, ModuleConfig, NodeParseOutput, PayloadCodec, Request, Response,
        RuntimeNodeRoutingHint, TypedEnvelope,
    };
    use crate::engine::task::module_dag_compiler::{
        ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef,
    };
    use crate::engine::task::module_node_runtime_bridge::{
        build_legacy_generate_runtime_input_with_common,
        build_legacy_parse_runtime_input_with_common,
        decode_parser_output_payload, decode_request_batch_payload,
    };
    use crate::errors::Result;
    use crate::schedule::dag::{DagNodeExecutionPolicy, NodePlacement};
    use async_trait::async_trait;
    use uuid::Uuid;

    use super::ModuleDagOrchestrator;

    struct DummyNode;

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(
            &self,
            ctx: NodeGenerateContext<'_>,
        ) -> Result<SyncBoxStream<'static, Request>> {
            Ok(vec![Request::new(
                format!("https://example.com/{}", ctx.routing.node_key),
                "GET",
            )]
            .to_stream())
        }

        async fn parser(
            &self,
            response: Response,
            ctx: NodeParseContext<'_>,
        ) -> Result<NodeParseOutput> {
            Ok(NodeParseOutput::default()
                .with_next(crate::common::model::NodeDispatch::new(
                    "detail",
                    crate::common::model::NodeInput::new(
                        "detail",
                        TypedEnvelope::new(
                            "node.input",
                            1,
                            PayloadCodec::Json,
                            format!(
                                "{{\"status\":{},\"node\":\"{}\"}}",
                                response.status_code, ctx.routing.node_key
                            )
                            .into_bytes(),
                        ),
                    ),
                ))
                .finish())
        }
    }

    struct DummyModule;

    struct CustomOnlyModule;

    struct BothModule;

    #[async_trait]
    impl ModuleTrait for DummyModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "dummy_module_for_orchestrator"
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
            vec![Arc::new(DummyNode)]
        }
    }

    #[async_trait]
    impl ModuleTrait for CustomOnlyModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "custom_only_module_for_orchestrator"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![ModuleDagNodeDef {
                    node_id: "custom_root".to_string(),
                    node: Arc::new(DummyNode),
                    placement_override: None,
                    policy_override: None,
                    tags: vec!["custom".to_string()],
                }],
                edges: vec![],
                entry_nodes: vec!["custom_root".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }
    }

    #[async_trait]
    impl ModuleTrait for BothModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> &'static str {
            "both_module_for_orchestrator"
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

        async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
            Some(ModuleDagDefinition {
                nodes: vec![
                    ModuleDagNodeDef {
                        node_id: "custom_root".to_string(),
                        node: Arc::new(DummyNode),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                    ModuleDagNodeDef {
                        node_id: "custom_leaf".to_string(),
                        node: Arc::new(DummyNode),
                        placement_override: None,
                        policy_override: None,
                        tags: vec![],
                    },
                ],
                edges: vec![ModuleDagEdgeDef {
                    from: "custom_root".to_string(),
                    to: "custom_leaf".to_string(),
                }],
                entry_nodes: vec!["custom_root".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![Arc::new(DummyNode), Arc::new(DummyNode)]
        }
    }

    #[tokio::test]
    async fn compile_module_uses_linear_steps_when_custom_definition_missing() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_module(module)
            .await
            .expect("module compile should succeed");
        assert_eq!(dag.node_ptrs().len(), 1);
    }

    #[tokio::test]
    async fn execute_compiled_linear_steps_fails_until_runtime_bridge_exists() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_module(module)
            .await
            .expect("module compile should succeed");
        let err = orchestrator
            .execute_dag(dag)
            .await
            .expect_err("scheduler execution should fail before runtime bridge is implemented");

        assert!(err.to_string().contains("requires runtime_input"));
    }

    #[tokio::test]
    async fn execute_compiled_linear_steps_with_generate_runtime_input_succeeds() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_module(module)
            .await
            .expect("module compile should succeed");
        let node_id = dag
            .node_ptrs()
            .first()
            .expect("compiled dag should have one node")
            .id
            .clone();
        let runtime_input = build_legacy_generate_runtime_input_with_common(
            "account-a-platform-x-dummy_module_for_orchestrator",
            Uuid::now_v7(),
            &node_id,
            crate::common::model::ResolvedCommonConfig::default(),
            &ModuleConfig::default(),
            serde_json::Map::new(),
            None,
            None,
        );

        let report = orchestrator
            .execute_dag_with_generate_runtime_input(dag, &node_id, &runtime_input, Vec::new())
            .await
            .expect("scheduler execution should succeed when runtime input is supplied");
        let requests = decode_request_batch_payload(
            report
                .outputs
                .get(&node_id)
                .expect("request batch output should exist"),
        )
        .expect("request batch should decode");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, format!("https://example.com/{node_id}"));
    }

    #[tokio::test]
    async fn execute_compiled_linear_steps_with_parser_runtime_input_succeeds() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_module(module)
            .await
            .expect("module compile should succeed");
        let node_id = dag
            .node_ptrs()
            .first()
            .expect("compiled dag should have one node")
            .id
            .clone();
        let response = Response {
            id: Uuid::now_v7(),
            platform: "platform-x".to_string(),
            account: "account-a".to_string(),
            module: "dummy_module_for_orchestrator".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: br#"{\"page\":1}"#.to_vec(),
            storage_path: None,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            task_retry_times: 1,
            metadata: Default::default(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: ExecutionMark::default().with_node_id(node_id.clone()),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::now_v7(),
            request_hash: None,
            priority: Default::default(),
        };
        let runtime_input = build_legacy_parse_runtime_input_with_common(
            "account-a-platform-x-dummy_module_for_orchestrator",
            &node_id,
            crate::common::model::ResolvedCommonConfig::default(),
            Some(&ModuleConfig::default()),
            &response,
        );

        let report = orchestrator
            .execute_dag_with_parser_runtime_input(dag, &node_id, &runtime_input, Vec::new())
            .await
            .expect("scheduler parser execution should succeed when runtime input is supplied");
        let parsed = decode_parser_output_payload(
            report
                .outputs
                .get(&node_id)
                .expect("parser output should exist"),
        )
        .expect("parser output should decode");

        assert_eq!(parsed.next.len(), 1);
        assert_eq!(parsed.next[0].target_node, "detail");
        assert!(parsed.finished);
    }

    #[tokio::test]
    async fn compile_module_uses_custom_definition_when_available() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(CustomOnlyModule);

        let dag = orchestrator
            .compile_module(module)
            .await
            .expect("custom module compile should succeed");
        assert_eq!(dag.node_ptrs().len(), 1);

        let topo = dag
            .topological_sort()
            .expect("topological sort should succeed");
        assert!(topo.iter().any(|id| id == "custom_root"));
    }

    #[tokio::test]
    async fn compile_module_prefers_custom_definition_when_both_exist() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(BothModule);

        let dag = orchestrator
            .compile_module(module)
            .await
            .expect("custom-first module compile should succeed");

        let topo = dag
            .topological_sort()
            .expect("topological sort should succeed");
        assert!(topo.iter().any(|id| id == "custom_root"));
        assert!(topo.iter().any(|id| id == "custom_leaf"));
        assert!(!topo.iter().any(|id| id.starts_with("legacy_")));
    }

    #[tokio::test]
    async fn build_definition_stamps_stable_dag_version() {
        let orchestrator = ModuleDagOrchestrator::default();
        let first = orchestrator.build_definition(Arc::new(BothModule)).await;
        let second = orchestrator.build_definition(Arc::new(BothModule)).await;

        let first_version = first
            .metadata
            .get("dag_version")
            .expect("dag version should be stamped")
            .clone();
        let second_version = second
            .metadata
            .get("dag_version")
            .expect("dag version should be stamped")
            .clone();

        assert!(!first_version.is_empty());
        assert_eq!(first_version, second_version);
    }

    #[tokio::test]
    async fn compile_module_copies_dag_version_into_compiled_dag_metadata() {
        let orchestrator = ModuleDagOrchestrator::default();
        let dag = orchestrator
            .compile_module(Arc::new(BothModule))
            .await
            .expect("merged module compile should succeed");

        assert!(
            dag.metadata("dag_version").is_some(),
            "compiled dag should expose dag_version metadata"
        );
    }

    #[test]
    fn routing_hints_to_runtime_overrides_merge_per_node() {
        let policy = DagNodeExecutionPolicy {
            max_retries: 2,
            ..DagNodeExecutionPolicy::default()
        };

        let overrides = ModuleDagOrchestrator::routing_hints_to_runtime_overrides(vec![
            RuntimeNodeRoutingHint::new("detail")
                .with_placement(NodePlacement::remote("wg-a")),
            RuntimeNodeRoutingHint::new("detail").with_policy(policy.clone()),
            RuntimeNodeRoutingHint::new("list"),
        ]);

        assert_eq!(overrides.len(), 1);
        assert_eq!(overrides[0].node_id, "detail");
        assert_eq!(overrides[0].placement, Some(NodePlacement::remote("wg-a")));
        assert_eq!(overrides[0].execution_policy, Some(policy));
    }
}
