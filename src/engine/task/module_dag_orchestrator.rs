use std::sync::Arc;

use crate::common::interface::ModuleTrait;
use crate::engine::task::module_dag_compiler::{
    ModuleDagCompiler, ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef,
};
use crate::schedule::dag::{Dag, DagError, DagExecutionReport, DagScheduler, DagSchedulerOptions};

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
    pub fn new(options: ModuleDagOrchestratorOptions) -> Self {
        Self { options }
    }

    /// Compiles a DAG from explicit module definition.
    pub fn compile_definition(&self, definition: ModuleDagDefinition) -> Result<Dag, DagError> {
        ModuleDagCompiler::compile(definition)
    }

    /// Compiles a linear-compatible DAG by adapting ModuleTrait.add_step().
    pub async fn compile_linear_compat(&self, module: Arc<dyn ModuleTrait>) -> Result<Dag, DagError> {
        let steps = module.add_step().await;
        let definition = ModuleDagDefinition::from_linear_steps(steps);
        ModuleDagCompiler::compile(definition)
    }

    /// Compiles DAG from module hooks.
    ///
    /// Behavior:
    /// - only `dag_definition()`: compile custom DAG.
    /// - only `add_step()`: compile linear-compat DAG.
    /// - both present: merge into one multi-route DAG and compile.
    pub async fn compile_module(&self, module: Arc<dyn ModuleTrait>) -> Result<Dag, DagError> {
        let custom_definition = module.dag_definition().await;
        let linear_definition = ModuleDagDefinition::from_linear_steps(module.add_step().await);

        let has_custom = custom_definition
            .as_ref()
            .map(|definition| !definition.nodes.is_empty())
            .unwrap_or(false);
        let has_linear = !linear_definition.nodes.is_empty();

        let definition = match (has_custom, has_linear) {
            (true, true) => {
                let custom = custom_definition.expect("checked custom definition exists");
                Self::merge_definitions(custom, linear_definition)
            }
            (true, false) => custom_definition.expect("checked custom definition exists"),
            (false, true) => linear_definition,
            (false, false) => ModuleDagDefinition::default(),
        };

        ModuleDagCompiler::compile(definition)
    }

    fn merge_definitions(
        mut custom: ModuleDagDefinition,
        linear: ModuleDagDefinition,
    ) -> ModuleDagDefinition {
        // Namespace linear-compat node ids when merged to avoid id collision with custom DAG.
        let rename = |node_id: &str| format!("legacy_{}", node_id);

        let mut merged_nodes = custom.nodes;
        let mut merged_edges = custom.edges;
        let mut merged_entries = custom.entry_nodes;

        merged_nodes.extend(linear.nodes.into_iter().map(|node| ModuleDagNodeDef {
            node_id: rename(&node.node_id),
            node: node.node,
            placement_override: node.placement_override,
            policy_override: node.policy_override,
            tags: node.tags,
        }));
        merged_edges.extend(linear.edges.into_iter().map(|edge| ModuleDagEdgeDef {
            from: rename(&edge.from),
            to: rename(&edge.to),
        }));
        merged_entries.extend(linear.entry_nodes.into_iter().map(|entry| rename(&entry)));

        custom.metadata.insert(
            "merged_with_linear_compat".to_string(),
            "true".to_string(),
        );

        ModuleDagDefinition {
            nodes: merged_nodes,
            edges: merged_edges,
            entry_nodes: merged_entries,
            default_policy: custom.default_policy,
            metadata: custom.metadata,
        }
    }

    /// Builds a merged `ModuleDagDefinition` from a module's hooks without compiling.
    ///
    /// Same merge semantics as `compile_module`, but returns the definition so it can be
    /// fed directly into `ModuleDagProcessor::init_from_definition`.
    pub async fn build_definition(&self, module: Arc<dyn ModuleTrait>) -> ModuleDagDefinition {
        let custom_definition = module.dag_definition().await;
        let linear_definition = ModuleDagDefinition::from_linear_steps(module.add_step().await);

        let has_custom = custom_definition
            .as_ref()
            .map(|d| !d.nodes.is_empty())
            .unwrap_or(false);
        let has_linear = !linear_definition.nodes.is_empty();

        match (has_custom, has_linear) {
            (true, true) => {
                let custom = custom_definition.expect("checked custom definition exists");
                Self::merge_definitions(custom, linear_definition)
            }
            (true, false) => custom_definition.expect("checked custom definition exists"),
            (false, true) => linear_definition,
            (false, false) => ModuleDagDefinition::default(),
        }
    }

    /// Executes a precompiled DAG with DagScheduler defaults from orchestrator options.
    pub async fn execute_dag(&self, dag: Dag) -> Result<DagExecutionReport, DagError> {
        DagScheduler::new(dag)
            .with_options(self.options.scheduler_options.clone())
            .execute_parallel()
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Map;

    use crate::common::interface::{
        ModuleNodeTrait, ModuleTrait, SyncBoxStream, ToSyncBoxStream,
    };
    use crate::common::model::login_info::LoginInfo;
    use crate::common::model::message::TaskOutputEvent;
    use crate::common::model::{ModuleConfig, Request, Response};
    use crate::engine::task::module_dag_compiler::{
        ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef,
    };
    use crate::errors::Result;

    use super::ModuleDagOrchestrator;

    struct DummyNode;

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(
            &self,
            _config: Arc<ModuleConfig>,
            _params: Map<String, serde_json::Value>,
            _login_info: Option<LoginInfo>,
        ) -> Result<SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _config: Option<Arc<ModuleConfig>>,
        ) -> Result<TaskOutputEvent> {
            Ok(TaskOutputEvent::default())
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

        fn name(&self) -> String {
            "dummy_module_for_orchestrator".to_string()
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

        fn name(&self) -> String {
            "custom_only_module_for_orchestrator".to_string()
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

        fn name(&self) -> String {
            "both_module_for_orchestrator".to_string()
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
    async fn compile_linear_compat_succeeds() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_linear_compat(module)
            .await
            .expect("linear compat compile should succeed");
        assert_eq!(dag.node_ptrs().len(), 1);
    }

    #[tokio::test]
    async fn execute_compiled_linear_compat_succeeds() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_linear_compat(module)
            .await
            .expect("linear compat compile should succeed");
        let report = orchestrator
            .execute_dag(dag)
            .await
            .expect("execution should succeed");

        assert!(report.outputs.contains_key("step_0"));
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

        let topo = dag.topological_sort().expect("topological sort should succeed");
        assert!(topo.iter().any(|id| id == "custom_root"));
    }

    #[tokio::test]
    async fn compile_module_merges_custom_and_linear_when_both_exist() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(BothModule);

        let dag = orchestrator
            .compile_module(module)
            .await
            .expect("merged module compile should succeed");

        let topo = dag.topological_sort().expect("topological sort should succeed");
        assert!(topo.iter().any(|id| id == "custom_root"));
        assert!(topo.iter().any(|id| id == "legacy_step_0"));
        assert!(topo.iter().any(|id| id == "legacy_step_1"));
    }
}
