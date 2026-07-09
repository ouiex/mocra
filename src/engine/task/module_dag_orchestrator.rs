use std::sync::Arc;

use crate::common::interface::ModuleTrait;
use crate::engine::task::module_dag_compiler::{
    ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef,
};

/// Builds a [`ModuleDagDefinition`] from a module's `dag_definition()` / `add_step()` hooks,
/// ready to feed into [`ModuleDagProcessor::init_from_definition`](crate::engine::task::module_dag_processor::ModuleDagProcessor::init_from_definition).
///
/// This is the single module-DAG assembly path; execution then runs through the
/// queue-backed `ModuleDagProcessor`.
pub struct ModuleDagOrchestrator;

impl ModuleDagOrchestrator {
    /// Builds a merged `ModuleDagDefinition` from a module's hooks.
    ///
    /// - only `dag_definition()`: use the custom graph.
    /// - only `add_step()`: build a linear-compat graph.
    /// - both present: merge into one multi-route graph (linear nodes namespaced `legacy_*`).
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

        custom
            .metadata
            .insert("merged_with_linear_compat".to_string(), "true".to_string());

        ModuleDagDefinition {
            nodes: merged_nodes,
            edges: merged_edges,
            entry_nodes: merged_entries,
            default_policy: custom.default_policy,
            metadata: custom.metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Map;

    use super::ModuleDagOrchestrator;
    use crate::common::interface::{ModuleNodeTrait, ModuleTrait, SyncBoxStream, ToSyncBoxStream};
    use crate::common::model::login_info::LoginInfo;
    use crate::common::model::message::TaskOutputEvent;
    use crate::common::model::{ModuleConfig, Request, Response};
    use crate::engine::task::module_dag_compiler::{ModuleDagDefinition, ModuleDagNodeDef};
    use crate::errors::Result;

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

    struct BothModule;

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
                nodes: vec![ModuleDagNodeDef {
                    node_id: "custom_root".to_string(),
                    node: Arc::new(DummyNode),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                }],
                edges: vec![],
                entry_nodes: vec!["custom_root".to_string()],
                default_policy: None,
                metadata: Default::default(),
            })
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![Arc::new(DummyNode), Arc::new(DummyNode)]
        }
    }

    /// The real merge path: custom graph + linear steps → one definition with the
    /// linear nodes namespaced `legacy_*` (previously covered via the removed shadow compiler).
    #[tokio::test]
    async fn build_definition_merges_custom_and_linear() {
        let def = ModuleDagOrchestrator
            .build_definition(Arc::new(BothModule))
            .await;

        assert!(def.nodes.iter().any(|n| n.node_id == "custom_root"));
        assert_eq!(
            def.nodes
                .iter()
                .filter(|n| n.node_id.starts_with("legacy_"))
                .count(),
            2
        );
        assert_eq!(
            def.metadata
                .get("merged_with_linear_compat")
                .map(|s| s.as_str()),
            Some("true")
        );
    }
}
