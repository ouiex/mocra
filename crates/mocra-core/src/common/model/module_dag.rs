//! Module DAG definition types — the graph a [`ModuleTrait`](crate::common::interface::ModuleTrait)
//! describes via `dag_definition()` / `add_step()`.
//!
//! These live in `common` (not `engine`) so the `ModuleTrait` contract does not depend on the
//! engine — the engine's `ModuleDagProcessor` consumes them. Node-execution policy / placement
//! types come from the standalone `mocra-dag` crate.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use mocra_dag::{DagNodeExecutionPolicy, NodePlacement};

use crate::common::interface::ModuleNodeTrait;

#[derive(Clone)]
pub struct ModuleDagNodeDef {
    pub node_id: String,
    pub node: Arc<dyn ModuleNodeTrait>,
    pub placement_override: Option<NodePlacement>,
    pub policy_override: Option<DagNodeExecutionPolicy>,
    pub tags: Vec<String>,
}
impl ModuleDagNodeDef {
    pub fn new(node: Arc<dyn ModuleNodeTrait>) -> Self {
        let key = node.stable_node_key();
        let node_id = if key.is_empty() {
            uuid::Uuid::now_v7().to_string()
        } else {
            key.to_string()
        };
        Self {
            node_id,
            node,
            placement_override: None,
            policy_override: None,
            tags: Vec::new(),
        }
    }

    /// Overrides the auto-generated node ID with an explicit stable identifier.
    ///
    /// Use this when you need multiple instances of the same node type in one DAG, or
    /// when you cannot implement `stable_node_key()` on the node struct directly.
    /// The ID must be unique within the DAG.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = id.into();
        self
    }
}

#[derive(Debug, Clone)]
pub struct ModuleDagEdgeDef {
    pub from: String,
    pub to: String,
}
impl ModuleDagEdgeDef {
    pub fn new(from: &ModuleDagNodeDef, to: &ModuleDagNodeDef) -> Self {
        Self {
            from: from.node_id.clone(),
            to: to.node_id.clone(),
        }
    }
}

#[derive(Clone, Default)]
pub struct ModuleDagDefinition {
    pub nodes: Vec<ModuleDagNodeDef>,
    pub edges: Vec<ModuleDagEdgeDef>,
    pub entry_nodes: Vec<String>,
    pub default_policy: Option<DagNodeExecutionPolicy>,
    pub metadata: HashMap<String, String>,
}

impl ModuleDagDefinition {
    /// Returns a fluent builder.
    ///
    /// ```ignore
    /// let dag = ModuleDagDefinition::builder()
    ///     .edge(&login_node, &cate_list_node)
    ///     .edge(&cate_list_node, &brand_rank_downloader)
    ///     .edge(&cate_list_node, &goods_cate_downloader)
    ///     .edge(&brand_rank_downloader, &download_url_node)
    ///     .edge(&goods_cate_downloader, &download_url_node)
    ///     .edge(&download_url_node, &download_file_node)
    ///     .build();
    /// ```
    ///
    /// Nodes are collected automatically from edges in first-seen order.
    /// Entry nodes (those with no incoming edges) are derived automatically.
    /// Isolated nodes (no edges at all) can be added with `.node()`.
    pub fn builder() -> ModuleDagBuilder {
        ModuleDagBuilder::new()
    }

    pub fn from_linear_steps(steps: Vec<Arc<dyn ModuleNodeTrait>>) -> Self {
        let mut nodes = Vec::with_capacity(steps.len());
        let mut edges = Vec::with_capacity(steps.len().saturating_sub(1));

        for step in steps {
            nodes.push(ModuleDagNodeDef::new(step));
        }
        for idx in 1..nodes.len() {
            edges.push(ModuleDagEdgeDef::new(&nodes[idx - 1], &nodes[idx]));
        }

        let entry_nodes = nodes
            .first()
            .map(|n| vec![n.node_id.clone()])
            .unwrap_or_default();

        Self {
            nodes,
            edges,
            entry_nodes,
            default_policy: None,
            metadata: HashMap::new(),
        }
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Fluent builder for `ModuleDagDefinition`.
///
/// Collects nodes automatically from `.edge()` calls. Entry nodes and the
/// `nodes` list are both derived automatically when `.build()` is called, so
/// callers only need to describe the edges between node definitions.
pub struct ModuleDagBuilder {
    /// Nodes in insertion order, keyed by node_id to deduplicate.
    nodes: IndexMap<String, ModuleDagNodeDef>,
    edges: Vec<ModuleDagEdgeDef>,
    default_policy: Option<DagNodeExecutionPolicy>,
}

impl ModuleDagBuilder {
    pub fn new() -> Self {
        Self {
            nodes: IndexMap::new(),
            edges: Vec::new(),
            default_policy: None,
        }
    }

    /// Registers a directed edge `from → to`.
    ///
    /// Both nodes are inserted into the node registry on first encounter
    /// (subsequent calls with the same `node_id` are silently ignored so
    /// the same `ModuleDagNodeDef` reference is safe to reuse across calls).
    pub fn edge(mut self, from: &ModuleDagNodeDef, to: &ModuleDagNodeDef) -> Self {
        self.nodes
            .entry(from.node_id.clone())
            .or_insert_with(|| from.clone());
        self.nodes
            .entry(to.node_id.clone())
            .or_insert_with(|| to.clone());
        self.edges.push(ModuleDagEdgeDef {
            from: from.node_id.clone(),
            to: to.node_id.clone(),
        });
        self
    }

    /// Registers an isolated node (no edges). Useful for single-node DAGs.
    pub fn node(mut self, node: &ModuleDagNodeDef) -> Self {
        self.nodes
            .entry(node.node_id.clone())
            .or_insert_with(|| node.clone());
        self
    }

    /// Sets a default execution policy applied to every node.
    pub fn default_policy(mut self, policy: DagNodeExecutionPolicy) -> Self {
        self.default_policy = Some(policy);
        self
    }

    /// Consumes the builder and produces a `ModuleDagDefinition`.
    ///
    /// Entry nodes are derived as any node that never appears as a `to` in an edge.
    pub fn build(self) -> ModuleDagDefinition {
        let to_set: std::collections::HashSet<&str> =
            self.edges.iter().map(|e| e.to.as_str()).collect();

        let entry_nodes: Vec<String> = self
            .nodes
            .keys()
            .filter(|id| !to_set.contains(id.as_str()))
            .cloned()
            .collect();

        ModuleDagDefinition {
            nodes: self.nodes.into_values().collect(),
            edges: self.edges,
            entry_nodes,
            default_policy: self.default_policy,
            metadata: HashMap::new(),
        }
    }
}

impl Default for ModuleDagBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Map;

    use super::ModuleDagDefinition;
    use crate::common::interface::{ModuleNodeTrait, SyncBoxStream, ToSyncBoxStream};
    use crate::common::model::login_info::LoginInfo;
    use crate::common::model::message::TaskOutputEvent;
    use crate::common::model::{ModuleConfig, Request, Response};
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

    fn dummy_node() -> Arc<dyn ModuleNodeTrait> {
        Arc::new(DummyNode)
    }

    #[test]
    fn from_linear_steps_builds_expected_shape() {
        let steps = vec![dummy_node(), dummy_node(), dummy_node()];
        let definition = ModuleDagDefinition::from_linear_steps(steps);

        assert_eq!(definition.nodes.len(), 3);
        assert_eq!(definition.edges.len(), 2);
        assert_eq!(
            definition.entry_nodes,
            vec![definition.nodes[0].node_id.clone()]
        );
        assert_eq!(definition.edges[0].from, definition.nodes[0].node_id);
        assert_eq!(definition.edges[0].to, definition.nodes[1].node_id);
        assert_eq!(definition.edges[1].from, definition.nodes[1].node_id);
        assert_eq!(definition.edges[1].to, definition.nodes[2].node_id);
    }
}
