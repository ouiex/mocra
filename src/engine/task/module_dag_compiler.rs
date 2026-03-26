use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use crate::common::interface::ModuleNodeTrait;
use crate::engine::task::module_node_dag_adapter::ModuleNodeDagAdapter;
use crate::schedule::dag::{
    Dag, DagError, DagNodeExecutionPolicy, DagNodePtr, NodePlacement,
};

#[derive(Clone)]
pub struct ModuleDagNodeDef {
    pub node_id: String,
    pub node: Arc<dyn ModuleNodeTrait>,
    pub placement_override: Option<NodePlacement>,
    pub policy_override: Option<DagNodeExecutionPolicy>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ModuleDagEdgeDef {
    pub from: String,
    pub to: String,
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
    pub fn from_linear_steps(steps: Vec<Arc<dyn ModuleNodeTrait>>) -> Self {
        let mut nodes = Vec::with_capacity(steps.len());
        let mut edges = Vec::with_capacity(steps.len().saturating_sub(1));

        for (idx, step) in steps.into_iter().enumerate() {
            let node_id = format!("step_{idx}");
            if idx > 0 {
                edges.push(ModuleDagEdgeDef {
                    from: format!("step_{}", idx - 1),
                    to: node_id.clone(),
                });
            }
            nodes.push(ModuleDagNodeDef {
                node_id,
                node: step,
                placement_override: None,
                policy_override: None,
                tags: Vec::new(),
            });
        }

        let entry_nodes = if nodes.is_empty() {
            Vec::new()
        } else {
            vec!["step_0".to_string()]
        };

        Self {
            nodes,
            edges,
            entry_nodes,
            default_policy: None,
            metadata: HashMap::new(),
        }
    }
}

#[derive(Default)]
pub struct ModuleDagCompiler;

impl ModuleDagCompiler {
    pub fn compile(definition: ModuleDagDefinition) -> Result<Dag, DagError> {
        if definition.nodes.is_empty() {
            return Err(DagError::EmptyGraph);
        }

        let mut seen = BTreeSet::new();
        let mut node_map: HashMap<String, ModuleDagNodeDef> = HashMap::new();
        for node in definition.nodes {
            if Dag::is_control_node(&node.node_id) {
                return Err(DagError::ReservedControlNode(node.node_id));
            }
            if !seen.insert(node.node_id.clone()) {
                return Err(DagError::DuplicateNode(node.node_id));
            }
            node_map.insert(node.node_id.clone(), node);
        }

        for edge in &definition.edges {
            if !node_map.contains_key(&edge.from) {
                return Err(DagError::NodeNotFound(edge.from.clone()));
            }
            if !node_map.contains_key(&edge.to) {
                return Err(DagError::NodeNotFound(edge.to.clone()));
            }
        }

        for entry in &definition.entry_nodes {
            if !node_map.contains_key(entry) {
                return Err(DagError::NodeNotFound(entry.clone()));
            }
        }

        let mut predecessors: HashMap<String, Vec<String>> = HashMap::new();
        let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();
        for node_id in node_map.keys() {
            predecessors.insert(node_id.clone(), Vec::new());
            outgoing.insert(node_id.clone(), Vec::new());
        }

        for edge in &definition.edges {
            predecessors
                .get_mut(&edge.to)
                .expect("edge destination is pre-validated")
                .push(edge.from.clone());
            outgoing
                .get_mut(&edge.from)
                .expect("edge source is pre-validated")
                .push(edge.to.clone());
        }

        let mut indegree: HashMap<String, usize> = predecessors
            .iter()
            .map(|(node, preds)| (node.clone(), preds.len()))
            .collect();

        let mut zero_indegree: Vec<String> = indegree
            .iter()
            .filter_map(|(node, degree)| if *degree == 0 { Some(node.clone()) } else { None })
            .collect();
        zero_indegree.sort();

        let mut queue: VecDeque<String> = zero_indegree.into_iter().collect();
        let mut dag = Dag::new();
        let mut pointers: HashMap<String, DagNodePtr> = HashMap::new();
        let mut visited = 0usize;

        while let Some(node_id) = queue.pop_front() {
            let node_def = node_map
                .remove(&node_id)
                .ok_or_else(|| DagError::NodeNotFound(node_id.clone()))?;

            let pred_ptrs: Vec<DagNodePtr> = predecessors
                .get(&node_id)
                .expect("node predecessors are initialized")
                .iter()
                .map(|pid| {
                    pointers
                        .get(pid)
                        .cloned()
                        .ok_or_else(|| DagError::PrecedingNodeNotFound(pid.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let adapter = Arc::new(ModuleNodeDagAdapter::new(node_def.node));
            let ptr = if pred_ptrs.is_empty() {
                dag.add_node_with_id(None, &node_id, adapter)?
            } else {
                dag.add_node_with_id(Some(&pred_ptrs), &node_id, adapter)?
            };

            if let Some(policy) = node_def
                .policy_override
                .or_else(|| definition.default_policy.clone())
            {
                dag.set_node_execution_policy(&ptr, policy)?;
            }
            if let Some(placement) = node_def.placement_override {
                dag.set_node_placement(&ptr, placement)?;
            }

            pointers.insert(node_id.clone(), ptr);
            visited += 1;

            if let Some(next_nodes) = outgoing.get(&node_id) {
                for next in next_nodes {
                    if let Some(degree) = indegree.get_mut(next) {
                        *degree = degree.saturating_sub(1);
                        if *degree == 0 {
                            queue.push_back(next.clone());
                        }
                    }
                }
            }
        }

        if visited != indegree.len() {
            return Err(DagError::CycleDetected);
        }

        dag.topological_sort()?;
        Ok(dag)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Map;

    use crate::common::interface::{ModuleNodeTrait, SyncBoxStream, ToSyncBoxStream};
    use crate::common::model::login_info::LoginInfo;
    use crate::common::model::message::TaskOutputEvent;
    use crate::common::model::{ModuleConfig, Request, Response};
    use crate::errors::Result;
    use crate::schedule::dag::{DagError, DagNodeExecutionPolicy, NodePlacement};

    use super::{ModuleDagCompiler, ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef};

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
        assert_eq!(definition.entry_nodes, vec!["step_0".to_string()]);
        assert_eq!(definition.edges[0].from, "step_0");
        assert_eq!(definition.edges[0].to, "step_1");
        assert_eq!(definition.edges[1].from, "step_1");
        assert_eq!(definition.edges[1].to, "step_2");
    }

    #[test]
    fn compile_linear_definition_succeeds() {
        let def = ModuleDagDefinition::from_linear_steps(vec![dummy_node(), dummy_node()]);
        let dag = ModuleDagCompiler::compile(def).expect("linear dag should compile");

        assert_eq!(dag.node_ptrs().len(), 2);
        let topo = dag.topological_sort().expect("topo sort should succeed");
        assert!(topo.iter().any(|id| id == "step_0"));
        assert!(topo.iter().any(|id| id == "step_1"));
    }

    #[test]
    fn compile_rejects_duplicate_node_ids() {
        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "n1".to_string(),
                    node: dummy_node(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "n1".to_string(),
                    node: dummy_node(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![],
            entry_nodes: vec!["n1".to_string()],
            default_policy: None,
            metadata: Default::default(),
        };

        match ModuleDagCompiler::compile(def) {
            Ok(_) => panic!("duplicate id should fail"),
            Err(err) => match err {
            DagError::DuplicateNode(id) => assert_eq!(id, "n1"),
            other => panic!("unexpected error: {other:?}"),
            },
        }
    }

    #[test]
    fn compile_rejects_unknown_edge_nodes() {
        let def = ModuleDagDefinition {
            nodes: vec![ModuleDagNodeDef {
                node_id: "n1".to_string(),
                node: dummy_node(),
                placement_override: None,
                policy_override: None,
                tags: vec![],
            }],
            edges: vec![ModuleDagEdgeDef {
                from: "n1".to_string(),
                to: "missing".to_string(),
            }],
            entry_nodes: vec!["n1".to_string()],
            default_policy: None,
            metadata: Default::default(),
        };

        match ModuleDagCompiler::compile(def) {
            Ok(_) => panic!("unknown edge endpoint should fail"),
            Err(err) => match err {
            DagError::NodeNotFound(id) => assert_eq!(id, "missing"),
            other => panic!("unexpected error: {other:?}"),
            },
        }
    }

    #[test]
    fn compile_detects_cycle() {
        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "a".to_string(),
                    node: dummy_node(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "b".to_string(),
                    node: dummy_node(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![
                ModuleDagEdgeDef {
                    from: "a".to_string(),
                    to: "b".to_string(),
                },
                ModuleDagEdgeDef {
                    from: "b".to_string(),
                    to: "a".to_string(),
                },
            ],
            entry_nodes: vec![],
            default_policy: None,
            metadata: Default::default(),
        };

        match ModuleDagCompiler::compile(def) {
            Ok(_) => panic!("cycle should fail"),
            Err(err) => match err {
            DagError::CycleDetected => {}
            other => panic!("unexpected error: {other:?}"),
            },
        }
    }

    #[test]
    fn compile_applies_policy_and_placement_overrides() {
        let policy = DagNodeExecutionPolicy {
            max_retries: 3,
            timeout_ms: Some(1500),
            retry_backoff_ms: 200,
            idempotency_key: Some("m-node-key".to_string()),
            ..DagNodeExecutionPolicy::default()
        };

        let def = ModuleDagDefinition {
            nodes: vec![ModuleDagNodeDef {
                node_id: "n1".to_string(),
                node: dummy_node(),
                placement_override: Some(NodePlacement::Remote {
                    worker_group: "wg-a".to_string(),
                }),
                policy_override: Some(policy.clone()),
                tags: vec![],
            }],
            edges: vec![],
            entry_nodes: vec!["n1".to_string()],
            default_policy: None,
            metadata: Default::default(),
        };

        let dag = ModuleDagCompiler::compile(def).expect("dag should compile");
        let ptr = dag
            .node_ptrs()
            .into_iter()
            .find(|n| n.id == "n1")
            .expect("n1 should exist");
        let node = dag.get_node(&ptr).expect("node should be readable");

        match &node.placement {
            NodePlacement::Remote { worker_group } => assert_eq!(worker_group, "wg-a"),
            other => panic!("unexpected placement: {other:?}"),
        }
        assert_eq!(node.execution_policy.max_retries, 3);
        assert_eq!(node.execution_policy.timeout_ms, Some(1500));
        assert_eq!(node.execution_policy.retry_backoff_ms, 200);
        assert_eq!(
            node.execution_policy.idempotency_key.as_deref(),
            Some("m-node-key")
        );
    }
}
