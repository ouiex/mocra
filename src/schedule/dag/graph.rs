use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

use super::types::{
    DagError, DagNodeExecutionPolicy, DagNodeRecord, DagNodeStatus, DagNodeTrait, NodePlacement,
};

pub type DagNodePtr = Arc<DagNodeRecord>;

pub struct DagChainBuilder<'a> {
    dag: &'a mut Dag,
    last: DagNodePtr,
}

impl<'a> DagChainBuilder<'a> {
    pub fn add_chain_node(
        mut self,
        current_node: Arc<dyn DagNodeTrait>,
    ) -> Result<Self, DagError> {
        let next = self
            .dag
            .add_node(Some(&[self.last.clone()]), current_node)?;
        self.last = next;
        Ok(self)
    }

    pub fn add_chain_node_with_id(
        mut self,
        node_id: impl AsRef<str>,
        current_node: Arc<dyn DagNodeTrait>,
    ) -> Result<Self, DagError> {
        let next = self
            .dag
            .add_node_with_id(Some(&[self.last.clone()]), node_id, current_node)?;
        self.last = next;
        Ok(self)
    }

    pub fn last(&self) -> &DagNodePtr {
        &self.last
    }

    pub fn finish(self) -> DagNodePtr {
        self.last
    }
}

#[derive(Clone)]
pub struct Dag {
    pub(crate) nodes: HashMap<String, DagNodeRecord>,
    pub(crate) edges: HashMap<String, Vec<String>>,
}

impl Dag {
    pub const CONTROL_START_NODE: &'static str = "__start__";
    pub const CONTROL_END_NODE: &'static str = "__end__";

    pub fn new() -> Self {
        let mut dag = Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
        };

        dag.insert_control_node(Self::CONTROL_START_NODE);
        dag.insert_control_node(Self::CONTROL_END_NODE);
        dag.connect(Self::CONTROL_START_NODE, Self::CONTROL_END_NODE)
            .ok();

        dag
    }

    fn insert_control_node(&mut self, node_id: &str) {
        if self.nodes.contains_key(node_id) {
            return;
        }
        self.nodes.insert(
            node_id.to_string(),
            DagNodeRecord {
                id: node_id.to_string(),
                predecessors: Vec::new(),
                status: DagNodeStatus::Pending,
                placement: NodePlacement::Local,
                execution_policy: DagNodeExecutionPolicy::default(),
                executor: None,
                result: None,
                metadata: HashMap::new(),
                error: None,
            },
        );
        self.edges.entry(node_id.to_string()).or_default();
    }

    pub fn is_control_node(node_id: &str) -> bool {
        node_id == Self::CONTROL_START_NODE || node_id == Self::CONTROL_END_NODE
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.len() <= 2
    }

    pub fn node_ptrs(&self) -> Vec<DagNodePtr> {
        self.nodes
            .values()
            .filter(|node| !Self::is_control_node(&node.id))
            .cloned()
            .map(Arc::new)
            .collect()
    }

    pub fn control_start_ptr(&self) -> DagNodePtr {
        Arc::new(
            self.nodes
                .get(Self::CONTROL_START_NODE)
                .cloned()
                .expect("control start node should always exist"),
        )
    }

    pub fn control_end_ptr(&self) -> DagNodePtr {
        Arc::new(
            self.nodes
                .get(Self::CONTROL_END_NODE)
                .cloned()
                .expect("control end node should always exist"),
        )
    }

    pub fn get_node(&self, node: &DagNodePtr) -> Result<DagNodePtr, DagError> {
        self.nodes
            .get(&node.id)
            .cloned()
            .map(Arc::new)
            .ok_or_else(|| DagError::NodeNotFound(node.id.clone()))
    }

    pub fn add_node(
        &mut self,
        preceding_nodes: Option<&[DagNodePtr]>,
        current_node: Arc<dyn DagNodeTrait>,
    ) -> Result<DagNodePtr, DagError> {
        let generated_id = Uuid::now_v7().to_string();
        self.add_node_with_id(preceding_nodes, &generated_id, current_node)
    }

    pub fn add_chain_node(
        &mut self,
        current_node: Arc<dyn DagNodeTrait>,
    ) -> Result<DagChainBuilder<'_>, DagError> {
        let first = self.add_node(None, current_node)?;
        Ok(DagChainBuilder {
            dag: self,
            last: first,
        })
    }

    pub fn add_chain_node_with_id(
        &mut self,
        node_id: impl AsRef<str>,
        current_node: Arc<dyn DagNodeTrait>,
    ) -> Result<DagChainBuilder<'_>, DagError> {
        let first = self.add_node_with_id(None, node_id, current_node)?;
        Ok(DagChainBuilder {
            dag: self,
            last: first,
        })
    }

    pub fn add_node_with_id(
        &mut self,
        preceding_nodes: Option<&[DagNodePtr]>,
        node_id: impl AsRef<str>,
        current_node: Arc<dyn DagNodeTrait>,
    ) -> Result<DagNodePtr, DagError> {
        let node_id = node_id.as_ref().to_string();

        if Self::is_control_node(&node_id) {
            return Err(DagError::ReservedControlNode(node_id));
        }
        if self.nodes.contains_key(&node_id) {
            return Err(DagError::DuplicateNode(node_id));
        }

        let preceding_ids: Vec<String> = match preceding_nodes {
            None => vec![Self::CONTROL_START_NODE.to_string()],
            Some(list) if list.is_empty() => vec![Self::CONTROL_START_NODE.to_string()],
            Some(list) => {
                let mut ids = Vec::with_capacity(list.len());
                for p in list {
                    let pid = p.id.clone();
                    if !self.nodes.contains_key(&pid) {
                        return Err(DagError::PrecedingNodeNotFound(pid));
                    }
                    if pid == Self::CONTROL_END_NODE {
                        return Err(DagError::InvalidPrecedingControlNode(pid));
                    }
                    ids.push(pid);
                }
                ids
            }
        };

        self.nodes.insert(
            node_id.clone(),
            DagNodeRecord {
                id: node_id.clone(),
                predecessors: Vec::new(),
                status: DagNodeStatus::Pending,
                placement: NodePlacement::Local,
                execution_policy: DagNodeExecutionPolicy::default(),
                executor: Some(current_node),
                result: None,
                metadata: HashMap::new(),
                error: None,
            },
        );
        self.edges.entry(node_id.clone()).or_default();

        for predecessor in &preceding_ids {
            self.connect(predecessor, &node_id)?;
            self.disconnect(predecessor, Self::CONTROL_END_NODE);
        }

        self.connect(&node_id, Self::CONTROL_END_NODE)?;

        self
            .nodes
            .get(&node_id)
            .cloned()
            .map(Arc::new)
            .ok_or_else(|| DagError::NodeNotFound(node_id))
    }

    fn connect(&mut self, from: impl AsRef<str>, to: impl AsRef<str>) -> Result<(), DagError> {
        let from = from.as_ref().to_string();
        let to = to.as_ref().to_string();

        if !self.nodes.contains_key(&from) {
            return Err(DagError::NodeNotFound(from));
        }
        if !self.nodes.contains_key(&to) {
            return Err(DagError::NodeNotFound(to));
        }

        let tos = self.edges.entry(from.clone()).or_default();
        if !tos.iter().any(|x| x == &to) {
            tos.push(to.clone());
        }

        if let Some(node) = self.nodes.get_mut(&to) {
            if !node.predecessors.iter().any(|p| p == &from) {
                node.predecessors.push(from);
            }
        }

        Ok(())
    }

    fn disconnect(&mut self, from: impl AsRef<str>, to: impl AsRef<str>) {
        let from = from.as_ref();
        let to = to.as_ref();
        if let Some(tos) = self.edges.get_mut(from) {
            tos.retain(|x| x != to);
        }
        if let Some(node) = self.nodes.get_mut(to) {
            node.predecessors.retain(|p| p != from);
        }
    }

    pub fn set_node_placement(
        &mut self,
        node: &DagNodePtr,
        placement: NodePlacement,
    ) -> Result<(), DagError> {
        if Dag::is_control_node(&node.id) {
            return Err(DagError::ReservedControlNode(node.id.clone()));
        }
        let record = self
            .nodes
            .get_mut(&node.id)
            .ok_or_else(|| DagError::NodeNotFound(node.id.clone()))?;
        record.placement = placement;
        Ok(())
    }

    pub fn set_node_execution_policy(
        &mut self,
        node: &DagNodePtr,
        policy: DagNodeExecutionPolicy,
    ) -> Result<(), DagError> {
        if Dag::is_control_node(&node.id) {
            return Err(DagError::ReservedControlNode(node.id.clone()));
        }
        let record = self
            .nodes
            .get_mut(&node.id)
            .ok_or_else(|| DagError::NodeNotFound(node.id.clone()))?;
        record.execution_policy = policy;
        Ok(())
    }

    pub fn successors(&self, node: &DagNodePtr) -> Result<Vec<DagNodePtr>, DagError> {
        let id = &node.id;
        if !self.nodes.contains_key(id) {
            return Err(DagError::NodeNotFound(id.to_string()));
        }
        Ok(self
            .edges
            .get(id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|nid| self.nodes.get(&nid).cloned())
            .map(Arc::new)
            .collect())
    }

    pub fn topological_sort(&self) -> Result<Vec<String>, DagError> {
        if self.nodes.is_empty() {
            return Err(DagError::EmptyGraph);
        }

        let mut indegree: HashMap<String, usize> = self
            .nodes
            .keys()
            .map(|n| (n.clone(), 0usize))
            .collect();

        for tos in self.edges.values() {
            for to in tos {
                if let Some(v) = indegree.get_mut(to) {
                    *v += 1;
                }
            }
        }

        let mut queue: BinaryHeap<Reverse<String>> = indegree
            .iter()
            .filter_map(|(n, deg)| if *deg == 0 { Some(Reverse(n.clone())) } else { None })
            .collect();

        let mut ordered = Vec::with_capacity(self.nodes.len());
        while let Some(Reverse(node)) = queue.pop() {
            ordered.push(node.clone());
            if let Some(next_nodes) = self.edges.get(&node) {
                let mut sorted_next = next_nodes.clone();
                sorted_next.sort();
                for next in sorted_next {
                    if let Some(v) = indegree.get_mut(&next) {
                        *v -= 1;
                        if *v == 0 {
                            queue.push(Reverse(next.clone()));
                        }
                    }
                }
            }
        }

        if ordered.len() != self.nodes.len() {
            return Err(DagError::CycleDetected);
        }
        Ok(ordered)
    }

    pub(crate) fn set_status(&mut self, node_id: &str, to: DagNodeStatus) -> Result<(), DagError> {
        let node = self
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| DagError::NodeNotFound(node_id.to_string()))?;
        let from = node.status.clone();

        let valid = matches!(
            (&from, &to),
            (DagNodeStatus::Pending, DagNodeStatus::Ready)
                | (DagNodeStatus::Ready, DagNodeStatus::Running)
                | (DagNodeStatus::Running, DagNodeStatus::Pending)
                | (DagNodeStatus::Running, DagNodeStatus::Succeeded)
                | (DagNodeStatus::Running, DagNodeStatus::Failed)
        );

        if !valid {
            return Err(DagError::InvalidStateTransition {
                node_id: node_id.to_string(),
                from,
                to,
            });
        }

        node.status = to;
        Ok(())
    }

    pub(crate) fn reset_runtime(&mut self) {
        for node in self.nodes.values_mut() {
            node.status = DagNodeStatus::Pending;
            node.result = None;
            node.error = None;
        }
    }
}
