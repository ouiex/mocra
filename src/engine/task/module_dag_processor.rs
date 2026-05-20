use crate::cacheable::{CacheAble, CacheService};
/// Queue-backed DAG processor.
///
/// Design principles:
/// - Node routing uses `ExecutionMark.node_id` carried by Request/Response.
/// - Topology (successors map) is built from `ModuleDagDefinition` at init time.
/// - Fallback traces prior request via `prefix_request`; requests are persisted by `request.id`.
/// - Generate failure allows at most one fallback per `(node_id, prefix_uuid)` gate.
/// - Parser failure emits an error envelope seed tagged with same-node retry context.
/// - If parser succeeds but yields no downstream dispatch while successors exist,
///   a per-successor one-shot advance gate synthesizes a placeholder dispatch seed.
/// - Multi-branch support: when a node has N successors and parser returns one unrouted task,
///   it is fanned out to all N successors automatically.
use crate::common::interface::module::{ModuleNodeTrait, SyncBoxStream};
use crate::common::model::chain_key;
use crate::common::model::login_info::LoginInfo;
use crate::common::model::message::TaskEvent;
use crate::common::model::{
    insert_runtime_node_hint, ExecutionMark, ModuleConfig, NodeDispatch, NodeParseOutput,
    PayloadCodec, Request, ResolvedCommonConfig, Response, RuntimeNodeRoutingHint,
    TypedEnvelope,
};
use crate::engine::task::module_dag_compiler::{
    ModuleDagCompiler, ModuleDagDefinition, ModuleDagNodeDef,
};
use crate::engine::task::node_context_adapter::{
    build_legacy_generate_context_with_common, build_legacy_parse_context_with_common,
};
use crate::engine::task::parser_error_adapter::{
    ErrorEnvelopeSeed, ParserDispatchSeed, TypedParserOutput,
};
use crate::errors::Result;
use crate::errors::{ModuleError, RequestError};
use futures::StreamExt;
use indexmap::IndexMap;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;
use crate::schedule::dag::NodePlacement;

// ── Distributed gate types ──────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct DagNodeAdvanceGate(pub bool);

impl CacheAble for DagNodeAdvanceGate {
    fn field() -> impl AsRef<str> {
        "dag_advance"
    }
}

#[derive(Serialize, Deserialize)]
pub struct DagStopSignal(pub bool);

impl CacheAble for DagStopSignal {
    fn field() -> impl AsRef<str> {
        "dag_stop"
    }
}

fn typed_output_from_node_parse_output<F>(
    response: &Response,
    output: NodeParseOutput,
    mut context_for_target: F,
) -> TypedParserOutput
where
    F: FnMut(&str) -> ExecutionMark,
{
    let metadata_fallback = response
        .metadata
        .task
        .as_object()
        .cloned()
        .unwrap_or_default();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let next_dispatches = output
        .next
        .into_iter()
        .map(|dispatch| {
            let target_node = normalize_dispatch_target(&dispatch);
            ParserDispatchSeed {
                request_id: Uuid::now_v7(),
                task_model: TaskEvent {
                    account: response.account.clone(),
                    platform: response.platform.clone(),
                    module: Some(vec![response.module.clone()]),
                    priority: response.priority,
                    run_id: response.run_id,
                },
                timestamp,
                metadata: decode_payload_to_metadata(&dispatch.input.payload, metadata_fallback.clone()),
                context: context_for_target(&target_node),
                run_id: response.run_id,
                prefix_request: response.prefix_request,
                dispatch: Some(dispatch),
            }
        })
        .collect();

    TypedParserOutput {
        data: output.data,
        next_dispatches,
        error: None,
        stop: output.finished,
    }
}

fn placeholder_dispatch(target_node: impl Into<String>, metadata: &Map<String, Value>) -> NodeDispatch {
    let target_node = target_node.into();
    let payload = TypedEnvelope::new(
        "mocra.node_input.v1",
        1,
        PayloadCodec::Json,
        serde_json::to_vec(&Value::Object(metadata.clone())).unwrap_or_default(),
    );
    NodeDispatch::new(
        target_node.clone(),
        crate::common::model::NodeInput::new(target_node, payload),
    )
}

fn retarget_dispatch(seed: &mut ParserDispatchSeed, target_node: &str) {
    seed.context.node_id = Some(target_node.to_string());
    if let Some(dispatch) = seed.dispatch.as_mut() {
        dispatch.target_node = target_node.to_string();
        dispatch.input.target_node = target_node.to_string();
    }
}

fn decode_payload_to_metadata(
    envelope: &TypedEnvelope,
    fallback: Map<String, Value>,
) -> Map<String, Value> {
    if envelope.codec == PayloadCodec::Json
        && let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&envelope.bytes)
    {
        return map;
    }
    fallback
}

fn normalize_dispatch_target(dispatch: &crate::common::model::NodeDispatch) -> String {
    if !dispatch.target_node.is_empty() {
        return dispatch.target_node.clone();
    }
    dispatch.input.target_node.clone()
}

// ── Processor ───────────────────────────────────────────────────────────────

/// Queue-backed DAG processor that routes execution by `ExecutionMark.node_id`.
///
/// Nodes and topology are populated once from a `ModuleDagDefinition`, then used
/// immutably for the lifetime of a task run.
#[derive(Clone)]
pub struct ModuleDagProcessor {
    module_id: String,
    run_id: Uuid,
    profile_version: u64,
    dag_version: String,
    cache: Arc<CacheService>,
    ttl: u64,
    default_common_config: Arc<RwLock<ResolvedCommonConfig>>,
    /// Node registry: preserves definition order so index-based backward-compat lookup works.
    nodes: Arc<RwLock<IndexMap<String, Arc<dyn ModuleNodeTrait>>>>,
    /// Explicit runtime routing hints derived from DAG node placement and policy.
    node_routing_hints: Arc<RwLock<HashMap<String, RuntimeNodeRoutingHint>>>,
    /// Adjacency list: node_id → ordered list of successor node_ids.
    successors: Arc<RwLock<HashMap<String, Vec<String>>>>,
    /// Entry nodes (no predecessors). Used when `pending_ctx.node_id` is not set.
    entry_nodes: Arc<RwLock<Vec<String>>>,
    stop: Arc<RwLock<bool>>,
    last_stop_check: Arc<AtomicU64>,
}

impl ModuleDagProcessor {
    /// Creates an empty processor. Call `init_from_definition` before use.
    pub fn new(module_id: String, cache: Arc<CacheService>, run_id: Uuid, ttl: u64) -> Self {
        Self {
            module_id,
            run_id,
            profile_version: 0,
            dag_version: String::new(),
            cache,
            ttl,
            default_common_config: Arc::new(RwLock::new(ResolvedCommonConfig::default())),
            nodes: Arc::new(RwLock::new(IndexMap::new())),
            node_routing_hints: Arc::new(RwLock::new(HashMap::new())),
            successors: Arc::new(RwLock::new(HashMap::new())),
            entry_nodes: Arc::new(RwLock::new(Vec::new())),
            stop: Arc::new(RwLock::new(false)),
            last_stop_check: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Synchronizes processor run scope to the source-of-truth run_id.
    pub fn set_run_id(&mut self, run_id: Uuid) {
        self.run_id = run_id;
    }

    pub async fn set_default_common_config(&self, common: ResolvedCommonConfig) {
        *self.default_common_config.write().await = common;
    }

    pub fn set_execution_binding(&mut self, profile_version: u64, dag_version: impl Into<String>) {
        self.profile_version = profile_version;
        self.dag_version = dag_version.into();
    }

    fn execution_binding_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        if self.profile_version != 0 {
            metadata.insert("profile_version".to_string(), self.profile_version.to_string());
        }
        if !self.dag_version.is_empty() {
            metadata.insert("dag_version".to_string(), self.dag_version.clone());
        }
        metadata
    }

    fn execution_binding_enabled(&self) -> bool {
        self.profile_version != 0 || !self.dag_version.is_empty()
    }

    fn execution_binding_error(
        &self,
        stage: &str,
        message: impl Into<String>,
    ) -> crate::errors::Error {
        ModuleError::Model(
            std::io::Error::other(format!(
                "execution binding mismatch at {stage}: {}",
                message.into()
            ))
            .into(),
        )
        .into()
    }

    fn validate_execution_binding(
        &self,
        stage: &str,
        run_id: Uuid,
        metadata: &Map<String, serde_json::Value>,
    ) -> Result<()> {
        if !self.execution_binding_enabled() {
            return Ok(());
        }

        if run_id != self.run_id {
            return Err(self.execution_binding_error(
                stage,
                format!("run_id expected={} actual={}", self.run_id, run_id),
            ));
        }

        let actual_run_id = metadata
            .get("run_id")
            .and_then(|value| value.as_str())
            .ok_or_else(|| self.execution_binding_error(stage, "missing run_id"))?;
        if actual_run_id != self.run_id.to_string() {
            return Err(self.execution_binding_error(
                stage,
                format!("run_id expected={} actual={actual_run_id}", self.run_id),
            ));
        }

        let actual_profile_version = metadata
            .get("profile_version")
            .and_then(|value| value.as_u64())
            .ok_or_else(|| self.execution_binding_error(stage, "missing profile_version"))?;
        if actual_profile_version != self.profile_version {
            return Err(self.execution_binding_error(
                stage,
                format!(
                    "profile_version expected={} actual={actual_profile_version}",
                    self.profile_version
                ),
            ));
        }

        let actual_dag_version = metadata
            .get("dag_version")
            .and_then(|value| value.as_str())
            .ok_or_else(|| self.execution_binding_error(stage, "missing dag_version"))?;
        if actual_dag_version != self.dag_version {
            return Err(self.execution_binding_error(
                stage,
                format!("dag_version expected={} actual={actual_dag_version}", self.dag_version),
            ));
        }

        Ok(())
    }

    /// Populates nodes and topology from a compiled DAG definition.
    pub async fn init_from_definition(&self, definition: &ModuleDagDefinition) {
        let mut nodes: tokio::sync::RwLockWriteGuard<IndexMap<String, Arc<dyn ModuleNodeTrait>>> =
            self.nodes.write().await;
        let mut node_routing_hints = self.node_routing_hints.write().await;
        let mut successors = self.successors.write().await;
        let mut entry_nodes = self.entry_nodes.write().await;
        let default_policy = definition.default_policy.clone();

        nodes.clear();
        node_routing_hints.clear();
        successors.clear();

        // Register nodes in definition order.
        for node_def in &definition.nodes {
            nodes.insert(node_def.node_id.clone(), node_def.node.clone());
            let mut hint = RuntimeNodeRoutingHint::new(node_def.node_id.clone())
                .with_placement(
                    node_def
                        .placement_override
                        .clone()
                        .unwrap_or_else(NodePlacement::local),
                );
            if let Some(policy) = node_def
                .policy_override
                .clone()
                .or_else(|| default_policy.clone())
            {
                hint = hint.with_policy(policy);
            }
            node_routing_hints.insert(node_def.node_id.clone(), hint);
            successors.entry(node_def.node_id.clone()).or_default();
        }

        // Build successor adjacency list.
        for edge in &definition.edges {
            successors
                .entry(edge.from.clone())
                .or_default()
                .push(edge.to.clone());
        }

        // Derive entry nodes: nodes present in definition.entry_nodes, or nodes with no predecessors.
        if !definition.entry_nodes.is_empty() {
            *entry_nodes = definition.entry_nodes.clone();
        } else {
            let all_targets: std::collections::HashSet<&str> =
                definition.edges.iter().map(|e| e.to.as_str()).collect();
            *entry_nodes = definition
                .nodes
                .iter()
                .filter(|n| !all_targets.contains(n.node_id.as_str()))
                .map(|n| n.node_id.clone())
                .collect();
        }

        debug!(
            "[dag] module={} run={} init: nodes={} edges={} entries={:?}",
            self.module_id,
            self.run_id,
            nodes.len(),
            definition.edges.len(),
            *entry_nodes
        );
    }

    /// Total registered nodes (used for legacy compatibility checks).
    pub async fn get_total_nodes(&self) -> usize {
        let nodes: tokio::sync::RwLockReadGuard<IndexMap<String, Arc<dyn ModuleNodeTrait>>> =
            self.nodes.read().await;
        nodes.len()
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /// Resolves the target `node_id` from an optional `ExecutionMark`.
    ///
    /// Priority:
    /// 1. `ctx.node_id` if set
    /// 2. `ctx.step_idx` → index into `nodes` (backward compat for in-flight queue messages)
    /// 3. First entry node (initial call with no context)
    async fn resolve_node_id(&self, ctx: &Option<ExecutionMark>) -> Option<String> {
        if let Some(mark) = ctx {
            if let Some(ref nid) = mark.node_id {
                if !nid.is_empty() {
                    return Some(nid.clone());
                }
            }
            // Backward compat: step_idx → positional lookup.
            if let Some(idx) = mark.step_idx {
                let nodes: tokio::sync::RwLockReadGuard<
                    IndexMap<String, Arc<dyn ModuleNodeTrait>>,
                > = self.nodes.read().await;
                if let Some((id, _)) = nodes.get_index(idx as usize) {
                    return Some(id.clone());
                }
            }
        }
        // Default: first entry node.
        let entry = self.entry_nodes.read().await;
        entry.first().cloned()
    }

    async fn get_node(&self, node_id: &str) -> Option<Arc<dyn ModuleNodeTrait>> {
        let nodes: tokio::sync::RwLockReadGuard<IndexMap<String, Arc<dyn ModuleNodeTrait>>> =
            self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    async fn get_successors(&self, node_id: &str) -> Vec<String> {
        let succ = self.successors.read().await;
        succ.get(node_id).cloned().unwrap_or_default()
    }

    async fn runtime_node_hint(&self, node_id: &str) -> Option<RuntimeNodeRoutingHint> {
        let node_routing_hints = self.node_routing_hints.read().await;
        node_routing_hints.get(node_id).cloned()
    }

    pub async fn resolve_runtime_hint(
        &self,
        ctx: &Option<ExecutionMark>,
    ) -> Option<RuntimeNodeRoutingHint> {
        let node_id = self.resolve_node_id(ctx).await?;
        self.runtime_node_hint(&node_id).await
    }

    pub async fn compile_generate_node_dag(
        &self,
        ctx: &Option<ExecutionMark>,
    ) -> std::result::Result<Option<(crate::schedule::dag::Dag, String, Option<RuntimeNodeRoutingHint>)>, crate::schedule::dag::DagError> {
        let Some(node_id) = self.resolve_node_id(ctx).await else {
            return Ok(None);
        };
        let Some(node) = self.get_node(&node_id).await else {
            return Ok(None);
        };
        let hint = self.runtime_node_hint(&node_id).await;
        let mut definition = ModuleDagDefinition::builder()
            .node(&ModuleDagNodeDef {
                node_id: node_id.clone(),
                node,
                placement_override: hint.as_ref().and_then(|value| value.placement.clone()),
                policy_override: hint.as_ref().and_then(|value| value.policy.clone()),
                tags: vec![],
            })
            .build();
        definition.metadata = self.execution_binding_metadata();
        let dag = ModuleDagCompiler::compile(definition)?;
        Ok(Some((dag, node_id, hint)))
    }

    pub(crate) fn resolve_parse_fallback_index(response: &Response) -> usize {
        response.context.step_idx.unwrap_or(0) as usize
    }

    async fn resolve_parse_node_id_for_compile(&self, response: &Response) -> Option<String> {
        self.resolve_parse_target(response)
            .await
            .map(|(node_id, _)| node_id)
    }

    pub async fn resolve_parse_target(&self, response: &Response) -> Option<(String, usize)> {
        match response.context.node_id.as_deref() {
            Some(id) if !id.is_empty() => {
                let nodes = self.nodes.read().await;
                if let Some(idx) = nodes.get_index_of(id) {
                    Some((id.to_string(), idx))
                } else {
                    let fallback_idx = Self::resolve_parse_fallback_index(response);
                    match nodes.get_index(fallback_idx) {
                        Some((fallback_id, _)) => {
                            warn!(
                                "[dag] module={} run={} execute_parse: node '{}' stale, falling back to index {} ('{}')",
                                self.module_id, self.run_id, id, fallback_idx, fallback_id
                            );
                            Some((fallback_id.clone(), fallback_idx))
                        }
                        None => {
                            warn!(
                                "[dag] module={} run={} execute_parse: node '{}' not found and step_idx {} out of range, returning empty",
                                self.module_id, self.run_id, id, fallback_idx
                            );
                            None
                        }
                    }
                }
            }
            _ => {
                let idx = Self::resolve_parse_fallback_index(response);
                let nodes = self.nodes.read().await;
                match nodes.get_index(idx) {
                    Some((id, _)) => Some((id.clone(), idx)),
                    None => {
                        debug!(
                            "[dag] module={} run={} execute_parse: no node at index {}, returning empty",
                            self.module_id, self.run_id, idx
                        );
                        None
                    }
                }
            }
        }
    }

    pub async fn compile_parse_node_dag(
        &self,
        response: &Response,
    ) -> std::result::Result<Option<(crate::schedule::dag::Dag, String, Option<RuntimeNodeRoutingHint>)>, crate::schedule::dag::DagError> {
        let Some(node_id) = self.resolve_parse_node_id_for_compile(response).await else {
            return Ok(None);
        };
        let Some(node) = self.get_node(&node_id).await else {
            return Ok(None);
        };
        let hint = self.runtime_node_hint(&node_id).await;
        let mut definition = ModuleDagDefinition::builder()
            .node(&ModuleDagNodeDef {
                node_id: node_id.clone(),
                node,
                placement_override: hint.as_ref().and_then(|value| value.placement.clone()),
                policy_override: hint.as_ref().and_then(|value| value.policy.clone()),
                tags: vec![],
            })
            .build();
        definition.metadata = self.execution_binding_metadata();
        let dag = ModuleDagCompiler::compile(definition)?;
        Ok(Some((dag, node_id, hint)))
    }

    pub(crate) async fn route_parsed_output(
        &self,
        response: Response,
        node_id: String,
        parsed: NodeParseOutput,
    ) -> Result<TypedParserOutput> {
        let node_successors = self.get_successors(&node_id).await;
        let mut data = typed_output_from_node_parse_output(&response, parsed, |target| {
            ExecutionMark::default()
                .with_module_id(self.module_id.clone())
                .with_node_id(target.to_string())
        });
        if data.stop {
            self.set_stopped().await?;
        }

        if !data.next_dispatches.is_empty() {
            // ── Route explicit parser dispatches ─────────────────────────
            let mut routed: Vec<ParserDispatchSeed> = Vec::with_capacity(data.next_dispatches.len());

            for mut dispatch in data.next_dispatches.drain(..) {
                dispatch.prefix_request = response.prefix_request;

                let task_modules = dispatch.task_model.module.clone().unwrap_or_default();

                let same_module = self.is_task_for_current_module(&dispatch.context, &task_modules);

                if same_module {
                    let mut next_ctx = dispatch.context.clone();
                    if next_ctx.module_id.is_none() {
                        next_ctx.module_id = Some(self.module_id.clone());
                    }

                    if next_ctx.stay_current_step {
                        // Explicit retry on same node.
                        dispatch.context = next_ctx;
                        retarget_dispatch(&mut dispatch, &node_id);
                        routed.push(dispatch);
                        continue;
                    }

                    if next_ctx.node_id.is_none()
                        || next_ctx.node_id.as_deref() == Some(node_id.as_str())
                    {
                        // Parser didn't specify a different target (node_id is
                        // absent or still points at the current node): auto-route
                        // to successors.
                        if node_successors.is_empty() {
                            // Leaf node — DAG execution complete for this path.
                            debug!(
                                "[dag] module={} run={} execute_parse: leaf node '{}', discarding unrouted dispatch",
                                self.module_id, self.run_id, node_id
                            );
                            continue;
                        }
                        if node_successors.len() == 1 {
                            dispatch.context = next_ctx;
                            retarget_dispatch(&mut dispatch, node_successors[0].as_str());
                            self.apply_runtime_node_hint(
                                &mut dispatch.metadata,
                                node_successors[0].as_str(),
                            )
                            .await;
                            routed.push(dispatch);
                        } else {
                            // Fan-out: replicate dispatch for each successor.
                            for succ in &node_successors {
                                let mut next_dispatch = dispatch.clone();
                                next_dispatch.context = next_ctx.clone();
                                retarget_dispatch(&mut next_dispatch, succ);
                                self.apply_runtime_node_hint(&mut next_dispatch.metadata, succ).await;
                                routed.push(next_dispatch);
                            }
                        }
                        continue;
                    }

                    // Parser set an explicit node_id — use it verbatim.
                    dispatch.context = next_ctx;
                    if let Some(target_node_id) = dispatch.context.node_id.clone() {
                        retarget_dispatch(&mut dispatch, &target_node_id);
                        self.apply_runtime_node_hint(&mut dispatch.metadata, &target_node_id)
                            .await;
                    }
                }
                // Cross-module dispatch: pass through unchanged.
                routed.push(dispatch);
            }

            data.next_dispatches = routed;
        } else {
            // ── No dispatches: synthesize placeholder per successor ───────
            let task_metadata = response
                .metadata
                .task
                .as_object()
                .cloned()
                .unwrap_or_default();
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            for succ in &node_successors {
                if self.try_mark_node_advanced_once(&node_id, succ).await? {
                    let next_ctx = ExecutionMark::default()
                        .with_module_id(self.module_id.clone())
                        .with_node_id(succ.clone());
                    let mut next_dispatch = ParserDispatchSeed {
                        request_id: Uuid::now_v7(),
                        task_model: TaskEvent {
                            account: response.account.clone(),
                            platform: response.platform.clone(),
                            module: Some(vec![response.module.clone()]),
                            priority: response.priority,
                            run_id: response.run_id,
                        },
                        timestamp,
                        metadata: task_metadata.clone(),
                        context: next_ctx,
                        run_id: response.run_id,
                        prefix_request: response.prefix_request,
                        dispatch: Some(placeholder_dispatch(succ.clone(), &task_metadata)),
                    };
                    self.apply_runtime_node_hint(&mut next_dispatch.metadata, succ).await;
                    data = data.with_next_dispatch(next_dispatch);
                    debug!(
                        "[dag] module={} run={} execute_parse: advance gate won for '{}' -> synthesized dispatch to '{}'",
                        self.module_id, self.run_id, node_id, succ
                    );
                } else {
                    debug!(
                        "[dag] module={} run={} execute_parse: advance gate lost for '{}' -> '{}'",
                        self.module_id, self.run_id, node_id, succ
                    );
                }
            }
        }

        Ok(data)
    }

    pub(crate) async fn build_parser_error_output(
        &self,
        response: &Response,
        node_id: &str,
        step_idx: usize,
        error_message: String,
    ) -> TypedParserOutput {
        let meta = response
            .metadata
            .task
            .as_object()
            .cloned()
            .unwrap_or_default();
        let mut error_seed = ErrorEnvelopeSeed {
            request_id: response.id,
            task_model: TaskEvent {
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: Some(vec![response.module.clone()]),
                run_id: response.run_id,
                priority: crate::common::model::Priority::default(),
            },
            error_message,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            metadata: meta,
            context: ExecutionMark {
                module_id: Some(self.module_id.clone()),
                node_id: Some(node_id.to_string()),
                step_idx: Some(step_idx as u32),
                stay_current_step: true,
                ..Default::default()
            },
            run_id: response.run_id,
            prefix_request: response.prefix_request,
        };
        self.apply_runtime_node_hint(&mut error_seed.metadata, node_id)
            .await;
        TypedParserOutput::default().with_error(error_seed)
    }

    pub async fn execute_parse_with_node_error(
        &self,
        response: Response,
        node_id: &str,
        error_message: impl Into<String>,
    ) -> Result<TypedParserOutput> {
        let empty_meta = Map::new();
        let task_meta = response.metadata.task.as_object().unwrap_or(&empty_meta);
        self.validate_execution_binding("parse", response.run_id, task_meta)?;

        if self.check_stop().await? {
            return Ok(TypedParserOutput::default());
        }

        let step_idx = {
            let nodes = self.nodes.read().await;
            nodes.get_index_of(node_id)
                .unwrap_or_else(|| Self::resolve_parse_fallback_index(&response))
        };

        Ok(self
            .build_parser_error_output(&response, node_id, step_idx, error_message.into())
            .await)
    }

    async fn apply_runtime_node_hint(
        &self,
        metadata: &mut Map<String, serde_json::Value>,
        node_id: &str,
    ) {
        if let Some(hint) = self.runtime_node_hint(node_id).await {
            insert_runtime_node_hint(metadata, &hint);
        }
    }

    async fn try_mark_node_advanced_once(&self, node_id: &str, successor_id: &str) -> Result<bool> {
        let key = chain_key::dag_node_advance_gate_key(
            self.run_id,
            &self.module_id,
            node_id,
            successor_id,
        );
        if DagNodeAdvanceGate::sync(&key, &self.cache)
            .await
            .map_err(Into::<crate::errors::Error>::into)?
            .is_some()
        {
            return Ok(false);
        }
        let gate = DagNodeAdvanceGate(true);
        // No TTL — the advance gate is a permanent per-run fact (keyed by UUID run_id).
        // Using a short TTL (e.g. cache.ttl=60s) would allow the gate to expire mid-run,
        // letting a pending login retry re-win the gate and restart the entire DAG fan-out.
        gate.send_nx(&key, &self.cache, None)
            .await
            .map_err(Into::into)
    }

    async fn set_stopped(&self) -> Result<()> {
        let mut stop = self.stop.write().await;
        *stop = true;
        let key = chain_key::dag_stop_key(self.run_id, &self.module_id);
        let signal = DagStopSignal(true);
        // Use send_persistent (no TTL) so the stop signal outlives cache.ttl.
        // The gate-key cleanup below deletes all gate keys; if the stop signal
        // were to expire (e.g. TTL=60s), queued error tasks could re-win the
        // already-deleted gate and restart the entire DAG fan-out.
        signal.send_persistent(&key, &self.cache).await.ok();

        // Clean up all advance gate keys for this run.
        // Successors are already in memory — enumerate every edge and delete its gate key
        // without needing a Redis SCAN.
        let gate_keys: Vec<String> = {
            let succ = self.successors.read().await;
            succ.iter()
                .flat_map(|(from, tos)| {
                    tos.iter().map(move |to| {
                        chain_key::dag_node_advance_gate_key(self.run_id, &self.module_id, from, to)
                    })
                })
                .collect()
        };
        if !gate_keys.is_empty() {
            let refs: Vec<&str> = gate_keys.iter().map(String::as_str).collect();
            if let Err(e) = self.cache.del_batch(&refs).await {
                debug!(
                    "[dag] module={} run={} set_stopped: failed to delete {} gate keys: {}",
                    self.module_id,
                    self.run_id,
                    refs.len(),
                    e
                );
            } else {
                debug!(
                    "[dag] module={} run={} set_stopped: deleted {} advance gate keys",
                    self.module_id,
                    self.run_id,
                    refs.len()
                );
            }
        }

        Ok(())
    }

    /// Deletes the persistent session state for the given run.
    /// Called by Module::parser() with the correctly-patched Module.run_id,
    /// since self.run_id (processor) may be stale when the task was loaded from factory cache.
    pub async fn delete_session_for_run(&self, run_id: Uuid) {
        // Key format mirrors CacheAble::cache_id: "{namespace}:session_state:{module_id}:{run_id}"
        let session_key = format!(
            "{}:session_state:{}:{}",
            self.cache.namespace(),
            self.module_id,
            run_id
        );
        if let Err(e) = self.cache.del(&session_key).await {
            warn!(
                "Failed to delete session state: module={} run={} error={:?}",
                self.module_id, run_id, e
            );
        }
    }

    /// Rate-limited stop signal check (at most once per second).
    pub(crate) async fn check_stop(&self) -> Result<bool> {
        {
            let stop = self.stop.read().await;
            if *stop {
                return Ok(true);
            }
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last = self.last_stop_check.load(Ordering::Relaxed);
        if now.saturating_sub(last) < 1 {
            return Ok(false);
        }
        self.last_stop_check.store(now, Ordering::Relaxed);
        let key = chain_key::dag_stop_key(self.run_id, &self.module_id);
        if let Ok(Some(DagStopSignal(true))) = DagStopSignal::sync(&key, &self.cache).await {
            let mut stop = self.stop.write().await;
            *stop = true;
            return Ok(true);
        }
        Ok(false)
    }

    /// Returns true when a downstream parser dispatch targets this processor's module.
    fn is_task_for_current_module(&self, ctx: &ExecutionMark, task_modules: &[String]) -> bool {
        if let Some(ref mid) = ctx.module_id {
            if !mid.is_empty() {
                return mid == &self.module_id;
            }
        }
        if task_modules.is_empty() {
            return true;
        }
        task_modules
            .iter()
            .any(|m| self.module_id.ends_with(m.as_str()) || m == &self.module_id)
    }

    // ── Public execution API ─────────────────────────────────────────────────

    /// Generates a request stream for the resolved DAG node.
    ///
    /// On generate failure the error is returned directly so the caller's retry
    /// policy can re-schedule the same node.
    pub async fn execute_generate(
        &self,
        config: Arc<ModuleConfig>,
        meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
        ctx: Option<ExecutionMark>,
        prefix_request: Option<Uuid>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let prefix = prefix_request.unwrap_or_default();

        // Guard: if this run has already been stopped, skip generation entirely.
        // This prevents queued error tasks from restarting a completed run even
        // when no pending_ctx carries an explicit node_id (they would default to
        // the entry node and re-trigger the full DAG fan-out).
        if self.check_stop().await? {
            debug!(
                "[dag] module={} run={} execute_generate: run is stopped, skipping",
                self.module_id, self.run_id
            );
            return Ok(Box::pin(futures::stream::empty()));
        }

        self.validate_execution_binding("generate", self.run_id, &meta)?;

        let Some(node_id) = self.resolve_node_id(&ctx).await else {
            debug!(
                "[dag] module={} run={} execute_generate: no nodes registered",
                self.module_id, self.run_id
            );
            return Ok(Box::pin(futures::stream::empty()));
        };

        if let Some(hint) = self.runtime_node_hint(&node_id).await {
            if let Some(NodePlacement::Remote { worker_group }) = hint.placement {
                let message = format!(
                    "local fast path cannot execute remote-placed node '{}' for module '{}' (worker_group='{}')",
                    node_id, self.module_id, worker_group
                );
                warn!(
                    "[dag] module={} run={} execute_generate: {}",
                    self.module_id, self.run_id, message
                );
                return Err(RequestError::InvalidMetaForRemote(
                    std::io::Error::new(std::io::ErrorKind::Other, message).into(),
                )
                .into());
            }
        }

        debug!(
            "[dag] module={} run={} execute_generate: node={} prefix={}",
            self.module_id, self.run_id, node_id, prefix
        );

        let Some(node) = self.get_node(&node_id).await else {
            warn!(
                "[dag] module={} run={} execute_generate: node '{}' not found",
                self.module_id, self.run_id, node_id
            );
            return Ok(Box::pin(futures::stream::empty()));
        };

        let gen_ctx = {
            let mut mark = ctx.clone().unwrap_or_default();
            mark.node_id = Some(node_id.clone());
            if mark.module_id.is_none() {
                mark.module_id = Some(self.module_id.clone());
            }
            mark
        };
        let default_common = self.default_common_config.read().await.clone();
        let node_ctx = build_legacy_generate_context_with_common(
            &self.module_id,
            self.run_id,
            &node_id,
            default_common,
            config.as_ref(),
            meta,
            login_info,
            (!prefix.is_nil()).then_some(prefix),
        );

        match node.generate(node_ctx.borrowed()).await {
            Ok(stream) => {
                let run_id = self.run_id;
                let module_id = self.module_id.clone();
                let gen_ctx_clone = gen_ctx.clone();

                let stream = stream.map(move |mut req| {
                    req.context = gen_ctx_clone.clone();
                    req.run_id = run_id;
                    req.prefix_request = prefix;
                    if req.id.is_nil() {
                        req.id = Uuid::now_v7();
                    }

                    info!(
                        "[dag] module={} run={} execute_generate: produced request id={} node={} url={}",
                        module_id, run_id, req.id, req.context.node_id.as_deref().unwrap_or("?"), req.url
                    );
                    req
                });

                Ok(Box::pin(stream))
            }
            Err(e) => {
                warn!(
                    "[dag] module={} run={} execute_generate: generate error at node '{}', will retry current node: {}",
                    self.module_id, self.run_id, node_id, e
                );
                Err(e)
            }
        }
    }

    /// Parses a response at the routed DAG node and determines next-node progression.
    ///
    /// - Parser success with tasks: advance each task to its successor node(s).
    /// - Parser success without tasks: synthesize one placeholder per successor via advance gate.
    /// - Parser failure: emit an error envelope seed for same-node retry.
    pub async fn execute_parse(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<TypedParserOutput> {
        let empty_meta = Map::new();
        let task_meta = response.metadata.task.as_object().unwrap_or(&empty_meta);
        self.validate_execution_binding("parse", response.run_id, task_meta)?;

        // Resolve node_id + its current index in the processor's IndexMap.
        //
        // Priority:
        //   1. response.context.node_id  (exact string lookup)
        //   2. If (1) misses (stale UUID after DAG rebuild), try step_idx from context
        //   3. step_idx-only path (no node_id set)
        //
        // `node_idx` is also captured so error tasks carry the CORRECT step_idx for
        // reliable index-based fallback on the next retry, regardless of UUID staleness.
        let Some((node_id, node_idx)) = self.resolve_parse_target(&response).await else {
            return Ok(TypedParserOutput::default());
        };

        debug!(
            "[dag] module={} run={} execute_parse: node={} idx={} prefix={}",
            self.module_id, self.run_id, node_id, node_idx, response.prefix_request
        );

        if self.check_stop().await? {
            return Ok(TypedParserOutput::default());
        }

        if let Some(hint) = self.runtime_node_hint(&node_id).await {
            if let Some(NodePlacement::Remote { worker_group }) = hint.placement {
                let message = format!(
                    "local fast path cannot execute remote-placed node '{}' for module '{}' (worker_group='{}')",
                    node_id, self.module_id, worker_group
                );
                warn!(
                    "[dag] module={} run={} execute_parse: {}",
                    self.module_id, self.run_id, message
                );
                return Err(RequestError::InvalidMetaForRemote(
                    std::io::Error::new(std::io::ErrorKind::Other, message).into(),
                )
                .into());
            }
        }

        let Some(node) = self.get_node(&node_id).await else {
            // Unreachable: node existence was verified during resolution above.
            warn!(
                "[dag] module={} run={} execute_parse: node '{}' not found (guard)",
                self.module_id, self.run_id, node_id
            );
            return Ok(TypedParserOutput::default());
        };

        let default_common = self.default_common_config.read().await.clone();
        let parse_ctx = build_legacy_parse_context_with_common(
            &self.module_id,
            &node_id,
            default_common,
            config.as_deref(),
            &response,
        );

        match node.parser(response.clone(), parse_ctx.borrowed()).await {
            Ok(parsed) => self.route_parsed_output(response, node_id, parsed).await,
            Err(e) => {
                warn!(
                    "[dag] module={} run={} execute_parse: parser error at node='{}' account={} platform={} request_id={} error={}",
                    self.module_id,
                    self.run_id,
                    node_id,
                    response.account,
                    response.platform,
                    response.id,
                    e
                );
                Ok(self
                    .build_parser_error_output(&response, &node_id, node_idx, e.to_string())
                    .await)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::interface::{
        NodeGenerateContext, NodeParseContext, SyncBoxStream, ToSyncBoxStream,
    };
    use crate::common::model::{
        extract_runtime_node_hint, ModuleConfig, NodeDispatch, NodeInput, NodeParseOutput,
        PayloadCodec, Request, Response, TypedEnvelope,
    };
    use crate::engine::task::module_dag_compiler::{
        ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef,
    };
    use crate::errors::Result as CResult;
    use crate::schedule::dag::{DagNodeExecutionPolicy, DagNodeRetryMode, NodePlacement};
    use async_trait::async_trait;
    use serde_json::Map;
    use std::sync::Arc;

    #[test]
    fn decode_payload_to_metadata_prefers_json_envelope() {
        let envelope = TypedEnvelope::new(
            "mocra.node_input.v1",
            1,
            PayloadCodec::Json,
            serde_json::to_vec(&serde_json::json!({"page": 3})).expect("json bytes"),
        );
        let fallback = Map::from_iter([("fallback".to_string(), serde_json::json!(true))]);

        let decoded = decode_payload_to_metadata(&envelope, fallback);

        assert_eq!(
            decoded.get("page").and_then(|value| value.as_i64()),
            Some(3)
        );
    }

    struct DummyNode {
        pub name: &'static str,
    }

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> CResult<SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> CResult<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }
    }

    fn make_definition(edges: &[(&str, &str)]) -> ModuleDagDefinition {
        let node_ids: std::collections::BTreeSet<String> = edges
            .iter()
            .flat_map(|(a, b)| [a.to_string(), b.to_string()])
            .collect();
        let mut nodes: Vec<ModuleDagNodeDef> = node_ids
            .iter()
            .map(|id| ModuleDagNodeDef {
                node_id: id.clone(),
                node: Arc::new(DummyNode {
                    name: Box::leak(id.clone().into_boxed_str()),
                }),
                placement_override: None,
                policy_override: None,
                tags: vec![],
            })
            .collect();
        // Stable node order for tests
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));

        let edge_defs = edges
            .iter()
            .map(|(a, b)| ModuleDagEdgeDef {
                from: a.to_string(),
                to: b.to_string(),
            })
            .collect();

        // entry_nodes: nodes that are not targets of any edge
        let targets: std::collections::HashSet<&str> = edges.iter().map(|(_, b)| *b).collect();
        let entry_nodes = node_ids
            .iter()
            .filter(|id| !targets.contains(id.as_str()))
            .map(|id| id.clone())
            .collect();

        ModuleDagDefinition {
            nodes,
            edges: edge_defs,
            entry_nodes,
            default_policy: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    fn make_cache() -> Arc<CacheService> {
        Arc::new(CacheService::new(None, "test".to_string(), None, None))
    }

    #[tokio::test]
    async fn resolve_node_id_by_node_id_field() {
        let def = make_definition(&[("node_a", "node_b")]);
        let mut proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.set_execution_binding(7, "dag-v1");
        proc.init_from_definition(&def).await;

        let ctx = Some(ExecutionMark::default().with_node_id("node_b"));
        let resolved = proc.resolve_node_id(&ctx).await;
        assert_eq!(resolved.as_deref(), Some("node_b"));
    }

    #[tokio::test]
    async fn resolve_node_id_defaults_to_entry() {
        let def = make_definition(&[("node_a", "node_b")]);
        let mut proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.set_execution_binding(7, "dag-v1");
        proc.init_from_definition(&def).await;

        let resolved = proc.resolve_node_id(&None).await;
        assert_eq!(resolved.as_deref(), Some("node_a"));
    }

    #[tokio::test]
    async fn get_successors_linear() {
        let def = make_definition(&[("node_a", "node_b"), ("node_b", "node_c")]);
        let mut proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.set_execution_binding(7, "dag-v1");
        proc.init_from_definition(&def).await;

        let succ_a = proc.get_successors("node_a").await;
        assert_eq!(succ_a, vec!["node_b"]);
        let succ_b = proc.get_successors("node_b").await;
        assert_eq!(succ_b, vec!["node_c"]);
        let succ_c = proc.get_successors("node_c").await;
        assert!(succ_c.is_empty());
    }

    #[tokio::test]
    async fn get_successors_branch() {
        let def = make_definition(&[("node_a", "node_b"), ("node_a", "node_c")]);
        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let mut succ = proc.get_successors("node_a").await;
        succ.sort();
        assert_eq!(succ, vec!["node_b", "node_c"]);
    }

    // ── Metadata propagation tests ──────────────────────────────────────

    use crate::common::model::meta::MetaData;
    use std::sync::Mutex as StdMutex;

    /// Node that captures the params it receives in generate() and returns
    /// a configurable parser output.
    struct CapturingNode {
        captured_params: Arc<StdMutex<Vec<Map<String, serde_json::Value>>>>,
        parser_output: StdMutex<NodeParseOutput>,
    }

    impl CapturingNode {
        fn new(parser_output: NodeParseOutput) -> Self {
            CapturingNode {
                captured_params: Arc::new(StdMutex::new(Vec::new())),
                parser_output: StdMutex::new(parser_output),
            }
        }
        fn captured(&self) -> Vec<Map<String, serde_json::Value>> {
            self.captured_params.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for CapturingNode {
        async fn generate(
            &self,
            ctx: NodeGenerateContext<'_>,
        ) -> CResult<SyncBoxStream<'static, Request>> {
            let params =
                serde_json::from_slice::<Map<String, serde_json::Value>>(&ctx.input.payload.bytes)
                    .unwrap_or_default();
            self.captured_params.lock().unwrap().push(params);
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> CResult<NodeParseOutput> {
            Ok(self.parser_output.lock().unwrap().clone())
        }
    }

    fn json_input(target: &str, value: serde_json::Value) -> NodeInput {
        NodeInput::new(
            target,
            TypedEnvelope::new(
                "mocra.node_input.v1",
                1,
                PayloadCodec::Json,
                serde_json::to_vec(&value).expect("json payload"),
            ),
        )
    }

    fn make_response(
        node_id: &str,
        task_meta: serde_json::Map<String, serde_json::Value>,
    ) -> Response {
        Response {
            id: Uuid::now_v7(),
            platform: "pf".to_string(),
            account: "acc".to_string(),
            module: "mod".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: vec![],
            storage_path: None,
            headers: vec![],
            task_retry_times: 0,
            metadata: MetaData::default().add_task_config(task_meta),
            download_middleware: vec![],
            data_middleware: vec![],
            task_finished: false,
            context: ExecutionMark::default()
                .with_node_id(node_id)
                .with_module_id("mod"),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::now_v7(),
            request_hash: None,
            priority: Default::default(),
        }
    }

    fn make_binding_meta(run_id: Uuid, profile_version: u64, dag_version: &str) -> Map<String, serde_json::Value> {
        let mut metadata = Map::new();
        metadata.insert(
            "run_id".to_string(),
            serde_json::Value::String(run_id.to_string()),
        );
        metadata.insert(
            "profile_version".to_string(),
            serde_json::Value::from(profile_version),
        );
        metadata.insert(
            "dag_version".to_string(),
            serde_json::Value::String(dag_version.to_string()),
        );
        metadata
    }

    /// Explicit parser_task: metadata set via add_meta() should be preserved
    /// through routing and available for the next node's generate.
    #[tokio::test]
    async fn explicit_parser_task_metadata_preserved_through_routing() {
        let mut meta = Map::new();
        meta.insert("user_id".into(), serde_json::Value::String("abc".into()));
        // node_a parser returns a task with metadata
        let node_a = Arc::new(CapturingNode::new(NodeParseOutput::default().with_next(
            NodeDispatch::new(
                "node_a",
                json_input(
                    "node_a",
                    serde_json::json!({ "user_id": "abc", "page": 42 }),
                ),
            ),
        )));
        let node_b = Arc::new(CapturingNode::new(NodeParseOutput::default()));

        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "node_a".into(),
                    node: node_a.clone(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_b".into(),
                    node: node_b.clone(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![ModuleDagEdgeDef {
                from: "node_a".into(),
                to: "node_b".into(),
            }],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let response = make_response("node_a", Map::new());
        let result = proc.execute_parse(response, None).await.unwrap();

        // The routed task should have metadata preserved and be targeted at node_b
        assert_eq!(result.next_dispatches.len(), 1);
        let routed = &result.next_dispatches[0];
        assert_eq!(routed.context.node_id.as_deref(), Some("node_b"));
        assert_eq!(
            routed.metadata.get("user_id").and_then(|v| v.as_str()),
            Some("abc")
        );
        assert_eq!(
            routed.metadata.get("page").and_then(|v| v.as_i64()),
            Some(42)
        );

        // Now feed metadata into execute_generate to verify it reaches node_b
        let ctx = Some(routed.context.clone());
        let _ = proc
            .execute_generate(
                Arc::new(ModuleConfig::default()),
                routed.metadata.clone(),
                None,
                ctx,
                None,
            )
            .await
            .unwrap();

        let captured = node_b.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].get("user_id").and_then(|v| v.as_str()),
            Some("abc")
        );
        assert_eq!(captured[0].get("page").and_then(|v| v.as_i64()), Some(42));
    }

    /// Advance-gate path: when parser returns empty parser_task, synthesized
    /// tasks should carry forward the response's task metadata.
    #[tokio::test]
    async fn advance_gate_forwards_response_task_metadata() {
        // node_a parser returns empty parser_task (advance gate triggers)
        let node_a = Arc::new(CapturingNode::new(NodeParseOutput::default()));
        let node_b = Arc::new(CapturingNode::new(NodeParseOutput::default()));

        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "node_a".into(),
                    node: node_a.clone(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_b".into(),
                    node: node_b.clone(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![ModuleDagEdgeDef {
                from: "node_a".into(),
                to: "node_b".into(),
            }],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        // Response carries task metadata from node_a's generate
        let mut task_meta = Map::new();
        task_meta.insert("session_id".into(), serde_json::Value::String("s1".into()));
        let response = make_response("node_a", task_meta);

        let result = proc.execute_parse(response, None).await.unwrap();

        // Advance gate should synthesize a task with the forwarded metadata
        assert_eq!(result.next_dispatches.len(), 1);
        let synthesized = &result.next_dispatches[0];
        assert_eq!(synthesized.context.node_id.as_deref(), Some("node_b"));
        assert_eq!(
            synthesized
                .metadata
                .get("session_id")
                .and_then(|v| v.as_str()),
            Some("s1"),
            "advance-gate synthesized task should carry response.metadata.task"
        );

        // Verify it reaches node_b's generate
        let _ = proc
            .execute_generate(
                Arc::new(ModuleConfig::default()),
                synthesized.metadata.clone(),
                None,
                Some(synthesized.context.clone()),
                None,
            )
            .await
            .unwrap();

        let captured = node_b.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].get("session_id").and_then(|v| v.as_str()),
            Some("s1")
        );
    }

    /// Fan-out: metadata should be replicated to each successor.
    #[tokio::test]
    async fn fanout_replicates_metadata_to_all_successors() {
        let node_a = Arc::new(CapturingNode::new(NodeParseOutput::default().with_next(
            NodeDispatch::new(
                "node_a",
                json_input("node_a", serde_json::json!({ "key": "shared_value" })),
            ),
        )));
        let node_b = Arc::new(CapturingNode::new(NodeParseOutput::default()));
        let node_c = Arc::new(CapturingNode::new(NodeParseOutput::default()));

        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "node_a".into(),
                    node: node_a.clone(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_b".into(),
                    node: node_b.clone(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_c".into(),
                    node: node_c.clone(),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![
                ModuleDagEdgeDef {
                    from: "node_a".into(),
                    to: "node_b".into(),
                },
                ModuleDagEdgeDef {
                    from: "node_a".into(),
                    to: "node_c".into(),
                },
            ],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let response = make_response("node_a", Map::new());
        let result = proc.execute_parse(response, None).await.unwrap();

        // Fan-out should produce 2 tasks, each with the same metadata
        assert_eq!(result.next_dispatches.len(), 2);
        for dispatch in &result.next_dispatches {
            assert_eq!(
                dispatch.metadata.get("key").and_then(|v| v.as_str()),
                Some("shared_value")
            );
        }
        let mut targets: Vec<_> = result
            .next_dispatches
            .iter()
            .map(|dispatch| dispatch.context.node_id.clone().unwrap())
            .collect();
        targets.sort();
        assert_eq!(targets, vec!["node_b", "node_c"]);
    }

    #[tokio::test]
    async fn auto_routed_parser_task_carries_scheduler_hint_for_target_node() {
        let node_a = Arc::new(CapturingNode::new(NodeParseOutput::default().with_next(
            NodeDispatch::new(
                "node_a",
                json_input("node_a", serde_json::json!({ "page": 1 })),
            ),
        )));
        let node_b = Arc::new(CapturingNode::new(NodeParseOutput::default()));
        let policy = DagNodeExecutionPolicy {
            max_retries: 3,
            timeout_ms: Some(2500),
            retry_backoff_ms: 400,
            idempotency_key: Some("node-b".to_string()),
            retry_mode: DagNodeRetryMode::RetryableOnly,
            circuit_breaker_failure_threshold: Some(4),
            circuit_breaker_open_ms: 1500,
        };

        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "node_a".into(),
                    node: node_a,
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_b".into(),
                    node: node_b,
                    placement_override: Some(NodePlacement::remote("wg-parser")),
                    policy_override: Some(policy.clone()),
                    tags: vec![],
                },
            ],
            edges: vec![ModuleDagEdgeDef {
                from: "node_a".into(),
                to: "node_b".into(),
            }],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let result = proc
            .execute_parse(make_response("node_a", Map::new()), None)
            .await
            .unwrap();

        assert_eq!(result.next_dispatches.len(), 1);
        let hint = extract_runtime_node_hint(&result.next_dispatches[0].metadata)
            .expect("runtime node hint should be attached");
        assert_eq!(hint.node_key, "node_b");
        assert_eq!(hint.placement, Some(NodePlacement::remote("wg-parser")));
        assert_eq!(hint.policy, Some(policy));
    }

    #[tokio::test]
    async fn execute_generate_rejects_remote_target_on_local_fast_path() {
        let def = ModuleDagDefinition {
            nodes: vec![ModuleDagNodeDef {
                node_id: "node_a".into(),
                node: Arc::new(DummyNode { name: "node_a" }),
                placement_override: Some(NodePlacement::remote("wg-parser")),
                policy_override: None,
                tags: vec![],
            }],
            edges: vec![],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let result = proc
            .execute_generate(
                Arc::new(ModuleConfig::default()),
                Map::new(),
                None,
                Some(ExecutionMark::default().with_node_id("node_a")),
                None,
            )
            .await;
        let err = result
            .err()
            .expect("remote placement should not run on the local fast path");

        assert!(err.to_string().contains("local fast path cannot execute remote-placed node"));
    }

    #[tokio::test]
    async fn execute_generate_rejects_mismatched_execution_binding() {
        let def = make_definition(&[("node_a", "node_b")]);
        let run_id = Uuid::now_v7();
        let mut proc = ModuleDagProcessor::new("mod".into(), make_cache(), run_id, 60);
        proc.set_execution_binding(7, "dag-v1");
        proc.init_from_definition(&def).await;

        let err = match proc
            .execute_generate(
                Arc::new(ModuleConfig::default()),
                make_binding_meta(run_id, 8, "dag-v1"),
                None,
                Some(ExecutionMark::default().with_node_id("node_a")),
                None,
            )
            .await
        {
            Ok(_) => panic!("mismatched binding should be rejected"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("profile_version"));
    }

    #[tokio::test]
    async fn execute_parse_rejects_remote_target_on_local_fast_path() {
        let def = ModuleDagDefinition {
            nodes: vec![ModuleDagNodeDef {
                node_id: "node_a".into(),
                node: Arc::new(DummyNode { name: "node_a" }),
                placement_override: Some(NodePlacement::remote("wg-parser")),
                policy_override: None,
                tags: vec![],
            }],
            edges: vec![],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let err = proc
            .execute_parse(make_response("node_a", Map::new()), None)
            .await
            .expect_err("remote placement should not run on the local fast path");

        assert!(err.to_string().contains("local fast path cannot execute remote-placed node"));
    }

    #[tokio::test]
    async fn execute_parse_rejects_mismatched_execution_binding() {
        let def = make_definition(&[("node_a", "node_b")]);
        let run_id = Uuid::now_v7();
        let mut proc = ModuleDagProcessor::new("mod".into(), make_cache(), run_id, 60);
        proc.set_execution_binding(7, "dag-v1");
        proc.init_from_definition(&def).await;

        let mismatched_run_id = Uuid::now_v7();
        let mut response = make_response(
            "node_a",
            make_binding_meta(mismatched_run_id, 7, "dag-v1"),
        );
        response.run_id = mismatched_run_id;

        let err = proc
            .execute_parse(response, None)
            .await
            .expect_err("mismatched binding should be rejected");

        assert!(err.to_string().contains("run_id"));
    }

    #[tokio::test]
    async fn compile_generate_node_dag_preserves_target_runtime_settings() {
        let policy = DagNodeExecutionPolicy {
            max_retries: 3,
            timeout_ms: Some(500),
            ..DagNodeExecutionPolicy::default()
        };
        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "node_a".into(),
                    node: Arc::new(DummyNode { name: "node_a" }),
                    placement_override: Some(NodePlacement::remote("wg-a")),
                    policy_override: Some(policy.clone()),
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_b".into(),
                    node: Arc::new(DummyNode { name: "node_b" }),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![ModuleDagEdgeDef {
                from: "node_a".into(),
                to: "node_b".into(),
            }],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let mut proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.set_execution_binding(7, "dag-v1");
        proc.init_from_definition(&def).await;

        let (dag, node_id, hint) = proc
            .compile_generate_node_dag(&Some(ExecutionMark::default().with_node_id("node_a")))
            .await
            .expect("single-node dag compile should succeed")
            .expect("target node should resolve");
        let node = dag
            .node_ptrs()
            .pop()
            .expect("single-node dag should contain one node");

        assert_eq!(node_id, "node_a");
        assert_eq!(node.id, "node_a");
        assert_eq!(node.placement, NodePlacement::remote("wg-a"));
        assert_eq!(node.execution_policy, policy);
        assert_eq!(dag.metadata("profile_version"), Some("7"));
        assert_eq!(dag.metadata("dag_version"), Some("dag-v1"));
        assert_eq!(hint.expect("hint should exist").node_key, "node_a");
    }

    #[tokio::test]
    async fn compile_parse_node_dag_preserves_target_runtime_settings() {
        let policy = DagNodeExecutionPolicy {
            max_retries: 3,
            timeout_ms: Some(500),
            ..DagNodeExecutionPolicy::default()
        };
        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "node_a".into(),
                    node: Arc::new(DummyNode { name: "node_a" }),
                    placement_override: Some(NodePlacement::remote("wg-a")),
                    policy_override: Some(policy.clone()),
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_b".into(),
                    node: Arc::new(DummyNode { name: "node_b" }),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![ModuleDagEdgeDef {
                from: "node_a".into(),
                to: "node_b".into(),
            }],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let mut proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.set_execution_binding(7, "dag-v1");
        proc.init_from_definition(&def).await;

        let (dag, node_id, hint) = proc
            .compile_parse_node_dag(&make_response("node_a", Map::new()))
            .await
            .expect("single-node dag compile should succeed")
            .expect("target node should resolve");
        let node = dag
            .node_ptrs()
            .pop()
            .expect("single-node dag should contain one node");

        assert_eq!(node_id, "node_a");
        assert_eq!(node.id, "node_a");
        assert_eq!(node.placement, NodePlacement::remote("wg-a"));
        assert_eq!(node.execution_policy, policy);
        assert_eq!(dag.metadata("profile_version"), Some("7"));
        assert_eq!(dag.metadata("dag_version"), Some("dag-v1"));
        assert_eq!(hint.expect("hint should exist").node_key, "node_a");
    }

    #[tokio::test]
    async fn compile_parse_node_dag_falls_back_to_step_idx_when_node_id_is_stale() {
        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef {
                    node_id: "node_a".into(),
                    node: Arc::new(DummyNode { name: "node_a" }),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
                ModuleDagNodeDef {
                    node_id: "node_b".into(),
                    node: Arc::new(DummyNode { name: "node_b" }),
                    placement_override: None,
                    policy_override: None,
                    tags: vec![],
                },
            ],
            edges: vec![ModuleDagEdgeDef {
                from: "node_a".into(),
                to: "node_b".into(),
            }],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let mut response = make_response("stale_node", Map::new());
        response.context.step_idx = Some(1);

        let (_dag, node_id, _hint) = proc
            .compile_parse_node_dag(&response)
            .await
            .expect("single-node dag compile should succeed")
            .expect("target node should resolve");

        assert_eq!(node_id, "node_b");
    }

    /// with_meta replaces all metadata; add_meta appends.
    #[tokio::test]
    async fn add_meta_appends_with_meta_replaces() {
        let response = make_response("node_a", Map::new());
        let mut task = ParserDispatchSeed {
            request_id: Uuid::now_v7(),
            task_model: crate::common::model::message::TaskEvent {
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: Some(vec![response.module.clone()]),
                priority: response.priority,
                run_id: response.run_id,
            },
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: response.metadata.task.as_object().cloned().unwrap_or_default(),
            context: response.context.clone(),
            run_id: response.run_id,
            prefix_request: response.prefix_request,
            dispatch: None,
        };
        task.metadata.insert("a".into(), serde_json::Value::from(1));
        task.metadata.insert("b".into(), serde_json::Value::from(2));
        assert_eq!(task.metadata.len(), 2);

        let mut new_map = Map::new();
        new_map.insert("c".into(), serde_json::Value::from(3));
        let mut task2 = task;
        task2.metadata = new_map;
        assert_eq!(task2.metadata.len(), 1);
        assert_eq!(task2.metadata.get("c").and_then(|v| v.as_i64()), Some(3));
    }

    /// Explicit parser dispatch seed construction should forward task metadata from
    /// the response's MetaData.task slot.
    #[tokio::test]
    async fn parser_dispatch_seed_construction_forwards_task_metadata() {
        let mut task_meta = Map::new();
        task_meta.insert("forwarded".into(), serde_json::Value::Bool(true));
        let response = make_response("node_a", task_meta);

        let parsed = ParserDispatchSeed {
            request_id: Uuid::now_v7(),
            task_model: crate::common::model::message::TaskEvent {
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: Some(vec![response.module.clone()]),
                priority: response.priority,
                run_id: response.run_id,
            },
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: response.metadata.task.as_object().cloned().unwrap_or_default(),
            context: response.context.clone(),
            run_id: response.run_id,
            prefix_request: response.prefix_request,
            dispatch: None,
        };
        assert_eq!(
            parsed.metadata.get("forwarded").and_then(|v| v.as_bool()),
            Some(true),
            "parser dispatch seed construction should forward metadata.task into ParserDispatchSeed.metadata"
        );
    }

    /// Explicit parser dispatch seed construction with empty metadata still produces empty map.
    #[tokio::test]
    async fn parser_dispatch_seed_construction_empty_metadata_stays_empty() {
        let response = make_response("node_a", Map::new());
        let parsed = ParserDispatchSeed {
            request_id: Uuid::now_v7(),
            task_model: crate::common::model::message::TaskEvent {
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: Some(vec![response.module.clone()]),
                priority: response.priority,
                run_id: response.run_id,
            },
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: response.metadata.task.as_object().cloned().unwrap_or_default(),
            context: response.context.clone(),
            run_id: response.run_id,
            prefix_request: response.prefix_request,
            dispatch: None,
        };
        assert!(parsed.metadata.is_empty());
    }
}
