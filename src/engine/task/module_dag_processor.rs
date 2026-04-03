/// Queue-backed DAG processor.
///
/// Design principles:
/// - Node routing uses `ExecutionMark.node_id` carried by Request/Response.
/// - Topology (successors map) is built from `ModuleDagDefinition` at init time.
/// - Fallback traces prior request via `prefix_request`; requests are persisted by `request.id`.
/// - Generate failure allows at most one fallback per `(node_id, prefix_uuid)` gate.
/// - Parser failure emits `TaskErrorEvent` tagged with same-node retry context.
/// - If parser succeeds but yields no `TaskParserEvent` while successors exist,
///   a per-successor one-shot advance gate synthesizes a placeholder task.
/// - Multi-branch support: when a node has N successors and parser returns one unrouted task,
///   it is fanned out to all N successors automatically.

use crate::common::interface::module::{ModuleNodeTrait, SyncBoxStream};
use crate::common::model::chain_key;
use crate::common::model::login_info::LoginInfo;
use crate::common::model::message::{TaskErrorEvent, TaskEvent, TaskOutputEvent, TaskParserEvent};
use crate::common::model::{ExecutionMark, ModuleConfig, Request, Response};
use crate::errors::Result;
use futures::StreamExt;
use indexmap::IndexMap;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;
use crate::cacheable::{CacheAble, CacheService};
use crate::engine::task::module_dag_compiler::ModuleDagDefinition;

// ── Distributed gate types ──────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
pub struct DagNodeAdvanceGate(pub bool);

impl CacheAble for DagNodeAdvanceGate {
    fn field() -> impl AsRef<str> {
        "dag_advance"
    }
}

#[derive(Serialize, Deserialize)]
pub struct DagNodeFallbackGate(pub bool);

impl CacheAble for DagNodeFallbackGate {
    fn field() -> impl AsRef<str> {
        "dag_fallback"
    }
}

#[derive(Serialize, Deserialize)]
pub struct DagStopSignal(pub bool);

impl CacheAble for DagStopSignal {
    fn field() -> impl AsRef<str> {
        "dag_stop"
    }
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
    cache: Arc<CacheService>,
    ttl: u64,
    /// Node registry: preserves definition order so index-based backward-compat lookup works.
    nodes: Arc<RwLock<IndexMap<String, Arc<dyn ModuleNodeTrait>>>>,
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
            cache,
            ttl,
            nodes: Arc::new(RwLock::new(IndexMap::new())),
            successors: Arc::new(RwLock::new(HashMap::new())),
            entry_nodes: Arc::new(RwLock::new(Vec::new())),
            stop: Arc::new(RwLock::new(false)),
            last_stop_check: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Populates nodes and topology from a compiled DAG definition.
    pub async fn init_from_definition(&self, definition: &ModuleDagDefinition) {
        let mut nodes: tokio::sync::RwLockWriteGuard<IndexMap<String, Arc<dyn ModuleNodeTrait>>> = self.nodes.write().await;
        let mut successors = self.successors.write().await;
        let mut entry_nodes = self.entry_nodes.write().await;

        nodes.clear();
        successors.clear();

        // Register nodes in definition order.
        for node_def in &definition.nodes {
            nodes.insert(node_def.node_id.clone(), node_def.node.clone());
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
            let all_targets: std::collections::HashSet<&str> = definition
                .edges
                .iter()
                .map(|e| e.to.as_str())
                .collect();
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
        let nodes: tokio::sync::RwLockReadGuard<IndexMap<String, Arc<dyn ModuleNodeTrait>>> = self.nodes.read().await;
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
                let nodes: tokio::sync::RwLockReadGuard<IndexMap<String, Arc<dyn ModuleNodeTrait>>> = self.nodes.read().await;
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
        let nodes: tokio::sync::RwLockReadGuard<IndexMap<String, Arc<dyn ModuleNodeTrait>>> = self.nodes.read().await;
        nodes.get(node_id).cloned()
    }

    async fn get_successors(&self, node_id: &str) -> Vec<String> {
        let succ = self.successors.read().await;
        succ.get(node_id).cloned().unwrap_or_default()
    }

    async fn load_request(&self, id: &Uuid) -> Result<Option<Request>> {
        let id_str = id.to_string();
        if let Ok(Some(req)) = Request::sync(&id_str, &self.cache).await {
            return Ok(Some(req));
        }
        Ok(None)
    }

    async fn try_allow_fallback_once(&self, node_id: &str, prefix: &Uuid) -> Result<bool> {
        let key = chain_key::dag_node_fallback_gate_key(self.run_id, &self.module_id, node_id, *prefix);
        if DagNodeFallbackGate::sync(&key, &self.cache).await.map_err(Into::<crate::errors::Error>::into)?.is_some() {
            return Ok(false);
        }
        let gate = DagNodeFallbackGate(true);
        gate.send_nx(&key, &self.cache, Some(std::time::Duration::from_secs(self.ttl)))
            .await
            .map_err(Into::into)
    }

    async fn try_mark_node_advanced_once(&self, node_id: &str, successor_id: &str) -> Result<bool> {
        let key = chain_key::dag_node_advance_gate_key(self.run_id, &self.module_id, node_id, successor_id);
        if DagNodeAdvanceGate::sync(&key, &self.cache).await.map_err(Into::<crate::errors::Error>::into)?.is_some() {
            return Ok(false);
        }
        let gate = DagNodeAdvanceGate(true);
        gate.send_nx(&key, &self.cache, Some(std::time::Duration::from_secs(self.ttl)))
            .await
            .map_err(Into::into)
    }

    async fn set_stopped(&self) -> Result<()> {
        let mut stop = self.stop.write().await;
        *stop = true;
        let key = chain_key::dag_stop_key(self.run_id, &self.module_id);
        let signal = DagStopSignal(true);
        signal
            .send_with_ttl(&key, &self.cache, std::time::Duration::from_secs(self.ttl))
            .await
            .ok();
        Ok(())
    }

    /// Rate-limited stop signal check (at most once per second).
    async fn check_stop(&self) -> Result<bool> {
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

    /// Returns true when a `TaskParserEvent` targets this processor's module.
    fn is_task_for_current_module(
        &self,
        ctx: &ExecutionMark,
        task_modules: &[String],
    ) -> bool {
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
    /// On generate failure, attempts a one-shot fallback to the previous request
    /// identified by `prefix_request` (backed by Redis).
    pub async fn execute_generate(
        &self,
        config: Arc<ModuleConfig>,
        meta: Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
        ctx: Option<ExecutionMark>,
        prefix_request: Option<Uuid>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let prefix = prefix_request.unwrap_or_default();

        let Some(node_id) = self.resolve_node_id(&ctx).await else {
            debug!("[dag] module={} run={} execute_generate: no nodes registered", self.module_id, self.run_id);
            return Ok(Box::pin(futures::stream::empty()));
        };

        debug!(
            "[dag] module={} run={} execute_generate: node={} prefix={}",
            self.module_id, self.run_id, node_id, prefix
        );

        let Some(node) = self.get_node(&node_id).await else {
            warn!("[dag] module={} run={} execute_generate: node '{}' not found", self.module_id, self.run_id, node_id);
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

        match node.generate(config, meta, login_info).await {
            Ok(stream) => {
                let run_id = self.run_id;
                let module_id = self.module_id.clone();
                let cache = self.cache.clone();
                let gen_ctx_clone = gen_ctx.clone();

                let stream = stream.map(move |mut req| {
                    req.context = gen_ctx_clone.clone();
                    req.run_id = run_id;
                    req.prefix_request = prefix;
                    if req.id.is_nil() {
                        req.id = Uuid::now_v7();
                    }

                    // Background request persistence for fallback recovery.
                    let req_save = req.clone();
                    let req_save_id = req_save.id;
                    let cache_save = cache.clone();
                    tokio::spawn(async move {
                        if let Ok(Ok(bytes)) = tokio::task::spawn_blocking(move || {
                            serde_json::to_vec(&req_save)
                        })
                        .await
                        {
                            let key = Request::cache_id(&req_save_id.to_string(), &cache_save);
                            let _ = cache_save.set(&key, &bytes, cache_save.default_ttl()).await;
                        }
                    });

                    info!(
                        "[dag] module={} run={} execute_generate: produced request id={} node={}",
                        module_id, run_id, req.id, req.context.node_id.as_deref().unwrap_or("?")
                    );
                    req
                });

                Ok(Box::pin(stream))
            }
            Err(e) => {
                // Entry node has no fallback target.
                if prefix.is_nil() {
                    warn!(
                        "[dag] module={} run={} execute_generate: generate error at entry node '{}', no prefix: {}",
                        self.module_id, self.run_id, node_id, e
                    );
                    return Err(e);
                }

                // Non-entry node: one-shot fallback to previous request.
                if self.try_allow_fallback_once(&node_id, &prefix).await? {
                    match self.load_request(&prefix).await? {
                        Some(prev) => {
                            debug!(
                                "[dag] module={} run={} execute_generate: fallback allowed for node '{}' -> prev request id={}",
                                self.module_id, self.run_id, node_id, prefix
                            );
                            return Ok(Box::pin(futures::stream::once(async move { prev })));
                        }
                        None => {
                            debug!(
                                "[dag] module={} run={} execute_generate: fallback allowed but prev request not found",
                                self.module_id, self.run_id
                            );
                        }
                    }
                } else {
                    warn!(
                        "[dag] module={} run={} execute_generate: fallback gate consumed for node '{}', err={}",
                        self.module_id, self.run_id, node_id, e
                    );
                }

                Err(e)
            }
        }
    }

    /// Parses a response at the routed DAG node and determines next-node progression.
    ///
    /// - Parser success with tasks: advance each task to its successor node(s).
    /// - Parser success without tasks: synthesize one placeholder per successor via advance gate.
    /// - Parser failure: emit `TaskErrorEvent` for same-node retry.
    pub async fn execute_parse(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        // Resolve node_id: prefer node_id, fall back to step_idx for backward compat.
        let node_id = match response.context.node_id.as_deref() {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => {
                let idx = response.context.step_idx.unwrap_or(0) as usize;
                let nodes = self.nodes.read().await;
                match nodes.get_index(idx) {
                    Some((id, _)) => id.clone(),
                    None => {
                        debug!(
                            "[dag] module={} run={} execute_parse: no node at index {}, returning empty",
                            self.module_id, self.run_id, idx
                        );
                        return Ok(TaskOutputEvent::default());
                    }
                }
            }
        };

        debug!(
            "[dag] module={} run={} execute_parse: node={} prefix={}",
            self.module_id, self.run_id, node_id, response.prefix_request
        );

        if self.check_stop().await? {
            return Ok(TaskOutputEvent::default());
        }

        let Some(node) = self.get_node(&node_id).await else {
            warn!("[dag] module={} run={} execute_parse: node '{}' not found", self.module_id, self.run_id, node_id);
            return Ok(TaskOutputEvent::default());
        };

        let node_successors = self.get_successors(&node_id).await;

        match node.parser(response.clone(), config).await {
            Ok(mut data) => {
                if data.stop.unwrap_or(false) {
                    self.set_stopped().await?;
                }

                if !data.parser_task.is_empty() {
                    // ── Route explicit parser tasks ──────────────────────────────
                    let mut routed: Vec<TaskParserEvent> = Vec::with_capacity(data.parser_task.len());

                    for mut task in data.parser_task.drain(..) {
                        task.prefix_request = response.prefix_request;

                        let task_modules = task
                            .account_task
                            .module
                            .clone()
                            .unwrap_or_default();

                        let same_module = self.is_task_for_current_module(&task.context, &task_modules);

                        if same_module {
                            let mut next_ctx = task.context.clone();
                            if next_ctx.module_id.is_none() {
                                next_ctx.module_id = Some(self.module_id.clone());
                            }

                            if next_ctx.stay_current_step {
                                // Explicit retry on same node.
                                next_ctx.node_id = Some(node_id.clone());
                                task.context = next_ctx;
                                routed.push(task);
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
                                        "[dag] module={} run={} execute_parse: leaf node '{}', discarding unrouted task",
                                        self.module_id, self.run_id, node_id
                                    );
                                    continue;
                                }
                                if node_successors.len() == 1 {
                                    next_ctx.node_id = Some(node_successors[0].clone());
                                    task.context = next_ctx;
                                    routed.push(task);
                                } else {
                                    // Fan-out: replicate task for each successor.
                                    for succ in &node_successors {
                                        let mut t = task.clone();
                                        let mut ctx = next_ctx.clone();
                                        ctx.node_id = Some(succ.clone());
                                        t.context = ctx;
                                        routed.push(t);
                                    }
                                }
                                continue;
                            }

                            // Parser set an explicit node_id — use it verbatim.
                            task.context = next_ctx;
                        }
                        // Cross-module task: pass through unchanged.
                        routed.push(task);
                    }

                    data.parser_task = routed;
                } else {
                    // ── No tasks: synthesize placeholder per successor ────────────
                    for succ in &node_successors {
                        if self.try_mark_node_advanced_once(&node_id, succ).await? {
                            let base: TaskParserEvent = (&response).into();
                            let next_ctx = ExecutionMark::default()
                                .with_module_id(self.module_id.clone())
                                .with_node_id(succ.clone());
                            let mut next_task = base.with_context(next_ctx);
                            next_task.prefix_request = response.prefix_request;
                            data = data.with_task(next_task);
                            debug!(
                                "[dag] module={} run={} execute_parse: advance gate won for '{}' -> synthesized task to '{}'",
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
            Err(e) => {
                // Parser failure: emit error for same-node retry.
                let step_idx_u32 = response.context.step_idx.unwrap_or(0);
                let meta = response.metadata.task.as_object().cloned().unwrap_or_default();
                let error_task = TaskErrorEvent {
                    id: response.id,
                    account_task: TaskEvent {
                        account: response.account.clone(),
                        platform: response.platform.clone(),
                        module: Some(vec![response.module.clone()]),
                        run_id: response.run_id,
                        priority: crate::common::model::Priority::default(),
                    },
                    error_msg: e.to_string(),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    metadata: meta,
                    context: ExecutionMark {
                        module_id: Some(self.module_id.clone()),
                        node_id: Some(node_id.clone()),
                        step_idx: Some(step_idx_u32),
                        stay_current_step: true,
                        ..Default::default()
                    },
                    run_id: response.run_id,
                    prefix_request: response.prefix_request,
                };
                Ok(TaskOutputEvent::default().with_error(error_task))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::task::module_dag_compiler::{ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef};
    use crate::common::interface::{SyncBoxStream, ToSyncBoxStream};
    use crate::common::model::message::TaskOutputEvent;
    use crate::common::model::{ModuleConfig, Request, Response};
    use crate::errors::Result as CResult;
    use async_trait::async_trait;
    use serde_json::Map;
    use std::sync::Arc;

    struct DummyNode {
        pub name: &'static str,
    }

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(
            &self,
            _config: Arc<ModuleConfig>,
            _params: Map<String, serde_json::Value>,
            _login_info: Option<LoginInfo>,
        ) -> CResult<SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _config: Option<Arc<ModuleConfig>>,
        ) -> CResult<TaskOutputEvent> {
            Ok(TaskOutputEvent::default())
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
                node: Arc::new(DummyNode { name: Box::leak(id.clone().into_boxed_str()) }),
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
        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let ctx = Some(ExecutionMark::default().with_node_id("node_b"));
        let resolved = proc.resolve_node_id(&ctx).await;
        assert_eq!(resolved.as_deref(), Some("node_b"));
    }

    #[tokio::test]
    async fn resolve_node_id_defaults_to_entry() {
        let def = make_definition(&[("node_a", "node_b")]);
        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let resolved = proc.resolve_node_id(&None).await;
        assert_eq!(resolved.as_deref(), Some("node_a"));
    }

    #[tokio::test]
    async fn get_successors_linear() {
        let def = make_definition(&[("node_a", "node_b"), ("node_b", "node_c")]);
        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
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

    use std::sync::Mutex as StdMutex;
    use crate::common::model::meta::MetaData;

    /// Node that captures the params it receives in generate() and returns
    /// a configurable parser output.
    struct CapturingNode {
        captured_params: Arc<StdMutex<Vec<Map<String, serde_json::Value>>>>,
        parser_output: StdMutex<TaskOutputEvent>,
    }

    impl CapturingNode {
        fn new(parser_output: TaskOutputEvent) -> Self {
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
            _config: Arc<ModuleConfig>,
            params: Map<String, serde_json::Value>,
            _login_info: Option<LoginInfo>,
        ) -> CResult<SyncBoxStream<'static, Request>> {
            self.captured_params.lock().unwrap().push(params);
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _config: Option<Arc<ModuleConfig>>,
        ) -> CResult<TaskOutputEvent> {
            Ok(self.parser_output.lock().unwrap().clone())
        }
    }

    fn make_response(node_id: &str, task_meta: serde_json::Map<String, serde_json::Value>) -> Response {
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

    /// Explicit parser_task: metadata set via add_meta() should be preserved
    /// through routing and available for the next node's generate.
    #[tokio::test]
    async fn explicit_parser_task_metadata_preserved_through_routing() {
        let mut meta = Map::new();
        meta.insert("user_id".into(), serde_json::Value::String("abc".into()));
        let task = TaskParserEvent::from(&make_response("node_a", Map::new()))
            .add_meta("user_id", "abc")
            .add_meta("page", 42);

        // node_a parser returns a task with metadata
        let node_a = Arc::new(CapturingNode::new(
            TaskOutputEvent::default().with_task(task),
        ));
        let node_b = Arc::new(CapturingNode::new(TaskOutputEvent::default()));

        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef { node_id: "node_a".into(), node: node_a.clone(), placement_override: None, policy_override: None, tags: vec![] },
                ModuleDagNodeDef { node_id: "node_b".into(), node: node_b.clone(), placement_override: None, policy_override: None, tags: vec![] },
            ],
            edges: vec![ModuleDagEdgeDef { from: "node_a".into(), to: "node_b".into() }],
            entry_nodes: vec!["node_a".into()],
            default_policy: None,
            metadata: Default::default(),
        };

        let proc = ModuleDagProcessor::new("mod".into(), make_cache(), Uuid::now_v7(), 60);
        proc.init_from_definition(&def).await;

        let response = make_response("node_a", Map::new());
        let result = proc.execute_parse(response, None).await.unwrap();

        // The routed task should have metadata preserved and be targeted at node_b
        assert_eq!(result.parser_task.len(), 1);
        let routed = &result.parser_task[0];
        assert_eq!(routed.context.node_id.as_deref(), Some("node_b"));
        assert_eq!(routed.metadata.get("user_id").and_then(|v| v.as_str()), Some("abc"));
        assert_eq!(routed.metadata.get("page").and_then(|v| v.as_i64()), Some(42));

        // Now feed metadata into execute_generate to verify it reaches node_b
        let ctx = Some(routed.context.clone());
        let _ = proc.execute_generate(
            Arc::new(ModuleConfig::default()),
            routed.metadata.clone(),
            None,
            ctx,
            None,
        ).await.unwrap();

        let captured = node_b.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].get("user_id").and_then(|v| v.as_str()), Some("abc"));
        assert_eq!(captured[0].get("page").and_then(|v| v.as_i64()), Some(42));
    }

    /// Advance-gate path: when parser returns empty parser_task, synthesized
    /// tasks should carry forward the response's task metadata.
    #[tokio::test]
    async fn advance_gate_forwards_response_task_metadata() {
        // node_a parser returns empty parser_task (advance gate triggers)
        let node_a = Arc::new(CapturingNode::new(TaskOutputEvent::default()));
        let node_b = Arc::new(CapturingNode::new(TaskOutputEvent::default()));

        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef { node_id: "node_a".into(), node: node_a.clone(), placement_override: None, policy_override: None, tags: vec![] },
                ModuleDagNodeDef { node_id: "node_b".into(), node: node_b.clone(), placement_override: None, policy_override: None, tags: vec![] },
            ],
            edges: vec![ModuleDagEdgeDef { from: "node_a".into(), to: "node_b".into() }],
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
        assert_eq!(result.parser_task.len(), 1);
        let synthesized = &result.parser_task[0];
        assert_eq!(synthesized.context.node_id.as_deref(), Some("node_b"));
        assert_eq!(
            synthesized.metadata.get("session_id").and_then(|v| v.as_str()),
            Some("s1"),
            "advance-gate synthesized task should carry response.metadata.task"
        );

        // Verify it reaches node_b's generate
        let _ = proc.execute_generate(
            Arc::new(ModuleConfig::default()),
            synthesized.metadata.clone(),
            None,
            Some(synthesized.context.clone()),
            None,
        ).await.unwrap();

        let captured = node_b.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].get("session_id").and_then(|v| v.as_str()), Some("s1"));
    }

    /// Fan-out: metadata should be replicated to each successor.
    #[tokio::test]
    async fn fanout_replicates_metadata_to_all_successors() {
        let task = TaskParserEvent::from(&make_response("node_a", Map::new()))
            .add_meta("key", "shared_value");

        let node_a = Arc::new(CapturingNode::new(
            TaskOutputEvent::default().with_task(task),
        ));
        let node_b = Arc::new(CapturingNode::new(TaskOutputEvent::default()));
        let node_c = Arc::new(CapturingNode::new(TaskOutputEvent::default()));

        let def = ModuleDagDefinition {
            nodes: vec![
                ModuleDagNodeDef { node_id: "node_a".into(), node: node_a.clone(), placement_override: None, policy_override: None, tags: vec![] },
                ModuleDagNodeDef { node_id: "node_b".into(), node: node_b.clone(), placement_override: None, policy_override: None, tags: vec![] },
                ModuleDagNodeDef { node_id: "node_c".into(), node: node_c.clone(), placement_override: None, policy_override: None, tags: vec![] },
            ],
            edges: vec![
                ModuleDagEdgeDef { from: "node_a".into(), to: "node_b".into() },
                ModuleDagEdgeDef { from: "node_a".into(), to: "node_c".into() },
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
        assert_eq!(result.parser_task.len(), 2);
        for task in &result.parser_task {
            assert_eq!(task.metadata.get("key").and_then(|v| v.as_str()), Some("shared_value"));
        }
        let mut targets: Vec<_> = result.parser_task.iter()
            .map(|t| t.context.node_id.clone().unwrap())
            .collect();
        targets.sort();
        assert_eq!(targets, vec!["node_b", "node_c"]);
    }

    /// with_meta replaces all metadata; add_meta appends.
    #[tokio::test]
    async fn add_meta_appends_with_meta_replaces() {
        let response = make_response("node_a", Map::new());
        let task = TaskParserEvent::from(&response)
            .add_meta("a", 1)
            .add_meta("b", 2);
        assert_eq!(task.metadata.len(), 2);

        let mut new_map = Map::new();
        new_map.insert("c".into(), serde_json::Value::from(3));
        let task2 = task.with_meta(new_map);
        assert_eq!(task2.metadata.len(), 1);
        assert_eq!(task2.metadata.get("c").and_then(|v| v.as_i64()), Some(3));
    }

    /// From<&Response> for TaskParserEvent forwards task metadata from
    /// the response's MetaData.task slot.
    #[tokio::test]
    async fn from_response_forwards_task_metadata() {
        let mut task_meta = Map::new();
        task_meta.insert("forwarded".into(), serde_json::Value::Bool(true));
        let response = make_response("node_a", task_meta);

        let parsed: TaskParserEvent = (&response).into();
        assert_eq!(
            parsed.metadata.get("forwarded").and_then(|v| v.as_bool()),
            Some(true),
            "From<&Response> should forward metadata.task into TaskParserEvent.metadata"
        );
    }

    /// From<&Response> with empty metadata still produces empty map.
    #[tokio::test]
    async fn from_response_empty_metadata_stays_empty() {
        let response = make_response("node_a", Map::new());
        let parsed: TaskParserEvent = (&response).into();
        assert!(parsed.metadata.is_empty());
    }
}
