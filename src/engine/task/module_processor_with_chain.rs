#![allow(unused)]
// Chain-oriented module processor without a global state machine index.
// Design principles:
// - Step routing uses ExecutionMark carried by Request/Response (module_id + step_idx).
// - Fallback traces prior request via prefix_request; requests are persisted by request.id.
// - Generation failure allows at most one fallback per (step, prefix) gate.
// - Parser failure emits ErrorTaskModel tagged with current step context.
// - If parser succeeds but yields no ParserTaskModel while next step exists,
//   a one-shot advance gate can synthesize a placeholder task.

use crate::common::model::ExecutionMark;
use crate::common::model::chain_key;
use crate::common::interface::module::{ModuleNodeTrait, SyncBoxStream};
use crate::common::model::{ ModuleConfig, Request, Response};
use crate::errors::{RequestError, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;
use futures::StreamExt;
use crate::cacheable::{CacheAble, CacheService};
use crate::common::model::login_info::LoginInfo;
use crate::common::model::message::{ErrorTaskModel, ParserData, ParserTaskModel, TaskModel};
// distributed atomic gating only; no in-memory state needed

// --- Sync State Structs ---

#[derive(Serialize, Deserialize)]
pub struct AdvanceGate(pub bool);

impl CacheAble for AdvanceGate {
    fn field() -> impl AsRef<str> {
        "chain_advance"
    }
}

#[derive(Serialize, Deserialize)]
pub struct FallbackGate(pub bool);

impl CacheAble for FallbackGate {
    fn field() -> impl AsRef<str> {
        "chain_fallback"
    }
}

#[derive(Serialize, Deserialize)]
pub struct StopSignal(pub bool);

impl CacheAble for StopSignal {
    fn field() -> impl AsRef<str> {
        "chain_stop"
    }
}

#[derive(Serialize, Deserialize)]
pub struct RequeueData(pub Request);

impl CacheAble for RequeueData {
    fn field() -> impl AsRef<str> {
        "chain_requeue"
    }
}

// --------------------------

/// Execution status snapshot for diagnostics and observability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStatus {
    pub module_id: String,
    pub run_id: Uuid,
    pub total_steps: usize,
    pub active_gates: Vec<GateInfo>,
}

/// Gate status details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateInfo {
    pub gate_type: String,
    pub step_idx: Option<usize>,
    pub field_name: String,
    pub value: serde_json::Value,
}

/// Per-step lightweight statistics payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepStats {
    pub step_idx: usize,
    pub module_id: String,
    pub run_id: Uuid,
}

/// Chain processor that routes execution by per-request context instead of global cursor.
///
/// Key behavior:
/// - ExecutionMark drives node selection.
/// - prefix_request enables fallback tracing.
/// - Request persistence allows recovering previous hop on failure.
#[derive(Clone)]
pub struct ModuleProcessorWithChain {
    module_id: String,
    cache: Arc<CacheService>,
    steps: Arc<RwLock<Vec<Arc<dyn ModuleNodeTrait>>>>,
    run_id: Uuid,
    ttl: u64,
    stop: Arc<RwLock<bool>>,
    last_stop_check: Arc<AtomicU64>,
}

impl ModuleProcessorWithChain {
    /// Creates a chain processor scoped to one module + run id.
    pub fn new<M: Into<String>>(
        module_id: M,
        state: Arc<CacheService>,
        run_id: Uuid,
        ttl: u64,
    ) -> Self {
        Self {
            module_id: module_id.into(),
            cache: state,
            steps: Arc::new(RwLock::new(Vec::new())),
            run_id,
            ttl,
            stop: Arc::new(RwLock::new(false)),
            last_stop_check: Arc::new(AtomicU64::new(0)),
        }
    }

    // Internal helper note: single-step request generation is delegated by execute_request_impl.

    /// Adds one step node. Insertion order equals execution order.
    pub async fn add_step_node(&self, node: Arc<dyn ModuleNodeTrait>) {
        let mut steps_guard = self.steps.write().await;
        steps_guard.push(node);
        debug!(
            "[chain] module={} run={} add_step_node: total_steps={}",
            self.module_id,
            self.run_id,
            steps_guard.len()
        );
    }

    fn advance_key(&self) -> String {
        chain_key::execution_state_key(self.run_id, &self.module_id)
    }

    fn advance_key_legacy(&self) -> String {
        chain_key::legacy_execution_state_key(self.run_id, &self.module_id)
    }

    /// Persists request by request.id for fallback/recovery lookup.
    async fn save_request(&self, req: &Request) -> Result<()> {
        let id = req.id.to_string();
        req.send(&id,&self.cache).await.ok();

        debug!(
            "[chain] module={} run={} save_request: id={} step={} prefix={}",
            self.module_id,
            self.run_id,
            id,
            req.context.step_idx.unwrap_or(0),
            req.prefix_request
        );
        Ok(())
    }

    // One-shot advance marker used when synthesizing placeholder task progression.
    async fn try_mark_step_advanced_once(&self, step_idx: usize) -> Result<bool> {
        let id_str = chain_key::module_step_advance_once_key(self.run_id, &self.module_id, step_idx);
        let legacy_id_str =
            chain_key::legacy_module_step_advance_once_key(self.run_id, &self.module_id, step_idx);

        if AdvanceGate::sync(&id_str, &self.cache).await?.is_some()
            || AdvanceGate::sync(&legacy_id_str, &self.cache).await?.is_some()
        {
            return Ok(false);
        }
        
        let gate = AdvanceGate(true);
        // Use send_nx for atomic check-and-set with explicit TTL
        let won = gate.send_nx(&id_str, &self.cache, Some(std::time::Duration::from_secs(self.ttl))).await?;

        debug!(
            "[chain] module={} run={} try_mark_step_advanced_once result: step_idx={} won={}",
            self.module_id, self.run_id, step_idx, won
        );
        Ok(won)
    }

    // One-shot fallback marker to prevent infinite fallback loops.
    async fn try_allow_fallback_once(&self, step_idx: usize, prefix: &Uuid) -> Result<bool> {
        let id_str =
            chain_key::module_step_fallback_once_key(self.run_id, &self.module_id, step_idx, *prefix);
        let legacy_id_str = chain_key::legacy_module_step_fallback_once_key(
            self.run_id,
            &self.module_id,
            step_idx,
            *prefix,
        );

        if FallbackGate::sync(&id_str, &self.cache).await?.is_some()
            || FallbackGate::sync(&legacy_id_str, &self.cache).await?.is_some()
        {
            return Ok(false);
        }
        
        let gate = FallbackGate(true);
        // Use send_nx for atomic check-and-set
        let won = gate.send_nx(&id_str, &self.cache, Some(std::time::Duration::from_secs(self.ttl))).await?;

        debug!(
            "[chain] module={} run={} try_allow_fallback_once result: step_idx={} prefix={} allowed={}",
            self.module_id, self.run_id, step_idx, prefix, won
        );
        Ok(won)
    }

    async fn load_request(&self, id: &Uuid) -> Result<Option<Request>> {
        let id_str = id.to_string();
        if let Ok(Some(req)) = Request::sync(&id_str,&self.cache).await{
            return Ok(Some(req));
        }
        debug!(
            "[chain] module={} run={} load_request: id={} miss",
            self.module_id, self.run_id, id
        );
        Ok(None)
    }

    /// Persists requeue intent and returns recoverable previous request when available.
    pub async fn mark_requeue(&self, id: &Uuid) -> Result<Option<Request>> {
        if id.is_nil() {
            // First-step/non-chain path has no previous request to recover.
            return Ok(None);
        }
        debug!(
            "[chain] module={} run={} mark_requeue: id={}",
            self.module_id, self.run_id, id
        );
        if let Some(req) = self.load_request(id).await? {
            // Persist RequeueData
            let data = RequeueData(req.clone());
            data.send(&id.to_string(), &self.cache).await?;

            debug!(
                "[chain] module={} run={} mark_requeue: id={} persisted",
                self.module_id, self.run_id, id
            );
            return Ok(Some(req));
        }
        debug!(
            "[chain] module={} run={} mark_requeue: id={} previous_request_not_found",
            self.module_id, self.run_id, id
        );
        Ok(None)
    }

    pub async fn get_total_steps(&self) -> usize {
        self.steps.read().await.len()
    }

    /// Returns current execution status summary.
    pub async fn get_execution_status(&self) -> Result<ExecutionStatus> {
        let steps_count = self.steps.read().await.len();
        let active_gates = self.query_active_gates().await?;

        Ok(ExecutionStatus {
            module_id: self.module_id.clone(),
            run_id: self.run_id,
            total_steps: steps_count,
            active_gates,
        })
    }

    /// Scans and returns active advance/fallback gates.
    async fn query_active_gates(&self) -> Result<Vec<GateInfo>> {
        let mut gates = Vec::new();
        let id_base = self.advance_key(); // run:{run}:module:{mod}
        let ns = self.cache.namespace();

        // 1. AdvanceGate: format {ns}:chain_advance:run:{run}:module:{mod}:step:{step}
        if let Ok(adv_keys) = AdvanceGate::scan(&format!("{}*", id_base), &self.cache).await {
            let adv_field = AdvanceGate::field();
            let adv_prefix = format!("{}:{}:", ns, adv_field.as_ref());
    
            for key in adv_keys {
                if let Some(id) = key.strip_prefix(&adv_prefix)
                    && let Some(pos) = id.rfind(":step:")
                        && let Ok(step_idx) = id[pos+6..].parse::<usize>() {
                            // OPTIMIZATION: Assume existence means true, skip individual GETs
                            gates.push(GateInfo {
                                gate_type: "advance".to_string(),
                                step_idx: Some(step_idx),
                                field_name: adv_field.as_ref().to_string(),
                                value: serde_json::Value::Bool(true),
                            });
                        }
            }
        }

        // 2. FallbackGate: format {ns}:chain_fallback:run:{run}:module:{mod}:step:{step}:prefix:{uuid}
        if let Ok(fb_keys) = FallbackGate::scan(&format!("{}*", id_base), &self.cache).await {
            let fb_field = FallbackGate::field();
            let fb_prefix = format!("{}:{}:", ns, fb_field.as_ref());
    
            for key in fb_keys {
                if let Some(id) = key.strip_prefix(&fb_prefix)
                     && let Some(step_pos) = id.find(":step:") {
                        let rest = &id[step_pos+6..];
                        if let Some(prefix_pos) = rest.find(":prefix:") {
                            let step_str = &rest[..prefix_pos];
                            if let Ok(step_idx) = step_str.parse::<usize>() {
                                // OPTIMIZATION: Assume existence means true
                                gates.push(GateInfo {
                                    gate_type: "fallback".to_string(),
                                    step_idx: Some(step_idx),
                                    field_name: fb_field.as_ref().to_string(),
                                    value: serde_json::Value::Bool(true),
                                });
                            }
                        }
                    }
            }
        }

        debug!(
            "[chain] module={} run={} query_active_gates: found {} gates",
            self.module_id, self.run_id, gates.len()
        );
        Ok(gates)
    }

    /// Returns step-level stats payload for one step index.
    pub async fn get_step_stats(&self, step_idx: usize) -> Result<StepStats> {
        let steps_count = self.steps.read().await.len();

        if step_idx >= steps_count {
            return Err(RequestError::BuildFailed(
                format!("Step {} exceeds total steps {}", step_idx, steps_count).into()
            ).into());
        }

        Ok(StepStats {
            step_idx,
            module_id: self.module_id.clone(),
            run_id: self.run_id,
            // Extension point for richer metrics (attempt count, success rate, latency).
        })
    }

    // Removed legacy steps_len traversal path; chain progression is context-driven.

    /// Returns whether a parser task targets current module.
    fn is_task_for_current_module(
        &self,
        ctx: &ExecutionMark,
        task_modules: &[String],
    ) -> bool {
        if let Some(mid) = &ctx.module_id {
            return mid == &self.module_id;
        }
        // If context has no module id, fallback to task module list.
        task_modules.iter().any(|m| m == &self.module_id)
    }

    /// Determines whether chain should terminate at last step to avoid loops.
    pub fn should_terminate_at_last_step(
        &self,
        current_step: usize,
        task_step: usize,
        total_steps: usize,
        stay_current: bool,
    ) -> bool {
        let is_last_step = current_step + 1 >= total_steps;
        if !is_last_step {
            return false;
        }

        // Last-step loop guard.
        task_step <= current_step && !stay_current
    }

    /// Resolves effective execution context from optional ctx + prefix request.
    async fn resolve_execution_context(
        &self,
        ctx: Option<ExecutionMark>,
        prefix_request: Option<Uuid>,
    ) -> Result<(ExecutionMark, Uuid)> {
        let prefix = prefix_request.unwrap_or(Uuid::nil());

        // Case 1: infer next step from prefix request when allowed.
        let should_infer_from_prefix = if let Some(ref existing_ctx) = ctx {
            // Infer only when module matches and step is absent.
            existing_ctx.module_id.as_ref().map(|m| m == &self.module_id).unwrap_or(false)
                && existing_ctx.step_idx.is_none()
                && !prefix.is_nil()
        } else {
            // No ctx: infer when prefix exists.
            !prefix.is_nil()
        };

        if should_infer_from_prefix {
            debug!(
                "[chain] module={} run={} resolve_execution_context: infer next from prefix id={}",
                self.module_id, self.run_id, prefix
            );
            return if let Some(prev) = self.load_request(&prefix).await? {
                let prev_step = prev.context.step_idx.unwrap_or(0);
                let next_step = prev_step.saturating_add(1);
                debug!(
                    "[chain] module={} run={} resolve_execution_context: prev_step={} -> next_step={}",
                    self.module_id, self.run_id, prev_step, next_step
                );
                let inferred_ctx = prev
                    .context
                    .clone()
                    .with_module_id(self.module_id.clone())
                    .with_step_idx(next_step);
                Ok((inferred_ctx, prefix))
            } else {
                Err(RequestError::NotFound.into())
            }
        }

        // Case 2: normalize provided context.
        let mut effective_ctx = match ctx {
            Some(ctx) => {
            // Rebuild context when module_id mismatches current module.
                if ctx.module_id.as_ref().map(|m| m == &self.module_id).unwrap_or(false) {
                    ctx
                } else {
                    debug!(
                        "[chain] module={} run={} resolve_execution_context: module_id mismatch, reset to step 0",
                        self.module_id, self.run_id
                    );
                    ExecutionMark::default()
                        .with_module_id(self.module_id.clone())
                        .with_step_idx(0)
                }
            }
            None => {
                ExecutionMark::default()
                    .with_module_id(self.module_id.clone())
                    .with_step_idx(0)
            }
        };

        // Ensure module_id and step_idx are set.
        if effective_ctx.module_id.is_none() {
            effective_ctx = effective_ctx.with_module_id(self.module_id.clone());
        }
        if effective_ctx.step_idx.is_none() {
            effective_ctx = effective_ctx.with_step_idx(0);
        }

        Ok((effective_ctx, prefix))
    }

    /// Unified request-generation entry for chain mode.
    ///
    /// Rules:
    /// - when prefix is provided and context lacks explicit step, infer next step;
    /// - otherwise use ctx.step_idx (default 0).
    pub async fn execute_request(
        &self,
        config: Arc<ModuleConfig>,
        meta: serde_json::Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
        ctx: Option<ExecutionMark>,
        prefix_request: Option<Uuid>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        if self.is_stopped().await? {
            return Ok(Box::pin(futures::stream::empty()));
        }
        // Resolve effective execution context.
        let (effective_ctx, effective_prefix) = self
            .resolve_execution_context(ctx, prefix_request)
            .await?;

        debug!(
            "[chain] module={} run={} execute_request: effective_step={} prefix={}",
            self.module_id,
            self.run_id,
            effective_ctx.step_idx.unwrap_or(0),
            effective_prefix
        );

        self.execute_request_impl(config, meta, login_info, effective_ctx, effective_prefix)
            .await
    }

    /// Generates requests for one resolved step.
    ///
    /// On failure with non-nil prefix, a one-shot fallback may return previous request.
    async fn execute_request_impl(
        &self,
        config: Arc<ModuleConfig>,
        meta: serde_json::Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
        ctx: ExecutionMark,
        prefix_request: Uuid,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let step_idx = ctx.step_idx.unwrap_or(0) as usize;
        debug!(
            "[chain] module={} run={} execute_request_impl: step={} prefix={}",
            self.module_id, self.run_id, step_idx, prefix_request
        );
        let step_node = {
            let steps = self.steps.read().await;
            if step_idx >= steps.len() {
                return Ok(Box::pin(futures::stream::empty()));
            }
            steps[step_idx].clone()
        };
        // One-time advancement logic lives in execute_parser placeholder synthesis path.
        let gen_ctx = {
            // Ensure module_id and step_idx are set
            let mut mark = ctx.clone();
            if mark.module_id.is_none() {
                mark = mark.with_module_id(self.module_id.clone());
            }
            if mark.step_idx.is_none() {
                mark = mark.with_step_idx(step_idx as u32);
            }
            mark
        };
        match step_node
            .generate(config.clone(), meta, login_info.clone())
            .await
        {
            Ok(stream) => {
                debug!(
                    "[chain] module={} run={} execute_request_impl: generated stream for step {}",
                    self.module_id,
                    self.run_id,
                    step_idx
                );

                let gen_ctx = gen_ctx.clone();
                let run_id = self.run_id;
                let module_id = self.module_id.clone();
                let cache = self.cache.clone();

                let stream = stream.map(move |mut req| {
                    // tag independent context and run id
                    req.context = gen_ctx.clone();
                    req.run_id = run_id;
                    // chain back-point
                    req.prefix_request = prefix_request;
                    // Ensure request has a unique id before persist
                    if req.id.is_nil() {
                        req.id = Uuid::now_v7();
                    }
                    
                    // Background persistence for chain fallback support
                    // Optimized: perform serialization and cache set in background to avoid blocking main loop
                    let req_save = req.clone();
                    let cache_save = cache.clone();
                    tokio::spawn(async move {
                         let id_str = req_save.id.to_string();
                         // Offload serialization to blocking thread
                         let res = tokio::task::spawn_blocking(move || {
                             serde_json::to_vec(&req_save)
                         }).await;
                         
                         if let Ok(Ok(bytes)) = res {
                             // Use Request::cache_id to ensure consistent key generation
                             let key = Request::cache_id(&id_str, &cache_save);
                             // cache_save.set now handles compression on blocking thread as well
                             let _ = cache_save.set(&key, &bytes, cache_save.default_ttl()).await;
                         }
                    });

                    info!(
                        "[chain] module={} run={} execute_request_impl: generated request id={} step={} prefix={}",
                        module_id,
                        run_id,
                        req.id,
                        req.context.step_idx.unwrap_or(0),
                        req.prefix_request
                    );
                    req
                });
                Ok(Box::pin(stream))
            }
            Err(e) => {
                // Generation failure recovery policy.
                if prefix_request.is_nil() {
                    // First step: no fallback target.
                    warn!(
                        "[chain] module={} run={} execute_request_impl: generation error at first step={}, no prefix, err={}",
                        self.module_id, self.run_id, step_idx, e
                    );
                    Err(e)
                } else {
                    // Non-first step: try one-shot fallback to previous request.
                    if self
                        .try_allow_fallback_once(step_idx, &prefix_request)
                        .await?
                    {
                        match self.load_request(&prefix_request).await? {
                            Some(prev) => {
                                debug!(
                                    "[chain] module={} run={} execute_request_impl: generation error at step={}, fallback allowed -> return previous request id={}",
                                    self.module_id, self.run_id, step_idx, prefix_request
                                );
                                Ok(Box::pin(futures::stream::once(async move { prev })))
                            }
                            None => {
                                debug!(
                                    "[chain] module={} run={} execute_request_impl: generation error at step={}, fallback allowed but previous not found, err={}",
                                    self.module_id, self.run_id, step_idx, e
                                );
                                Err(e)
                            }
                        }
                    } else {
                        // Fallback already consumed.
                        warn!(
                            "[chain] module={} run={} execute_request_impl: generation error at step={}, fallback suppressed (already done), err={}",
                            self.module_id, self.run_id, step_idx, e
                        );
                        Err(e)
                    }
                }
            }
        }
    }

    /// Parses response at routed step and decides next task progression.
    ///
    /// Success path may advance context or synthesize placeholder task via one-shot gate.
    /// Failure path emits ErrorTaskModel with current-step context for precise retry.
    pub async fn execute_parser(
        &self,
        response: Response,
        config: Option<Arc<ModuleConfig>>,
    ) -> Result<ParserData> {
        // Route strictly by the step_idx from response.context
        let step_idx = response.context.step_idx.unwrap_or(0) as usize;
        debug!(
            "[chain] module={} run={} execute_parser: step={} prefix={}",
            self.module_id, self.run_id, step_idx, response.prefix_request
        );
        let step_node = {
            let steps = self.steps.read().await;
            if step_idx >= steps.len() {
                return Ok(ParserData::default());
            }
            steps[step_idx].clone()
        };
        match step_node.parser(response.clone(), config).await {
            Ok(mut data) => {
                if data.stop.unwrap_or(false) {
                    self.set_stopped().await?;
                }
                // Ensure chain prefix points to the current request (request.id carried in response.prefix_request)
                if let Some(ref mut task) = data.parser_task {
                    // 1) Bind fallback pointer to the current-step request id.
                    task.prefix_request = response.prefix_request;

                    // 2) Defensive step advancement:
                    //    - ensure module_id exists for same-module continuation,
                    //    - advance to next step when parser didn't explicitly advance.
                    let total = {
                        let steps = self.steps.read().await;
                        steps.len()
                    };
                    // Base adjustments on task context to avoid overriding cross-module handoff.
                    let mut next_ctx = task.context.clone();
                    let task_modules = task
                        .account_task
                        .module
                        .clone()
                        .unwrap_or_default();

                    // Check whether task targets current module.
                    let same_module = self.is_task_for_current_module(&next_ctx, &task_modules);

                    // Fill module_id only for same-module path.
                    if same_module && next_ctx.module_id.is_none() {
                        next_ctx = next_ctx.with_module_id(self.module_id.clone());
                    }

                    // Current/default step index.
                    let task_step = next_ctx.step_idx.unwrap_or(step_idx as u32) as usize;
                    let mut desired_step = task_step;

                    if same_module {
                        // Honor stay_current_step: do not advance when explicitly requested.
                        if !next_ctx.stay_current_step {
                            // Auto-advance by one when parser did not explicitly advance.
                            if task_step <= step_idx {
                                let maybe_next = step_idx + 1;
                                if maybe_next < total {
                                    desired_step = maybe_next;
                                }
                            }
                        } else {
                            desired_step = step_idx;
                        }

                        // Apply last-step loop guard.
                        if self.should_terminate_at_last_step(
                            step_idx,
                            task_step,
                            total,
                            next_ctx.stay_current_step,
                        ) {
                            debug!(
                                "[chain] module={} run={} execute_parser: last step {} reached, terminating to avoid infinite loop",
                                self.module_id, self.run_id, step_idx
                            );
                            data.parser_task = None;
                            return Ok(data);
                        }
                    } else {
                        // Cross-module task: leave context untouched for upper-layer routing.
                        task.context = next_ctx;
                        debug!(
                            "[chain] module={} run={} execute_parser: task targets other module -> pass-through",
                            self.module_id, self.run_id
                        );
                        return Ok(data);
                    }

                    // Write context only when changed.
                    if next_ctx.step_idx.map(|s| s as usize) != Some(desired_step) {
                        next_ctx = next_ctx.with_step_idx(desired_step as u32);
                        task.context = next_ctx;
                        debug!(
                            "[chain] module={} run={} execute_parser: task present, advance context to step {} (from resp step {})",
                            self.module_id, self.run_id, desired_step, step_idx
                        );
                    } else {
                        // Keep module id consistent even without step movement.
                        task.context = next_ctx;
                        debug!(
                            "[chain] module={} run={} execute_parser: task present, keep context at step {}",
                            self.module_id, self.run_id, desired_step
                        );
                    }
                } else {
                    // No ParserTaskModel: optionally synthesize placeholder task for next step.
                    let total = {
                        let steps = self.steps.read().await;
                        steps.len()
                    };
                    let next_idx = step_idx + 1;
                    if next_idx < total {
                        debug!(
                            "[chain] module={} run={} execute_parser: no task, consider advance to step {} via advance gate",
                            self.module_id, self.run_id, next_idx
                        );
                        // Progression is gate-driven rather than prefix-driven.
                        if self.try_mark_step_advanced_once(step_idx).await? {
                            let base: ParserTaskModel = (&response).into();
                            let next_ctx = ExecutionMark::default()
                                .with_module_id(self.module_id.clone())
                                .with_step_idx(next_idx as u32);
                            let mut next_task = base.with_context(next_ctx);
                            next_task.prefix_request = response.prefix_request;
                            data = data.with_task(next_task);
                            debug!(
                                "[chain] module={} run={} execute_parser: advance gate won -> synthesize placeholder to step {}",
                                self.module_id, self.run_id, next_idx
                            );
                        } else {
                            debug!(
                                "[chain] module={} run={} execute_parser: advance gate lost -> skip synthesizing step {}",
                                self.module_id, self.run_id, next_idx
                            );
                        }
                    }
                }
                Ok(data)
            }
            Err(e) => {
                // Parser failure: emit ErrorTaskModel for precise same-step retry.
                let step_idx_u32 = response.context.step_idx.unwrap_or(0);
                // Preserve metadata as-is; avoid retry-step metadata pollution.
                let meta =
                    serde_json::to_value(&response.metadata).unwrap_or(serde_json::json!({}));
                let error_task =ErrorTaskModel {
                    id: response.id,
                    account_task: TaskModel {
                        account: response.account.clone(),
                        platform: response.platform.clone(),
                        module: Some(vec![response.module.clone()]),
                        run_id: response.run_id,
                        priority: response.priority,
                    },
                    error_msg: e.to_string(),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    metadata: meta,
                    context: response
                        .context
                        .clone()
                        .with_module_id(self.module_id.clone())
                        .with_step_idx(step_idx_u32)
                        .with_stay_current_step(true),
                    run_id: response.run_id,
                    prefix_request: response.prefix_request,
                };
                warn!(
                    "[chain] module={} run={} execute_parser: parser error at step {} -> emit ErrorTaskModel",
                    self.module_id, self.run_id, step_idx_u32
                );
                Ok(ParserData::default().with_error(error_task))
            }
        }
    }

    async fn is_stopped(&self) -> Result<bool> {
        // Check local cache first
        if *self.stop.read().await {
            return Ok(true);
        }

        // Rate limit Redis checks (e.g., every 1 seconds) to avoid I/O in hot path
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let last_check = self.last_stop_check.load(Ordering::Relaxed);
        
        if now < last_check + 1 {
            return Ok(false);
        }

        // Check distributed state
        let id_str = self.advance_key();
        let legacy_id_str = self.advance_key_legacy();
        if let Ok(Some(signal)) = StopSignal::sync(&id_str, &self.cache).await
            && signal.0 {
                // Update local cache
                *self.stop.write().await = true;
                return Ok(true);
            }
        if let Ok(Some(signal)) = StopSignal::sync(&legacy_id_str, &self.cache).await
            && signal.0 {
                *self.stop.write().await = true;
                return Ok(true);
        }
        
        // Update last check time
        self.last_stop_check.store(now, Ordering::Relaxed);
        Ok(false)
    }

    async fn set_stopped(&self) -> Result<()> {
        // Update local cache
        *self.stop.write().await = true;

        // Update distributed state
        let id_str = self.advance_key();
        let legacy_id_str = self.advance_key_legacy();
        StopSignal(true).send(&id_str, &self.cache).await?;
        let _ = StopSignal(true).send(&legacy_id_str, &self.cache).await;
        debug!(
            "[chain] module={} run={} set_stopped: marked as stopped",
            self.module_id, self.run_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::cacheable::CacheAble;
    use crate::cacheable::CacheService;
    use crate::common::interface::ModuleNodeTrait;
    use crate::common::model::meta::MetaData;
    use crate::common::model::request::RequestMethod;
    use futures::StreamExt;

    enum ParserBehavior {
        ReturnTask,
        ReturnError,
    }

    struct TestNode {
        behavior: ParserBehavior,
        module_id: String,
    }

    struct GenerateErrorNode;

    #[async_trait]
    impl ModuleNodeTrait for GenerateErrorNode {
        async fn generate(
            &self,
            _config: Arc<ModuleConfig>,
            _params: serde_json::Map<String, serde_json::Value>,
            _login_info: Option<LoginInfo>,
        ) -> Result<SyncBoxStream<'static, Request>> {
            Err(RequestError::BuildFailed("generate failed".into()).into())
        }

        async fn parser(
            &self,
            _response: Response,
            _config: Option<Arc<ModuleConfig>>,
        ) -> Result<ParserData> {
            Ok(ParserData::default())
        }
    }

    #[async_trait]
    impl ModuleNodeTrait for TestNode {
        async fn generate(
            &self,
            _config: Arc<ModuleConfig>,
            _params: serde_json::Map<String, serde_json::Value>,
            _login_info: Option<LoginInfo>,
        ) -> Result<SyncBoxStream<'static, Request>> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn parser(
            &self,
            response: Response,
            _config: Option<Arc<ModuleConfig>>,
        ) -> Result<ParserData> {
            match self.behavior {
                ParserBehavior::ReturnTask => {
                    let task = ParserTaskModel::from(&response)
                        .with_context(response.context.clone().with_module_id(self.module_id.clone()));
                    Ok(ParserData::default().with_task(task))
                }
                ParserBehavior::ReturnError => {
                    Err(RequestError::BuildFailed("parser failed".into()).into())
                }
            }
        }
    }

    fn build_response(module: &str, step_idx: u32, prefix_request: Uuid, run_id: Uuid) -> Response {
        Response {
            id: Uuid::now_v7(),
            platform: "pf".to_string(),
            account: "acc".to_string(),
            module: module.to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: vec![],
            storage_path: None,
            headers: vec![],
            task_retry_times: 0,
            metadata: MetaData::default(),
            download_middleware: vec![],
            data_middleware: vec![],
            task_finished: false,
            context: ExecutionMark::default().with_step_idx(step_idx),
            run_id,
            prefix_request,
            request_hash: None,
            priority: Default::default(),
        }
    }

    #[tokio::test]
    async fn execute_parser_success_advances_to_next_step_and_keeps_prefix() {
        let run_id = Uuid::now_v7();
        let module_id = "acc-pf-m1".to_string();
        let processor = ModuleProcessorWithChain::new(
            module_id.clone(),
            Arc::new(CacheService::new(None, "test".to_string(), None, None)),
            run_id,
            60,
        );
        processor
            .add_step_node(Arc::new(TestNode {
                behavior: ParserBehavior::ReturnTask,
                module_id: module_id.clone(),
            }))
            .await;
        processor
            .add_step_node(Arc::new(TestNode {
                behavior: ParserBehavior::ReturnTask,
                module_id: module_id.clone(),
            }))
            .await;

        let prefix = Uuid::now_v7();
        let response = build_response("m1", 0, prefix, run_id);
        let data = processor.execute_parser(response, None).await.expect("execute_parser should succeed");

        let task = data.parser_task.expect("parser task should be produced");
        assert_eq!(task.context.step_idx, Some(1));
        assert_eq!(task.context.module_id.as_deref(), Some(module_id.as_str()));
        assert_eq!(task.prefix_request, prefix);
    }

    #[tokio::test]
    async fn execute_parser_failure_emits_error_task_with_stay_current_step() {
        let run_id = Uuid::now_v7();
        let module_id = "acc-pf-m1".to_string();
        let processor = ModuleProcessorWithChain::new(
            module_id.clone(),
            Arc::new(CacheService::new(None, "test".to_string(), None, None)),
            run_id,
            60,
        );
        processor
            .add_step_node(Arc::new(TestNode {
                behavior: ParserBehavior::ReturnError,
                module_id: module_id.clone(),
            }))
            .await;

        let prefix = Uuid::now_v7();
        let response = build_response("m1", 0, prefix, run_id);
        let data = processor.execute_parser(response, None).await.expect("execute_parser should return parser data with error task");

        let err_task = data.error_task.expect("error task should be produced");
        assert_eq!(err_task.context.step_idx, Some(0));
        assert_eq!(err_task.context.module_id.as_deref(), Some(module_id.as_str()));
        assert!(err_task.context.stay_current_step);
        assert_eq!(err_task.prefix_request, prefix);
        assert!(!err_task.error_msg.is_empty());
    }

    #[tokio::test]
    async fn execute_request_fallback_is_allowed_once_then_blocked_for_same_step_and_prefix() {
        let run_id = Uuid::now_v7();
        let module_id = "acc-pf-m1".to_string();
        let cache = Arc::new(CacheService::new(None, "test".to_string(), None, None));
        let processor = ModuleProcessorWithChain::new(module_id.clone(), cache.clone(), run_id, 60);

        processor
            .add_step_node(Arc::new(TestNode {
                behavior: ParserBehavior::ReturnTask,
                module_id: module_id.clone(),
            }))
            .await;
        processor.add_step_node(Arc::new(GenerateErrorNode)).await;

        let prefix = Uuid::now_v7();
        let mut prev_req = Request::new("http://example.local", RequestMethod::Get.as_ref());
        prev_req.id = prefix;
        prev_req.account = "acc".to_string();
        prev_req.platform = "pf".to_string();
        prev_req.module = "m1".to_string();
        prev_req.context = ExecutionMark::default()
            .with_module_id(module_id.clone())
            .with_step_idx(0);
        prev_req.run_id = run_id;
        prev_req.prefix_request = Uuid::nil();
        prev_req
            .send(&prefix.to_string(), &cache)
            .await
            .expect("persist previous request for fallback");

        let ctx = Some(
            ExecutionMark::default()
                .with_module_id(module_id.clone())
                .with_step_idx(1),
        );

        let first = processor
            .execute_request(
                Arc::new(ModuleConfig::default()),
                serde_json::Map::new(),
                None,
                ctx.clone(),
                Some(prefix),
            )
            .await
            .expect("first fallback should succeed");
        let mut first_stream = first;
        let recovered = first_stream
            .next()
            .await
            .expect("fallback should return previous request");
        assert_eq!(recovered.id, prefix);

        match processor
            .execute_request(
                Arc::new(ModuleConfig::default()),
                serde_json::Map::new(),
                None,
                ctx,
                Some(prefix),
            )
            .await
        {
            Ok(_) => panic!("second fallback must be blocked by fallback gate"),
            Err(_) => {}
        }
    }
}
