// 模块链式处理器（无全局状态机版本）
// 设计要点：
// - 不维护全局 step 索引，所有上下文通过 Request/Response 自带的 ExecutionMark 传递（包含 module_id/step_idx）。
// - 链式回退通过 request.prefix_request（指向“前置请求”的 request.id）来追溯，所有 Request 以 request.id 为 key 持久化到 Redis。
// - 生成失败（execute_request）时：若存在前置请求可回退，则最多回退一次（FallbackGate 门闸），否则直接报错。
// - 解析失败（execute_parser）时：直接返回带错误的 ParserData（ErrorTaskModel），并通过 ExecutionMark.context 标记当前步；
//   错误链路重入生成时，通过外部传入的 ExecutionMark（例如 ErrorTaskModel.context）精确重试当前步的 generate（不推进到下一步），不再污染 metadata。
// - 若解析成功但未返回 ParserTaskModel 且存在下一步，使用一次性门闸（AdvanceGate）合成“占位任务”推进下一步，避免多实例重复推进。

use common::model::ExecutionMark;
use common::interface::module::{ModuleNodeTrait, SyncBoxStream};
use common::model::{ ModuleConfig, Request, Response};
use errors::{RequestError, Result};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;
use futures::StreamExt;
use cacheable::{CacheAble, CacheService};
use common::model::login_info::LoginInfo;
use common::model::message::{ErrorTaskModel, ParserData, ParserTaskModel, TaskModel};
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

/// 执行状态信息（用于调试和监控）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStatus {
    pub module_id: String,
    pub run_id: Uuid,
    pub total_steps: usize,
    pub active_gates: Vec<GateInfo>,
}

/// 门闸状态信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateInfo {
    pub gate_type: String, // "advance" or "fallback"
    pub step_idx: Option<usize>,
    pub field_name: String,
    pub value: serde_json::Value,
}

/// 步骤执行统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepStats {
    pub step_idx: usize,
    pub module_id: String,
    pub run_id: Uuid,
}

/// 链式处理器：不依赖全局状态机/索引。
/// - 每个 Request/Response 自带 ExecutionMark（module_id/step_idx）。
/// - 节点间的链式追溯通过 request.prefix_request（指向前置 request.id）。
/// - Request 以 request.id 持久化在 Redis，失败时可从 Redis 恢复上一个请求。
#[derive(Clone)]
pub struct ModuleProcessorWithChain {
    module_id: String,
    cache: Arc<CacheService>,
    steps: Arc<RwLock<Vec<Arc<dyn ModuleNodeTrait>>>>,
    run_id: Uuid,
    ttl: u64,
    stop: Arc<RwLock<bool>>,
}

impl ModuleProcessorWithChain {
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
            stop:Arc::new(RwLock::new(false)),
        }
    }

    // 内部：为单个 step 生成请求（可链式/非链式），由统一入口调用 execute_request_impl

    /// 添加一个节点（顺序即执行顺序）
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
        format!("run:{}:module:{}", self.run_id, self.module_id)
    }

    /// 将 Request 以 request.id 作为 key 持久化（供回退/恢复使用）
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

    // 使用一次性门闸标记“已推进到下一步”：
    // 仅用于在解析成功但未返回 ParserTaskModel 时，合成占位任务推进下一步（避免多实例重复推进）。
    async fn try_mark_step_advanced_once(&self, step_idx: usize) -> Result<bool> {
        let id_str = format!("{}:step:{}", self.advance_key(), step_idx);
        debug!(
            "[chain] module={} run={} try_mark_step_advanced_once: id={} ttl={}s",
            self.module_id, self.run_id, id_str, self.ttl
        );
        
        // 使用 AdvanceGate 检查并标记该步骤已推进（分布式门闸 check-and-set）
        if let Ok(Some(_)) = AdvanceGate::sync(&id_str, &self.cache).await {
            debug!(
                "[chain] module={} run={} try_mark_step_advanced_once result: step_idx={} won=false (already exists)",
                self.module_id, self.run_id, step_idx
            );
            return Ok(false);
        }

        let gate = AdvanceGate(true);
        gate.send(&id_str, &self.cache).await?;

        debug!(
            "[chain] module={} run={} try_mark_step_advanced_once result: step_idx={} won=true",
            self.module_id, self.run_id, step_idx
        );
        Ok(true)
    }

    // 回退门闸：对同一 (step_idx, prefix_request) 仅允许回退一次，防止生成失败时的无限回退循环。
    async fn try_allow_fallback_once(&self, step_idx: usize, prefix: &Uuid) -> Result<bool> {
        let id_str = format!("{}:step:{}:prefix:{}", self.advance_key(), step_idx, prefix);
        debug!(
            "[chain] module={} run={} try_allow_fallback_once: id={} ttl={}s",
            self.module_id, self.run_id, id_str, self.ttl
        );
        
        if let Ok(Some(_)) = FallbackGate::sync(&id_str, &self.cache).await {
            debug!(
                "[chain] module={} run={} try_allow_fallback_once result: step_idx={} prefix={} allowed=false (already exists)",
                self.module_id, self.run_id, step_idx, prefix
            );
            return Ok(false);
        }

        let gate = FallbackGate(true);
        gate.send(&id_str, &self.cache).await?;

        debug!(
            "[chain] module={} run={} try_allow_fallback_once result: step_idx={} prefix={} allowed=true",
            self.module_id, self.run_id, step_idx, prefix
        );
        Ok(true)
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

    /// 将“需要重入队列”的意图持久化（上层可据此拉起重入）。
    /// 返回可恢复的前置 Request（若存在）。
    pub async fn mark_requeue(&self, id: &Uuid) -> Result<Option<Request>> {
        if id.is_nil() {
            // 非链式或首节点，无需回退
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

    /// 获取当前执行状态（用于调试和监控）
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

    /// 查询所有活跃的门闸状态
    async fn query_active_gates(&self) -> Result<Vec<GateInfo>> {
        let key = self.advance_key();
        let gates = Vec::new();

        // 这里简化处理：实际应该通过 Redis SCAN 命令扫描所有相关字段
        // 目前返回空列表，具体实现需要根据 CacheAble (AdvanceGate/FallbackGate) 的键模式进行扫描
        debug!(
            "[chain] module={} run={} query_active_gates: key={}",
            self.module_id, self.run_id, key
        );

        // TODO: 实现实际的门闸查询逻辑
        Ok(gates)
    }

    /// 获取指定步骤的执行统计
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
            // 这里可以扩展更多统计信息，如执行次数、成功率等
        })
    }

    // 已移除：steps_len（非链式遍历不再支持，统一按链式推进）

    /// 判断任务是否面向当前模块
    /// - 若 context.module_id 明确指定，直接比较
    /// - 若 context.module_id 缺省，检查 TaskModel.module 列表是否包含当前模块
    fn is_task_for_current_module(
        &self,
        ctx: &ExecutionMark,
        task_modules: &[String],
    ) -> bool {
        if let Some(mid) = &ctx.module_id {
            return mid == &self.module_id;
        }
        // context 没有 module_id，检查 task 的 module 列表
        task_modules.iter().any(|m| m == &self.module_id)
    }

    /// 判断是否应该在最后一步终止
    /// 避免无限循环：在最后一步且没有明确推进到更远步骤时应该终止
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

        // 在最后一步且没有显式推进到更远的步骤
        // 注意：即使 stay_current_step=true，如果 task_step <= current_step 也应该终止
        // 否则会陷入无限重入循环
        task_step <= current_step && !stay_current
    }

    /// 解析执行上下文：统一处理 ctx 和 prefix_request 的各种组合
    /// 返回 (ExecutionMark, prefix_uuid)
    async fn resolve_execution_context(
        &self,
        ctx: Option<ExecutionMark>,
        prefix_request: Option<Uuid>,
    ) -> Result<(ExecutionMark, Uuid)> {
        let prefix = prefix_request.unwrap_or(Uuid::nil());

        // 情况1: 需要从 prefix_request 推断下一步
        // - ctx 不存在，或
        // - ctx 存在但 module_id 匹配且 step_idx 缺失
        let should_infer_from_prefix = if let Some(ref existing_ctx) = ctx {
            // ctx 存在：只有当 module_id 匹配且 step_idx 缺失时才推断
            existing_ctx.module_id.as_ref().map(|m| m == &self.module_id).unwrap_or(false)
                && existing_ctx.step_idx.is_none()
                && !prefix.is_nil()
        } else {
            // ctx 不存在：如果有非空 prefix 则推断
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

        // 情况2: 使用传入的 ctx（需要验证和补全）
        let mut effective_ctx = match ctx {
            Some(ctx) => {
                // 若传递的 ctx.module_id 与当前不符，则重建上下文
                // 此情况适用于跨模块调用时，确保上下文正确
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

        // 确保 module_id 和 step_idx 都有值
        if effective_ctx.module_id.is_none() {
            effective_ctx = effective_ctx.with_module_id(self.module_id.clone());
        }
        if effective_ctx.step_idx.is_none() {
            effective_ctx = effective_ctx.with_step_idx(0);
        }

        Ok((effective_ctx, prefix))
    }

    /// 统一入口：按链式生成当前 step 的请求
    /// 行为规则
    /// - 若未提供 step 且传入非空 prefix_request：从前置 Request 推断下一步并生成。
    /// - 否则：使用 ctx.step_idx（默认 0）在当前节点生成；prefix 缺省为 Nil（首节点）。
    pub async fn execute_request(
        &self,
        config: ModuleConfig,
        meta: serde_json::Map<String, serde_json::Value>,
        login_info: Option<LoginInfo>,
        ctx: Option<ExecutionMark>,
        prefix_request: Option<Uuid>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        if self.is_stopped().await? {
            return Ok(Box::pin(futures::stream::empty()));
        }
        // 统一解析执行上下文
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

    /// 在 ctx.step_idx（默认 0）所指节点生成请求（无全局状态）。

    /// 在 ctx.step_idx（默认 0）所指节点生成请求（无全局状态）。
    /// - 成功：每个 Request 以 request.id 持久化。
    /// - 失败：若 prefix_request 非 Nil，则尝试回退到前置 Request 并返回；否则（首节点）冒泡错误。
    async fn execute_request_impl(
        &self,
        config: ModuleConfig,
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
        // 非链式的“只推进一次”逻辑已移至 execute_parser 的占位任务生成前做校验
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
                let prefix_request = prefix_request;
                let module_id = self.module_id.clone();

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
                    
                    debug!(
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
                // 生成失败恢复策略
                if prefix_request.is_nil() {
                    // 首节点：没有前驱请求可回退，直接返回错误
                    warn!(
                        "[chain] module={} run={} execute_request_impl: generation error at first step={}, no prefix, err={}",
                        self.module_id, self.run_id, step_idx, e
                    );
                    Err(e)
                } else {
                    // 非首节点：优先返回“前置 Request”给上层，触发上一跳重试；
                    // 使用一次性门闸避免在同一 (step_idx, prefix_request) 上无限回退。
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
                            } // 未能找回前置请求，则退回错误
                        }
                    } else {
                        // 回退已发生过：直接冒泡错误，打破死循环
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

    /// 将 Response 路由到其 context.step_idx 对应的解析节点（无全局状态）。
    /// - 若解析成功：
    ///   - 返回 ParserTaskModel 时：如未明确推进且存在下一步，则自动推进一步；同时绑定 prefix_request。
    ///   - 未返回 ParserTaskModel 且存在下一步：通过一次性门闸合成“占位任务”推进下一步（避免重复推进）。
    /// - 若解析失败：
    ///   - 返回携带 ErrorTaskModel 的 ParserData，使用 ExecutionMark.context 标明当前步；
    ///     后续错误链路重入时，上层将该 context 传回 execute_request 以精确重试当前步。
    pub async fn execute_parser(
        &self,
        response: Response,
        config: Option<ModuleConfig>,
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
                if let Some(stop) = data.stop && stop {
                    self.set_stopped().await?;
                }
                // Ensure chain prefix points to the current request (request.id carried in response.prefix_request)
                if let Some(ref mut task) = data.parser_task {
                    // 1) 将回退指针绑定到“当前这一步的请求”（即 request.id，经 downloader 写入 response.prefix_request）
                    task.prefix_request = response.prefix_request;

                    // 2) 防御性推进：若解析返回的任务未设置下一步上下文，或仍停留在当前 step，则推进到下一步
                    //    - 确保 module_id 存在
                    //    - 当存在下一步节点时，step_idx 置为 step_idx + 1
                    let total = {
                        let steps = self.steps.read().await;
                        steps.len()
                    };
                    // 以当前任务的上下文为基准；避免覆盖跨模块交接
                    let mut next_ctx = task.context.clone();
                    let task_modules = task
                        .account_task
                        .module
                        .clone()
                        .unwrap_or_default();

                    // 使用辅助方法判定该任务是否面向当前模块
                    let same_module = self.is_task_for_current_module(&next_ctx, &task_modules);

                    // 仅当确认同模块时，才补齐 module_id，确保跨模块时不被错误覆盖
                    if same_module && next_ctx.module_id.is_none() {
                        next_ctx = next_ctx.with_module_id(self.module_id.clone());
                    }

                    // 当前/缺省步骤索引
                    let task_step = next_ctx.step_idx.unwrap_or(step_idx as u32) as usize;
                    let mut desired_step = task_step;

                    if same_module {
                        // honour stay_current_step：若显式要求停留在当前步，则不推进
                        if !next_ctx.stay_current_step {
                            // 当任务没有显式推进，且尚有下一步时，推进到下一步
                            if task_step <= step_idx {
                                let maybe_next = step_idx + 1;
                                if maybe_next < total {
                                    desired_step = maybe_next;
                                }
                            }
                        } else {
                            desired_step = step_idx;
                        }

                        // 使用辅助方法判断是否应该终止
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
                        // 跨模块任务：不在本模块内调整步进或终止，直接返回由上层调度到目标模块
                        task.context = next_ctx;
                        debug!(
                            "[chain] module={} run={} execute_parser: task targets other module -> pass-through",
                            self.module_id, self.run_id
                        );
                        return Ok(data);
                    }

                    // 只有在需要变更时才写入，避免无意义写入
                    if next_ctx.step_idx.map(|s| s as usize) != Some(desired_step) {
                        next_ctx = next_ctx.with_step_idx(desired_step as u32);
                        task.context = next_ctx;
                        debug!(
                            "[chain] module={} run={} execute_parser: task present, advance context to step {} (from resp step {})",
                            self.module_id, self.run_id, desired_step, step_idx
                        );
                    } else {
                        // 也要确保 module_id 一致
                        task.context = next_ctx;
                        debug!(
                            "[chain] module={} run={} execute_parser: task present, keep context at step {}",
                            self.module_id, self.run_id, desired_step
                        );
                    }
                } else {
                    // 若无 ParserTaskModel，且存在下一个 step，则尝试通过一次性门闸合成“占位”任务推进下一步
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
                        // 统一：不再依赖 prefix 判定链式/非链式；仅通过一次性门闸控制推进
                        // 可选：若担心下一步已执行过，可在此处先查 exec gate 再决定是否推进
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
                // 解析失败：直接返回一个 ErrorTaskModel，标明当前 step，以便错误链路据此重试当前节点
                let step_idx_u32 = response.context.step_idx.unwrap_or(0);
                // 直接透传现有元数据；重试步信息不再写入 metadata，避免污染
                let meta =
                    serde_json::to_value(&response.metadata).unwrap_or(serde_json::json!({}));
                let error_task =ErrorTaskModel {
                    id: response.id,
                    account_task: TaskModel {
                        account: response.account.clone(),
                        platform: response.platform.clone(),
                        module: Some(vec![response.module.clone()]),
                        run_id: response.run_id,
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

        // Check distributed state
        let id_str = self.advance_key();
        if let Ok(Some(signal)) = StopSignal::sync(&id_str, &self.cache).await {
            if signal.0 {
                // Update local cache
                *self.stop.write().await = true;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn set_stopped(&self) -> Result<()> {
        // Update local cache
        *self.stop.write().await = true;

        // Update distributed state
        let id_str = self.advance_key();
        StopSignal(true).send(&id_str, &self.cache).await?;
        debug!(
            "[chain] module={} run={} set_stopped: marked as stopped",
            self.module_id, self.run_id
        );
        Ok(())
    }
}
