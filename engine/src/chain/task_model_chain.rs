#![allow(unused)]
use common::status_tracker::ErrorDecision;
use log::info;
use crate::events::{
    EventBus, EventEnvelope, EventPhase, EventType, ModuleGenerateEvent, ParserTaskModelEvent,
    RequestEvent, TaskModelEvent,
};
use crate::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};


use async_trait::async_trait;
use errors::{Error, ModuleError, Result};
use common::model::chain_key;
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel, UnifiedTaskInput};
use common::model::{ModuleConfig, Request};
use common::state::State;

use log::{debug, error, warn};
use metrics::counter;
use queue::{QueueManager, QueuedItem};
use common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use common::processors::processor_chain::ErrorStrategy;
use std::sync::Arc;
use uuid::Uuid;
use futures::stream::{StreamExt};
use std::marker::PhantomData;
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::deduplication::Deduplicator;
use crate::lua::LuaScriptRegistry;
use crate::chain::backpressure::{BackpressureSendState, send_with_backpressure};

#[derive(Debug, Clone)]
pub enum ChainDecision {
    Continue,
    RetryAfter {
        delay: std::time::Duration,
        action_applied: bool,
    },
    TerminateModule {
        reason: String,
        action_applied: bool,
    },
    TerminateTask {
        reason: String,
        action_applied: bool,
    },
}

impl ChainDecision {
    fn action_applied(&self) -> bool {
        match self {
            ChainDecision::Continue => false,
            ChainDecision::RetryAfter { action_applied, .. } => *action_applied,
            ChainDecision::TerminateModule { action_applied, .. } => *action_applied,
            ChainDecision::TerminateTask { action_applied, .. } => *action_applied,
        }
    }
}

#[cfg(test)]
mod decision_tests {
    use super::ChainDecision;

    #[test]
    fn chain_decision_action_applied_flags_are_stable() {
        let continue_decision = ChainDecision::Continue;
        assert!(!continue_decision.action_applied());

        let retry_lua = ChainDecision::RetryAfter {
            delay: std::time::Duration::from_millis(1000),
            action_applied: true,
        };
        let retry_fallback = ChainDecision::RetryAfter {
            delay: std::time::Duration::from_millis(1000),
            action_applied: false,
        };
        assert!(retry_lua.action_applied());
        assert!(!retry_fallback.action_applied());

        let terminate_module_lua = ChainDecision::TerminateModule {
            reason: "module limit".to_string(),
            action_applied: true,
        };
        let terminate_module_fallback = ChainDecision::TerminateModule {
            reason: "module limit".to_string(),
            action_applied: false,
        };
        assert!(terminate_module_lua.action_applied());
        assert!(!terminate_module_fallback.action_applied());

        let terminate_task_lua = ChainDecision::TerminateTask {
            reason: "task limit".to_string(),
            action_applied: true,
        };
        let terminate_task_fallback = ChainDecision::TerminateTask {
            reason: "task limit".to_string(),
            action_applied: false,
        };
        assert!(terminate_task_lua.action_applied());
        assert!(!terminate_task_fallback.action_applied());
    }
}

#[async_trait]
pub trait ThresholdDecisionService: Send + Sync {
    async fn task_precheck(&self, task_id: &str) -> ChainDecision;
    async fn error_task_decide(&self, input: &ErrorTaskModel) -> ChainDecision;
}

#[derive(Clone)]
pub struct StatusTrackerThresholdDecisionService {
    state: Arc<State>,
    lua_registry: Option<Arc<LuaScriptRegistry>>,
}

impl StatusTrackerThresholdDecisionService {
    pub fn new(state: Arc<State>, lua_registry: Option<Arc<LuaScriptRegistry>>) -> Self {
        Self { state, lua_registry }
    }
}

#[async_trait]
impl ThresholdDecisionService for StatusTrackerThresholdDecisionService {
    async fn task_precheck(&self, task_id: &str) -> ChainDecision {
        match self.state.status_tracker.should_task_continue(task_id).await {
            Ok(ErrorDecision::Continue) => ChainDecision::Continue,
            Ok(ErrorDecision::Terminate(reason)) => ChainDecision::TerminateTask {
                reason,
                action_applied: false,
            },
            Err(err) => {
                warn!(
                    "[ThresholdDecisionService] task precheck failed: task_id={} error={}, fallback=continue",
                    task_id, err
                );
                ChainDecision::Continue
            }
            _ => ChainDecision::Continue,
        }
    }

    async fn error_task_decide(&self, input: &ErrorTaskModel) -> ChainDecision {
        let task_id = chain_key::task_runtime_id(
            &input.account_task.platform,
            &input.account_task.account,
            input.run_id,
        );

        if let (Some(lua_registry), Some(modules)) = (&self.lua_registry, input.account_task.module.as_ref())
            && let Some(module_name) = modules.first()
        {
            let module_id = chain_key::module_runtime_id(
                &input.account_task.account,
                &input.account_task.platform,
                module_name,
            );
            let module_counter_key = chain_key::module_threshold_key(&task_id, &module_id);
            let task_counter_key = chain_key::task_threshold_key(&task_id);

            let cfg = self.state.config.read().await;
            let module_threshold = cfg.crawler.module_max_errors.to_string();
            let task_threshold = cfg.crawler.task_max_errors.to_string();
            let retry_after_ms = "1000".to_string();
            let ttl_secs = cfg.cache.ttl.to_string();
            drop(cfg);

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .to_string();

            let keys = [module_counter_key.as_str(), task_counter_key.as_str()];
            let args = [
                module_threshold.as_str(),
                task_threshold.as_str(),
                retry_after_ms.as_str(),
                ttl_secs.as_str(),
            ];

            match lua_registry
                .eval_triplet_with_fallback(
                    self.state.cache_service.as_ref(),
                    "etm_threshold_decide.lua",
                    include_str!("../../lua/etm_threshold_decide.lua"),
                    &keys,
                    &args,
                )
                .await
            {
                Ok((0, _, _)) => return ChainDecision::Continue,
                Ok((1, _, _)) => {
                    let retry_schedule_key = chain_key::error_retry_schedule_key(&task_id);
                    let retry_member = format!("{}:{}:{}", task_id, module_id, input.prefix_request);
                    let schedule_keys = [retry_schedule_key.as_str()];
                    let schedule_args = [
                        retry_member.as_str(),
                        retry_after_ms.as_str(),
                        now_ms.as_str(),
                        ttl_secs.as_str(),
                    ];
                    match lua_registry
                        .eval_triplet_with_fallback(
                            self.state.cache_service.as_ref(),
                            "etm_retry_schedule.lua",
                            include_str!("../../lua/etm_retry_schedule.lua"),
                            &schedule_keys,
                            &schedule_args,
                        )
                        .await
                    {
                        Ok((0, _, _)) => {
                            counter!("error_task_lua_action_total", "action" => "retry_scheduled").increment(1);
                        }
                        Ok((1, msg, _)) | Ok((2, msg, _)) => {
                            counter!("error_task_lua_action_total", "action" => "retry_schedule_rejected").increment(1);
                            warn!(
                                "[ThresholdDecisionService] etm_retry_schedule rejected: task_id={} module_id={} msg={}",
                                task_id, module_id, msg
                            );
                        }
                        Ok((code, msg, _)) => {
                            counter!("error_task_lua_action_total", "action" => "retry_schedule_unknown").increment(1);
                            warn!(
                                "[ThresholdDecisionService] etm_retry_schedule unexpected code={} task_id={} module_id={} msg={}",
                                code, task_id, module_id, msg
                            );
                        }
                        Err(err) => {
                            counter!("error_task_lua_action_total", "action" => "retry_schedule_error").increment(1);
                            warn!(
                                "[ThresholdDecisionService] etm_retry_schedule failed: task_id={} module_id={} error={}",
                                task_id, module_id, err
                            );
                        }
                    }
                    return ChainDecision::RetryAfter {
                        delay: std::time::Duration::from_millis(1000),
                        action_applied: true,
                    };
                }
                Ok((2, msg, _)) => {
                    let terminate_key = chain_key::terminate_module_key(&task_id, &module_id);
                    let terminate_keys = [terminate_key.as_str()];
                    let terminate_args = [msg.as_str(), now_ms.as_str(), ttl_secs.as_str()];
                    match lua_registry
                        .eval_triplet_with_fallback(
                            self.state.cache_service.as_ref(),
                            "etm_terminate_mark.lua",
                            include_str!("../../lua/etm_terminate_mark.lua"),
                            &terminate_keys,
                            &terminate_args,
                        )
                        .await
                    {
                        Ok((0, _, _)) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_module_marked").increment(1);
                        }
                        Ok((1, _, _)) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_module_already_marked").increment(1);
                        }
                        Ok((code, _, _)) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_module_unknown").increment(1);
                            warn!(
                                "[ThresholdDecisionService] etm_terminate_mark(module) unexpected code={} task_id={} module_id={}",
                                code, task_id, module_id
                            );
                        }
                        Err(err) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_module_error").increment(1);
                            warn!(
                                "[ThresholdDecisionService] etm_terminate_mark(module) failed: task_id={} module_id={} error={}",
                                task_id, module_id, err
                            );
                        }
                    }
                    self.state.status_tracker.release_module_locker(&module_id).await;
                    return ChainDecision::TerminateModule {
                        reason: msg,
                        action_applied: true,
                    };
                }
                Ok((3, msg, _)) => {
                    let terminate_key = chain_key::terminate_task_key(&task_id);
                    let terminate_keys = [terminate_key.as_str()];
                    let terminate_args = [msg.as_str(), now_ms.as_str(), ttl_secs.as_str()];
                    match lua_registry
                        .eval_triplet_with_fallback(
                            self.state.cache_service.as_ref(),
                            "etm_terminate_mark.lua",
                            include_str!("../../lua/etm_terminate_mark.lua"),
                            &terminate_keys,
                            &terminate_args,
                        )
                        .await
                    {
                        Ok((0, _, _)) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_task_marked").increment(1);
                        }
                        Ok((1, _, _)) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_task_already_marked").increment(1);
                        }
                        Ok((code, _, _)) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_task_unknown").increment(1);
                            warn!(
                                "[ThresholdDecisionService] etm_terminate_mark(task) unexpected code={} task_id={}",
                                code, task_id
                            );
                        }
                        Err(err) => {
                            counter!("error_task_lua_action_total", "action" => "terminate_task_error").increment(1);
                            warn!(
                                "[ThresholdDecisionService] etm_terminate_mark(task) failed: task_id={} error={}",
                                task_id, err
                            );
                        }
                    }
                    let _ = self.state.status_tracker.mark_task_terminated(&task_id).await;
                    return ChainDecision::TerminateTask {
                        reason: msg,
                        action_applied: true,
                    };
                }
                Ok((code, msg, _)) => {
                    warn!(
                        "[ThresholdDecisionService] unexpected lua decision code={} msg={}, fallback rust",
                        code, msg
                    );
                }
                Err(err) => {
                    warn!(
                        "[ThresholdDecisionService] lua decide failed, fallback rust: task_id={} module_id={} error={}",
                        task_id, module_id, err
                    );
                }
            }
        }

        let parse_error: Error = ModuleError::ModuleNotFound(input.error_msg.clone().into()).into();
        let mut retry_after: Option<std::time::Duration> = None;

        if let Some(modules) = &input.account_task.module {
            for module_name in modules {
                let module_id = chain_key::module_runtime_id(
                    &input.account_task.account,
                    &input.account_task.platform,
                    module_name,
                );

                if input.prefix_request != Uuid::nil() {
                    match self
                        .state
                        .status_tracker
                        .record_parse_error(
                            &task_id,
                            &module_id,
                            &input.prefix_request.to_string(),
                            &parse_error,
                        )
                        .await
                    {
                        Ok(ErrorDecision::RetryAfter(delay)) => {
                            retry_after = Some(delay);
                        }
                        Ok(_) => {}
                        Err(err) => {
                            warn!(
                                "[ThresholdDecisionService] record_parse_error failed: task_id={} module_id={} error={}",
                                task_id, module_id, err
                            );
                        }
                    }
                }

                match self.state.status_tracker.should_module_continue(&module_id).await {
                    Ok(ErrorDecision::Terminate(reason)) => {
                        return ChainDecision::TerminateModule {
                            reason,
                            action_applied: false,
                        };
                    }
                    Ok(_) => {}
                    Err(err) => {
                        warn!(
                            "[ThresholdDecisionService] module precheck failed: module_id={} error={}",
                            module_id, err
                        );
                    }
                }
            }
        }

        match self.task_precheck(&task_id).await {
            ChainDecision::TerminateTask { reason, .. } => ChainDecision::TerminateTask {
                reason,
                action_applied: false,
            },
            _ => retry_after
                .map(|delay| ChainDecision::RetryAfter {
                    delay,
                    action_applied: false,
                })
                .unwrap_or(ChainDecision::Continue),
        }
    }
}

/// 任务模型处理器
///
/// 负责处理 TaskModel，协调配置加载、任务转换、请求发布等流程。
/// 作为事件驱动的处理链的一部分，它会发布处理过程中的状态变更事件。
pub struct TaskModelProcessor {
    task_manager: Arc<TaskManager>,
    state: Arc<State>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    threshold_decision_service: Arc<dyn ThresholdDecisionService>,
}

impl TaskModelProcessor {
    async fn task_precheck(&self, task_id: &str) -> ChainDecision {
        self.threshold_decision_service.task_precheck(task_id).await
    }

    async fn error_task_decide(&self, input: &ErrorTaskModel) -> ChainDecision {
        self.threshold_decision_service.error_task_decide(input).await
    }

    async fn persist_error_retry_schedule(&self, input: &ErrorTaskModel, delay: std::time::Duration) {
        let task_id = chain_key::task_runtime_id(
            &input.account_task.platform,
            &input.account_task.account,
            input.run_id,
        );
        let module_id = input
            .context
            .module_id
            .clone()
            .or_else(|| {
                input
                    .account_task
                    .module
                    .as_ref()
                    .and_then(|m| m.first().map(|name| {
                        chain_key::module_runtime_id(
                            &input.account_task.account,
                            &input.account_task.platform,
                            name,
                        )
                    }))
            })
            .unwrap_or_else(|| {
                chain_key::module_runtime_id(
                    &input.account_task.account,
                    &input.account_task.platform,
                    "unknown",
                )
            });
        let retry_key = chain_key::error_retry_schedule_key(&task_id);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64;
        let score = now_ms + delay.as_millis() as f64;
        let member = format!("{}:{}:{}", task_id, module_id, input.prefix_request);

        if let Err(err) = self
            .state
            .cache_service
            .zadd(&retry_key, score, member.as_bytes())
            .await
        {
            warn!(
                "[TaskModelProcessor<ErrorTaskModel>] failed to persist retry schedule: key={} error={}",
                retry_key, err
            );
        }
    }

    async fn persist_terminate_mark(
        &self,
        input: &ErrorTaskModel,
        terminate_module: Option<&str>,
        reason: &str,
    ) {
        let task_id = chain_key::task_runtime_id(
            &input.account_task.platform,
            &input.account_task.account,
            input.run_id,
        );
        let terminate_key = if let Some(module_id) = terminate_module {
            chain_key::terminate_module_key(&task_id, module_id)
        } else {
            chain_key::terminate_task_key(&task_id)
        };

        let ttl_secs = {
            let cfg = self.state.config.read().await;
            cfg.cache.ttl
        };
        let payload = json!({
            "reason": reason,
            "task_id": task_id,
            "module_id": terminate_module,
            "prefix_request": input.prefix_request.to_string(),
        })
        .to_string();
        if let Err(err) = self
            .state
            .cache_service
            .set_nx(
                &terminate_key,
                payload.as_bytes(),
                Some(std::time::Duration::from_secs(ttl_secs)),
            )
            .await
        {
            warn!(
                "[TaskModelProcessor<ErrorTaskModel>] failed to persist terminate mark: key={} error={}",
                terminate_key, err
            );
        }
    }

    async fn emit_threshold_terminated_event(
        &self,
        input: &ErrorTaskModel,
        decision: &str,
        reason: &str,
    ) {
        let Some(event_bus) = &self.event_bus else {
            return;
        };

        let payload = json!({
            "run_id": input.run_id.to_string(),
            "account": input.account_task.account,
            "platform": input.account_task.platform,
            "module_id": input.context.module_id,
            "step_idx": input.context.step_idx,
            "prefix_request": input.prefix_request.to_string(),
            "decision": decision,
            "reason": reason,
        });

        if let Err(err) = event_bus
            .publish(EventEnvelope::engine(
                EventType::TaskTerminatedByThreshold,
                EventPhase::Completed,
                payload,
            ))
            .await
        {
            warn!(
                "[TaskModelProcessor<ErrorTaskModel>] failed to publish threshold termination event: {}",
                err
            );
        }
    }
}
#[async_trait]
impl ProcessorTrait<TaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(&self, input: TaskModel, context: ProcessorContext) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor] Processing Task: account={} platform={} modules={:?} retry={}",
            input.account,
            input.platform,
            input.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );

        let task_id = format!("{}-{}", input.account, input.platform);
        match self.task_precheck(&task_id).await {
            ChainDecision::Continue => {}
            ChainDecision::TerminateTask { reason, .. } => {
                error!(
                    "[TaskModelProcessor<TaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                if let Err(e) = self.queue_manager.send_to_dlq("task", &input, &reason).await {
                    error!("[TaskModelProcessor<TaskModel>] failed to send to DLQ: {}", e);
                }
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            _ => {}
        }

        let task = self.task_manager.load_with_model(&input).await;
        debug!(
            "[TaskModelProcessor] load_with_model result: {:?}",
            task.as_ref().map(|t| t.id())
        );

        match task {
            Ok(task) => {
                if task.is_empty() {
                    return ProcessorResult::FatalFailure(
                        ModuleError::ModuleNotFound(
                            format!(
                                "No modules found for the given TaskModel, task_model: {input:?}"
                            )
                            .into(),
                        )
                        .into(),
                    );
                }
                let default_locker_ttl = self.state.config.read().await.crawler.module_locker_ttl;
                let mut all_locked = true;
                for m in task.modules.iter() {
                    if !self
                        .state
                        .status_tracker
                        .is_module_locker(m.id().as_ref(), default_locker_ttl)
                        .await
                    {
                        all_locked = false;
                        break;
                    }
                }
                if all_locked {
                    warn!(
                        "[TaskModelProcessor<TaskModel>] all target modules locked, requeue TaskModel: account={} platform={}",
                        input.account, input.platform
                    );
                    let sender = self.queue_manager.get_task_push_channel();
                    if let Err(e) = sender.send(QueuedItem::new(input.clone())).await {
                        warn!(
                            "[TaskModelProcessor<TaskModel>] requeue TaskModel failed, will retry: {}",
                            e
                        );
                    }
                    return ProcessorResult::RetryableFailure(
                        context.retry_policy.unwrap_or_default(),
                    );
                }

                info!(
                    "[TaskModelProcessor] Successfully converted TaskModel to Task. Task ID: {}, Modules count: {}",
                    task.id(),
                    task.modules.len()
                );

                ProcessorResult::Success(task)
            }
            Err(e) => {
                debug!("[TaskModelProcessor] load_with_model failed: {e}");
                warn!("[TaskModelProcessor<TaskModel>] load_with_model failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &TaskModel, _context: &ProcessorContext) -> Result<()> {
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &TaskModel,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {
        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorTaskModel {
            id: Default::default(),
            account_task: input.clone(),
            error_msg: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: input.run_id,
            prefix_request: Uuid::nil(),
        };
        if let Err(e) = sender.send(QueuedItem::new(error_msg)).await {
            error!("[TaskModelProcessor<TaskModel>] failed to enqueue ErrorTaskModel: {e}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }
}
#[async_trait]
impl EventProcessorTrait<TaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &TaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Started,
            TaskModelEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &TaskModel, output: &Task) -> Option<EventEnvelope> {
        let mut task_model_event: TaskModelEvent = input.into();
        task_model_event.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Completed,
            task_model_event,
        ))
    }

    fn working_status(&self, input: &TaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Started,
            TaskModelEvent::from(input),
        ))
    }

    fn error_status(&self, input: &TaskModel, error: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::TaskModel,
            EventPhase::Failed,
            TaskModelEvent::from(input),
            error,
        ))
    }

    fn retry_status(&self, input: &TaskModel, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Retry,
            json!({
                "data": TaskModelEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
#[async_trait]
impl ProcessorTrait<ParserTaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: ParserTaskModel,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor<ParserTaskModel>] start: account={} platform={} crawler={:?} retry_count={}",
            input.account_task.account,
            input.account_task.platform,
            input.account_task.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );

        // 首先检查 Task 是否已被标记为终止
        let task_id = chain_key::task_runtime_id(
            &input.account_task.platform,
            &input.account_task.account,
            input.run_id,
        );
        match self.task_precheck(&task_id).await {
            ChainDecision::Continue => {
                debug!(
                    "[TaskModelProcessor<ParserTaskModel>] task can continue: task_id={}",
                    task_id
                );
            }
            ChainDecision::TerminateTask { reason, .. } => {
                error!(
                    "[TaskModelProcessor<ParserTaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            _ => {}
        }

        let task = self.task_manager.load_parser(&input).await;
        match task {
            Ok(task) => {
                ProcessorResult::Success(task)
            }
            Err(e) => {
                warn!("[TaskModelProcessor<ParserTaskModel>] load_parser failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(
        &self,
        _input: &ParserTaskModel,
        _context: &ProcessorContext,
    ) -> Result<()> {
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &ParserTaskModel,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {
        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorTaskModel {
            id: Default::default(),
            account_task: input.account_task.clone(),
            error_msg: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: input.run_id,
            prefix_request: input.prefix_request,
        };
        if let Err(e) = sender.send(QueuedItem::new(error_msg)).await {
            error!("[TaskModelProcessor<ParserTaskModel>] failed to enqueue ErrorTaskModel: {e}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }
}
#[async_trait]
impl EventProcessorTrait<ParserTaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &ParserTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &ParserTaskModel, output: &Task) -> Option<EventEnvelope> {
        let mut evt: ParserTaskModelEvent = input.into();
        evt.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Completed,
            evt,
        ))
    }

    fn working_status(&self, input: &ParserTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn error_status(&self, input: &ParserTaskModel, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ParserTaskModel,
            EventPhase::Failed,
            ParserTaskModelEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &ParserTaskModel, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Retry,
            json!({
                "data": ParserTaskModelEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
#[async_trait]
impl ProcessorTrait<ErrorTaskModel, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: ErrorTaskModel,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        debug!(
            "[TaskModelProcessor<ErrorTaskModel>] start: account={} platform={} crawler={:?} retry_count={}",
            input.account_task.account,
            input.account_task.platform,
            input.account_task.module,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );

        let task_id = chain_key::task_runtime_id(
            &input.account_task.platform,
            &input.account_task.account,
            input.run_id,
        );
        match self.error_task_decide(&input).await {
            ChainDecision::Continue => {
                counter!("error_task_threshold_decision_total", "decision" => "continue").increment(1);
                debug!(
                    "[TaskModelProcessor<ErrorTaskModel>] task can continue: task_id={}",
                    task_id
                );
            }
            ChainDecision::RetryAfter {
                delay,
                action_applied,
            } => {
                counter!("error_task_threshold_decision_total", "decision" => "retry_after").increment(1);
                if !(ChainDecision::RetryAfter {
                    delay,
                    action_applied,
                })
                .action_applied()
                {
                    self.persist_error_retry_schedule(&input, delay).await;
                }
                let mut retry_policy = context.retry_policy.unwrap_or_default();
                retry_policy.retry_delay = std::cmp::max(delay.as_millis() as u64, 1);
                retry_policy.reason = Some(format!("error task retry after {}ms", retry_policy.retry_delay));
                return ProcessorResult::RetryableFailure(retry_policy);
            }
            ChainDecision::TerminateModule {
                reason,
                action_applied,
            } => {
                counter!("error_task_threshold_decision_total", "decision" => "terminate_module").increment(1);
                let module_id = input
                    .context
                    .module_id
                    .clone()
                    .or_else(|| {
                        input
                            .account_task
                            .module
                            .as_ref()
                            .and_then(|m| m.first().map(|name| {
                                chain_key::module_runtime_id(
                                    &input.account_task.account,
                                    &input.account_task.platform,
                                    name,
                                )
                            }))
                    })
                    .unwrap_or_else(|| {
                        chain_key::module_runtime_id(
                            &input.account_task.account,
                            &input.account_task.platform,
                            "unknown",
                        )
                    });
                if !(ChainDecision::TerminateModule {
                    reason: reason.clone(),
                    action_applied,
                })
                .action_applied()
                {
                    self.persist_terminate_mark(&input, Some(&module_id), &reason).await;
                }
                self.emit_threshold_terminated_event(&input, "terminate_module", &reason)
                    .await;
                warn!(
                    "[TaskModelProcessor<ErrorTaskModel>] module terminated by threshold: task_id={} reason={}",
                    task_id, reason
                );
                return ProcessorResult::FatalFailure(
                    ModuleError::ModuleMaxError(reason.into()).into(),
                );
            }
            ChainDecision::TerminateTask {
                reason,
                action_applied,
            } => {
                counter!("error_task_threshold_decision_total", "decision" => "terminate_task").increment(1);
                if !(ChainDecision::TerminateTask {
                    reason: reason.clone(),
                    action_applied,
                })
                .action_applied()
                {
                    self.persist_terminate_mark(&input, None, &reason).await;
                }
                self.emit_threshold_terminated_event(&input, "terminate_task", &reason)
                    .await;
                error!(
                    "[TaskModelProcessor<ErrorTaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id,
                    reason
                );
                if let Err(e) = self.queue_manager.send_to_dlq("error_task", &input, &reason).await {
                    error!("[TaskModelProcessor<ErrorTaskModel>] failed to send to DLQ: {}", e);
                }
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
        }

        let task = self.task_manager.load_error(&input).await;
        match task {
            Ok(task) => {
                ProcessorResult::Success(task)
            }
            Err(e) => {
                warn!("[TaskModelProcessor<ErrorTaskModel>] load_error failed, will retry: {e}");
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &ErrorTaskModel, _context: &ProcessorContext) -> Result<()> {
        Ok(())
    }
    // async fn handle_error(
    //     &self,
    //     input: &ParserErrorMessage,
    //     _error: Error,
    //     _context: &ProcessorContext,
    // ) -> ProcessorResult<Task> {
    //     // 由于使用的是动态加载dylib,task_manager.load_error可能未能加载到正确的库
    //     let sender = self.queue_manager.get_error_push_channel();
    //     sender.send(input.to_owned()).await.unwrap();
    //     ProcessorResult::FatalFailure(
    //         ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
    //     )
    // }
}
#[async_trait]
impl EventProcessorTrait<ErrorTaskModel, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &ErrorTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &ErrorTaskModel, output: &Task) -> Option<EventEnvelope> {
        let mut evt: ParserTaskModelEvent = input.into();
        evt.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Completed,
            evt,
        ))
    }

    fn working_status(&self, input: &ErrorTaskModel) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Started,
            ParserTaskModelEvent::from(input),
        ))
    }

    fn error_status(&self, input: &ErrorTaskModel, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ParserTaskModel,
            EventPhase::Failed,
            ParserTaskModelEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &ErrorTaskModel, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserTaskModel,
            EventPhase::Retry,
            json!({
                "data": ParserTaskModelEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}

pub struct TaskModuleProcessor {
    state: Arc<State>,
}
#[async_trait]
impl ProcessorTrait<Task, Vec<Module>> for TaskModuleProcessor {
    fn name(&self) -> &'static str {
        "TaskModuleProcessor"
    }

    async fn process(
        &self,
        input: Task,
        _context: ProcessorContext,
    ) -> ProcessorResult<Vec<Module>> {
        debug!(
            "[TaskModuleProcessor] start: task_id={} module_count={}",
            input.id(),
            input.modules.len()
        );
        let metadata = input.metadata.clone();
        let login_info = input.login_info.clone();
        let mut modules: Vec<Module> = Vec::new();
        let default_locker_ttl = self.state.config.read().await.crawler.module_locker_ttl;
        for mut module in input.modules {
            // 使用 ErrorTracker 检查 Module 是否应该继续
            match self
                .state
                .status_tracker
                .should_module_continue(&module.id())
                .await
            {
                Ok(ErrorDecision::Continue) => {
                    debug!(
                        "[TaskModuleProcessor] module can continue: module_id={}",
                        module.id()
                    );
                }
                Ok(ErrorDecision::Terminate(reason)) => {
                    // Module 已达到错误阈值，跳过该 Module，继续处理其他 Module
                    error!(
                        "[TaskModuleProcessor] skip terminated module: module_id={} reason={}",
                        module.id(),
                        reason
                    );
                    // 不返回错误，只是跳过这个 Module
                    continue;
                }
                Err(e) => {
                    warn!(
                        "[TaskModuleProcessor] error tracker check failed for module: module_id={} error={}, continue anyway",
                        module.id(),
                        e
                    );
                }
                _ => {}
            }

            module.locker_ttl = module
                .config
                .get_config_value("module_locker_ttl")
                .and_then(|v| v.as_u64())
                .unwrap_or(default_locker_ttl);
            module.bind_task_context(metadata.clone(), login_info.clone());
            modules.push(module);
        }

        let mut filtered_modules: Vec<Module> = Vec::new();
        for x in modules.into_iter() {
            filtered_modules.push(x);
        }
        let modules = filtered_modules;
        ProcessorResult::Success(modules)
    }
}

#[async_trait]
impl EventProcessorTrait<Task, Vec<Module>> for TaskModuleProcessor {
    fn pre_status(&self, _input: &Task) -> Option<EventEnvelope> {
        None
    }

    fn finish_status(&self, _input: &Task, _output: &Vec<Module>) -> Option<EventEnvelope> {
        None
    }

    fn working_status(&self, _input: &Task) -> Option<EventEnvelope> {
        None
    }

    fn error_status(&self, _input: &Task, _err: &Error) -> Option<EventEnvelope> {
        None
    }

    fn retry_status(&self, _input: &Task, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        None
    }
}

pub struct TaskProcessor {
}
#[async_trait]
impl ProcessorTrait<Module, SyncBoxStream<'static, Request>> for TaskProcessor {
    fn name(&self) -> &'static str {
        "TaskProcessor"
    }

    async fn process(
        &self,
        input: Module,
        context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, Request>> {
        debug!(
            "[TaskProcessor] start generate: module_id={} module_name={}",
            input.id(),
            input.module.name()
        );

        let (meta, login_info) = input.runtime_task_context();
        // TaskModel
        // ParserTaskModel.meta=>Task.metadata=>Module.bind_task_context=>Module.generate=>ModuleTrait.generate
        // [LOG_OPTIMIZATION] debug!("[TaskProcessor] start generate: module_id={}", input.id());
        // panic!("DEBUG PANIC: Reached input.generate"); 
        let requests: SyncBoxStream<'static, Request> = match input.generate(meta, login_info).await {
            Ok(stream) => stream,
            Err(e) => {
                warn!("[TaskProcessor] generate error, will retry: {e}");
                return ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                );
            }
        };
        ProcessorResult::Success(requests)
    }
    async fn post_process(
        &self,
        _input: &Module,
        _output: &SyncBoxStream<'static, Request>,
        _context: &ProcessorContext,
    ) -> Result<()> {
        // Stream based post_process can't easily check for empty output without consuming.
        // We might need to move lock release logic elsewhere or wrap the stream.
        // For now, we skip the empty check.
        // if output.is_empty() {
        //    self.sync_service.release_module_locker(&input.id()).await;
        // }
        Ok(())
    }
}
#[async_trait]
impl EventProcessorTrait<Module, SyncBoxStream<'static, Request>> for TaskProcessor {
    fn pre_status(&self, input: &Module) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Started,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &Module, _output: &SyncBoxStream<'static, Request>) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Completed,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn working_status(&self, input: &Module) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Started,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn error_status(&self, input: &Module, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ModuleGenerate,
            EventPhase::Failed,
            ModuleGenerateEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &Module, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Retry,
            json!({
                "data": ModuleGenerateEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
pub struct RequestPublish {
    queue_manager: Arc<QueueManager>,
    state: Arc<State>,
    deduplicator: Option<Arc<Deduplicator>>,
}

#[async_trait]
impl ProcessorTrait<Request, ()> for RequestPublish {
    fn name(&self) -> &'static str {
        "RequestPublish"
    }

    async fn process(&self, input: Request, context: ProcessorContext) -> ProcessorResult<()> {
        let request_id = input.id;
        let module_id = input.module_id();
        let request_hash = input.hash();
        let backpressure_retry_delay_ms = {
            let cfg = self.state.config.read().await;
            cfg.crawler.backpressure_retry_delay_ms
        };

        if let Some(deduplicator) = &self.deduplicator {
            match deduplicator.check_and_set(&request_hash).await {
                Ok(false) => {
                    info!(
                        "[RequestPublish] duplicate request skipped: request_id={} module_id={} hash={}",
                        request_id,
                        module_id,
                        request_hash
                    );
                    return ProcessorResult::Success(());
                }
                Ok(true) => {}
                Err(e) => {
                    warn!(
                        "[RequestPublish] deduplication check failed, allowing request: request_id={} module_id={} error={}",
                        request_id,
                        module_id,
                        e
                    );
                }
            }
        }
        info!(
            "[RequestPublish] publish request: request_id={} module_id={}",
            request_id,
            module_id
        );

        // 1. Persist request to Redis (for chain fallback)
        // Moved from ModuleProcessorWithChain to support streaming
        let id = input.id.to_string();

        // Performance Optimization: Fire-and-forget cache write using spawn
        // Offload serialization and IO to background task to unblock stream processing
        let cache_service = self.state.cache_service.clone();
        let request_clone = input.clone();

        tokio::spawn(async move {
            if let Err(e) = request_clone.send(&id, &cache_service).await {
                // Log at debug level to avoid spamming warns if cache is just busy
                debug!("[RequestPublish] persist failed (background): {e}");
            }
        });

        // [LOG_OPTIMIZATION] debug!("[RequestPublish] start queue send: request_id={}", request_id);
        let tx = self.queue_manager.get_request_push_channel();
        match send_with_backpressure(&tx, QueuedItem::new(input)).await {
            Ok(BackpressureSendState::Direct) => {}
            Ok(BackpressureSendState::RecoveredFromFull) => {
                counter!("request_publish_backpressure_total", "reason" => "queue_full").increment(1);
                warn!(
                    "[RequestPublish] queue full, falling back to awaited send: request_id={} module_id={} remaining_capacity={}",
                    request_id,
                    module_id,
                    tx.capacity()
                );
            }
            Err(err) => {
                if err.after_full {
                    counter!("request_publish_backpressure_total", "reason" => "queue_full").increment(1);
                    warn!(
                        "[RequestPublish] queue full before close: request_id={} module_id={} remaining_capacity={}",
                        request_id,
                        module_id,
                        tx.capacity()
                    );
                }
                counter!("request_publish_backpressure_total", "reason" => "queue_closed").increment(1);
                let retry_reason = format!(
                    "request queue closed: request_id={} module_id={}",
                    err.item.inner.id,
                    err.item.inner.module_id()
                );
                error!("[RequestPublish] {retry_reason}");
                let mut retry_policy = context.retry_policy.unwrap_or_default();
                if let Some(delay_ms) = backpressure_retry_delay_ms {
                    retry_policy.retry_delay = delay_ms.max(1);
                }
                retry_policy.reason = Some(retry_reason);
                return ProcessorResult::RetryableFailure(retry_policy);
            }
        }
        // [LOG_OPTIMIZATION] debug!("[RequestPublish] end queue send: request_id={}", id); // id is string here
        ProcessorResult::Success(())
    }
    async fn handle_error(
        &self,
        input: &Request,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<()> {
        // If we can't publish the request after retries, release the module lock
        // to avoid locking out future runs of this module.
        self.state.status_tracker
            .release_module_locker(&input.module_id())
            .await;
        ProcessorResult::FatalFailure(error)
    }
}
#[async_trait]
impl EventProcessorTrait<Request, ()> for RequestPublish {
    fn pre_status(&self, input: &Request) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Started,
            RequestEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &Request, _output: &()) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Completed,
            RequestEvent::from(input),
        ))
    }

    fn working_status(&self, input: &Request) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Started,
            RequestEvent::from(input),
        ))
    }

    fn error_status(&self, input: &Request, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::RequestPublish,
            EventPhase::Failed,
            RequestEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &Request, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestPublish,
            EventPhase::Retry,
            json!({
                "data": RequestEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
pub struct ConfigProcessor {
    pub state: Arc<State>,
}
#[async_trait]
impl ProcessorTrait<Request, (Request, Option<ModuleConfig>)> for ConfigProcessor {
    fn name(&self) -> &'static str {
        "ConfigProcessor"
    }

    async fn process(
        &self,
        input: Request,
        context: ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
        // ModuleConfig在factory::load_with_model里进行了上传，使用module::id()作为唯一标识
        // 这里进行下载
        match ModuleConfig::sync(&input.module_id(), &self.state.cache_service).await {
            Ok(Some(config)) => ProcessorResult::Success((input, Some(config))),
            Ok(None) => ProcessorResult::Success((input, None)),
            Err(e) => {
                error!(
                    "Failed to fetch config for module {}: {}",
                    input.task_id(),
                    e
                );
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &Request, _context: &ProcessorContext) -> Result<()> {
        self.state.status_tracker.lock_module(&_input.module_id()).await;
        Ok(())
    }
    async fn handle_error(
        &self,
        _input: &Request,
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
        ProcessorResult::Success((_input.clone(), None))
    }
}
#[async_trait]
impl EventProcessorTrait<Request, (Request, Option<ModuleConfig>)> for ConfigProcessor {
    fn pre_status(&self, _input: &Request) -> Option<EventEnvelope> {
        None
    }

    fn finish_status(
        &self,
        _input: &Request,
        _out: &(Request, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        None
    }

    fn working_status(&self, _input: &Request) -> Option<EventEnvelope> {
        None
    }

    fn error_status(&self, _input: &Request, _err: &Error) -> Option<EventEnvelope> {
        None
    }

    fn retry_status(&self, _input: &Request, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        None
    }
}

use futures::stream::Stream;
use std::pin::Pin;
use cacheable::{CacheAble};
use crate::task::{Task, TaskManager};
use crate::task::module::Module;

pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item=T> + Send + Sync + 'a>>;

/// task_model -> task -> request -> () (publish request to queue)
pub struct VecToStreamProcessor<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for VecToStreamProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> VecToStreamProcessor<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<Vec<T>, SyncBoxStream<'static, T>> for VecToStreamProcessor<T> {
    fn name(&self) -> &'static str {
        "VecToStreamProcessor"
    }

    async fn process(
        &self,
        input: Vec<T>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let stream = futures::stream::iter(input);
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<Vec<T>, SyncBoxStream<'static, T>> for VecToStreamProcessor<T> {
    fn pre_status(&self, _input: &Vec<T>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &Vec<T>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &Vec<T>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &Vec<T>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &Vec<T>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

pub struct FlattenStreamVecProcessor<T> {
    _marker: PhantomData<T>,
    #[allow(clippy::type_complexity)]
    logger: Option<Arc<dyn Fn(&T) + Send + Sync>>,
}

impl<T> Default for FlattenStreamVecProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> FlattenStreamVecProcessor<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
            logger: None,
        }
    }
    pub fn with_logger<F>(mut self, logger: F) -> Self
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        self.logger = Some(Arc::new(logger));
        self
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<SyncBoxStream<'static, Vec<T>>, SyncBoxStream<'static, T>>
for FlattenStreamVecProcessor<T>
{
    fn name(&self) -> &'static str {
        "FlattenStreamVecProcessor"
    }

    async fn process(
        &self,
        input: SyncBoxStream<'static, Vec<T>>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let logger = self.logger.clone();
        let stream = input.flat_map(move |v| {
            let logger = logger.clone();
            let iter = v.into_iter().inspect(move |item| {
                if let Some(l) = &logger {
                    l(item);
                }
            });
            futures::stream::iter(iter)
        });
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<SyncBoxStream<'static, Vec<T>>, SyncBoxStream<'static, T>> for FlattenStreamVecProcessor<T> {
    fn pre_status(&self, _input: &SyncBoxStream<'static, Vec<T>>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &SyncBoxStream<'static, Vec<T>>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &SyncBoxStream<'static, Vec<T>>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &SyncBoxStream<'static, Vec<T>>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &SyncBoxStream<'static, Vec<T>>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

pub struct StreamLoggerProcessor<T> {
    _marker: PhantomData<T>,
    name: String,
    #[allow(clippy::type_complexity)]
    logger: Option<Arc<dyn Fn(&T) + Send + Sync>>,
}

impl<T> StreamLoggerProcessor<T> {
    pub fn new(name: &str) -> Self {
        Self {
            _marker: PhantomData,
            name: name.to_string(),
            logger: None,
        }
    }
    pub fn with_logger<F>(mut self, logger: F) -> Self
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        self.logger = Some(Arc::new(logger));
        self
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<SyncBoxStream<'static, T>, SyncBoxStream<'static, T>>
for StreamLoggerProcessor<T>
{
    fn name(&self) -> &'static str {
        "StreamLoggerProcessor"
    }

    async fn process(
        &self,
        input: SyncBoxStream<'static, T>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let logger = self.logger.clone();
        let stream = input.map(move |item| {
            if let Some(l) = &logger {
                l(&item);
            }
            item
        });
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<SyncBoxStream<'static, T>, SyncBoxStream<'static, T>> for StreamLoggerProcessor<T> {
    fn pre_status(&self, _input: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &SyncBoxStream<'static, T>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &SyncBoxStream<'static, T>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &SyncBoxStream<'static, T>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

pub struct FlattenStreamProcessor<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for FlattenStreamProcessor<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> FlattenStreamProcessor<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> ProcessorTrait<SyncBoxStream<'static, SyncBoxStream<'static, T>>, SyncBoxStream<'static, T>>
for FlattenStreamProcessor<T>
{
    fn name(&self) -> &'static str {
        "FlattenStreamProcessor"
    }

    async fn process(
        &self,
        input: SyncBoxStream<'static, SyncBoxStream<'static, T>>,
        _context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, T>> {
        let stream = input.flatten();
        let boxed: SyncBoxStream<'static, T> = Box::pin(stream);
        ProcessorResult::Success(boxed)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> EventProcessorTrait<SyncBoxStream<'static, SyncBoxStream<'static, T>>, SyncBoxStream<'static, T>> for FlattenStreamProcessor<T> {
    fn pre_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>) -> Option<EventEnvelope> { None }
    fn finish_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>, _output: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> { None }
    fn working_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>) -> Option<EventEnvelope> { None }
    fn error_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>, _err: &Error) -> Option<EventEnvelope> { None }
    fn retry_status(&self, _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> { None }
}

async fn build_request_deduplicator(state: &Arc<State>) -> Option<Arc<Deduplicator>> {
    let dedup_ttl = state
        .config
        .read()
        .await
        .crawler
        .dedup_ttl_secs
        .unwrap_or(3600) as usize;
    let namespace = state.cache_service.namespace().to_string();
    state
        .locker
        .get_pool()
        .map(|pool| Arc::new(Deduplicator::new(pool.clone(), dedup_ttl, namespace)))
}

pub struct UnifiedTaskIngressChain {
    task_chain: Arc<EventAwareTypedChain<TaskModel, SyncBoxStream<'static, ()>>>,
    parser_chain: Arc<EventAwareTypedChain<ParserTaskModel, SyncBoxStream<'static, ()>>>,
    error_chain: Arc<EventAwareTypedChain<ErrorTaskModel, SyncBoxStream<'static, ()>>>,
}

impl UnifiedTaskIngressChain {
    pub fn new(
        task_chain: Arc<EventAwareTypedChain<TaskModel, SyncBoxStream<'static, ()>>>,
        parser_chain: Arc<EventAwareTypedChain<ParserTaskModel, SyncBoxStream<'static, ()>>>,
        error_chain: Arc<EventAwareTypedChain<ErrorTaskModel, SyncBoxStream<'static, ()>>>,
    ) -> Self {
        Self {
            task_chain,
            parser_chain,
            error_chain,
        }
    }

    pub async fn execute(
        &self,
        input: UnifiedTaskInput,
        context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
        match input {
            UnifiedTaskInput::Task(task) => self.task_chain.execute(task, context).await,
            UnifiedTaskInput::ParserTask(task) => self.parser_chain.execute(task, context).await,
            UnifiedTaskInput::ErrorTask(task) => self.error_chain.execute(task, context).await,
        }
    }
}

pub async fn create_unified_task_ingress_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    state: Arc<State>,
    lua_registry: Option<Arc<LuaScriptRegistry>>,
) -> UnifiedTaskIngressChain {
    let task_concurrency = state.config.read().await.crawler.task_concurrency.unwrap_or(1024);
    let parser_concurrency = {
        let cfg = state.config.read().await;
        cfg.crawler
            .parser_concurrency
            .or(cfg.crawler.task_concurrency)
            .unwrap_or(256)
    };
    let error_task_concurrency = {
        let cfg = state.config.read().await;
        cfg.crawler
            .error_task_concurrency
            .or(cfg.crawler.parser_concurrency)
            .or(cfg.crawler.task_concurrency)
            .unwrap_or(4)
    };
    let task_publish_concurrency = state.config.read().await.crawler.publish_concurrency.unwrap_or(1024);
    let parser_publish_concurrency = state.config.read().await.crawler.publish_concurrency.unwrap_or(256);
    let error_publish_concurrency = state.config.read().await.crawler.publish_concurrency.unwrap_or(32);

    let task_deduplicator = build_request_deduplicator(&state).await;
    let task_chain = Arc::new(
        EventAwareTypedChain::<TaskModel, TaskModel>::new(event_bus.clone())
            .then::<Task, _>(TaskModelProcessor {
                task_manager: task_manager.clone(),
                state: state.clone(),
                queue_manager: queue_manager.clone(),
                event_bus: event_bus.clone(),
                threshold_decision_service: Arc::new(StatusTrackerThresholdDecisionService::new(
                    state.clone(),
                    lua_registry.clone(),
                )),
            })
            .then::<Vec<Module>, _>(TaskModuleProcessor {
                state: state.clone(),
            })
            .then(VecToStreamProcessor::new())
            .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
                TaskProcessor {},
                task_concurrency,
                ErrorStrategy::Skip,
            )
            .then_one_shot(FlattenStreamProcessor::new())
            .then_one_shot(StreamLoggerProcessor::new("AfterFlatten").with_logger(|req: &Request| {
                info!(
                    "[FlattenStreamProcessor] yielding request: request_id={}",
                    req.id
                );
            }))
            .then_map_stream_in_with_strategy::<(), _>(
                RequestPublish {
                    queue_manager: queue_manager.clone(),
                    state: state.clone(),
                    deduplicator: task_deduplicator,
                },
                task_publish_concurrency,
                ErrorStrategy::Skip,
            ),
    );

    let parser_deduplicator = build_request_deduplicator(&state).await;
    let parser_chain = Arc::new(
        EventAwareTypedChain::<ParserTaskModel, ParserTaskModel>::new(event_bus.clone())
            .then::<Task, _>(TaskModelProcessor {
                task_manager: task_manager.clone(),
                state: state.clone(),
                queue_manager: queue_manager.clone(),
                event_bus: event_bus.clone(),
                threshold_decision_service: Arc::new(StatusTrackerThresholdDecisionService::new(
                    state.clone(),
                    lua_registry.clone(),
                )),
            })
            .then::<Vec<Module>, _>(TaskModuleProcessor {
                state: state.clone(),
            })
            .then(VecToStreamProcessor::new())
            .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
                TaskProcessor {},
                parser_concurrency,
                ErrorStrategy::Skip,
            )
            .then_one_shot(FlattenStreamProcessor::new())
            .then_map_stream_in_with_strategy::<(), _>(
                RequestPublish {
                    queue_manager: queue_manager.clone(),
                    state: state.clone(),
                    deduplicator: parser_deduplicator,
                },
                parser_publish_concurrency,
                ErrorStrategy::Skip,
            ),
    );

    let error_deduplicator = build_request_deduplicator(&state).await;
    let error_chain = Arc::new(
        EventAwareTypedChain::<ErrorTaskModel, ErrorTaskModel>::new(event_bus)
            .then::<Task, _>(TaskModelProcessor {
                task_manager,
                state: state.clone(),
                queue_manager: queue_manager.clone(),
                event_bus: None,
                threshold_decision_service: Arc::new(StatusTrackerThresholdDecisionService::new(
                    state.clone(),
                    lua_registry,
                )),
            })
            .then::<Vec<Module>, _>(TaskModuleProcessor {
                state: state.clone(),
            })
            .then(VecToStreamProcessor::new())
            .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
                TaskProcessor {},
                error_task_concurrency,
                ErrorStrategy::Skip,
            )
            .then_one_shot(FlattenStreamProcessor::new())
            .then_map_stream_in_with_strategy::<(), _>(
                RequestPublish {
                    queue_manager,
                    state,
                    deduplicator: error_deduplicator,
                },
                error_publish_concurrency,
                ErrorStrategy::Skip,
            ),
    );

    UnifiedTaskIngressChain::new(task_chain, parser_chain, error_chain)
}
