#![allow(unused)]
use crate::common::status_tracker::ErrorDecision;
use crate::engine::events::{
    EventBus, EventEnvelope, EventPhase, EventType, ModuleGenerateEvent, ParserDispatchEvent,
    RequestEvent, TaskModelEvent,
};
use crate::engine::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use log::info;
use log::{debug, error, warn};

use crate::common::metrics as metrics_facade;
use crate::common::model::chain_key;
use crate::common::model::message::{TaskEvent, UnifiedTaskInput};
use crate::common::model::workflow_profile::TaskProfileSnapshot;
use crate::common::model::{
    ErrorDispatchContext, ExecutionMeta, ModuleConfig, NodeDispatchEnvelope, NodeErrorEnvelope,
    NodeInput, ParserDispatchContext, PayloadCodec, Request, RoutingMeta, RuntimeNodeRoutingHint,
    TypedEnvelope,
};
use crate::common::state::State;
use crate::errors::{Error, ModuleError, RequestError, Result};
use async_trait::async_trait;
use futures::stream::StreamExt;
use metrics::counter;
use serde_json::json;

use crate::common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use crate::common::processors::processor_chain::ErrorStrategy;
use crate::engine::chain::backpressure::{BackpressureSendState, send_with_backpressure};
use crate::engine::task::module_dag_orchestrator::ModuleDagOrchestrator;
use crate::engine::task::module_node_runtime_bridge::{
    SchedulerNodeGenerateRuntimeInput, decode_request_batch_payload,
};
use crate::engine::task::parser_error_adapter::{
    ErrorEnvelopeSeed, build_error_envelope_from_seed, extract_error_envelope_seed,
    extract_parser_dispatch_seed,
};
use crate::engine::task::request_response_adapter::build_request_dispatch;
use crate::queue::{QueueManager, QueuedItem};
use crate::schedule::DagError;
use crate::schedule::dag::NodePlacement;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// High-level action produced by threshold checks in ingress chains.
#[derive(Debug, Clone)]
pub enum ChainDecision {
    /// Continue processing without side effects.
    Continue,
    /// Retry current path after delay.
    RetryAfter {
        delay: std::time::Duration,
        action_applied: bool,
    },
    /// Stop processing for the current module scope.
    TerminateModule {
        reason: String,
        action_applied: bool,
    },
    /// Stop processing for the current task scope.
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
mod threshold_decision_tests {
    use super::ChainDecision;

    #[test]
    fn chain_decision_action_applied_tracks_external_side_effects() {
        let retry_applied = ChainDecision::RetryAfter {
            delay: std::time::Duration::from_millis(100),
            action_applied: true,
        };
        let retry_unapplied = ChainDecision::RetryAfter {
            delay: std::time::Duration::from_millis(100),
            action_applied: false,
        };
        assert!(retry_applied.action_applied());
        assert!(!retry_unapplied.action_applied());

        let terminate_module_applied = ChainDecision::TerminateModule {
            reason: "module limit".to_string(),
            action_applied: true,
        };
        let terminate_module_unapplied = ChainDecision::TerminateModule {
            reason: "module limit".to_string(),
            action_applied: false,
        };
        assert!(terminate_module_applied.action_applied());
        assert!(!terminate_module_unapplied.action_applied());

        let terminate_task_applied = ChainDecision::TerminateTask {
            reason: "task limit".to_string(),
            action_applied: true,
        };
        let terminate_task_unapplied = ChainDecision::TerminateTask {
            reason: "task limit".to_string(),
            action_applied: false,
        };
        assert!(terminate_task_applied.action_applied());
        assert!(!terminate_task_unapplied.action_applied());
    }
}

#[cfg(test)]
mod task_routing_policy_tests {
    use super::{
        derive_generate_retry_policy, local_fast_path_error, module_matches_pending_context,
    };
    use crate::common::model::{ExecutionMark, RuntimeNodeRoutingHint};
    use crate::common::processors::processor::{ProcessorContext, RetryPolicy};
    use crate::errors::RequestError;
    use crate::schedule::dag::{DagNodeExecutionPolicy, DagNodeRetryMode, NodePlacement};

    #[test]
    fn module_target_filter_uses_pending_ctx_module_id() {
        let pending_ctx = Some(ExecutionMark::default().with_module_id("module-a"));
        assert!(module_matches_pending_context("module-a", &pending_ctx));
        assert!(!module_matches_pending_context("module-b", &pending_ctx));
    }

    #[test]
    fn local_fast_path_rejects_remote_runtime_hint() {
        let err = local_fast_path_error(
            "module-a",
            &RuntimeNodeRoutingHint::new("node-a")
                .with_placement(NodePlacement::remote("wg-parser")),
        )
        .expect("remote placement should be rejected on the local fast path");

        assert!(
            err.to_string()
                .contains("local fast path cannot execute remote-placed node")
        );
    }

    #[test]
    fn retry_policy_uses_runtime_hint_policy() {
        let hint = RuntimeNodeRoutingHint::new("node-a").with_policy(DagNodeExecutionPolicy {
            max_retries: 5,
            timeout_ms: Some(1200),
            retry_backoff_ms: 250,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::RetryableOnly,
            circuit_breaker_failure_threshold: None,
            circuit_breaker_open_ms: 0,
        });
        let context = ProcessorContext::default().with_retry_policy(RetryPolicy {
            max_retries: 1,
            retry_delay: 10,
            current_retry: 2,
            reason: None,
            meta: Default::default(),
        });
        let err: crate::errors::Error = RequestError::Timeout.into();

        let retry_policy = derive_generate_retry_policy(&context, Some(&hint), &err);
        assert_eq!(retry_policy.max_retries, 5);
        assert_eq!(retry_policy.retry_delay, 250);
        assert_eq!(retry_policy.current_retry, 2);
        assert!(
            retry_policy
                .reason
                .as_deref()
                .unwrap_or_default()
                .contains("request error")
        );
    }
}

fn module_matches_pending_context(
    module_id: &str,
    pending_ctx: &Option<crate::common::model::ExecutionMark>,
) -> bool {
    let Some(target_module_id) = pending_ctx
        .as_ref()
        .and_then(|ctx| ctx.module_id.as_deref())
        .filter(|module_id| !module_id.is_empty())
    else {
        return true;
    };

    target_module_id == module_id
}

fn local_fast_path_error(module_id: &str, hint: &RuntimeNodeRoutingHint) -> Option<Error> {
    match hint.placement.as_ref() {
        Some(NodePlacement::Remote { worker_group }) => Some(
            RequestError::InvalidMetaForRemote(
                std::io::Error::other(
                    format!(
                        "local fast path cannot execute remote-placed node '{}' for module '{}' (worker_group='{}')",
                        hint.node_key, module_id, worker_group
                    ),
                )
                .into(),
            )
            .into(),
        ),
        _ => None,
    }
}

fn derive_generate_retry_policy(
    context: &ProcessorContext,
    hint: Option<&RuntimeNodeRoutingHint>,
    err: &Error,
) -> RetryPolicy {
    let mut policy = context.retry_policy.clone().unwrap_or_default();
    if let Some(execution_policy) = hint.and_then(|hint| hint.policy.as_ref()) {
        policy.max_retries = execution_policy.max_retries as u32;
        policy.retry_delay = execution_policy.retry_backoff_ms;
    }
    policy.reason = Some(err.to_string());
    policy
}

#[async_trait]
/// Abstraction for threshold and termination decisions.
pub trait ThresholdDecisionService: Send + Sync {
    /// Performs preflight validation before task expansion.
    async fn task_precheck(&self, task_id: &str) -> ChainDecision;
    /// Decides how to handle an `ErrorTaskModel` path.
    async fn error_task_decide(&self, input: &ErrorEnvelopeSeed) -> ChainDecision;
}

/// Default threshold decision service backed by `StatusTracker`.
#[derive(Clone)]
pub struct StatusTrackerThresholdDecisionService {
    state: Arc<State>,
}

impl StatusTrackerThresholdDecisionService {
    pub fn new(state: Arc<State>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ThresholdDecisionService for StatusTrackerThresholdDecisionService {
    async fn task_precheck(&self, task_id: &str) -> ChainDecision {
        match self
            .state
            .status_tracker
            .should_task_continue(task_id)
            .await
        {
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

    async fn error_task_decide(&self, input: &ErrorEnvelopeSeed) -> ChainDecision {
        let task_id = chain_key::task_runtime_id(
            &input.task_model.platform,
            &input.task_model.account,
            input.run_id,
        );

        let parse_error: Error =
            ModuleError::ModuleNotFound(input.error_message.clone().into()).into();
        let mut retry_after: Option<std::time::Duration> = None;

        if let Some(modules) = &input.task_model.module {
            for module_name in modules {
                let module_id = chain_key::module_runtime_id(
                    &input.task_model.account,
                    &input.task_model.platform,
                    module_name,
                );
                // Run-scoped module ID for StatusTracker error isolation across runs
                let module_id_scoped = format!("{}-{}", module_id, input.run_id);

                if input.prefix_request != Uuid::nil() {
                    match self
                        .state
                        .status_tracker
                        .record_parse_error(
                            &task_id,
                            &module_id_scoped,
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
                                task_id, module_id_scoped, err
                            );
                        }
                    }
                }

                match self
                    .state
                    .status_tracker
                    .should_module_continue(&module_id_scoped)
                    .await
                {
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

/// First ingress processor for `TaskModel` and parser-dispatch variants.
///
/// Responsibilities:
/// - pre-check task thresholds,
/// - load/construct `Task`,
/// - handle error-task retry/terminate decisions,
/// - emit semantic events for observability.
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

    async fn error_task_decide(&self, input: &ErrorEnvelopeSeed) -> ChainDecision {
        self.threshold_decision_service
            .error_task_decide(input)
            .await
    }

    async fn persist_error_retry_schedule(
        &self,
        input: &ErrorEnvelopeSeed,
        delay: std::time::Duration,
    ) {
        let task_id = chain_key::task_runtime_id(
            &input.task_model.platform,
            &input.task_model.account,
            input.run_id,
        );
        let module_id = input
            .context
            .module_id
            .clone()
            .or_else(|| {
                input.task_model.module.as_ref().and_then(|m| {
                    m.first().map(|name| {
                        chain_key::module_runtime_id(
                            &input.task_model.account,
                            &input.task_model.platform,
                            name,
                        )
                    })
                })
            })
            .unwrap_or_else(|| {
                chain_key::module_runtime_id(
                    &input.task_model.account,
                    &input.task_model.platform,
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
        input: &ErrorEnvelopeSeed,
        terminate_module: Option<&str>,
        reason: &str,
    ) {
        let task_id = chain_key::task_runtime_id(
            &input.task_model.platform,
            &input.task_model.account,
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
        input: &ErrorEnvelopeSeed,
        decision: &str,
        reason: &str,
    ) {
        let Some(event_bus) = &self.event_bus else {
            return;
        };

        let payload = json!({
            "run_id": input.run_id.to_string(),
            "account": input.task_model.account,
            "platform": input.task_model.platform,
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

    async fn process_parser_dispatch(
        &self,
        input: NodeDispatchEnvelope,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        let parser_seed = match extract_parser_dispatch_seed(&input) {
            Ok(seed) => seed,
            Err(err) => return ProcessorResult::FatalFailure(err),
        };

        let task_id = chain_key::task_runtime_id(
            &parser_seed.task_model.platform,
            &parser_seed.task_model.account,
            parser_seed.run_id,
        );
        match self.task_precheck(&task_id).await {
            ChainDecision::Continue => {}
            ChainDecision::TerminateTask { reason, .. } => {
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            _ => {}
        }

        match self.task_manager.load_parser_dispatch(&input).await {
            Ok(task) => ProcessorResult::Success(task),
            Err(err) => ProcessorResult::RetryableFailure(
                context
                    .retry_policy
                    .unwrap_or(RetryPolicy::default().with_reason(err.to_string())),
            ),
        }
    }

    async fn handle_parser_dispatch_error(
        &self,
        input: &NodeDispatchEnvelope,
        error: Error,
    ) -> ProcessorResult<Task> {
        let parser_seed = match extract_parser_dispatch_seed(input) {
            Ok(seed) => seed,
            Err(err) => return ProcessorResult::FatalFailure(err),
        };
        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorEnvelopeSeed {
            request_id: Uuid::nil(),
            task_model: parser_seed.task_model.clone(),
            error_message: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: parser_seed.run_id,
            prefix_request: parser_seed.prefix_request,
        };
        let envelope = match build_error_envelope_from_seed(
            &error_msg,
            self.queue_manager.namespace.clone(),
        ) {
            Ok(envelope) => envelope,
            Err(err) => return ProcessorResult::FatalFailure(err),
        };
        if let Err(err) = sender.send(QueuedItem::new(envelope)).await {
            error!("[TaskModelProcessor<ParserDispatch>] failed to enqueue ErrorTaskModel: {err}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }

    async fn process_error_envelope(
        &self,
        input: NodeErrorEnvelope,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        let error_seed = match extract_error_envelope_seed(&input) {
            Ok(seed) => seed,
            Err(err) => return ProcessorResult::FatalFailure(err),
        };

        let task_id = chain_key::task_runtime_id(
            &error_seed.task_model.platform,
            &error_seed.task_model.account,
            error_seed.run_id,
        );
        match self.error_task_decide(&error_seed).await {
            ChainDecision::Continue => {
                counter!("mocra_error_task_threshold_decision_total", "decision" => "continue")
                    .increment(1);
                debug!(
                    "[TaskModelProcessor<ErrorEnvelope>] task can continue: task_id={}",
                    task_id
                );
            }
            ChainDecision::RetryAfter {
                delay,
                action_applied,
            } => {
                counter!("mocra_error_task_threshold_decision_total", "decision" => "retry_after")
                    .increment(1);
                if !(ChainDecision::RetryAfter {
                    delay,
                    action_applied,
                })
                .action_applied()
                {
                    self.persist_error_retry_schedule(&error_seed, delay).await;
                }
                let mut retry_policy = context.retry_policy.unwrap_or_default();
                retry_policy.retry_delay = std::cmp::max(delay.as_millis() as u64, 1);
                retry_policy.reason = Some(format!(
                    "error task retry after {}ms",
                    retry_policy.retry_delay
                ));
                return ProcessorResult::RetryableFailure(retry_policy);
            }
            ChainDecision::TerminateModule {
                reason,
                action_applied,
            } => {
                counter!("mocra_error_task_threshold_decision_total", "decision" => "terminate_module").increment(1);
                let module_id = error_seed
                    .context
                    .module_id
                    .clone()
                    .or_else(|| {
                        error_seed.task_model.module.as_ref().and_then(|m| {
                            m.first().map(|name| {
                                chain_key::module_runtime_id(
                                    &error_seed.task_model.account,
                                    &error_seed.task_model.platform,
                                    name,
                                )
                            })
                        })
                    })
                    .unwrap_or_else(|| {
                        chain_key::module_runtime_id(
                            &error_seed.task_model.account,
                            &error_seed.task_model.platform,
                            "unknown",
                        )
                    });
                if !(ChainDecision::TerminateModule {
                    reason: reason.clone(),
                    action_applied,
                })
                .action_applied()
                {
                    self.persist_terminate_mark(&error_seed, Some(&module_id), &reason)
                        .await;
                }
                self.emit_threshold_terminated_event(&error_seed, "terminate_module", &reason)
                    .await;
                return ProcessorResult::FatalFailure(
                    ModuleError::ModuleMaxError(reason.into()).into(),
                );
            }
            ChainDecision::TerminateTask {
                reason,
                action_applied,
            } => {
                counter!("mocra_error_task_threshold_decision_total", "decision" => "terminate_task").increment(1);
                if !(ChainDecision::TerminateTask {
                    reason: reason.clone(),
                    action_applied,
                })
                .action_applied()
                {
                    self.persist_terminate_mark(&error_seed, None, &reason)
                        .await;
                }
                self.emit_threshold_terminated_event(&error_seed, "terminate_task", &reason)
                    .await;
                if let Err(err) = self
                    .queue_manager
                    .send_to_dlq("error_task", &input, &reason)
                    .await
                {
                    error!(
                        "[TaskModelProcessor<ErrorEnvelope>] failed to send to DLQ: {}",
                        err
                    );
                }
                return ProcessorResult::FatalFailure(
                    ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
        }

        match self.task_manager.load_error_envelope(&input).await {
            Ok(task) => ProcessorResult::Success(task),
            Err(err) => ProcessorResult::RetryableFailure(
                context
                    .retry_policy
                    .unwrap_or(RetryPolicy::default().with_reason(err.to_string())),
            ),
        }
    }
}
#[async_trait]
impl ProcessorTrait<TaskEvent, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(&self, input: TaskEvent, context: ProcessorContext) -> ProcessorResult<Task> {
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

        let task_id = chain_key::task_runtime_id(&input.platform, &input.account, input.run_id);
        match self.task_precheck(&task_id).await {
            ChainDecision::Continue => {}
            ChainDecision::TerminateTask { reason, .. } => {
                error!(
                    "[TaskModelProcessor<TaskModel>] task terminated (pre-check): task_id={} reason={}",
                    task_id, reason
                );
                if let Err(e) = self
                    .queue_manager
                    .send_to_dlq("task", &input, &reason)
                    .await
                {
                    error!(
                        "[TaskModelProcessor<TaskModel>] failed to send to DLQ: {}",
                        e
                    );
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
                        "[TaskModelProcessor<TaskModel>] all target modules locked, defer via retry policy: account={} platform={} run_id={}",
                        input.account, input.platform, input.run_id
                    );
                    // Avoid duplicate amplification: this branch used to both
                    // push back into `task-normal` and return RetryableFailure,
                    // which could multiply the same TaskModel exponentially.
                    // Keep a single retry path (executor-managed retry only).
                    return ProcessorResult::RetryableFailure(
                        context
                            .retry_policy
                            .unwrap_or_default()
                            .with_reason("all target modules locked".to_string()),
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
                debug!(
                    "[TaskModelProcessor] load_with_model failed: account={} platform={} run_id={} error={e}",
                    input.account, input.platform, input.run_id
                );
                warn!(
                    "[TaskModelProcessor<TaskModel>] load_with_model failed, will retry: account={} platform={} run_id={} error={e}",
                    input.account, input.platform, input.run_id
                );
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, _input: &TaskEvent, _context: &ProcessorContext) -> Result<()> {
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &TaskEvent,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {
        let sender = self.queue_manager.get_error_push_channel();
        let error_msg = ErrorEnvelopeSeed {
            request_id: Uuid::nil(),
            task_model: input.clone(),
            error_message: error.to_string(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            metadata: Default::default(),
            context: Default::default(),
            run_id: input.run_id,
            prefix_request: Uuid::nil(),
        };
        let envelope = match build_error_envelope_from_seed(
            &error_msg,
            self.queue_manager.namespace.clone(),
        ) {
            Ok(envelope) => envelope,
            Err(err) => {
                error!(
                    "[TaskModelProcessor<TaskModel>] failed to build ErrorTaskModel envelope: {err}"
                );
                return ProcessorResult::FatalFailure(
                    ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
                );
            }
        };
        if let Err(e) = sender.send(QueuedItem::new(envelope)).await {
            error!("[TaskModelProcessor<TaskModel>] failed to enqueue ErrorTaskModel: {e}");
        }
        ProcessorResult::FatalFailure(
            ModuleError::ModuleNotFound("Failed to load task, re-queued".into()).into(),
        )
    }
}
#[async_trait]
impl EventProcessorTrait<TaskEvent, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &TaskEvent) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Started,
            TaskModelEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &TaskEvent, output: &Task) -> Option<EventEnvelope> {
        let mut task_model_event: TaskModelEvent = input.into();
        task_model_event.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Completed,
            task_model_event,
        ))
    }

    fn working_status(&self, input: &TaskEvent) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::TaskModel,
            EventPhase::Started,
            TaskModelEvent::from(input),
        ))
    }

    fn error_status(&self, input: &TaskEvent, error: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::TaskModel,
            EventPhase::Failed,
            TaskModelEvent::from(input),
            error,
        ))
    }

    fn retry_status(&self, input: &TaskEvent, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
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
impl ProcessorTrait<NodeDispatchEnvelope, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: NodeDispatchEnvelope,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        self.process_parser_dispatch(input, context).await
    }

    async fn pre_process(
        &self,
        _input: &NodeDispatchEnvelope,
        _context: &ProcessorContext,
    ) -> Result<()> {
        Ok(())
    }

    async fn handle_error(
        &self,
        input: &NodeDispatchEnvelope,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Task> {
        self.handle_parser_dispatch_error(input, error).await
    }
}

#[async_trait]
impl EventProcessorTrait<NodeDispatchEnvelope, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &NodeDispatchEnvelope) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Started,
            ParserDispatchEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &NodeDispatchEnvelope, output: &Task) -> Option<EventEnvelope> {
        let mut evt: ParserDispatchEvent = input.into();
        evt.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Completed,
            evt,
        ))
    }

    fn working_status(&self, input: &NodeDispatchEnvelope) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Started,
            ParserDispatchEvent::from(input),
        ))
    }

    fn error_status(&self, input: &NodeDispatchEnvelope, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ParserDispatch,
            EventPhase::Failed,
            ParserDispatchEvent::from(input),
            err,
        ))
    }

    fn retry_status(
        &self,
        input: &NodeDispatchEnvelope,
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Retry,
            json!({
                "data": ParserDispatchEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
#[async_trait]
impl ProcessorTrait<NodeErrorEnvelope, Task> for TaskModelProcessor {
    fn name(&self) -> &'static str {
        "TaskModelProcessor"
    }

    async fn process(
        &self,
        input: NodeErrorEnvelope,
        context: ProcessorContext,
    ) -> ProcessorResult<Task> {
        self.process_error_envelope(input, context).await
    }

    async fn pre_process(
        &self,
        _input: &NodeErrorEnvelope,
        _context: &ProcessorContext,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl EventProcessorTrait<NodeErrorEnvelope, Task> for TaskModelProcessor {
    fn pre_status(&self, input: &NodeErrorEnvelope) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Started,
            ParserDispatchEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &NodeErrorEnvelope, output: &Task) -> Option<EventEnvelope> {
        let mut evt: ParserDispatchEvent = input.into();
        evt.modules = Some(output.get_module_names());
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Completed,
            evt,
        ))
    }

    fn working_status(&self, input: &NodeErrorEnvelope) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Started,
            ParserDispatchEvent::from(input),
        ))
    }

    fn error_status(&self, input: &NodeErrorEnvelope, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ParserDispatch,
            EventPhase::Failed,
            ParserDispatchEvent::from(input),
            err,
        ))
    }

    fn retry_status(
        &self,
        input: &NodeErrorEnvelope,
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ParserDispatch,
            EventPhase::Retry,
            json!({
                "data": ParserDispatchEvent::from(input),
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
            if !module_matches_pending_context(&module.id(), &module.pending_ctx) {
                debug!(
                    "[TaskModuleProcessor] skip module due to pending_ctx target mismatch: module_id={} target={:?}",
                    module.id(),
                    module
                        .pending_ctx
                        .as_ref()
                        .and_then(|ctx| ctx.module_id.as_deref())
                );
                continue;
            }

            // Check module-level threshold before generating requests.
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
                    // Module reached threshold; skip it and continue with others.
                    error!(
                        "[TaskModuleProcessor] skip terminated module: module_id={} reason={}",
                        module.id(),
                        reason
                    );
                    // Skip only this module; keep pipeline alive.
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
    task_manager: Arc<TaskManager>,
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

        let runtime_hint = input
            .processor
            .resolve_runtime_hint(&input.pending_ctx)
            .await;
        if let Some(err) = runtime_hint
            .as_ref()
            .and_then(|hint| local_fast_path_error(&input.id(), hint))
        {
            warn!("[TaskProcessor] generate blocked by runtime routing hint: {err}");
            return ProcessorResult::FatalFailure(err);
        }

        let (meta, login_info) = input.runtime_task_context();

        // Context propagation path:
        // `ParserDispatchSeed.metadata -> Task.metadata -> Module.bind_task_context -> Module.generate`.
        let requests: SyncBoxStream<'static, Request> = match input.generate(meta, login_info).await
        {
            Ok(stream) => stream,
            Err(e) => {
                warn!("[TaskProcessor] generate error, will retry: {e}");
                return ProcessorResult::RetryableFailure(derive_generate_retry_policy(
                    &context,
                    runtime_hint.as_ref(),
                    &e,
                ));
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
        // Stream-based output cannot be checked for emptiness here without consumption.
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

    fn finish_status(
        &self,
        input: &Module,
        _output: &SyncBoxStream<'static, Request>,
    ) -> Option<EventEnvelope> {
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
}

#[async_trait]
impl ProcessorTrait<Request, ()> for RequestPublish {
    fn name(&self) -> &'static str {
        "RequestPublish"
    }

    async fn process(&self, input: Request, context: ProcessorContext) -> ProcessorResult<()> {
        let request_id = input.id;
        let module_id = input.module_id();
        let backpressure_retry_delay_ms = {
            let cfg = self.state.config.read().await;
            cfg.crawler.backpressure_retry_delay_ms
        };

        info!(
            "[RequestPublish] publish request: request_id={} module_id={}",
            request_id, module_id
        );

        // Persist request for downstream fallback/recovery.
        let id = input.id.to_string();

        // Fire-and-forget cache persistence to avoid blocking hot stream path.
        let cache_service = self.state.cache_service.clone();
        let request_clone = input.clone();

        tokio::spawn(async move {
            if let Err(e) = request_clone.send(&id, &cache_service).await {
                // Log at debug level to avoid spamming warns if cache is just busy
                debug!("[RequestPublish] persist failed (background): {e}");
            }
        });

        let dispatch = match build_request_dispatch(&input, self.queue_manager.namespace.clone()) {
            Ok(dispatch) => dispatch,
            Err(err) => {
                let mut retry_policy = context.retry_policy.unwrap_or_default();
                if let Some(delay_ms) = backpressure_retry_delay_ms {
                    retry_policy.retry_delay = delay_ms.max(1);
                }
                retry_policy.reason = Some(format!(
                    "request envelope build failed: request_id={} module_id={} error={err}",
                    request_id, module_id
                ));
                return ProcessorResult::RetryableFailure(retry_policy);
            }
        };

        let tx = self.queue_manager.get_request_push_channel();
        match send_with_backpressure(&tx, QueuedItem::new(dispatch)).await {
            Ok(BackpressureSendState::Direct) => {}
            Ok(BackpressureSendState::RecoveredFromFull) => {
                counter!("mocra_request_publish_backpressure_total", "reason" => "queue_full")
                    .increment(1);
                warn!(
                    "[RequestPublish] queue full, falling back to awaited send: request_id={} module_id={} remaining_capacity={}",
                    request_id,
                    module_id,
                    tx.capacity()
                );
            }
            Err(err) => {
                if err.after_full {
                    counter!("mocra_request_publish_backpressure_total", "reason" => "queue_full")
                        .increment(1);
                    warn!(
                        "[RequestPublish] queue full before close: request_id={} module_id={} remaining_capacity={}",
                        request_id,
                        module_id,
                        tx.capacity()
                    );
                }
                counter!("mocra_request_publish_backpressure_total", "reason" => "queue_closed")
                    .increment(1);
                let retry_reason = format!(
                    "request queue closed: request_id={} module_id={}",
                    request_id, module_id
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
        self.state
            .status_tracker
            .release_module_locker(&input.module_runtime_id())
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
    pub namespace: String,
}
#[async_trait]
impl ProcessorTrait<Request, (Request, Arc<TaskProfileSnapshot>)> for ConfigProcessor {
    fn name(&self) -> &'static str {
        "ConfigProcessor"
    }

    async fn process(
        &self,
        input: Request,
        _context: ProcessorContext,
    ) -> ProcessorResult<(Request, Arc<TaskProfileSnapshot>)> {
        let profile = self
            .state
            .profile_store
            .get_profile(
                &self.namespace,
                &input.account,
                &input.platform,
                &input.module,
            )
            .unwrap_or_default();
        ProcessorResult::Success((input, Arc::new(profile)))
    }
    async fn pre_process(&self, _input: &Request, _context: &ProcessorContext) -> Result<()> {
        self.state
            .status_tracker
            .lock_module(&_input.module_runtime_id())
            .await;
        Ok(())
    }
    async fn handle_error(
        &self,
        _input: &Request,
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<(Request, Arc<TaskProfileSnapshot>)> {
        ProcessorResult::Success((_input.clone(), Arc::new(TaskProfileSnapshot::default())))
    }
}
#[async_trait]
impl EventProcessorTrait<Request, (Request, Arc<TaskProfileSnapshot>)> for ConfigProcessor {
    fn pre_status(&self, _input: &Request) -> Option<EventEnvelope> {
        None
    }

    fn finish_status(
        &self,
        _input: &Request,
        _out: &(Request, Arc<TaskProfileSnapshot>),
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

use crate::cacheable::CacheAble;
use crate::engine::task::module::Module;
use crate::engine::task::{Task, TaskManager};
use futures::stream::Stream;
use std::pin::Pin;

pub type SyncBoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + Sync + 'a>>;

/// Converts a vector into a boxed stream for stream-native chain stages.
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
impl<T: Send + Sync + 'static> ProcessorTrait<Vec<T>, SyncBoxStream<'static, T>>
    for VecToStreamProcessor<T>
{
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
impl<T: Send + Sync + 'static> EventProcessorTrait<Vec<T>, SyncBoxStream<'static, T>>
    for VecToStreamProcessor<T>
{
    fn pre_status(&self, _input: &Vec<T>) -> Option<EventEnvelope> {
        None
    }
    fn finish_status(
        &self,
        _input: &Vec<T>,
        _output: &SyncBoxStream<'static, T>,
    ) -> Option<EventEnvelope> {
        None
    }
    fn working_status(&self, _input: &Vec<T>) -> Option<EventEnvelope> {
        None
    }
    fn error_status(&self, _input: &Vec<T>, _err: &Error) -> Option<EventEnvelope> {
        None
    }
    fn retry_status(&self, _input: &Vec<T>, _retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        None
    }
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
impl<T: Send + Sync + 'static>
    ProcessorTrait<SyncBoxStream<'static, Vec<T>>, SyncBoxStream<'static, T>>
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
impl<T: Send + Sync + 'static>
    EventProcessorTrait<SyncBoxStream<'static, Vec<T>>, SyncBoxStream<'static, T>>
    for FlattenStreamVecProcessor<T>
{
    fn pre_status(&self, _input: &SyncBoxStream<'static, Vec<T>>) -> Option<EventEnvelope> {
        None
    }
    fn finish_status(
        &self,
        _input: &SyncBoxStream<'static, Vec<T>>,
        _output: &SyncBoxStream<'static, T>,
    ) -> Option<EventEnvelope> {
        None
    }
    fn working_status(&self, _input: &SyncBoxStream<'static, Vec<T>>) -> Option<EventEnvelope> {
        None
    }
    fn error_status(
        &self,
        _input: &SyncBoxStream<'static, Vec<T>>,
        _err: &Error,
    ) -> Option<EventEnvelope> {
        None
    }
    fn retry_status(
        &self,
        _input: &SyncBoxStream<'static, Vec<T>>,
        _retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        None
    }
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
impl<T: Send + Sync + 'static>
    EventProcessorTrait<SyncBoxStream<'static, T>, SyncBoxStream<'static, T>>
    for StreamLoggerProcessor<T>
{
    fn pre_status(&self, _input: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> {
        None
    }
    fn finish_status(
        &self,
        _input: &SyncBoxStream<'static, T>,
        _output: &SyncBoxStream<'static, T>,
    ) -> Option<EventEnvelope> {
        None
    }
    fn working_status(&self, _input: &SyncBoxStream<'static, T>) -> Option<EventEnvelope> {
        None
    }
    fn error_status(
        &self,
        _input: &SyncBoxStream<'static, T>,
        _err: &Error,
    ) -> Option<EventEnvelope> {
        None
    }
    fn retry_status(
        &self,
        _input: &SyncBoxStream<'static, T>,
        _retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        None
    }
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
impl<T: Send + Sync + 'static>
    ProcessorTrait<SyncBoxStream<'static, SyncBoxStream<'static, T>>, SyncBoxStream<'static, T>>
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
impl<T: Send + Sync + 'static>
    EventProcessorTrait<
        SyncBoxStream<'static, SyncBoxStream<'static, T>>,
        SyncBoxStream<'static, T>,
    > for FlattenStreamProcessor<T>
{
    fn pre_status(
        &self,
        _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>,
    ) -> Option<EventEnvelope> {
        None
    }
    fn finish_status(
        &self,
        _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>,
        _output: &SyncBoxStream<'static, T>,
    ) -> Option<EventEnvelope> {
        None
    }
    fn working_status(
        &self,
        _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>,
    ) -> Option<EventEnvelope> {
        None
    }
    fn error_status(
        &self,
        _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>,
        _err: &Error,
    ) -> Option<EventEnvelope> {
        None
    }
    fn retry_status(
        &self,
        _input: &SyncBoxStream<'static, SyncBoxStream<'static, T>>,
        _retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        None
    }
}

pub struct UnifiedTaskIngressChain {
    /// Ingress chain for standard task creation path.
    task_chain: Arc<EventAwareTypedChain<TaskEvent, SyncBoxStream<'static, ()>>>,
    /// Ingress chain for parser-produced follow-up tasks.
    parser_chain: Arc<EventAwareTypedChain<NodeDispatchEnvelope, SyncBoxStream<'static, ()>>>,
    /// Ingress chain for error-task retries/termination path.
    error_chain: Arc<EventAwareTypedChain<NodeErrorEnvelope, SyncBoxStream<'static, ()>>>,
    namespace: String,
    /// Task manager for scheduler-based ingress (Phase 3).
    task_manager: Option<Arc<TaskManager>>,
    /// Queue manager for publishing requests from scheduler path.
    queue_manager: Option<Arc<QueueManager>>,
    /// State for deduplication and config access.
    state: Option<Arc<State>>,
}

struct SchedulerGenerateRuntimeInputRequest<'a> {
    module_id: &'a str,
    profile: Option<&'a crate::common::model::TaskProfileSnapshot>,
    routing: &'a RoutingMeta,
    module_name: &'a str,
    node_id: &'a str,
    metadata: serde_json::Map<String, serde_json::Value>,
    login_info: Option<crate::common::model::login_info::LoginInfo>,
    prefix_request: Option<Uuid>,
}

impl UnifiedTaskIngressChain {
    const FALLBACK_FAILURE_GATE_BLOCKED: &'static str = "failure_gate_blocked";
    const FALLBACK_GRAY_GATE_BLOCKED: &'static str = "gray_gate_blocked";

    /// Constructs a multiplexer over three ingress typed chains.
    pub fn new(
        task_chain: Arc<EventAwareTypedChain<TaskEvent, SyncBoxStream<'static, ()>>>,
        parser_chain: Arc<EventAwareTypedChain<NodeDispatchEnvelope, SyncBoxStream<'static, ()>>>,
        error_chain: Arc<EventAwareTypedChain<NodeErrorEnvelope, SyncBoxStream<'static, ()>>>,
        namespace: String,
    ) -> Self {
        Self {
            task_chain,
            parser_chain,
            error_chain,
            namespace,
            task_manager: None,
            queue_manager: None,
            state: None,
        }
    }

    /// Attaches scheduler ingress bridge dependencies for Phase 3 cutover.
    pub fn with_scheduler_bridge(
        mut self,
        task_manager: Arc<TaskManager>,
        queue_manager: Arc<QueueManager>,
        state: Arc<State>,
    ) -> Self {
        self.task_manager = Some(task_manager);
        self.queue_manager = Some(queue_manager);
        self.state = Some(state);
        self
    }

    pub async fn execute(
        &self,
        input: UnifiedTaskInput,
        context: ProcessorContext,
    ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
        match input {
            UnifiedTaskInput::Task(task) => self.task_chain.execute(task, context).await,
            UnifiedTaskInput::ParserDispatch(dispatch) => {
                if let Some(result) = self.try_scheduler_parser_dispatch(&dispatch).await {
                    return result;
                }
                self.parser_chain.execute(dispatch, context).await
            }
            UnifiedTaskInput::ErrorEnvelope(envelope) => {
                if let Some(result) = self.try_scheduler_error_dispatch(&envelope).await {
                    return result;
                }
                self.error_chain.execute(envelope, context).await
            }
        }
    }

    /// Attempts scheduler-based execution for a parser dispatch envelope.
    ///
    /// Returns `Some(result)` when the scheduler fast-path was taken (success or failure).
    /// Returns `None` when the fast-path is not eligible, signalling fallback to the parser ingress chain.
    async fn try_scheduler_parser_dispatch(
        &self,
        dispatch: &NodeDispatchEnvelope,
    ) -> Option<ProcessorResult<SyncBoxStream<'static, ()>>> {
        let parser_ctx = dispatch.parser_context.as_ref();
        let resolved_node_id = self.resolve_parser_node_id(dispatch, parser_ctx);
        let node_id = match resolved_node_id.as_deref().filter(|s| !s.is_empty()) {
            Some(id) => id,
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "node_id_missing").increment(1);
                self.record_scheduler_ingress_result("parser", "node_id_missing");
                self.record_scheduler_ingress_error("parser", "node_id_missing");
                return Some(self.scheduler_missing_target_result(
                    "parser",
                    "node_id",
                    dispatch.routing.module.as_str(),
                ));
            }
        };

        let module_name = match self.resolve_parser_module_name(dispatch, parser_ctx) {
            Some(m) if !m.is_empty() => m,
            _ => {
                counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "modules_missing").increment(1);
                self.record_scheduler_ingress_result("parser", "modules_missing");
                self.record_scheduler_ingress_error("parser", "modules_missing");
                return Some(self.scheduler_missing_target_result(
                    "parser",
                    "module_name",
                    dispatch.routing.module.as_str(),
                ));
            }
        };

        let (task_manager, queue_manager, _state) = match self.scheduler_bridge_deps() {
            Some(deps) => deps,
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "bridge_missing").increment(1);
                self.record_scheduler_ingress_result("parser", "bridge_missing");
                self.record_scheduler_ingress_error("parser", "bridge_missing");
                return Some(self.scheduler_bridge_missing_result("parser", module_name.as_str()));
            }
        };

        if let Some(reason) = self
            .scheduler_cutover_gate_fallback_reason(
                &task_manager,
                &module_name,
                dispatch.routing.run_id,
            )
            .await
        {
            self.record_scheduler_ingress_fallback("parser", reason, Some(module_name.as_str()));
            task_manager.record_module_dag_cutover_failure(&module_name);
            return Some(ProcessorResult::RetryableFailure(
                RetryPolicy::default()
                    .with_reason(format!("scheduler parser gate blocked: {reason}")),
            ));
        }

        let dag = match task_manager.get_module_dag(&module_name) {
            Some(d) => d,
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "dag_missing").increment(1);
                self.record_scheduler_ingress_result("parser", "dag_missing");
                self.record_scheduler_ingress_error("parser", "dag_missing");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(self.scheduler_dag_missing_result("parser", &module_name));
            }
        };

        // Load full Task to obtain module config and login info.
        let task = match task_manager.load_parser_dispatch(dispatch).await {
            Ok(t) => t,
            Err(err) => {
                counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "load_failed").increment(1);
                self.record_scheduler_ingress_result("parser", "load_failed");
                self.record_scheduler_ingress_error("parser", "load_failed");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(ProcessorResult::RetryableFailure(
                    RetryPolicy::default()
                        .with_reason(format!("scheduler parser load failed: {err}")),
                ));
            }
        };

        let target_module = task.modules.iter().find(|m| {
            m.pending_ctx
                .as_ref()
                .and_then(|ctx| ctx.node_id.as_deref())
                == Some(node_id)
                || m.module.name() == module_name
        });
        let (config, login_info, module_id, profile, dag_dispatcher) = match target_module {
            Some(m) => (
                m.config.clone(),
                m.bound_login_info.clone(),
                m.id(),
                m.profile.clone(),
                m.dag_dispatcher.clone(),
            ),
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "module_not_found").increment(1);
                self.record_scheduler_ingress_result("parser", "module_not_found");
                self.record_scheduler_ingress_error("parser", "module_not_found");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(self.scheduler_task_module_not_found_result(
                    "parser",
                    &module_name,
                    node_id,
                ));
            }
        };

        let metadata = self.resolve_parser_metadata(parser_ctx).unwrap_or_default();
        let prefix_request = self.resolve_parser_prefix_request(dispatch, parser_ctx);

        let runtime_input = match Self::build_scheduler_generate_runtime_input(
            SchedulerGenerateRuntimeInputRequest {
                module_id: &module_id,
                profile: profile.as_deref(),
                routing: &dispatch.routing,
                module_name: &module_name,
                node_id,
                metadata,
                login_info,
                prefix_request,
            },
        ) {
            Ok(runtime_input) => runtime_input,
            Err(err) => {
                counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "runtime_input_invalid").increment(1);
                self.record_scheduler_ingress_result("parser", "runtime_input_invalid");
                self.record_scheduler_ingress_error("parser", "runtime_input_invalid");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(ProcessorResult::RetryableFailure(
                    RetryPolicy::default()
                        .with_reason(format!("scheduler parser runtime input invalid: {err}")),
                ));
            }
        };

        let hints: Vec<RuntimeNodeRoutingHint> = parser_ctx
            .and_then(|ctx| ctx.runtime_node.clone())
            .into_iter()
            .collect();

        if Self::requires_remote_dispatch_hint(&hints) && dag_dispatcher.is_none() {
            counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "no_dispatcher").increment(1);
            self.record_scheduler_ingress_result("parser", "no_dispatcher");
            self.record_scheduler_ingress_error("parser", "no_dispatcher");
            task_manager.record_module_dag_cutover_failure(&module_name);
            return Some(ProcessorResult::RetryableFailure(
                RetryPolicy::default().with_reason(format!(
                    "scheduler parser execution failed: remote node '{}' requires dag_dispatcher but none configured",
                    node_id
                )),
            ));
        }

        let report = match ModuleDagOrchestrator::default()
            .execute_dag_with_generate_runtime_input_and_dispatcher(
                dag,
                node_id,
                &runtime_input,
                hints,
                dag_dispatcher,
            )
            .await
        {
            Ok(r) => r,
            Err(err) => {
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(self.scheduler_dispatch_failed_result("parser", &err));
            }
        };

        let requests = match report.outputs.get(node_id) {
            Some(payload) => match decode_request_batch_payload(payload) {
                Ok(reqs) => reqs,
                Err(err) => {
                    counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "decode_error").increment(1);
                    self.record_scheduler_ingress_result("parser", "decode_error");
                    self.record_scheduler_ingress_error("parser", "decode_error");
                    task_manager.record_module_dag_cutover_failure(&module_name);
                    return Some(ProcessorResult::RetryableFailure(
                        RetryPolicy::default()
                            .with_reason(format!("scheduler parser output decode failed: {err}")),
                    ));
                }
            },
            None => vec![],
        };

        // Publish requests to the download queue.
        let sender = queue_manager.get_request_push_channel();
        for request in &requests {
            let dispatch_envelope = match build_request_dispatch(request, self.namespace.clone()) {
                Ok(d) => d,
                Err(err) => {
                    warn!("[scheduler_ingress] failed to build request dispatch: {err}");
                    continue;
                }
            };
            if let Err(err) = sender.send(QueuedItem::new(dispatch_envelope)).await {
                warn!("[scheduler_ingress] failed to publish request: {err}");
            }
        }

        counter!("mocra_scheduler_ingress_total", "path" => "parser", "result" => "success")
            .increment(1);
        self.record_scheduler_ingress_result("parser", "success");
        task_manager.record_module_dag_cutover_success(&module_name);
        Some(ProcessorResult::Success(
            Box::pin(futures::stream::empty()) as SyncBoxStream<'static, ()>
        ))
    }

    /// Attempts scheduler-based execution for an error dispatch envelope.
    ///
    /// Returns `Some(result)` when the scheduler fast-path was taken.
    /// Returns `None` when the fast-path is not eligible.
    async fn try_scheduler_error_dispatch(
        &self,
        envelope: &NodeErrorEnvelope,
    ) -> Option<ProcessorResult<SyncBoxStream<'static, ()>>> {
        let error_ctx = envelope.error_context.as_ref();
        let resolved_node_id = self.resolve_error_node_id(envelope, error_ctx);
        let node_id = match resolved_node_id.as_deref().filter(|s| !s.is_empty()) {
            Some(id) => id,
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "node_id_missing").increment(1);
                self.record_scheduler_ingress_result("error", "node_id_missing");
                self.record_scheduler_ingress_error("error", "node_id_missing");
                return Some(self.scheduler_missing_target_result(
                    "error",
                    "node_id",
                    envelope.routing.module.as_str(),
                ));
            }
        };

        let module_name = match self.resolve_error_module_name(envelope, error_ctx) {
            Some(m) if !m.is_empty() => m,
            _ => {
                counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "modules_missing").increment(1);
                self.record_scheduler_ingress_result("error", "modules_missing");
                self.record_scheduler_ingress_error("error", "modules_missing");
                return Some(self.scheduler_missing_target_result(
                    "error",
                    "module_name",
                    envelope.routing.module.as_str(),
                ));
            }
        };

        let (task_manager, queue_manager, _state) = match self.scheduler_bridge_deps() {
            Some(deps) => deps,
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "bridge_missing").increment(1);
                self.record_scheduler_ingress_result("error", "bridge_missing");
                self.record_scheduler_ingress_error("error", "bridge_missing");
                return Some(self.scheduler_bridge_missing_result("error", module_name.as_str()));
            }
        };

        if let Some(reason) = self
            .scheduler_cutover_gate_fallback_reason(
                &task_manager,
                &module_name,
                envelope.routing.run_id,
            )
            .await
        {
            self.record_scheduler_ingress_fallback("error", reason, Some(module_name.as_str()));
            task_manager.record_module_dag_cutover_failure(&module_name);
            return Some(ProcessorResult::RetryableFailure(
                RetryPolicy::default()
                    .with_reason(format!("scheduler error gate blocked: {reason}")),
            ));
        }

        let dag = match task_manager.get_module_dag(&module_name) {
            Some(d) => d,
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "dag_missing").increment(1);
                self.record_scheduler_ingress_result("error", "dag_missing");
                self.record_scheduler_ingress_error("error", "dag_missing");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(self.scheduler_dag_missing_result("error", &module_name));
            }
        };

        let task = match task_manager.load_error_envelope(envelope).await {
            Ok(t) => t,
            Err(err) => {
                counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "load_failed").increment(1);
                self.record_scheduler_ingress_result("error", "load_failed");
                self.record_scheduler_ingress_error("error", "load_failed");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(ProcessorResult::RetryableFailure(
                    RetryPolicy::default()
                        .with_reason(format!("scheduler error load failed: {err}")),
                ));
            }
        };

        let target_module = task.modules.iter().find(|m| {
            m.pending_ctx
                .as_ref()
                .and_then(|ctx| ctx.node_id.as_deref())
                == Some(node_id)
                || m.module.name() == module_name
        });
        let (config, login_info, module_id, profile, dag_dispatcher) = match target_module {
            Some(m) => (
                m.config.clone(),
                m.bound_login_info.clone(),
                m.id(),
                m.profile.clone(),
                m.dag_dispatcher.clone(),
            ),
            None => {
                counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "module_not_found").increment(1);
                self.record_scheduler_ingress_result("error", "module_not_found");
                self.record_scheduler_ingress_error("error", "module_not_found");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(self.scheduler_task_module_not_found_result(
                    "error",
                    &module_name,
                    node_id,
                ));
            }
        };

        let metadata = self.resolve_error_metadata(error_ctx).unwrap_or_default();
        let prefix_request = self.resolve_error_prefix_request(envelope, error_ctx);

        let runtime_input = match Self::build_scheduler_generate_runtime_input(
            SchedulerGenerateRuntimeInputRequest {
                module_id: &module_id,
                profile: profile.as_deref(),
                routing: &envelope.routing,
                module_name: &module_name,
                node_id,
                metadata,
                login_info,
                prefix_request,
            },
        ) {
            Ok(runtime_input) => runtime_input,
            Err(err) => {
                counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "runtime_input_invalid").increment(1);
                self.record_scheduler_ingress_result("error", "runtime_input_invalid");
                self.record_scheduler_ingress_error("error", "runtime_input_invalid");
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(ProcessorResult::RetryableFailure(
                    RetryPolicy::default()
                        .with_reason(format!("scheduler error runtime input invalid: {err}")),
                ));
            }
        };

        let hints: Vec<RuntimeNodeRoutingHint> = error_ctx
            .and_then(|ctx| ctx.runtime_node.clone())
            .into_iter()
            .collect();

        if Self::requires_remote_dispatch_hint(&hints) && dag_dispatcher.is_none() {
            counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "no_dispatcher").increment(1);
            self.record_scheduler_ingress_result("error", "no_dispatcher");
            self.record_scheduler_ingress_error("error", "no_dispatcher");
            task_manager.record_module_dag_cutover_failure(&module_name);
            return Some(ProcessorResult::RetryableFailure(
                RetryPolicy::default().with_reason(format!(
                    "scheduler error execution failed: remote node '{}' requires dag_dispatcher but none configured",
                    node_id
                )),
            ));
        }

        let report = match ModuleDagOrchestrator::default()
            .execute_dag_with_generate_runtime_input_and_dispatcher(
                dag,
                node_id,
                &runtime_input,
                hints,
                dag_dispatcher,
            )
            .await
        {
            Ok(r) => r,
            Err(err) => {
                task_manager.record_module_dag_cutover_failure(&module_name);
                return Some(self.scheduler_dispatch_failed_result("error", &err));
            }
        };

        let requests = match report.outputs.get(node_id) {
            Some(payload) => match decode_request_batch_payload(payload) {
                Ok(reqs) => reqs,
                Err(err) => {
                    counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "decode_error").increment(1);
                    self.record_scheduler_ingress_result("error", "decode_error");
                    self.record_scheduler_ingress_error("error", "decode_error");
                    task_manager.record_module_dag_cutover_failure(&module_name);
                    return Some(ProcessorResult::RetryableFailure(
                        RetryPolicy::default()
                            .with_reason(format!("scheduler error output decode failed: {err}")),
                    ));
                }
            },
            None => vec![],
        };

        let sender = queue_manager.get_request_push_channel();
        for request in &requests {
            let dispatch_envelope = match build_request_dispatch(request, self.namespace.clone()) {
                Ok(d) => d,
                Err(err) => {
                    warn!("[scheduler_ingress] failed to build request dispatch: {err}");
                    continue;
                }
            };
            if let Err(err) = sender.send(QueuedItem::new(dispatch_envelope)).await {
                warn!("[scheduler_ingress] failed to publish request: {err}");
            }
        }

        counter!("mocra_scheduler_ingress_total", "path" => "error", "result" => "success")
            .increment(1);
        self.record_scheduler_ingress_result("error", "success");
        task_manager.record_module_dag_cutover_success(&module_name);
        Some(ProcessorResult::Success(
            Box::pin(futures::stream::empty()) as SyncBoxStream<'static, ()>
        ))
    }

    /// Returns scheduler bridge dependencies if all are present.
    fn scheduler_bridge_deps(&self) -> Option<(Arc<TaskManager>, Arc<QueueManager>, Arc<State>)> {
        Some((
            self.task_manager.clone()?,
            self.queue_manager.clone()?,
            self.state.clone()?,
        ))
    }

    fn requires_remote_dispatch_hint(hints: &[RuntimeNodeRoutingHint]) -> bool {
        hints
            .iter()
            .any(|hint| matches!(hint.placement, Some(NodePlacement::Remote { .. })))
    }

    fn scheduler_dispatch_failed_result(
        &self,
        path: &'static str,
        err: &DagError,
    ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
        counter!("mocra_scheduler_ingress_total", "path" => path, "result" => "scheduler_error")
            .increment(1);
        self.record_scheduler_ingress_result(path, "scheduler_error");
        self.record_scheduler_ingress_error(path, "scheduler_error");
        ProcessorResult::RetryableFailure(
            RetryPolicy::default().with_reason(format!("scheduler {path} execution failed: {err}")),
        )
    }

    fn scheduler_task_module_not_found_result(
        &self,
        path: &'static str,
        module_name: &str,
        node_id: &str,
    ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
        ProcessorResult::RetryableFailure(
            RetryPolicy::default().with_reason(format!(
                "scheduler {path} execution failed: reconstructed task does not contain module '{module_name}' for node '{node_id}'"
            )),
        )
    }

    fn scheduler_dag_missing_result(
        &self,
        path: &'static str,
        module_name: &str,
    ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
        ProcessorResult::RetryableFailure(RetryPolicy::default().with_reason(format!(
            "scheduler {path} execution failed: precompiled DAG missing for module '{module_name}'"
        )))
    }

    fn scheduler_bridge_missing_result(
        &self,
        path: &'static str,
        module_name: &str,
    ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
        ProcessorResult::RetryableFailure(
            RetryPolicy::default().with_reason(format!(
                "scheduler {path} execution failed: scheduler bridge not attached for module '{module_name}'"
            )),
        )
    }

    fn scheduler_missing_target_result(
        &self,
        path: &'static str,
        field: &'static str,
        module_name: &str,
    ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
        ProcessorResult::RetryableFailure(
            RetryPolicy::default().with_reason(format!(
                "scheduler {path} execution failed: required {field} is missing from parser/error transport for module '{module_name}'"
            )),
        )
    }

    /// Resolution order is strictly typed context -> transport envelope.
    fn resolve_parser_node_id(
        &self,
        dispatch: &NodeDispatchEnvelope,
        parser_ctx: Option<&ParserDispatchContext>,
    ) -> Option<String> {
        parser_ctx
            .and_then(|ctx| ctx.context.node_id.clone())
            .or_else(|| {
                (!dispatch.dispatch.target_node.is_empty())
                    .then(|| dispatch.dispatch.target_node.clone())
            })
            .or_else(|| {
                (!dispatch.dispatch.input.target_node.is_empty())
                    .then(|| dispatch.dispatch.input.target_node.clone())
            })
    }

    /// Resolution order is strictly typed context -> transport envelope.
    fn resolve_parser_module_name(
        &self,
        dispatch: &NodeDispatchEnvelope,
        parser_ctx: Option<&ParserDispatchContext>,
    ) -> Option<String> {
        parser_ctx
            .and_then(|ctx| ctx.modules.as_ref().and_then(|mods| mods.first().cloned()))
            .or_else(|| {
                (!dispatch.routing.module.is_empty()).then(|| dispatch.routing.module.clone())
            })
    }

    /// Metadata stays on the typed context path when present.
    fn resolve_parser_metadata(
        &self,
        parser_ctx: Option<&ParserDispatchContext>,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        parser_ctx.map(|ctx| ctx.metadata.clone())
    }

    /// Prefix request follows the typed context first, then transport parent pointer.
    fn resolve_parser_prefix_request(
        &self,
        dispatch: &NodeDispatchEnvelope,
        parser_ctx: Option<&ParserDispatchContext>,
    ) -> Option<Uuid> {
        parser_ctx
            .and_then(|ctx| ctx.prefix_request_id)
            .or(dispatch.routing.parent_request_id)
    }

    /// Resolution order is strictly typed context -> transport envelope.
    fn resolve_error_node_id(
        &self,
        envelope: &NodeErrorEnvelope,
        error_ctx: Option<&ErrorDispatchContext>,
    ) -> Option<String> {
        error_ctx
            .and_then(|ctx| ctx.context.node_id.clone())
            .or_else(|| {
                (!envelope.routing.node_key.is_empty()).then(|| envelope.routing.node_key.clone())
            })
    }

    /// Resolution order is strictly typed context -> transport envelope.
    fn resolve_error_module_name(
        &self,
        envelope: &NodeErrorEnvelope,
        error_ctx: Option<&ErrorDispatchContext>,
    ) -> Option<String> {
        error_ctx
            .and_then(|ctx| ctx.modules.as_ref().and_then(|mods| mods.first().cloned()))
            .or_else(|| {
                (!envelope.routing.module.is_empty()).then(|| envelope.routing.module.clone())
            })
    }

    /// Metadata stays on the typed context path when present.
    fn resolve_error_metadata(
        &self,
        error_ctx: Option<&ErrorDispatchContext>,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        error_ctx.map(|ctx| ctx.metadata.clone())
    }

    /// Prefix request follows the typed context first, then transport parent pointer.
    fn resolve_error_prefix_request(
        &self,
        envelope: &NodeErrorEnvelope,
        error_ctx: Option<&ErrorDispatchContext>,
    ) -> Option<Uuid> {
        error_ctx
            .and_then(|ctx| ctx.prefix_request_id)
            .or(envelope.routing.parent_request_id)
    }

    fn build_scheduler_generate_runtime_input(
        request: SchedulerGenerateRuntimeInputRequest<'_>,
    ) -> Result<SchedulerNodeGenerateRuntimeInput> {
        let _ = request.module_id;
        let profile = request.profile.ok_or_else(|| {
            ModuleError::Model(
                std::io::Error::other(format!(
                    "scheduler ingress requires loaded profile for node '{}' in module '{}'",
                    request.node_id, request.module_name
                ))
                .into(),
            )
        })?;
        let Some(resolved_config) = profile.resolve_node_config(request.node_id) else {
            return Err(ModuleError::Model(
                std::io::Error::other(format!(
                    "profile node config missing for scheduler ingress node '{}'",
                    request.node_id
                ))
                .into(),
            )
            .into());
        };
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Ok(SchedulerNodeGenerateRuntimeInput {
            routing: RoutingMeta {
                namespace: request.routing.namespace.clone(),
                account: request.routing.account.clone(),
                platform: request.routing.platform.clone(),
                module: request.module_name.to_string(),
                node_key: request.node_id.to_string(),
                run_id: request.routing.run_id,
                request_id: Uuid::now_v7(),
                parent_request_id: request.prefix_request,
                priority: request.routing.priority,
            },
            exec: ExecutionMeta {
                profile_version: resolved_config.profile_version,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                ..ExecutionMeta::default()
            },
            config: resolved_config,
            input: NodeInput::new(
                request.node_id,
                TypedEnvelope::new(
                    "mocra.node_input.v1",
                    1,
                    PayloadCodec::Json,
                    serde_json::to_vec(&serde_json::Value::Object(request.metadata))
                        .unwrap_or_default(),
                ),
            ),
            login_info: request.login_info,
        })
    }

    fn record_scheduler_ingress_fallback(
        &self,
        path: &'static str,
        reason: &'static str,
        module: Option<&str>,
    ) {
        let module = module
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("unknown")
            .to_string();
        counter!(
            "mocra_scheduler_ingress_total",
            "path" => path,
            "result" => "fallback",
            "reason" => reason,
            "module" => module
        )
        .increment(1);
        self.record_scheduler_ingress_result(path, &format!("fallback_{reason}"));
        if Self::scheduler_ingress_fallback_counts_as_error(reason) {
            self.record_scheduler_ingress_error(path, reason);
        }
    }

    fn scheduler_ingress_fallback_counts_as_error(reason: &'static str) -> bool {
        !matches!(
            reason,
            Self::FALLBACK_GRAY_GATE_BLOCKED | Self::FALLBACK_FAILURE_GATE_BLOCKED
        )
    }

    fn record_scheduler_ingress_result(&self, path: &'static str, result: &str) {
        metrics_facade::inc_throughput("engine", "scheduler_ingress", path, result, 1);
    }

    fn record_scheduler_ingress_error(&self, path: &'static str, code: &str) {
        metrics_facade::inc_error("engine", "scheduler_ingress", path, code, 1);
    }

    async fn scheduler_cutover_gate_fallback_reason(
        &self,
        task_manager: &TaskManager,
        module_name: &str,
        run_id: Uuid,
    ) -> Option<&'static str> {
        let gate = self.scheduler_cutover_gate_config().await;
        if !Self::allow_scheduler_cutover_by_gray_ratio(run_id, gate.gray_ratio) {
            return Some(Self::FALLBACK_GRAY_GATE_BLOCKED);
        }

        if !task_manager.should_allow_module_dag_cutover(
            module_name,
            gate.failure_threshold,
            gate.recovery_window_secs,
        ) {
            return Some(Self::FALLBACK_FAILURE_GATE_BLOCKED);
        }

        None
    }

    async fn scheduler_cutover_gate_config(
        &self,
    ) -> crate::common::model::config::SchedulerIngressCutoverGateConfigResolved {
        match self.state.as_ref() {
            Some(state) => {
                let config = state.config.read().await;
                config.crawler.scheduler_ingress_cutover_gate_config()
            }
            None => {
                crate::common::model::config::SchedulerIngressCutoverGateConfigResolved::default()
            }
        }
    }

    fn allow_scheduler_cutover_by_gray_ratio(run_id: Uuid, gray_ratio: f64) -> bool {
        if gray_ratio <= 0.0 {
            return false;
        }
        if gray_ratio >= 1.0 {
            return true;
        }

        let slot = run_id.as_u128() % 10_000;
        let allowed = (gray_ratio * 10_000.0).floor() as u128;
        slot < allowed
    }
}

/// Builds the unified ingress chain graph used by task workers.
///
/// The resulting chain set handles three inputs:
/// - `TaskModel` (initial tasks),
/// - parser-dispatch envelopes (parser-produced tasks),
/// - `ErrorTaskModel` (retry/threshold-controlled tasks).
pub async fn create_unified_task_ingress_chain(
    task_manager: Arc<TaskManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    state: Arc<State>,
) -> UnifiedTaskIngressChain {
    let ingress_namespace = queue_manager.namespace.clone();
    let task_concurrency = state
        .config
        .read()
        .await
        .crawler
        .task_concurrency
        .unwrap_or(1024);
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
    let task_publish_concurrency = state
        .config
        .read()
        .await
        .crawler
        .publish_concurrency
        .unwrap_or(1024);
    let parser_publish_concurrency = state
        .config
        .read()
        .await
        .crawler
        .publish_concurrency
        .unwrap_or(256);
    let error_publish_concurrency = state
        .config
        .read()
        .await
        .crawler
        .publish_concurrency
        .unwrap_or(32);

    let task_chain = Arc::new(
        EventAwareTypedChain::<TaskEvent, TaskEvent>::new(event_bus.clone())
            .then::<Task, _>(TaskModelProcessor {
                task_manager: task_manager.clone(),
                state: state.clone(),
                queue_manager: queue_manager.clone(),
                event_bus: event_bus.clone(),
                threshold_decision_service: Arc::new(StatusTrackerThresholdDecisionService::new(
                    state.clone(),
                )),
            })
            .then::<Vec<Module>, _>(TaskModuleProcessor {
                state: state.clone(),
            })
            .then(VecToStreamProcessor::new())
            .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
                TaskProcessor {
                    task_manager: task_manager.clone(),
                },
                task_concurrency,
                ErrorStrategy::Skip,
            )
            .then_one_shot(FlattenStreamProcessor::new())
            .then_one_shot(StreamLoggerProcessor::new("AfterFlatten").with_logger(
                |req: &Request| {
                    info!(
                        "[FlattenStreamProcessor] yielding request: request_id={}",
                        req.id
                    );
                },
            ))
            .then_map_stream_in_with_strategy::<(), _>(
                RequestPublish {
                    queue_manager: queue_manager.clone(),
                    state: state.clone(),
                },
                task_publish_concurrency,
                ErrorStrategy::Skip,
            ),
    );

    let parser_chain = Arc::new(
        EventAwareTypedChain::<NodeDispatchEnvelope, NodeDispatchEnvelope>::new(event_bus.clone())
            .then::<Task, _>(TaskModelProcessor {
                task_manager: task_manager.clone(),
                state: state.clone(),
                queue_manager: queue_manager.clone(),
                event_bus: event_bus.clone(),
                threshold_decision_service: Arc::new(StatusTrackerThresholdDecisionService::new(
                    state.clone(),
                )),
            })
            .then::<Vec<Module>, _>(TaskModuleProcessor {
                state: state.clone(),
            })
            .then(VecToStreamProcessor::new())
            .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
                TaskProcessor {
                    task_manager: task_manager.clone(),
                },
                parser_concurrency,
                ErrorStrategy::Skip,
            )
            .then_one_shot(FlattenStreamProcessor::new())
            .then_map_stream_in_with_strategy::<(), _>(
                RequestPublish {
                    queue_manager: queue_manager.clone(),
                    state: state.clone(),
                },
                parser_publish_concurrency,
                ErrorStrategy::Skip,
            ),
    );

    let scheduler_task_manager = task_manager.clone();
    let scheduler_queue_manager = queue_manager.clone();
    let scheduler_state = state.clone();

    let error_chain = Arc::new(
        EventAwareTypedChain::<NodeErrorEnvelope, NodeErrorEnvelope>::new(event_bus)
            .then::<Task, _>(TaskModelProcessor {
                task_manager: task_manager.clone(),
                state: state.clone(),
                queue_manager: queue_manager.clone(),
                event_bus: None,
                threshold_decision_service: Arc::new(StatusTrackerThresholdDecisionService::new(
                    state.clone(),
                )),
            })
            .then::<Vec<Module>, _>(TaskModuleProcessor {
                state: state.clone(),
            })
            .then(VecToStreamProcessor::new())
            .then_map_stream_in_with_strategy::<SyncBoxStream<'static, Request>, _>(
                TaskProcessor {
                    task_manager: task_manager.clone(),
                },
                error_task_concurrency,
                ErrorStrategy::Skip,
            )
            .then_one_shot(FlattenStreamProcessor::new())
            .then_map_stream_in_with_strategy::<(), _>(
                RequestPublish {
                    queue_manager,
                    state,
                },
                error_publish_concurrency,
                ErrorStrategy::Skip,
            ),
    );

    UnifiedTaskIngressChain::new(task_chain, parser_chain, error_chain, ingress_namespace)
        .with_scheduler_bridge(
            scheduler_task_manager,
            scheduler_queue_manager,
            scheduler_state,
        )
}

#[cfg(test)]
mod scheduler_ingress_tests {
    use std::collections::BTreeMap;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::cacheable::{CacheService, CacheServiceConfig};
    use crate::common::interface::{
        ModuleNodeTrait, ModuleTrait, NodeGenerateContext, NodeParseContext, ToSyncBoxStream,
    };
    use crate::common::model::config::{
        BlobStorageConfig, CacheConfig, ChannelConfig, Config, CrawlerConfig, DatabaseConfig,
        DownloadConfig, SchedulerIngressCutoverGateConfig,
    };
    use crate::common::model::message::{TaskEvent, UnifiedTaskInput};
    use crate::common::model::{
        ErrorDispatchContext, ExecutionMark, ExecutionMeta, NodeDispatch, NodeDispatchEnvelope,
        NodeErrorEnvelope, NodeInput, NodeParseOutput, ParserDispatchContext, PayloadCodec,
        PipelineStage, Request, Response, RoutingMeta, TaskProfileSnapshot, TypedEnvelope,
    };
    use crate::common::state::State as AppState;
    use crate::common::status_tracker::{ErrorTrackerConfig, StatusTracker};
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use crate::engine::task::request_response_adapter::decode_request_dispatch;
    use crate::engine::task::task_manager::TaskManager;
    use crate::queue::QueueManager;
    use sea_orm::{ConnectionTrait, DbBackend, Statement};
    use tokio::sync::RwLock;

    fn make_routing(module: &str) -> RoutingMeta {
        RoutingMeta {
            namespace: "test".into(),
            account: "acc".into(),
            platform: "plt".into(),
            module: module.into(),
            node_key: "step_0".into(),
            run_id: Uuid::now_v7(),
            request_id: Uuid::now_v7(),
            parent_request_id: None,
            priority: Default::default(),
        }
    }

    fn make_dispatch_envelope(module: &str, node_id: Option<&str>) -> NodeDispatchEnvelope {
        let routing = make_routing(module);
        let payload = TypedEnvelope::new(
            "transport.parser_dispatch",
            1,
            PayloadCodec::Json,
            b"{}".to_vec(),
        );
        let mut envelope = NodeDispatchEnvelope::new(
            routing,
            ExecutionMeta::default(),
            NodeDispatch::new("step_0", NodeInput::new("step_0", payload)),
        );
        if let Some(nid) = node_id {
            envelope = envelope.with_parser_context(ParserDispatchContext {
                modules: Some(vec![module.to_string()]),
                metadata: Default::default(),
                context: ExecutionMark {
                    module_id: Some(format!("acc-plt-{}", module)),
                    node_id: Some(nid.to_string()),
                    ..Default::default()
                },
                prefix_request_id: None,
                runtime_node: None,
            });
        }
        envelope
    }

    fn make_error_envelope(module: &str, node_id: Option<&str>) -> NodeErrorEnvelope {
        let routing = make_routing(module);
        let mut envelope = NodeErrorEnvelope::new(
            routing,
            ExecutionMeta::default(),
            PipelineStage::ParserTask,
            "test error",
        );
        if let Some(nid) = node_id {
            envelope = envelope.with_error_context(ErrorDispatchContext {
                modules: Some(vec![module.to_string()]),
                metadata: Default::default(),
                context: ExecutionMark {
                    module_id: Some(format!("acc-plt-{}", module)),
                    node_id: Some(nid.to_string()),
                    ..Default::default()
                },
                prefix_request_id: None,
                runtime_node: None,
            });
        }
        envelope
    }

    fn make_profile(module: &str, node_id: &str, version: u64) -> TaskProfileSnapshot {
        TaskProfileSnapshot {
            namespace: "test".into(),
            account: "acc".into(),
            platform: "plt".into(),
            module: module.into(),
            version,
            node_configs: BTreeMap::from([(
                node_id.to_string(),
                TypedEnvelope::new("typed.node_config", 1, PayloadCodec::Json, b"{}".to_vec()),
            )]),
            ..TaskProfileSnapshot::default()
        }
    }

    /// Constructs a chain without scheduler bridge by building a minimal but
    /// type-correct ingress chain using `UnifiedTaskIngressChain::new()`.
    fn make_ingress_chain_no_bridge() -> UnifiedTaskIngressChain {
        // Build type-correct dummy chains via `.then()` — the processors will
        // never execute, so returning FatalFailure is fine.
        let task_chain = Arc::new(
            EventAwareTypedChain::<TaskEvent, TaskEvent>::new(None)
                .then::<SyncBoxStream<'static, ()>, _>(StubTerminalProcessor),
        );
        let parser_chain = Arc::new(
            EventAwareTypedChain::<NodeDispatchEnvelope, NodeDispatchEnvelope>::new(None)
                .then::<SyncBoxStream<'static, ()>, _>(StubTerminalProcessor),
        );
        let error_chain = Arc::new(
            EventAwareTypedChain::<NodeErrorEnvelope, NodeErrorEnvelope>::new(None)
                .then::<SyncBoxStream<'static, ()>, _>(StubTerminalProcessor),
        );
        UnifiedTaskIngressChain::new(task_chain, parser_chain, error_chain, "test".into())
    }

    fn make_ingress_chain_with_recorders(
        parser_seen: Arc<Mutex<Vec<NodeDispatchEnvelope>>>,
        error_seen: Arc<Mutex<Vec<NodeErrorEnvelope>>>,
    ) -> UnifiedTaskIngressChain {
        let task_chain = Arc::new(
            EventAwareTypedChain::<TaskEvent, TaskEvent>::new(None)
                .then::<SyncBoxStream<'static, ()>, _>(StubTerminalProcessor),
        );
        let parser_chain = Arc::new(
            EventAwareTypedChain::<NodeDispatchEnvelope, NodeDispatchEnvelope>::new(None)
                .then::<SyncBoxStream<'static, ()>, _>(RecordingParserTerminalProcessor {
                seen: parser_seen,
            }),
        );
        let error_chain = Arc::new(
            EventAwareTypedChain::<NodeErrorEnvelope, NodeErrorEnvelope>::new(None)
                .then::<SyncBoxStream<'static, ()>, _>(RecordingErrorTerminalProcessor {
                    seen: error_seen,
                }),
        );
        UnifiedTaskIngressChain::new(task_chain, parser_chain, error_chain, "test".into())
    }

    fn scheduler_test_config(
        scheduler_ingress_cutover_gate: Option<SchedulerIngressCutoverGateConfig>,
    ) -> Config {
        Config {
            name: "test".to_string(),
            db: DatabaseConfig {
                url: None,
                database_schema: None,
                pool_size: None,
                tls: None,
            },
            download_config: DownloadConfig {
                downloader_expire: 60,
                timeout: 30,
                rate_limit: 5.0,
                enable_session: true,
                enable_locker: false,
                enable_rate_limit: true,
                cache_ttl: 60,
                wss_timeout: 30,
                pool_size: None,
                max_response_size: None,
            },
            cache: CacheConfig {
                backend: None,
                ttl: 60,
                compression_threshold: None,
                enable_l1: Some(false),
                l1_ttl_secs: None,
                l1_max_entries: None,
            },
            crawler: CrawlerConfig {
                request_max_retries: 3,
                task_max_errors: 5,
                module_max_errors: 5,
                module_locker_ttl: 60,
                node_id: None,
                task_concurrency: None,
                publish_concurrency: None,
                parser_concurrency: None,
                error_task_concurrency: None,
                backpressure_retry_delay_ms: None,
                dedup_ttl_secs: None,
                idle_stop_secs: None,
                scheduler_ingress_cutover_gate,
            },
            scheduler: None,
            sync: None,
            channel_config: ChannelConfig {
                blob_storage: Some(BlobStorageConfig { path: None }),
                kafka: None,
                minid_time: 0,
                capacity: 16,
                queue_codec: None,
                batch_concurrency: None,
                compression_threshold: None,
                nack_max_retries: None,
                nack_backoff_ms: None,
                federation_request_namespaces: Vec::new(),
                federation_response_cache_api_endpoints: Default::default(),
            },
            proxy: None,
            api: None,
            event_bus: None,
            logger: None,
            policy: None,
            raft: None,
        }
    }

    async fn build_scheduler_test_state_with_cache_service(
        cache_service: Arc<CacheService>,
        scheduler_ingress_cutover_gate: Option<SchedulerIngressCutoverGateConfig>,
    ) -> Arc<AppState> {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite should connect");
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("scheduler-ingress").unwrap());
        Arc::new(AppState {
            db: Arc::new(db),
            config: Arc::new(RwLock::new(scheduler_test_config(
                scheduler_ingress_cutover_gate,
            ))),
            cache_service,
            cookie_service: None,
            locker: Arc::new(crate::utils::distributed_lock::DistributedLockManager::new(
                None, "test",
            )),
            limiter: Arc::new(
                crate::utils::distributed_rate_limit::DistributedSlidingWindowRateLimiter::new(
                    None,
                    Arc::new(crate::utils::distributed_lock::DistributedLockManager::new(
                        None, "test",
                    )),
                    "test",
                    crate::utils::distributed_rate_limit::RateLimitConfig {
                        max_requests_per_second: 5.0,
                        window_size_millis: 1000,
                        base_max_requests_per_second: Some(5.0),
                    },
                ),
            ),
            api_limiter: None,
            status_tracker: Arc::new(StatusTracker::new(
                ErrorTrackerConfig {
                    task_max_errors: 5,
                    module_max_errors: 5,
                    request_max_retries: 3,
                    parse_max_retries: 3,
                    enable_success_decay: true,
                    success_decay_amount: 1,
                    enable_time_window: false,
                    time_window_seconds: 3600,
                    consecutive_error_threshold: 3,
                    error_ttl: 60,
                },
                profile_store.clone(),
            )),
            profile_store,
            raft_runtime_config: None,
            raft_runtime: None,
        })
    }

    async fn build_scheduler_test_state(
        scheduler_ingress_cutover_gate: Option<SchedulerIngressCutoverGateConfig>,
    ) -> Arc<AppState> {
        build_scheduler_test_state_with_cache_service(
            Arc::new(CacheService::new(
                CacheServiceConfig::local("test:cache")
                    .with_default_ttl(Some(Duration::from_secs(60))),
            )),
            scheduler_ingress_cutover_gate,
        )
        .await
    }

    async fn make_ingress_chain_with_scheduler_bridge_and_recorders(
        parser_seen: Arc<Mutex<Vec<NodeDispatchEnvelope>>>,
        error_seen: Arc<Mutex<Vec<NodeErrorEnvelope>>>,
        scheduler_ingress_cutover_gate: Option<SchedulerIngressCutoverGateConfig>,
    ) -> (UnifiedTaskIngressChain, Arc<TaskManager>) {
        let chain = make_ingress_chain_with_recorders(parser_seen, error_seen);
        let state = build_scheduler_test_state(scheduler_ingress_cutover_gate).await;
        let task_manager = Arc::new(TaskManager::new(state.clone()));
        let queue_manager = Arc::new(QueueManager::new(None, 16));
        (
            chain.with_scheduler_bridge(task_manager.clone(), queue_manager, state),
            task_manager,
        )
    }

    struct FastPathSuccessNode;

    #[async_trait]
    impl ModuleNodeTrait for FastPathSuccessNode {
        async fn generate(
            &self,
            _ctx: NodeGenerateContext<'_>,
        ) -> crate::errors::Result<SyncBoxStream<'static, Request>> {
            Ok(vec![Request::new("https://example.com/fast-path", "GET")].to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _ctx: NodeParseContext<'_>,
        ) -> crate::errors::Result<NodeParseOutput> {
            Ok(NodeParseOutput::default())
        }

        fn stable_node_key(&self) -> &'static str {
            "step_0"
        }
    }

    struct FastPathSuccessModule;

    #[async_trait]
    impl ModuleTrait for FastPathSuccessModule {
        fn name(&self) -> &'static str {
            "fast_path_mod"
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
            vec![Arc::new(FastPathSuccessNode)]
        }
    }

    async fn seed_scheduler_task_factory_state(
        state: &AppState,
        module_name: &str,
        account: &str,
        platform: &str,
    ) {
        let attach_result = state
            .db
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "ATTACH DATABASE ':memory:' AS base".to_string(),
            ))
            .await;
        if let Err(err) = attach_result {
            let msg = err.to_string();
            assert!(
                msg.contains("database base is already in use"),
                "unexpected sqlite attach error: {msg}"
            );
        }

        let statements = [
            "CREATE TABLE IF NOT EXISTS base.module (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', priority INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, version INTEGER NOT NULL DEFAULT 1)",
            "CREATE TABLE IF NOT EXISTS base.data_middleware (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS base.rel_module_data_middleware (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, data_middleware_id INTEGER NOT NULL REFERENCES data_middleware(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, data_middleware_id))",
            "CREATE TABLE IF NOT EXISTS base.download_middleware (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS base.rel_module_download_middleware (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, download_middleware_id INTEGER NOT NULL REFERENCES download_middleware(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, download_middleware_id))",
            "CREATE TABLE IF NOT EXISTS base.account (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, modules TEXT NOT NULL DEFAULT '{}', enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', priority INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS base.platform (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, description TEXT, base_url VARCHAR(255), enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
            "CREATE TABLE IF NOT EXISTS base.rel_account_platform (account_id INTEGER NOT NULL REFERENCES account(id) ON DELETE CASCADE, platform_id INTEGER NOT NULL REFERENCES platform(id) ON DELETE CASCADE, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (account_id, platform_id))",
            "CREATE TABLE IF NOT EXISTS base.rel_module_account (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, account_id INTEGER NOT NULL REFERENCES account(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, account_id))",
            "CREATE TABLE IF NOT EXISTS base.rel_module_platform (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, platform_id INTEGER NOT NULL REFERENCES platform(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, platform_id))",
            &format!(
                "INSERT OR IGNORE INTO base.module (name, version, priority, enabled) VALUES ('{module_name}', 1, 5, 1)"
            ),
            &format!(
                "INSERT OR IGNORE INTO base.account (name, enabled, priority) VALUES ('{account}', 1, 5)"
            ),
            &format!(
                "INSERT OR IGNORE INTO base.platform (name, description, enabled) VALUES ('{platform}', 'scheduler ingress test', 1)"
            ),
            &format!(
                "INSERT OR IGNORE INTO base.rel_module_account (module_id, account_id, priority, enabled) SELECT m.id, a.id, 5, 1 FROM base.module m, base.account a WHERE m.name = '{module_name}' AND a.name = '{account}'"
            ),
            &format!(
                "INSERT OR IGNORE INTO base.rel_module_platform (module_id, platform_id, priority, enabled) SELECT m.id, p.id, 5, 1 FROM base.module m, base.platform p WHERE m.name = '{module_name}' AND p.name = '{platform}'"
            ),
            &format!(
                "INSERT OR IGNORE INTO base.rel_account_platform (account_id, platform_id, enabled) SELECT a.id, p.id, 1 FROM base.account a, base.platform p WHERE a.name = '{account}' AND p.name = '{platform}'"
            ),
        ];

        for sql in statements {
            state
                .db
                .execute(Statement::from_string(DbBackend::Sqlite, sql.to_string()))
                .await
                .unwrap_or_else(|err| {
                    panic!("failed to seed scheduler ingress sqlite state: {err}; sql={sql}")
                });
        }
    }

    async fn make_seeded_scheduler_bridge_chain(
        parser_seen: Arc<Mutex<Vec<NodeDispatchEnvelope>>>,
        error_seen: Arc<Mutex<Vec<NodeErrorEnvelope>>>,
    ) -> (UnifiedTaskIngressChain, Arc<TaskManager>, Arc<QueueManager>) {
        let chain = make_ingress_chain_with_recorders(parser_seen, error_seen);
        let state = build_scheduler_test_state(Some(SchedulerIngressCutoverGateConfig {
            failure_threshold: Some(3),
            recovery_window_secs: Some(60),
            gray_ratio: Some(1.0),
        }))
        .await;
        seed_scheduler_task_factory_state(&state, "fast_path_mod", "acc", "plt").await;
        let task_manager = Arc::new(TaskManager::new(state.clone()));
        task_manager
            .add_module(Arc::new(FastPathSuccessModule))
            .await;
        let queue_manager = Arc::new(QueueManager::new(None, 16));
        (
            chain.with_scheduler_bridge(task_manager.clone(), queue_manager.clone(), state),
            task_manager,
            queue_manager,
        )
    }

    async fn make_seeded_scheduler_bridge_chain_with_cache_service(
        parser_seen: Arc<Mutex<Vec<NodeDispatchEnvelope>>>,
        error_seen: Arc<Mutex<Vec<NodeErrorEnvelope>>>,
        cache_service: Arc<CacheService>,
    ) -> (UnifiedTaskIngressChain, Arc<TaskManager>, Arc<QueueManager>) {
        let chain = make_ingress_chain_with_recorders(parser_seen, error_seen);
        let state = build_scheduler_test_state_with_cache_service(
            cache_service,
            Some(SchedulerIngressCutoverGateConfig {
                failure_threshold: Some(3),
                recovery_window_secs: Some(60),
                gray_ratio: Some(1.0),
            }),
        )
        .await;
        seed_scheduler_task_factory_state(&state, "fast_path_mod", "acc", "plt").await;
        let task_manager = Arc::new(TaskManager::new(state.clone()));
        task_manager
            .add_module(Arc::new(FastPathSuccessModule))
            .await;
        let queue_manager = Arc::new(QueueManager::new(None, 16));
        (
            chain.with_scheduler_bridge(task_manager.clone(), queue_manager.clone(), state),
            task_manager,
            queue_manager,
        )
    }

    #[test]
    fn scheduler_generate_runtime_input_prefers_profile_node_config() {
        let routing = make_routing("catalog");
        let profile = make_profile("catalog", "step_0", 17);
        let runtime_input = UnifiedTaskIngressChain::build_scheduler_generate_runtime_input(
            SchedulerGenerateRuntimeInputRequest {
                module_id: "acc-plt-catalog",
                profile: Some(&profile),
                routing: &routing,
                module_name: "catalog",
                node_id: "step_0",
                metadata: Default::default(),
                login_info: None,
                prefix_request: None,
            },
        )
        .expect("typed scheduler runtime input should build");

        assert_eq!(runtime_input.routing.namespace, "test");
        assert_eq!(runtime_input.routing.module, "catalog");
        assert_eq!(runtime_input.routing.node_key, "step_0");
        assert_eq!(runtime_input.config.profile_version, 17);
        assert_eq!(
            runtime_input.config.node_config.schema_id,
            "typed.node_config"
        );
        assert_eq!(runtime_input.exec.profile_version, 17);
    }

    #[test]
    fn scheduler_generate_runtime_input_errors_when_profile_missing() {
        let routing = make_routing("catalog");
        let err = UnifiedTaskIngressChain::build_scheduler_generate_runtime_input(
            SchedulerGenerateRuntimeInputRequest {
                module_id: "acc-plt-catalog",
                profile: None,
                routing: &routing,
                module_name: "catalog",
                node_id: "step_0",
                metadata: Default::default(),
                login_info: None,
                prefix_request: None,
            },
        )
        .expect_err("missing scheduler ingress profile should fail closed");

        assert!(
            err.to_string()
                .contains("scheduler ingress requires loaded profile for node 'step_0'"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn scheduler_generate_runtime_input_errors_when_profile_node_config_missing() {
        let routing = make_routing("catalog");
        let profile = make_profile("catalog", "other", 17);
        let err = UnifiedTaskIngressChain::build_scheduler_generate_runtime_input(
            SchedulerGenerateRuntimeInputRequest {
                module_id: "acc-plt-catalog",
                profile: Some(&profile),
                routing: &routing,
                module_name: "catalog",
                node_id: "step_0",
                metadata: Default::default(),
                login_info: None,
                prefix_request: None,
            },
        )
        .expect_err("missing profile node config should fail closed");

        assert!(
            err.to_string()
                .contains("profile node config missing for scheduler ingress node 'step_0'"),
            "unexpected error message: {err}"
        );
    }

    /// Stub processor that always returns a fatal error — used only to satisfy
    /// the chain type signature (In → SyncBoxStream) in tests.
    struct StubTerminalProcessor;

    #[async_trait]
    impl<T: Send + 'static> ProcessorTrait<T, SyncBoxStream<'static, ()>> for StubTerminalProcessor {
        fn name(&self) -> &'static str {
            "StubTerminal"
        }
        async fn process(
            &self,
            _input: T,
            _context: ProcessorContext,
        ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
            ProcessorResult::FatalFailure(
                crate::errors::ModuleError::ModuleNotFound("stub".into()).into(),
            )
        }
    }

    #[async_trait]
    impl<T: Send + Sync + Clone + 'static> EventProcessorTrait<T, SyncBoxStream<'static, ()>>
        for StubTerminalProcessor
    {
        fn pre_status(&self, _: &T) -> Option<EventEnvelope> {
            None
        }
        fn finish_status(&self, _: &T, _: &SyncBoxStream<'static, ()>) -> Option<EventEnvelope> {
            None
        }
        fn working_status(&self, _: &T) -> Option<EventEnvelope> {
            None
        }
        fn error_status(&self, _: &T, _: &Error) -> Option<EventEnvelope> {
            None
        }
        fn retry_status(&self, _: &T, _: &RetryPolicy) -> Option<EventEnvelope> {
            None
        }
    }

    struct RecordingParserTerminalProcessor {
        seen: Arc<Mutex<Vec<NodeDispatchEnvelope>>>,
    }

    #[async_trait]
    impl ProcessorTrait<NodeDispatchEnvelope, SyncBoxStream<'static, ()>>
        for RecordingParserTerminalProcessor
    {
        fn name(&self) -> &'static str {
            "RecordingParserTerminal"
        }

        async fn process(
            &self,
            input: NodeDispatchEnvelope,
            _context: ProcessorContext,
        ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
            self.seen
                .lock()
                .expect("parser recorder mutex poisoned")
                .push(input);
            ProcessorResult::Success(Box::pin(futures::stream::empty()))
        }
    }

    #[async_trait]
    impl EventProcessorTrait<NodeDispatchEnvelope, SyncBoxStream<'static, ()>>
        for RecordingParserTerminalProcessor
    {
        fn pre_status(&self, _: &NodeDispatchEnvelope) -> Option<EventEnvelope> {
            None
        }
        fn finish_status(
            &self,
            _: &NodeDispatchEnvelope,
            _: &SyncBoxStream<'static, ()>,
        ) -> Option<EventEnvelope> {
            None
        }
        fn working_status(&self, _: &NodeDispatchEnvelope) -> Option<EventEnvelope> {
            None
        }
        fn error_status(&self, _: &NodeDispatchEnvelope, _: &Error) -> Option<EventEnvelope> {
            None
        }
        fn retry_status(&self, _: &NodeDispatchEnvelope, _: &RetryPolicy) -> Option<EventEnvelope> {
            None
        }
    }

    struct RecordingErrorTerminalProcessor {
        seen: Arc<Mutex<Vec<NodeErrorEnvelope>>>,
    }

    #[async_trait]
    impl ProcessorTrait<NodeErrorEnvelope, SyncBoxStream<'static, ()>>
        for RecordingErrorTerminalProcessor
    {
        fn name(&self) -> &'static str {
            "RecordingErrorTerminal"
        }

        async fn process(
            &self,
            input: NodeErrorEnvelope,
            _context: ProcessorContext,
        ) -> ProcessorResult<SyncBoxStream<'static, ()>> {
            self.seen
                .lock()
                .expect("error recorder mutex poisoned")
                .push(input);
            ProcessorResult::Success(Box::pin(futures::stream::empty()))
        }
    }

    #[async_trait]
    impl EventProcessorTrait<NodeErrorEnvelope, SyncBoxStream<'static, ()>>
        for RecordingErrorTerminalProcessor
    {
        fn pre_status(&self, _: &NodeErrorEnvelope) -> Option<EventEnvelope> {
            None
        }
        fn finish_status(
            &self,
            _: &NodeErrorEnvelope,
            _: &SyncBoxStream<'static, ()>,
        ) -> Option<EventEnvelope> {
            None
        }
        fn working_status(&self, _: &NodeErrorEnvelope) -> Option<EventEnvelope> {
            None
        }
        fn error_status(&self, _: &NodeErrorEnvelope, _: &Error) -> Option<EventEnvelope> {
            None
        }
        fn retry_status(&self, _: &NodeErrorEnvelope, _: &RetryPolicy) -> Option<EventEnvelope> {
            None
        }
    }

    #[tokio::test]
    async fn parser_dispatch_without_bridge_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let dispatch = make_dispatch_envelope("my_mod", Some("node_a"));
        let result = chain.try_scheduler_parser_dispatch(&dispatch).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler parser execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("scheduler bridge not attached"),
                    "unexpected reason: {reason}"
                );
                assert!(reason.contains("my_mod"), "unexpected reason: {reason}");
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn parser_dispatch_gate_blocked_fails_closed() {
        let parser_seen = Arc::new(Mutex::new(Vec::new()));
        let error_seen = Arc::new(Mutex::new(Vec::new()));
        let (chain, _task_manager) = make_ingress_chain_with_scheduler_bridge_and_recorders(
            parser_seen.clone(),
            error_seen.clone(),
            Some(SchedulerIngressCutoverGateConfig {
                failure_threshold: Some(3),
                recovery_window_secs: Some(60),
                gray_ratio: Some(0.0),
            }),
        )
        .await;
        let dispatch = make_dispatch_envelope("my_mod", Some("node_a"));

        let result = chain
            .execute(
                UnifiedTaskInput::ParserDispatch(dispatch),
                ProcessorContext::default(),
            )
            .await;

        assert!(
            matches!(result, ProcessorResult::RetryableFailure(_)),
            "gate-blocked parser dispatch must fail-closed"
        );
        assert_eq!(parser_seen.lock().unwrap().len(), 0);
        assert_eq!(error_seen.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn error_envelope_gate_blocked_fails_closed() {
        let parser_seen = Arc::new(Mutex::new(Vec::new()));
        let error_seen = Arc::new(Mutex::new(Vec::new()));
        let (chain, task_manager) = make_ingress_chain_with_scheduler_bridge_and_recorders(
            parser_seen.clone(),
            error_seen.clone(),
            Some(SchedulerIngressCutoverGateConfig {
                failure_threshold: Some(1),
                recovery_window_secs: Some(60),
                gray_ratio: Some(1.0),
            }),
        )
        .await;
        task_manager.record_module_dag_cutover_failure("my_mod");
        let envelope = make_error_envelope("my_mod", Some("node_a"));

        let result = chain
            .execute(
                UnifiedTaskInput::ErrorEnvelope(envelope),
                ProcessorContext::default(),
            )
            .await;

        assert!(
            matches!(result, ProcessorResult::RetryableFailure(_)),
            "gate-blocked error envelope must fail-closed"
        );
        assert_eq!(parser_seen.lock().unwrap().len(), 0);
        assert_eq!(error_seen.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn parser_dispatch_with_attached_bridge_fast_path_publishes_request_when_gate_allows() {
        let parser_seen = Arc::new(Mutex::new(Vec::new()));
        let error_seen = Arc::new(Mutex::new(Vec::new()));
        let (chain, _task_manager, queue_manager) =
            make_seeded_scheduler_bridge_chain(parser_seen.clone(), error_seen.clone()).await;
        let dispatch = make_dispatch_envelope("fast_path_mod", Some("step_0"));

        let result = chain
            .execute(
                UnifiedTaskInput::ParserDispatch(dispatch),
                ProcessorContext::default(),
            )
            .await;

        assert!(matches!(result, ProcessorResult::Success(_)));
        assert!(parser_seen.lock().unwrap().is_empty());
        assert!(error_seen.lock().unwrap().is_empty());

        let queued = queue_manager
            .get_request_pop_channel()
            .lock()
            .await
            .try_recv()
            .expect("scheduler parser fast-path should publish one request");
        let request = decode_request_dispatch(queued.inner)
            .expect("queued parser fast-path payload should decode as request dispatch");
        assert_eq!(request.url, "https://example.com/fast-path");
    }

    #[tokio::test]
    async fn error_envelope_with_attached_bridge_fast_path_publishes_request_when_gate_allows() {
        let parser_seen = Arc::new(Mutex::new(Vec::new()));
        let error_seen = Arc::new(Mutex::new(Vec::new()));
        let (chain, _task_manager, queue_manager) =
            make_seeded_scheduler_bridge_chain(parser_seen.clone(), error_seen.clone()).await;
        let envelope = make_error_envelope("fast_path_mod", Some("step_0"));

        let result = chain
            .execute(
                UnifiedTaskInput::ErrorEnvelope(envelope),
                ProcessorContext::default(),
            )
            .await;

        assert!(matches!(result, ProcessorResult::Success(_)));
        assert!(parser_seen.lock().unwrap().is_empty());
        assert!(error_seen.lock().unwrap().is_empty());

        let queued = queue_manager
            .get_request_pop_channel()
            .lock()
            .await
            .try_recv()
            .expect("scheduler error fast-path should publish one request");
        let request = decode_request_dispatch(queued.inner)
            .expect("queued error fast-path payload should decode as request dispatch");
        assert_eq!(request.url, "https://example.com/fast-path");
    }

    #[tokio::test]
    async fn parser_dispatch_with_attached_bridge_remote_hint_dispatcher_backend_failure_is_retryable_and_fail_closed()
     {
        let parser_seen = Arc::new(Mutex::new(Vec::new()));
        let error_seen = Arc::new(Mutex::new(Vec::new()));
        let (chain, _task_manager, queue_manager) =
            make_seeded_scheduler_bridge_chain(parser_seen.clone(), error_seen.clone()).await;
        let mut dispatch = make_dispatch_envelope("fast_path_mod", Some("step_0"));
        dispatch.parser_context.as_mut().unwrap().runtime_node = Some(
            RuntimeNodeRoutingHint::new("step_0")
                .with_placement(crate::schedule::dag::NodePlacement::remote("wg-test")),
        );

        let result = chain
            .execute(
                UnifiedTaskInput::ParserDispatch(dispatch),
                ProcessorContext::default(),
            )
            .await;

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler parser execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("requires dag_dispatcher but none configured"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }

        assert!(parser_seen.lock().unwrap().is_empty());
        assert!(error_seen.lock().unwrap().is_empty());
        assert!(
            queue_manager
                .get_request_pop_channel()
                .lock()
                .await
                .try_recv()
                .is_err()
        );
    }

    #[tokio::test]
    async fn error_envelope_with_attached_bridge_remote_hint_dispatcher_backend_failure_is_retryable_and_fail_closed()
     {
        let parser_seen = Arc::new(Mutex::new(Vec::new()));
        let error_seen = Arc::new(Mutex::new(Vec::new()));
        let (chain, _task_manager, queue_manager) =
            make_seeded_scheduler_bridge_chain(parser_seen.clone(), error_seen.clone()).await;
        let mut envelope = make_error_envelope("fast_path_mod", Some("step_0"));
        envelope.error_context.as_mut().unwrap().runtime_node = Some(
            RuntimeNodeRoutingHint::new("step_0")
                .with_placement(crate::schedule::dag::NodePlacement::remote("wg-test")),
        );

        let result = chain
            .execute(
                UnifiedTaskInput::ErrorEnvelope(envelope),
                ProcessorContext::default(),
            )
            .await;

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler error execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("requires dag_dispatcher but none configured"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }

        assert!(parser_seen.lock().unwrap().is_empty());
        assert!(error_seen.lock().unwrap().is_empty());
        assert!(
            queue_manager
                .get_request_pop_channel()
                .lock()
                .await
                .try_recv()
                .is_err()
        );
    }

    #[tokio::test]
    async fn parser_dispatch_without_typed_context_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let dispatch = make_dispatch_envelope("my_mod", None);
        let mut dispatch = dispatch;
        dispatch.routing.module.clear();
        dispatch.dispatch.target_node.clear();
        dispatch.dispatch.input.target_node.clear();
        let result = chain.try_scheduler_parser_dispatch(&dispatch).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("required node_id is missing"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn error_dispatch_without_bridge_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let envelope = make_error_envelope("my_mod", Some("node_a"));
        let result = chain.try_scheduler_error_dispatch(&envelope).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler error execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("scheduler bridge not attached"),
                    "unexpected reason: {reason}"
                );
                assert!(reason.contains("my_mod"), "unexpected reason: {reason}");
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn error_dispatch_without_typed_context_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let envelope = make_error_envelope("my_mod", None);
        let mut envelope = envelope;
        envelope.routing.module.clear();
        envelope.routing.node_key.clear();
        let result = chain.try_scheduler_error_dispatch(&envelope).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("required node_id is missing"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn parser_dispatch_with_empty_node_id_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let mut dispatch = make_dispatch_envelope("my_mod", Some("node_a"));
        if let Some(ctx) = dispatch.parser_context.as_mut() {
            ctx.context.node_id = Some(String::new());
        }
        dispatch.dispatch.target_node.clear();
        dispatch.dispatch.input.target_node.clear();
        let result = chain.try_scheduler_parser_dispatch(&dispatch).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("required node_id is missing"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn parser_dispatch_without_module_name_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let mut dispatch = make_dispatch_envelope("my_mod", Some("node_a"));
        if let Some(ctx) = dispatch.parser_context.as_mut() {
            ctx.modules = None;
        }
        dispatch.routing.module.clear();
        let result = chain.try_scheduler_parser_dispatch(&dispatch).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("required module_name is missing"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn parser_dispatch_without_modules_list_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let mut dispatch = make_dispatch_envelope("my_mod", Some("node_a"));
        if let Some(ctx) = dispatch.parser_context.as_mut() {
            ctx.modules = None;
        }
        dispatch.routing.module.clear();
        let result = chain.try_scheduler_parser_dispatch(&dispatch).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("required module_name is missing"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn error_dispatch_without_module_name_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let mut envelope = make_error_envelope("my_mod", Some("node_a"));
        if let Some(ctx) = envelope.error_context.as_mut() {
            ctx.modules = None;
        }
        envelope.routing.module.clear();
        let result = chain.try_scheduler_error_dispatch(&envelope).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("required module_name is missing"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[tokio::test]
    async fn error_dispatch_without_modules_list_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let mut envelope = make_error_envelope("my_mod", Some("node_a"));
        if let Some(ctx) = envelope.error_context.as_mut() {
            ctx.modules = None;
        }
        envelope.routing.module.clear();
        let result = chain.try_scheduler_error_dispatch(&envelope).await;
        match result {
            Some(ProcessorResult::RetryableFailure(policy)) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("required module_name is missing"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn resolve_parser_targets_require_typed_context_or_transport_fields() {
        let chain = make_ingress_chain_no_bridge();
        let mut dispatch = make_dispatch_envelope("my_mod", None);
        dispatch.routing.module.clear();
        dispatch.dispatch.target_node.clear();
        dispatch.dispatch.input.target_node.clear();
        assert_eq!(chain.resolve_parser_node_id(&dispatch, None), None);
        assert_eq!(chain.resolve_parser_module_name(&dispatch, None), None);
    }

    #[test]
    fn resolve_error_targets_require_typed_context_or_transport_fields() {
        let chain = make_ingress_chain_no_bridge();
        let mut envelope = make_error_envelope("my_mod", None);
        envelope.routing.module.clear();
        envelope.routing.node_key.clear();
        assert_eq!(chain.resolve_error_node_id(&envelope, None), None);
        assert_eq!(chain.resolve_error_module_name(&envelope, None), None);
    }

    #[test]
    fn resolve_parser_targets_can_use_transport_fields_without_seed() {
        let chain = make_ingress_chain_no_bridge();
        let dispatch = make_dispatch_envelope("transport_mod", None);

        assert_eq!(
            chain.resolve_parser_node_id(&dispatch, None).as_deref(),
            Some("step_0")
        );
        assert_eq!(
            chain.resolve_parser_module_name(&dispatch, None).as_deref(),
            Some("transport_mod")
        );
    }

    #[test]
    fn resolve_parser_targets_ignore_malformed_payload_when_transport_is_complete() {
        let chain = make_ingress_chain_no_bridge();
        let mut dispatch = make_dispatch_envelope("transport_mod", None);
        dispatch.dispatch.input.payload.bytes = b"not-valid-transport-seed".to_vec();

        assert_eq!(
            chain.resolve_parser_node_id(&dispatch, None).as_deref(),
            Some("step_0")
        );
        assert_eq!(
            chain.resolve_parser_module_name(&dispatch, None).as_deref(),
            Some("transport_mod")
        );
    }

    #[test]
    fn resolve_error_targets_can_use_transport_fields_without_seed() {
        let chain = make_ingress_chain_no_bridge();
        let envelope = make_error_envelope("transport_mod", None);

        assert_eq!(
            chain.resolve_error_node_id(&envelope, None).as_deref(),
            Some("step_0")
        );
        assert_eq!(
            chain.resolve_error_module_name(&envelope, None).as_deref(),
            Some("transport_mod")
        );
    }

    #[test]
    fn resolve_parser_metadata_and_prefix_prefer_typed_context() {
        let chain = make_ingress_chain_no_bridge();
        let mut dispatch = make_dispatch_envelope("my_mod", Some("node_a"));
        let typed_prefix = Uuid::now_v7();
        let transport_prefix = Uuid::now_v7();
        dispatch.routing.parent_request_id = Some(transport_prefix);
        if let Some(ctx) = dispatch.parser_context.as_mut() {
            ctx.metadata
                .insert("typed".to_string(), serde_json::Value::from(1));
            ctx.prefix_request_id = Some(typed_prefix);
        }

        let metadata = chain
            .resolve_parser_metadata(dispatch.parser_context.as_ref())
            .expect("typed parser metadata should exist");
        let prefix =
            chain.resolve_parser_prefix_request(&dispatch, dispatch.parser_context.as_ref());

        assert_eq!(metadata.get("typed"), Some(&serde_json::Value::from(1)));
        assert_eq!(prefix, Some(typed_prefix));
    }

    #[test]
    fn resolve_parser_metadata_and_prefix_fallback_to_transport_only() {
        let chain = make_ingress_chain_no_bridge();
        let mut dispatch = make_dispatch_envelope("my_mod", None);
        let transport_parent = Uuid::now_v7();
        dispatch.routing.parent_request_id = Some(transport_parent);
        let metadata = chain.resolve_parser_metadata(None);
        let prefix = chain.resolve_parser_prefix_request(&dispatch, None);

        assert!(metadata.is_none());
        assert_eq!(prefix, Some(transport_parent));

        dispatch.routing.parent_request_id = None;
        let prefix_from_transport = chain.resolve_parser_prefix_request(&dispatch, None);
        assert_eq!(prefix_from_transport, None);
    }

    #[test]
    fn resolve_error_metadata_and_prefix_prefer_typed_context() {
        let chain = make_ingress_chain_no_bridge();
        let mut envelope = make_error_envelope("my_mod", Some("node_a"));
        let typed_prefix = Uuid::now_v7();
        let transport_prefix = Uuid::now_v7();
        envelope.routing.parent_request_id = Some(transport_prefix);
        if let Some(ctx) = envelope.error_context.as_mut() {
            ctx.metadata
                .insert("typed".to_string(), serde_json::Value::from(1));
            ctx.prefix_request_id = Some(typed_prefix);
        }

        let metadata = chain
            .resolve_error_metadata(envelope.error_context.as_ref())
            .expect("typed error metadata should exist");
        let prefix = chain.resolve_error_prefix_request(&envelope, envelope.error_context.as_ref());

        assert_eq!(metadata.get("typed"), Some(&serde_json::Value::from(1)));
        assert_eq!(prefix, Some(typed_prefix));
    }

    #[test]
    fn resolve_error_metadata_and_prefix_fallback_to_transport_only() {
        let chain = make_ingress_chain_no_bridge();
        let mut envelope = make_error_envelope("my_mod", None);
        let transport_parent = Uuid::now_v7();
        envelope.routing.parent_request_id = Some(transport_parent);
        let metadata = chain.resolve_error_metadata(None);
        let prefix = chain.resolve_error_prefix_request(&envelope, None);

        assert!(metadata.is_none());
        assert_eq!(prefix, Some(transport_parent));

        envelope.routing.parent_request_id = None;
        let prefix_from_transport = chain.resolve_error_prefix_request(&envelope, None);
        assert_eq!(prefix_from_transport, None);
    }

    #[test]
    fn parser_dispatcher_failure_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let result = chain.scheduler_dispatch_failed_result(
            "parser",
            &DagError::NodeExecutionFailed {
                node_id: "page".to_string(),
                reason: "forced scheduler bridge failure".to_string(),
            },
        );

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler parser execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("forced scheduler bridge failure"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn error_dispatcher_failure_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let result = chain.scheduler_dispatch_failed_result(
            "error",
            &DagError::NodeExecutionFailed {
                node_id: "page".to_string(),
                reason: "network timeout while dispatching remote node".to_string(),
            },
        );

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler error execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("network timeout while dispatching remote node"),
                    "unexpected reason: {reason}"
                );
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn gray_gate_blocked_fallback_does_not_count_as_stage_error() {
        assert!(
            !UnifiedTaskIngressChain::scheduler_ingress_fallback_counts_as_error(
                UnifiedTaskIngressChain::FALLBACK_GRAY_GATE_BLOCKED,
            )
        );
    }

    #[test]
    fn failure_gate_blocked_fallback_does_not_count_as_stage_error() {
        assert!(
            !UnifiedTaskIngressChain::scheduler_ingress_fallback_counts_as_error(
                UnifiedTaskIngressChain::FALLBACK_FAILURE_GATE_BLOCKED,
            )
        );
    }

    #[test]
    fn parser_missing_target_result_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let result = chain.scheduler_missing_target_result("parser", "node_id", "catalog");

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler parser execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("required node_id is missing"),
                    "unexpected reason: {reason}"
                );
                assert!(reason.contains("catalog"), "unexpected reason: {reason}");
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn parser_task_module_not_found_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let result = chain.scheduler_task_module_not_found_result("parser", "catalog", "step_0");

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler parser execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(reason.contains("catalog"), "unexpected reason: {reason}");
                assert!(reason.contains("step_0"), "unexpected reason: {reason}");
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn error_task_module_not_found_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let result = chain.scheduler_task_module_not_found_result("error", "catalog", "step_0");

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler error execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(reason.contains("catalog"), "unexpected reason: {reason}");
                assert!(reason.contains("step_0"), "unexpected reason: {reason}");
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn parser_dag_missing_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let result = chain.scheduler_dag_missing_result("parser", "catalog");

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler parser execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("precompiled DAG missing"),
                    "unexpected reason: {reason}"
                );
                assert!(reason.contains("catalog"), "unexpected reason: {reason}");
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn error_dag_missing_is_retryable_and_fail_closed() {
        let chain = make_ingress_chain_no_bridge();
        let result = chain.scheduler_dag_missing_result("error", "catalog");

        match result {
            ProcessorResult::RetryableFailure(policy) => {
                let reason = policy.reason.expect("retry policy reason should exist");
                assert!(
                    reason.contains("scheduler error execution failed"),
                    "unexpected reason: {reason}"
                );
                assert!(
                    reason.contains("precompiled DAG missing"),
                    "unexpected reason: {reason}"
                );
                assert!(reason.contains("catalog"), "unexpected reason: {reason}");
            }
            _ => panic!("expected retryable failure"),
        }
    }

    #[test]
    fn requires_remote_dispatch_hint_detects_remote_placement() {
        let hints = vec![
            RuntimeNodeRoutingHint::new("node_a")
                .with_placement(crate::schedule::dag::NodePlacement::remote("wg-test")),
        ];
        assert!(UnifiedTaskIngressChain::requires_remote_dispatch_hint(
            &hints
        ));
    }

    #[test]
    fn requires_remote_dispatch_hint_ignores_local_or_empty_hints() {
        let local_hints = vec![
            RuntimeNodeRoutingHint::new("node_a")
                .with_placement(crate::schedule::dag::NodePlacement::local()),
        ];
        let empty_hints: Vec<RuntimeNodeRoutingHint> = vec![];

        assert!(!UnifiedTaskIngressChain::requires_remote_dispatch_hint(
            &local_hints
        ));
        assert!(!UnifiedTaskIngressChain::requires_remote_dispatch_hint(
            &empty_hints
        ));
    }
}
