use crate::common::interface::middleware_manager::MiddlewareManager;
use crate::common::model::data::DataEvent;
use crate::common::model::message::TaskEvent;
use crate::common::model::workflow_profile::TaskProfileSnapshot;
use crate::common::model::Response;
use crate::common::state::State;
use crate::common::status_tracker::ErrorDecision;
use crate::engine::events::{
    DataMiddlewareEvent, DataStoreEvent, EventBus, EventEnvelope, EventPhase, EventType,
    ModuleGenerateEvent, ParserEvent,
};
use crate::engine::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use crate::engine::task::TaskManager;
use crate::errors::{DataMiddlewareError, Error, Result};
use async_trait::async_trait;

use crate::cacheable::CacheService;
use crate::common::model::login_info::LoginInfo;
use crate::common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use crate::common::processors::processor_chain::ErrorStrategy;
#[cfg(test)]
use crate::common::response_cache::localize_response_cache_entry;
use crate::common::response_cache::{current_owner_api_base_url, persist_response_cache_entry};
use crate::engine::chain::backpressure::{BackpressureSendState, send_with_backpressure};
use crate::engine::task::module::Module;
use crate::engine::task::parser_error_adapter::{
    build_error_envelope_from_seed, build_parser_dispatch_from_seed, ErrorEnvelopeSeed,
};
use crate::queue::{QueueManager, QueuedItem};
use log::{debug, error, info, warn};
use metrics::counter;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
fn localize_parser_side_response_cache_entry(
    response: &Response,
    local_namespace: &str,
    local_node_id: Option<&str>,
) -> Option<Response> {
    localize_response_cache_entry(response, local_namespace, local_node_id, None, None)
        .map(|(localized, _)| localized)
}

async fn persist_parser_side_response_cache_entry(
    response: &Response,
    local_namespace: &str,
    local_node_id: Option<&str>,
    cache_service: &CacheService,
    profile_store: &crate::engine::api::profile_store::ProfileControlPlaneStore,
) {
    let owner_api_base_url = current_owner_api_base_url(
        profile_store,
        local_node_id,
        Duration::from_secs(30),
    );
    let _ = persist_response_cache_entry(
        response,
        local_namespace,
        local_node_id,
        owner_api_base_url.as_deref(),
        cache_service.default_ttl(),
        cache_service,
        profile_store,
        "parser_side_refresh",
    )
    .await;
}

/// Resolves response to module/config/login context before parsing.
pub struct ResponseModuleProcessor {
    task_manager: Arc<TaskManager>,
    state: Arc<State>,
}
#[async_trait]
impl ProcessorTrait<Response, (Response, Arc<Module>, Option<LoginInfo>)>
    for ResponseModuleProcessor
{
    fn name(&self) -> &'static str {
        "ResponseModuleProcessor"
    }

    async fn process(
        &self,
        input: Response,
        context: ProcessorContext,
    ) -> ProcessorResult<(Response, Arc<Module>, Option<LoginInfo>)> {
        // [LOG_OPTIMIZATION] Removed start log to reduce I/O blocking latency
        // info!(
        //     "[ResponseModuleProcessor] start: request_id={} module_id={}",
        //     input.id,
        //     input.module_id()
        // );

        // Guard checks before parser execution.
        // 1) Task-level threshold.
        match self
            .state
            .status_tracker
            .should_task_continue(&input.task_runtime_id())
            .await
        {
            Ok(ErrorDecision::Continue) => {
                // [LOG_OPTIMIZATION] debug!("[ResponseModuleProcessor] task check passed: task_id={}", input.task_runtime_id());
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[ResponseModuleProcessor] task terminated before parsing: task_id={} reason={}",
                    input.task_runtime_id(),
                    reason
                );
                // Release lock on task termination path.
                self.state
                    .status_tracker
                    .release_module_locker(&input.module_runtime_id())
                    .await;
                return ProcessorResult::FatalFailure(
                    crate::errors::ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[ResponseModuleProcessor] task error check failed, continue anyway: task_id={} error={}",
                    input.task_runtime_id(),
                    e
                );
            }
            _ => {}
        }

        // 2) Module-level threshold.
        match self
            .state
            .status_tracker
            .should_module_continue(&input.module_runtime_id())
            .await
        {
            Ok(ErrorDecision::Continue) => {
                // [LOG_OPTIMIZATION] debug!("[ResponseModuleProcessor] module check passed: module_id={}", input.module_runtime_id());
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[ResponseModuleProcessor] module terminated before parsing: module_id={} reason={}",
                    input.module_runtime_id(),
                    reason
                );
                // Release lock on module termination path.
                self.state
                    .status_tracker
                    .release_module_locker(&input.module_runtime_id())
                    .await;
                return ProcessorResult::FatalFailure(
                    crate::errors::ModuleError::ModuleMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[ResponseModuleProcessor] module error check failed, continue anyway: module_id={} error={}",
                    input.module_runtime_id(),
                    e
                );
            }
            _ => {}
        }

        let task: Result<(Arc<Module>, Option<LoginInfo>)> =
            self.task_manager.load_module_with_response(&input).await;
        match task {
            Ok((module, login_info)) => {
                ProcessorResult::Success((input, module, login_info))
            }
            Err(e) => {
                warn!(
                    "[ResponseModuleProcessor] load_with_response failed, will retry: account={} platform={} request_id={} err={e}",
                    input.account, input.platform, input.id
                );
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
    async fn pre_process(&self, input: &Response, _context: &ProcessorContext) -> Result<()> {
        if self.state.config.read().await.download_config.enable_locker {
            debug!(
                "[ResponseModuleProcessor] lock module before parsing: module_id={}",
                input.module_runtime_id()
            );
            self.state
                .status_tracker
                .lock_module(&input.module_runtime_id())
                .await;
        }
        Ok(())
    }
}
#[async_trait]
impl EventProcessorTrait<Response, (Response, Arc<Module>, Option<LoginInfo>)>
    for ResponseModuleProcessor
{
    fn pre_status(&self, input: &Response) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Started,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn finish_status(
        &self,
        input: &Response,
        _output: &(Response, Arc<Module>, Option<LoginInfo>),
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Completed,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn working_status(&self, input: &Response) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Started,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn error_status(&self, input: &Response, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::ModuleGenerate,
            EventPhase::Failed,
            ModuleGenerateEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &Response, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
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

pub struct ResponseParserProcessor {
    queue_manager: Arc<QueueManager>,
    state: Arc<State>,
    cache_service: Arc<CacheService>,
    event_bus: Option<Arc<EventBus>>,
}

impl ResponseParserProcessor {
    async fn emit_semantic_event(&self, event_type: EventType, payload: serde_json::Value) {
        if let Some(event_bus) = &self.event_bus {
            let _ = event_bus
                .publish(EventEnvelope::engine(
                    event_type,
                    EventPhase::Completed,
                    payload,
                ))
                .await;
        }
    }

    async fn send_with_backpressure<T>(
        &self,
        tx: &tokio::sync::mpsc::Sender<QueuedItem<T>>,
        item: QueuedItem<T>,
        queue_kind: &'static str,
        log_context: &str,
    ) -> bool {
        match send_with_backpressure(tx, item).await {
            Ok(BackpressureSendState::Direct) => true,
            Ok(BackpressureSendState::RecoveredFromFull) => {
                counter!("mocra_parser_chain_backpressure_total", "queue" => queue_kind, "reason" => "queue_full").increment(1);
                warn!(
                    "[ResponseParserProcessor] queue full, fallback to awaited send: queue={} context={} remaining_capacity={}",
                    queue_kind,
                    log_context,
                    tx.capacity()
                );
                true
            }
            Err(err) => {
                if err.after_full {
                    counter!("mocra_parser_chain_backpressure_total", "queue" => queue_kind, "reason" => "queue_full").increment(1);
                    warn!(
                        "[ResponseParserProcessor] queue full before close: queue={} context={} remaining_capacity={}",
                        queue_kind,
                        log_context,
                        tx.capacity()
                    );
                }
                counter!("mocra_parser_chain_backpressure_total", "queue" => queue_kind, "reason" => "queue_closed").increment(1);
                error!(
                    "[ResponseParserProcessor] queue closed before send: queue={} context={}",
                    queue_kind, log_context
                );
                false
            }
        }
    }
}

#[async_trait]
impl ProcessorTrait<(Response, Arc<Module>, Option<LoginInfo>), Vec<DataEvent>>
    for ResponseParserProcessor
{
    fn name(&self) -> &'static str {
        "ResponseParserProcessor"
    }

    async fn process(
        &self,
        input: (Response, Arc<Module>, Option<LoginInfo>),
        context: ProcessorContext,
    ) -> ProcessorResult<Vec<DataEvent>> {
        info!(
            "[ResponseParserProcessor] start parse: account={} platform={} module={} request_id={} module_id={}",
            input.0.account,
            input.0.platform,
            input.0.module,
            input.0.id,
            input.0.module_id()
        );
        let module = input.1.clone();
        let profile = module.profile.clone().unwrap_or_default();
        // Propagate profile snapshot through processor context for downstream data stages.
        // DataMiddlewareProcessor/DataStoreProcessor read `context.metadata["profile"]`.
        match serde_json::to_value(profile.as_ref()) {
            Ok(cfg_val) => {
                context
                    .metadata
                    .write()
                    .await
                    .insert("profile".to_string(), cfg_val);
            }
            Err(e) => {
                warn!(
                    "[ResponseParserProcessor] failed to serialize profile snapshot into context: request_id={} module_id={} error={}",
                    input.0.id,
                    input.0.module_id(),
                    e
                );
            }
        }

        // StateHandle has been removed; SyncService is used internally by ModuleProcessor.
        let task_id = input.0.task_runtime_id();
        let module_id = input.0.module_runtime_id();
        let request_id = input.0.id.to_string();
        let account = input.0.account.clone();
        let platform = input.0.platform.clone();
        let module_name = input.0.module.clone();

        let data = module.parser(input.0.clone()).await;
        let mut data = match data {
            Ok(d) => {
                let has_next_task = !d.next_dispatches.is_empty();
                let has_error = d.error.is_some();
                info!(
                    "[ResponseParserProcessor] parser returned: request_id={} data_len={} has_next_task={} has_error={}",
                    request_id,
                    d.data.len(),
                    has_next_task,
                    has_error
                );

                // Record parser success metrics/state.
                self.state
                    .status_tracker
                    .record_parse_success(&request_id)
                    .await
                    .ok();

                d
            }
            Err(e) => {
                warn!(
                    "[ResponseParserProcessor] parser error: account={} platform={} module={} request_id={} error={e}",
                    account, platform, module_name, request_id
                );

                // Record parser error and retrieve threshold decision.
                match self
                    .state
                    .status_tracker
                    .record_parse_error(&task_id, &module_id, &request_id, &e)
                    .await
                {
                    Ok(ErrorDecision::Continue) | Ok(ErrorDecision::RetryAfter(_)) => {
                        debug!(
                            "[ResponseParserProcessor] will retry parsing: request_id={}",
                            request_id
                        );
                        return ProcessorResult::RetryableFailure(
                            context
                                .retry_policy
                                .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                        );
                    }
                    Ok(ErrorDecision::Skip) => {
                        warn!(
                            "[ResponseParserProcessor] skip parse after max retries: request_id={}",
                            request_id
                        );
                        // Skip this parse attempt and return empty output.
                        return ProcessorResult::Success(vec![]);
                    }
                    Ok(ErrorDecision::Terminate(reason)) => {
                        error!("[ResponseParserProcessor] terminate: {}", reason);
                        return ProcessorResult::FatalFailure(e);
                    }
                    Err(err) => {
                        error!("[ResponseParserProcessor] error tracker failed: {}", err);
                        return ProcessorResult::FatalFailure(e);
                    }
                }
            }
        };

        let parser_task_queue = self.queue_manager.get_parser_task_push_channel();
        for dispatch_seed in data.next_dispatches.drain(..) {
            let task_account = dispatch_seed.task_model.account.clone();
            let task_platform = dispatch_seed.task_model.platform.clone();
            let task_run_id = dispatch_seed.run_id;
            let task_module_id = dispatch_seed.context.module_id.clone();
            let task_step_idx = dispatch_seed.context.step_idx;
            let task_prefix_request = dispatch_seed.prefix_request;
            let context = format!(
                "parser_task account={} platform={} run_id={} module_id={}",
                dispatch_seed.task_model.account,
                dispatch_seed.task_model.platform,
                dispatch_seed.run_id,
                dispatch_seed.context.module_id.as_deref().unwrap_or("unknown")
            );
            let dispatch = match build_parser_dispatch_from_seed(
                &dispatch_seed,
                self.queue_manager.namespace.clone(),
            ) {
                Ok(dispatch) => dispatch,
                Err(err) => {
                    error!(
                        "[ResponseParserProcessor] failed to build parser task envelope: {}",
                        err
                    );
                    continue;
                }
            };
            if !self
                .send_with_backpressure(
                    &parser_task_queue,
                    QueuedItem::new(dispatch),
                    "parser_task",
                    &context,
                )
                .await
            {
                error!(
                    "[ResponseParserProcessor] failed to send parser task: {}",
                    context
                );
            } else {
                self.emit_semantic_event(
                    EventType::ParserTaskProduced,
                    json!({
                        "account": task_account,
                        "platform": task_platform,
                        "run_id": task_run_id,
                        "module_id": task_module_id,
                        "step_idx": task_step_idx,
                        "prefix_request": task_prefix_request,
                        "path": "parser_task_queue"
                    }),
                )
                .await;
                self.emit_semantic_event(
                    EventType::ModuleStepAdvanced,
                    json!({
                        "account": task_account,
                        "platform": task_platform,
                        "run_id": task_run_id,
                        "module_id": task_module_id,
                        "step_idx": task_step_idx,
                        "prefix_request": task_prefix_request,
                        "mode": "parser_task_queue"
                    }),
                )
                .await;
            }
        }

        if let Some(mut msg) = data.error {
            warn!(
                "[ResponseParserProcessor] recorded response error for request_id={}, message={}",
                input.0.id, msg.error_message
            );
            msg.prefix_request = input.0.prefix_request;
            msg.run_id = input.0.run_id;
            let queue = self.queue_manager.get_error_push_channel();
            let context = format!(
                "error_task request_id={} module_id={}",
                input.0.id,
                input.0.module_id()
            );
            let envelope = match build_error_envelope_from_seed(
                &msg,
                self.queue_manager.namespace.clone(),
            ) {
                Ok(envelope) => envelope,
                Err(err) => {
                    error!(
                        "[ResponseParserProcessor] failed to build parser error envelope: {}",
                        err
                    );
                    return ProcessorResult::Success(data.data);
                }
            };
            if !self
                .send_with_backpressure(&queue, QueuedItem::new(envelope), "error", &context)
                .await
            {
                error!(
                    "[ResponseParserProcessor] failed to send parser error: {}",
                    context
                );
            } else {
                self.emit_semantic_event(
                    EventType::ErrorTaskProduced,
                    json!({
                        "request_id": input.0.id,
                        "account": input.0.account,
                        "platform": input.0.platform,
                        "module_id": input.0.module_id(),
                        "step_idx": input.0.context.step_idx,
                        "prefix_request": input.0.prefix_request
                    }),
                )
                .await;
            }

            // Legacy `record_response_error` is no longer used.
            // Errors are already tracked via `error_tracker`.
        }

        data.data.iter_mut().for_each(|x| {
            x.account = input.0.account.clone();
            x.platform = input.0.platform.clone();
            // Keep parser-provided middleware when present; otherwise inherit module defaults.
            if x.data_middleware.is_empty() {
                x.data_middleware = input.0.data_middleware.clone();
            }
            x.request_id = input.0.id;
            x.meta = input.0.metadata.clone();
        });
        ProcessorResult::Success(data.data)
    }
    async fn post_process(
        &self,
        input: &(Response, Arc<Module>, Option<LoginInfo>),
        _output: &Vec<DataEvent>,
        _context: &ProcessorContext,
    ) -> Result<()> {
        // Centralized lock release after parsing to avoid deadlocks and duplicate unlocks.

        let config = self.state.config.read().await;

        // Keep a local rollback copy in the parser namespace so follow-up requests can reuse
        // cached responses without rewriting foreign namespace ownership anchors.
        persist_parser_side_response_cache_entry(
            &input.0,
            &config.name,
            config.crawler.node_id.as_deref(),
            &self.cache_service,
            &self.state.profile_store,
        )
        .await;

        if config.download_config.enable_locker {
            self.state
                .status_tracker
                .release_module_locker(&input.0.module_runtime_id())
                .await;
            debug!(
                "[ResponseParserProcessor] released module lock after parsing: module_id={}",
                input.0.module_runtime_id()
            );
        }
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &(Response, Arc<Module>, Option<LoginInfo>),
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Vec<DataEvent>> {
        error!(
            "[ResponseParserProcessor] fatal parser error: request_id={} module_id={} error={}",
            input.0.id,
            input.0.module_id(),
            error
        );

        // Error has already been tracked in `process()` via `error_tracker`.
        // Here we only need to build and enqueue `ErrorTaskModel`.

        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs(),
            Err(e) => {
                warn!(
                    "[ResponseParserProcessor] system time before UNIX_EPOCH, fallback to 0: {}",
                    e
                );
                0
            }
        };
        let error_metadata = input
            .0
            .metadata
            .task
            .as_object()
            .cloned()
            .unwrap_or_default();
        let error_task = ErrorEnvelopeSeed {
            request_id: input.0.id,
            task_model: TaskEvent {
                account: input.0.account.clone(),
                platform: input.0.platform.clone(),
                module: Some(vec![input.0.module.clone()]),
                run_id: input.0.run_id,
                priority: input.0.priority,
            },
            error_message: error.to_string(),
            timestamp,
            metadata: error_metadata,
            context: input.0.context.clone(),
            run_id: input.0.run_id,
            prefix_request: input.0.prefix_request,
        };
        let queue = self.queue_manager.get_error_push_channel();
        let context = format!(
            "handle_error request_id={} module_id={}",
            input.0.id,
            input.0.module_id()
        );
        let envelope = match build_error_envelope_from_seed(&error_task, self.queue_manager.namespace.clone()) {
            Ok(envelope) => envelope,
            Err(err) => {
                error!(
                    "[ResponseParserProcessor] failed to build ErrorTaskModel envelope: {}",
                    err
                );
                return ProcessorResult::FatalFailure(error);
            }
        };
        if !self
            .send_with_backpressure(&queue, QueuedItem::new(envelope), "error", &context)
            .await
        {
            error!(
                "[ResponseParserProcessor] failed to enqueue ErrorTaskModel: {}",
                context
            );
        }

        // Release module lock after enqueueing error task.
        if self.state.config.read().await.download_config.enable_locker {
            self.state
                .status_tracker
                .release_module_locker(&input.0.module_runtime_id())
                .await;
        }

        ProcessorResult::FatalFailure(error)
    }
}
impl
    EventProcessorTrait<
        (Response, Arc<Module>, Option<LoginInfo>),
        Vec<DataEvent>,
    > for ResponseParserProcessor
{
    fn pre_status(
        &self,
        input: &(Response, Arc<Module>, Option<LoginInfo>),
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::Parser,
            EventPhase::Started,
            ParserEvent::from(&input.0),
        ))
    }

    fn finish_status(
        &self,
        input: &(Response, Arc<Module>, Option<LoginInfo>),
        _output: &Vec<DataEvent>,
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::Parser,
            EventPhase::Completed,
            ParserEvent::from(&input.0),
        ))
    }

    fn working_status(
        &self,
        input: &(Response, Arc<Module>, Option<LoginInfo>),
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::Parser,
            EventPhase::Started,
            ParserEvent::from(&input.0),
        ))
    }

    fn error_status(
        &self,
        input: &(Response, Arc<Module>, Option<LoginInfo>),
        err: &Error,
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::Parser,
            EventPhase::Failed,
            ParserEvent::from(&input.0),
            err,
        ))
    }

    fn retry_status(
        &self,
        input: &(Response, Arc<Module>, Option<LoginInfo>),
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::Parser,
            EventPhase::Retry,
            json!({
                "data": ParserEvent::from(&input.0),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}

pub struct DataMiddlewareProcessor {
    middleware_manager: Arc<MiddlewareManager>,
}

#[async_trait]
impl ProcessorTrait<DataEvent, DataEvent> for DataMiddlewareProcessor {
    fn name(&self) -> &'static str {
        "DataMiddlewareProcessor"
    }

    async fn process(
        &self,
        input: DataEvent,
        context: ProcessorContext,
    ) -> ProcessorResult<DataEvent> {
        // info!(
        //    "[DataMiddlewareProcessor] start: account={} platform={} size={}",
        //    input.account,
        //    input.platform,
        //    input.size()
        // );
        let start = std::time::Instant::now();
        let profile = context
            .metadata
            .read()
            .await
            .get("profile")
            .map(|x| {
                serde_json::from_value::<TaskProfileSnapshot>(x.clone()).unwrap_or_else(|e| {
                    warn!(
                        "[DataMiddlewareProcessor] profile conversion failed, using default: request_id={} module={} error={}",
                        input.request_id,
                        input.module,
                        e
                    );
                    TaskProfileSnapshot::default()
                })
            })
            .unwrap_or_default();
        let modified_data = self.middleware_manager.handle_data(input, &profile).await;
        let elapsed_ms = start.elapsed().as_millis();
        if elapsed_ms > 10 {
            info!(
                "[DataMiddlewareProcessor] SLOW middleware execution: {} ms",
                elapsed_ms
            );
        }
        match modified_data {
            Some(data) => ProcessorResult::Success(data),
            None => ProcessorResult::FatalFailure(
                DataMiddlewareError::EmptyData("data dropped by data middleware".into()).into(),
            ),
        }
    }
}
impl EventProcessorTrait<DataEvent, DataEvent> for DataMiddlewareProcessor {
    fn pre_status(&self, input: &DataEvent) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::MiddlewareBefore,
            EventPhase::Started,
            DataMiddlewareEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &DataEvent, output: &DataEvent) -> Option<EventEnvelope> {
        let mut event: DataMiddlewareEvent = input.into();
        event.after_size = output.size().into();
        Some(EventEnvelope::engine(
            EventType::MiddlewareBefore,
            EventPhase::Completed,
            event,
        ))
    }

    fn working_status(&self, input: &DataEvent) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::MiddlewareBefore,
            EventPhase::Started,
            DataMiddlewareEvent::from(input),
        ))
    }

    fn error_status(&self, input: &DataEvent, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::MiddlewareBefore,
            EventPhase::Failed,
            DataMiddlewareEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &DataEvent, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::MiddlewareBefore,
            EventPhase::Retry,
            json!({
                "data": DataMiddlewareEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}

pub struct DataStoreProcessor {
    middleware_manager: Arc<MiddlewareManager>,
}
#[async_trait]
impl ProcessorTrait<DataEvent, ()> for DataStoreProcessor {
    fn name(&self) -> &'static str {
        "DataStoreProcessor"
    }
    /// This stage uses `RetryableFailure` to trigger retries.
    ///
    /// Additional retry context can be passed through `retry_policy.meta`
    /// for downstream retry behavior customization.
    async fn process(&self, input: DataEvent, context: ProcessorContext) -> ProcessorResult<()> {
        info!(
            "[DataStoreProcessor] start store: request_id={} account={} platform={} module={} size={}",
            input.request_id,
            input.account,
            input.platform,
            input.module,
            input.size()
        );
        let mut middleware = vec![];
        if let Some(retry_policy) = &context.retry_policy
            && let Some(m_val) = retry_policy.meta.get("middleware")
            && let Some(m) = m_val.as_array()
        {
            middleware = m
                .iter()
                .filter_map(|x| x.as_str())
                .map(|x| x.to_string())
                .collect::<Vec<String>>();
        }
        let profile = context
            .metadata
            .read()
            .await
            .get("profile")
            .map(|x| {
                serde_json::from_value::<TaskProfileSnapshot>(x.clone()).unwrap_or_else(|e| {
                    warn!(
                        "[DataStoreProcessor] profile conversion failed, using default: request_id={} module={} error={}",
                        input.request_id,
                        input.module,
                        e
                    );
                    TaskProfileSnapshot::default()
                })
            })
            .unwrap_or_default();
        let request_id = input.request_id;
        let res = if middleware.is_empty() {
            self.middleware_manager
                .handle_store_data(input, &profile)
                .await
        } else {
            self.middleware_manager
                .handle_store_data_with_middleware(input, middleware, &profile)
                .await
        };
        if res.is_empty() {
            info!(
                "[DataStoreProcessor] store success, request_id={}",
                request_id
            );
            ProcessorResult::Success(())
        } else {
            let error_msg = res
                .iter()
                .map(|(m, e)| format!("Middleware: {m}, Error: {e:?}"))
                .collect::<Vec<String>>()
                .join("; ");
            let mut retry_policy = context
                .retry_policy
                .unwrap_or_default()
                .with_reason(error_msg);
            let error_middleware = res.keys().map(|x| x.to_string()).collect::<Vec<String>>();
            retry_policy.meta = serde_json::json!({ "middleware": error_middleware });
            warn!(
                "[DataStoreProcessor] request={}, store error, will retry: {}",
                request_id,
                retry_policy.reason.clone().unwrap_or_default()
            );
            ProcessorResult::RetryableFailure(retry_policy)
        }
    }
}
impl EventProcessorTrait<DataEvent, ()> for DataStoreProcessor {
    fn pre_status(&self, input: &DataEvent) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::DataStore,
            EventPhase::Started,
            DataStoreEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &DataEvent, _output: &()) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::DataStore,
            EventPhase::Completed,
            DataStoreEvent::from(input),
        ))
    }

    fn working_status(&self, input: &DataEvent) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::DataStore,
            EventPhase::Started,
            DataStoreEvent::from(input),
        ))
    }

    fn error_status(&self, input: &DataEvent, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::DataStore,
            EventPhase::Failed,
            DataStoreEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &DataEvent, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::DataStore,
            EventPhase::Retry,
            json!({
                "data": DataStoreEvent::from(input),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}

/// Builds parser chain for response processing and data persistence.
///
/// Flow:
/// `response -> module/context -> parser -> data middleware -> data store`.
///
/// Notes:
/// - All `Data` emitted from one response share the same module config context.
/// - Data middleware and store stages run in parallel map form with skip-on-error strategy.
pub async fn create_parser_chain(
    state: Arc<State>,
    task_manager: Arc<TaskManager>,
    middleware_manager: Arc<MiddlewareManager>,
    queue_manager: Arc<QueueManager>,
    event_bus: Option<Arc<EventBus>>,
    cache_service: Arc<CacheService>,
) -> EventAwareTypedChain<Response, Vec<()>> {
    let response_module_processor = ResponseModuleProcessor {
        task_manager: task_manager.clone(),
        state: state.clone(),
    };
    let response_parser_processor = ResponseParserProcessor {
        queue_manager,
        state,
        cache_service: cache_service.clone(),
        event_bus: event_bus.clone(),
    };
    let data_middleware_processor = DataMiddlewareProcessor {
        middleware_manager: middleware_manager.clone(),
    };
    let data_store_processor = DataStoreProcessor { middleware_manager };

    EventAwareTypedChain::<Response, Response>::new(event_bus)
        .then::<(Response, Arc<Module>, Option<LoginInfo>), _>(
            response_module_processor,
        )
        .then::<Vec<DataEvent>, _>(response_parser_processor)
        .then_map_vec_parallel_with_strategy_silent::<DataEvent, _>(
            data_middleware_processor,
            64,
            ErrorStrategy::Skip,
        )
        .then_map_vec_parallel_with_strategy_silent::<(), _>(
            data_store_processor,
            64,
            ErrorStrategy::Skip,
        )
}

#[cfg(test)]
mod tests {
    use super::{
        localize_parser_side_response_cache_entry, persist_parser_side_response_cache_entry,
    };
    use crate::cacheable::{CacheAble, CacheService};
    use crate::common::model::meta::MetaData;
    use crate::common::model::{ExecutionMark, Priority, Response};
    use crate::common::registry::NodeInfo;
    use crate::common::response_cache::{
        current_time_ms, RESPONSE_CACHE_EXPIRES_AT_KEY, RESPONSE_CACHE_OWNER_API_BASE_URL_KEY,
        RESPONSE_CACHE_OWNER_NAMESPACE_KEY, RESPONSE_CACHE_OWNER_NODE_ID_KEY,
    };
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    fn sample_response() -> Response {
        Response {
            id: Uuid::now_v7(),
            platform: "platform-a".to_string(),
            account: "account-a".to_string(),
            module: "catalog".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: b"ok".to_vec(),
            storage_path: None,
            headers: Vec::new(),
            task_retry_times: 0,
            metadata: MetaData::default(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: ExecutionMark::default(),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            request_hash: Some("cache-key-1".to_string()),
            priority: Priority::Normal,
        }
    }

    #[test]
    fn parser_side_cache_refresh_localizes_remote_owner_namespace() {
        let mut response = sample_response();
        response.metadata = response
            .metadata
            .add_trait_config("response_cache_owner_namespace", "download-pool");
        response.metadata = response
            .metadata
            .add_trait_config("response_cache_owner_node_id", "download-node-a");

        let localized = localize_parser_side_response_cache_entry(
            &response,
            "origin-app",
            Some("origin-node-1"),
        )
        .expect("request hash should produce cache entry");

        assert_eq!(
            localized
                .metadata
                .get_trait_config::<String>("response_cache_owner_namespace")
                .as_deref(),
            Some("origin-app")
        );
        assert_eq!(
            localized
                .metadata
                .get_trait_config::<String>("response_cache_owner_node_id")
                .as_deref(),
            Some("origin-node-1")
        );
    }

    #[test]
    fn parser_side_cache_refresh_keeps_local_or_legacy_cache_entries() {
        let mut response = sample_response();
        response.metadata = response
            .metadata
            .add_trait_config("response_cache_owner_namespace", "origin-app");
        let localized = localize_parser_side_response_cache_entry(&response, "origin-app", None)
            .expect("request hash should produce cache entry");
        assert_eq!(
            localized
                .metadata
                .get_trait_config::<String>("response_cache_owner_namespace")
                .as_deref(),
            Some("origin-app")
        );

        response.metadata = MetaData::default();
        let localized = localize_parser_side_response_cache_entry(&response, "origin-app", None)
            .expect("legacy entries should still localize");
        assert_eq!(
            localized
                .metadata
                .get_trait_config::<String>("response_cache_owner_namespace")
                .as_deref(),
            Some("origin-app")
        );
        assert_eq!(
            localized
                .metadata
                .get_trait_config::<String>("response_cache_owner_node_id"),
            None::<String>
        );
    }

    #[tokio::test]
    async fn parser_side_cache_refresh_records_local_owner_index() {
        let cache_service = Arc::new(CacheService::new(None, "parser-cache".to_string(), None, None));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        let mut response = sample_response();
        response.metadata = response
            .metadata
            .add_trait_config("response_cache_owner_namespace", "download-pool")
            .add_trait_config("response_cache_owner_node_id", "download-node-a");

        persist_parser_side_response_cache_entry(
            &response,
            "origin-app",
            Some("origin-node-1"),
            &cache_service,
            &profile_store,
        )
        .await;

        let cached = Response::sync("cache-key-1", &cache_service)
            .await
            .expect("cache sync should succeed")
            .expect("localized response should be cached");
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>("response_cache_owner_namespace")
                .as_deref(),
            Some("origin-app")
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner index should exist");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("origin-node-1"));
    }

    #[tokio::test]
    async fn parser_side_cache_refresh_preserves_existing_expiry_contract() {
        let cache_service = Arc::new(CacheService::new(
            None,
            "parser-cache".to_string(),
            Some(Duration::from_secs(600)),
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        let mut response = sample_response();
        let expires_at = current_time_ms() + 45_000;
        response.metadata = response
            .metadata
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "download-pool")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "download-node-a")
            .add_trait_config(RESPONSE_CACHE_EXPIRES_AT_KEY, expires_at);

        persist_parser_side_response_cache_entry(
            &response,
            "origin-app",
            Some("origin-node-1"),
            &cache_service,
            &profile_store,
        )
        .await;

        let cached = Response::sync("cache-key-1", &cache_service)
            .await
            .expect("cache sync should succeed")
            .expect("localized response should be cached");
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY),
            Some(expires_at)
        );

        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner index should exist");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("origin-node-1"));
        assert_eq!(owner.expires_at, Some(expires_at));
    }

    #[tokio::test]
    async fn parser_side_cache_refresh_records_local_owner_endpoint_from_heartbeat() {
        let cache_service = Arc::new(CacheService::new(None, "parser-cache".to_string(), None, None));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .heartbeat_node(NodeInfo {
                id: "origin-node-1".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "origin-host".to_string(),
                api_port: Some(18181),
                last_heartbeat: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                version: "test".to_string(),
            })
            .await
            .expect("node heartbeat should be recorded");

        let mut response = sample_response();
        response.metadata = response
            .metadata
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "download-pool")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "download-node-a");

        persist_parser_side_response_cache_entry(
            &response,
            "origin-app",
            Some("origin-node-1"),
            &cache_service,
            &profile_store,
        )
        .await;

        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner index should exist");
        assert_eq!(
            owner.owner_api_base_url.as_deref(),
            Some("http://127.0.0.1:18181")
        );

        let cached = Response::sync("cache-key-1", &cache_service)
            .await
            .expect("cache sync should succeed")
            .expect("localized response should be cached");
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_API_BASE_URL_KEY)
                .as_deref(),
            Some("http://127.0.0.1:18181")
        );
    }
}
