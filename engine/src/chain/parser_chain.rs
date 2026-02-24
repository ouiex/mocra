use common::status_tracker::ErrorDecision;
use crate::events::{
    DataMiddlewareEvent, DataStoreEvent, EventBus, EventEnvelope, EventPhase, EventType,
    ModuleGenerateEvent, ParserEvent,
};
use crate::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use async_trait::async_trait;
use errors::{DataMiddlewareError, Error, Result};
use crate::task::TaskManager;
use common::interface::middleware_manager::MiddlewareManager;
use common::model::data::Data;
use common::model::message::{ErrorTaskModel, TaskModel};
use common::model::{ModuleConfig, Response};
use common::state::State;

use log::{debug, error, info, warn};
use metrics::counter;
use queue::{QueueManager, QueuedItem};
use common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use common::processors::processor_chain::ErrorStrategy;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration, Instant};
use dashmap::DashMap;
use cacheable::{CacheAble, CacheService};
use crate::task::module::Module;
use futures::StreamExt;
use common::model::login_info::LoginInfo;
use serde_json::json;
use crate::chain::backpressure::{BackpressureSendState, send_with_backpressure};

/// Resolves response to module/config/login context before parsing.
pub struct ResponseModuleProcessor {
    task_manager: Arc<TaskManager>,
    cache_service: Arc<CacheService>,
    state: Arc<State>,
    config_cache: Arc<DashMap<String, (Arc<ModuleConfig>, Instant)>>,
}
#[async_trait]
impl ProcessorTrait<Response, (Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)> for ResponseModuleProcessor {
    fn name(&self) -> &'static str {
        "ResponseModuleProcessor"
    }

    async fn process(
        &self,
        input: Response,
        context: ProcessorContext,
    ) -> ProcessorResult<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)> {
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
            .should_task_continue(&input.task_id())
            .await
        {
            Ok(ErrorDecision::Continue) => {
                // [LOG_OPTIMIZATION] debug!("[ResponseModuleProcessor] task check passed: task_id={}", input.task_id());
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[ResponseModuleProcessor] task terminated before parsing: task_id={} reason={}",
                    input.task_id(),
                    reason
                );
                // Release lock on task termination path.
                self.state.status_tracker
                    .release_module_locker(&input.module_id())
                    .await;
                return ProcessorResult::FatalFailure(
                    errors::ModuleError::TaskMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[ResponseModuleProcessor] task error check failed, continue anyway: task_id={} error={}",
                    input.task_id(),
                    e
                );
            }
            _ => {}
        }

        // 2) Module-level threshold.
        match self
            .state
            .status_tracker
            .should_module_continue(&input.module_id())
            .await
        {
            Ok(ErrorDecision::Continue) => {
                // [LOG_OPTIMIZATION] debug!("[ResponseModuleProcessor] module check passed: module_id={}", input.module_id());
            }
            Ok(ErrorDecision::Terminate(reason)) => {
                error!(
                    "[ResponseModuleProcessor] module terminated before parsing: module_id={} reason={}",
                    input.module_id(),
                    reason
                );
                // Release lock on module termination path.
                self.state.status_tracker
                    .release_module_locker(&input.module_id())
                    .await;
                return ProcessorResult::FatalFailure(
                    errors::ModuleError::ModuleMaxError(reason.into()).into(),
                );
            }
            Err(e) => {
                warn!(
                    "[ResponseModuleProcessor] module error check failed, continue anyway: module_id={} error={}",
                    input.module_id(),
                    e
                );
            }
            _ => {}
        }

        let task: Result<(Arc<Module>, Option<LoginInfo>)> = self.task_manager.load_module_with_response(&input).await;
        match task {
            Ok((module, login_info)) => {
                // Prefer local short-lived config cache.
                let module_id = module.id();
                let cached_config = if let Some(entry) = self.config_cache.get(&module_id) {
                    let (cfg, expires_at) = entry.value();
                    if Instant::now() < *expires_at {
                        Some(cfg.clone())
                    } else {
                        None
                    }
                } else {
                    None
                };

                let config = if let Some(c) = cached_config {
                    c
                } else {
                    match ModuleConfig::sync(
                        &module_id,
                        &self.cache_service,
                    )
                        .await
                    {
                        Ok(Some(config)) => {
                            let config = Arc::new(config);
                            self.config_cache.insert(module_id, (config.clone(), Instant::now() + Duration::from_secs(10)));
                            config
                        },
                        _ => module.config.clone(),
                    }
                };
                
                // info!(
                //     "[ResponseModuleProcessor] module loaded: module_name={} module_id={}",
                //     module.module.name(),
                //     module.id()
                // );
                ProcessorResult::Success((input, module, config, login_info))
            }
            Err(e) => {
                warn!("[ResponseModuleProcessor] load_with_response failed, will retry: err={e}");
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
                input.module_id()
            );
            self.state.status_tracker.lock_module(&input.module_id()).await;
        }
        Ok(())
    }
}
#[async_trait]
impl EventProcessorTrait<Response, (Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)> for ResponseModuleProcessor {
    fn pre_status(&self, input: &Response) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::ModuleGenerate,
            EventPhase::Started,
            ModuleGenerateEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &Response, _output: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)) -> Option<EventEnvelope> {
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
    async fn emit_semantic_event(
        &self,
        event_type: EventType,
        payload: serde_json::Value,
    ) {
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
        tx: tokio::sync::mpsc::Sender<QueuedItem<T>>,
        item: QueuedItem<T>,
        queue_kind: &'static str,
        log_context: &str,
    ) -> bool {
        match send_with_backpressure(&tx, item).await {
            Ok(BackpressureSendState::Direct) => true,
            Ok(BackpressureSendState::RecoveredFromFull) => {
                counter!("parser_chain_backpressure_total", "queue" => queue_kind, "reason" => "queue_full").increment(1);
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
                    counter!("parser_chain_backpressure_total", "queue" => queue_kind, "reason" => "queue_full").increment(1);
                    warn!(
                        "[ResponseParserProcessor] queue full before close: queue={} context={} remaining_capacity={}",
                        queue_kind,
                        log_context,
                        tx.capacity()
                    );
                }
                counter!("parser_chain_backpressure_total", "queue" => queue_kind, "reason" => "queue_closed").increment(1);
                error!(
                    "[ResponseParserProcessor] queue closed before send: queue={} context={}",
                    queue_kind,
                    log_context
                );
                false
            }
        }
    }
}

#[async_trait]
impl ProcessorTrait<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), Vec<Data>> for ResponseParserProcessor {
    fn name(&self) -> &'static str {
        "ResponseParserProcessor"
    }

    async fn process(
        &self,
        input: (Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
        context: ProcessorContext,
    ) -> ProcessorResult<Vec<Data>> {
        info!(
            "[ResponseParserProcessor] start parse: request_id={} module_id={}",
            input.0.id,
            input.0.module_id()
        );
        let module = input.1.clone();
        let config = input.2.clone();
        let login_info = input.3.clone();

        // StateHandle has been removed; SyncService is used internally by ModuleProcessor.
        let task_id = input.0.task_id();
        let module_id = input.0.module_id();
        let request_id = input.0.id.to_string();

        let data = module.parser(input.0.clone(), Some(config)).await;
        let mut data = match data {
            Ok(d) => {
                let has_next_task = d.parser_task.is_some();
                let has_error = d.error_task.is_some();
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
                warn!("[ResponseParserProcessor] parser error: err={e}");

                // Record parser error and retrieve threshold decision.
                match self
                    .state
                    .status_tracker
                    .record_parse_error(&task_id, &module_id, &request_id, &e)
                    .await
                {
                    Ok(ErrorDecision::Continue)
                    | Ok(ErrorDecision::RetryAfter(_)) => {
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

           if let Some(task) = data.parser_task.take() {
               let task_account = task.account_task.account.clone();
               let task_platform = task.account_task.platform.clone();
               let task_run_id = task.run_id;
               let task_module_id = task.context.module_id.clone();
               let task_step_idx = task.context.step_idx;
               let task_prefix_request = task.prefix_request;
             // OPTIMIZATION: Check if we can bypass the queue and generate requests locally
             let target_module_name = task.account_task.module.as_ref().and_then(|v| v.first());
             let is_same_module = target_module_name.map_or(true, |name| name == module.module.name().as_str());
             
             // Check if account/platform match (usually yes for parser tasks)
             let is_same_context = task.account_task.account == module.account.name && task.account_task.platform == module.platform.name;

             if is_same_module && is_same_context {
                 debug!("[ResponseParserProcessor] Optimizing: Generating requests locally for same module");
                 
                 // Create a shallow clone of the module to inject context
                 let mut module_clone = (*module).clone(); 
                 module_clone.pending_ctx = Some(task.context.clone());
                 module_clone.prefix_request = task.prefix_request;
                 module_clone.run_id = task.run_id;

                 let task_meta = task.metadata.as_object().cloned().unwrap_or_default();
                 
                 match module_clone.generate(task_meta, login_info).await {
                     Ok(mut stream) => {
                         let request_queue = self.queue_manager.get_request_push_channel();
                         let mut generated_count = 0;
                         while let Some(req) = stream.next().await {
                               let req_id = req.id.to_string();
                               let req_module_id = req.module_id();
                               let context = format!(
                                   "generated_request request_id={} module_id={}",
                                   req_id,
                                   req_module_id
                               );
                               if self
                                   .send_with_backpressure(
                                       request_queue.clone(),
                                       QueuedItem::new(req),
                                       "request",
                                       &context,
                                   )
                                   .await
                               {
                                   generated_count += 1;
                               }
                         }
                         info!("[ResponseParserProcessor] Locally generated {} requests", generated_count);
                         self.emit_semantic_event(
                             EventType::ParserTaskProduced,
                             json!({
                                 "account": task_account,
                                 "platform": task_platform,
                                 "run_id": task_run_id,
                                 "module_id": task_module_id,
                                 "step_idx": task_step_idx,
                                 "prefix_request": task_prefix_request,
                                 "path": "local_generate"
                             }),
                         ).await;
                         self.emit_semantic_event(
                             EventType::ModuleStepAdvanced,
                             json!({
                                 "account": task_account,
                                 "platform": task_platform,
                                 "run_id": task_run_id,
                                 "module_id": task_module_id,
                                 "step_idx": task_step_idx,
                                 "prefix_request": task_prefix_request,
                                 "generated_count": generated_count,
                                 "mode": "local_generate"
                             }),
                         ).await;
                         // Successfully handled, do NOT enqueue ParserTaskModel
                     },
                     Err(e) => {
                         error!("[ResponseParserProcessor] Failed to generate requests locally: {}, falling back to queue", e);
                         // Fallback: put the task back into parser_task queue
                         let queue = self.queue_manager.get_parser_task_push_channel();
                         let context = format!(
                             "parser_task_fallback account={} platform={} run_id={} module_id={}",
                             task.account_task.account,
                             task.account_task.platform,
                             task.run_id,
                             task.context.module_id.as_deref().unwrap_or("unknown")
                         );
                         if !self
                             .send_with_backpressure(
                                 queue,
                                 QueuedItem::new(task),
                                 "parser_task",
                                 &context,
                             )
                             .await
                         {
                             error!("[ResponseParserProcessor] failed to send parser task fallback: {}", context);
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
                                     "path": "fallback_queue"
                                 }),
                             ).await;
                             self.emit_semantic_event(
                                 EventType::ModuleStepFallback,
                                 json!({
                                     "account": task_account,
                                     "platform": task_platform,
                                     "run_id": task_run_id,
                                     "module_id": task_module_id,
                                     "step_idx": task_step_idx,
                                     "prefix_request": task_prefix_request,
                                     "reason": "local_generate_failed",
                                     "error": e.to_string(),
                                     "path": "parser_task_queue"
                                 }),
                             ).await;
                         }
                     }
                 }
             } else {
                // Different module/context, enqueue as usual
                let queue = self.queue_manager.get_parser_task_push_channel();
                let context = format!(
                    "parser_task account={} platform={} run_id={} module_id={}",
                    task.account_task.account,
                    task.account_task.platform,
                    task.run_id,
                    task.context.module_id.as_deref().unwrap_or("unknown")
                );
                if !self
                    .send_with_backpressure(
                        queue,
                        QueuedItem::new(task),
                        "parser_task",
                        &context,
                    )
                    .await
                {
                     error!("[ResponseParserProcessor] failed to send parser task: {}", context);
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
                    ).await;
                }
             }
        }
        
        if let Some(mut msg) = data.error_task {
            warn!(
                "[ResponseParserProcessor] recorded response error for request_id={}, message={}",
                input.0.id, msg.error_msg
            );
            msg.prefix_request = input.0.prefix_request;
            msg.run_id = input.0.run_id;
            let queue = self.queue_manager.get_error_push_channel();
            let context = format!(
                "error_task request_id={} module_id={}",
                input.0.id,
                input.0.module_id()
            );
            if !self
                .send_with_backpressure(queue, QueuedItem::new(msg), "error", &context)
                .await
            {
                error!("[ResponseParserProcessor] failed to send parser error: {}", context);
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
                ).await;
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
        input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
        _output: &Vec<Data>,
        _context: &ProcessorContext,
    ) -> Result<()> {
        // Centralized lock release after parsing to avoid deadlocks and duplicate unlocks.

        let config = self.state.config.read().await;

        // Cache response payload so duplicate downloads can be short-circuited.
        if config.download_config.enable_cache {
            if let Some(request_hash) = &input.0.request_hash
            {
                input.0.send(request_hash, &self.cache_service).await.ok();
            }
        }

        if config.download_config.enable_locker {
            self.state.status_tracker
                .release_module_locker(&input.0.module_id())
                .await;
            debug!(
                "[ResponseParserProcessor] released module lock after parsing: module_id={}",
                input.0.module_id()
            );
        }
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<Vec<Data>> {
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
        let error_task = ErrorTaskModel {
            id: input.0.id,
            account_task: TaskModel {
                account: input.0.account.clone(),
                platform: input.0.platform.clone(),
                module: Some(vec![input.0.module.clone()]),
                run_id: input.0.run_id,
                priority: input.0.priority,
            },
            error_msg: error.to_string(),
            timestamp,
            metadata: input.0.metadata.clone().into(),
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
        if !self
            .send_with_backpressure(queue, QueuedItem::new(error_task), "error", &context)
            .await
        {
            error!("[ResponseParserProcessor] failed to enqueue ErrorTaskModel: {}", context);
        }

        // Release module lock after enqueueing error task.
        if self.state.config.read().await.download_config.enable_locker {
           self.state
               .status_tracker
               .release_module_locker(&input.0.module_id())
               .await;
        }

        ProcessorResult::FatalFailure(error)
    }
}
impl EventProcessorTrait<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), Vec<Data>> for ResponseParserProcessor {
    fn pre_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::Parser,
            EventPhase::Started,
            ParserEvent::from(&input.0),
        ))
    }

    fn finish_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), _output: &Vec<Data>) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::Parser,
            EventPhase::Completed,
            ParserEvent::from(&input.0),
        ))
    }

    fn working_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>)) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::Parser,
            EventPhase::Started,
            ParserEvent::from(&input.0),
        ))
    }

    fn error_status(&self, input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::Parser,
            EventPhase::Failed,
            ParserEvent::from(&input.0),
            err,
        ))
    }

    fn retry_status(
        &self,
        input: &(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>),
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
impl ProcessorTrait<Data, Data> for DataMiddlewareProcessor {
    fn name(&self) -> &'static str {
        "DataMiddlewareProcessor"
    }

    async fn process(&self, input: Data, context: ProcessorContext) -> ProcessorResult<Data> {
        // info!(
        //    "[DataMiddlewareProcessor] start: account={} platform={} size={}",
        //    input.account,
        //    input.platform,
        //    input.size()
        // );
        let start = std::time::Instant::now();
        let config = context
            .metadata
            .read()
            .await
            .get("config")
            .map(|x| {
                serde_json::from_value::<ModuleConfig>(x.clone()).unwrap_or_else(|e| {
                    warn!(
                        "[DataMiddlewareProcessor] config conversion failed, using default: request_id={} module={} error={}",
                        input.request_id,
                        input.module,
                        e
                    );
                    ModuleConfig::default()
                })
            });
        let modified_data = self.middleware_manager.handle_data(input, &config).await;
        if start.elapsed().as_millis() > 10 {
            info!("[DataMiddlewareProcessor] SLOW middleware execution: {} ms", start.elapsed().as_millis());
        }
        match modified_data {
            Some(data) => ProcessorResult::Success(data),
            None => ProcessorResult::FatalFailure(
                DataMiddlewareError::EmptyData("data dropped by data middleware".into()).into(),
            ),
        }
    }
}
impl EventProcessorTrait<Data, Data> for DataMiddlewareProcessor {
    fn pre_status(&self, input: &Data) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::MiddlewareBefore,
            EventPhase::Started,
            DataMiddlewareEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &Data, output: &Data) -> Option<EventEnvelope> {
        let mut event: DataMiddlewareEvent = input.into();
        event.after_size = output.size().into();
        Some(EventEnvelope::engine(
            EventType::MiddlewareBefore,
            EventPhase::Completed,
            event,
        ))
    }

    fn working_status(&self, input: &Data) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::MiddlewareBefore,
            EventPhase::Started,
            DataMiddlewareEvent::from(input),
        ))
    }

    fn error_status(&self, input: &Data, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::MiddlewareBefore,
            EventPhase::Failed,
            DataMiddlewareEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &Data, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
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
impl ProcessorTrait<Data, ()> for DataStoreProcessor {
    fn name(&self) -> &'static str {
        "DataStoreProcessor"
    }
    /// This stage uses `RetryableFailure` to trigger retries.
    ///
    /// Additional retry context can be passed through `retry_policy.meta`
    /// for downstream retry behavior customization.
    async fn process(&self, input: Data, context: ProcessorContext) -> ProcessorResult<()> {
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
                && let Some(m) = m_val.as_array() {
                    middleware = m.iter()
                        .filter_map(|x| x.as_str())
                        .map(|x| x.to_string())
                        .collect::<Vec<String>>();
                }
        let config = context
            .metadata
            .read()
            .await
            .get("config")
            .map(|x| {
                serde_json::from_value::<ModuleConfig>(x.clone()).unwrap_or_else(|e| {
                    warn!(
                        "[DataStoreProcessor] config conversion failed, using default: request_id={} module={} error={}",
                        input.request_id,
                        input.module,
                        e
                    );
                    ModuleConfig::default()
                })
            });
        let request_id = input.request_id;
        let res = if middleware.is_empty() {
            self.middleware_manager
                .handle_store_data(input, &config)
                .await
        } else {
            self.middleware_manager
                .handle_store_data_with_middleware(input, middleware, &config)
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
impl EventProcessorTrait<Data, ()> for DataStoreProcessor {
    fn pre_status(&self, input: &Data) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::DataStore,
            EventPhase::Started,
            DataStoreEvent::from(input),
        ))
    }

    fn finish_status(&self, input: &Data, _output: &()) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::DataStore,
            EventPhase::Completed,
            DataStoreEvent::from(input),
        ))
    }

    fn working_status(&self, input: &Data) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::DataStore,
            EventPhase::Started,
            DataStoreEvent::from(input),
        ))
    }

    fn error_status(&self, input: &Data, err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::DataStore,
            EventPhase::Failed,
            DataStoreEvent::from(input),
            err,
        ))
    }

    fn retry_status(&self, input: &Data, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
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
        task_manager,
        cache_service: cache_service.clone(),
        state: state.clone(),
        config_cache: Arc::new(DashMap::new()),
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
        .then::<(Response, Arc<Module>, Arc<ModuleConfig>, Option<LoginInfo>), _>(response_module_processor)
        .then::<Vec<Data>, _>(response_parser_processor)
        .then_map_vec_parallel_with_strategy_silent::<Data, _>(
            data_middleware_processor,
            64,
            ErrorStrategy::Skip,
        )
        .then_map_vec_parallel_with_strategy_silent::<(), _>(data_store_processor, 64, ErrorStrategy::Skip)
}
