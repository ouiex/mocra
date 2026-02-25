use crate::queue::QueuedItem;
use crate::engine::chain::ConfigProcessor;
use crate::engine::events::{
    DownloadEvent, EventBus, EventEnvelope, EventPhase, EventType, RequestMiddlewareEvent,
    ResponseEvent,
};
use crate::engine::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use async_trait::async_trait;
use crate::downloader::DownloaderManager;
use crate::errors::{Error, ModuleError, Result};
use crate::common::interface::middleware_manager::MiddlewareManager;
use crate::common::model::ModuleConfig;
use crate::common::model::download_config::DownloadConfig;
use crate::common::model::{Request, Response};
use crate::common::state::State;
use log::{debug, warn, error};
use metrics::counter;
use crate::queue::QueueManager;
use crate::common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use crate::proxy::ProxyManager;
use std::sync::Arc;
use dashmap::DashMap;
use std::time::{Instant, Duration};
use crate::common::status_tracker::ErrorDecision;
use serde_json::json;
use crate::engine::chain::backpressure::{BackpressureSendState, send_with_backpressure};

/// Download-stage processor.
///
/// It performs defensive threshold checks, selects a downloader implementation,
/// executes the request, and maps outcomes into chain semantics.
pub struct DownloadProcessor {
    pub(crate) downloader_manager: Arc<DownloaderManager>,
    pub(crate) state: Arc<State>,
    pub(crate) decision_cache: Arc<DashMap<String, (Instant, ErrorDecision)>>,
}

#[async_trait]
impl ProcessorTrait<(Option<Request>, Option<ModuleConfig>), (Option<Response>, Option<ModuleConfig>)>
for DownloadProcessor
{
    fn name(&self) -> &'static str {
        "DownloadProcessor"
    }

    async fn process(
        &self,
        input: (Option<Request>, Option<ModuleConfig>),
        context: ProcessorContext,
    ) -> ProcessorResult<(Option<Response>, Option<ModuleConfig>)> {
        let request = match input.0 {
            Some(request) => request,
            None => return ProcessorResult::Success((None, input.1)),
        };
        debug!(
            "[DownloadProcessor] begin process: request_id={} module_id={} task_id={} retry_count={}",
            request.id,
            request.module_id(),
            request.task_id(),
            context.retry_policy.as_ref().map(|r| r.current_retry).unwrap_or(0)
        );

        let is_retry = context.retry_policy.as_ref().map(|r| r.current_retry > 0).unwrap_or(false);

        if !is_retry {
            // Defensive checks in distributed mode: ingress and download may run on different nodes.

            // 1) Task-level threshold check with short local cache.
            let task_id = request.task_id();
            // Try cached decision first.
            let cached_task_decision = {
                if let Some(entry) = self.decision_cache.get(&task_id) {
                    let (ts, decision) = entry.value();
                    // 1s TTL to avoid hot-path Redis amplification.
                    if ts.elapsed() < Duration::from_secs(1) {
                        Some(Ok(decision.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            let task_decision_result = match cached_task_decision {
                Some(res) => res,
                None => {
                    let res = self.state.status_tracker.should_task_continue(&task_id).await;
                    if let Ok(ref d) = res {
                        self.decision_cache.insert(task_id.clone(), (Instant::now(), d.clone()));
                    }
                    res
                }
            };

            match task_decision_result {
                Ok(ErrorDecision::Continue) => {
                    // [LOG_OPTIMIZATION] debug!("[DownloadProcessor] task check passed: task_id={}", input.0.task_id());
                }
                Ok(ErrorDecision::Terminate(reason)) => {
                    error!(
                        "[DownloadProcessor] task terminated before download: task_id={} reason={}",
                        request.task_id(),
                        reason
                    );
                    return ProcessorResult::FatalFailure(
                        ModuleError::TaskMaxError(reason.into()).into(),
                    );
                }
                Err(e) => {
                    warn!(
                        "[DownloadProcessor] task error check failed, continue anyway: task_id={} error={}",
                        request.task_id(),
                        e
                    );
                }
                _ => {}
            }

            // 2) Module-level threshold check with short local cache.
            let module_id = request.module_id();
            // Try cached decision first.
            let cached_decision = {
                if let Some(entry) = self.decision_cache.get(&module_id) {
                    let (ts, decision) = entry.value();
                    // 1s TTL to cap status-check pressure.
                    if ts.elapsed() < Duration::from_secs(1) {
                        Some(Ok(decision.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            // On miss/expiry, fetch fresh status and refresh local cache.
            let decision_result = match cached_decision {
                Some(res) => res,
                None => {
                    let res = self.state.status_tracker.should_module_continue(&module_id).await;
                    if let Ok(ref d) = res {
                        self.decision_cache.insert(module_id.clone(), (Instant::now(), d.clone()));
                    }
                    res
                }
            };

            match decision_result {
                Ok(ErrorDecision::Continue) => {
                    // [LOG_OPTIMIZATION] debug!("[DownloadProcessor] module check passed: module_id={}", input.0.module_id());
                }
                Ok(ErrorDecision::Terminate(reason)) => {
                    error!(
                        "[DownloadProcessor] module terminated before download: module_id={} reason={}",
                        request.module_id(),
                        reason
                    );
                    // Module terminated: release lock and skip this request.
                    self.state
                        .status_tracker
                        .release_module_locker(&request.module_id())
                        .await;

                    // Return success with None to keep stream progressing.
                    return ProcessorResult::Success((None, input.1));
                }
                Err(e) => {
                    warn!(
                        "[DownloadProcessor] module error check failed, continue anyway: module_id={} error={}",
                        request.module_id(),
                        e
                    );
                }
                _ => {}
            }
        } else {
            // [LOG_OPTIMIZATION] debug!("[DownloadProcessor] skipping task/module checks for retry: request_id={}", input.0.id);
        }

        let download_config =
            DownloadConfig::load(&input.1, &self.state.config.read().await.download_config);
        let downloader = self
            .downloader_manager
            .get_downloader(&request, download_config)
            .await;
        debug!("[DownloadProcessor] acquired downloader, start download: request_id={}", request.id);

        let module_id = request.module_id();
        let task_id = request.task_id();
        let request_id = request.id;

        match downloader.download(request).await {
            Ok(response) => {
                // [LOG_OPTIMIZATION]
                // debug!(
                //     "[DownloadProcessor] download success: status={} content_len={} module_id={}",
                //     response.status_code,
                //     response.content.len(),
                //     response.module_id()
                // );
                let content_len = response.content.len();
                debug!(
                    "[DownloadProcessor] download finished: request_id={} status={} len={} module_id={}",
                    request_id,
                    response.status_code,
                    content_len,
                    module_id
                );

                // Record request-local success.
                let state_clone = self.state.clone();
                let request_id_clone = request_id.to_string();
                tokio::spawn(async move {
                    state_clone
                        .status_tracker
                        .record_download_success(&request_id_clone)
                        .await
                        .ok();
                });

                ProcessorResult::Success((Some(response), input.1))
            }
            Err(e) => {
                // 1) Local retry first.
                let retry_policy = context.retry_policy.clone().unwrap_or_default();
                if retry_policy.should_retry() {
                    debug!(
                        "[DownloadProcessor] download failed, will retry locally: request_id={} retry={}/{} reason={}",
                        request_id, retry_policy.current_retry, retry_policy.max_retries, e
                    );
                    return ProcessorResult::RetryableFailure(
                        retry_policy.with_reason(e.to_string())
                    );
                }

                warn!(
                    "[DownloadProcessor] download failed after max retries: module_id={} request_id={} reason={}",
                    module_id,
                    request_id,
                    e
                );

                // 2) Retries exhausted; record error and follow tracker decision.
                match self.state
                    .status_tracker
                    .record_download_error(&task_id, &module_id, &request_id.to_string(), &e)
                    .await
                {
                    Ok(ErrorDecision::Terminate(reason)) => {
                        error!("[DownloadProcessor] terminate: {}", reason);
                        ProcessorResult::FatalFailure(
                            ModuleError::ModuleMaxError(reason.into()).into()
                        )
                    }
                    // Continue/RetryAfter/Skip all map to dropping current request here.
                    Ok(_) => {
                        warn!("[DownloadProcessor] skip request after max retries (recorded in tracker): request_id={}", request_id);
                        ProcessorResult::Success((None, input.1))
                    }
                    Err(err) => {
                        error!("[DownloadProcessor] error tracker failed: {}", err);
                        // Tracker failure: conservatively drop this request.
                        ProcessorResult::Success((None, input.1))
                    }
                }
            }
        }
    }
    async fn handle_error(
        &self,
        _input: &(Option<Request>, Option<ModuleConfig>),
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<(Option<Response>, Option<ModuleConfig>)> {
        let request = match &_input.0 {
            Some(request) => request,
            None => return ProcessorResult::Success((None, _input.1.clone())),
        };

        error!(
            "[DownloadProcessor] handle_error: request_id={} module_id={} error={}",
            request.id,
            request.module_id(),
            _error
        );

        // Error is already tracked in `process()` via `error_tracker`.
        // Only release lock and return `None` here.

        // Download failed terminally in this chain; no parser stage will release the lock.
        // Ensure we release the module lock to avoid stale locks.
        self.state
            .status_tracker
            .release_module_locker(&request.module_id())
            .await;
        ProcessorResult::Success((None, _input.1.clone()))
    }
}

#[async_trait]
impl EventProcessorTrait<(Option<Request>, Option<ModuleConfig>), (Option<Response>, Option<ModuleConfig>)>
for DownloadProcessor
{
    fn pre_status(&self, input: &(Option<Request>, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let ev: DownloadEvent = request.into();
                Some(EventEnvelope::engine(EventType::Download, EventPhase::Started, ev))
            }
            None => Some(EventEnvelope::system_error(
                "download_skipped_without_request",
                EventPhase::Completed,
            )),
        }
    }

    fn finish_status(
        &self,
        input: &(Option<Request>, Option<ModuleConfig>),
        out: &(Option<Response>, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let mut ev: DownloadEvent = request.into();
                if let Some(resp) = &out.0 {
                    ev.status_code = Some(resp.status_code);
                }
                Some(EventEnvelope::engine(EventType::Download, EventPhase::Completed, ev))
            }
            None => Some(EventEnvelope::system_error(
                "download_skipped_without_request",
                EventPhase::Completed,
            )),
        }
    }

    fn working_status(&self, input: &(Option<Request>, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let ev: DownloadEvent = request.into();
                Some(EventEnvelope::engine(EventType::Download, EventPhase::Started, ev))
            }
            None => Some(EventEnvelope::system_error(
                "download_skipped_without_request",
                EventPhase::Completed,
            )),
        }
    }

    fn error_status(&self, input: &(Option<Request>, Option<ModuleConfig>), err: &Error) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let ev: DownloadEvent = request.into();
                Some(EventEnvelope::engine_error(
                    EventType::Download,
                    EventPhase::Failed,
                    ev,
                    err,
                ))
            }
            None => Some(EventEnvelope::system_error(
                format!("download_skipped_with_error: {err}"),
                EventPhase::Failed,
            )),
        }
    }

    fn retry_status(
        &self,
        input: &(Option<Request>, Option<ModuleConfig>),
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let ev: DownloadEvent = request.into();
                Some(EventEnvelope::engine(
                    EventType::Download,
                    EventPhase::Retry,
                    json!({
                        "data": ev,
                        "retry_count": retry_policy.current_retry,
                        "reason": retry_policy.reason.clone().unwrap_or_default(),
                    }),
                ))
            }
            None => Some(EventEnvelope::system_error(
                "download_skipped_retry_without_request",
                EventPhase::Completed,
            )),
        }
    }
}

pub struct ResponsePublishProcessor {
    pub(crate) queue_manager: Arc<QueueManager>,
    pub(crate) state:Arc<State>,
}

#[async_trait]
impl ProcessorTrait<Option<Response>, ()> for ResponsePublishProcessor {
    fn name(&self) -> &'static str {
        "ResponsePublish"
    }

    async fn process(
        &self,
        input: Option<Response>,
        context: ProcessorContext,
    ) -> ProcessorResult<()> {
        let input = match input {
            Some(resp) => resp,
            None => return ProcessorResult::Success(()),
        };
        let id = input.id.to_string();
        debug!(
            "[ResponsePublish] publishing response: request_id={} module_id={}",
            input.id,
            input.module_id()
        );
        let backpressure_retry_delay_ms = {
            let cfg = self.state.config.read().await;
            cfg.crawler.backpressure_retry_delay_ms
        };
        // [LOG_OPTIMIZATION] debug!("[ResponsePublish] start queue send: request_id={}", id);
        let item = QueuedItem::new(input);
        
        // OPTIMIZATION: Try local channel first to avoid serialization overhead
        // If local channel is full or closed, fall back to the configured backend (Redis/Kafka)
        let result = self.queue_manager.try_send_local_response(item);
        
        if let Err(e) = match result {
            Ok(_) => {
                debug!("[ResponsePublish] Sent response locally: request_id={}", id);
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(returned_item)) => {
                counter!("download_response_backpressure_total", "queue" => "local_response", "reason" => "queue_full").increment(1);
                warn!(
                    "[ResponsePublish] local response queue full, fallback to backend channel: request_id={}",
                    id
                );
                let tx = self.queue_manager.get_response_push_channel();
                match send_with_backpressure(&tx, returned_item).await {
                    Ok(BackpressureSendState::Direct) => Ok(()),
                    Ok(BackpressureSendState::RecoveredFromFull) => {
                        counter!("download_response_backpressure_total", "queue" => "response", "reason" => "queue_full").increment(1);
                        warn!(
                            "[ResponsePublish] backend response queue full, waiting send: request_id={} remaining_capacity={}",
                            id,
                            tx.capacity()
                        );
                        Ok(())
                    }
                    Err(err) => {
                        if err.after_full {
                            counter!("download_response_backpressure_total", "queue" => "response", "reason" => "queue_full").increment(1);
                            warn!(
                                "[ResponsePublish] backend response queue full before close: request_id={} remaining_capacity={}",
                                id,
                                tx.capacity()
                            );
                        }
                        counter!("download_response_backpressure_total", "queue" => "response", "reason" => "queue_closed").increment(1);
                        Err("response queue closed".to_string())
                    }
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(returned_item)) => {
                counter!("download_response_backpressure_total", "queue" => "local_response", "reason" => "queue_closed").increment(1);
                warn!(
                    "[ResponsePublish] local response queue closed, fallback to backend channel: request_id={}",
                    id
                );
                let tx = self.queue_manager.get_response_push_channel();
                tx.send(returned_item).await.map_err(|e| e.to_string())
            }
        } {
            error!("Failed to send response to queue: {e}");
            warn!("[ResponsePublish] will retry due to queue send error");
            let mut retry_policy = context.retry_policy.unwrap_or_default();
            if let Some(delay_ms) = backpressure_retry_delay_ms {
                retry_policy.retry_delay = delay_ms.max(1);
            }
            retry_policy.reason = Some(e);
            return ProcessorResult::RetryableFailure(
                retry_policy,
            );
        }
        debug!("[ResponsePublish] end queue send: request_id={}", id);
        // [LOG_OPTIMIZATION] debug!("[ResponsePublish] end queue send: request_id={}", id);
        ProcessorResult::Success(())
    }
    async fn pre_process(
        &self,
        _input: &Option<Response>,
        _context: &ProcessorContext,
    ) -> Result<()> {
        if let Some(resp) = _input {
            // [LOG_OPTIMIZATION]
            // debug!(
            //     "[ResponsePublish] lock module before publish: module_id={} request_id={}",
            //     resp.module_id(),
            //     resp.id
            // );
            self.state.status_tracker.lock_module(&resp.module_id()).await;
            // [LOG_OPTIMIZATION]
            // debug!(
            //     "[ResponsePublish] lock module acquired: module_id={} request_id={}",
            //     resp.module_id(),
            //     resp.id
            // );
        }
        Ok(())
    }
    async fn handle_error(
        &self,
        input: &Option<Response>,
        error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<()> {
        if let Some(resp) = input {
            // Ensure we release the lock if publishing the response ultimately fails
            self.state.status_tracker
                .release_module_locker(&resp.module_id())
                .await;

            // Response publish failures are queue-transport issues,
            // not crawler business-logic failures.

            error!(
                "[ResponsePublish] fatal error publishing response: request_id={} module_id={} error={}",
                resp.id,
                resp.module_id(),
                error
            );
        }
        ProcessorResult::FatalFailure(error)
    }
}
#[async_trait]
impl EventProcessorTrait<Option<Response>, ()> for ResponsePublishProcessor {
    fn pre_status(&self, input: &Option<Response>) -> Option<EventEnvelope> {
        match input {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponsePublish,
                EventPhase::Started,
                ResponseEvent::from(resp),
            )),
            None => Some(EventEnvelope::system_error(
                "no_response_to_publish",
                EventPhase::Completed,
            )),
        }
    }

    fn finish_status(&self, input: &Option<Response>, _out: &()) -> Option<EventEnvelope> {
        match input {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponsePublish,
                EventPhase::Completed,
                ResponseEvent::from(resp),
            )),
            None => Some(EventEnvelope::system_error(
                "no_response_to_publish",
                EventPhase::Completed,
            )),
        }
    }

    fn working_status(&self, input: &Option<Response>) -> Option<EventEnvelope> {
        match input {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponsePublish,
                EventPhase::Started,
                ResponseEvent::from(resp),
            )),
            None => Some(EventEnvelope::system_error(
                "no_response_to_publish",
                EventPhase::Completed,
            )),
        }
    }

    fn error_status(&self, input: &Option<Response>, err: &Error) -> Option<EventEnvelope> {
        match input {
            Some(resp) => Some(EventEnvelope::engine_error(
                EventType::ResponsePublish,
                EventPhase::Failed,
                ResponseEvent::from(resp),
                err,
            )),
            None => Some(EventEnvelope::system_error(
                format!("response_publish_error_without_response: {err}"),
                EventPhase::Failed,
            )),
        }
    }

    fn retry_status(&self, input: &Option<Response>, retry_policy: &RetryPolicy) -> Option<EventEnvelope> {
        match input {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponsePublish,
                EventPhase::Retry,
                json!({
                    "data": ResponseEvent::from(resp),
                    "retry_count": retry_policy.current_retry,
                    "reason": retry_policy.reason.clone().unwrap_or_default(),
                }),
            )),
            None => Some(EventEnvelope::system_error(
                "response_publish_retry_without_response",
                EventPhase::Completed,
            )),
        }
    }
}
pub struct RequestMiddlewareProcessor {
    pub(crate) middleware_manager: Arc<MiddlewareManager>,
}
#[async_trait]
impl ProcessorTrait<(Request, Option<ModuleConfig>), (Option<Request>, Option<ModuleConfig>)>
for RequestMiddlewareProcessor
{
    fn name(&self) -> &'static str {
        "DownloadMiddlewareProcessor"
    }

    async fn process(
        &self,
        input: (Request, Option<ModuleConfig>),
        _context: ProcessorContext,
    ) -> ProcessorResult<(Option<Request>, Option<ModuleConfig>)> {
        debug!(
            "[RequestMiddleware] handling request middleware: request_id={} module_id={}",
            input.0.id,
            input.0.module_id()
        );
        let modified_request = self
            .middleware_manager
            .handle_request(input.0, &input.1)
            .await;
        ProcessorResult::Success((modified_request, input.1))
    }
}
#[async_trait]
impl EventProcessorTrait<(Request, Option<ModuleConfig>), (Option<Request>, Option<ModuleConfig>)>
for RequestMiddlewareProcessor
{
    fn pre_status(&self, input: &(Request, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestMiddleware,
            EventPhase::Started,
            RequestMiddlewareEvent::from(&input.0),
        ))
    }

    fn finish_status(
        &self,
        _input: &(Request, Option<ModuleConfig>),
        out: &(Option<Request>, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        match &out.0 {
            Some(request) => Some(EventEnvelope::engine(
                EventType::RequestMiddleware,
                EventPhase::Completed,
                RequestMiddlewareEvent::from(request),
            )),
            None => Some(EventEnvelope::system_error(
                "request_skipped_by_middleware",
                EventPhase::Completed,
            )),
        }
    }

    fn working_status(&self, input: &(Request, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestMiddleware,
            EventPhase::Started,
            RequestMiddlewareEvent::from(&input.0),
        ))
    }

    fn error_status(&self, input: &(Request, Option<ModuleConfig>), err: &Error) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine_error(
            EventType::RequestMiddleware,
            EventPhase::Failed,
            RequestMiddlewareEvent::from(&input.0),
            err,
        ))
    }

    fn retry_status(
        &self,
        input: &(Request, Option<ModuleConfig>),
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestMiddleware,
            EventPhase::Retry,
            json!({
                "data": RequestMiddlewareEvent::from(&input.0),
                "retry_count": retry_policy.current_retry,
                "reason": retry_policy.reason.clone().unwrap_or_default(),
            }),
        ))
    }
}
pub struct ResponseMiddlewareProcessor {
    pub(crate) middleware_manager: Arc<MiddlewareManager>,
}
#[async_trait]
impl ProcessorTrait<(Option<Response>, Option<ModuleConfig>), Option<Response>>
for ResponseMiddlewareProcessor
{
    fn name(&self) -> &'static str {
        "DownloadMiddlewareProcessor"
    }

    async fn process(
        &self,
        input: (Option<Response>, Option<ModuleConfig>),
        _context: ProcessorContext,
    ) -> ProcessorResult<Option<Response>> {
        let response = match input.0 {
            Some(resp) => resp,
            None => return ProcessorResult::Success(None),
        };
        debug!(
            "[ResponseMiddleware] handling response middleware: request_id={} module_id={} status={}",
            response.id,
            response.module_id(),
            response.status_code
        );
        let modified_response = self
            .middleware_manager
            .handle_response(response, &input.1)
            .await;
        ProcessorResult::Success(modified_response)
    }
}
#[async_trait]
impl EventProcessorTrait<(Option<Response>, Option<ModuleConfig>), Option<Response>>
for ResponseMiddlewareProcessor
{
    fn pre_status(&self, input: &(Option<Response>, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        match &input.0 {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponseMiddleware,
                EventPhase::Started,
                ResponseEvent::from(resp),
            )),
            None => Some(EventEnvelope::system_error(
                "no_response_to_process",
                EventPhase::Completed,
            )),
        }
    }

    fn finish_status(
        &self,
        _input: &(Option<Response>, Option<ModuleConfig>),
        out: &Option<Response>,
    ) -> Option<EventEnvelope> {
        match out {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponseMiddleware,
                EventPhase::Completed,
                ResponseEvent::from(resp),
            )),
            None => Some(EventEnvelope::system_error(
                "no_response_to_process",
                EventPhase::Completed,
            )),
        }
    }

    fn working_status(&self, input: &(Option<Response>, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        match &input.0 {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponseMiddleware,
                EventPhase::Started,
                ResponseEvent::from(resp),
            )),
            None => Some(EventEnvelope::system_error(
                "no_response_to_process",
                EventPhase::Completed,
            )),
        }
    }

    fn error_status(
        &self,
        input: &(Option<Response>, Option<ModuleConfig>),
        err: &Error,
    ) -> Option<EventEnvelope> {
        match &input.0 {
            Some(resp) => Some(EventEnvelope::engine_error(
                EventType::ResponseMiddleware,
                EventPhase::Failed,
                ResponseEvent::from(resp),
                err,
            )),
            None => Some(EventEnvelope::system_error(
                format!("response_middleware_error_without_response: {err}"),
                EventPhase::Failed,
            )),
        }
    }

    fn retry_status(
        &self,
        input: &(Option<Response>, Option<ModuleConfig>),
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        match &input.0 {
            Some(resp) => Some(EventEnvelope::engine(
                EventType::ResponseMiddleware,
                EventPhase::Retry,
                json!({
                    "data": ResponseEvent::from(resp),
                    "retry_count": retry_policy.current_retry,
                    "reason": retry_policy.reason.clone().unwrap_or_default(),
                }),
            )),
            None => Some(EventEnvelope::system_error(
                "response_middleware_retry_without_response",
                EventPhase::Completed,
            )),
        }
    }
}
pub struct ProxyMiddlewareProcessor {
    pub(crate) proxy_manager: Option<Arc<ProxyManager>>,
}

#[async_trait]
impl ProcessorTrait<(Request, Option<ModuleConfig>), (Request, Option<ModuleConfig>)>
for ProxyMiddlewareProcessor
{
    fn name(&self) -> &'static str {
        "ProxyMiddlewareProcessor"
    }

    async fn process(
        &self,
        input: (Request, Option<ModuleConfig>),
        context: ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
        let enable_proxy = input.1.as_ref().is_some_and(|cfg| {
            cfg.get_config::<bool>("enable_proxy").unwrap_or(false)
        });
        if !enable_proxy {
            debug!(
                "[ProxyMiddleware] proxy disabled for request_id={} module_id={}",
                input.0.id,
                input.0.module_id()
            );
            return ProcessorResult::Success(input);
        }
        let proxy_manager = match &self.proxy_manager {
            Some(manager) => manager,
            None => return ProcessorResult::Success(input),
        };
        let proxy = proxy_manager.get_proxy(None).await;
        match proxy {
            Ok(proxy) => {
                let mut req = input.0;
                req.proxy = Some(proxy);
                debug!(
                    "[ProxyMiddleware] proxy attached for request_id={} module_id={}",
                    req.id,
                    req.module_id()
                );
                ProcessorResult::Success((req, input.1))
            }
            Err(e) => {
                error!("Failed to get proxy: {e}");
                warn!(
                    "[ProxyMiddleware] will retry due to proxy error: request_id={} module_id={}",
                    input.0.id,
                    input.0.module_id()
                );
                ProcessorResult::RetryableFailure(
                    context
                        .retry_policy
                        .unwrap_or(RetryPolicy::default().with_reason(e.to_string())),
                )
            }
        }
    }
}
#[async_trait]
impl EventProcessorTrait<(Request, Option<ModuleConfig>), (Request, Option<ModuleConfig>)>
for ProxyMiddlewareProcessor
{
    fn pre_status(&self, _input: &(Request, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        None
    }

    fn finish_status(
        &self,
        _input: &(Request, Option<ModuleConfig>),
        _out: &(Request, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        None
    }

    fn working_status(&self, _input: &(Request, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        None
    }

    fn error_status(&self, _input: &(Request, Option<ModuleConfig>), _err: &Error) -> Option<EventEnvelope> {
        None
    }

    fn retry_status(
        &self,
        _input: &(Request, Option<ModuleConfig>),
        _retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        None
    }
}

/// Builds request download chain:
/// config -> proxy middleware -> request middleware -> download -> response middleware -> publish.
pub async fn create_download_chain(
    state: Arc<State>,
    downloader_manager: Arc<DownloaderManager>,
    queue_manager: Arc<QueueManager>,
    middleware_manager: Arc<MiddlewareManager>,
    event_bus: Option<Arc<EventBus>>,
    proxy_manager: Option<Arc<ProxyManager>>,
) -> EventAwareTypedChain<Request, ()> {
    let download_processor = DownloadProcessor {
        downloader_manager,
        state:state.clone(),
        decision_cache: Arc::new(DashMap::new()),
    };
    let response_publish = ResponsePublishProcessor {
        queue_manager,
        state:state.clone(),
    };
    let request_middleware = RequestMiddlewareProcessor {
        middleware_manager: middleware_manager.clone(),
    };
    let response_middleware = ResponseMiddlewareProcessor { middleware_manager };
    let config_processor = ConfigProcessor {state: state.clone() };
    let proxy_middleware = ProxyMiddlewareProcessor { proxy_manager };

    EventAwareTypedChain::<Request, Request>::new(event_bus)
        .then_silent::<(Request, Option<ModuleConfig>), _>(config_processor)
        .then::<(Request, Option<ModuleConfig>), _>(proxy_middleware)
        .then_silent::<(Option<Request>, Option<ModuleConfig>), _>(request_middleware)
        .then::<(Option<Response>, Option<ModuleConfig>), _>(download_processor)
        .then_silent::<Option<Response>, _>(response_middleware)
        .then::<(), _>(response_publish)
}
