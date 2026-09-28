use crate::common::context::PipelineContext;
use crate::common::interface::middleware_manager::MiddlewareManager;
use crate::common::model::ModuleConfig;
use crate::common::model::download_config::DownloadConfig;
use crate::common::model::{Request, Response};
use crate::common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use crate::common::status_tracker::ErrorDecision;
use crate::downloader::DownloaderManager;
use crate::engine::chain::ConfigProcessor;
use crate::engine::chain::backpressure::{BackpressureSendState, send_with_backpressure};
use crate::engine::chain::proxy_attempt::{
    ProxyAttempt, can_rotate, clear_proxy_failure, is_proxy_failure, mark_proxy_failure,
    select_retry_proxy,
};
use crate::engine::events::{
    DownloadEvent, EventBus, EventEnvelope, EventPhase, EventType, RequestMiddlewareEvent,
    ResponseEvent,
};
use crate::engine::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use crate::errors::{DownloadError, Error, ModuleError, Result};
use crate::queue::QueueManager;
use crate::queue::QueuedItem;
use async_trait::async_trait;
use dashmap::DashMap;
use log::{debug, error, info, warn};
use metrics::counter;
use mocra_proxy::ProxyManager;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Download-stage processor.
///
/// It performs defensive threshold checks, selects a downloader implementation,
/// executes the request, and maps outcomes into chain semantics.
pub struct DownloadProcessor {
    pub(crate) downloader_manager: Arc<DownloaderManager>,
    pub(crate) proxy_manager: Option<Arc<ProxyManager>>,
    pub(crate) state: Arc<PipelineContext>,
    pub(crate) decision_cache: Arc<DashMap<String, (Instant, ErrorDecision)>>,
}

#[async_trait]
impl
    ProcessorTrait<
        (Option<Request>, Option<ModuleConfig>),
        (Option<Response>, Option<ModuleConfig>),
    > for DownloadProcessor
{
    fn name(&self) -> &'static str {
        "DownloadProcessor"
    }

    async fn process(
        &self,
        input: (Option<Request>, Option<ModuleConfig>),
        context: ProcessorContext,
    ) -> ProcessorResult<(Option<Response>, Option<ModuleConfig>)> {
        let mut request = match input.0 {
            Some(request) => request,
            None => return ProcessorResult::Success((None, input.1)),
        };
        let _req_id = request.id;
        info!(
            "[DownloadProcessor] begin process: request_id={} retry={}",
            request.id,
            context
                .retry_policy
                .as_ref()
                .map(|r| r.current_retry)
                .unwrap_or(0)
        );

        let is_retry = context
            .retry_policy
            .as_ref()
            .map(|r| r.current_retry > 0)
            .unwrap_or(false);

        if !is_retry {
            // Defensive checks in distributed mode: ingress and download may run on different nodes.

            // 1) Task-level threshold check with short local cache.
            let task_id = request.task_runtime_id();
            // Try cached decision first.
            let cached_task_decision = {
                if let Some(entry) = self.decision_cache.get(&task_id) {
                    let (ts, decision) = entry.value();
                    // 1s TTL to avoid hot-path cache amplification.
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
                    let res = self
                        .state
                        .status_tracker
                        .should_task_continue(&task_id)
                        .await;
                    if let Ok(ref d) = res {
                        self.decision_cache
                            .insert(task_id.clone(), (Instant::now(), d.clone()));
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
            let module_id = request.module_runtime_id();
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
                    let res = self
                        .state
                        .status_tracker
                        .should_module_continue(&module_id)
                        .await;
                    if let Ok(ref d) = res {
                        self.decision_cache
                            .insert(module_id.clone(), (Instant::now(), d.clone()));
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
                        request.module_runtime_id(),
                        reason
                    );
                    // Module terminated: release lock and skip this request.
                    self.state
                        .status_tracker
                        .release_module_locker(&request.module_runtime_id())
                        .await;

                    // Return success with None to keep stream progressing.
                    return ProcessorResult::Success((None, input.1));
                }
                Err(e) => {
                    warn!(
                        "[DownloadProcessor] module error check failed, continue anyway: module_id={} error={}",
                        request.module_runtime_id(),
                        e
                    );
                }
                _ => {}
            }
        } else {
            // [LOG_OPTIMIZATION] debug!("[DownloadProcessor] skipping task/module checks for retry: request_id={}", input.0.id);
        }

        select_retry_proxy(
            &self.proxy_manager,
            &mut request,
            &input.1,
            &context.retry_policy,
        )
        .await;

        info!("[DownloadProcessor] loading config: request_id={}", _req_id);
        let download_config =
            DownloadConfig::load(&input.1, &self.state.config.read().await.download_config);
        info!(
            "[DownloadProcessor] getting downloader: request_id={}",
            _req_id
        );
        let downloader = self
            .downloader_manager
            .get_downloader(&request, download_config)
            .await;
        info!(
            "[DownloadProcessor] starting download: request_id={}",
            _req_id
        );

        let module_id = request.module_runtime_id();
        let task_id = request.task_runtime_id();
        let request_id = request.id;
        let url = request.url.clone();
        let account = request.account.clone();
        let platform = request.platform.clone();
        let used_proxy = request.proxy.clone();
        let replayable = can_rotate(&request);
        let mut proxy_attempt = match ProxyAttempt::begin(&self.proxy_manager, &request).await {
            Ok(attempt) => attempt,
            Err(error) => {
                counter!("mocra_proxy_selection_errors_total").increment(1);
                let mut retry_policy = context.retry_policy.clone().unwrap_or_default();
                if let Some(proxy) = &used_proxy {
                    mark_proxy_failure(&mut retry_policy, proxy);
                }
                if retry_policy.should_retry() {
                    return ProcessorResult::RetryableFailure(
                        retry_policy.with_reason(error.to_string()),
                    );
                }
                return ProcessorResult::FatalFailure(error);
            }
        };
        let proxy_kind = if proxy_attempt.is_some() {
            "managed"
        } else if used_proxy.is_some() {
            "explicit"
        } else {
            "none"
        };
        counter!("mocra_download_attempts_total", "proxy" => proxy_kind).increment(1);

        let download_result = downloader.download(request).await;
        let proxy_status_failure = if let (Some(manager), Some(proxy), Ok(response)) =
            (&self.proxy_manager, &used_proxy, &download_result)
        {
            response.status_code == 407 || manager.is_retry_code(proxy, response.status_code).await
        } else {
            false
        };
        let download_result = match download_result {
            Ok(response) if proxy_status_failure && replayable => {
                Err(DownloadError::ProxyRetryStatus(response.status_code).into())
            }
            other => other,
        };
        match download_result {
            Ok(response) => {
                if let Some(attempt) = proxy_attempt.as_mut() {
                    attempt.finish(!proxy_status_failure).await;
                }
                // [LOG_OPTIMIZATION]
                // debug!(
                //     "[DownloadProcessor] download success: status={} content_len={} module_id={}",
                //     response.status_code,
                //     response.content.len(),
                //     response.module_id()
                // );
                let content_len = response.content.len();
                debug!(
                    "[DownloadProcessor] download finished: account={} platform={} module={} url={} request_id={} status={} len={}",
                    account,
                    platform,
                    module_id,
                    url,
                    request_id,
                    response.status_code,
                    content_len
                );

                // Record request-local success.
                if !proxy_status_failure {
                    let state_clone = self.state.clone();
                    let request_id_clone = request_id.to_string();
                    tokio::spawn(async move {
                        state_clone
                            .status_tracker
                            .record_download_success(&request_id_clone)
                            .await
                            .ok();
                    });
                }

                ProcessorResult::Success((Some(response), input.1))
            }
            Err(e) => {
                let proxy_failure = used_proxy.is_some() && is_proxy_failure(&e);
                if let Some(attempt) = proxy_attempt.as_mut() {
                    attempt.finish(!proxy_failure).await;
                }
                // 1) Local retry first.
                let mut retry_policy = context.retry_policy.clone().unwrap_or_default();
                if proxy_failure {
                    if let Some(proxy) = &used_proxy {
                        mark_proxy_failure(&mut retry_policy, proxy);
                    }
                } else {
                    clear_proxy_failure(&mut retry_policy);
                }
                if retry_policy.should_retry() {
                    debug!(
                        "[DownloadProcessor] download failed, will retry locally: account={} platform={} module={} url={} request_id={} retry={}/{} reason={}",
                        account,
                        platform,
                        module_id,
                        url,
                        request_id,
                        retry_policy.current_retry,
                        retry_policy.max_retries,
                        e
                    );
                    return ProcessorResult::RetryableFailure(
                        retry_policy.with_reason(e.to_string()),
                    );
                }

                warn!(
                    "[DownloadProcessor] download failed after max retries: account={} platform={} module={} url={} request_id={} reason={}",
                    account, platform, module_id, url, request_id, e
                );

                // 2) Retries exhausted; record error and follow tracker decision.
                match self
                    .state
                    .status_tracker
                    .record_download_error(&task_id, &module_id, &request_id.to_string(), &e)
                    .await
                {
                    Ok(ErrorDecision::Terminate(reason)) => {
                        error!(
                            "[DownloadProcessor] terminate: account={} platform={} module={} url={} request_id={} reason={}",
                            account, platform, module_id, url, request_id, reason
                        );
                        ProcessorResult::FatalFailure(
                            ModuleError::ModuleMaxError(reason.into()).into(),
                        )
                    }
                    // Continue/RetryAfter/Skip all map to dropping current request here.
                    Ok(_) => {
                        warn!(
                            "[DownloadProcessor] skip request after max retries (recorded in tracker): request_id={}",
                            request_id
                        );
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
            "[DownloadProcessor] handle_error: account={} platform={} module={} url={} request_id={} error={}",
            request.account,
            request.platform,
            request.module_id(),
            request.url,
            request.id,
            _error
        );

        // Error is already tracked in `process()` via `error_tracker`.
        // Only release lock and return `None` here.

        // Download failed terminally in this chain; no parser stage will release the lock.
        // Ensure we release the module lock to avoid stale locks.
        self.state
            .status_tracker
            .release_module_locker(&request.module_runtime_id())
            .await;
        ProcessorResult::Success((None, _input.1.clone()))
    }
}

#[async_trait]
impl
    EventProcessorTrait<
        (Option<Request>, Option<ModuleConfig>),
        (Option<Response>, Option<ModuleConfig>),
    > for DownloadProcessor
{
    fn pre_status(&self, input: &(Option<Request>, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let ev: DownloadEvent = request.into();
                Some(EventEnvelope::engine(
                    EventType::Download,
                    EventPhase::Started,
                    ev,
                ))
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
                Some(EventEnvelope::engine(
                    EventType::Download,
                    EventPhase::Completed,
                    ev,
                ))
            }
            None => Some(EventEnvelope::system_error(
                "download_skipped_without_request",
                EventPhase::Completed,
            )),
        }
    }

    fn working_status(
        &self,
        input: &(Option<Request>, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let ev: DownloadEvent = request.into();
                Some(EventEnvelope::engine(
                    EventType::Download,
                    EventPhase::Started,
                    ev,
                ))
            }
            None => Some(EventEnvelope::system_error(
                "download_skipped_without_request",
                EventPhase::Completed,
            )),
        }
    }

    fn error_status(
        &self,
        input: &(Option<Request>, Option<ModuleConfig>),
        err: &Error,
    ) -> Option<EventEnvelope> {
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
    pub(crate) state: Arc<PipelineContext>,
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
        // If local channel is full or closed, fall back to the configured backend (Kafka/NATS)
        let result = self.queue_manager.try_send_local_response(item);

        if let Err(e) = match result {
            Ok(_) => {
                debug!("[ResponsePublish] Sent response locally: request_id={}", id);
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(returned_item)) => {
                counter!("mocra_download_response_backpressure_total", "queue" => "local_response", "reason" => "queue_full").increment(1);
                warn!(
                    "[ResponsePublish] local response queue full, fallback to backend channel: request_id={}",
                    id
                );
                let tx = self.queue_manager.get_response_push_channel();
                match send_with_backpressure(&tx, returned_item).await {
                    Ok(BackpressureSendState::Direct) => Ok(()),
                    Ok(BackpressureSendState::RecoveredFromFull) => {
                        counter!("mocra_download_response_backpressure_total", "queue" => "response", "reason" => "queue_full").increment(1);
                        warn!(
                            "[ResponsePublish] backend response queue full, waiting send: request_id={} remaining_capacity={}",
                            id,
                            tx.capacity()
                        );
                        Ok(())
                    }
                    Err(err) => {
                        if err.after_full {
                            counter!("mocra_download_response_backpressure_total", "queue" => "response", "reason" => "queue_full").increment(1);
                            warn!(
                                "[ResponsePublish] backend response queue full before close: request_id={} remaining_capacity={}",
                                id,
                                tx.capacity()
                            );
                        }
                        counter!("mocra_download_response_backpressure_total", "queue" => "response", "reason" => "queue_closed").increment(1);
                        Err("response queue closed".to_string())
                    }
                }
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(returned_item)) => {
                counter!("mocra_download_response_backpressure_total", "queue" => "local_response", "reason" => "queue_closed").increment(1);
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
            return ProcessorResult::RetryableFailure(retry_policy);
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
            self.state
                .status_tracker
                .lock_module(&resp.module_runtime_id())
                .await;
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
            self.state
                .status_tracker
                .release_module_locker(&resp.module_runtime_id())
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

    fn retry_status(
        &self,
        input: &Option<Response>,
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
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
        let original_proxy = input.0.proxy.clone();
        let modified_request = self
            .middleware_manager
            .handle_request(input.0, &input.1)
            .await;
        let modified_request = modified_request.map(|mut request| {
            if request.proxy != original_proxy {
                request.proxy_from_pool = false;
            }
            request
        });
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

    fn error_status(
        &self,
        input: &(Request, Option<ModuleConfig>),
        err: &Error,
    ) -> Option<EventEnvelope> {
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
    fn pre_status(
        &self,
        input: &(Option<Response>, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
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

    fn working_status(
        &self,
        input: &(Option<Response>, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
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
        let enable_proxy = input
            .1
            .as_ref()
            .is_some_and(|cfg| cfg.get_config::<bool>("enable_proxy").unwrap_or(false));
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
        if input.0.proxy.is_some() {
            return ProcessorResult::Success(input);
        }
        let proxy = proxy_manager.get_proxy_for_attempt(None).await;
        match proxy {
            Ok(proxy) => {
                let mut req = input.0;
                req.proxy = Some(proxy);
                req.proxy_from_pool = true;
                debug!(
                    "[ProxyMiddleware] proxy attached for request_id={} module_id={}",
                    req.id,
                    req.module_id()
                );
                ProcessorResult::Success((req, input.1))
            }
            Err(e) => {
                counter!("mocra_proxy_selection_errors_total").increment(1);
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

    fn error_status(
        &self,
        _input: &(Request, Option<ModuleConfig>),
        _err: &Error,
    ) -> Option<EventEnvelope> {
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
    state: Arc<PipelineContext>,
    downloader_manager: Arc<DownloaderManager>,
    queue_manager: Arc<QueueManager>,
    middleware_manager: Arc<MiddlewareManager>,
    event_bus: Option<Arc<EventBus>>,
    proxy_manager: Option<Arc<ProxyManager>>,
) -> EventAwareTypedChain<Request, ()> {
    let download_processor = DownloadProcessor {
        downloader_manager,
        proxy_manager: proxy_manager.clone(),
        state: state.clone(),
        decision_cache: Arc::new(DashMap::new()),
    };
    let response_publish = ResponsePublishProcessor {
        queue_manager,
        state: state.clone(),
    };
    let request_middleware = RequestMiddlewareProcessor {
        middleware_manager: middleware_manager.clone(),
    };
    let response_middleware = ResponseMiddlewareProcessor { middleware_manager };
    let config_processor = ConfigProcessor {
        state: state.clone(),
    };
    let proxy_middleware = ProxyMiddlewareProcessor { proxy_manager };

    EventAwareTypedChain::<Request, Request>::new(event_bus)
        .then_silent::<(Request, Option<ModuleConfig>), _>(config_processor)
        .then::<(Request, Option<ModuleConfig>), _>(proxy_middleware)
        .then_silent::<(Option<Request>, Option<ModuleConfig>), _>(request_middleware)
        .then::<(Option<Response>, Option<ModuleConfig>), _>(download_processor)
        .then_silent::<Option<Response>, _>(response_middleware)
        .then::<(), _>(response_publish)
}

#[cfg(test)]
mod rotation_tests {
    use super::*;
    use crate::common::config::ConfigProvider;
    use crate::common::model::Cookies;
    use crate::common::model::config::Config;
    use crate::common::state::State;
    use crate::downloader::Downloader;
    use mocra_proxy::{DirectProxy, PoolConfig, PoolStats, ProxyConfig};
    use semver::Version;
    use std::sync::Mutex;
    use tokio::sync::watch;

    #[cfg(target_os = "linux")]
    mod acceptance_workload {
        use super::*;
        use crate::queue::batcher::Batcher;
        use crate::queue::{QueueManager, QueuedItem};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio::sync::Semaphore;

        const REQUESTS: usize = 320;
        const CHANNEL_CAPACITY: usize = 32;
        const BATCH_SIZE: usize = 8;
        const MAX_BATCHES: usize = 8;

        struct RoundResult {
            throughput: f64,
            p95_us: u128,
            p99_us: u128,
            rss_kib: usize,
            cpu_ticks: u64,
            queue_peak: usize,
            batches_peak: usize,
            ack: usize,
            nack: usize,
            bad_attempts: usize,
            good_attempts: usize,
            selection_wait_avg_us: f64,
            selection_wait_max_us: f64,
            recovery_p95_us: u128,
            attempts_started: u64,
            feedback_succeeded: u64,
            feedback_failed: u64,
            rate_limited: u64,
        }

        fn rss_kib() -> usize {
            std::fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|status| {
                    status
                        .lines()
                        .find(|line| line.starts_with("VmRSS:"))?
                        .split_whitespace()
                        .nth(1)?
                        .parse()
                        .ok()
                })
                .unwrap_or(0)
        }

        fn cpu_ticks() -> u64 {
            std::fs::read_to_string("/proc/self/stat")
                .ok()
                .and_then(|stat| {
                    let fields: Vec<_> = stat.rsplit_once(") ")?.1.split_whitespace().collect();
                    Some(
                        fields.get(11)?.parse::<u64>().ok()?
                            + fields.get(12)?.parse::<u64>().ok()?,
                    )
                })
                .unwrap_or(0)
        }

        async fn start_proxy(
            status: u16,
            count: Arc<AtomicUsize>,
            failure_times: Arc<DashMap<String, Instant>>,
            recovery_latencies: Arc<Mutex<Vec<u128>>>,
        ) -> (String, tokio::task::JoinHandle<()>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        break;
                    };
                    let count = count.clone();
                    let failure_times = failure_times.clone();
                    let recovery_latencies = recovery_latencies.clone();
                    tokio::spawn(async move {
                        let mut pending = Vec::new();
                        let mut buffer = [0u8; 4096];
                        while let Ok(size) = socket.read(&mut buffer).await {
                            if size == 0 {
                                break;
                            }
                            pending.extend_from_slice(&buffer[..size]);
                            while let Some(end) =
                                pending.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                            {
                                let request_line = pending[..end]
                                    .split(|byte| *byte == b'\n')
                                    .next()
                                    .and_then(|line| std::str::from_utf8(line).ok())
                                    .unwrap_or("");
                                let uri = request_line
                                    .split_whitespace()
                                    .nth(1)
                                    .unwrap_or("")
                                    .to_string();
                                if status == 407 {
                                    failure_times.entry(uri).or_insert_with(Instant::now);
                                } else if let Some((_, failed_at)) = failure_times.remove(&uri) {
                                    recovery_latencies
                                        .lock()
                                        .unwrap()
                                        .push(failed_at.elapsed().as_micros());
                                }
                                pending.drain(..end + 4);
                                count.fetch_add(1, Ordering::Relaxed);
                                let response = if status == 200 {
                                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok".as_slice()
                                } else {
                                    b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n".as_slice()
                                };
                                if socket.write_all(response).await.is_err() {
                                    return;
                                }
                            }
                        }
                    });
                }
            });
            (address, task)
        }

        async fn run_round(rotate: bool, bad_proxy_count: usize, requests: usize) -> RoundResult {
            let good_attempts = Arc::new(AtomicUsize::new(0));
            let bad_attempts = Arc::new(AtomicUsize::new(0));
            let failure_times = Arc::new(DashMap::new());
            let recovery_latencies = Arc::new(Mutex::new(Vec::<u128>::new()));
            let mut proxy_tasks = Vec::new();
            let mut direct = Vec::new();
            for index in 0..8 {
                let bad = index < bad_proxy_count;
                let (url, task) = start_proxy(
                    if bad { 407 } else { 200 },
                    if bad {
                        bad_attempts.clone()
                    } else {
                        good_attempts.clone()
                    },
                    failure_times.clone(),
                    recovery_latencies.clone(),
                )
                .await;
                proxy_tasks.push(task);
                direct.push(DirectProxy {
                    name: Some(format!("proxy-{index}")),
                    url,
                    rate_limit: Some(0.0),
                    expire_time: None,
                });
            }

            let mut application_config = config();
            application_config
                .download_config
                .proxy_client_cache_capacity = Some(8);
            let state = Arc::new(
                State::try_new_with_provider(Box::new(StaticConfig(application_config)))
                    .await
                    .unwrap(),
            );
            let downloader_manager = Arc::new(
                DownloaderManager::new(
                    state.config.clone(),
                    state.limiter.clone(),
                    state.locker.clone(),
                    state.cache_service.clone(),
                )
                .await,
            );
            let proxy_manager = Arc::new(
                ProxyManager::from_proxy_config(&ProxyConfig {
                    tunnel: None,
                    direct: Some(direct),
                    ip_provider: None,
                    pool_config: Some(PoolConfig {
                        max_errors: 10_000,
                        health_check_interval_secs: 0,
                        ..PoolConfig::default()
                    }),
                })
                .await
                .unwrap(),
            );

            let manager = QueueManager::new(None, CHANNEL_CAPACITY);
            let sender = manager.get_request_push_channel();
            let receiver = manager.get_request_pop_channel();
            drop(manager);
            let acknowledged = Arc::new(AtomicUsize::new(0));
            let rejected = Arc::new(AtomicUsize::new(0));
            let active_batches = Arc::new(AtomicUsize::new(0));
            let peak_batches = Arc::new(AtomicUsize::new(0));
            let timings = Arc::new(DashMap::<uuid::Uuid, Instant>::new());
            let latencies = Arc::new(Mutex::new(Vec::<u128>::new()));
            let processor = {
                let proxy_manager = proxy_manager.clone();
                let downloader_manager = downloader_manager.clone();
                let state = state.clone();
                let active_batches = active_batches.clone();
                let peak_batches = peak_batches.clone();
                let timings = timings.clone();
                let latencies = latencies.clone();
                move |items: Vec<QueuedItem<Request>>| {
                    let proxy_manager = proxy_manager.clone();
                    let downloader_manager = downloader_manager.clone();
                    let state = state.clone();
                    let active_batches = active_batches.clone();
                    let peak_batches = peak_batches.clone();
                    let timings = timings.clone();
                    let latencies = latencies.clone();
                    async move {
                        let active = active_batches.fetch_add(1, Ordering::SeqCst) + 1;
                        peak_batches.fetch_max(active, Ordering::SeqCst);
                        for item in items {
                            let (mut request, ack, nack) = item.into_parts();
                            let request_id = request.id;
                            let module_config = ModuleConfig {
                                module_config: json!({"enable_proxy": true}),
                                ..ModuleConfig::default()
                            };
                            let input = if rotate {
                                match (ProxyMiddlewareProcessor {
                                    proxy_manager: Some(proxy_manager.clone()),
                                })
                                .process(
                                    (request, Some(module_config)),
                                    ProcessorContext::default(),
                                )
                                .await
                                {
                                    ProcessorResult::Success(value) => Some(value),
                                    _ => None,
                                }
                            } else {
                                request.proxy =
                                    proxy_manager.get_proxy_for_attempt(None).await.ok();
                                Some((request, Some(module_config)))
                            };
                            let success = if let Some((request, module_config)) = input {
                                let chain = EventAwareTypedChain::<
                                    (Option<Request>, Option<ModuleConfig>),
                                    _,
                                >::new(None)
                                .then::<(Option<Response>, Option<ModuleConfig>), _>(
                                    DownloadProcessor {
                                        downloader_manager: downloader_manager.clone(),
                                        proxy_manager: Some(proxy_manager.clone()),
                                        state: state.pipeline_ctx(),
                                        decision_cache: Arc::new(DashMap::new()),
                                    },
                                );
                                let policy = RetryPolicy {
                                    max_retries: 1,
                                    retry_delay: 1,
                                    ..RetryPolicy::default()
                                };
                                matches!(chain.execute((Some(request), module_config), ProcessorContext::default().with_retry_policy(policy)).await, ProcessorResult::Success((Some(response), _)) if response.status_code == 200)
                            } else {
                                false
                            };
                            if let Some((_, sent_at)) = timings.remove(&request_id) {
                                latencies
                                    .lock()
                                    .unwrap()
                                    .push(sent_at.elapsed().as_micros());
                            }
                            if success {
                                if let Some(ack) = ack {
                                    ack().await.unwrap();
                                }
                            } else if let Some(nack) = nack {
                                nack("download failed".into()).await.unwrap();
                            }
                        }
                        active_batches.fetch_sub(1, Ordering::SeqCst);
                    }
                }
            };

            let worker = tokio::spawn(async move {
                let mut receiver = receiver.lock().await;
                Batcher::run(
                    &mut receiver,
                    BATCH_SIZE,
                    1,
                    Arc::new(Semaphore::new(MAX_BATCHES)),
                    processor,
                )
                .await;
            });
            let peak_rss = Arc::new(AtomicUsize::new(rss_kib()));
            let sampler = {
                let peak_rss = peak_rss.clone();
                tokio::spawn(async move {
                    loop {
                        peak_rss.fetch_max(rss_kib(), Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
            };
            let cpu_start = cpu_ticks();
            let started = Instant::now();
            let mut peak_queue = 0;
            for index in 0..requests {
                let mut request = Request::new(format!("http://example.test/item/{index}"), "GET");
                request.account = "acceptance".into();
                request.platform = "local".into();
                request.module = "proxy".into();
                let request_id = request.id;
                timings.insert(request_id, Instant::now());
                let ack_count = acknowledged.clone();
                let nack_count = rejected.clone();
                sender
                    .send(QueuedItem::with_ack(
                        request,
                        move || {
                            Box::pin(async move {
                                ack_count.fetch_add(1, Ordering::Relaxed);
                                Ok(())
                            })
                        },
                        move |_| {
                            Box::pin(async move {
                                nack_count.fetch_add(1, Ordering::Relaxed);
                                Ok(())
                            })
                        },
                    ))
                    .await
                    .unwrap();
                peak_queue = peak_queue.max(CHANNEL_CAPACITY - sender.capacity());
            }
            drop(sender);
            tokio::time::timeout(Duration::from_secs(30), worker)
                .await
                .unwrap()
                .unwrap();
            let wall = started.elapsed().as_secs_f64();
            let cpu_delta = cpu_ticks().saturating_sub(cpu_start);
            sampler.abort();
            for task in proxy_tasks {
                task.abort();
            }
            let mut samples = latencies.lock().unwrap().clone();
            samples.sort_unstable();
            assert_eq!(samples.len(), requests);
            let p95 = samples[(samples.len() * 95 / 100).min(samples.len() - 1)];
            let p99 = samples[(samples.len() * 99 / 100).min(samples.len() - 1)];
            let ack = acknowledged.load(Ordering::Relaxed);
            let nack = rejected.load(Ordering::Relaxed);
            assert_eq!(ack + nack, requests);
            assert!(peak_queue <= CHANNEL_CAPACITY);
            assert!(peak_batches.load(Ordering::SeqCst) <= MAX_BATCHES);
            let stats = proxy_manager.get_detailed_stats().await;
            assert_eq!(stats.total_proxies, 8);
            let lock_wait = proxy_manager.selection_wait_stats();
            let attempts = proxy_manager.attempt_stats();
            if rotate {
                assert_eq!(
                    attempts.started as usize,
                    bad_attempts.load(Ordering::Relaxed) + good_attempts.load(Ordering::Relaxed)
                );
                assert_eq!(
                    attempts.succeeded as usize,
                    good_attempts.load(Ordering::Relaxed)
                );
                assert_eq!(
                    attempts.failed as usize,
                    bad_attempts.load(Ordering::Relaxed)
                );
            } else {
                assert_eq!(attempts.started, 0);
            }
            assert_eq!(attempts.started, attempts.succeeded + attempts.failed);
            assert_eq!(attempts.rate_limited, 0);
            let mut recoveries = recovery_latencies.lock().unwrap().clone();
            recoveries.sort_unstable();
            RoundResult {
                throughput: requests as f64 / wall,
                p95_us: p95,
                p99_us: p99,
                rss_kib: peak_rss.load(Ordering::Relaxed),
                cpu_ticks: cpu_delta,
                queue_peak: peak_queue,
                batches_peak: peak_batches.load(Ordering::SeqCst),
                ack,
                nack,
                bad_attempts: bad_attempts.load(Ordering::Relaxed),
                good_attempts: good_attempts.load(Ordering::Relaxed),
                selection_wait_avg_us: lock_wait.total_wait_ns as f64
                    / lock_wait.count.max(1) as f64
                    / 1000.0,
                selection_wait_max_us: lock_wait.max_wait_ns as f64 / 1000.0,
                recovery_p95_us: if recoveries.is_empty() {
                    0
                } else {
                    recoveries[(recoveries.len() * 95 / 100).min(recoveries.len() - 1)]
                },
                attempts_started: attempts.started,
                feedback_succeeded: attempts.succeeded,
                feedback_failed: attempts.failed,
                rate_limited: attempts.rate_limited,
            }
        }

        #[tokio::test]
        #[ignore = "local HTTP proxy and bounded queue load workload"]
        async fn local_full_path_acceptance() {
            let mut baseline_success = Vec::new();
            let mut rotation_success = Vec::new();
            for bad_proxy_count in [0, 4] {
                for rotate in [false, true] {
                    for round in 0..5 {
                        let result = run_round(rotate, bad_proxy_count, REQUESTS).await;
                        if bad_proxy_count == 4 {
                            if rotate {
                                rotation_success.push(result.ack);
                            } else {
                                baseline_success.push(result.ack);
                            }
                        }
                        println!(
                            "ACCEPTANCE bad_proxies={bad_proxy_count} rotate={rotate} round={round} throughput={:.1} p95_us={} p99_us={} rss_kib={} cpu_ticks={} queue_peak={} batches_peak={} ack={} nack={} bad_attempts={} good_attempts={} selection_wait_avg_us={:.3} selection_wait_max_us={:.3} recovery_p95_us={} attempts_started={} feedback_ok={} feedback_failed={} rate_limited={}",
                            result.throughput,
                            result.p95_us,
                            result.p99_us,
                            result.rss_kib,
                            result.cpu_ticks,
                            result.queue_peak,
                            result.batches_peak,
                            result.ack,
                            result.nack,
                            result.bad_attempts,
                            result.good_attempts,
                            result.selection_wait_avg_us,
                            result.selection_wait_max_us,
                            result.recovery_p95_us,
                            result.attempts_started,
                            result.feedback_succeeded,
                            result.feedback_failed,
                            result.rate_limited
                        );
                    }
                }
            }
            baseline_success.sort_unstable();
            rotation_success.sort_unstable();
            assert!(rotation_success[2] > baseline_success[2] + 100);
        }

        #[tokio::test]
        #[ignore = "longer local overload workload"]
        async fn local_full_path_overload() {
            let result = run_round(true, 4, 3200).await;
            println!(
                "OVERLOAD throughput={:.1} p95_us={} p99_us={} rss_kib={} cpu_ticks={} queue_peak={} batches_peak={} ack={} nack={} selection_wait_avg_us={:.3} selection_wait_max_us={:.3} recovery_p95_us={} attempts_started={} feedback_ok={} feedback_failed={} rate_limited={}",
                result.throughput,
                result.p95_us,
                result.p99_us,
                result.rss_kib,
                result.cpu_ticks,
                result.queue_peak,
                result.batches_peak,
                result.ack,
                result.nack,
                result.selection_wait_avg_us,
                result.selection_wait_max_us,
                result.recovery_p95_us,
                result.attempts_started,
                result.feedback_succeeded,
                result.feedback_failed,
                result.rate_limited
            );
        }
    }

    struct StaticConfig(Config);

    #[async_trait]
    impl ConfigProvider for StaticConfig {
        async fn load_config(&self) -> std::result::Result<Config, String> {
            Ok(self.0.clone())
        }
        async fn watch(&self) -> std::result::Result<watch::Receiver<Config>, String> {
            Ok(watch::channel(self.0.clone()).1)
        }
    }

    #[derive(Clone)]
    struct FakeDownloader {
        seen: Arc<Mutex<Vec<String>>>,
        behavior: FakeBehavior,
    }

    #[derive(Clone, Copy)]
    enum FakeBehavior {
        FailFirst,
        AlwaysFail,
        Status(u16),
        StatusFirst(u16),
    }

    #[derive(Clone, Copy)]
    enum ProxyMode {
        Managed,
        ManagedSingle,
        Explicit,
        None,
    }

    #[async_trait]
    impl Downloader for FakeDownloader {
        async fn set_config(&self, _: &str, _: DownloadConfig) {}
        async fn set_limit(&self, _: &str, _: f32) {}
        fn name(&self) -> String {
            "proxy_probe".into()
        }
        fn version(&self) -> Version {
            Version::new(1, 0, 0)
        }
        async fn health_check(&self) -> Result<()> {
            Ok(())
        }
        async fn download(&self, request: Request) -> Result<Response> {
            let proxy = request
                .proxy
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            let call = {
                let mut seen = self.seen.lock().unwrap();
                seen.push(proxy);
                seen.len()
            };
            if matches!(self.behavior, FakeBehavior::AlwaysFail)
                || (matches!(self.behavior, FakeBehavior::FailFirst) && call == 1)
            {
                return Err(DownloadError::NetworkError("proxy connection refused".into()).into());
            }
            Ok(Response {
                id: request.id,
                platform: request.platform,
                account: request.account,
                module: request.module,
                status_code: match self.behavior {
                    FakeBehavior::Status(code) => code,
                    FakeBehavior::StatusFirst(code) if call == 1 => code,
                    _ => 200,
                },
                cookies: Cookies::default(),
                content: b"ok".to_vec(),
                storage_path: None,
                headers: vec![],
                task_retry_times: request.task_retry_times,
                metadata: request.meta,
                download_middleware: request.download_middleware,
                data_middleware: request.data_middleware,
                task_finished: request.task_finished,
                context: request.context,
                run_id: request.run_id,
                prefix_request: request.prefix_request,
                request_hash: None,
                priority: request.priority,
            })
        }
    }

    fn config() -> Config {
        toml::from_str(
            r#"
            name = "proxy_rotation_test"
            [db]
            database_schema = "public"
            [download_config]
            downloader_expire = 3600
            timeout = 5
            rate_limit = 0.0
            enable_session = false
            enable_locker = false
            enable_rate_limit = false
            cache_ttl = 60
            wss_timeout = 5
            [cache]
            ttl = 60
            [crawler]
            request_max_retries = 1
            task_max_errors = 10
            module_max_errors = 10
            module_locker_ttl = 5
            [channel_config]
            minid_time = 0
            capacity = 10
        "#,
        )
        .unwrap()
    }

    async fn run_case(
        method: &str,
        mode: ProxyMode,
        behavior: FakeBehavior,
    ) -> (
        ProcessorResult<(Option<Response>, Option<ModuleConfig>)>,
        Vec<String>,
        PoolStats,
    ) {
        let state = Arc::new(
            State::try_new_with_provider(Box::new(StaticConfig(config())))
                .await
                .unwrap(),
        );
        let downloader_manager = Arc::new(
            DownloaderManager::new(
                state.config.clone(),
                state.limiter.clone(),
                state.locker.clone(),
                state.cache_service.clone(),
            )
            .await,
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        downloader_manager
            .set_default_downloader(Box::new(FakeDownloader {
                seen: seen.clone(),
                behavior,
            }))
            .await;
        let direct_proxies = vec![
            DirectProxy {
                name: Some("a".into()),
                url: "http://127.0.0.1:8080".into(),
                rate_limit: Some(10.0),
                expire_time: None,
            },
            DirectProxy {
                name: Some("b".into()),
                url: "http://127.0.0.1:8081".into(),
                rate_limit: Some(10.0),
                expire_time: None,
            },
        ];
        let proxy_manager = Arc::new(
            ProxyManager::from_proxy_config(&ProxyConfig {
                tunnel: None,
                direct: Some(if matches!(mode, ProxyMode::ManagedSingle) {
                    direct_proxies.into_iter().take(1).collect()
                } else {
                    direct_proxies
                }),
                ip_provider: None,
                pool_config: Some(PoolConfig::default()),
            })
            .await
            .unwrap(),
        );
        let mut request = Request::new("http://example.test", method);
        request.account = "acct".into();
        request.platform = "site".into();
        request.module = "probe".into();
        let (request, module_config) = match mode {
            ProxyMode::Managed | ProxyMode::ManagedSingle => {
                let module_config = ModuleConfig {
                    module_config: json!({"enable_proxy": true}),
                    ..ModuleConfig::default()
                };
                match (ProxyMiddlewareProcessor {
                    proxy_manager: Some(proxy_manager.clone()),
                })
                .process((request, Some(module_config)), ProcessorContext::default())
                .await
                {
                    ProcessorResult::Success(value) => value,
                    other => panic!("proxy selection failed: {other:?}"),
                }
            }
            ProxyMode::Explicit => {
                request.proxy = Some(proxy_manager.get_proxy_for_attempt(None).await.unwrap());
                (request, None)
            }
            ProxyMode::None => (request, None),
        };
        let processor = DownloadProcessor {
            downloader_manager,
            proxy_manager: Some(proxy_manager.clone()),
            state: state.pipeline_ctx(),
            decision_cache: Arc::new(DashMap::new()),
        };
        let chain = EventAwareTypedChain::<(Option<Request>, Option<ModuleConfig>), _>::new(None)
            .then::<(Option<Response>, Option<ModuleConfig>), _>(processor);
        let mut policy = RetryPolicy::default();
        policy.max_retries = 1;
        policy.retry_delay = 1;
        let result = chain
            .execute(
                (Some(request), module_config),
                ProcessorContext::default().with_retry_policy(policy),
            )
            .await;
        let seen = seen.lock().unwrap().clone();
        let stats = proxy_manager.get_detailed_stats().await;
        (result, seen, stats)
    }

    #[tokio::test]
    async fn managed_proxy_failure_rotates_within_download_retry_budget() {
        let (result, seen, stats) =
            run_case("GET", ProxyMode::Managed, FakeBehavior::FailFirst).await;
        assert!(matches!(result, ProcessorResult::Success((Some(_), _))));
        assert_eq!(seen.len(), 2);
        assert_ne!(seen[0], seen[1]);
        assert_eq!(stats.total_proxies, 2);
        assert_eq!(stats.avg_success_rate, 0.5);
    }

    #[tokio::test]
    async fn exhausted_proxies_use_only_the_existing_retry_budget() {
        let (result, seen, stats) =
            run_case("GET", ProxyMode::Managed, FakeBehavior::AlwaysFail).await;
        assert!(!matches!(result, ProcessorResult::Success((Some(_), _))));
        assert_eq!(seen.len(), 2);
        assert_ne!(seen[0], seen[1]);
        assert_eq!(stats.total_proxies, 2);
        assert_eq!(stats.avg_success_rate, 0.0);
    }

    #[tokio::test]
    async fn single_proxy_retries_without_busy_rotation() {
        let (result, seen, stats) =
            run_case("GET", ProxyMode::ManagedSingle, FakeBehavior::AlwaysFail).await;
        assert!(!matches!(result, ProcessorResult::Success((Some(_), _))));
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0], seen[1]);
        assert_eq!(stats.total_proxies, 1);
        assert_eq!(stats.avg_success_rate, 0.0);
    }

    #[tokio::test]
    async fn proxy_auth_status_rotates_and_reports_failure() {
        let (result, seen, stats) =
            run_case("GET", ProxyMode::Managed, FakeBehavior::StatusFirst(407)).await;
        assert!(
            matches!(result, ProcessorResult::Success((Some(response), _)) if response.status_code == 200)
        );
        assert_eq!(seen.len(), 2);
        assert_ne!(seen[0], seen[1]);
        assert_eq!(stats.avg_success_rate, 0.5);
    }

    #[tokio::test]
    async fn target_server_error_does_not_penalize_proxy() {
        for status in [404, 503] {
            let (result, seen, stats) =
                run_case("GET", ProxyMode::Managed, FakeBehavior::Status(status)).await;
            assert!(
                matches!(result, ProcessorResult::Success((Some(response), _)) if response.status_code == status)
            );
            assert_eq!(seen.len(), 1);
            assert_eq!(stats.avg_success_rate, 1.0);
        }
    }

    #[tokio::test]
    async fn post_and_explicit_proxy_keep_their_retry_target() {
        for (method, mode) in [("POST", ProxyMode::Managed), ("GET", ProxyMode::Explicit)] {
            let (result, seen, _) = run_case(method, mode, FakeBehavior::FailFirst).await;
            assert!(matches!(result, ProcessorResult::Success((Some(_), _))));
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0], seen[1]);
        }
    }

    #[tokio::test]
    async fn no_proxy_path_keeps_retries_and_has_no_pool_feedback() {
        let (result, seen, stats) = run_case("GET", ProxyMode::None, FakeBehavior::FailFirst).await;
        assert!(matches!(result, ProcessorResult::Success((Some(_), _))));
        assert_eq!(seen, ["", ""]);
        assert_eq!(stats.total_proxies, 2);
        assert_eq!(stats.avg_success_rate, 1.0);
    }
}
