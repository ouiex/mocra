use queue::QueuedItem;
use crate::chain::ConfigProcessor;
use crate::events::{
    DownloadEvent, EventBus, EventEnvelope, EventPhase, EventType, RequestMiddlewareEvent,
    ResponseEvent,
};
use crate::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use async_trait::async_trait;
use downloader::DownloaderManager;
use errors::{Error, ModuleError, Result};
use common::interface::middleware_manager::MiddlewareManager;
use common::model::ModuleConfig;
use common::model::download_config::DownloadConfig;
use common::model::{Request, Response};
use common::state::State;
use log::{debug, warn, error};
use queue::QueueManager;
use common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use proxy::ProxyManager;
use std::sync::Arc;
use dashmap::DashMap;
use std::time::{Instant, Duration};
use common::status_tracker::ErrorDecision;
use serde_json::json;
pub struct DownloadProcessor {
    pub(crate) downloader_manager: Arc<DownloaderManager>,
    pub(crate) state: Arc<State>,
    pub(crate) decision_cache: Arc<DashMap<String, (Instant, ErrorDecision)>>,
}

#[async_trait]
impl ProcessorTrait<(Request, Option<ModuleConfig>), (Option<Response>, Option<ModuleConfig>)>
for DownloadProcessor
{
    fn name(&self) -> &'static str {
        "DownloadProcessor"
    }

    async fn process(
        &self,
        input: (Request, Option<ModuleConfig>),
        context: ProcessorContext,
    ) -> ProcessorResult<(Option<Response>, Option<ModuleConfig>)> {
        debug!(
            "[DownloadProcessor] begin process: request_id={} module_id={} task_id={} retry_count={}",
            input.0.id,
            input.0.module_id(),
            input.0.task_id(),
            context.retry_policy.as_ref().map(|r| r.current_retry).unwrap_or(0)
        );

        let is_retry = context.retry_policy.as_ref().map(|r| r.current_retry > 0).unwrap_or(false);

        if !is_retry {
            // 分布式场景下的防御性检查
            // TaskModelChain 和 DownloadChain 运行在不同节点，需要在执行前再次检查最新错误状态

            // 1. 检查 Task 级别错误 (带缓存优化)
            let task_id = input.0.task_id();
            // 尝试从缓存获取决策
            let cached_task_decision = {
                if let Some(entry) = self.decision_cache.get(&task_id) {
                    let (ts, decision) = entry.value();
                    // 缓存有效期 1 秒
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
                        input.0.task_id(),
                        reason
                    );
                    return ProcessorResult::FatalFailure(
                        ModuleError::TaskMaxError(reason.into()).into(),
                    );
                }
                Err(e) => {
                    warn!(
                        "[DownloadProcessor] task error check failed, continue anyway: task_id={} error={}",
                        input.0.task_id(),
                        e
                    );
                }
                _ => {}
            }

            // 2. 检查 Module 级别错误 (带缓存优化)
            let module_id = input.0.module_id();
            // 尝试从缓存获取决策
            let cached_decision = {
                if let Some(entry) = self.decision_cache.get(&module_id) {
                    let (ts, decision) = entry.value();
                    // 缓存有效期 1 秒，避免高频请求打爆 Redis
                    if ts.elapsed() < Duration::from_secs(1) {
                        Some(Ok(decision.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            };

            // 缓存未命中或过期，则查询 Redis 并更新缓存
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
                        input.0.module_id(),
                        reason
                    );
                    // Module 已终止，释放锁并返回 None，让链路继续处理其他请求
                    self.state
                        .status_tracker
                        .release_module_locker(&input.0.module_id())
                        .await;

                    // 返回 None 表示跳过该请求，而不是 FatalFailure
                    // 这样可以让其他请求继续处理
                    return ProcessorResult::Success((None, input.1));
                }
                Err(e) => {
                    warn!(
                        "[DownloadProcessor] module error check failed, continue anyway: module_id={} error={}",
                        input.0.module_id(),
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
            .get_downloader(&input.0, download_config)
            .await;
        debug!("[DownloadProcessor] acquired downloader, start download: request_id={}", input.0.id);

        let module_id = input.0.module_id();
        let task_id = input.0.task_id();
        let request_id = input.0.id;

        match downloader.download(input.0).await {
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

                // 记录下载成功（仅减少 Request 级别的错误计数）
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
                // 1. 检查是否可以本地重试
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

                // 2. 超过重试次数，记录下载错误并获取决策
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
                    // 其他情况（Continue, RetryAfter, Skip）都视为放弃当前请求
                    Ok(_) => {
                        warn!("[DownloadProcessor] skip request after max retries (recorded in tracker): request_id={}", request_id);
                        ProcessorResult::Success((None, input.1))
                    }
                    Err(err) => {
                        error!("[DownloadProcessor] error tracker failed: {}", err);
                        // 追踪器失败，保守起见放弃请求
                        ProcessorResult::Success((None, input.1))
                    }
                }
            }
        }
    }
    async fn handle_error(
        &self,
        _input: &(Request, Option<ModuleConfig>),
        _error: Error,
        _context: &ProcessorContext,
    ) -> ProcessorResult<(Option<Response>, Option<ModuleConfig>)> {
        error!(
            "[DownloadProcessor] handle_error: request_id={} module_id={} error={}",
            _input.0.id,
            _input.0.module_id(),
            _error
        );

        // 错误已经在 process() 方法中通过 error_tracker 记录
        // 这里只需要释放模块锁并返回 None

        // Download failed terminally in this chain; no parser stage will release the lock.
        // Ensure we release the module lock to avoid stale locks.
        self.state
            .status_tracker
            .release_module_locker(&_input.0.module_id())
            .await;
        ProcessorResult::Success((None, _input.1.clone()))
    }
}

#[async_trait]
impl EventProcessorTrait<(Request, Option<ModuleConfig>), (Option<Response>, Option<ModuleConfig>)>
for DownloadProcessor
{
    fn pre_status(&self, input: &(Request, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        let ev: DownloadEvent = (&input.0).into();
        Some(EventEnvelope::engine(EventType::Download, EventPhase::Started, ev))
    }

    fn finish_status(
        &self,
        input: &(Request, Option<ModuleConfig>),
        out: &(Option<Response>, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        // Build a DownloadEvent; enrich with response info if available
        let mut ev: DownloadEvent = (&input.0).into();
        if let Some(resp) = &out.0 {
            ev.status_code = Some(resp.status_code);
            // Keeping duration_ms/response_size as None unless tracked elsewhere
        }
        Some(EventEnvelope::engine(EventType::Download, EventPhase::Completed, ev))
    }

    fn working_status(&self, input: &(Request, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        let ev: DownloadEvent = (&input.0).into();
        Some(EventEnvelope::engine(EventType::Download, EventPhase::Started, ev))
    }

    fn error_status(&self, input: &(Request, Option<ModuleConfig>), err: &Error) -> Option<EventEnvelope> {
        let ev: DownloadEvent = (&input.0).into();
        Some(EventEnvelope::engine_error(
            EventType::Download,
            EventPhase::Failed,
            ev,
            err,
        ))
    }

    fn retry_status(
        &self,
        input: &(Request, Option<ModuleConfig>),
        retry_policy: &RetryPolicy,
    ) -> Option<EventEnvelope> {
        let ev: DownloadEvent = (&input.0).into();
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
            Err(tokio::sync::mpsc::error::TrySendError::Full(returned_item)) 
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(returned_item)) => {
                 self.queue_manager
                    .get_response_push_channel()
                    .send(returned_item)
                    .await
                    .map_err(|e| e.to_string())
            }
        } {
            error!("Failed to send response to queue: {e}");
            warn!("[ResponsePublish] will retry due to queue send error");
            return ProcessorResult::RetryableFailure(
                context
                    .retry_policy
                    .unwrap_or(RetryPolicy::default().with_reason(e)),
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

            // Response publish 错误不记录到错误计数
            // 因为这是消息队列的问题，不是爬取逻辑的问题
            // 如果需要监控 publish 失败，应该使用独立的监控系统

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
impl ProcessorTrait<(Request, Option<ModuleConfig>), (Request, Option<ModuleConfig>)>
for RequestMiddlewareProcessor
{
    fn name(&self) -> &'static str {
        "DownloadMiddlewareProcessor"
    }

    async fn process(
        &self,
        input: (Request, Option<ModuleConfig>),
        _context: ProcessorContext,
    ) -> ProcessorResult<(Request, Option<ModuleConfig>)> {
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
impl EventProcessorTrait<(Request, Option<ModuleConfig>), (Request, Option<ModuleConfig>)>
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
        out: &(Request, Option<ModuleConfig>),
    ) -> Option<EventEnvelope> {
        Some(EventEnvelope::engine(
            EventType::RequestMiddleware,
            EventPhase::Completed,
            RequestMiddlewareEvent::from(&out.0),
        ))
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
        ProcessorResult::Success(Some(modified_response))
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
        .then_silent::<(Request, Option<ModuleConfig>), _>(request_middleware)
        .then::<(Option<Response>, Option<ModuleConfig>), _>(download_processor)
        .then_silent::<Option<Response>, _>(response_middleware)
        .then::<(), _>(response_publish)
}
