use crate::cacheable::{CacheAble, CacheService};
use crate::common::context::PipelineContext;
use crate::common::interface::MiddlewareManager;
use crate::common::model::{ModuleConfig, Request};
use crate::common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use crate::common::stream_stats::StreamStats;
use crate::downloader::{DownloaderManager, WebSocketDownloader};
use crate::engine::chain::proxy_attempt::{
    ProxyAttempt, clear_proxy_failure, is_proxy_failure, mark_proxy_failure, proxy_status_code,
    select_retry_proxy,
};
use crate::engine::chain::{ConfigProcessor, ProxyMiddlewareProcessor, RequestMiddlewareProcessor};
use crate::engine::events::{DownloadEvent, EventBus, EventEnvelope, EventPhase, EventType};
use crate::engine::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use crate::errors::Error;
use crate::queue::{QueueManager, QueuedItem};
use async_trait::async_trait;
use log::{error, warn};
use metrics::counter;
use mocra_proxy::ProxyManager;
use serde_json::json;
use std::sync::Arc;

/// WebSocket download processor that delegates response publishing in post-process.
///
/// This processor emits `()` on success because actual response handling is driven
/// by the websocket subscription loop started in `post_process`.
struct WebSocketDownloadProcessor {
    proxy_manager: Option<Arc<ProxyManager>>,
    queue_manager: Arc<QueueManager>,
    wss_downloader: Arc<WebSocketDownloader>,
    middleware_manager: Arc<MiddlewareManager>,
    cache_service: Arc<CacheService>,
    state: Arc<PipelineContext>,
}

#[async_trait]
impl ProcessorTrait<(Option<Request>, Option<ModuleConfig>), ()> for WebSocketDownloadProcessor {
    fn name(&self) -> &'static str {
        "WebSocketDownloadProcessor"
    }

    async fn process(
        &self,
        input: (Option<Request>, Option<ModuleConfig>),
        context: ProcessorContext,
    ) -> ProcessorResult<()> {
        let mut request = match input.0 {
            Some(request) => request,
            None => return ProcessorResult::Success(()),
        };
        select_retry_proxy(
            &self.proxy_manager,
            &mut request,
            &input.1,
            &context.retry_policy,
        )
        .await;
        let request_id = request.id;
        let module_id = request.module_id();
        let used_proxy = request.proxy.clone();
        let mut proxy_attempt = match ProxyAttempt::begin(&self.proxy_manager, &request).await {
            Ok(attempt) => attempt,
            Err(error) => {
                counter!("mocra_proxy_selection_errors_total").increment(1);
                let mut policy = context.retry_policy.unwrap_or_default();
                if let Some(proxy) = &used_proxy {
                    mark_proxy_failure(&mut policy, proxy);
                }
                return if policy.should_retry() {
                    ProcessorResult::RetryableFailure(policy.with_reason(error.to_string()))
                } else {
                    ProcessorResult::FatalFailure(error)
                };
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
        let response = self.wss_downloader.send(request).await;
        match response {
            // For websocket mode, response forwarding happens asynchronously in post-process.
            Ok(_resp) => {
                if let Some(attempt) = proxy_attempt.as_mut() {
                    attempt.finish(true).await;
                }
                ProcessorResult::Success(())
            }
            Err(e) => {
                let provider_retry_status = if let (Some(manager), Some(proxy), Some(status)) =
                    (&self.proxy_manager, &used_proxy, proxy_status_code(&e))
                {
                    manager.is_retry_code(proxy, status).await
                } else {
                    false
                };
                let proxy_failure =
                    used_proxy.is_some() && (is_proxy_failure(&e) || provider_retry_status);
                if let Some(attempt) = proxy_attempt.as_mut() {
                    attempt.finish(!proxy_failure).await;
                }
                let mut policy = context.retry_policy.unwrap_or_default();
                if proxy_failure {
                    if let Some(proxy) = &used_proxy {
                        mark_proxy_failure(&mut policy, proxy);
                    }
                } else {
                    clear_proxy_failure(&mut policy);
                }
                warn!(
                    "[WebSocketDownloadProcessor] download failed, will retry: request_id={} module_id={} error={}",
                    request_id, module_id, e
                );
                ProcessorResult::RetryableFailure(policy.with_reason(e.to_string()))
            }
        }
    }
    async fn post_process(
        &self,
        input: &(Option<Request>, Option<ModuleConfig>),
        _output: &(),
        _context: &ProcessorContext,
    ) -> crate::errors::Result<()> {
        let request = match &input.0 {
            Some(request) => request,
            None => return Ok(()),
        };
        // Read responses from websocket receiver and publish through queue manager.
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let timeout = input
            .1
            .as_ref()
            .and_then(|x| x.get_config::<u64>("wss_timeout"))
            .unwrap_or(self.state.config.read().await.download_config.wss_timeout as u64);
        let module_id = request.module_id();

        // Register module-scoped subscription.
        self.wss_downloader.subscribe(module_id.clone(), tx).await;

        let sender = self.queue_manager.get_response_push_channel().clone();
        let queue_manager = self.queue_manager.clone();
        let middleware_manager = self.middleware_manager.clone();
        let config = input.1.clone();
        let wss_downloader = self.wss_downloader.clone();
        let module_id_clone = module_id.clone();
        let run_id = request.run_id;
        let cache_service = self.cache_service.clone();
        tokio::spawn(async move {
            use tokio::time::{Duration, interval};
            let mut stop_check = interval(Duration::from_secs(5));
            let mut last_activity = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    _ = stop_check.tick() => {
                        let key = format!("run:{}:module:{}", run_id, module_id_clone);
                        // Check distributed stop flag.
                        let stream_stats = StreamStats::sync(&key,&cache_service).await;
                        if let Ok(Some(val)) = stream_stats
                             && val.0{
                                 log::info!("[ResponsePublish] Module {} stopped, closing connection...", module_id_clone);
                                 wss_downloader.close(&module_id_clone).await;
                                 break;
                             }

                        // Check idle timeout.
                        if last_activity.elapsed() > Duration::from_secs(timeout) {
                             let active = wss_downloader.active_count().await;
                             if active == 0 {
                                log::info!("[ResponsePublish] No active WebSocket connections and idle for 60s, exiting...");
                                break;
                             }
                        }
                    }
                    res = rx.recv() => {
                        last_activity = tokio::time::Instant::now();
                        match res {
                            Some(response) => {
                                // Handle middleware + queue publish.
                                let modified_response = middleware_manager.handle_response(response, &config).await;
                                let Some(modified_response) = modified_response else {
                                    continue;
                                };
                                let item = QueuedItem::new(modified_response);
                                if let Err(e) = match queue_manager.try_send_local_response(item) {
                                    Ok(_) => Ok(()),
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(returned_item))
                                    | Err(tokio::sync::mpsc::error::TrySendError::Closed(returned_item)) => {
                                        sender.send(returned_item).await.map_err(|e| e.to_string())
                                    }
                                } {
                                    error!("Failed to send response to queue: {e}");
                                    warn!("[ResponsePublish] will retry due to queue send error");
                                }
                            }
                            None => {
                                // Channel closed, exit loop gracefully.
                                log::info!("[ResponsePublish] WebSocket response channel closed for module {}, exiting...", module_id_clone);
                                break;
                            }
                        }
                    }
                }
            }
            // Always remove subscription on task exit.
            wss_downloader.unsubscribe(&module_id_clone).await;
            log::info!(
                "[ResponsePublish] Task completed for module {}",
                module_id_clone
            );
        });

        Ok(())
    }
}
impl EventProcessorTrait<(Option<Request>, Option<ModuleConfig>), ()>
    for WebSocketDownloadProcessor
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
                "wss_download_skipped_without_request",
                EventPhase::Completed,
            )),
        }
    }

    fn finish_status(
        &self,
        input: &(Option<Request>, Option<ModuleConfig>),
        _output: &(),
    ) -> Option<EventEnvelope> {
        match &input.0 {
            Some(request) => {
                let ev: DownloadEvent = request.into();
                Some(EventEnvelope::engine(
                    EventType::Download,
                    EventPhase::Completed,
                    ev,
                ))
            }
            None => Some(EventEnvelope::system_error(
                "wss_download_skipped_without_request",
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
                "wss_download_skipped_without_request",
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
                format!("wss_download_skipped_with_error: {err}"),
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
                "wss_download_skipped_retry_without_request",
                EventPhase::Completed,
            )),
        }
    }
}

/// Builds websocket request chain:
/// config -> proxy middleware -> request middleware -> websocket download/subscription.
pub async fn create_wss_download_chain(
    state: Arc<PipelineContext>,
    downloader_manager: Arc<DownloaderManager>,
    queue_manager: Arc<QueueManager>,
    middleware_manager: Arc<MiddlewareManager>,
    cache_service: Arc<CacheService>,
    event_bus: Option<Arc<EventBus>>,
    proxy_manager: Option<Arc<ProxyManager>>,
) -> EventAwareTypedChain<Request, ()> {
    let download_processor = WebSocketDownloadProcessor {
        proxy_manager: proxy_manager.clone(),
        queue_manager: queue_manager.clone(),
        wss_downloader: downloader_manager.wss_downloader.clone(),
        middleware_manager: middleware_manager.clone(),
        cache_service: cache_service.clone(),
        state: state.clone(),
    };

    let request_middleware = RequestMiddlewareProcessor {
        middleware_manager: middleware_manager.clone(),
    };
    let config_processor = ConfigProcessor {
        state: state.clone(),
    };
    let proxy_middleware = ProxyMiddlewareProcessor { proxy_manager };

    EventAwareTypedChain::<Request, Request>::new(event_bus)
        .then::<(Request, Option<ModuleConfig>), _>(config_processor)
        .then::<(Request, Option<ModuleConfig>), _>(proxy_middleware)
        .then::<(Option<Request>, Option<ModuleConfig>), _>(request_middleware)
        .then::<(), _>(download_processor)
}

#[cfg(test)]
mod proxy_rotation_tests {
    use super::*;
    use crate::common::config::ConfigProvider;
    use crate::common::model::config::Config;
    use crate::common::state::State;
    use mocra_proxy::{DirectProxy, PoolConfig, ProxyConfig};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::watch;

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

    async fn rejecting_proxy() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n",
                )
                .await
                .unwrap();
        });
        (url, task)
    }

    #[tokio::test]
    async fn websocket_retries_another_proxy_after_auth_failure() {
        let config: Config = toml::from_str(
            r#"
            name = "websocket_rotation_test"
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
        .unwrap();
        let state = Arc::new(
            State::try_new_with_provider(Box::new(StaticConfig(config)))
                .await
                .unwrap(),
        );
        let (url_a, served_a) = rejecting_proxy().await;
        let (url_b, served_b) = rejecting_proxy().await;
        let manager = Arc::new(
            ProxyManager::from_proxy_config(&ProxyConfig {
                tunnel: None,
                direct: Some(vec![
                    DirectProxy {
                        name: Some("a".into()),
                        url: url_a,
                        rate_limit: Some(10.0),
                        expire_time: None,
                    },
                    DirectProxy {
                        name: Some("b".into()),
                        url: url_b,
                        rate_limit: Some(10.0),
                        expire_time: None,
                    },
                ]),
                ip_provider: None,
                pool_config: Some(PoolConfig::default()),
            })
            .await
            .unwrap(),
        );
        let processor = WebSocketDownloadProcessor {
            proxy_manager: Some(manager.clone()),
            queue_manager: Arc::new(QueueManager::new(None, 10)),
            wss_downloader: Arc::new(WebSocketDownloader::new()),
            middleware_manager: Arc::new(MiddlewareManager::new()),
            cache_service: state.cache_service.clone(),
            state: state.pipeline_ctx(),
        };
        let mut request = Request::new("ws://example.test/stream", "WSS");
        request.proxy = Some(manager.get_proxy_for_attempt(None).await.unwrap());
        request.proxy_from_pool = true;
        let mut policy = match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            processor.process((Some(request.clone()), None), ProcessorContext::default()),
        )
        .await
        .unwrap()
        {
            ProcessorResult::RetryableFailure(policy) => policy,
            other => panic!("expected authentication failure: {other:?}"),
        };
        policy.current_retry = 1;
        assert!(matches!(
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                processor.process(
                    (Some(request), None),
                    ProcessorContext::default().with_retry_policy(policy)
                )
            )
            .await
            .unwrap(),
            ProcessorResult::RetryableFailure(_)
        ));
        served_a.await.unwrap();
        served_b.await.unwrap();
        assert_eq!(manager.get_detailed_stats().await.total_proxies, 2);
        assert_eq!(manager.get_detailed_stats().await.avg_success_rate, 0.0);
    }
}
