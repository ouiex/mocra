use crate::chain::{
    ConfigProcessor, ProxyMiddlewareProcessor, RequestMiddlewareProcessor,
};
use crate::events::{
    DownloadEvent, EventBus, EventEnvelope, EventPhase, EventType,
};
use serde_json::json;
use crate::processors::event_processor::{EventAwareTypedChain, EventProcessorTrait};
use async_trait::async_trait;
use downloader::{DownloaderManager, WebSocketDownloader};
use errors::Error;
use log::{error, warn};
use queue::{QueueManager, QueuedItem};
use common::processors::processor::{
    ProcessorContext, ProcessorResult, ProcessorTrait, RetryPolicy,
};
use proxy::ProxyManager;
use std::sync::Arc;
use cacheable::{CacheAble, CacheService};
use common::interface::MiddlewareManager;
use common::model::{ModuleConfig, Request};
use common::state::State;
use common::stream_stats::StreamStats;

struct WebSocketDownloadProcessor {
    queue_manager: Arc<QueueManager>,
    wss_downloader: Arc<WebSocketDownloader>,
    middleware_manager: Arc<MiddlewareManager>,
    cache_service: Arc<CacheService>,
}
#[async_trait]
impl ProcessorTrait<(Request, Option<ModuleConfig>), ()> for WebSocketDownloadProcessor {
    fn name(&self) -> &'static str {
        "WebSocketDownloadProcessor"
    }

    async fn process(
        &self,
        input: (Request, Option<ModuleConfig>),
        context: ProcessorContext,
    ) -> ProcessorResult<()> {
        let (request, _) = input;
        let request_id = request.id;
        let module_id = request.module_id();
        let response = self.wss_downloader.send(request).await;
        match response {
            // web_socket_downloader需要持有一个与queue_manager不同的消息队列，需要取出Response进行middleware处理和发布
            // 这种情况下，返回值为()，表示下载成功，需要在post_process中使用middleware处理和发布
            // post_process
            Ok(_resp) => ProcessorResult::Success(()),
            Err(e) => {
                warn!(
                    "[WebSocketDownloadProcessor] download failed, will retry: request_id={} module_id={} error={}",
                    request_id,
                    module_id,
                    e
                );
                ProcessorResult::RetryableFailure(context.retry_policy.unwrap_or_default())
            }
        }
    }
    async fn post_process(
        &self,
        input: &(Request, Option<ModuleConfig>),
        _output: &(),
        _context: &ProcessorContext,
    ) -> errors::Result<()> {
        // 从response_recv中取出Response进行middleware处理和发布
        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        let timeout = input.1.as_ref().and_then(|x|x.get_config::<u64>("wss_timeout")).unwrap_or(60);
        let module_id = input.0.module_id();


        // 注册监听器，确保只接收当前模块的消息
        self.wss_downloader.subscribe(module_id.clone(), tx).await;

        let sender = self.queue_manager.get_response_push_channel().clone();
        let queue_manager = self.queue_manager.clone();
        let middleware_manager = self.middleware_manager.clone();
        let config = input.1.clone();
        let wss_downloader = self.wss_downloader.clone();
        let module_id_clone = module_id.clone();
        let run_id = input.0.run_id;
        let cache_service = self.cache_service.clone();
        tokio::spawn(async move {
            use tokio::time::{Duration, interval};
            let mut stop_check = interval(Duration::from_secs(5));
            let mut last_activity = tokio::time::Instant::now();
            loop {
                tokio::select! {
                    _ = stop_check.tick() => {
                        let key = format!("run:{}:module:{}", run_id, module_id_clone);
                        // 检查 Redis 中的 stop 字段
                        let stream_stats = StreamStats::sync(&key,&cache_service).await;
                        if let Ok(Some(val)) = stream_stats
                             && val.0{
                                 log::info!("[ResponsePublish] Module {} stopped, closing connection...", module_id_clone);
                                 wss_downloader.close(&module_id_clone).await;
                                 break;
                             }
                        
                        // 检查空闲超时 (60s)
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
                                // 收到响应，进行处理
                                let modified_response = middleware_manager.handle_response(response, &config).await;
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
                                // 通道已关闭，正常退出
                                log::info!("[ResponsePublish] WebSocket response channel closed for module {}, exiting...", module_id_clone);
                                break;
                            }
                        }
                    }
                }
            }
            // 任务结束时取消注册
            wss_downloader.unsubscribe(&module_id_clone).await;
            log::info!("[ResponsePublish] Task completed for module {}", module_id_clone);
        });

        Ok(())
    }
}
impl EventProcessorTrait<(Request, Option<ModuleConfig>), ()> for WebSocketDownloadProcessor {
    fn pre_status(&self, input: &(Request, Option<ModuleConfig>)) -> Option<EventEnvelope> {
        let ev: DownloadEvent = (&input.0).into();
        Some(EventEnvelope::engine(EventType::Download, EventPhase::Started, ev))
    }

    fn finish_status(&self, input: &(Request, Option<ModuleConfig>), _output: &()) -> Option<EventEnvelope> {
        let ev: DownloadEvent = (&input.0).into();

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

pub async fn create_wss_download_chain(
    state:Arc<State>,
    downloader_manager: Arc<DownloaderManager>,
    queue_manager: Arc<QueueManager>,
    middleware_manager: Arc<MiddlewareManager>,
    cache_service: Arc<CacheService>,
    event_bus: Option<Arc<EventBus>>,
    proxy_manager: Option<Arc<ProxyManager>>,
) -> EventAwareTypedChain<Request, ()> {
    let download_processor = WebSocketDownloadProcessor {
        queue_manager: queue_manager.clone(),
        wss_downloader: downloader_manager.wss_downloader.clone(),
        middleware_manager: middleware_manager.clone(),
        cache_service: cache_service.clone(),
    };

    let request_middleware = RequestMiddlewareProcessor {
        middleware_manager: middleware_manager.clone(),
    };
    let config_processor = ConfigProcessor { state: state.clone() };
    let proxy_middleware = ProxyMiddlewareProcessor { proxy_manager };

    EventAwareTypedChain::<Request, Request>::new(event_bus)
        .then::<(Request, Option<ModuleConfig>), _>(config_processor)
        .then::<(Request, Option<ModuleConfig>), _>(proxy_middleware)
        .then::<(Request, Option<ModuleConfig>), _>(request_middleware)
        .then::<(), _>(download_processor)
}
