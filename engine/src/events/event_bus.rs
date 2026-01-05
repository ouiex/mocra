use super::SystemEvent;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::{RwLock, Semaphore};
use std::collections::HashMap;
use log::{debug, error};

/// 事件处理器trait
#[async_trait]
pub trait EventHandler: Send + Sync {
    async fn handle(&self, event: &SystemEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn name(&self) -> &'static str;
}

/// 事件总线
pub struct EventBus {
    handlers: Arc<RwLock<HashMap<String, Vec<Arc<dyn EventHandler>>>>>,
    sender: mpsc::Sender<SystemEvent>,
    _receiver: Arc<RwLock<Option<mpsc::Receiver<SystemEvent>>>>,
    concurrency_limit: Arc<Semaphore>,
}

impl EventBus {
    pub fn new() -> Self {
        // Use a bounded channel to prevent unbounded memory growth under backpressure.
        let (sender, receiver) = mpsc::channel(1024);
        
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
            sender,
            _receiver: Arc::new(RwLock::new(Some(receiver))),
            // Cap concurrent handler tasks to avoid unbounded task growth
            concurrency_limit: Arc::new(Semaphore::new(64)),
        }
    }

    /// 注册事件处理器
    pub async fn register_handler<T: EventHandler + 'static>(&self, event_type: String, handler: T) {
        let mut handlers = self.handlers.write().await;
        handlers
            .entry(event_type)
            .or_insert_with(Vec::new)
            .push(Arc::new(handler));
    }

    /// 发布事件
    pub async fn publish(&self, event: SystemEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        
        if let Err(e) = self.sender.send(event).await {
            error!("Failed to publish event: {e}");
            return Err(Box::new(e));
        }
        
        Ok(())
    }

    /// 启动事件处理循环
    pub async fn start(&self) {
    let receiver = {
            let mut receiver_guard = self._receiver.write().await;
            receiver_guard.take()
        };

    if let Some(mut receiver) = receiver {
            let handlers = Arc::clone(&self.handlers);
            let concurrency = Arc::clone(&self.concurrency_limit);
            
            tokio::spawn(async move {
                while let Some(event) = receiver.recv().await {
                    let event_type = event.event_type().to_string();
                    let handlers_guard = handlers.read().await;
                    
                    if let Some(event_handlers) = handlers_guard.get(&event_type) {
                        for handler in event_handlers {
                            let handler = Arc::clone(handler);
                            let event = event.clone();
                            let sem = concurrency.clone();
                            tokio::spawn(async move {
                                let _permit = sem.acquire_owned().await.expect("semaphore closed");
                                if let Err(e) = handler.handle(&event).await {
                                    error!("Event handler '{}' failed: {}", handler.name(), e);
                                }
                            });
                        }
                    }
                    
                    // 处理通用事件处理器（监听所有事件）
                    if let Some(universal_handlers) = handlers_guard.get("*") {
                        for handler in universal_handlers {
                            let handler = Arc::clone(handler);
                            let event = event.clone();
                            let sem = concurrency.clone();
                            tokio::spawn(async move {
                                let _permit = sem.acquire_owned().await.expect("semaphore closed");
                                if let Err(e) = handler.handle(&event).await {
                                    error!("Universal event handler '{}' failed: {}", handler.name(), e);
                                }
                            });
                        }
                    }
                }
            });
        }
    }

    /// 停止事件总线
    pub fn stop(&self) {
        // 关闭发送端，这将导致接收循环退出
        // 注意：这会消费掉sender，所以这个方法只能调用一次
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

/// 默认的日志事件处理器
pub struct LogEventHandler;

#[async_trait]
impl EventHandler for LogEventHandler {
    async fn handle(&self, event: &SystemEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Event logged: {} at {}", event.event_type(), event.timestamp());
        Ok(())
    }

    fn name(&self) -> &'static str {
        "LogEventHandler"
    }
}

/// 指标收集事件处理器
pub struct MetricsEventHandler {
    // 这里可以添加指标收集的具体实现
}

impl MetricsEventHandler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl EventHandler for MetricsEventHandler {
    async fn handle(&self, _event: &SystemEvent) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 根据事件类型收集相应的指标
        // match event {
        //     SystemEvent::RequestCompleted(_) => {
        //         // 增加成功请求计数
        //     }
        //     SystemEvent::RequestFailed(_) => {
        //         // 增加失败请求计数
        //     }
        //     SystemEvent::DownloadCompleted(download_event) => {
        //         // 记录下载时间和大小
        //         debug!("Download completed in {}ms, size: {} bytes", 
        //               download_event.duration_ms, download_event.response_size);
        //     }
        //     _ => {}
        // }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "MetricsEventHandler"
    }
}
