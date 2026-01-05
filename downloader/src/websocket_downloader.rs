use tokio_tungstenite::{connect_async, tungstenite::Message};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{mpsc, Mutex};
use futures_util::{SinkExt, StreamExt};
use kernel::model::{Request, Response};
use log::{error, info, warn};
use error::{Error, Result};
use uuid::Uuid;
use reqwest::{Client, Proxy};
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::WebSocketStream;
use tokio::io::{AsyncRead, AsyncWrite};

/// WebSocketDownloader 管理多个 WebSocket 连接
/// 每个 module_id 对应一个连接

#[derive(Clone)]
pub struct WebSocketDownloader {
    // 存储活跃的连接发送端，用于发送消息
    // Key: module_id
    connections: Arc<Mutex<HashMap<String, mpsc::Sender<Message>>>>,
    // 监听器映射：Key: module_id -> Value: Response Sender
    // 每个模块可以注册自己的接收通道，实现消息隔离
    listeners: Arc<Mutex<HashMap<String, mpsc::Sender<Response>>>>,
    // 活跃连接计数器
    active_connections: Arc<Mutex<usize>>,
}

impl Default for WebSocketDownloader {
    fn default() -> Self {
        Self::new()
    }
}

impl WebSocketDownloader {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            listeners: Arc::new(Mutex::new(HashMap::new())),
            active_connections: Arc::new(Mutex::new(0)),
        }
    }

    /// 注册监听器
    pub async fn subscribe(&self, module_id: String, sender: mpsc::Sender<Response>) {
        let mut listeners = self.listeners.lock().await;
        listeners.insert(module_id, sender);
    }

    /// 取消注册监听器
    pub async fn unsubscribe(&self, module_id: &str) {
        let mut listeners = self.listeners.lock().await;
        listeners.remove(module_id);
    }
    
    /// 获取活跃连接数
    pub async fn active_count(&self) -> usize {
        *self.active_connections.lock().await
    }
    fn spawn_task<S>(&self, stream: WebSocketStream<S>, module_id: String, request: Request, mut rx: mpsc::Receiver<Message>)
    where S: AsyncRead + AsyncWrite + Unpin + Send + 'static 
    {
        let (mut write, mut read) = stream.split();
        let listeners = self.listeners.clone();
        let module_id_clone = module_id.clone();
        let request_clone = request.clone();
        let active_connections = self.active_connections.clone();
        
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // 1. 处理发送消息 (Write)
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if let Err(e) = write.send(msg).await {
                                    error!("[WebSocketDownloader] Write error for module {}: {}", module_id_clone, e);
                                    break;
                                }
                            }
                            None => break, // 发送通道已关闭
                        }
                    }
                    // 2. 处理接收消息 (Read)
                    item = read.next() => {
                        match item {
                            Some(Ok(msg)) if msg.is_text() || msg.is_binary() => {
                                let data = msg.into_data();
                                let response = Response {
                                    id: Uuid::now_v7(),
                                    platform: request_clone.platform.clone(),
                                    account: request_clone.account.clone(),
                                    module: request_clone.module.clone(),
                                    status_code: 200,
                                    cookies: Default::default(),
                                    content: data.to_vec(),
                                    headers: vec![],
                                    task_retry_times: request_clone.task_retry_times,
                                    metadata: request_clone.meta.clone(),
                                    download_middleware: request_clone.download_middleware.clone(),
                                    data_middleware: request_clone.data_middleware.clone(),
                                    task_finished: false,
                                    context: request_clone.context.clone(),
                                    run_id: request_clone.run_id,
                                    prefix_request: request_clone.id,
                                    request_hash: None,
                                };
                                
                                // 分发消息给对应的监听器
                                let listeners_guard = listeners.lock().await;
                                if let Some(sender) = listeners_guard.get(&module_id_clone) {
                                    if let Err(e) = sender.send(response).await {
                                        warn!("[WebSocketDownloader] Failed to forward response for module {} (receiver dropped): {}", module_id_clone, e);
                                        // 这里不退出循环，因为可能只是当前的 Chain 停止了，连接还需要保持
                                    }
                                } else {
                                    warn!("[WebSocketDownloader] No listener registered for module {}, dropping message", module_id_clone);
                                }
                            }
                            Some(Ok(Message::Close(_))) => {
                                info!("[WebSocketDownloader] Connection closed by server for module {}", module_id_clone);
                                break;
                            }
                            Some(Ok(_)) => {} // Ignore Ping/Pong
                            Some(Err(e)) => {
                                error!("[WebSocketDownloader] Read error for module {}: {}", module_id_clone, e);
                                break;
                            }
                            None => break, // 连接已关闭
                        }
                    }
                }
            }
            // 清理工作
            let _ = write.close().await;
            
            // 减少活跃连接计数
            let mut count = active_connections.lock().await;
            if *count > 0 {
                *count -= 1;
            }
            info!("[WebSocketDownloader] Connection loop ended for module {}, active connections: {}", module_id_clone, *count);
        });
    }

    /// 发送请求
    /// 如果连接不存在，会自动建立连接
    /// 如果连接已存在，直接复用
    pub async fn send(&self, request: Request) -> Result<()> {
        let module_id = request.module_id();
        let mut connections = self.connections.lock().await;

        // 检查连接是否存在且活跃
        if let Some(sender) = connections.get(&module_id) {
            if !sender.is_closed() {
                // 连接存在，直接发送数据
                if let Some(body) = &request.body {
                    let msg = Message::Text(String::from_utf8_lossy(body).to_string().into());
                    if let Err(e) = sender.send(msg).await {
                        warn!("[WebSocketDownloader] Failed to send message to existing connection: {}, reconnecting...", e);
                        // 发送失败，移除旧连接，准备重连
                        connections.remove(&module_id);
                    } else {
                        return Ok(());
                    }
                } else {
                    // 没有 body，仅建立连接或保持活跃
                    return Ok(());
                }
            } else {
                // 连接已关闭，移除
                connections.remove(&module_id);
            }
        }

        // 建立新连接
        info!("[WebSocketDownloader] Connecting to {} for module {}", request.url, module_id);
        
        // 增加活跃连接计数
        {
            let mut count = self.active_connections.lock().await;
            *count += 1;
        }
        
        // 创建发送通道，用于向 WebSocket 发送消息
        let (tx, rx) = mpsc::channel::<Message>(32);

        if let Some(proxy_config) = &request.proxy {
            let proxy_url = proxy_config.to_string();
            let proxy = Proxy::all(&proxy_url).map_err(|e| Error::download_failed(format!("Invalid proxy: {}", e)))?;
            let client = Client::builder().proxy(proxy).build().map_err(|e| Error::download_failed(format!("Client build failed: {}", e)))?;
            
            let url = request.url.replace("ws://", "http://").replace("wss://", "https://");
            let req = client.get(&url)
                .header("Connection", "Upgrade")
                .header("Upgrade", "websocket")
                .header("Sec-WebSocket-Version", "13")
                .header("Sec-WebSocket-Key", generate_key())
                .send()
                .await
                .map_err(|e| Error::download_failed(format!("Proxy request failed: {}", e)))?;
                
            if req.status() != reqwest::StatusCode::SWITCHING_PROTOCOLS {
                 return Err(Error::download_failed(format!("Proxy handshake failed: status {}", req.status())));
            }
            
            let upgraded = req.upgrade().await.map_err(|e| Error::download_failed(format!("Upgrade failed: {}", e)))?;
            let stream = WebSocketStream::from_raw_socket(upgraded, Role::Client, None).await;
            
            self.spawn_task(stream, module_id.clone(), request.clone(), rx);
        } else {
            let (ws_stream, _) = connect_async(&request.url).await.map_err(|e| {
                Error::download_failed(format!("WebSocket connect failed: {}", e))
            })?;
            self.spawn_task(ws_stream, module_id.clone(), request.clone(), rx);
        }

        // 保存连接
        connections.insert(module_id, tx.clone());

        // 如果有初始数据，发送
        if let Some(body) = &request.body {
            let msg = Message::Text(String::from_utf8_lossy(body).to_string().into());
            tx.send(msg).await.map_err(|e| {
                Error::download_failed(format!("Failed to send initial message: {}", e))
            })?;
        }

        Ok(())
    }

    /// 关闭指定模块的连接
    pub async fn close(&self, module_id: &str) {
        let mut connections = self.connections.lock().await;
        if let Some(sender) = connections.remove(module_id) {
            // 发送 Close 消息或者直接 Drop sender 都会触发连接关闭
            let _ = sender.send(Message::Close(None)).await;
        }
    }
}
