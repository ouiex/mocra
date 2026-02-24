use errors::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use common::model::{Request, Response};
use log::{error, info, warn};
use reqwest::{Client, Proxy};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

/// WebSocketDownloader manages multiple WebSocket connections.
/// Each `module_id` maps to one connection.

#[derive(Clone)]
pub struct WebSocketDownloader {
    // Stores active connection senders used to send messages.
    // Key: module_id
    connections: Arc<Mutex<HashMap<String, mpsc::Sender<Message>>>>,
    // Listener map: Key: module_id -> Value: Response sender.
    // Each module can register its own receive channel for message isolation.
    listeners: Arc<Mutex<HashMap<String, mpsc::Sender<Response>>>>,
    // Active connection counter.
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

    /// Registers a listener.
    pub async fn subscribe(&self, module_id: String, sender: mpsc::Sender<Response>) {
        let mut listeners = self.listeners.lock().await;
        listeners.insert(module_id, sender);
    }

    /// Unregisters a listener.
    pub async fn unsubscribe(&self, module_id: &str) {
        let mut listeners = self.listeners.lock().await;
        listeners.remove(module_id);
    }

    /// Returns the number of active connections.
    pub async fn active_count(&self) -> usize {
        *self.active_connections.lock().await
    }
    fn spawn_task<S>(
        &self,
        stream: WebSocketStream<S>,
        module_id: String,
        request: Request,
        mut rx: mpsc::Receiver<Message>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut write, mut read) = stream.split();
        let listeners = self.listeners.clone();
        let module_id_clone = module_id.clone();
        let request_clone = request.clone();
        let active_connections = self.active_connections.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // 1. Handle outbound messages (write).
                    msg = rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if let Err(e) = write.send(msg).await {
                                    error!("[WebSocketDownloader] Write error for module {}: {}", module_id_clone, e);
                                    break;
                                }
                            }
                            None => break, // Send channel closed.
                        }
                    }
                    // 2. Handle inbound messages (read).
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
                                    storage_path: None,
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
                                    priority: request_clone.priority,
                                };

                                // Dispatch message to the corresponding listener.
                                let listeners_guard = listeners.lock().await;
                                if let Some(sender) = listeners_guard.get(&module_id_clone) {
                                    if let Err(e) = sender.send(response).await {
                                        warn!("[WebSocketDownloader] Failed to forward response for module {} (receiver dropped): {}", module_id_clone, e);
                                        // Do not break here: the current chain may have stopped,
                                        // but the connection should remain alive.
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
                            None => break, // Connection closed.
                        }
                    }
                }
            }
            // Cleanup.
            let _ = write.close().await;

            // Decrease active connection count.
            let mut count = active_connections.lock().await;
            if *count > 0 {
                *count -= 1;
            }
            info!(
                "[WebSocketDownloader] Connection loop ended for module {}, active connections: {}",
                module_id_clone, *count
            );
        });
    }

    /// Sends a request.
    /// If the connection does not exist, it will be created automatically.
    /// If the connection already exists, it will be reused.
    pub async fn send(&self, request: Request) -> Result<()> {
        let module_id = request.module_id();
        let mut connections = self.connections.lock().await;

        // Check whether a connection exists and is active.
        if let Some(sender) = connections.get(&module_id) {
            if !sender.is_closed() {
                // Connection exists, send data directly.
                if let Some(body) = &request.body {
                    let msg = Message::Text(String::from_utf8_lossy(body).to_string().into());
                    if let Err(e) = sender.send(msg).await {
                        warn!(
                            "[WebSocketDownloader] Failed to send message to existing connection: {}, reconnecting...",
                            e
                        );
                        // Sending failed: remove old connection and prepare to reconnect.
                        connections.remove(&module_id);
                    } else {
                        return Ok(());
                    }
                } else {
                    // No body: only establish connection or keep it alive.
                    return Ok(());
                }
            } else {
                // Connection is closed: remove it.
                connections.remove(&module_id);
            }
        }

        // Establish a new connection.
        info!(
            "[WebSocketDownloader] Connecting to {} for module {}",
            request.url, module_id
        );

        // Increase active connection count.
        {
            let mut count = self.active_connections.lock().await;
            *count += 1;
        }

        // Create send channel for WebSocket outbound messages.
        let (tx, rx) = mpsc::channel::<Message>(32);

        if let Some(proxy_config) = &request.proxy {
            let proxy_url = proxy_config.to_string();
            let proxy = Proxy::all(&proxy_url)
                .map_err(|e| Error::download_failed(format!("Invalid proxy: {}", e)))?;
            let client = Client::builder()
                .proxy(proxy)
                .build()
                .map_err(|e| Error::download_failed(format!("Client build failed: {}", e)))?;

            let url = request
                .url
                .replace("ws://", "http://")
                .replace("wss://", "https://");
            let req = client
                .get(&url)
                .header("Connection", "Upgrade")
                .header("Upgrade", "websocket")
                .header("Sec-WebSocket-Version", "13")
                .header("Sec-WebSocket-Key", generate_key())
                .send()
                .await
                .map_err(|e| Error::download_failed(format!("Proxy request failed: {}", e)))?;

            if req.status() != reqwest::StatusCode::SWITCHING_PROTOCOLS {
                return Err(Error::download_failed(format!(
                    "Proxy handshake failed: status {}",
                    req.status()
                )));
            }

            let upgraded = req
                .upgrade()
                .await
                .map_err(|e| Error::download_failed(format!("Upgrade failed: {}", e)))?;
            let stream = WebSocketStream::from_raw_socket(upgraded, Role::Client, None).await;

            self.spawn_task(stream, module_id.clone(), request.clone(), rx);
        } else {
            let (ws_stream, _) = connect_async(&request.url)
                .await
                .map_err(|e| Error::download_failed(format!("WebSocket connect failed: {}", e)))?;
            self.spawn_task(ws_stream, module_id.clone(), request.clone(), rx);
        }

        // Save connection.
        connections.insert(module_id, tx.clone());

        // Send initial payload if present.
        if let Some(body) = &request.body {
            let msg = Message::Text(String::from_utf8_lossy(body).to_string().into());
            tx.send(msg).await.map_err(|e| {
                Error::download_failed(format!("Failed to send initial message: {}", e))
            })?;
        }

        Ok(())
    }

    /// Closes the connection for the specified module.
    pub async fn close(&self, module_id: &str) {
        let mut connections = self.connections.lock().await;
        if let Some(sender) = connections.remove(module_id) {
            // Sending Close or dropping sender will both trigger connection shutdown.
            let _ = sender.send(Message::Close(None)).await;
        }
    }
}
