use crate::MqBackend;
use crate::channel::Channel;
use crate::compensation::{Compensator, Identifiable};
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use common::model::{Request, Response};
use log::{debug, error};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::sync::mpsc::{Receiver, Sender};
use utils::logger::LogModel;

pub struct QueueManager {
    pub channel: Arc<Channel>,
    pub backend: Option<Arc<dyn MqBackend>>,
    pub compensator: Option<Arc<dyn Compensator>>,
}

impl QueueManager {
    pub fn new(backend: Option<Arc<dyn MqBackend>>, capacity: usize) -> Self {
        QueueManager {
            channel: Arc::new(Channel::new(capacity)),
            backend,
            compensator: None,
        }
    }
    pub fn with_backend(&mut self, backend: Arc<dyn MqBackend>) {
        self.backend = Some(backend);
    }

    pub fn with_compensator(&mut self, compensator: Arc<dyn Compensator>) {
        self.compensator = Some(compensator);
    }

    pub fn subscribe(&self) {
        if let Some(backend) = &self.backend {
            let backend = backend.clone();
            let channel = self.channel.clone();
            let compensator = self.compensator.clone();

            let backend_clone = backend.clone();
            let channel_clone = channel.clone();
            let compensator_clone = compensator.clone();
            tokio::spawn(async move {
                Self::subscribe_topic::<TaskModel>(
                    "task",
                    channel_clone.task_sender.clone(),
                    backend_clone.clone(),
                    compensator_clone.clone(),
                )
                .await;
                Self::subscribe_topic::<Request>(
                    "request",
                    channel_clone.download_request_sender.clone(),
                    backend_clone.clone(),
                    compensator_clone.clone(),
                )
                .await;
                Self::subscribe_topic::<Response>(
                    "response",
                    channel_clone.remote_response_sender.clone(),
                    backend_clone.clone(),
                    compensator_clone.clone(),
                )
                .await;
                Self::subscribe_topic::<ParserTaskModel>(
                    "parser_task",
                    channel_clone.remote_parser_task_sender.clone(),
                    backend_clone.clone(),
                    compensator_clone.clone(),
                )
                .await;
                Self::subscribe_topic::<ErrorTaskModel>(
                    "error_task",
                    channel_clone.remote_error_sender.clone(),
                    backend_clone.clone(),
                    compensator_clone.clone(),
                )
                .await;
            });

            let backend_clone = backend.clone();
            let channel_clone = channel.clone();

            // TaskModel: Only subscribe (Inbound). Do NOT forward (Outbound) to avoid loops.
            // Request: Subscribe to download_request_sender (Inbound). Forward request_receiver (Outbound).
            // Response: Subscribe to remote_response_sender. Forward response_receiver.
            // ParserTask: Subscribe to remote_parser_task_sender. Forward parser_task_receiver.
            // ErrorTask: Subscribe to remote_error_sender. Forward error_receiver.

            // Note: We removed forward_channel for "task" because the Engine consumes task_receiver locally.
            // If we forward it, we create a loop: Redis -> task_sender -> task_receiver -> Redis.

            // Self::forward_channel::<TaskModel>("task", channel_clone.task_receiver.clone(), backend_clone.clone());
            Self::forward_channel::<Request>(
                "request",
                channel_clone.request_receiver.clone(),
                backend_clone.clone(),
            );
            Self::forward_channel::<Response>(
                "response",
                channel_clone.response_receiver.clone(),
                backend_clone.clone(),
            );
            Self::forward_channel::<ParserTaskModel>(
                "parser_task",
                channel_clone.parser_task_receiver.clone(),
                backend_clone.clone(),
            );
            Self::forward_channel::<ErrorTaskModel>(
                "error_task",
                channel_clone.error_receiver.clone(),
                backend_clone.clone(),
            );
            Self::forward_channel::<LogModel>(
                "log",
                channel_clone.log_receiver.clone(),
                backend_clone.clone(),
            );
        }
    }

    async fn subscribe_topic<T>(
        topic: &str,
        sender: Sender<T>,
        backend: Arc<dyn MqBackend>,
        compensator: Option<Arc<dyn Compensator>>,
    ) where
        T: serde::de::DeserializeOwned + Send + 'static + std::fmt::Debug + Identifiable,
    {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        if let Err(e) = backend.subscribe(topic, tx).await {
            error!("Failed to subscribe to topic {}: {}", topic, e);
            return;
        }

        let topic = topic.to_string();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Ok(item) = serde_json::from_slice::<T>(&msg.payload) {
                    let id = item.get_id();
                    debug!(
                        "[QueueManager] subscribe_topic received: topic={} id={}",
                        topic, id
                    );
                    // Add to compensation queue if configured
                    if let Some(comp) = &compensator
                        && let Err(e) = comp.add_task(&topic, &id, &msg.payload).await
                    {
                        error!(
                            "Failed to add task to compensation queue for topic {}: {}",
                            topic, e
                        );
                    }
                    debug!(
                        "[QueueManager] subscribe_topic comp_done: topic={} id={}",
                        topic, id
                    );

                    if sender.send(item).await.is_ok() {
                        debug!(
                            "[QueueManager] subscribe_topic sent: topic={} id={}",
                            topic, id
                        );
                        let _ = msg.ack().await;
                    }
                } else {
                    error!("Failed to deserialize message from topic {}", topic);
                }
            }
            log::warn!("Topic {} subscription closed", topic);
        });
    }

    fn forward_channel<T>(
        topic: &str,
        receiver: Arc<Mutex<Receiver<T>>>,
        backend: Arc<dyn MqBackend>,
    ) where
        T: serde::Serialize + Send + 'static + Identifiable,
    {
        let topic = topic.to_string();
        // Concurrency limit for publishing
        let semaphore = Arc::new(Semaphore::new(100));

        tokio::spawn(async move {
            let mut rx = receiver.lock().await;
            while let Some(item) = rx.recv().await {
                let id = item.get_id();
                // [LOG_OPTIMIZATION] Upgraded to INFO for performance analysis
                log::info!(
                    "[QueueManager] forward_channel received: topic={} id={}",
                    topic, id
                );
                
                // Acquire permit to limit concurrency
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break, // Semaphore closed
                };

                let backend = backend.clone();
                let topic = topic.clone();

                match serde_json::to_vec(&item) {
                    Ok(payload) => {
                        tokio::spawn(async move {
                            // Permit is moved into the closure and dropped when task completes
                            let _permit = permit;
                            
                            if let Err(e) = backend.publish(&topic, &payload).await {
                                error!("Failed to publish to topic {}: {}", topic, e);
                            } else {
                                // [LOG_OPTIMIZATION] Upgraded to INFO for performance analysis
                                log::info!(
                                    "[QueueManager] forward_channel published: topic={} id={}",
                                    topic, id
                                );
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to serialize item for topic {}: {}", topic, e);
                        // Permit is dropped here immediately
                    }
                }
            }
        });
    }

    pub fn get_task_push_channel(&self) -> Sender<TaskModel> {
        self.channel.task_sender.clone()
    }
    pub fn get_task_pop_channel(&self) -> Arc<Mutex<Receiver<TaskModel>>> {
        Arc::clone(&self.channel.task_receiver)
    }
    pub fn get_request_pop_channel(&self) -> Arc<Mutex<Receiver<Request>>> {
        self.channel.download_request_receiver.clone()
    }
    pub fn get_request_push_channel(&self) -> Sender<Request> {
        if self.backend.is_none() {
            return self.channel.download_request_sender.clone();
        }
        self.channel.request_sender.clone()
    }
    pub fn get_response_push_channel(&self) -> Sender<Response> {
        if self.backend.is_none() {
            return self.channel.remote_response_sender.clone();
        }
        self.channel.response_sender.clone()
    }
    pub fn get_response_pop_channel(&self) -> Arc<Mutex<Receiver<Response>>> {
        Arc::clone(&self.channel.remote_response_receiver)
    }
    pub fn get_parser_task_pop_channel(&self) -> Arc<Mutex<Receiver<ParserTaskModel>>> {
        self.channel.remote_parser_task_receiver.clone()
    }
    pub fn get_parser_task_push_channel(&self) -> Sender<ParserTaskModel> {
        if self.backend.is_none() {
            return self.channel.remote_parser_task_sender.clone();
        }
        self.channel.parser_task_sender.clone()
    }
    pub fn get_error_pop_channel(&self) -> Arc<Mutex<Receiver<ErrorTaskModel>>> {
        Arc::clone(&self.channel.remote_error_receiver)
    }
    pub fn get_error_push_channel(&self) -> Sender<ErrorTaskModel> {
        if self.backend.is_none() {
            return self.channel.remote_error_sender.clone();
        }
        self.channel.error_sender.clone()
    }
    pub fn get_log_pop_channel(&self) -> Sender<LogModel> {
        self.channel.log_sender.clone()
    }

    pub async fn clean_storage(&self) -> errors::Result<()> {
        if let Some(backend) = &self.backend {
            backend.clean_storage().await?;
        }
        Ok(())
    }
}
