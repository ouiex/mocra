use crate::common::interface::storage::{BlobStorage, Offloadable};
use crate::common::model::config::{ChannelConfig, Config};
use crate::common::model::{
    DeadLetterEnvelope, DeadLetterEnvelopeConfig, NodeDispatchEnvelope, NodeErrorEnvelope,
    Prioritizable, Priority, QueueEnvelope, QueueTopicKind, RequestDispatchEnvelope,
    ResponseDispatchEnvelope, TaskDispatchEnvelope,
};
use crate::common::policy::PolicyResolver;
use crate::errors::ErrorKind;
use crate::queue::batcher::Batcher;
use crate::queue::channel::Channel;
use crate::queue::codec::{queue_codec, set_queue_codec_from_config};
use crate::queue::compensation::{Compensator, Identifiable, QueueNativeCompensator};
use crate::queue::compression::{compress_payload_owned, decompress_payload};
use crate::queue::contract::{
    LARGE_PAYLOAD_BYTES, QueueRoute, QueueRouteContract, qualify_topic_namespace,
};
use crate::queue::kafka::{KafkaQueue, KafkaQueueConfig};
use crate::queue::{HEADER_ATTEMPT, HEADER_CREATED_AT, MqBackend, NackPolicy, QueuedItem};
use crate::utils::logger::LogModel;
use crate::utils::storage::FileSystemBlobStorage;
use futures::StreamExt;
use futures::future::join_all;
use log::{error, info};
use metrics::counter;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, Semaphore};

const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024;
fn default_headers() -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string();
    headers.insert(HEADER_CREATED_AT.to_string(), now_ms);
    headers
}

#[derive(Debug, Clone)]
pub struct HydratedDlqRecord {
    pub record_id: String,
    pub envelope: DeadLetterEnvelope,
}

pub struct QueueManager {
    pub channel: Arc<Channel>,
    pub backend: Option<Arc<dyn MqBackend>>,
    pub compensator: Option<Arc<dyn Compensator>>,
    pub blob_storage: Option<Arc<dyn BlobStorage>>,
    pub batch_concurrency: usize,
    pub compression_threshold: usize,
    pub nack_policy: NackPolicy,
    pub log_topic: String,
    pub namespace: String,
    pub request_namespace_subscriptions: Vec<String>,
}

impl QueueManager {
    pub fn new(backend: Option<Arc<dyn MqBackend>>, capacity: usize) -> Self {
        QueueManager {
            channel: Arc::new(Channel::new(capacity)),
            backend,
            compensator: None,
            blob_storage: None,
            batch_concurrency: 50,
            compression_threshold: DEFAULT_COMPRESSION_THRESHOLD,
            nack_policy: NackPolicy::default(),
            log_topic: "log".to_string(),
            namespace: String::new(),
            request_namespace_subscriptions: Vec::new(),
        }
    }

    pub fn from_config(cfg: &Config) -> Arc<Self> {
        Self::from_config_with_log_topic(cfg, None)
    }

    pub fn from_config_with_log_topic(cfg: &Config, log_topic: Option<&str>) -> Arc<Self> {
        set_queue_codec_from_config(cfg);
        let channel_config = &cfg.channel_config;
        let namespace = &cfg.name;

        let nack_policy = if let Some(policy_cfg) = &cfg.policy
            && !policy_cfg.overrides.is_empty()
        {
            let resolver = PolicyResolver::new(Some(policy_cfg));
            let decision =
                resolver.resolve_with_kind("queue", Some("nack"), Some("failed"), ErrorKind::Queue);
            NackPolicy {
                max_retries: decision.policy.max_retries,
                backoff_ms: decision.policy.backoff_ms,
            }
        } else {
            NackPolicy {
                max_retries: channel_config.nack_max_retries.unwrap_or(0),
                backoff_ms: channel_config.nack_backoff_ms.unwrap_or(0),
            }
        };

        let mut queue_manager = if let Some(kafka_config) = &channel_config.kafka {
            match KafkaQueue::new(KafkaQueueConfig {
                kafka_config,
                minid_time: channel_config.minid_time,
                namespace,
                nack_policy,
            }) {
                Ok(kafka_queue) => {
                    info!("KafkaQueue initialized successfully");
                    QueueManager::new(Some(Arc::new(kafka_queue)), channel_config.capacity)
                }
                Err(e) => {
                    error!("KafkaQueue init failed, fallback to in-memory queue: {}", e);
                    QueueManager::new(None, channel_config.capacity)
                }
            }
        } else {
            info!("In-Memory Queue initialized (Single Node Mode)");
            QueueManager::new(None, 10000)
        };

        if let Some(topic) = log_topic {
            queue_manager.log_topic = topic.to_string();
        }

        queue_manager.nack_policy = nack_policy;
        queue_manager.namespace = namespace.to_string();
        queue_manager.request_namespace_subscriptions =
            Self::resolve_request_namespace_subscriptions(channel_config, namespace);

        if let Some(concurrency) = channel_config.batch_concurrency {
            queue_manager.with_concurrency(concurrency);
        }
        if let Some(threshold) = channel_config.compression_threshold {
            queue_manager.with_compression_threshold(threshold);
        }

        if let Some(blob_config) = &channel_config.blob_storage
            && let Some(path) = &blob_config.path
        {
            let storage = Arc::new(FileSystemBlobStorage::new(path));
            queue_manager.with_blob_storage(storage);
            info!("BlobStorage initialized at: {}", path);
        }

        queue_manager.install_default_compensator(channel_config, namespace);

        queue_manager.subscribe();
        Arc::new(queue_manager)
    }

    pub fn with_backend(&mut self, backend: Arc<dyn MqBackend>) {
        self.backend = Some(backend);
    }

    pub fn with_compensator(&mut self, compensator: Arc<dyn Compensator>) {
        self.compensator = Some(compensator);
    }

    pub(crate) fn install_default_compensator(
        &mut self,
        _channel_config: &ChannelConfig,
        namespace: &str,
    ) {
        if self.backend.is_some() {
            self.with_compensator(Arc::new(QueueNativeCompensator::new(namespace)));
        }
    }

    pub fn with_blob_storage(&mut self, storage: Arc<dyn BlobStorage>) {
        self.blob_storage = Some(storage);
    }

    pub fn with_concurrency(&mut self, concurrency: usize) {
        self.batch_concurrency = concurrency;
    }

    pub fn with_compression_threshold(&mut self, threshold: usize) {
        self.compression_threshold = threshold;
    }

    pub fn with_log_topic(&mut self, topic: impl Into<String>) {
        self.log_topic = topic.into();
    }

    pub fn subscribe(&self) {
        if let Some(backend) = &self.backend {
            let backend = backend.clone();
            let channel = self.channel.clone();
            let compensator = self.compensator.clone();
            let blob_storage = self.blob_storage.clone();
            let concurrency = self.batch_concurrency;
            let compression_threshold = self.compression_threshold;
            let task_contract = QueueRouteContract::new(QueueRoute::Task, &self.log_topic);
            let request_contract = QueueRouteContract::new(QueueRoute::Request, &self.log_topic);
            let response_contract = QueueRouteContract::new(QueueRoute::Response, &self.log_topic);
            let parser_contract = QueueRouteContract::new(QueueRoute::ParserTask, &self.log_topic);
            let error_contract = QueueRouteContract::new(QueueRoute::ErrorTask, &self.log_topic);
            let log_contract = QueueRouteContract::new(QueueRoute::Log, &self.log_topic);
            let request_topics = self.request_subscription_topics(&request_contract);

            // Define outbound channels (Local -> Remote)
            self.spawn_forwarder(
                task_contract.clone(),
                channel.remote_task_receiver.clone(),
                backend.clone(),
                blob_storage.clone(),
                concurrency,
                compression_threshold,
            );
            self.spawn_forwarder(
                request_contract.clone(),
                channel.request_receiver.clone(),
                backend.clone(),
                blob_storage.clone(),
                concurrency,
                compression_threshold,
            );
            self.spawn_forwarder(
                response_contract.clone(),
                channel.response_receiver.clone(),
                backend.clone(),
                blob_storage.clone(),
                concurrency,
                compression_threshold,
            );
            self.spawn_forwarder(
                parser_contract.clone(),
                channel.parser_task_receiver.clone(),
                backend.clone(),
                blob_storage.clone(),
                concurrency,
                compression_threshold,
            );
            self.spawn_forwarder(
                error_contract.clone(),
                channel.error_receiver.clone(),
                backend.clone(),
                blob_storage.clone(),
                concurrency,
                compression_threshold,
            );
            self.spawn_forwarder(
                log_contract,
                channel.log_receiver.clone(),
                backend.clone(),
                blob_storage.clone(),
                concurrency,
                compression_threshold,
            );

            // Define inbound subscriptions (Remote -> Local)
            tokio::spawn(async move {
                Self::subscribe_route(
                    task_contract,
                    channel.task_sender.clone(),
                    backend.clone(),
                    compensator.clone(),
                    blob_storage.clone(),
                    concurrency,
                )
                .await;
                Self::subscribe_route_with_topics(
                    request_contract,
                    request_topics,
                    channel.download_request_sender.clone(),
                    backend.clone(),
                    compensator.clone(),
                    blob_storage.clone(),
                    concurrency,
                )
                .await;
                Self::subscribe_route(
                    response_contract,
                    channel.remote_response_sender.clone(),
                    backend.clone(),
                    compensator.clone(),
                    blob_storage.clone(),
                    concurrency,
                )
                .await;
                Self::subscribe_route(
                    parser_contract,
                    channel.remote_parser_task_sender.clone(),
                    backend.clone(),
                    compensator.clone(),
                    blob_storage.clone(),
                    concurrency,
                )
                .await;
                Self::subscribe_route(
                    error_contract,
                    channel.remote_error_sender.clone(),
                    backend.clone(),
                    compensator.clone(),
                    blob_storage.clone(),
                    concurrency,
                )
                .await;
            });
        }
    }

    async fn subscribe_route<T>(
        contract: QueueRouteContract,
        sender: Sender<QueuedItem<T>>,
        backend: Arc<dyn MqBackend>,
        compensator: Option<Arc<dyn Compensator>>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
    ) where
        T: serde::de::DeserializeOwned
            + Send
            + 'static
            + std::fmt::Debug
            + Identifiable
            + Offloadable,
    {
        let topics = contract.priority_topics();
        Self::subscribe_route_with_topics(
            contract,
            topics,
            sender,
            backend,
            compensator,
            blob_storage,
            concurrency,
        )
        .await;
    }

    async fn subscribe_route_with_topics<T>(
        contract: QueueRouteContract,
        topics: Vec<String>,
        sender: Sender<QueuedItem<T>>,
        backend: Arc<dyn MqBackend>,
        compensator: Option<Arc<dyn Compensator>>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
    ) where
        T: serde::de::DeserializeOwned
            + Send
            + 'static
            + std::fmt::Debug
            + Identifiable
            + Offloadable,
    {
        futures::stream::iter(topics)
            .for_each_concurrent(None, |topic| {
                let sender = sender.clone();
                let backend = backend.clone();
                let compensator = compensator.clone();
                let blob_storage = blob_storage.clone();
                let contract = contract.clone();
                async move {
                    Self::subscribe_topic(
                        contract,
                        topic,
                        sender,
                        backend,
                        compensator,
                        blob_storage,
                        concurrency,
                    )
                    .await;
                }
            })
            .await;
    }

    fn resolve_request_namespace_subscriptions(
        channel_config: &ChannelConfig,
        local_namespace: &str,
    ) -> Vec<String> {
        let mut namespaces = BTreeSet::new();
        for namespace in &channel_config.federation_request_namespaces {
            let trimmed = namespace.trim();
            if trimmed.is_empty() || trimmed == local_namespace {
                continue;
            }
            namespaces.insert(trimmed.to_string());
        }
        namespaces.into_iter().collect()
    }

    fn request_subscription_topics(&self, contract: &QueueRouteContract) -> Vec<String> {
        let local_topics = contract.priority_topics();
        let mut topics = local_topics.clone();
        for namespace in &self.request_namespace_subscriptions {
            for topic in &local_topics {
                topics.push(qualify_topic_namespace(namespace, topic));
            }
        }
        topics
    }

    async fn subscribe_topic<T>(
        contract: QueueRouteContract,
        topic: String,
        sender: Sender<QueuedItem<T>>,
        backend: Arc<dyn MqBackend>,
        compensator: Option<Arc<dyn Compensator>>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
    ) where
        T: serde::de::DeserializeOwned
            + Send
            + 'static
            + std::fmt::Debug
            + Identifiable
            + Offloadable,
    {
        let topic_name = topic.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        if let Err(e) = backend.subscribe(&topic, tx).await {
            error!(
                "Failed to subscribe to topic {} for route {}: {}",
                topic,
                contract.route_name(),
                e
            );
            return;
        }

        let batch_policy = contract.inbound_batch();
        let batch_concurrency = contract.subscribe_parallelism(concurrency);
        let semaphore = Arc::new(Semaphore::new(batch_concurrency));

        tokio::spawn(async move {
            let topic_clone = topic_name.clone();
            let sender_clone = sender.clone();
            let compensator_clone = compensator.clone();
            let blob_storage_clone = blob_storage.clone();
            let contract_clone = contract.clone();

            Batcher::run(
                &mut rx,
                batch_policy.max_items,
                batch_policy.max_wait_ms,
                semaphore,
                move |items| {
                    let topic = topic_clone.clone();
                    let sender = sender_clone.clone();
                    let compensator = compensator_clone.clone();
                    let blob_storage = blob_storage_clone.clone();
                    let contract = contract_clone.clone();
                    async move {
                        Self::process_batch_messages(
                            items,
                            &contract,
                            &topic,
                            &sender,
                            &compensator,
                            &blob_storage,
                        )
                        .await;
                    }
                },
            )
            .await;
            log::warn!(
                "Topic {} subscription closed for route {}",
                topic_name,
                contract.route_name()
            );
        });
    }

    async fn process_batch_messages<T>(
        messages: Vec<crate::queue::Message>,
        contract: &QueueRouteContract,
        topic: &str,
        sender: &Sender<QueuedItem<T>>,
        compensator: &Option<Arc<dyn Compensator>>,
        blob_storage: &Option<Arc<dyn BlobStorage>>,
    ) where
        T: serde::de::DeserializeOwned
            + Send
            + 'static
            + std::fmt::Debug
            + Identifiable
            + Offloadable,
    {
        let total_bytes = messages.iter().map(|msg| msg.payload.len()).sum::<usize>();
        let use_blocking = contract
            .inbound_batch()
            .should_use_blocking(messages.len(), total_bytes);
        let results = if use_blocking {
            tokio::task::spawn_blocking(move || {
                messages
                    .into_iter()
                    .map(|msg| {
                        let payload_slice = msg.payload.as_slice();
                        let decoded_payload = decompress_payload(payload_slice);
                        let item_res = queue_codec().decode::<T>(decoded_payload.as_ref());
                        (msg, item_res)
                    })
                    .collect::<Vec<_>>()
            })
            .await
        } else {
            let processed_items = messages
                .into_iter()
                .map(|msg| {
                    let payload_slice = msg.payload.as_slice();
                    let decoded_payload = decompress_payload(payload_slice);
                    let item_res = queue_codec().decode::<T>(decoded_payload.as_ref());
                    (msg, item_res)
                })
                .collect::<Vec<_>>();
            Ok(processed_items)
        };

        match results {
            Ok(processed_items) => {
                let tasks = processed_items.into_iter().map(|(msg, result)| {
                    let topic = topic.to_string();
                    let compensator = compensator.clone();
                    let storage = blob_storage.clone();

                    async move {
                        match result {
                            Ok(mut item) => {
                                // Reload content from blob storage if necessary
                                if let Some(storage) = storage
                                    && let Err(e) = item.reload(&storage).await {
                                        error!(
                                            "Failed to reload item content from storage for topic {}: {}",
                                            topic, e
                                        );
                                        if let Err(e) = msg.nack("Blob reload failed").await {
                                            error!("Failed to NACK message: {}", e);
                                        }
                                        return None;
                                    }

                                let id = item.get_id();

                                if let Some(comp) = compensator
                                    && let Err(e) =
                                        comp.add_task(&topic, &id, msg.payload.clone()).await
                                    {
                                        error!(
                                            "Failed to add task to compensation queue for topic {}: {}",
                                            topic, e
                                        );
                                    }

                                let msg_ack = msg.clone();
                                let msg_nack = msg.clone();

                                let queued_item = QueuedItem::with_ack(
                                    item,
                                    move || Box::pin(async move { msg_ack.ack().await }),
                                    move |reason| {
                                        Box::pin(async move { msg_nack.nack(reason).await })
                                    },
                                );

                                Some((msg, queued_item))
                            }
                            Err(e) => {
                                let payload_len = msg.payload.len();
                                let codec = queue_codec().name();

                                error!(
                                    "Failed to deserialize message from topic {} (route={}, scope={}, codec={}, bytes={}): {}",
                                    topic,
                                    contract.route_name(),
                                    contract.backpressure_scope(),
                                    codec,
                                    payload_len,
                                    e
                                );
                                if let Err(e) = msg.nack("Deserialization failed").await {
                                    error!("Failed to NACK poison message: {}", e);
                                }
                                None
                            }
                        }
                    }
                });

                let results = join_all(tasks).await;

                for (msg, item) in results.into_iter().flatten() {
                    if let Err(_e) = sender.send(item).await {
                        let _ = msg.nack("Channel closed").await;
                    }
                }
            }
            Err(e) => {
                error!("Batch deserialization task failed: {}", e);
                // We can't easily NACK here because we lost ownership of msgs inside the closure if it panicked.
                // But spawn_blocking usually returns JoinError if panicked.
            }
        }
    }

    fn spawn_forwarder<T>(
        &self,
        contract: QueueRouteContract,
        receiver: Arc<Mutex<Receiver<QueuedItem<T>>>>,
        backend: Arc<dyn MqBackend>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
        compression_threshold: usize,
    ) where
        T: serde::Serialize + Send + Sync + 'static + Identifiable + Prioritizable + Offloadable,
    {
        Self::forward_channel(
            contract,
            receiver,
            backend,
            blob_storage,
            concurrency,
            compression_threshold,
        )
    }

    fn forward_channel<T>(
        contract: QueueRouteContract,
        receiver: Arc<Mutex<Receiver<QueuedItem<T>>>>,
        backend: Arc<dyn MqBackend>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
        compression_threshold: usize,
    ) where
        T: serde::Serialize + Send + Sync + 'static + Identifiable + Prioritizable + Offloadable,
    {
        let publish_policy = contract.outbound_batch();
        let semaphore = Arc::new(Semaphore::new(concurrency));

        tokio::spawn(async move {
            let mut rx = receiver.lock().await;
            let backend_clone = backend.clone();
            let blob_storage = blob_storage.clone();
            let compression_threshold = compression_threshold;
            let contract_clone = contract.clone();

            Batcher::run(
                &mut *rx,
                publish_policy.max_items,
                publish_policy.max_wait_ms,
                semaphore,
                move |items| {
                    let backend = backend_clone.clone();
                    let blob_storage = blob_storage.clone();
                    let contract = contract_clone.clone();
                    async move {
                        Self::flush_batch_grouped(
                            contract,
                            backend,
                            blob_storage,
                            items,
                            compression_threshold,
                        )
                        .await;
                    }
                },
            )
            .await;
        });
    }

    pub fn get_task_push_channel(&self) -> Sender<QueuedItem<TaskDispatchEnvelope>> {
        if self.backend.is_none() {
            return self.channel.task_sender.clone();
        }
        self.channel.remote_task_sender.clone()
    }
    pub fn get_task_pop_channel(&self) -> Arc<Mutex<Receiver<QueuedItem<TaskDispatchEnvelope>>>> {
        Arc::clone(&self.channel.task_receiver)
    }
    pub fn get_request_pop_channel(
        &self,
    ) -> Arc<Mutex<Receiver<QueuedItem<RequestDispatchEnvelope>>>> {
        self.channel.download_request_receiver.clone()
    }
    pub fn get_request_push_channel(&self) -> Sender<QueuedItem<RequestDispatchEnvelope>> {
        if self.backend.is_none() {
            return self.channel.download_request_sender.clone();
        }
        self.channel.request_sender.clone()
    }
    pub fn get_response_push_channel(&self) -> Sender<QueuedItem<ResponseDispatchEnvelope>> {
        if self.backend.is_none() {
            return self.channel.remote_response_sender.clone();
        }
        self.channel.response_sender.clone()
    }

    pub fn response_namespace_override(
        &self,
        dispatch: &ResponseDispatchEnvelope,
    ) -> Option<String> {
        let namespace = dispatch.routing.namespace.trim();
        if namespace.is_empty() || namespace == self.namespace {
            return None;
        }
        Some(namespace.to_string())
    }

    pub fn should_use_local_response_fast_path(&self, dispatch: &ResponseDispatchEnvelope) -> bool {
        self.backend.is_none() || self.response_namespace_override(dispatch).is_none()
    }

    /// Attempts to send directly to local consumers before using a backend queue.
    /// Optimization: if local consumers exist and the channel is not full, send
    /// directly to avoid serialization and network overhead.
    pub fn try_send_local_response(
        &self,
        item: QueuedItem<ResponseDispatchEnvelope>,
    ) -> Result<(), tokio::sync::mpsc::error::TrySendError<QueuedItem<ResponseDispatchEnvelope>>>
    {
        self.channel.remote_response_sender.try_send(item)
    }

    pub fn get_response_pop_channel(
        &self,
    ) -> Arc<Mutex<Receiver<QueuedItem<ResponseDispatchEnvelope>>>> {
        Arc::clone(&self.channel.remote_response_receiver)
    }
    pub fn get_parser_task_pop_channel(
        &self,
    ) -> Arc<Mutex<Receiver<QueuedItem<NodeDispatchEnvelope>>>> {
        self.channel.remote_parser_task_receiver.clone()
    }
    pub fn get_parser_task_push_channel(&self) -> Sender<QueuedItem<NodeDispatchEnvelope>> {
        if self.backend.is_none() {
            return self.channel.remote_parser_task_sender.clone();
        }
        self.channel.parser_task_sender.clone()
    }
    pub fn get_error_pop_channel(&self) -> Arc<Mutex<Receiver<QueuedItem<NodeErrorEnvelope>>>> {
        Arc::clone(&self.channel.remote_error_receiver)
    }
    pub fn get_error_push_channel(&self) -> Sender<QueuedItem<NodeErrorEnvelope>> {
        if self.backend.is_none() {
            return self.channel.remote_error_sender.clone();
        }
        self.channel.error_sender.clone()
    }
    pub fn get_log_push_channel(&self) -> Sender<QueuedItem<LogModel>> {
        self.channel.log_sender.clone()
    }

    /// Local pending message count across processor queues
    pub async fn local_pending_count(&self) -> usize {
        let (task, download, response, parser, error, remote_task) =
            self.local_pending_breakdown().await;
        task + download + response + parser + error + remote_task
    }

    pub async fn local_pending_breakdown(&self) -> (usize, usize, usize, usize, usize, usize) {
        let task = self
            .channel
            .task_sender
            .max_capacity()
            .saturating_sub(self.channel.task_sender.capacity());
        let download = self
            .channel
            .download_request_sender
            .max_capacity()
            .saturating_sub(self.channel.download_request_sender.capacity());
        let response = self
            .channel
            .remote_response_sender
            .max_capacity()
            .saturating_sub(self.channel.remote_response_sender.capacity());
        let parser = self
            .channel
            .remote_parser_task_sender
            .max_capacity()
            .saturating_sub(self.channel.remote_parser_task_sender.capacity());
        let error = self
            .channel
            .remote_error_sender
            .max_capacity()
            .saturating_sub(self.channel.remote_error_sender.capacity());
        let remote_task = self
            .channel
            .remote_task_sender
            .max_capacity()
            .saturating_sub(self.channel.remote_task_sender.capacity());

        (task, download, response, parser, error, remote_task)
    }

    async fn flush_batch_grouped<T>(
        contract: QueueRouteContract,
        backend: Arc<dyn MqBackend>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        mut items: Vec<QueuedItem<T>>,
        compression_threshold: usize,
    ) where
        T: serde::Serialize + Identifiable + Send + Sync + Prioritizable + Offloadable + 'static,
    {
        // Try offloading large payloads if storage is configured
        if let Some(storage) = &blob_storage {
            // We iterate mutably to potentially modify items (offload content)
            for item in &mut items {
                if item.inner.should_offload(LARGE_PAYLOAD_BYTES)
                    && let Err(e) = item.inner.offload(storage).await
                {
                    error!("Failed to offload item payload to blob storage: {}", e);
                    // Continue, hoping it might still pass or fail later
                }
            }
        }

        // Optimization: Fast path if all items have same priority (likely normal)
        // or if items is empty
        if items.is_empty() {
            return;
        }

        let first_priority = items[0].get_priority();
        let first_namespace = items[0].namespace_override().map(str::to_owned);
        let all_same_route = items.iter().all(|item| {
            item.get_priority() == first_priority
                && item.namespace_override() == first_namespace.as_deref()
        });

        if all_same_route {
            let topic = match first_namespace.as_deref() {
                Some(namespace) => {
                    qualify_topic_namespace(namespace, &contract.topic_for_priority(first_priority))
                }
                None => contract.topic_for_priority(first_priority),
            };
            Self::flush_batch(
                topic,
                backend,
                items,
                compression_threshold,
                contract.outbound_batch(),
            )
            .await;
            return;
        }

        let mut groups: HashMap<(Priority, Option<String>), Vec<QueuedItem<T>>> = HashMap::new();
        for item in items {
            let route_key = (
                item.get_priority(),
                item.namespace_override().map(str::to_owned),
            );
            groups.entry(route_key).or_default().push(item);
        }

        for ((priority, namespace_override), group_items) in groups {
            let topic = match namespace_override.as_deref() {
                Some(namespace) => {
                    qualify_topic_namespace(namespace, &contract.topic_for_priority(priority))
                }
                None => contract.topic_for_priority(priority),
            };
            Self::flush_batch(
                topic,
                backend.clone(),
                group_items,
                compression_threshold,
                contract.outbound_batch(),
            )
            .await;
        }
    }

    async fn flush_batch<T>(
        topic: String,
        backend: Arc<dyn MqBackend>,
        items: Vec<QueuedItem<T>>,
        compression_threshold: usize,
        batch_policy: crate::queue::contract::QueueBatchPolicy,
    ) where
        T: serde::Serialize + Identifiable + Send + Sync + 'static,
    {
        let use_blocking = batch_policy.should_use_blocking(items.len(), 0);

        let payloads_result = if use_blocking {
            tokio::task::spawn_blocking(move || Self::encode_items(&items, compression_threshold))
                .await
        } else {
            Ok(Self::encode_items(&items, compression_threshold))
        };

        match payloads_result {
            Ok((payloads, ids, count)) => {
                if payloads.is_empty() {
                    return;
                }

                if let Err(e) = backend.publish_batch_with_headers(&topic, &payloads).await {
                    error!("Failed to publish batch to topic {}: {}", topic, e);
                } else {
                    log::info!(
                        "[QueueManager] forward_channel published batch: topic={} count={}",
                        topic,
                        count
                    );
                    if let Some(id_list) = ids {
                        for id in id_list {
                            log::debug!(
                                "[QueueManager] forward_channel published: topic={} id={}",
                                topic,
                                id
                            );
                        }
                    }
                }
            }
            Err(e) => {
                error!("Serialization task join error: {}", e);
            }
        }
    }

    pub async fn clean_storage(&self) -> crate::errors::Result<()> {
        if let Some(backend) = &self.backend {
            backend.clean_storage().await?;
        }
        Ok(())
    }

    /// Encode items into publish-ready payloads (serialize once + compress).
    fn encode_items<T>(
        items: &[QueuedItem<T>],
        compression_threshold: usize,
    ) -> (
        Vec<(Option<String>, Vec<u8>, HashMap<String, String>)>,
        Option<Vec<String>>,
        usize,
    )
    where
        T: serde::Serialize + Identifiable,
    {
        let mut payloads: Vec<(Option<String>, Vec<u8>, HashMap<String, String>)> =
            Vec::with_capacity(items.len());
        let mut ids = if log::log_enabled!(log::Level::Debug) {
            Some(Vec::with_capacity(items.len()))
        } else {
            None
        };

        for item in items {
            let id = item.get_id();
            if let Some(id_list) = ids.as_mut() {
                id_list.push(id.clone());
            }
            let encoded = queue_codec().encode(&item.inner);
            match encoded {
                Ok(p) => {
                    let final_payload = compress_payload_owned(p, compression_threshold);
                    let headers = default_headers();
                    payloads.push((Some(id), final_payload, headers));
                }
                Err(e) => {
                    error!(
                        "Failed to serialize item id={}: {}. Item will be skipped (unrecoverable).",
                        id, e
                    );
                    counter!("mocra_queue_encode_errors_total").increment(1);
                }
            }
        }
        (payloads, ids, items.len())
    }

    /// Send a message to the Dead Letter Queue (DLQ) manually.
    ///
    /// This is useful when the application logic decides a message cannot be processed
    /// even if the message delivery itself was successful (e.g., max logic retries exceeded).
    pub async fn send_to_dlq<T>(
        &self,
        topic: &str,
        item: &T,
        reason: &str,
    ) -> crate::errors::Result<()>
    where
        T: serde::Serialize + Identifiable + Send + Sync,
    {
        if let Some(backend) = &self.backend {
            let payload = queue_codec()
                .encode(item)
                .map_err(|e| crate::errors::error::QueueError::OperationFailed(Box::new(e)))?;
            let envelope =
                self.build_dead_letter_envelope(topic, &item.get_id(), payload, reason, 0)?;
            let dlq_payload = queue_codec()
                .encode(&envelope)
                .map_err(|e| crate::errors::error::QueueError::OperationFailed(Box::new(e)))?;
            backend
                .send_to_dlq(topic, &item.get_id(), &dlq_payload, reason)
                .await?;
        }
        Ok(())
    }

    pub async fn read_dlq(
        &self,
        topic: &str,
        count: usize,
    ) -> crate::errors::Result<Vec<DeadLetterEnvelope>> {
        self.read_dlq_records(topic, count)
            .await
            .map(|records| records.into_iter().map(|record| record.envelope).collect())
    }

    pub async fn read_dlq_records(
        &self,
        topic: &str,
        count: usize,
    ) -> crate::errors::Result<Vec<HydratedDlqRecord>> {
        if let Some(backend) = &self.backend {
            backend
                .read_dlq(topic, count)
                .await?
                .into_iter()
                .map(|record| self.hydrate_dead_letter_record(topic, record))
                .collect()
        } else {
            Ok(Vec::new())
        }
    }

    pub async fn requeue_dlq_message(
        &self,
        topic: &str,
        record_id: &str,
    ) -> crate::errors::Result<bool> {
        let Some(backend) = &self.backend else {
            return Ok(false);
        };

        let Some(record) = self.read_dlq_record(topic, record_id).await? else {
            return Ok(false);
        };

        let payload = compress_payload_owned(
            record.envelope.payload.payload.clone(),
            self.compression_threshold,
        );
        let headers = default_headers();
        backend
            .publish_with_headers(
                &record.envelope.source_topic,
                Some(&record.envelope.source_message_id),
                &payload,
                &headers,
            )
            .await?;
        backend.delete_dlq(topic, record_id).await
    }

    pub async fn delete_dlq_message(
        &self,
        topic: &str,
        record_id: &str,
    ) -> crate::errors::Result<bool> {
        if let Some(backend) = &self.backend {
            backend.delete_dlq(topic, record_id).await
        } else {
            Ok(false)
        }
    }

    fn queue_topic_kind(topic: &str) -> QueueTopicKind {
        QueueTopicKind::from_topic_name(topic).unwrap_or(QueueTopicKind::Dlq)
    }

    fn current_time_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn headers_for_attempt(attempt: u32) -> HashMap<String, String> {
        let mut headers = default_headers();
        headers.insert(HEADER_ATTEMPT.to_string(), attempt.to_string());
        headers
    }

    fn build_dead_letter_envelope(
        &self,
        topic: &str,
        source_message_id: &str,
        payload: Vec<u8>,
        reason: &str,
        attempt: u32,
    ) -> crate::errors::Result<DeadLetterEnvelope> {
        let topic_kind = Self::queue_topic_kind(topic);
        let payload = QueueEnvelope::new(
            topic_kind.schema_id(),
            1,
            queue_codec().payload_codec(),
            payload,
        )
        .map_err(|e| crate::errors::Error::new(crate::errors::ErrorKind::Queue, Some(e)))?;

        Ok(DeadLetterEnvelope::new(DeadLetterEnvelopeConfig {
            namespace: self.namespace.clone(),
            topic: topic_kind,
            source_topic: topic.to_string(),
            source_message_id: source_message_id.to_string(),
            reason: reason.to_string(),
            attempt,
            failed_at_ms: Self::current_time_ms(),
            payload,
        })
        .with_headers(Self::headers_for_attempt(attempt)))
    }

    async fn read_dlq_record(
        &self,
        topic: &str,
        record_id: &str,
    ) -> crate::errors::Result<Option<HydratedDlqRecord>> {
        let Some(backend) = &self.backend else {
            return Ok(None);
        };

        backend
            .read_dlq_record(topic, record_id)
            .await?
            .map(|record| self.hydrate_dead_letter_record(topic, record))
            .transpose()
    }

    fn hydrate_dead_letter_record(
        &self,
        topic: &str,
        record: crate::queue::DlqRecord,
    ) -> crate::errors::Result<HydratedDlqRecord> {
        let record_id = record.id.clone();

        if let Ok(envelope) = queue_codec().decode::<DeadLetterEnvelope>(&record.payload) {
            return Ok(HydratedDlqRecord {
                record_id,
                envelope,
            });
        }

        let payload = decompress_payload(record.payload.as_slice()).into_owned();
        let envelope = self.build_dead_letter_envelope(
            topic,
            &record.original_id,
            payload,
            &record.reason,
            record.attempt.unwrap_or(0),
        )?;

        Ok(HydratedDlqRecord {
            record_id,
            envelope,
        })
    }
}
