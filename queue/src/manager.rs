use futures::StreamExt;
use futures::future::join_all;
use crate::batcher::Batcher;
use crate::{MqBackend, QueuedItem, NackPolicy, HEADER_ATTEMPT, HEADER_CREATED_AT};
use crate::channel::Channel;
use crate::compression::{compress_payload_owned, decompress_payload};
use crate::compensation::{Compensator, Identifiable, RedisCompensator};
use crate::redis::RedisQueue;
use crate::kafka::KafkaQueue;
use common::policy::PolicyResolver;
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use common::model::{Request, Response, Prioritizable, Priority};
use common::model::config::Config;
use common::interface::storage::{BlobStorage, Offloadable};
use errors::ErrorKind;
use utils::storage::FileSystemBlobStorage;
use log::{error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::sync::mpsc::{Receiver, Sender};
use utils::logger::LogModel;
use rmp_serde as rmps;
use once_cell::sync::OnceCell;
use serde_path_to_error;

const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024;
const BLOCKING_PAYLOAD_BYTES: usize = 64 * 1024;

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

fn msgpack_encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, rmps::encode::Error> {
    rmps::to_vec(value)
}

fn msgpack_decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, serde_path_to_error::Error<rmps::decode::Error>> {
    let mut deserializer = rmps::Deserializer::new(bytes);
    serde_path_to_error::deserialize(&mut deserializer)
}

fn msgpack_encoded_size<T: serde::Serialize>(value: &T) -> Result<usize, rmps::encode::Error> {
    msgpack_encode(value).map(|bytes| bytes.len())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueCodec {
    Json,
    Msgpack,
}

static QUEUE_CODEC_OVERRIDE: OnceCell<QueueCodec> = OnceCell::new();

fn queue_codec() -> QueueCodec {
    if let Some(codec) = QUEUE_CODEC_OVERRIDE.get() {
        return *codec;
    }
    QueueCodec::Msgpack
}

fn set_queue_codec_from_config(cfg: &Config) {
    let codec = cfg
        .channel_config
        .queue_codec
        .as_deref()
        .unwrap_or("msgpack")
        .to_lowercase();

    let mapped = match codec.as_str() {
        "json" => QueueCodec::Json,
        "msgpack" | "rmp" => QueueCodec::Msgpack,
        _ => QueueCodec::Msgpack,
    };

    let _ = QUEUE_CODEC_OVERRIDE.set(mapped);
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
            let decision = resolver.resolve_with_kind(
                "queue",
                Some("nack"),
                Some("failed"),
                ErrorKind::Queue,
            );
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

        let mut queue_manager = if let Some(redis_config) = &channel_config.redis {
            let batch_size = channel_config.batch_concurrency.unwrap_or(500);
            let redis_queue = RedisQueue::new(
                redis_config,
                channel_config.minid_time,
                namespace,
                batch_size,
                nack_policy,
            )
                .expect("Failed to create RedisQueue");
            info!("RedisQueue initialized successfully with batch_size: {}", batch_size);
            QueueManager::new(Some(Arc::new(redis_queue)), channel_config.capacity)
        } else if let Some(kafka_config) = &channel_config.kafka {
            let kafka_queue = KafkaQueue::new(kafka_config, channel_config.minid_time, namespace, nack_policy)
                .expect("Failed to create KafkaQueue");
            info!("KafkaQueue initialized successfully");
            QueueManager::new(Some(Arc::new(kafka_queue)), channel_config.capacity)
        } else {
            info!("In-Memory Queue initialized (Single Node Mode)");
            QueueManager::new(None, 10000)
        };

        if let Some(topic) = log_topic {
            queue_manager.log_topic = topic.to_string();
        }

        queue_manager.nack_policy = nack_policy;

        if let Some(concurrency) = channel_config.batch_concurrency {
            queue_manager.with_concurrency(concurrency);
        }
        if let Some(threshold) = channel_config.compression_threshold {
            queue_manager.with_compression_threshold(threshold);
        }

        if let Some(blob_config) = &channel_config.blob_storage {
            if let Some(path) = &blob_config.path {
                let storage = Arc::new(FileSystemBlobStorage::new(path));
                queue_manager.with_blob_storage(storage);
                info!("BlobStorage initialized at: {}", path);
            }
        }

        // Initialize compensator
        if let Some(redis_config) = &channel_config.compensator {
            let compensator = Arc::new(
                RedisCompensator::new(redis_config, namespace)
                    .expect("Failed to create RedisCompensator"),
            );
            queue_manager.with_compensator(compensator);
        }

        queue_manager.subscribe();
        Arc::new(queue_manager)
    }

    pub fn with_backend(&mut self, backend: Arc<dyn MqBackend>) {
        self.backend = Some(backend);
    }

    pub fn with_compensator(&mut self, compensator: Arc<dyn Compensator>) {
        self.compensator = Some(compensator);
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

            // Define outbound channels (Local -> Remote)
            // format: (topic, receiver_channel)
            self.spawn_forwarder("task", channel.remote_task_receiver.clone(), backend.clone(), blob_storage.clone(), concurrency, compression_threshold);
            self.spawn_forwarder("request", channel.request_receiver.clone(), backend.clone(), blob_storage.clone(), concurrency, compression_threshold);
            self.spawn_forwarder("response", channel.response_receiver.clone(), backend.clone(), blob_storage.clone(), concurrency, compression_threshold);
            self.spawn_forwarder("parser_task", channel.parser_task_receiver.clone(), backend.clone(), blob_storage.clone(), concurrency, compression_threshold);
            self.spawn_forwarder("error_task", channel.error_receiver.clone(), backend.clone(), blob_storage.clone(), concurrency, compression_threshold);
            self.spawn_forwarder(self.log_topic.as_str(), channel.log_receiver.clone(), backend.clone(), blob_storage.clone(), concurrency, compression_threshold);

            // Define inbound subscriptions (Remote -> Local)
            tokio::spawn(async move {
                Self::subscribe_all_priorities("task", channel.task_sender.clone(), backend.clone(), compensator.clone(), blob_storage.clone(), concurrency).await;
                Self::subscribe_all_priorities("request", channel.download_request_sender.clone(), backend.clone(), compensator.clone(), blob_storage.clone(), concurrency).await;
                Self::subscribe_all_priorities("response", channel.remote_response_sender.clone(), backend.clone(), compensator.clone(), blob_storage.clone(), concurrency).await;
                Self::subscribe_all_priorities("parser_task", channel.remote_parser_task_sender.clone(), backend.clone(), compensator.clone(), blob_storage.clone(), concurrency).await;
                Self::subscribe_all_priorities("error_task", channel.remote_error_sender.clone(), backend.clone(), compensator.clone(), blob_storage.clone(), concurrency).await;
            });
        }
    }

    async fn subscribe_all_priorities<T>(
        topic_base: &str,
        sender: Sender<QueuedItem<T>>,
        backend: Arc<dyn MqBackend>,
        compensator: Option<Arc<dyn Compensator>>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
    ) where
        T: serde::de::DeserializeOwned + Send + 'static + std::fmt::Debug + Identifiable + Offloadable,
    {
         let priorities = [Priority::High, Priority::Normal, Priority::Low];
         futures::stream::iter(priorities)
             .for_each_concurrent(None, |priority| {
                 let sender = sender.clone();
                 let backend = backend.clone();
                 let compensator = compensator.clone();
                 let blob_storage = blob_storage.clone();
                 async move {
                     let topic = format!("{}-{}", topic_base, priority.suffix());
                     Self::subscribe_topic(
                         &topic,
                         sender,
                         backend,
                         compensator,
                         blob_storage,
                         concurrency,
                     ).await;
                 }
             })
             .await;
    }

    async fn subscribe_topic<T>(
        topic: &str,
        sender: Sender<QueuedItem<T>>,
        backend: Arc<dyn MqBackend>,
        compensator: Option<Arc<dyn Compensator>>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
    ) where
        T: serde::de::DeserializeOwned + Send + 'static + std::fmt::Debug + Identifiable + Offloadable,
    {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        if let Err(e) = backend.subscribe(topic, tx).await {
            error!("Failed to subscribe to topic {}: {}", topic, e);
            return;
        }

        let topic = topic.to_string();
        // Use semaphore to limit number of concurrent BATCHES roughly
        let batch_concurrency = (concurrency / 50).max(1);
        let semaphore = Arc::new(Semaphore::new(batch_concurrency));

        tokio::spawn(async move {
            let topic_clone = topic.clone();
            let sender_clone = sender.clone();
            let compensator_clone = compensator.clone();
            let blob_storage_clone = blob_storage.clone();

            Batcher::run(
                &mut rx,
                50,
                5,
                semaphore,
                move |items| {
                    let topic = topic_clone.clone();
                    let sender = sender_clone.clone();
                    let compensator = compensator_clone.clone();
                    let blob_storage = blob_storage_clone.clone();
                    async move {
                        Self::process_batch_messages(items, &topic, &sender, &compensator, &blob_storage).await;
                    }
                }
            ).await;
            log::warn!("Topic {} subscription closed", topic);
        });
    }

    async fn process_batch_messages<T>(
        messages: Vec<crate::Message>,
        topic: &str,
        sender: &Sender<QueuedItem<T>>,
        compensator: &Option<Arc<dyn Compensator>>,
        blob_storage: &Option<Arc<dyn BlobStorage>>,
    ) where
        T: serde::de::DeserializeOwned + Send + 'static + std::fmt::Debug + Identifiable + Offloadable,
    {
        // Offload deserialization to blocking thread for larger batches to avoid blocking the async runtime.
        // For small batches, decode inline to reduce thread switch overhead.
        let mut use_blocking = messages.len() >= 32;
        if !use_blocking {
            let mut total_bytes = 0usize;
            for msg in &messages {
                total_bytes += msg.payload.len();
                if total_bytes >= BLOCKING_PAYLOAD_BYTES {
                    use_blocking = true;
                    break;
                }
            }
        }
        let results = if use_blocking {
            tokio::task::spawn_blocking(move || {
                messages.into_iter().map(|msg| {
                    let payload_slice = msg.payload.as_slice();
                    let decoded_payload = decompress_payload(payload_slice);
                    let item_res = match queue_codec() {
                        QueueCodec::Json => serde_json::from_slice::<T>(decoded_payload.as_ref())
                            .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                        QueueCodec::Msgpack => msgpack_decode::<T>(decoded_payload.as_ref())
                            .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                    };
                    (msg, item_res)
                }).collect::<Vec<_>>()
            }).await
        } else {
            let processed_items = messages.into_iter().map(|msg| {
                let payload_slice = msg.payload.as_slice();
                let decoded_payload = decompress_payload(payload_slice);
                let item_res = match queue_codec() {
                    QueueCodec::Json => serde_json::from_slice::<T>(decoded_payload.as_ref())
                        .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                    QueueCodec::Msgpack => msgpack_decode::<T>(decoded_payload.as_ref())
                        .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                };
                (msg, item_res)
            }).collect::<Vec<_>>();
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
                                if let Some(storage) = storage {
                                    if let Err(e) = item.reload(&storage).await {
                                         error!("Failed to reload item content from storage for topic {}: {}", topic, e);
                                         if let Err(e) = msg.nack("Blob reload failed").await {
                                             error!("Failed to NACK message: {}", e);
                                         }
                                         return None;
                                    }
                                }

                                let id = item.get_id();
                                
                                if let Some(comp) = compensator {
                                     if let Err(e) = comp.add_task(&topic, &id, msg.payload.clone()).await {
                                        error!(
                                            "Failed to add task to compensation queue for topic {}: {}",
                                            topic, e
                                        );
                                     }
                                }
                                
                                let msg_ack = msg.clone();
                                let msg_nack = msg.clone();
                
                                let queued_item = QueuedItem::with_ack(
                                    item,
                                    move || Box::pin(async move { msg_ack.ack().await }),
                                    move |reason| Box::pin(async move { msg_nack.nack(reason).await })
                                );
                                
                                Some((msg, queued_item))
                            }
                            Err(e) => {
                                let payload_len = msg.payload.len();
                                let codec = match queue_codec() {
                                    QueueCodec::Json => "json",
                                    QueueCodec::Msgpack => "msgpack",
                                };
                            
                                error!(
                                    "Failed to deserialize message from topic {} (codec={}, bytes={}): {}",
                                    topic,
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

                for res in results {
                    if let Some((msg, item)) = res {
                        if let Err(_e) = sender.send(item).await {
                                let _ = msg.nack("Channel closed").await;
                        }
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
        topic: &str,
        receiver: Arc<Mutex<Receiver<QueuedItem<T>>>>,
        backend: Arc<dyn MqBackend>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
        compression_threshold: usize,
    ) where
        T: serde::Serialize + Send + Sync + 'static + Identifiable + Prioritizable + Offloadable,
    {
        Self::forward_channel(topic, receiver, backend, blob_storage, concurrency, compression_threshold)
    }

    fn forward_channel<T>(
        topic: &str,
        receiver: Arc<Mutex<Receiver<QueuedItem<T>>>>,
        backend: Arc<dyn MqBackend>,
        blob_storage: Option<Arc<dyn BlobStorage>>,
        concurrency: usize,
        compression_threshold: usize,
    ) where
        T: serde::Serialize + Send + Sync + 'static + Identifiable + Prioritizable + Offloadable,
    {
        let topic = topic.to_string();
        // Concurrency limit for publishing batches
        let semaphore = Arc::new(Semaphore::new(concurrency));

        tokio::spawn(async move {
            let mut rx = receiver.lock().await;
            let topic_clone = topic.clone();
            let backend_clone = backend.clone();
            let blob_storage = blob_storage.clone();
            let compression_threshold = compression_threshold;

            Batcher::run(
                &mut *rx,
                500,
                5,
                semaphore,
                move |items| {
                    let topic = topic_clone.clone();
                    let backend = backend_clone.clone();
                    let blob_storage = blob_storage.clone();
                    async move {
                        Self::flush_batch_grouped(topic, backend, blob_storage, items, compression_threshold).await;
                    }
                }
            ).await;
        });
    }

    pub fn get_task_push_channel(&self) -> Sender<QueuedItem<TaskModel>> {
        if self.backend.is_none() {
            return self.channel.task_sender.clone();
        }
        self.channel.remote_task_sender.clone()
    }
    pub fn get_task_pop_channel(&self) -> Arc<Mutex<Receiver<QueuedItem<TaskModel>>>> {
        Arc::clone(&self.channel.task_receiver)
    }
    pub fn get_request_pop_channel(&self) -> Arc<Mutex<Receiver<QueuedItem<Request>>>> {
        self.channel.download_request_receiver.clone()
    }
    pub fn get_request_push_channel(&self) -> Sender<QueuedItem<Request>> {
        if self.backend.is_none() {
            return self.channel.download_request_sender.clone();
        }
        self.channel.request_sender.clone()
    }
    pub fn get_response_push_channel(&self) -> Sender<QueuedItem<Response>> {
        if self.backend.is_none() {
            return self.channel.remote_response_sender.clone();
        }
        self.channel.response_sender.clone()
    }

    /// 尝试直接发送到本地消费者 (绕过 Redis/Kafka)
    /// 用于优化：如果本地有消费者且 channel 未满，直接发送，避免序列化和网络开销。
    pub fn try_send_local_response(&self, item: QueuedItem<Response>) -> Result<(), tokio::sync::mpsc::error::TrySendError<QueuedItem<Response>>> {
        self.channel.remote_response_sender.try_send(item)
    }

    pub fn get_response_pop_channel(&self) -> Arc<Mutex<Receiver<QueuedItem<Response>>>> {
        Arc::clone(&self.channel.remote_response_receiver)
    }
    pub fn get_parser_task_pop_channel(&self) -> Arc<Mutex<Receiver<QueuedItem<ParserTaskModel>>>> {
        self.channel.remote_parser_task_receiver.clone()
    }
    pub fn get_parser_task_push_channel(&self) -> Sender<QueuedItem<ParserTaskModel>> {
        if self.backend.is_none() {
            return self.channel.remote_parser_task_sender.clone();
        }
        self.channel.parser_task_sender.clone()
    }
    pub fn get_error_pop_channel(&self) -> Arc<Mutex<Receiver<QueuedItem<ErrorTaskModel>>>> {
        Arc::clone(&self.channel.remote_error_receiver)
    }
    pub fn get_error_push_channel(&self) -> Sender<QueuedItem<ErrorTaskModel>> {
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
        let mut total = 0usize;

        total += self.channel.task_receiver.lock().await.len();
        total += self.channel.download_request_receiver.lock().await.len();
        total += self.channel.remote_response_receiver.lock().await.len();
        total += self.channel.remote_parser_task_receiver.lock().await.len();
        total += self.channel.remote_error_receiver.lock().await.len();

        total
    }

    async fn flush_batch_grouped<T>(
        base_topic: String,
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
                 if item.inner.should_offload(BLOCKING_PAYLOAD_BYTES) {
                     if let Err(e) = item.inner.offload(storage).await {
                         error!("Failed to offload item payload to blob storage: {}", e);
                         // Continue, hoping it might still pass or fail later
                     }
                 }
             }
        }

        // Optimization: Fast path if all items have same priority (likely normal)
        // or if items is empty
        if items.is_empty() {
             return;
        }

        let first_priority = items[0].get_priority();
        let all_same_priority = items.iter().all(|i| i.get_priority() == first_priority);

        if all_same_priority {
            let topic = format!("{}-{}", base_topic, first_priority.suffix());
            Self::flush_batch(topic, backend, items, compression_threshold).await;
            return;
        }

        let mut groups: HashMap<Priority, Vec<QueuedItem<T>>> = HashMap::new();
        for item in items {
            groups.entry(item.get_priority()).or_default().push(item);
        }

        for (priority, group_items) in groups {
            let topic = format!("{}-{}", base_topic, priority.suffix());
            Self::flush_batch(topic, backend.clone(), group_items, compression_threshold).await;
        }
    }

    async fn flush_batch<T>(
        topic: String,
        backend: Arc<dyn MqBackend>,
        items: Vec<QueuedItem<T>>,
        compression_threshold: usize,
    ) where
        T: serde::Serialize + Identifiable + Send + Sync + 'static,
    {
        let mut use_blocking = items.len() >= 32;
        if !use_blocking {
            let mut estimated_total_bytes = 0usize;
            for item in &items {
                match queue_codec() {
                    QueueCodec::Json => match serde_json::to_vec(&item.inner) {
                        Ok(bytes) => {
                            estimated_total_bytes += bytes.len();
                            if estimated_total_bytes >= BLOCKING_PAYLOAD_BYTES {
                                use_blocking = true;
                                break;
                            }
                        }
                        Err(e) => error!("Failed to estimate payload size: {}", e),
                    },
                    QueueCodec::Msgpack => match msgpack_encoded_size(&item.inner) {
                        Ok(size) => {
                            estimated_total_bytes += size;
                            if estimated_total_bytes >= BLOCKING_PAYLOAD_BYTES {
                                use_blocking = true;
                                break;
                            }
                        }
                        Err(e) => error!("Failed to estimate payload size: {}", e),
                    },
                }
            }
        }
        let payloads_result = if use_blocking {
            // Offload serialization and compression to a blocking thread to avoid blocking the async runtime
            tokio::task::spawn_blocking(move || {
            let mut payloads: Vec<(Option<String>, Vec<u8>, HashMap<String, String>)> = Vec::with_capacity(items.len());
                
                // Only collect IDs if debug logging is enabled to avoid unnecessary allocation
                let mut ids = if log::log_enabled!(log::Level::Debug) {
                    Some(Vec::with_capacity(items.len()))
                } else {
                    None
                };

                for item in &items {
                    let id = item.get_id();
                    if let Some(id_list) = ids.as_mut() {
                        id_list.push(id.clone());
                    }
                    let encoded = match queue_codec() {
                        QueueCodec::Json => serde_json::to_vec(&item.inner)
                            .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                        QueueCodec::Msgpack => msgpack_encode(&item.inner)
                            .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                    };
                    match encoded {
                        Ok(p) => {
                            // Compression Logic (Threshold 1KB, Zstd)
                            let final_payload = compress_payload_owned(p, compression_threshold);
                            let headers = default_headers();
                            payloads.push((Some(id), final_payload, headers));
                        }
                        Err(e) => {
                            error!("Failed to serialize item: {}", e);
                        }
                    }
                }
                (payloads, ids, items.len())
            })
            .await
        } else {
            let mut payloads: Vec<(Option<String>, Vec<u8>, HashMap<String, String>)> = Vec::with_capacity(items.len());
            
            // Only collect IDs if debug logging is enabled to avoid unnecessary allocation
            let mut ids = if log::log_enabled!(log::Level::Debug) {
                Some(Vec::with_capacity(items.len()))
            } else {
                None
            };

            for item in &items {
                let id = item.get_id();
                if let Some(id_list) = ids.as_mut() {
                    id_list.push(id.clone());
                }
                let encoded = match queue_codec() {
                    QueueCodec::Json => serde_json::to_vec(&item.inner)
                        .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                    QueueCodec::Msgpack => msgpack_encode(&item.inner)
                        .map_err(|e| errors::Error::new(errors::ErrorKind::Queue, Some(e))),
                };
                match encoded {
                    Ok(p) => {
                        // Compression Logic (Threshold 1KB, Zstd)
                        let final_payload = compress_payload_owned(p, compression_threshold);
                        let headers = default_headers();
                        payloads.push((Some(id), final_payload, headers));
                    }
                    Err(e) => {
                        error!("Failed to serialize item: {}", e);
                    }
                }
            }
            Ok((payloads, ids, items.len()))
        };

        match payloads_result {
            Ok((payloads, ids, count)) => {
                if payloads.is_empty() {
                    return;
                }

                if let Err(e) = backend.publish_batch_with_headers(&topic, &payloads).await {
                    error!("Failed to publish batch to topic {}: {}", topic, e);
                } else {
                    log::info!("[QueueManager] forward_channel published batch: topic={} count={}", topic, count);
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

    pub async fn clean_storage(&self) -> errors::Result<()> {
        if let Some(backend) = &self.backend {
            backend.clean_storage().await?;
        }
        Ok(())
    }

    /// Send a message to the Dead Letter Queue (DLQ) manually.
    /// 
    /// This is useful when the application logic decides a message cannot be processed
    /// even if the message delivery itself was successful (e.g., max logic retries exceeded).
    pub async fn send_to_dlq<T>(&self, topic: &str, item: &T, reason: &str) -> errors::Result<()>
    where
        T: serde::Serialize + Identifiable + Send + Sync,
    {
        if let Some(backend) = &self.backend {
             let payload = match queue_codec() {
                 QueueCodec::Json => serde_json::to_vec(item).map_err(|e| errors::error::QueueError::OperationFailed(Box::new(e)))?,
                 QueueCodec::Msgpack => msgpack_encode(item).map_err(|e| errors::error::QueueError::OperationFailed(Box::new(e)))?,
             };
             backend.send_to_dlq(topic, &item.get_id(), &payload, reason).await?;
        }
        Ok(())
    }

    pub async fn read_dlq(&self, topic: &str, count: usize) -> errors::Result<Vec<(String, Vec<u8>, String, String)>> {
        if let Some(backend) = &self.backend {
            backend.read_dlq(topic, count).await
        } else {
            Ok(Vec::new())
        }
    }
}
