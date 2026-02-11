use crate::{AckAction, Message, MqBackend, NackPolicy, NackDisposition, decide_nack, parse_attempt, HEADER_ATTEMPT, HEADER_NACK_REASON};
use async_trait::async_trait;
use common::model::config::KafkaConfig;
use errors::Result;
use errors::error::QueueError;
use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, OwnedHeaders, Message as KafkaMessageTrait};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use metrics::counter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use rdkafka::admin::{AdminClient, AdminOptions, AlterConfig, NewTopic, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;

use std::collections::{HashMap, HashSet, BTreeSet};
use std::sync::RwLock;

#[derive(Clone)]
pub struct KafkaQueue {
    producer: FutureProducer,
    admin_client: Arc<AdminClient<DefaultClientContext>>,
    bootstrap_servers: String,
    group_id: String,
    minid_time: u64,
    namespace: String,
    known_topics: Arc<RwLock<HashSet<String>>>,
    config: KafkaConfig,
    nack_policy: NackPolicy,
}

impl KafkaQueue {
    pub fn new(
        kafka_config: &KafkaConfig,
        minid_time: u64,
        namespace: &str,
        nack_policy: NackPolicy,
    ) -> Result<Self> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_config.brokers.as_str());
        config.set("message.timeout.ms", "5000");

        let use_tls = kafka_config.tls.unwrap_or(false);

        if let (Some(user), Some(pass)) = (&kafka_config.username, &kafka_config.password) {
            if use_tls {
                config.set("security.protocol", "SASL_SSL");
            } else {
                config.set("security.protocol", "SASL_PLAINTEXT");
            }
            config
                .set("sasl.mechanism", "PLAIN")
                .set("sasl.username", user)
                .set("sasl.password", pass);
        } else if use_tls {
            config.set("security.protocol", "SSL");
        }

        let producer: FutureProducer = config.create().map_err(|_| QueueError::ConnectionFailed)?;
        let admin_client: AdminClient<DefaultClientContext> =
            config.create().map_err(|_| QueueError::ConnectionFailed)?;

        Ok(Self {
            producer,
            admin_client: Arc::new(admin_client),
            bootstrap_servers: kafka_config.brokers.clone(),
            group_id: format!("{}-crawler_group", namespace),
            minid_time,
            namespace: namespace.to_string(),
            known_topics: Arc::new(RwLock::new(HashSet::new())),
            config: kafka_config.clone(),
            nack_policy,
        })
    }

    async fn ensure_topic_exists(&self, topic_name: &str) -> Result<()> {
        // Check cache first
        if let Ok(cache) = self.known_topics.read()
            && cache.contains(topic_name) {
                return Ok(());
            }

        // Try to create topic using AdminClient
        // We do this optimistically. If it exists, we'll get an error which we can ignore/handle.
        let mut new_topic =
            NewTopic::new(topic_name, 1, rdkafka::admin::TopicReplication::Fixed(1));
        let retention_ms = (self.minid_time * 3600 * 1000).to_string();
        if self.minid_time > 0 {
            new_topic = new_topic.set("retention.ms", &retention_ms);
        }
        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

        match self.admin_client.create_topics(&[new_topic], &opts).await {
            Ok(results) => {
                for result in results {
                    match result {
                        Ok(_) => info!("Created Kafka topic: {}", topic_name),
                        Err((_, err)) => {
                            if err != rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists {
                                warn!("Failed to create Kafka topic {}: {}", topic_name, err);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Admin client failed to create topic {}: {}", topic_name, e);
            }
        }

        // Now wait for the producer to actually see the topic
        let producer = self.producer.clone();
        let topic = topic_name.to_string();

        tokio::task::spawn_blocking(move || {
            let client = producer.client();
            let start = std::time::Instant::now();
            // Poll for up to 10 seconds
            while start.elapsed() < Duration::from_secs(10) {
                if let Ok(meta) = client.fetch_metadata(Some(&topic), Duration::from_secs(1))
                    && meta
                        .topics()
                        .iter()
                        .any(|t| t.name() == topic && t.error().is_none())
                    {
                        return Ok(());
                    }
                // Small sleep to avoid busy loop
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(QueueError::OperationFailed(Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "Topic {} creation timed out or metadata not propagated",
                    topic
                ),
            ))))
        })
        .await
        .map_err(|e| QueueError::OperationFailed(Box::new(e)))??;

        // If we got here, topic exists and is visible to producer
        if let Ok(mut cache) = self.known_topics.write() {
            cache.insert(topic_name.to_string());
        }

        Ok(())
    }
}

#[async_trait]
impl MqBackend for KafkaQueue {
    async fn publish_with_headers(&self, topic: &str, key: Option<&str>, payload: &[u8], headers: &HashMap<String, String>) -> Result<()> {
        let topic_key = format!("{}-{}", self.namespace, topic);

        // Retry loop for publishing
        let mut retries = 3;
        loop {
            // 1. Check if we know the topic exists
            let known = self
                .known_topics
                .read()
                .map(|c| c.contains(&topic_key))
                .unwrap_or(false);

            if !known {
                // If not known, try to ensure it exists
                if let Err(e) = self.ensure_topic_exists(&topic_key).await {
                    warn!("Failed to ensure topic {} exists: {}", topic_key, e);
                    // Continue anyway, maybe it exists but check failed
                }
            }

            let mut record = FutureRecord::to(&topic_key).payload(payload);
            if let Some(k) = key {
                record = record.key(k);
            } else {
                 record = record.key("");
            }
            
            let mut kafka_headers = OwnedHeaders::new();
            for (k, v) in headers {
                kafka_headers = kafka_headers.insert(rdkafka::message::Header { key: k, value: Some(v.as_bytes()) });
            }
            record = record.headers(kafka_headers);

            // 2. Try to send
            return match self.producer.send(record, Duration::from_secs(5)).await {
                Ok(_) => Ok(()),
                Err((e, _)) => {
                    // Check if it's an unknown topic error
                    let is_unknown = matches!(&e, rdkafka::error::KafkaError::MessageProduction(
                        rdkafka::types::RDKafkaErrorCode::UnknownTopic,
                    ));

                    if is_unknown || retries > 0 {
                        warn!(
                            "Publish failed for {}, retrying... ({}) Error: {:?}",
                            topic_key, retries, e
                        );

                        if is_unknown {
                            // Force re-check/creation
                            if let Ok(mut cache) = self.known_topics.write() {
                                cache.remove(&topic_key);
                            }
                        }

                        retries -= 1;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }

                    error!("Kafka publish failed for topic {}: {:?}", topic_key, e);
                    Err(QueueError::PushFailed(Box::new(e)).into())
                }
            };
        }
    }

    async fn publish_batch(&self, topic: &str, items: &[(Option<String>, Vec<u8>)]) -> Result<()> {
        let items_with_headers: Vec<(Option<String>, Vec<u8>, HashMap<String, String>)> = items.iter().map(|(k, p)| (k.clone(), p.clone(), HashMap::new())).collect();
        self.publish_batch_with_headers(topic, &items_with_headers).await
    }

    async fn publish_batch_with_headers(&self, topic: &str, items: &[(Option<String>, Vec<u8>, HashMap<String, String>)]) -> Result<()> {
        let topic_key = format!("{}-{}", self.namespace, topic);

        // Ensure topic exists (naive check once for the batch)
        {
            let known = self
                .known_topics
                .read()
                .map(|c| c.contains(&topic_key))
                .unwrap_or(false);

            if !known
                && let Err(e) = self.ensure_topic_exists(&topic_key).await {
                     warn!("Failed to ensure topic {} exists for batch: {}", topic_key, e);
                }
        }

        let mut futures = Vec::with_capacity(items.len());
        
        for (key, payload, headers) in items {
             let mut record = FutureRecord::to(&topic_key).payload(payload);
             if let Some(k) = key {
                 record = record.key(k.as_str());
             } else {
                 record = record.key("");
             }
             
             let mut kafka_headers = OwnedHeaders::new();
             for (k, v) in headers {
                 kafka_headers = kafka_headers.insert(rdkafka::message::Header { key: k, value: Some(v.as_bytes()) });
             }
             record = record.headers(kafka_headers);
             
             futures.push(self.producer.send(record, Duration::from_secs(5)));
        }

        // Wait for all
        let results = futures::future::join_all(futures).await;

        let mut error_count = 0;
        for res in results {
            if let Err((e, _)) = res {
                error!("Kafka batch publish error: {:?}", e);
                error_count += 1;
            }
        }

        if error_count > 0 {
            // We return an error if ANY failed, but some might have succeeded.
            // For now, we just report failure.
            return Err(QueueError::OperationFailed(Box::new(std::io::Error::other(
                format!("Failed to publish {} messages in batch", error_count),
            ))).into());
        }

        Ok(())
    }

    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> Result<()> {
        let bootstrap_servers = self.bootstrap_servers.clone();
        let group_id = self.group_id.clone();
        let kafka_config = self.config.clone();
        let topic = format!("{}-{}", self.namespace, topic);

        // Ensure topic exists before subscribing
        if let Err(e) = self.ensure_topic_exists(&topic).await {
            warn!(
                "Failed to ensure topic {} exists before subscribing: {}",
                topic, e
            );
        }

        let dlq_producer = self.producer.clone();
        let nack_policy = self.nack_policy;

        tokio::spawn(async move {
            info!("Starting Kafka listener for topic: {}", topic);

            let mut client_config = ClientConfig::new();
            client_config
                .set("group.id", &group_id)
                .set("bootstrap.servers", &bootstrap_servers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.commit", "false")
                .set("auto.offset.reset", "earliest");
            
            // Re-apply security config (cannot easily share config from new() due to moving)
            if let (Some(user), Some(pass)) = (&kafka_config.username, &kafka_config.password) {
                if kafka_config.tls.unwrap_or(false) {
                    client_config.set("security.protocol", "SASL_SSL");
                } else {
                    client_config.set("security.protocol", "SASL_PLAINTEXT");
                }
                client_config
                    .set("sasl.mechanism", "PLAIN")
                    .set("sasl.username", user)
                    .set("sasl.password", pass);
            } else if kafka_config.tls.unwrap_or(false) {
                client_config.set("security.protocol", "SSL");
            }

            let consumer: StreamConsumer = match client_config.create()
            {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to create Kafka consumer: {}", e);
                    return;
                }
            };

            if let Err(e) = consumer.subscribe(&[&topic]) {
                error!("Failed to subscribe to Kafka topic {}: {}", topic, e);
                return;
            }

            let consumer = Arc::new(consumer);
            let ack_consumer = consumer.clone();
            let (ack_tx, mut ack_rx) = mpsc::channel::<(String, AckAction)>(1000);
            
            // Shared state for In-Flight tracking: Partition -> Set of Offsets
            let in_flight_tracker = Arc::new(RwLock::new(HashMap::<i32, BTreeSet<i64>>::new()));
            let tracker_producer = in_flight_tracker.clone();
            let tracker_consumer = in_flight_tracker.clone();

            // Ack Processor
            tokio::spawn(async move {
                // Keep track of the highest offset we've seen acked per partition, to handle the "empty queue" case
                let mut max_acked: HashMap<i32, i64> = HashMap::new();

                while let Some((id_str, action)) = ack_rx.recv().await {
                    let should_commit = match action {
                        AckAction::Ack => true,
                        AckAction::Nack(reason, payload, headers) => {
                             let attempt = parse_attempt(&headers);
                             let disposition = decide_nack(nack_policy, attempt);
                             let action_label = match disposition {
                                 NackDisposition::Retry { .. } => "retry",
                                 NackDisposition::Dlq => "dlq",
                             };
                             counter!(
                                 "policy_decisions_total",
                                 "domain" => "queue",
                                 "event_type" => "nack",
                                 "phase" => "failed",
                                 "kind" => "queue",
                                 "action" => action_label
                             )
                             .increment(1);

                             let parts: Vec<&str> = id_str.split(':').collect();
                             if !parts.is_empty() {
                                 let original_topic = parts[0];

                                 if let NackDisposition::Retry { next_attempt } = disposition {
                                     if nack_policy.backoff_ms > 0 {
                                         tokio::time::sleep(Duration::from_millis(nack_policy.backoff_ms)).await;
                                     }

                                     let mut next_headers = (*headers).clone();
                                     next_headers.insert(HEADER_ATTEMPT.to_string(), next_attempt.to_string());
                                     next_headers.insert(HEADER_NACK_REASON.to_string(), reason.clone());

                                     let mut record = FutureRecord::to(original_topic).payload(payload.as_slice()).key("");
                                     let mut kafka_headers = OwnedHeaders::new();
                                     for (k, v) in &next_headers {
                                         kafka_headers = kafka_headers.insert(rdkafka::message::Header { key: k, value: Some(v.as_bytes()) });
                                     }
                                     record = record.headers(kafka_headers);

                                     if let Err((e, _)) = dlq_producer.send(record, Duration::from_secs(5)).await {
                                         error!("Failed to retry publish to {}: {:?}", original_topic, e);
                                         false
                                     } else {
                                         true
                                     }
                                 } else {
                                     let dlq_topic = format!("{}-dlq", original_topic);
                                     let record = FutureRecord::to(&dlq_topic).payload(payload.as_slice()).key(&reason);
                                     if let Err((e, _)) = dlq_producer.send(record, Duration::from_secs(5)).await {
                                         error!("Failed to send to DLQ {}: {:?}", dlq_topic, e);
                                         false
                                     } else {
                                         true
                                     }
                                 }
                             } else {
                                 true
                             }
                        }
                    };

                    if should_commit {
                        let parts: Vec<&str> = id_str.split(':').collect();
                        if parts.len() == 3 {
                             let topic_name = parts[0];
                             if let (Ok(partition), Ok(offset)) = (parts[1].parse::<i32>(), parts[2].parse::<i64>()) {
                                 
                                 // Update Tracker
                                 let commit_offset = {
                                     let mut tracker = tracker_consumer.write().unwrap();
                                     if let Some(set) = tracker.get_mut(&partition) {
                                         set.remove(&offset);
                                         // Update max_acked
                                         let current_max = max_acked.entry(partition).or_insert(-1);
                                         if offset > *current_max {
                                             *current_max = offset;
                                         }
                                         
                                         if let Some(&min_inflight) = set.iter().next() {
                                             // If there are gaps, we can only commit up to the first gap.
                                             // min_inflight is the first message STILL processing.
                                             // So we can safely commit 'min_inflight'. 
                                             // (Kafka commit offset N means "I have processed N-1, give me N next")
                                             Some(min_inflight)
                                         } else {
                                             // No in-flight messages. We can commit max_acked + 1.
                                             Some(*current_max + 1)
                                         }
                                     } else {
                                         // Should not happen if logic is correct
                                         None
                                     }
                                 };

                                 if let Some(off) = commit_offset {
                                     let mut tpl = rdkafka::TopicPartitionList::new();
                                     tpl.add_partition_offset(topic_name, partition, rdkafka::Offset::Offset(off)).ok();
                                     // We use Async commit. It is non-blocking.
                                     // Using commit (not store_offset) ensures it's sent to broker.
                                     if let Err(e) = ack_consumer.commit(&tpl, rdkafka::consumer::CommitMode::Async) {
                                         // Use debug level for commit errors to avoid log spam (e.g. rebalance in progress)
                                         log::debug!("Failed to commit kafka message: {}", e);
                                     }
                                 }
                             }
                        }
                    }
                }
            });

            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            let partition = m.partition();
                            let offset = m.offset();
                            let id = format!("{}:{}:{}", m.topic(), partition, offset);
                            
                            // Register in-flight
                            {
                                let mut tracker = tracker_producer.write().unwrap();
                                tracker.entry(partition).or_default().insert(offset);
                            }
                            
                            let mut headers = HashMap::new();
                            if let Some(h) = m.headers() {
                                for i in 0..h.count() {
                                    let header = h.get(i);
                                    if let Some(v) = header.value {
                                        headers.insert(header.key.to_string(), String::from_utf8_lossy(v).to_string());
                                    }
                                }
                            }

                            let msg = Message {
                                payload: std::sync::Arc::new(payload.to_vec()),
                                id,
                                headers: std::sync::Arc::new(headers),
                                ack_tx: ack_tx.clone(),
                            };

                            if sender.send(msg).await.is_err() {
                                warn!("Subscriber channel closed for topic {}", topic);
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Kafka error: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn clean_storage(&self) -> Result<()> {
        if self.minid_time == 0 {
            return Ok(());
        }
        let retention_ms = (self.minid_time * 3600 * 1000).to_string();
        info!(
            "Starting Kafka storage cleanup for namespace {}, retention: {}ms",
            self.namespace, retention_ms
        );

        let producer = self.producer.clone();
        let namespace = self.namespace.clone();

        let metadata = tokio::task::spawn_blocking(move || {
            producer
                .client()
                .fetch_metadata(None, Duration::from_secs(10))
        })
        .await
        .map_err(|e| QueueError::OperationFailed(Box::new(e)))?
        .map_err(|e| QueueError::OperationFailed(Box::new(e)))?;

        let topics: Vec<String> = metadata
            .topics()
            .iter()
            .map(|t| t.name().to_string())
            .filter(|n| n.starts_with(&format!("{}-", namespace)))
            .collect();

        if topics.is_empty() {
            return Ok(());
        }

        let resources: Vec<AlterConfig> = topics
            .iter()
            .map(|t| {
                let mut entries = HashMap::new();
                entries.insert("retention.ms", retention_ms.as_str());

                AlterConfig {
                    specifier: ResourceSpecifier::Topic(t),
                    entries,
                }
            })
            .collect();

        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));

        match self.admin_client.alter_configs(&resources, &opts).await {
            Ok(results) => {
                for result in results {
                    if let Err(e) = result {
                        warn!("Failed to update retention for topic: {:?}", e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to alter configs: {}", e);
                return Err(QueueError::OperationFailed(Box::new(e)).into());
            }
        }
        Ok(())
    }

    async fn send_to_dlq(&self, topic: &str, _id: &str, payload: &[u8], reason: &str) -> Result<()> {
        let topic_key = format!("{}-{}-dlq", self.namespace, topic);
        let record = FutureRecord::to(&topic_key).payload(payload).key(reason);
        
        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => Err(QueueError::PushFailed(Box::new(e)).into()),
        }
    }

    async fn read_dlq(&self, _topic: &str, _count: usize) -> Result<Vec<(String, Vec<u8>, String, String)>> {
        // Kafka DLQ inspection requires a consumer which is heavy. 
        // Not implemented for now.
        warn!("DLQ inspection not implemented for KafkaQueue yet");
        Ok(Vec::new())
    }
}
