use crate::{Message, MqBackend};
use async_trait::async_trait;
use errors::Result;
use errors::error::QueueError;
use log::{error, info, warn};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as KafkaMessageTrait;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use common::model::config::KafkaConfig;

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, AlterConfig, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;

use std::collections::{HashSet, HashMap};
use std::sync::RwLock;

#[derive(Clone)]
pub struct KafkaQueue {
    producer: FutureProducer,
    admin_client: Arc<AdminClient<DefaultClientContext>>,
    bootstrap_servers: String,
    group_id: String,
    minid_time:u64,
    namespace: String,
    known_topics: Arc<RwLock<HashSet<String>>>,
}

impl KafkaQueue {
    pub fn new(kafka_config: &KafkaConfig,minid_time:u64, namespace: &str) -> Result<Self> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", kafka_config.brokers.as_str());
        config.set("message.timeout.ms", "5000");

        if let (Some(user), Some(pass)) = (&kafka_config.username, &kafka_config.password) {
            config
                .set("security.protocol", "SASL_PLAINTEXT")
                .set("sasl.mechanism", "PLAIN")
                .set("sasl.username", user)
                .set("sasl.password", pass);
        }

        let producer: FutureProducer = config.create().map_err(|_| QueueError::ConnectionFailed)?;
        let admin_client: AdminClient<DefaultClientContext> = config.create().map_err(|_| QueueError::ConnectionFailed)?;

        Ok(Self {
            producer,
            admin_client: Arc::new(admin_client),
            bootstrap_servers: kafka_config.brokers.clone(),
            group_id: format!("{}-crawler_group", namespace),
            minid_time,
            namespace: namespace.to_string(),
            known_topics: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    async fn ensure_topic_exists(&self, topic_name: &str) -> Result<()> {
        // Check cache first
        if let Ok(cache) = self.known_topics.read() {
            if cache.contains(topic_name) {
                return Ok(());
            }
        }

        // Try to create topic using AdminClient
        // We do this optimistically. If it exists, we'll get an error which we can ignore/handle.
        let mut new_topic = NewTopic::new(topic_name, 1, rdkafka::admin::TopicReplication::Fixed(1));
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
                 match client.fetch_metadata(Some(&topic), Duration::from_secs(1)) {
                     Ok(meta) => {
                         if meta.topics().iter().any(|t| t.name() == topic && t.error().is_none()) {
                             return Ok(());
                         }
                     }
                     Err(_) => {}
                 }
                 // Small sleep to avoid busy loop
                 std::thread::sleep(Duration::from_millis(100));
             }
             Err(QueueError::OperationFailed(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, format!("Topic {} creation timed out or metadata not propagated", topic)))))
        }).await.map_err(|e| QueueError::OperationFailed(Box::new(e)))??;
        
        // If we got here, topic exists and is visible to producer
        if let Ok(mut cache) = self.known_topics.write() {
            cache.insert(topic_name.to_string());
        }
        
        Ok(())
    }
}

#[async_trait]
impl MqBackend for KafkaQueue {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
        let topic_key = format!("{}-{}", self.namespace, topic);
        
        // Retry loop for publishing
        let mut retries = 3;
        loop {
            // 1. Check if we know the topic exists
            let known = self.known_topics.read().map(|c| c.contains(&topic_key)).unwrap_or(false);
            
            if !known {
                // If not known, try to ensure it exists
                if let Err(e) = self.ensure_topic_exists(&topic_key).await {
                    warn!("Failed to ensure topic {} exists: {}", topic_key, e);
                    // Continue anyway, maybe it exists but check failed
                }
            }

            let record = FutureRecord::to(&topic_key).payload(payload).key(""); 

            // 2. Try to send
            return match self.producer.send(record, Duration::from_secs(5)).await {
                Ok(_) => Ok(()),
                Err((e, _)) => {
                    // Check if it's an unknown topic error
                    let is_unknown = match &e {
                        rdkafka::error::KafkaError::MessageProduction(rdkafka::types::RDKafkaErrorCode::UnknownTopic) => true,
                        _ => false
                    };

                    if is_unknown || retries > 0 {
                        warn!("Publish failed for {}, retrying... ({}) Error: {:?}", topic_key, retries, e);

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
            }
        }
    }

    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> Result<()> {
        let bootstrap_servers = self.bootstrap_servers.clone();
        let group_id = self.group_id.clone();
        let topic = format!("{}-{}", self.namespace, topic);
        
        // Ensure topic exists before subscribing
        if let Err(e) = self.ensure_topic_exists(&topic).await {
             warn!("Failed to ensure topic {} exists before subscribing: {}", topic, e);
        }

        tokio::spawn(async move {
            info!("Starting Kafka listener for topic: {}", topic);

            let consumer: StreamConsumer = match ClientConfig::new()
                .set("group.id", &group_id)
                .set("bootstrap.servers", &bootstrap_servers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.commit", "true") 
                .set("auto.offset.reset", "earliest") 
                .create()
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

            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            let (ack_tx, mut ack_rx) = mpsc::channel(1);

                            let msg = Message {
                                payload: payload.to_vec(),
                                ack_tx,
                            };

                            if sender.send(msg).await.is_ok() && ack_rx.recv().await.is_some() {
                                if let Err(e) = consumer
                                    .commit_message(&m, rdkafka::consumer::CommitMode::Async)
                                {
                                    error!("Failed to commit message: {}", e);
                                }
                            } else {
                                // No subscribers
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
        info!("Starting Kafka storage cleanup for namespace {}, retention: {}ms", self.namespace, retention_ms);

        let producer = self.producer.clone();
        let namespace = self.namespace.clone();
        
        let metadata = tokio::task::spawn_blocking(move || {
            producer.client().fetch_metadata(None, Duration::from_secs(10))
        }).await.map_err(|e| QueueError::OperationFailed(Box::new(e)))?
        .map_err(|e| QueueError::OperationFailed(Box::new(e)))?;

        let topics: Vec<String> = metadata.topics().iter()
            .map(|t| t.name().to_string())
            .filter(|n| n.starts_with(&format!("{}-", namespace)))
            .collect();

        if topics.is_empty() {
            return Ok(());
        }

        let resources: Vec<AlterConfig> = topics.iter().map(|t| {
            let mut entries = HashMap::new();
            entries.insert("retention.ms", retention_ms.as_str());
            
            AlterConfig {
                specifier: ResourceSpecifier::Topic(t),
                entries,
            }
        }).collect();

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
}
