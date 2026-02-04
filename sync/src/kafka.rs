use super::backend::CoordinationBackend;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::sync::Arc;
// use std::sync::Arc;
use crate::RedisBackend;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone)]
pub struct KafkaBackend {
    producer: FutureProducer,
    redis_backend: Arc<RedisBackend>, // For KV and Lock
    brokers: String,
}

impl KafkaBackend {
    pub fn new(brokers: &str, redis_backend: Arc<RedisBackend>) -> Self {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", brokers);
        config.set("message.timeout.ms", "5000");

        let producer: FutureProducer = config.create().expect("Producer creation error");

        // let producer: FutureProducer = ClientConfig::new()
        //     .set("bootstrap.servers", brokers)
        //     .set("message.timeout.ms", "5000")
        //     .create()
        //     .expect("Producer creation error");

        Self {
            producer,
            redis_backend,
            brokers: brokers.to_string(),
        }
    }
}

#[async_trait]
impl CoordinationBackend for KafkaBackend {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String> {
        let record = FutureRecord::to(topic).payload(payload).key("");
        self.producer
            .send(record, std::time::Duration::from_secs(0))
            .await
            .map_err(|(e, _)| e.to_string())?;
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        let brokers = self.brokers.clone();
        let topic = topic.to_string();
        // Broadcast mode: Unique Group ID for each subscriber
        let group_id = format!("sync_group_{}", Uuid::new_v4());

        let (tx, rx) = mpsc::channel(1000);
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            let consumer: StreamConsumer = match ClientConfig::new()
                .set("group.id", &group_id)
                .set("bootstrap.servers", &brokers)
                .set("enable.partition.eof", "false")
                .set("session.timeout.ms", "6000")
                .set("enable.auto.commit", "true")
                .set("auto.offset.reset", "latest") // Start from latest for broadcast
                .create()
            {
                Ok(c) => c,
                Err(e) => {
                    let _ = signal_tx.send(Err(e.to_string()));
                    return;
                }
            };

            if let Err(e) = consumer.subscribe(&[&topic]) {
                let _ = signal_tx.send(Err(e.to_string()));
                return;
            }

            // Signal success
            if signal_tx.send(Ok(())).is_err() {
                return;
            }

            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if tx.send(payload.to_vec()).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Kafka error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        });

        match signal_rx.await {
            Ok(Ok(())) => Ok(rx),
            Ok(Err(e)) => Err(e),
            Err(_) => Err("Subscription task failed to start".to_string()),
        }
    }

    // Delegate KV and Lock operations to RedisBackend
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.redis_backend.set(key, value).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.redis_backend.get(key).await
    }

    async fn cas(&self, key: &str, old_val: Option<&[u8]>, new_val: &[u8]) -> Result<bool, String> {
        self.redis_backend.cas(key, old_val, new_val).await
    }

    async fn acquire_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        self.redis_backend.acquire_lock(key, value, ttl_ms).await
    }

    async fn renew_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        self.redis_backend.renew_lock(key, value, ttl_ms).await
    }
}
