use crate::common::model::Request;
#[cfg(feature = "queue-kafka")]
use crate::common::model::config::KafkaConfig;
use crate::common::policy::{PolicyConfig, PolicyOverride};
use crate::errors::ErrorKind;
use crate::errors::Result;
#[cfg(feature = "queue-kafka")]
use crate::queue::kafka::KafkaQueue;
use crate::queue::{
    AckAction, HEADER_ATTEMPT, HEADER_NACK_REASON, Message, MqBackend, NackDisposition, NackPolicy,
    QueueManager, QueuedItem, decide_nack,
};
use async_trait::async_trait;
use rmp_serde as rmps;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::Duration;
#[cfg(feature = "queue-kafka")]
use tokio::time::timeout;
use uuid::Uuid;

#[derive(Clone)]
struct MockBackend {
    pub messages: Arc<Mutex<Vec<(String, Vec<u8>, std::collections::HashMap<String, String>)>>>,
    /// Captures each message's partition key (verifies account-affinity routing).
    pub keys: Arc<Mutex<Vec<Option<String>>>>,
}

impl MockBackend {
    fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(Vec::new())),
            keys: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl MqBackend for MockBackend {
    async fn publish(&self, topic: &str, _key: Option<&str>, payload: &[u8]) -> Result<()> {
        self.messages.lock().unwrap().push((
            topic.to_string(),
            payload.to_vec(),
            std::collections::HashMap::new(),
        ));
        Ok(())
    }

    async fn publish_with_headers(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &[u8],
        headers: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        self.keys.lock().unwrap().push(key.map(|k| k.to_string()));
        self.messages
            .lock()
            .unwrap()
            .push((topic.to_string(), payload.to_vec(), headers.clone()));
        Ok(())
    }

    async fn publish_batch_with_headers(
        &self,
        topic: &str,
        items: &[(
            Option<String>,
            Vec<u8>,
            std::collections::HashMap<String, String>,
        )],
    ) -> Result<()> {
        for (key, payload, headers) in items {
            self.publish_with_headers(topic, key.as_deref(), payload, headers)
                .await?;
        }
        Ok(())
    }

    async fn subscribe(&self, _topic: &str, _sender: mpsc::Sender<Message>) -> Result<()> {
        Ok(())
    }

    async fn clean_storage(&self) -> Result<()> {
        Ok(())
    }

    async fn send_to_dlq(
        &self,
        _topic: &str,
        _id: &str,
        _payload: &[u8],
        _reason: &str,
    ) -> Result<()> {
        Ok(())
    }

    async fn read_dlq(
        &self,
        _topic: &str,
        _count: usize,
    ) -> Result<Vec<(String, Vec<u8>, String, String)>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn test_queue_manager_integration() {
    let backend = Arc::new(MockBackend::new());
    let manager = QueueManager::new(Some(backend.clone()), 100);

    // We need to trigger forward_channel, which is called in subscribe().
    manager.subscribe();

    let request = Request::new("http://example.com", "GET");
    let tx = manager.get_request_push_channel();

    // Send request. It should go to channel.request_sender -> channel.request_receiver -> forward_channel -> backend.publish_batch
    // Note: get_request_push_channel returns channel.request_sender when backend is present.
    // And forward_channel listens on channel.request_receiver.
    // They are connected because Channel creates mpsc::channel.

    tx.send(QueuedItem::new(request))
        .await
        .expect("Failed to send request");

    // Allow some time for async processing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    {
        let messages = backend.messages.lock().unwrap();
        assert!(
            !messages.is_empty(),
            "Backend should have received a message"
        );
        assert_eq!(
            messages[0].0, "request-normal",
            "Topic should be 'request-normal'"
        );
        assert_eq!(
            messages[0].2.get(HEADER_ATTEMPT).map(|v| v.as_str()),
            Some("0")
        );

        // Verify payload is deserializable back to Request
        let payload = &messages[0].1;
        let _req: Request = rmps::from_slice(payload).expect("Failed to deserialize payload");
    }

    // Test High Priority
    use crate::common::model::Priority;
    let mut request_high = Request::new("http://example.com/high", "GET");
    request_high.priority = Priority::High;
    tx.send(QueuedItem::new(request_high))
        .await
        .expect("Failed to send high priority request");

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let messages = backend.messages.lock().unwrap();
    // Should have 2 messages now
    // Note: ordering depends on how batch flushing works, but since we waited and sent sequentially, likely order is preserved or at least it exists.
    // We search for it.
    let high_msg = messages
        .iter()
        .find(|(topic, _, _)| topic == "request-high");
    assert!(
        high_msg.is_some(),
        "Should find message with topic 'request-high'"
    );
}

#[test]
fn task_event_partition_key_is_account_not_run_id() {
    use crate::common::model::message::TaskEvent;
    use crate::queue::compensation::Identifiable;

    let ev = TaskEvent {
        account: "acct-7".to_string(),
        platform: "p".to_string(),
        module: Some(vec!["m".to_string()]),
        run_id: Uuid::now_v7(),
        priority: Default::default(),
    };
    // Partition key = account (for affinity routing); id = run_id (for deduplication /
    // compensation); the two are independent.
    assert_eq!(ev.partition_key(), "acct-7");
    assert_eq!(ev.get_id(), ev.run_id.to_string());
    assert_ne!(ev.partition_key(), ev.get_id());
    // QueuedItem must forward partition_key through to inner (rather than falling back to
    // get_id).
    let qi = QueuedItem::new(ev.clone());
    assert_eq!(qi.partition_key(), "acct-7");
    assert_eq!(qi.get_id(), ev.run_id.to_string());
}

#[tokio::test]
async fn task_published_with_account_partition_key_end_to_end() {
    use crate::common::model::message::TaskEvent;

    let backend = Arc::new(MockBackend::new());
    let manager = QueueManager::new(Some(backend.clone()), 100);
    manager.subscribe();

    let ev = TaskEvent {
        account: "acct-9".to_string(),
        platform: "p".to_string(),
        module: Some(vec!["m".to_string()]),
        run_id: Uuid::now_v7(),
        priority: Default::default(),
    };
    manager
        .get_task_push_channel()
        .send(QueuedItem::new(ev))
        .await
        .expect("send task");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let keys = backend.keys.lock().unwrap();
    assert!(
        !keys.is_empty(),
        "task should have been published to backend"
    );
    // Through the full forwarder → encode_items path, the MQ partition key should be the account
    // (not the random run_id).
    assert_eq!(keys[0].as_deref(), Some("acct-9"));
}

fn minimal_config(policy: Option<PolicyConfig>) -> crate::common::model::config::Config {
    use crate::common::model::config::{
        CacheConfig, ChannelConfig, CrawlerConfig, DatabaseConfig, DownloadConfig,
    };

    crate::common::model::config::Config {
        name: "queue_policy_test".to_string(),
        db: DatabaseConfig {
            url: None,
            database_schema: None,
            pool_size: None,
            tls: None,
        },
        download_config: DownloadConfig {
            downloader_expire: 1,
            timeout: 1,
            rate_limit: 0.0,
            enable_session: false,
            enable_locker: false,
            enable_rate_limit: false,
            cache_ttl: 1,
            wss_timeout: 1,
            pool_size: None,
            max_response_size: None,
        },
        cache: CacheConfig {
            ttl: 1,
            compression_threshold: None,
            enable_l1: None,
            l1_ttl_secs: None,
            l1_max_entries: None,
        },
        crawler: CrawlerConfig {
            request_max_retries: 1,
            task_max_errors: 1,
            module_max_errors: 1,
            module_locker_ttl: 1,
            node_id: None,
            task_concurrency: None,
            publish_concurrency: None,
            parser_concurrency: None,
            error_task_concurrency: None,
            backpressure_retry_delay_ms: None,
            dedup_ttl_secs: None,
            idle_stop_secs: None,
        },
        scheduler: None,
        sync: None,
        channel_config: ChannelConfig {
            blob_storage: None,
            kafka: None,
            nats: None,
            minid_time: 0,
            capacity: 10,
            queue_codec: None,
            batch_concurrency: None,
            compression_threshold: None,
            nack_max_retries: Some(0),
            nack_backoff_ms: Some(0),
        },
        proxy: None,
        api: None,
        event_bus: None,
        logger: None,
        policy,
    }
}

#[test]
fn test_queue_policy_override_applies_nack_policy() {
    let policy_cfg = PolicyConfig {
        overrides: vec![PolicyOverride {
            domain: Some("queue".to_string()),
            event_type: Some("nack".to_string()),
            phase: Some("failed".to_string()),
            kind: ErrorKind::Queue,
            retryable: None,
            backoff: None,
            dlq: None,
            alert: None,
            max_retries: Some(5),
            backoff_ms: Some(200),
        }],
    };

    let cfg = minimal_config(Some(policy_cfg));
    let manager = QueueManager::from_config(&cfg);

    assert_eq!(manager.nack_policy.max_retries, 5);
    assert_eq!(manager.nack_policy.backoff_ms, 200);
}

#[tokio::test]
async fn test_queue_compression() {
    let backend = Arc::new(MockBackend::new());
    let manager = QueueManager::new(Some(backend.clone()), 100);
    manager.subscribe();

    // Create a request with large body > 1KB
    let large_body = vec![b'a'; 2000];
    let request = Request::new("http://example.com/large", "POST").with_body(large_body.clone());

    let tx = manager.get_request_push_channel();
    tx.send(QueuedItem::new(request))
        .await
        .expect("Failed to send large request");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let messages = backend.messages.lock().unwrap();
    let payload_tuple = messages
        .iter()
        .find(|(t, _, _)| t == "request-normal")
        .expect("Msg not found");
    let payload = &payload_tuple.1;

    // Verify payload is compressed (starts with Zstd magic bytes)
    assert!(payload.len() > 4);
    assert_eq!(payload[0], 0x28);
    assert_eq!(payload[1], 0xB5);
    assert_eq!(payload[2], 0x2F);
    assert_eq!(payload[3], 0xFD);

    // Verify it is smaller than raw MessagePack (Request struct overhead + 2000 bytes)
    // 2000 'a's compress very well.
    assert!(payload.len() < 1000, "Compressed payload should be small");
}

#[tokio::test]
async fn test_message_nack_preserves_headers() {
    let (ack_tx, mut ack_rx) = tokio_mpsc::channel::<(String, AckAction)>(1);
    let mut headers = std::collections::HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "2".to_string());

    let msg = Message {
        payload: Arc::new(vec![1, 2, 3]),
        id: "topic@1-0".to_string(),
        headers: Arc::new(headers),
        ack_tx,
    };

    msg.nack("boom").await.expect("nack should succeed");

    let (_, action) = ack_rx.recv().await.expect("should receive nack action");
    match action {
        AckAction::Nack(reason, _payload, headers) => {
            assert_eq!(reason, "boom");
            assert_eq!(headers.get(HEADER_ATTEMPT).map(|v| v.as_str()), Some("2"));
            assert!(headers.get(HEADER_NACK_REASON).is_none());
        }
        _ => panic!("expected Nack action"),
    }
}

#[test]
fn test_decide_nack_retry_vs_dlq() {
    let policy = NackPolicy {
        max_retries: 2,
        backoff_ms: 0,
    };

    assert_eq!(
        decide_nack(policy, 0),
        NackDisposition::Retry { next_attempt: 1 }
    );
    assert_eq!(
        decide_nack(policy, 1),
        NackDisposition::Retry { next_attempt: 2 }
    );
    assert_eq!(decide_nack(policy, 2), NackDisposition::Dlq);
}

#[cfg(feature = "queue-kafka")]
fn build_kafka_config() -> Option<KafkaConfig> {
    let brokers = std::env::var("KAFKA_BROKERS").ok()?;
    if brokers.trim().is_empty() {
        return None;
    }

    let username = std::env::var("KAFKA_USERNAME").ok();
    let password = std::env::var("KAFKA_PASSWORD").ok();
    let tls = std::env::var("KAFKA_TLS")
        .ok()
        .and_then(|v| v.parse::<bool>().ok());

    Some(KafkaConfig {
        brokers,
        username,
        password,
        tls,
    })
}

#[cfg(feature = "queue-kafka")]
#[tokio::test]
async fn test_kafka_retry_then_ack() {
    let Some(kafka_config) = build_kafka_config() else {
        eprintln!("Kafka not configured, skipping test_kafka_retry_then_ack");
        return;
    };

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy {
        max_retries: 1,
        backoff_ms: 0,
    };
    let queue = match KafkaQueue::new(&kafka_config, 0, &namespace, nack_policy) {
        Ok(q) => q,
        Err(e) => {
            eprintln!(
                "Kafka not available, skipping test_kafka_retry_then_ack: {}",
                e
            );
            return;
        }
    };

    let topic = format!("contract_retry_{}", Uuid::new_v4());
    let (tx, mut rx) = tokio_mpsc::channel(10);
    if let Err(e) = queue.subscribe(&topic, tx).await {
        eprintln!(
            "Kafka subscribe failed, skipping test_kafka_retry_then_ack: {}",
            e
        );
        return;
    }

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut headers = std::collections::HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());

    let payload = vec![4, 4, 4];
    queue
        .publish_with_headers(&topic, None, &payload, &headers)
        .await
        .expect("publish");

    let mut msg1 = None;
    for _ in 0..3 {
        msg1 = timeout(Duration::from_secs(5), rx.recv())
            .await
            .ok()
            .flatten();
        if msg1.is_some() {
            break;
        }
    }
    let msg1 = msg1.expect("msg1");
    msg1.nack("first_fail").await.expect("nack1");

    let msg2 = timeout(Duration::from_secs(10), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg2");
    assert_eq!(
        msg2.headers.get(HEADER_ATTEMPT).map(|v| v.as_str()),
        Some("1")
    );
    assert_eq!(
        msg2.headers.get(HEADER_NACK_REASON).map(|v| v.as_str()),
        Some("first_fail")
    );
    msg2.ack().await.expect("ack2");
}

#[cfg(feature = "queue-kafka")]
#[tokio::test]
async fn test_kafka_out_of_order_ack() {
    let Some(kafka_config) = build_kafka_config() else {
        eprintln!("Kafka not configured, skipping test_kafka_out_of_order_ack");
        return;
    };

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy {
        max_retries: 0,
        backoff_ms: 0,
    };
    let queue = match KafkaQueue::new(&kafka_config, 0, &namespace, nack_policy) {
        Ok(q) => q,
        Err(e) => {
            eprintln!(
                "Kafka not available, skipping test_kafka_out_of_order_ack: {}",
                e
            );
            return;
        }
    };

    let topic = format!("contract_order_{}", Uuid::new_v4());
    let (tx, mut rx) = tokio_mpsc::channel(10);
    if let Err(e) = queue.subscribe(&topic, tx).await {
        eprintln!(
            "Kafka subscribe failed, skipping test_kafka_out_of_order_ack: {}",
            e
        );
        return;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut headers = std::collections::HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());

    let payload1 = vec![1, 1, 1];
    let payload2 = vec![2, 2, 2];
    queue
        .publish_with_headers(&topic, None, &payload1, &headers)
        .await
        .expect("publish1");
    queue
        .publish_with_headers(&topic, None, &payload2, &headers)
        .await
        .expect("publish2");

    let msg1 = timeout(Duration::from_secs(10), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg1");
    let msg2 = timeout(Duration::from_secs(10), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg2");

    msg2.ack().await.expect("ack2");
    msg1.ack().await.expect("ack1");
}
