use crate::{AckAction, MqBackend, QueueManager, Message, QueuedItem, HEADER_ATTEMPT, HEADER_NACK_REASON, NackDisposition, NackPolicy, decide_nack};
use async_trait::async_trait;
use errors::Result;
use tokio::sync::mpsc;
use std::sync::{Arc, Mutex};
use common::model::Request;
use rmp_serde as rmps;
use tokio::sync::mpsc as tokio_mpsc;
use common::model::config::{RedisConfig, KafkaConfig};
use uuid::Uuid;
use tokio::time::{timeout, Duration};
use deadpool_redis::redis;
use crate::kafka::KafkaQueue;
use common::policy::{PolicyConfig, PolicyOverride};
use errors::ErrorKind;

#[derive(Clone)]
struct MockBackend {
    pub messages: Arc<Mutex<Vec<(String, Vec<u8>, std::collections::HashMap<String, String>)>>>,
}

impl MockBackend {
    fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl MqBackend for MockBackend {
    async fn publish(&self, topic: &str, _key: Option<&str>, payload: &[u8]) -> Result<()> {
        self.messages.lock().unwrap().push((topic.to_string(), payload.to_vec(), std::collections::HashMap::new()));
        Ok(())
    }
    
    async fn publish_with_headers(&self, topic: &str, _key: Option<&str>, payload: &[u8], headers: &std::collections::HashMap<String, String>) -> Result<()> {
        self.messages
            .lock()
            .unwrap()
            .push((topic.to_string(), payload.to_vec(), headers.clone()));
        Ok(())
    }
    
    async fn publish_batch_with_headers(&self, topic: &str, items: &[(Option<String>, Vec<u8>, std::collections::HashMap<String, String>)]) -> Result<()> {
        for (key, payload, headers) in items {
            self.publish_with_headers(topic, key.as_deref(), payload, headers).await?;
        }
        Ok(())
    }
    
    async fn subscribe(&self, _topic: &str, _sender: mpsc::Sender<Message>) -> Result<()> {
        Ok(())
    }

    async fn clean_storage(&self) -> Result<()> {
        Ok(())
    }
    
    async fn send_to_dlq(&self, _topic: &str, _id: &str, _payload: &[u8], _reason: &str) -> Result<()> {
        Ok(())
    }
    
    async fn read_dlq(&self, _topic: &str, _count: usize) -> Result<Vec<(String, Vec<u8>, String, String)>> {
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
    
    tx.send(QueuedItem::new(request)).await.expect("Failed to send request");
    
    // Allow some time for async processing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    {
        let messages = backend.messages.lock().unwrap();
        assert!(!messages.is_empty(), "Backend should have received a message");
        assert_eq!(messages[0].0, "request-normal", "Topic should be 'request-normal'");
        assert_eq!(messages[0].2.get(HEADER_ATTEMPT).map(|v| v.as_str()), Some("0"));
        
        // Verify payload is deserializable back to Request
        let payload = &messages[0].1;
        let _req: Request = rmps::from_slice(payload).expect("Failed to deserialize payload");
    }

    // Test High Priority
    use common::model::Priority;
    let mut request_high = Request::new("http://example.com/high", "GET");
    request_high.priority = Priority::High;
    tx.send(QueuedItem::new(request_high)).await.expect("Failed to send high priority request");
    
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    let messages = backend.messages.lock().unwrap();
    // Should have 2 messages now
    // Note: ordering depends on how batch flushing works, but since we waited and sent sequentially, likely order is preserved or at least it exists.
    // We search for it.
    let high_msg = messages.iter().find(|(topic, _, _)| topic == "request-high");
    assert!(high_msg.is_some(), "Should find message with topic 'request-high'");
}

fn minimal_config(policy: Option<PolicyConfig>) -> common::model::config::Config {
    use common::model::config::{
        CacheConfig, ChannelConfig, CrawlerConfig, DatabaseConfig, DownloadConfig,
    };

    common::model::config::Config {
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
            enable_cache: false,
            enable_locker: false,
            enable_rate_limit: false,
            cache_ttl: 1,
            wss_timeout: 1,
            pool_size: None,
            max_response_size: None,
        },
        cache: CacheConfig {
            ttl: 1,
            redis: None,
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
            dedup_ttl_secs: None,
            idle_stop_secs: None,
        },
        scheduler: None,
        sync: None,
        cookie: None,
        channel_config: ChannelConfig {
            blob_storage: None,
            redis: None,
            kafka: None,
            compensator: None,
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
    tx.send(QueuedItem::new(request)).await.expect("Failed to send large request");

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let messages = backend.messages.lock().unwrap();
    let payload_tuple = messages.iter().find(|(t,_,_)| t == "request-normal").expect("Msg not found");
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

    assert_eq!(decide_nack(policy, 0), NackDisposition::Retry { next_attempt: 1 });
    assert_eq!(decide_nack(policy, 1), NackDisposition::Retry { next_attempt: 2 });
    assert_eq!(decide_nack(policy, 2), NackDisposition::Dlq);
}

fn build_redis_config() -> RedisConfig {
    let host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("REDIS_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(6379);
    let db = std::env::var("REDIS_DB")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(0);

    RedisConfig {
        redis_host: host,
        redis_port: port,
        redis_db: db,
        redis_username: std::env::var("REDIS_USERNAME").ok(),
        redis_password: std::env::var("REDIS_PASSWORD").ok(),
        pool_size: Some(8),
        shards: Some(1),
        tls: Some(false),
        claim_min_idle: Some(1000),
        claim_count: Some(10),
        claim_interval: Some(500),
        listener_count: Some(1),
    }
}

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

async fn redis_available(cfg: &RedisConfig) -> bool {
    let pool = utils::connector::create_redis_pool(
        &cfg.redis_host,
        cfg.redis_port,
        cfg.redis_db,
        &cfg.redis_username,
        &cfg.redis_password,
        cfg.pool_size,
        cfg.tls.unwrap_or(false),
    );
    let Some(pool) = pool else {
        return false;
    };

    match pool.get().await {
        Ok(mut conn) => {
            let pong: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
            pong.is_ok()
        }
        Err(_) => false,
    }
}

#[tokio::test]
async fn test_redis_duplicate_delivery_and_order_independent_ack() {
    let cfg = build_redis_config();
    if !redis_available(&cfg).await {
        eprintln!("Redis not available, skipping test_redis_duplicate_delivery_and_order_independent_ack");
        return;
    }

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy { max_retries: 1, backoff_ms: 0 };
    let queue = crate::RedisQueue::new(&cfg, 0, &namespace, 10, nack_policy).expect("redis queue");

    let (tx, mut rx) = tokio_mpsc::channel(10);
    queue.subscribe("contract_dup", tx).await.expect("subscribe");

    let mut headers = std::collections::HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());

    let payload = vec![1, 2, 3, 4];
    queue
        .publish_with_headers("contract_dup", None, &payload, &headers)
        .await
        .expect("publish 1");
    queue
        .publish_with_headers("contract_dup", None, &payload, &headers)
        .await
        .expect("publish 2");

    let msg1 = timeout(Duration::from_secs(3), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg1");
    let msg2 = timeout(Duration::from_secs(3), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg2");

    assert_eq!(msg1.payload.as_slice(), payload.as_slice());
    assert_eq!(msg2.payload.as_slice(), payload.as_slice());
    assert_ne!(msg1.id, msg2.id);

    // Ack out of order to ensure ordering is not required for ack correctness.
    msg2.ack().await.expect("ack2");
    msg1.ack().await.expect("ack1");
}

#[tokio::test]
async fn test_redis_retry_then_dlq_path() {
    let cfg = build_redis_config();
    if !redis_available(&cfg).await {
        eprintln!("Redis not available, skipping test_redis_retry_then_dlq_path");
        return;
    }

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy { max_retries: 1, backoff_ms: 0 };
    let queue = crate::RedisQueue::new(&cfg, 0, &namespace, 10, nack_policy).expect("redis queue");

    let (tx, mut rx) = tokio_mpsc::channel(10);
    queue.subscribe("contract_retry", tx).await.expect("subscribe");

    let mut headers = std::collections::HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());

    let payload = vec![9, 9, 9];
    queue
        .publish_with_headers("contract_retry", None, &payload, &headers)
        .await
        .expect("publish");

    let msg1 = timeout(Duration::from_secs(3), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg1");
    msg1.nack("first_fail").await.expect("nack1");

    let msg2 = timeout(Duration::from_secs(5), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg2");
    assert_eq!(msg2.headers.get(HEADER_ATTEMPT).map(|v| v.as_str()), Some("1"));
    assert_eq!(msg2.headers.get(HEADER_NACK_REASON).map(|v| v.as_str()), Some("first_fail"));
    msg2.nack("second_fail").await.expect("nack2");

    let mut dlq_entries = Vec::new();
    for _ in 0..10 {
        dlq_entries = queue.read_dlq("contract_retry", 10).await.expect("read dlq");
        if !dlq_entries.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert!(!dlq_entries.is_empty(), "expected DLQ entry after retry exhaustion");
    let (_, dlq_payload, reason, _) = &dlq_entries[0];
    assert_eq!(dlq_payload.as_slice(), payload.as_slice());
    assert_eq!(reason, "second_fail");
}

#[tokio::test]
async fn test_kafka_retry_then_ack() {
    let Some(kafka_config) = build_kafka_config() else {
        eprintln!("Kafka not configured, skipping test_kafka_retry_then_ack");
        return;
    };

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy { max_retries: 1, backoff_ms: 0 };
    let queue = match KafkaQueue::new(&kafka_config, 0, &namespace, nack_policy) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("Kafka not available, skipping test_kafka_retry_then_ack: {}", e);
            return;
        }
    };

    let topic = format!("contract_retry_{}", Uuid::new_v4());
    let (tx, mut rx) = tokio_mpsc::channel(10);
    if let Err(e) = queue.subscribe(&topic, tx).await {
        eprintln!("Kafka subscribe failed, skipping test_kafka_retry_then_ack: {}", e);
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
    assert_eq!(msg2.headers.get(HEADER_ATTEMPT).map(|v| v.as_str()), Some("1"));
    assert_eq!(msg2.headers.get(HEADER_NACK_REASON).map(|v| v.as_str()), Some("first_fail"));
    msg2.ack().await.expect("ack2");
}

#[tokio::test]
async fn test_kafka_out_of_order_ack() {
    let Some(kafka_config) = build_kafka_config() else {
        eprintln!("Kafka not configured, skipping test_kafka_out_of_order_ack");
        return;
    };

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy { max_retries: 0, backoff_ms: 0 };
    let queue = match KafkaQueue::new(&kafka_config, 0, &namespace, nack_policy) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("Kafka not available, skipping test_kafka_out_of_order_ack: {}", e);
            return;
        }
    };

    let topic = format!("contract_order_{}", Uuid::new_v4());
    let (tx, mut rx) = tokio_mpsc::channel(10);
    if let Err(e) = queue.subscribe(&topic, tx).await {
        eprintln!("Kafka subscribe failed, skipping test_kafka_out_of_order_ack: {}", e);
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

#[tokio::test]
async fn test_redis_consumer_crash_redelivery() {
    let mut cfg = build_redis_config();
    cfg.claim_min_idle = Some(10);
    cfg.claim_interval = Some(50);
    cfg.claim_count = Some(10);
    cfg.listener_count = Some(1);
    cfg.shards = Some(1);

    if !redis_available(&cfg).await {
        eprintln!("Redis not available, skipping test_redis_consumer_crash_redelivery");
        return;
    }

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy { max_retries: 1, backoff_ms: 0 };
    let queue = crate::RedisQueue::new(&cfg, 0, &namespace, 10, nack_policy).expect("redis queue");

    let (tx, mut rx) = tokio_mpsc::channel(10);
    queue.subscribe("contract_crash", tx).await.expect("subscribe");

    let mut headers = std::collections::HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());

    let payload = vec![7, 7, 7];
    queue
        .publish_with_headers("contract_crash", None, &payload, &headers)
        .await
        .expect("publish");

    let msg1 = timeout(Duration::from_secs(3), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg1");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let msg2 = timeout(Duration::from_secs(5), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg2");

    assert_eq!(msg2.payload.as_slice(), payload.as_slice());
    assert_eq!(msg2.id, msg1.id, "redelivery should keep the same stream id");

    msg2.ack().await.expect("ack2");
}

#[tokio::test]
async fn test_redis_concurrent_out_of_order_ack() {
    let cfg = build_redis_config();
    if !redis_available(&cfg).await {
        eprintln!("Redis not available, skipping test_redis_concurrent_out_of_order_ack");
        return;
    }

    let namespace = format!("queue_test_{}", Uuid::new_v4());
    let nack_policy = NackPolicy { max_retries: 0, backoff_ms: 0 };
    let queue = crate::RedisQueue::new(&cfg, 0, &namespace, 10, nack_policy).expect("redis queue");

    let (tx, mut rx) = tokio_mpsc::channel(10);
    queue.subscribe("contract_concurrent", tx).await.expect("subscribe");

    let mut headers = std::collections::HashMap::new();
    headers.insert(HEADER_ATTEMPT.to_string(), "0".to_string());

    let payload1 = vec![1, 2, 3];
    let payload2 = vec![4, 5, 6];
    queue
        .publish_with_headers("contract_concurrent", None, &payload1, &headers)
        .await
        .expect("publish1");
    queue
        .publish_with_headers("contract_concurrent", None, &payload2, &headers)
        .await
        .expect("publish2");

    let msg1 = timeout(Duration::from_secs(3), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg1");
    let msg2 = timeout(Duration::from_secs(3), rx.recv())
        .await
        .ok()
        .flatten()
        .expect("msg2");

    let (ack2, ack1) = tokio::join!(
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            msg2.ack().await
        },
        async move { msg1.ack().await }
    );

    assert!(ack1.is_ok());
    assert!(ack2.is_ok());
}

