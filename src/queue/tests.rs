use crate::common::model::config::KafkaConfig;
use crate::common::model::{ExecutionMark, Priority, Request, Response};
use crate::common::policy::{PolicyConfig, PolicyOverride};
use crate::engine::task::request_response_adapter::{
    build_request_dispatch, build_response_dispatch, decode_request_dispatch,
    decode_response_dispatch,
};
use crate::errors::ErrorKind;
use crate::errors::Result;
use crate::queue::compensation::Identifiable;
use crate::queue::kafka::{KafkaQueue, KafkaQueueConfig};
use crate::queue::{
    AckAction, DlqRecord, HEADER_ATTEMPT, HEADER_NACK_REASON, Message, MqBackend, NackDisposition,
    NackPolicy, QueueManager, QueuedItem, decide_nack,
};
use async_trait::async_trait;
use rmp_serde as rmps;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::{Duration, timeout};
use uuid::Uuid;

#[derive(Clone)]
struct MockBackend {
    pub messages: Arc<Mutex<Vec<(String, Vec<u8>, std::collections::HashMap<String, String>)>>>,
    pub subscribed_topics: Arc<Mutex<Vec<String>>>,
}

impl MockBackend {
    fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(Vec::new())),
            subscribed_topics: Arc::new(Mutex::new(Vec::new())),
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
        _key: Option<&str>,
        payload: &[u8],
        headers: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
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

    async fn subscribe(&self, topic: &str, _sender: mpsc::Sender<Message>) -> Result<()> {
        self.subscribed_topics
            .lock()
            .unwrap()
            .push(topic.to_string());
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

    async fn read_dlq(&self, _topic: &str, _count: usize) -> Result<Vec<DlqRecord>> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct DlqOpsMockBackend {
    pub messages: Arc<Mutex<Vec<(String, Vec<u8>, std::collections::HashMap<String, String>)>>>,
    pub dlq_records: Arc<Mutex<std::collections::HashMap<String, DlqRecord>>>,
}

impl DlqOpsMockBackend {
    fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(Vec::new())),
            dlq_records: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }
}

#[async_trait]
impl MqBackend for DlqOpsMockBackend {
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
        _key: Option<&str>,
        payload: &[u8],
        headers: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        self.messages
            .lock()
            .unwrap()
            .push((topic.to_string(), payload.to_vec(), headers.clone()));
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
        id: &str,
        payload: &[u8],
        reason: &str,
    ) -> Result<()> {
        let mut records = self.dlq_records.lock().unwrap();
        let record_id = format!("dlq-{}", records.len() + 1);
        records.insert(
            record_id.clone(),
            DlqRecord {
                id: record_id,
                payload: payload.to_vec(),
                reason: reason.to_string(),
                original_id: id.to_string(),
                attempt: Some(0),
            },
        );
        Ok(())
    }

    async fn read_dlq(&self, _topic: &str, _count: usize) -> Result<Vec<DlqRecord>> {
        Ok(self.dlq_records.lock().unwrap().values().cloned().collect())
    }

    async fn read_dlq_record(&self, _topic: &str, record_id: &str) -> Result<Option<DlqRecord>> {
        Ok(self.dlq_records.lock().unwrap().get(record_id).cloned())
    }

    async fn delete_dlq(&self, _topic: &str, record_id: &str) -> Result<bool> {
        Ok(self.dlq_records.lock().unwrap().remove(record_id).is_some())
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

    let dispatch = build_request_dispatch(&request, "").expect("request dispatch should build");
    tx.send(QueuedItem::new(dispatch))
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
        let dispatch = rmps::from_slice(payload).expect("Failed to deserialize payload");
        let _req = decode_request_dispatch(dispatch).expect("Failed to decode request dispatch");
    }

    // Test High Priority
    use crate::common::model::Priority;
    let mut request_high = Request::new("http://example.com/high", "GET");
    request_high.priority = Priority::High;
    let dispatch_high =
        build_request_dispatch(&request_high, "").expect("high priority dispatch should build");
    tx.send(QueuedItem::new(dispatch_high))
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

#[tokio::test]
async fn test_dlq_record_id_requeue_and_delete() {
    let backend = Arc::new(DlqOpsMockBackend::new());
    let manager = QueueManager::new(Some(backend.clone()), 16);
    let request = Request::new("http://example.com", "GET");
    let dispatch =
        build_request_dispatch(&request, "origin").expect("request dispatch should build");

    manager
        .send_to_dlq("request-normal", &dispatch, "poison")
        .await
        .expect("send to dlq");

    let records = manager
        .read_dlq_records("request-normal", 10)
        .await
        .expect("read dlq records");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_id, "dlq-1");
    assert_eq!(records[0].envelope.source_topic, "request-normal");
    assert_eq!(records[0].envelope.payload.schema_id, "queue.request");

    let requeued = manager
        .requeue_dlq_message("request-normal", "dlq-1")
        .await
        .expect("requeue dlq record");
    assert!(requeued);

    let remaining = manager
        .read_dlq_records("request-normal", 10)
        .await
        .expect("read remaining dlq records");
    assert!(remaining.is_empty());

    let published = backend.messages.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, "request-normal");
    assert_eq!(
        published[0]
            .2
            .get(HEADER_ATTEMPT)
            .map(|value| value.as_str()),
        Some("0")
    );

    let replayed_dispatch =
        rmps::from_slice(&published[0].1).expect("decode replayed request dispatch");
    let replayed: Request = decode_request_dispatch(replayed_dispatch)
        .expect("decode replayed request dispatch payload");
    assert_eq!(replayed.get_id(), request.get_id());
}

#[tokio::test]
async fn test_response_dispatch_dlq_record_id_requeue_and_delete() {
    let backend = Arc::new(DlqOpsMockBackend::new());
    let manager = QueueManager::new(Some(backend.clone()), 16);
    let response = sample_response();
    let dispatch = build_response_dispatch(&response, "download-pool")
        .expect("response dispatch should build");

    manager
        .send_to_dlq("origin::response-normal", &dispatch, "poison")
        .await
        .expect("send response dispatch to dlq");

    let records = manager
        .read_dlq_records("origin::response-normal", 10)
        .await
        .expect("read response dlq records");

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_id, "dlq-1");
    assert_eq!(records[0].envelope.source_topic, "origin::response-normal");
    assert_eq!(records[0].envelope.payload.schema_id, "queue.response");

    let requeued = manager
        .requeue_dlq_message("origin::response-normal", "dlq-1")
        .await
        .expect("requeue response dlq record");
    assert!(requeued);

    let remaining = manager
        .read_dlq_records("origin::response-normal", 10)
        .await
        .expect("read remaining response dlq records");
    assert!(remaining.is_empty());

    let published = backend.messages.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0, "origin::response-normal");
    assert_eq!(
        published[0]
            .2
            .get(HEADER_ATTEMPT)
            .map(|value| value.as_str()),
        Some("0")
    );

    let replayed_dispatch =
        rmps::from_slice(&published[0].1).expect("decode replayed response dispatch");
    let replayed = decode_response_dispatch(replayed_dispatch)
        .expect("decode replayed response dispatch payload");
    assert_eq!(replayed.get_id(), response.get_id());
}

fn sample_response() -> Response {
    Response {
        id: Uuid::new_v4(),
        platform: "platform-a".to_string(),
        account: "account-a".to_string(),
        module: "module-a".to_string(),
        status_code: 200,
        cookies: Default::default(),
        content: b"ok".to_vec(),
        storage_path: None,
        headers: Vec::new(),
        task_retry_times: 0,
        metadata: Default::default(),
        download_middleware: Vec::new(),
        data_middleware: Vec::new(),
        task_finished: false,
        context: ExecutionMark::default(),
        run_id: Uuid::new_v4(),
        prefix_request: Uuid::new_v4(),
        request_hash: None,
        priority: Priority::Normal,
    }
}

#[tokio::test]
async fn cross_namespace_response_uses_explicit_namespace_topic() {
    let backend = Arc::new(MockBackend::new());
    let mut manager = QueueManager::new(Some(backend.clone()), 100);
    manager.namespace = "download-pool".to_string();
    manager.subscribe();

    let mut dispatch = build_response_dispatch(&sample_response(), manager.namespace.clone())
        .expect("response dispatch should build");
    dispatch.routing.namespace = "origin".to_string();

    let namespace_override = manager.response_namespace_override(&dispatch);
    let item = match namespace_override {
        Some(namespace) => QueuedItem::new(dispatch).with_namespace(namespace),
        None => QueuedItem::new(dispatch),
    };

    manager
        .get_response_push_channel()
        .send(item)
        .await
        .expect("send response dispatch");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let messages = backend.messages.lock().unwrap();
    let response_msg = messages
        .iter()
        .find(|(topic, _, _)| topic == "origin::response-normal");
    assert!(
        response_msg.is_some(),
        "should publish to explicit origin topic"
    );
}

#[test]
fn response_fast_path_stays_local_only_for_local_namespace() {
    let backend = Arc::new(MockBackend::new());
    let mut manager = QueueManager::new(Some(backend), 16);
    manager.namespace = "download-pool".to_string();

    let local_dispatch = build_response_dispatch(&sample_response(), manager.namespace.clone())
        .expect("local dispatch should build");
    assert!(manager.should_use_local_response_fast_path(&local_dispatch));

    let mut remote_dispatch =
        build_response_dispatch(&sample_response(), manager.namespace.clone())
            .expect("remote dispatch should build");
    remote_dispatch.routing.namespace = "origin".to_string();
    assert!(!manager.should_use_local_response_fast_path(&remote_dispatch));
}

#[test]
fn queue_manager_resolves_remote_request_namespace_subscriptions() {
    let mut cfg = minimal_config(None);
    cfg.name = "download-pool".to_string();
    cfg.channel_config.federation_request_namespaces = vec![
        "origin-a".to_string(),
        " origin-b ".to_string(),
        "download-pool".to_string(),
        "".to_string(),
        "origin-a".to_string(),
    ];

    let manager = QueueManager::from_config(&cfg);

    assert_eq!(
        manager.request_namespace_subscriptions,
        vec!["origin-a".to_string(), "origin-b".to_string()]
    );
}

#[tokio::test]
async fn federated_download_pool_subscribes_remote_request_topics() {
    let backend = Arc::new(MockBackend::new());
    let mut manager = QueueManager::new(Some(backend.clone()), 100);
    manager.namespace = "download-pool".to_string();
    manager.request_namespace_subscriptions = vec!["origin".to_string()];

    manager.subscribe();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let subscribed_topics = backend.subscribed_topics.lock().unwrap();
    assert!(
        subscribed_topics
            .iter()
            .any(|topic| topic == "request-normal")
    );
    assert!(
        subscribed_topics
            .iter()
            .any(|topic| topic == "origin::request-normal")
    );
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
            backend: None,
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
            scheduler_ingress_cutover_gate: None,
        },
        scheduler: None,
        sync: None,
        channel_config: ChannelConfig {
            blob_storage: None,
            kafka: None,
            minid_time: 0,
            capacity: 10,
            queue_codec: None,
            batch_concurrency: None,
            compression_threshold: None,
            nack_max_retries: Some(0),
            nack_backoff_ms: Some(0),
            federation_request_namespaces: Vec::new(),
            federation_response_cache_api_endpoints: Default::default(),
        },
        proxy: None,
        api: None,
        event_bus: None,
        logger: None,
        policy,
        raft: None,
    }
}

#[test]
fn remote_queue_defaults_to_native_compensator() {
    let backend = Arc::new(MockBackend::new());
    let mut manager = QueueManager::new(Some(backend), 16);
    let cfg = minimal_config(None);
    manager.install_default_compensator(&cfg.channel_config, &cfg.name);

    assert!(manager.compensator.is_some());
}

#[test]
fn local_queue_without_backend_keeps_compensator_disabled() {
    let mut manager = QueueManager::new(None, 16);
    let cfg = minimal_config(None);
    manager.install_default_compensator(&cfg.channel_config, &cfg.name);

    assert!(manager.compensator.is_none());
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
    let dispatch = build_request_dispatch(&request, "").expect("request dispatch should build");
    tx.send(QueuedItem::new(dispatch))
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
    let queue = match KafkaQueue::new(KafkaQueueConfig {
        kafka_config: &kafka_config,
        minid_time: 0,
        namespace: &namespace,
        nack_policy,
    }) {
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
    let queue = match KafkaQueue::new(KafkaQueueConfig {
        kafka_config: &kafka_config,
        minid_time: 0,
        namespace: &namespace,
        nack_policy,
    }) {
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
