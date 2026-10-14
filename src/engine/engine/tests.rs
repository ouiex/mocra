use super::*;
use crate::common::model::Request;
use crate::common::policy::{PolicyConfig, PolicyOverride};
use crate::errors::ErrorKind;
use crate::queue::{Message, MqBackend};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

#[derive(Clone, Default)]
struct TestBackend {
    dlq_entries: Arc<Mutex<Vec<(String, String)>>>,
    fail_dlq: Arc<AtomicBool>,
}

#[async_trait]
impl MqBackend for TestBackend {
    async fn publish(
        &self,
        _topic: &str,
        _key: Option<&str>,
        _payload: &[u8],
    ) -> crate::errors::Result<()> {
        Ok(())
    }

    async fn publish_with_headers(
        &self,
        _topic: &str,
        _key: Option<&str>,
        _payload: &[u8],
        _headers: &HashMap<String, String>,
    ) -> crate::errors::Result<()> {
        Ok(())
    }

    async fn publish_batch_with_headers(
        &self,
        _topic: &str,
        _items: &[(Option<String>, Vec<u8>, HashMap<String, String>)],
    ) -> crate::errors::Result<()> {
        Ok(())
    }

    async fn subscribe(
        &self,
        _topic: &str,
        _sender: mpsc::Sender<Message>,
    ) -> crate::errors::Result<()> {
        Ok(())
    }

    async fn clean_storage(&self) -> crate::errors::Result<()> {
        Ok(())
    }

    async fn send_to_dlq(
        &self,
        topic: &str,
        _id: &str,
        _payload: &[u8],
        reason: &str,
    ) -> crate::errors::Result<()> {
        if self.fail_dlq.load(Ordering::SeqCst) {
            return Err(crate::errors::Error::new(
                ErrorKind::Queue,
                Some("DLQ unavailable"),
            ));
        }
        self.dlq_entries
            .lock()
            .unwrap()
            .push((topic.to_string(), reason.to_string()));
        Ok(())
    }

    async fn read_dlq(
        &self,
        _topic: &str,
        _count: usize,
    ) -> crate::errors::Result<Vec<(String, Vec<u8>, String, String)>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn dlq_write_failure_keeps_original_unacked_until_recovery() {
    for phase in ["failed", "retry"] {
        let backend = Arc::new(TestBackend::default());
        backend.fail_dlq.store(true, Ordering::SeqCst);
        let queue_manager = QueueManager::new(Some(backend.clone()), 10);
        let policy_cfg = PolicyConfig {
            overrides: vec![PolicyOverride {
                domain: Some("engine".to_string()),
                event_type: Some("download".to_string()),
                phase: Some(phase.to_string()),
                kind: ErrorKind::ProcessorChain,
                retryable: Some(false),
                backoff: None,
                dlq: Some(DlqPolicy::Always),
                alert: None,
                max_retries: Some(0),
                backoff_ms: Some(0),
            }],
        };
        let resolver = PolicyResolver::new(Some(&policy_cfg));
        let request = Request::new("http://example.com", "GET");
        let acked = Arc::new(AtomicBool::new(false));

        for unavailable in [true, false] {
            backend.fail_dlq.store(unavailable, Ordering::SeqCst);
            let ack_flag = acked.clone();
            let mut ack_fn: Option<crate::queue::AckFn> = Some(Box::new(move || {
                Box::pin(async move {
                    ack_flag.store(true, Ordering::SeqCst);
                    Ok(())
                })
            }));
            let mut nack_fn = None;
            if phase == "failed" {
                Engine::handle_policy_failure(
                    &resolver,
                    &queue_manager,
                    "request",
                    "download",
                    &request,
                    &crate::errors::Error::new(ErrorKind::ProcessorChain, Some("test failure")),
                    &mut ack_fn,
                    &mut nack_fn,
                )
                .await;
            } else {
                Engine::handle_policy_retry(
                    &resolver,
                    &queue_manager,
                    "request",
                    "download",
                    &request,
                    &RetryPolicy::default().with_reason("test retry".to_string()),
                    &mut ack_fn,
                    &mut nack_fn,
                )
                .await;
            }
            assert_eq!(acked.load(Ordering::SeqCst), !unavailable, "phase={phase}");
        }
        assert_eq!(backend.dlq_entries.lock().unwrap().len(), 1);
    }
}

#[cfg(any(feature = "queue-nats", feature = "queue-kafka"))]
async fn apply_broker_dlq_policy(queue: &QueueManager, message: Message, request: &Request) {
    let config = PolicyConfig {
        overrides: vec![PolicyOverride {
            domain: Some("engine".into()),
            event_type: Some("download".into()),
            phase: Some("failed".into()),
            kind: ErrorKind::ProcessorChain,
            retryable: Some(false),
            backoff: None,
            dlq: Some(DlqPolicy::Always),
            alert: None,
            max_retries: Some(0),
            backoff_ms: Some(0),
        }],
    };
    let resolver = PolicyResolver::new(Some(&config));
    let ack_message = message.clone();
    let mut ack_fn: Option<crate::queue::AckFn> = Some(Box::new(move || {
        Box::pin(async move { ack_message.ack().await })
    }));
    let mut nack_fn: Option<crate::queue::NackFn> = Some(Box::new(move |reason| {
        Box::pin(async move { message.nack(reason).await })
    }));
    Engine::handle_policy_failure(
        &resolver,
        queue,
        "request-normal",
        "download",
        request,
        &crate::errors::Error::new(ErrorKind::ProcessorChain, Some("test failure")),
        &mut ack_fn,
        &mut nack_fn,
    )
    .await;
}

#[cfg(feature = "queue-nats")]
#[tokio::test]
#[ignore = "requires a local nats-server with JetStream (MOCRA_NATS_URL)"]
async fn nats_dlq_failure_redelivers_original_message() {
    use crate::common::model::config::NatsConfig;
    use crate::queue::{NackPolicy, nats::NatsQueue};

    let url = std::env::var("MOCRA_NATS_URL").expect("MOCRA_NATS_URL must be set");
    let namespace = format!("mocra_dlq_{}", uuid::Uuid::now_v7().simple());
    let config = NatsConfig {
        url: url.clone(),
        username: None,
        password: None,
        token: None,
    };
    let backend = Arc::new(NatsQueue::new(&config, 0, &namespace, NackPolicy::default()).unwrap());
    let (tx, mut rx) = mpsc::channel(16);
    backend.subscribe("request-normal", tx).await.unwrap();
    let request = Request::new("https://example.test", "GET");
    let payload = serde_json::to_vec(&request).unwrap();
    backend
        .publish("request-normal", None, &payload)
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let original_id = message.id.clone();
    let unavailable = Arc::new(TestBackend::default());
    unavailable.fail_dlq.store(true, Ordering::SeqCst);
    apply_broker_dlq_policy(&QueueManager::new(Some(unavailable), 16), message, &request).await;

    // JetStream must redeliver the unacknowledged original after its default AckWait.
    let redelivered = tokio::time::timeout(Duration::from_secs(40), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(redelivered.id, original_id);
    assert_eq!(redelivered.payload.as_slice(), payload.as_slice());
    apply_broker_dlq_policy(&QueueManager::new(Some(backend), 16), redelivered, &request).await;

    let js = async_nats::jetstream::new(async_nats::connect(url).await.unwrap());
    let stream = js.get_stream(format!("{namespace}_stream")).await.unwrap();
    let mut consumer = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>(&format!(
            "{namespace}_request_normal_c"
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while consumer.info().await.unwrap().num_ack_pending != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let dlq = stream.get_raw_message(2).await.unwrap();
    assert_eq!(
        dlq.subject.as_str(),
        format!("{namespace}._dlq.request-normal")
    );
    let stored: Request = rmp_serde::from_slice(&dlq.payload).unwrap();
    assert_eq!(stored.id, request.id);
    assert_eq!(stored.url, request.url);
    js.delete_stream(format!("{namespace}_stream"))
        .await
        .unwrap();
}

#[cfg(feature = "queue-kafka")]
#[tokio::test]
#[ignore = "requires a local Kafka broker (KAFKA_BROKERS)"]
async fn kafka_dlq_failure_keeps_offset_for_redelivery() {
    use crate::common::model::config::KafkaConfig;
    use crate::queue::{NackPolicy, kafka::KafkaQueue};
    use rdkafka::consumer::{BaseConsumer, Consumer};

    let brokers = std::env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS must be set");
    let namespace = format!("mocra_dlq_{}", uuid::Uuid::now_v7().simple());
    let config = KafkaConfig {
        brokers: brokers.clone(),
        username: None,
        password: None,
        tls: None,
    };
    let backend = Arc::new(KafkaQueue::new(&config, 0, &namespace, NackPolicy::default()).unwrap());
    let (tx, mut rx) = mpsc::channel(16);
    backend.subscribe("request-normal", tx).await.unwrap();
    let request = Request::new("https://example.test", "GET");
    let payload = serde_json::to_vec(&request).unwrap();
    backend
        .publish("request-normal", None, &payload)
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let original_id = message.id.clone();
    let unavailable = Arc::new(TestBackend::default());
    unavailable.fail_dlq.store(true, Ordering::SeqCst);
    apply_broker_dlq_policy(&QueueManager::new(Some(unavailable), 16), message, &request).await;

    let observer: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", format!("{namespace}-crawler_group"))
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let topic = format!("{namespace}-request-normal");
    let mut partitions = rdkafka::TopicPartitionList::new();
    partitions.add_partition(&topic, 0);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let committed = observer
        .committed_offsets(partitions.clone(), Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        committed.find_partition(&topic, 0).unwrap().offset(),
        rdkafka::Offset::Invalid
    );

    // Closing the receiver and sending a wakeup lets the original listener leave the group.
    drop(rx);
    backend
        .publish("request-normal", None, b"wakeup")
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    backend.subscribe("request-normal", tx).await.unwrap();
    let redelivered = tokio::time::timeout(Duration::from_secs(20), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(redelivered.id, original_id);
    assert_eq!(redelivered.payload.as_slice(), payload.as_slice());
    apply_broker_dlq_policy(&QueueManager::new(Some(backend), 16), redelivered, &request).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let offsets = observer
                .committed_offsets(partitions.clone(), Duration::from_secs(2))
                .unwrap();
            if offsets.find_partition(&topic, 0).unwrap().offset() == rdkafka::Offset::Offset(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let admin: rdkafka::admin::AdminClient<_> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create::<rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>>()
        .unwrap();
    admin
        .delete_topics(
            &[&topic, &format!("{topic}-dlq")],
            &rdkafka::admin::AdminOptions::new(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn retryable_failure_policy_can_route_to_dlq() {
    let backend = Arc::new(TestBackend::default());
    let queue_manager = QueueManager::new(Some(backend.clone()), 10);

    let policy_cfg = PolicyConfig {
        overrides: vec![PolicyOverride {
            domain: Some("engine".to_string()),
            event_type: Some("download".to_string()),
            phase: Some("retry".to_string()),
            kind: ErrorKind::ProcessorChain,
            retryable: Some(false),
            backoff: None,
            dlq: Some(DlqPolicy::Always),
            alert: None,
            max_retries: Some(0),
            backoff_ms: Some(0),
        }],
    };

    let policy_resolver = PolicyResolver::new(Some(&policy_cfg));
    let request = Request::new("http://example.com", "GET");

    let acked = Arc::new(AtomicBool::new(false));
    let nacked = Arc::new(AtomicBool::new(false));

    let ack_flag = acked.clone();
    let mut ack_fn: Option<crate::queue::AckFn> = Some(Box::new(move || {
        let ack_flag = ack_flag.clone();
        Box::pin(async move {
            ack_flag.store(true, Ordering::SeqCst);
            Ok(())
        })
    }));

    let nack_flag = nacked.clone();
    let mut nack_fn: Option<crate::queue::NackFn> = Some(Box::new(move |_reason| {
        let nack_flag = nack_flag.clone();
        Box::pin(async move {
            nack_flag.store(true, Ordering::SeqCst);
            Ok(())
        })
    }));

    let retry_policy = RetryPolicy::default().with_reason("unit test".to_string());

    Engine::handle_policy_retry(
        &policy_resolver,
        &queue_manager,
        "request",
        "download",
        &request,
        &retry_policy,
        &mut ack_fn,
        &mut nack_fn,
    )
    .await;

    assert!(acked.load(Ordering::SeqCst));
    assert!(!nacked.load(Ordering::SeqCst));

    let dlq_entries = backend.dlq_entries.lock().unwrap();
    assert_eq!(dlq_entries.len(), 1);
    assert_eq!(dlq_entries[0].0, "request");
}

use crate::queue::{Identifiable, QueuedItem};

#[derive(Clone)]
struct TestItem {
    id: String,
}

impl Identifiable for TestItem {
    fn get_id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::test]
async fn test_processor_failure_triggers_nack() {
    let ack_count = Arc::new(Mutex::new(0u32));
    let nack_reason = Arc::new(Mutex::new(None::<String>));

    let ack_count_clone = ack_count.clone();
    let nack_reason_clone = nack_reason.clone();

    let item = QueuedItem::with_ack(
        TestItem {
            id: "test-1".to_string(),
        },
        move || {
            let ack_count_clone = ack_count_clone.clone();
            Box::pin(async move {
                *ack_count_clone.lock().unwrap() += 1;
                Ok(())
            })
        },
        move |reason| {
            let nack_reason_clone = nack_reason_clone.clone();
            Box::pin(async move {
                *nack_reason_clone.lock().unwrap() = Some(reason);
                Ok(())
            })
        },
    );

    let (task, mut ack_fn, mut nack_fn) = item.into_parts();
    let _id = task.get_id();
    let failed = true;

    if !failed {
        if let Some(f) = ack_fn.take() {
            let _ = f().await;
        }
    } else if let Some(f) = nack_fn.take() {
        let _ = f("processor failed".to_string()).await;
    }

    assert_eq!(*ack_count.lock().unwrap(), 0);
    assert_eq!(
        nack_reason.lock().unwrap().as_deref(),
        Some("processor failed")
    );
}
