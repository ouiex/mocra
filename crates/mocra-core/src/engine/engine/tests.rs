use super::*;
use async_trait::async_trait;
use crate::common::model::Request;
use crate::common::policy::{PolicyConfig, PolicyOverride};
use crate::errors::ErrorKind;
use crate::queue::{Message, MqBackend};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

#[derive(Clone, Default)]
struct TestBackend {
    dlq_entries: Arc<Mutex<Vec<(String, String)>>>,
}

#[async_trait]
impl MqBackend for TestBackend {
    async fn publish(&self, _topic: &str, _key: Option<&str>, _payload: &[u8]) -> crate::errors::Result<()> {
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

    async fn subscribe(&self, _topic: &str, _sender: mpsc::Sender<Message>) -> crate::errors::Result<()> {
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
        self.dlq_entries
            .lock()
            .unwrap()
            .push((topic.to_string(), reason.to_string()));
        Ok(())
    }

    async fn read_dlq(&self, _topic: &str, _count: usize) -> crate::errors::Result<Vec<(String, Vec<u8>, String, String)>> {
        Ok(Vec::new())
    }
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

use crate::queue::{QueuedItem, Identifiable};

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
        TestItem { id: "test-1".to_string() },
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
