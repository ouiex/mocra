use super::*;
use crate::common::model::{DeadLetterEnvelope, Request, RequestDispatchEnvelope, Response, ResponseDispatchEnvelope};
use crate::common::policy::{PolicyConfig, PolicyOverride};
use crate::engine::task::request_response_adapter::{build_request_dispatch, build_response_dispatch, decode_request_dispatch, decode_response_dispatch};
use crate::errors::ErrorKind;
use crate::queue::codec::queue_codec;
use crate::queue::{DlqRecord, Message, MqBackend};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Clone, Default)]
struct TestBackend {
    dlq_entries: Arc<Mutex<Vec<(String, Vec<u8>, String)>>>,
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
        payload: &[u8],
        reason: &str,
    ) -> crate::errors::Result<()> {
        self.dlq_entries
            .lock()
            .unwrap()
            .push((topic.to_string(), payload.to_vec(), reason.to_string()));
        Ok(())
    }

    async fn read_dlq(
        &self,
        _topic: &str,
        _count: usize,
    ) -> crate::errors::Result<Vec<DlqRecord>> {
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

#[tokio::test]
async fn retryable_failure_policy_preserves_request_dispatch_payload_in_dlq() {
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
    let dispatch = build_request_dispatch(&request, "origin").expect("request dispatch should build");
    let retry_policy = RetryPolicy::default().with_reason("unit test".to_string());

    Engine::handle_policy_retry(
        &policy_resolver,
        &queue_manager,
        "request",
        "download",
        &dispatch,
        &retry_policy,
        &mut None,
        &mut None,
    )
    .await;

    let dlq_entries = backend.dlq_entries.lock().unwrap();
    assert_eq!(dlq_entries.len(), 1);

    let envelope: DeadLetterEnvelope = queue_codec()
        .decode(&dlq_entries[0].1)
        .expect("dlq payload should decode as envelope");
    let replay_dispatch: RequestDispatchEnvelope = queue_codec()
        .decode(&envelope.payload.payload)
        .expect("request dlq payload should remain a request dispatch envelope");
    let replay_request =
        decode_request_dispatch(replay_dispatch).expect("request dispatch should decode");
    assert_eq!(replay_request.get_id(), request.get_id());
}

#[tokio::test]
async fn retryable_failure_policy_preserves_response_dispatch_payload_in_dlq() {
    let backend = Arc::new(TestBackend::default());
    let queue_manager = QueueManager::new(Some(backend.clone()), 10);

    let policy_cfg = PolicyConfig {
        overrides: vec![PolicyOverride {
            domain: Some("engine".to_string()),
            event_type: Some("parser".to_string()),
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
    let response = Response {
        id: Uuid::new_v4(),
        platform: "".to_string(),
        account: "".to_string(),
        module: "".to_string(),
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
        context: Default::default(),
        run_id: Uuid::new_v4(),
        prefix_request: Uuid::nil(),
        request_hash: None,
        priority: Default::default(),
    };
    let dispatch = build_response_dispatch(&response, "download-pool")
        .expect("response dispatch should build");
    let retry_policy = RetryPolicy::default().with_reason("unit test".to_string());

    Engine::handle_policy_retry(
        &policy_resolver,
        &queue_manager,
        "response",
        "parser",
        &dispatch,
        &retry_policy,
        &mut None,
        &mut None,
    )
    .await;

    let dlq_entries = backend.dlq_entries.lock().unwrap();
    assert_eq!(dlq_entries.len(), 1);

    let envelope: DeadLetterEnvelope = queue_codec()
        .decode(&dlq_entries[0].1)
        .expect("dlq payload should decode as envelope");
    let replay_dispatch: ResponseDispatchEnvelope = queue_codec()
        .decode(&envelope.payload.payload)
        .expect("response dlq payload should remain a response dispatch envelope");
    let replay_response =
        decode_response_dispatch(replay_dispatch).expect("response dispatch should decode");
    assert_eq!(replay_response.get_id(), response.get_id());
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

#[test]
fn apply_pause_state_update_only_sends_on_change() {
    let (pause_tx, pause_rx) = watch::channel(false);

    assert!(Engine::apply_pause_state_update(&pause_tx, true));
    assert!(*pause_rx.borrow());

    assert!(!Engine::apply_pause_state_update(&pause_tx, true));
    assert!(Engine::apply_pause_state_update(&pause_tx, false));
    assert!(!*pause_rx.borrow());
}
