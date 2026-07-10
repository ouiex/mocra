use crate::sync::{CoordinationBackend, SyncAble, SyncService};
use crate::sync::distributed::SyncOptions;
use async_trait::async_trait;
use rmp_serde as rmps;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TestState {
    value: u32,
}

impl SyncAble for TestState {
    fn topic() -> String {
        "rollback_test".to_string()
    }
}

#[derive(Clone)]
struct TestBackend {
    kv: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    subs: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<Vec<u8>>>>>>,
}

impl TestBackend {
    fn new() -> Self {
        Self {
            kv: Arc::new(Mutex::new(HashMap::new())),
            subs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl CoordinationBackend for TestBackend {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String> {
        let senders = {
            let mut subs = self.subs.lock().unwrap();
            subs.entry(topic.to_string()).or_default().clone()
        };
        for sender in senders {
            let _ = sender.send(payload.to_vec()).await;
        }
        Ok(())
    }

    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        let (tx, rx) = mpsc::channel(16);
        let mut subs = self.subs.lock().unwrap();
        subs.entry(topic.to_string()).or_default().push(tx);
        Ok(rx)
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String> {
        let mut kv = self.kv.lock().unwrap();
        kv.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let kv = self.kv.lock().unwrap();
        Ok(kv.get(key).cloned())
    }

    async fn cas(&self, key: &str, old_val: Option<&[u8]>, new_val: &[u8]) -> Result<bool, String> {
        let mut kv = self.kv.lock().unwrap();
        let existing = kv.get(key).cloned();
        let matches = match (existing.as_deref(), old_val) {
            (None, None) => true,
            (Some(a), Some(b)) => a == b,
            _ => false,
        };
        if matches {
            kv.insert(key.to_string(), new_val.to_vec());
        }
        Ok(matches)
    }

    async fn acquire_lock(&self, _key: &str, _value: &[u8], _ttl_ms: u64) -> Result<bool, String> {
        Ok(true)
    }

    async fn renew_lock(&self, _key: &str, _value: &[u8], _ttl_ms: u64) -> Result<bool, String> {
        Ok(true)
    }
}

#[derive(Serialize)]
struct Envelope<T> {
    version: u64,
    timestamp_ms: u64,
    value: T,
}

#[tokio::test]
async fn test_sync_disallow_rollback_ignores_older_version() {
    let backend = Arc::new(TestBackend::new());
    let namespace = "sync_ns".to_string();
    let options = SyncOptions {
        allow_rollback: false,
        envelope_enabled: true,
    };
    let service = SyncService::new_with_options(Some(backend.clone()), namespace.clone(), options);

    let topic = TestState::topic();
    let kv_key = format!("sync_kv:{}:{}", namespace, topic);
    let stream_topic = format!("sync_stream_{}_{}", namespace, topic);

    let newer = Envelope {
        version: 2,
        timestamp_ms: 2,
        value: TestState { value: 2 },
    };
    let newer_bytes = rmps::to_vec(&newer).expect("encode newer");
    backend.set(&kv_key, &newer_bytes).await.expect("set kv");

    let sync = service.sync::<TestState>().await.expect("sync");
    assert_eq!(sync.get(), Some(TestState { value: 2 }));

    let older = Envelope {
        version: 1,
        timestamp_ms: 1,
        value: TestState { value: 1 },
    };
    let older_bytes = rmps::to_vec(&older).expect("encode older");
    backend.publish(&stream_topic, &older_bytes).await.expect("publish older");

    sleep(Duration::from_millis(200)).await;
    assert_eq!(sync.get(), Some(TestState { value: 2 }));
}

#[tokio::test]
async fn test_sync_ignores_poison_stream_update() {
    let backend = Arc::new(TestBackend::new());
    let namespace = "sync_ns".to_string();
    let options = SyncOptions {
        allow_rollback: false,
        envelope_enabled: true,
    };
    let service = SyncService::new_with_options(Some(backend.clone()), namespace.clone(), options);

    let topic = TestState::topic();
    let kv_key = format!("sync_kv:{}:{}", namespace, topic);
    let stream_topic = format!("sync_stream_{}_{}", namespace, topic);

    let initial = Envelope {
        version: 2,
        timestamp_ms: 2,
        value: TestState { value: 2 },
    };
    let initial_bytes = rmps::to_vec(&initial).expect("encode initial");
    backend.set(&kv_key, &initial_bytes).await.expect("set kv");

    let sync = service.sync::<TestState>().await.expect("sync");
    assert_eq!(sync.get(), Some(TestState { value: 2 }));

    backend
        .publish(&stream_topic, &[0, 1, 2, 3])
        .await
        .expect("publish poison");

    sleep(Duration::from_millis(200)).await;
    assert_eq!(sync.get(), Some(TestState { value: 2 }));
}
