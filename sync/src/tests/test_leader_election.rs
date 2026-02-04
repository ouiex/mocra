use crate::CoordinationBackend;
use crate::LeaderElector;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tokio::sync::mpsc;

struct MockBackend {
    locks: Arc<Mutex<HashMap<String, (Vec<u8>, u64)>>>,
}

impl MockBackend {
    fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl CoordinationBackend for MockBackend {
    async fn publish(&self, _topic: &str, _payload: &[u8]) -> Result<(), String> { Ok(()) }
    async fn subscribe(&self, _topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String> { 
        let (_, rx) = mpsc::channel(1);
        Ok(rx)
    }
    async fn set(&self, _key: &str, _value: &[u8]) -> Result<(), String> { Ok(()) }
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, String> { Ok(None) }
    async fn cas(&self, _key: &str, _old: Option<&[u8]>, _new: &[u8]) -> Result<bool, String> { Ok(true) }

    async fn acquire_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        let mut locks = self.locks.lock().unwrap();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        
        if let Some((_, exp)) = locks.get(key) {
            if now < *exp {
                return Ok(false);
            }
        }
        locks.insert(key.to_string(), (value.to_vec(), now + ttl_ms));
        Ok(true)
    }

    async fn renew_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        let mut locks = self.locks.lock().unwrap();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        
        if let Some((owner, exp)) = locks.get_mut(key) {
            if owner == value {
                // Only renew if not already expired? Or revive?
                // Redis PEXPIRE usually works if key exists.
                // But strict distributed lock usually requires checking if we still hold it.
                // Here we simplify: if key exists and owner matches, extend.
                *exp = now + ttl_ms;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[tokio::test]
async fn test_leader_election_lifecycle() {
    let backend = Arc::new(MockBackend::new());
    let (elector, mut rx) = LeaderElector::new(Some(backend.clone()), "test_lock".to_string(), 500);
    
    // Spawn leader elector
    let elector_clone = elector.clone();
    tokio::spawn(async move {
        elector_clone.start().await;
    });

    // 1. Should become leader
    // Wait for initial true
    if !*rx.borrow() {
        rx.changed().await.unwrap();
    }
    assert!(*rx.borrow(), "Should become leader initially");
    
    // 2. Simulate another node stealing the lock (e.g. partition causing expiry + takeover)
    {
        let mut locks = backend.locks.lock().unwrap();
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        locks.insert("test_lock".to_string(), (b"other_node".to_vec(), now + 10000));
    }

    // 3. Should lose leadership
    // The renewal happens every ttl/3 = 166ms.
    // Wait enough time for renewal to fail.
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
    
    // Check if current state is false
    assert!(!elector.is_leader(), "Should lose leadership after lock theft");
    
    // 4. Restore lock availability
    {
        let mut locks = backend.locks.lock().unwrap();
        locks.remove("test_lock");
    }
    
    // 5. Should regain leadership
    // Elector sleeps ttl/2 = 250ms when not leader before retrying.
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
    assert!(elector.is_leader(), "Should regain leadership after lock release");
}
