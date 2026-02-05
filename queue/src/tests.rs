use crate::{MqBackend, QueueManager, Message, QueuedItem};
use async_trait::async_trait;
use errors::Result;
use tokio::sync::mpsc;
use std::sync::{Arc, Mutex};
use common::model::Request;
use rmp_serde as rmps;

#[derive(Clone)]
struct MockBackend {
    pub messages: Arc<Mutex<Vec<(String, Vec<u8>)>>>,
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
        self.messages.lock().unwrap().push((topic.to_string(), payload.to_vec()));
        Ok(())
    }
    
    async fn publish_with_headers(&self, topic: &str, key: Option<&str>, payload: &[u8], _headers: &std::collections::HashMap<String, String>) -> Result<()> {
        self.publish(topic, key, payload).await
    }
    
    async fn publish_batch_with_headers(&self, topic: &str, items: &[(Option<String>, Vec<u8>, std::collections::HashMap<String, String>)]) -> Result<()> {
        for (key, payload, _headers) in items {
            self.publish(topic, key.as_deref(), payload).await?;
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
    let high_msg = messages.iter().find(|(topic, _)| topic == "request-high");
    assert!(high_msg.is_some(), "Should find message with topic 'request-high'");
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
    let payload_tuple = messages.iter().find(|(t,_)| t == "request-normal").expect("Msg not found");
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

