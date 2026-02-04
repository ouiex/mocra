// Deprecated: benchmark_redis entry removed; file retained for reference only.
use common::model::config::RedisConfig;
use queue::redis::RedisQueue;
use queue::{MqBackend, Message, AckAction, QueueManager, QueuedItem};
use common::model::meta::MetaData;
use common::model::{Response, ExecutionMark, Priority};
use common::interface::storage::BlobStorage;
use utils::storage::FileSystemBlobStorage;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use std::collections::HashMap;
use async_trait::async_trait;
use log::{info, error};
use uuid::Uuid;
use utils::logger;

struct MockBackend {
    sender: mpsc::Sender<Message>,
    simulated_delay: Duration,
    simulate_bandwidth_limit: bool, // If true, delay proportional to size
}

impl MockBackend {
    fn new(sender: mpsc::Sender<Message>, simulated_delay_ms: u64, limit_bandwidth: bool) -> Self {
        Self {
            sender,
            simulated_delay: Duration::from_millis(simulated_delay_ms),
            simulate_bandwidth_limit: limit_bandwidth,
        }
    }
}

#[async_trait]
impl MqBackend for MockBackend {
    async fn publish_with_headers(&self, topic: &str, _key: Option<&str>, payload: &[u8], headers: &HashMap<String, String>) -> errors::Result<()> {
        let mut delay = self.simulated_delay;
        
        if self.simulate_bandwidth_limit {
             // Simulate 10MB/s bandwidth -> 100KB takes 10ms
             // 1MB takes 100ms
             let nanos = (payload.len() as u64 * 1000) / 10; // Simple calc
             delay += Duration::from_nanos(nanos);
        }

        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        
        let (ack_tx, _) = mpsc::channel(100); 
        
        let msg = Message {
            payload: Arc::new(payload.to_vec()),
            id: uuid::Uuid::new_v4().to_string(),
            headers: Arc::new(headers.clone()),
            ack_tx,
        };
        
        let _ = self.sender.send(msg).await;
        Ok(())
    }

    async fn subscribe(&self, _topic: &str, _sender: mpsc::Sender<Message>) -> errors::Result<()> {
        Ok(())
    }

    async fn clean_storage(&self) -> errors::Result<()> { Ok(()) }
    async fn send_to_dlq(&self, _topic: &str, _id: &str, _payload: &[u8], _reason: &str) -> errors::Result<()> { Ok(()) }
    async fn read_dlq(&self, _topic: &str, _count: usize) -> errors::Result<Vec<(String, Vec<u8>, String, String)>> { Ok(vec![]) }
}

#[tokio::main]
async fn main() {
    let _ = logger::init_app_logger("benchmark_redis").await;
    
    // Config
    let redis_host = std::env::var("REDIS_HOST").unwrap_or_else(|_| "localhost".to_string());
    
    let batch_size = 50;
    let total_items = 100;
    let small_payload_size = 1024; // 1KB
    let large_payload_size = 100 * 1024; // 100KB

    // 1. Benchmark Raw Queue (Mock or Redis) - Small Payload
    println!("\n--- Benchmark 1: Raw Queue (Small Payload 1KB) ---");
    let backend = create_backend(&redis_host);
    run_benchmark(backend.clone(), batch_size, total_items, small_payload_size).await;

    // 2. Benchmark Raw Queue (Mock or Redis) - Large Payload
    // This demonstrates the bottleneck
    println!("\n--- Benchmark 2: Raw Queue (Large Payload 100KB) ---");
    let backend_large = create_backend(&redis_host);
    run_benchmark(backend_large.clone(), batch_size, total_items, large_payload_size).await;

    // 3. Benchmark QueueManager with Hybrid Storage - Large Payload
    println!("\n--- Benchmark 3: Hybrid Storage (Large Payload 100KB) ---");
    let temp_dir = std::env::temp_dir().join("mocra_bench_blobs");
    let _ = tokio::fs::remove_dir_all(&temp_dir).await; // Clean start
    
    let storage = Arc::new(FileSystemBlobStorage::new(&temp_dir));
    // Create once to avoid concurrent create_dir_all races in benchmark
    let _ = tokio::fs::create_dir_all(&temp_dir).await;
    let backend_hybrid = create_backend_with_namespace(&redis_host, "bench_hybrid");
    
    let mut manager = QueueManager::new(Some(backend_hybrid), 1000);
    manager.with_blob_storage(storage);
    manager.subscribe(); // Starts forwarding loops
    let manager = Arc::new(manager);

    run_hybrid_benchmark(manager, batch_size, total_items, large_payload_size).await;
    
    let _ = tokio::fs::remove_dir_all(&temp_dir).await; // Cleanup
}

fn create_backend(redis_host: &str) -> Arc<dyn MqBackend> {
    create_backend_with_namespace(redis_host, "bench")
}

fn create_backend_with_namespace(redis_host: &str, namespace: &str) -> Arc<dyn MqBackend> {
    if !redis_host.is_empty() {
        let config = RedisConfig {
            redis_host: redis_host.to_string(),
            redis_port: 6379,
            redis_db: 0,
            redis_username: None,
            redis_password: None,
            pool_size: Some(50),
            shards: Some(8),
            tls: None,
            claim_min_idle: None,
            claim_count: None,
            claim_interval: None,
            listener_count: Some(8),
        };
        Arc::new(RedisQueue::new(&config, 0, namespace, 500).expect("Redis failed"))
    } else {
        // Use MockBackendWithRouter for raw benchmark compatibility
        Arc::new(MockBackendWithRouter::new())
    }
}

// ... MockBackendWithRouter and impl MqBackend for it (same as before) ...
struct MockBackendWithRouter {
    subscribers: std::sync::RwLock<HashMap<String, mpsc::Sender<Message>>>,
}

impl MockBackendWithRouter {
    fn new() -> Self {
        Self {
            subscribers: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl MqBackend for MockBackendWithRouter {
    async fn publish_with_headers(&self, topic: &str, _key: Option<&str>, payload: &[u8], headers: &HashMap<String, String>) -> errors::Result<()> {
        // Simulate 1ms latency + 10MB/s bandwidth
        let bandwidth_delay_micros = (payload.len() as u64 * 1000) / 10240; // 10MB/s = 10KB/ms
        tokio::time::sleep(Duration::from_millis(1) + Duration::from_micros(bandwidth_delay_micros)).await;

        let sender = {
            let subscribers = self.subscribers.read().unwrap();
            subscribers.get(topic).cloned()
        };

        if let Some(sender) = sender {
             let (ack_tx, mut ack_rx) = mpsc::channel(10000);
             tokio::spawn(async move {
                 while let Some(_) = ack_rx.recv().await {}
             });

             let msg = Message {
                payload: Arc::new(payload.to_vec()),
                id: uuid::Uuid::new_v4().to_string(),
                headers: Arc::new(headers.clone()),
                ack_tx,
            };
            let _ = sender.send(msg).await;
        }
        Ok(())
    }

    async fn subscribe(&self, topic: &str, sender: mpsc::Sender<Message>) -> errors::Result<()> {
        let mut subs = self.subscribers.write().unwrap();
        subs.insert(topic.to_string(), sender);
        Ok(())
    }

    async fn clean_storage(&self) -> errors::Result<()> { Ok(()) }
    async fn send_to_dlq(&self, _topic: &str, _id: &str, _payload: &[u8], _reason: &str) -> errors::Result<()> { Ok(()) }
    async fn read_dlq(&self, _topic: &str, _count: usize) -> errors::Result<Vec<(String, Vec<u8>, String, String)>> { Ok(vec![]) }
}

async fn run_benchmark(queue: Arc<dyn MqBackend>, batch_size: usize, total_items: usize, payload_size: usize) {
    let topic = format!("test_topic_raw_{}", Uuid::new_v4());
    let (tx, mut rx) = mpsc::channel(10000);
    queue.subscribe(&topic, tx).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish
    let start_publish = Instant::now();
    let payload = vec![0u8; payload_size];
    let mut batch = Vec::with_capacity(batch_size);
    
    for _ in 0..total_items {
        batch.push((None, payload.clone()));
        if batch.len() >= batch_size {
            queue.publish_batch(&topic, &batch).await.unwrap();
            batch.clear();
        }
    }
    if !batch.is_empty() { queue.publish_batch(&topic, &batch).await.unwrap(); }
    
    let publish_duration = start_publish.elapsed();
    println!("  Publish: {} items in {:.3}s ({:.1}/s)", total_items, publish_duration.as_secs_f64(), total_items as f64 / publish_duration.as_secs_f64());

    // Consume
    let start_consume = Instant::now();
    let mut received = 0;
    let timeout = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout);
    
    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                received += 1;
                msg.ack().await.ok();
                if received >= total_items { break; }
            }
            _ = &mut timeout => { println!("  Timeout! Received {}/{}", received, total_items); break; }
        }
    }
    let consume_duration = start_consume.elapsed();
    println!("  Consume: {} items in {:.3}s ({:.1}/s)", received, consume_duration.as_secs_f64(), received as f64 / consume_duration.as_secs_f64());
}

async fn run_hybrid_benchmark(manager: Arc<QueueManager>, batch_size: usize, total_items: usize, payload_size: usize) {
    let topic_base = "response"; 
    // Note: QueueManager handles "response" -> "response-normal" etc.
    // We subscribe to receive from remote. QueueManager.subscribe() sets up the listeners.
    // But we need to tap into the *output* of QueueManager.
    // QueueManager puts received items into `channel.remote_response_receiver`.
    
    let receiver_lock = manager.get_response_pop_channel();
    
    // Simulate remote publishing: We push to `manager.get_response_push_channel()` 
    // which goes to `channel.response_sender` -> `spawn_forwarder` -> `backend`.
    // Then `backend` (Mock) routes to `subscribe_all_priorities` -> `Batcher` -> `process_batch_messages` -> `remote_response_receiver`.
    
    let publisher = manager.get_response_push_channel();

    let start_publish = Instant::now();
    let payload = vec![0u8; payload_size];
    
    let response_template = Response {
        id: Uuid::new_v4(),
        platform: "bench".to_string(),
        account: "bench".to_string(),
        module: "bench".to_string(),
        status_code: 200,
        cookies: Default::default(),
        content: payload,
        storage_path: None,
        headers: vec![],
        task_retry_times: 0,
        metadata: MetaData::default(),
        download_middleware: vec![],
        data_middleware: vec![],
        task_finished: true,
        context: ExecutionMark::default(),
        run_id: Uuid::new_v4(),
        prefix_request: Uuid::new_v4(),
        request_hash: None,
        priority: Priority::Normal,
    };

    for _ in 0..total_items {
        let mut item = QueuedItem::new(response_template.clone());
        item.inner.id = Uuid::new_v4(); // Unique ID
        publisher.send(item).await.unwrap();
    }
    
    let publish_duration = start_publish.elapsed();
    println!("  Hybrid Publish (Pipeline): {} items in {:.3}s ({:.1}/s)", total_items, publish_duration.as_secs_f64(), total_items as f64 / publish_duration.as_secs_f64());

    // Consume from the receiver
    let start_consume = Instant::now();
    let mut received = 0;
    let timeout = tokio::time::sleep(Duration::from_secs(10)); // longer timeout for FS ops
    tokio::pin!(timeout);
    
    loop {
        tokio::select! {
            // lock collision might be high here, but it's a test
            // We need to poll the mutex protected receiver.
            // Best way is to loop and try_lock or just lock.
             _ = tokio::time::sleep(Duration::from_micros(10)) => {
                 let mut rx = receiver_lock.lock().await;
                 // Drain as many as possible
                 while let Ok(item) = rx.try_recv() {
                     received += 1;
                     // Verify content is present (reloaded)
                     if item.content.len() != payload_size {
                         println!("    Error: Item {} content len {} != {}", received, item.content.len(), payload_size);
                     }
                 }
                 if received >= total_items { break; }
             }
            _ = &mut timeout => { println!("  Timeout! Received {}/{}", received, total_items); break; }
        }
    }
    let consume_duration = start_consume.elapsed();
    println!("  Hybrid Consume: {} items in {:.3}s ({:.1}/s)", received, consume_duration.as_secs_f64(), received as f64 / consume_duration.as_secs_f64());
}

