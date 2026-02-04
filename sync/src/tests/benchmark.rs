use sync::{self, RedisBackend, SyncAble};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BenchState {
    counter: u32,
}

impl SyncAble for BenchState {
    fn topic() -> String {
        "bench_state".to_string()
    }
}

#[tokio::main]
async fn main() {
    const REDIS_URL: &str = "redis://:Qaz.123456@localhost:6379";

    let backend = Arc::new(RedisBackend::new(REDIS_URL));

    const CONCURRENCY: usize = 50;
    const UPDATES_PER_TASK: usize = 50;
    const TOTAL_UPDATES: usize = CONCURRENCY * UPDATES_PER_TASK;

    println!("Starting benchmark with {} tasks, {} updates each (Total: {})", CONCURRENCY, UPDATES_PER_TASK, TOTAL_UPDATES);

    // 1. Reset State
    {
        let kafka_backend = sync::KafkaBackend::new("localhost:9095", backend.clone());
        let sync_service = sync::SyncService::new(Some(std::sync::Arc::new(kafka_backend)), "bench".to_string());
        let initial_state = BenchState { counter: 0 };
        sync_service.send(&initial_state).await.unwrap();
        println!("State reset to 0");
    }

    let start_time = Instant::now();
    let mut handles = vec![];
    // 2. Spawn Tasks
    for _i in 0..CONCURRENCY {
        let backend = backend.clone();
        let handle = tokio::spawn(async move {
            let kafka_backend = sync::KafkaBackend::new("localhost:9095", backend);
            let sync_service = sync::SyncService::new(Some(Arc::new(kafka_backend)), "bench".to_string());
            
            for _ in 0..UPDATES_PER_TASK {
                let _ = sync_service.optimistic_update::<BenchState, _>(|state| {
                    state.counter += 1;
                }).await.unwrap();
            }
            // println!("Task {} finished", i);
        });
        handles.push(handle);
    }

    // 3. Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start_time.elapsed();
    println!("Benchmark finished in {:?}", duration);
    println!("Throughput: {:.2} updates/sec", TOTAL_UPDATES as f64 / duration.as_secs_f64());

    // 4. Verify Result
    {
        let kafka_backend = sync::KafkaBackend::new("localhost:9095", backend.clone());
        let sync_service = sync::SyncService::new(Some(std::sync::Arc::new(kafka_backend)), "bench".to_string());
        let final_state = sync_service.fetch_latest::<BenchState>().await.unwrap().unwrap();
        println!("Final State: {:?}", final_state);
        assert_eq!(final_state.counter, TOTAL_UPDATES as u32, "Counter mismatch!");
        println!("Verification Successful!");
    }
}
