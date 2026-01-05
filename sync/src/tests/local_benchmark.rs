use sync::{self, SyncAble};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BenchState {
    counter: u32,
}

impl SyncAble for BenchState {
    fn topic() -> String {
        "local_bench_state".to_string()
    }
}

#[tokio::main]
async fn main() {
    const CONCURRENCY: usize = 50;
    const UPDATES_PER_TASK: usize = 50;
    const TOTAL_UPDATES: usize = CONCURRENCY * UPDATES_PER_TASK;

    println!("Starting LOCAL benchmark with {} tasks, {} updates each (Total: {})", CONCURRENCY, UPDATES_PER_TASK, TOTAL_UPDATES);

    // Create SyncService in Local Mode (None)
    // Note: SyncService holds the state in `local_store`.
    // We must share the SAME SyncService instance across tasks for them to see the same state.
    let sync_service = sync::SyncService::new(None, "local_bench".to_string());
    
    // 1. Reset/Init State
    let initial_state = BenchState { counter: 0 };
    sync_service.send(&initial_state).await.unwrap();
    println!("State initialized to 0");

    let start_time = Instant::now();
    let mut handles = vec![];

    // 2. Spawn Tasks
    for _ in 0..CONCURRENCY {
        let sync_service = sync_service.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..UPDATES_PER_TASK {
                let _ = sync_service.optimistic_update::<BenchState, _>(|state| {
                    state.counter += 1;
                }).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // 3. Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start_time.elapsed();
    println!("Local Benchmark finished in {:?}", duration);
    println!("Throughput: {:.2} updates/sec", TOTAL_UPDATES as f64 / duration.as_secs_f64());

    // 4. Verify Result
    let final_state = sync_service.fetch_latest::<BenchState>().await.unwrap().unwrap();
    println!("Final State: {:?}", final_state);
    assert_eq!(final_state.counter, TOTAL_UPDATES as u32, "Counter mismatch!");
    println!("Verification Successful!");
}
