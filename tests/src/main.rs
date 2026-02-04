mod modules;
mod middleware;
use tokio::signal;
use std::path::Path;
use std::sync::Arc;
use common::state::State;
use engine::engine::Engine;
use utils::logger;
use utils::logger::{set_log_sender, LogSender};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use std::fs;
use common::model::message::TaskModel;
use common::model::Priority;
use chrono::Utc;
use uuid::Uuid;
use std::sync::atomic::{AtomicU64, Ordering};

async fn seed_database(state: &Arc<State>) {
    let db_backend = state.db.get_database_backend();
    
    // 0. Kill other connections to this DB to release locks (Postgres only)
    if db_backend == DatabaseBackend::Postgres {
        let kill_query = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'crawler' AND pid <> pg_backend_pid()";
        println!("Terminating other connections...");
        let _ = state.db.execute(Statement::from_string(DatabaseBackend::Postgres, kill_query.to_string())).await;
    }

    // 1. Run Init SQL to reset schema
    let init_filename = if db_backend == DatabaseBackend::Sqlite { "init_sqlite.sql" } else { "init.sql" };
    let init_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(init_filename);
    
    if let Ok(sql) = fs::read_to_string(&init_path) {
        println!("Initializing database from {}", init_path.display());
        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        for (i, stmt) in stmts.iter().enumerate() {
             let stmt_trimmed = stmt.trim();
             // println!("Executing init statement {}: {}...", i, stmt_trimmed.split_whitespace().take(3).collect::<Vec<_>>().join(" "));
             if let Err(e) = state.db.execute(Statement::from_string(db_backend, stmt.to_string())).await {
                 eprintln!("Failed to execute init statement: {} \nError: {}", stmt, e);
             }
        }
    } else {
        eprintln!("Init file not found at {}", init_path.display());
    }

    // 2. Run Seed SQL (Skip for SQLite for now unless we create seed_sqlite.sql)
    if db_backend == DatabaseBackend::Postgres {
        let seed_path = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().join("seed_data.sql");
        if let Ok(sql) = fs::read_to_string(&seed_path) {
            println!("Seeding database from {}", seed_path.display());
            let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
            for (i, stmt) in stmts.iter().enumerate() {
                 let stmt_trimmed = stmt.trim();
                 println!("Executing seed statement {}: {}...", i, stmt_trimmed.split_whitespace().take(3).collect::<Vec<_>>().join(" "));
                 if let Err(e) = state.db.execute(Statement::from_string(DatabaseBackend::Postgres, stmt.to_string())).await {
                     eprintln!("Failed to execute seed statement: {} \nError: {}", stmt, e);
                 }
            }
        } else {
            eprintln!("Seed file not found at {}", seed_path.display());
        }
    }
}

async fn build_engine() -> Engine {
    let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.test.toml");
    let state = Arc::new(State::new(config_path.to_str().unwrap()).await);
    println!("Config loaded from {}", config_path.display());
    
    // Clear Redis Queues (Only if Redis is configured)
    let namespace = state.config.read().await.name.clone();
    let has_redis = state.config.read().await.channel_config.redis.is_some();

    if has_redis {
        // 1. Explicitly delete known queue topics using batch delete (optimized)
        // This is crucial for fixing "tag for enum is not valid" errors
        // which arise from stale incompatible binary data.
        let topics = vec![
            "task-high", "task-normal", "task-low",
            "request-high", "request-normal", "request-low",
            "response-high", "response-normal", "response-low",
            "parser-high", "parser-normal", "parser-low",
            "error-high", "error-normal", "error-low",
        ];
        println!("Explicitly clearing queue topics for namespace: {}", namespace);
        
        // Build all keys to delete in one batch
        let mut keys_to_delete: Vec<String> = Vec::with_capacity(topics.len() * 6);
        for topic in &topics {
            keys_to_delete.push(format!("{}:{}", namespace, topic));
            // opportunistic cleanup for shards (0-4) just in case sharding was enabled previously
            for i in 0..5 {
                keys_to_delete.push(format!("{}:{}:{}", namespace, topic, i));
            }
        }
        // Batch delete all topic keys
        let refs: Vec<&str> = keys_to_delete.iter().map(|s| s.as_str()).collect();
        let _ = state.cache_service.del_batch(&refs).await;
    
        // 2. Scan with limit and batch clear remaining keys
        let pattern = format!("{}:*", namespace);
        let cleanup_limit = 2000usize;
        if let Ok(keys) = state.cache_service.keys_with_limit(&pattern, cleanup_limit).await {
            let total_keys = keys.len();
            if total_keys > 0 {
                println!("Found {} additional keys to clear for namespace {}", total_keys, namespace);
                // Batch delete in chunks of 100 for efficiency
                for chunk in keys.chunks(100) {
                    let refs: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
                    let _ = state.cache_service.del_batch(&refs).await;
                }
                println!("Cleared {} Redis keys for namespace {}", total_keys, namespace);
            }
        }
    } else {
        println!("Skipping Redis cleanup (Redis not configured)");
    }

    // logger init
    let namespace = state.config.read().await.name.clone();
    let logging_enabled = logger::init_app_logger(&namespace).await.unwrap_or(false);
    if logging_enabled {
        println!(">>> Logging: ENABLED");
    } else {
        println!(">>> Logging: DISABLED (DISABLE_LOGS or MOCRA_DISABLE_LOGS is set)");
    }


    // Seed DB (Moved after logger init to ensure logs are captured if using logger, though we use println here)
    // Wrap in timeout to prevent hanging on locks
    if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(60), seed_database(&state)).await {
        eprintln!("Database seeding timed out! This usually means a table is locked by another process. Please restart PostgreSQL or kill stuck processes.");
        std::process::exit(1);
    }

    let engine: Engine = Engine::new(Arc::clone(&state), None).await;
    for middleware in middleware::register_data_middlewares(){
        engine.register_data_middleware(middleware).await;
    }
    for middleware in middleware::register_download_middlewares(){
        engine.register_download_middleware(middleware).await;
    }
    for middleware in middleware::register_data_store_middlewares(){
        engine.register_store_middleware(middleware).await;
    }

    for module in modules::register_modules(){
        engine.register_module(module).await;
    }

    if logging_enabled {
        let (log_tx, mut log_rx) = tokio::sync::mpsc::channel(10000);
        let queue_log_sender = engine.queue_manager.get_log_push_channel();
        
        // Spawn adapter task to wrap LogModel into QueuedItem
        tokio::spawn(async move {
            while let Some(log) = log_rx.recv().await {
                let item = queue::QueuedItem::new(log);
                match queue_log_sender.try_send(item) {
                    Ok(_) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        // Drop log silently if queue is full to avoid backpressure on the logger
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        break;
                    }
                }
            }
        });

        let _ = set_log_sender(LogSender {
            sender: log_tx,
            level: "WARN".to_string(),
        });
        println!(">>> Queue Logging ENABLED");
    } else {
        println!(">>> Queue Logging DISABLED");
    }

    engine
}

#[tokio::main]
async fn main() {
    // Configuration for the benchmark
    const TARGET_REQUESTS: u64 = 2000;
    const TARGET_URL: &str = "https://moc.dev";
    const MAX_WAIT_SECONDS: u64 = 120; // 2 minutes max wait
    const IDLE_TIMEOUT_SECONDS: u64 = 15; // If no progress for 15s, assume done
    
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║           Mocra Benchmark - moc.dev Test                     ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Target URL: {}                               ║", TARGET_URL);
    println!("║  Total Requests: {}                                       ║", TARGET_REQUESTS);
    println!("║  Max Wait: {} seconds                                     ║", MAX_WAIT_SECONDS);
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    // Build engine (includes logger init inside library)
    let engine = Arc::new(build_engine().await);
    
    // Counters for statistics
    let completed_counter = Arc::new(AtomicU64::new(0));
    let failed_counter = Arc::new(AtomicU64::new(0));
    let bytes_counter = Arc::new(AtomicU64::new(0));
    let request_generated = Arc::new(AtomicU64::new(0));

    // Trigger initial task with moc.dev module
    let queue_manager = engine.queue_manager.clone();
    let gen_counter = request_generated.clone();
    tokio::spawn(async move {
        println!(">>> Injecting task: moc.dev");

        let task_model = TaskModel {
            account: "benchmark".to_string(),
            platform: "test".to_string(),
            module: Some(vec!["moc.dev".to_string()]),
            priority: Priority::High,
            run_id: Uuid::now_v7(),
        };

        if let Err(e) = queue_manager.get_task_push_channel().send(queue::QueuedItem::new(task_model)).await {
            eprintln!("Failed to inject task: {}", e);
        } else {
            println!(">>> Task moc.dev injected successfully");
            // Mark that we expect TARGET_REQUESTS
            gen_counter.store(TARGET_REQUESTS, Ordering::Relaxed);
        }
    });

    // Subscribe to events for statistics  
    let mut rx = engine.event_bus.subscribe("*".to_string()).await;
    let completed_clone = completed_counter.clone();
    let failed_clone = failed_counter.clone();
    let bytes_clone = bytes_counter.clone();
    
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            match &event {
                engine::events::SystemEvent::Download(engine::events::EventDownload::DownloadCompleted(info)) => {
                    let count = completed_clone.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(size) = info.response_size {
                        bytes_clone.fetch_add(size as u64, Ordering::Relaxed);
                    }
                    // Log first few and milestone completions
                    if count <= 5 || count % 500 == 0 {
                        println!("  [EVENT] Download completed #{}: {} ({})", count, info.url, info.status_code.unwrap_or(0));
                    }
                }
                engine::events::SystemEvent::Download(engine::events::EventDownload::DownloadFailed(info)) => {
                    let count = failed_clone.fetch_add(1, Ordering::Relaxed) + 1;
                    println!("  [EVENT] Download FAILED #{}: {}", count, info.data.url);
                }
                _ => {}
            }
        }
    });

    let benchmark_start = std::time::Instant::now();

    // Progress reporter and completion checker combined
    let progress_counter = completed_counter.clone();
    let progress_failed = failed_counter.clone();
    let progress_bytes = bytes_counter.clone();
    let engine_for_shutdown = engine.clone();
    
    let monitor_handle = tokio::spawn(async move {
        let mut last_count = 0u64;
        let mut last_progress_time = std::time::Instant::now();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        
        loop {
            interval.tick().await;
            let current = progress_counter.load(Ordering::Relaxed);
            let failed = progress_failed.load(Ordering::Relaxed);
            let bytes = progress_bytes.load(Ordering::Relaxed);
            let elapsed = benchmark_start.elapsed().as_secs_f64();
            
            let delta = current.saturating_sub(last_count);
            let instant_rps = delta as f64 / 2.0;
            let avg_rps = if elapsed > 0.0 { current as f64 / elapsed } else { 0.0 };
            let mb_downloaded = bytes as f64 / (1024.0 * 1024.0);
            
            println!(
                "[{:>6.1}s] Completed: {:>5}/{} | Failed: {:>3} | Instant: {:>6.1} req/s | Avg: {:>6.1} req/s | Data: {:>6.2} MB",
                elapsed, current, TARGET_REQUESTS, failed, instant_rps, avg_rps, mb_downloaded
            );
            
            // Check if we made progress
            if delta > 0 {
                last_progress_time = std::time::Instant::now();
            }
            last_count = current;
            
            let total_processed = current + failed;
            
            // Condition 1: All requests processed
            if total_processed >= TARGET_REQUESTS {
                println!("\n>>> All {} requests processed!", TARGET_REQUESTS);
                break;
            }
            
            // Condition 2: Absolute timeout
            if benchmark_start.elapsed().as_secs() >= MAX_WAIT_SECONDS {
                println!("\n>>> Timeout after {} seconds", MAX_WAIT_SECONDS);
                break;
            }
            
            // Condition 3: Idle timeout (no progress for a while AND we have some results)
            if total_processed > 0 && last_progress_time.elapsed().as_secs() >= IDLE_TIMEOUT_SECONDS {
                println!("\n>>> No progress for {} seconds, assuming complete", IDLE_TIMEOUT_SECONDS);
                break;
            }
        }
        
        // Shutdown engine
        println!(">>> Shutting down engine...");
        engine_for_shutdown.shutdown().await;
    });

    // Run engine until shutdown or Ctrl+C
    tokio::select! {
        _ = engine.start() => {
            println!(">>> Engine stopped");
        }
        _ = signal::ctrl_c() => {
            println!("\n>>> Ctrl+C received, shutting down...");
            engine.shutdown().await;
        }
    }
    
    // Wait for monitor to finish
    let _ = monitor_handle.await;
    
    // Final statistics
    let elapsed = benchmark_start.elapsed();
    let total_completed = completed_counter.load(Ordering::Relaxed);
    let total_failed = failed_counter.load(Ordering::Relaxed);
    let total_bytes = bytes_counter.load(Ordering::Relaxed);
    let total_processed = total_completed + total_failed;
    
    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    BENCHMARK RESULTS                         ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Target URL:        {}                          ║", TARGET_URL);
    println!("║  Total Duration:    {:>10.2}s                           ║", elapsed.as_secs_f64());
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Target Requests:   {:>10}                            ║", TARGET_REQUESTS);
    println!("║  Completed:         {:>10}                            ║", total_completed);
    println!("║  Failed:            {:>10}                            ║", total_failed);
    println!("║  Total Processed:   {:>10}                            ║", total_processed);
    println!("║  Success Rate:      {:>9.2}%                            ║", 
        if total_processed > 0 { total_completed as f64 / total_processed as f64 * 100.0 } else { 0.0 });
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Average RPS:       {:>10.2} req/s                      ║", 
        if elapsed.as_secs_f64() > 0.0 { total_completed as f64 / elapsed.as_secs_f64() } else { 0.0 });
    println!("║  Data Downloaded:   {:>10.2} MB                         ║", 
        total_bytes as f64 / (1024.0 * 1024.0));
    println!("║  Throughput:        {:>10.2} MB/s                       ║", 
        if elapsed.as_secs_f64() > 0.0 { total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64() } else { 0.0 });
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("统计时间（UTC）: {}", Utc::now().to_rfc3339());
}
