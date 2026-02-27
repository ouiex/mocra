mod modules;
mod middleware;

use tokio::signal;
use std::path::Path;
use std::sync::Arc;
use mocra::common::state::State;
use mocra::engine::engine::Engine;
use mocra::utils::logger;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use std::fs;
use mocra::common::model::message::TaskModel;
use std::sync::atomic::{AtomicU64, Ordering};
use wiremock::{MockServer, Mock, ResponseTemplate};
use wiremock::matchers::{method, path};
use mocra::engine::events::{DownloadEvent, EventEnvelope, EventPhase, EventType};

async fn seed_database(state: &Arc<State>) {
    let db_backend = state.db.get_database_backend();

    // 1. Run Init SQL to reset schema
    let init_filename = if db_backend == DatabaseBackend::Sqlite { "init_sqlite.sql" } else { "init.sql" };
    let init_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(init_filename);

    if let Ok(sql) = fs::read_to_string(&init_path) {
        println!("Initializing database from {}", init_path.display());
        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        for stmt in stmts {
            if let Err(e) = state.db.execute(Statement::from_string(db_backend, stmt.to_string())).await {
                eprintln!("Failed to execute init statement: {} \nError: {}", stmt, e);
            }
        }
    } else {
        eprintln!("Init file not found at {}", init_path.display());
    }

    if db_backend == DatabaseBackend::Postgres {
        let seed_sql = r#"
            INSERT INTO base.module (name, version, priority, enabled)
            VALUES ('mock.dev', 1, 5, true)
            ON CONFLICT (name) DO NOTHING;

            INSERT INTO base.account (name, enabled, priority)
            VALUES ('benchmark', true, 5)
            ON CONFLICT (name) DO NOTHING;

            INSERT INTO base.platform (name, description, enabled)
            VALUES ('mock', 'Mock Platform for Benchmark', true)
            ON CONFLICT (name) DO NOTHING;

            INSERT INTO base.rel_module_account (module_id, account_id, priority, enabled)
            SELECT m.id, a.id, 5, true
            FROM base.module m, base.account a
            WHERE m.name = 'mock.dev' AND a.name = 'benchmark'
            ON CONFLICT DO NOTHING;

            INSERT INTO base.rel_module_platform (module_id, platform_id, priority, enabled)
            SELECT m.id, p.id, 5, true
            FROM base.module m, base.platform p
            WHERE m.name = 'mock.dev' AND p.name = 'mock'
            ON CONFLICT DO NOTHING;

            INSERT INTO base.rel_account_platform (account_id, platform_id, enabled)
            SELECT a.id, p.id, true
            FROM base.account a, base.platform p
            WHERE a.name = 'benchmark' AND p.name = 'mock'
            ON CONFLICT DO NOTHING;
        "#;

        let stmts: Vec<&str> = seed_sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        for stmt in stmts {
            if let Err(e) = state.db.execute(Statement::from_string(db_backend, stmt.to_string())).await {
                eprintln!("Failed to seed mock module data: {}", e);
            }
        }
    }
}

async fn build_engine(config_path: &Path) -> Engine {
    let state = Arc::new(State::new(config_path.to_str().unwrap()).await);
    println!("Config loaded from {}", config_path.display());

    // logger init
    let namespace = state.config.read().await.name.clone();
    let _ = logger::init_app_logger(&namespace).await;

    seed_database(&state).await;

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

    engine
}

#[tokio::main]
async fn main() {
    const TARGET_REQUESTS: u64 = 2000;
    const MAX_WAIT_SECONDS: u64 = 120;
    const IDLE_TIMEOUT_SECONDS: u64 = 10;

    // Start mock server (local URL only)
    let mock_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/test"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
        .mount(&mock_server)
        .await;

    let target_url = format!("{}/test", mock_server.uri());
    unsafe {
        std::env::set_var("MOCK_DEV_URL", &target_url);
        std::env::set_var("MOCK_DEV_COUNT", TARGET_REQUESTS.to_string());
    }

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║      Mocra Prod-Like Benchmark (Full Engine)                ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Target URL: {}                               ║", target_url);
    println!("║  Total Requests: {}                                       ║", TARGET_REQUESTS);
    println!("║  Max Wait: {} seconds                                     ║", MAX_WAIT_SECONDS);
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let _ = tokio::fs::create_dir_all("/home/eason/mocra/mocra-mc/tests/tmp").await;

    let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.prod_like.toml");
    let engine = Arc::new(build_engine(&config_path).await);

    let completed_counter = Arc::new(AtomicU64::new(0));
    let failed_counter = Arc::new(AtomicU64::new(0));

    if let Some(event_bus) = engine.event_bus.clone() {
        let mut rx = event_bus.subscribe("*".to_string()).await;
        let completed_clone = completed_counter.clone();
        let failed_clone = failed_counter.clone();

        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match &event {
                    EventEnvelope {
                        event_type: EventType::Download,
                        phase: EventPhase::Completed,
                        payload,
                        ..
                    } => {
                        if serde_json::from_value::<DownloadEvent>(payload.clone()).is_ok() {
                            let _ = completed_clone.fetch_add(1, Ordering::Relaxed) + 1;
                        }
                    }
                    EventEnvelope {
                        event_type: EventType::Download,
                        phase: EventPhase::Failed,
                        payload,
                        ..
                    } => {
                        if serde_json::from_value::<DownloadEvent>(payload.clone()).is_ok() {
                            let _ = failed_clone.fetch_add(1, Ordering::Relaxed) + 1;
                        }
                    }
                    _ => {}
                }
            }
        });
    } else {
        println!(">>> EventBus disabled; skipping event-based counters");
    }

    let queue_manager = engine.queue_manager.clone();
    let task = TaskModel {
        account: "benchmark".to_string(),
        platform: "mock".to_string(),
        module: Some(vec!["mock.dev".to_string()]),
        priority: Default::default(),
        run_id: uuid::Uuid::now_v7(),
    };
    let sender = queue_manager.get_task_push_channel();
        if let Err(e) = sender.send(mocra::queue::QueuedItem::new(task)).await {
        eprintln!("Failed to enqueue task: {e}");
        return;
    }

    let benchmark_start = std::time::Instant::now();
    let progress_counter = completed_counter.clone();
    let progress_failed = failed_counter.clone();
    let engine_for_shutdown = engine.clone();

    let monitor_handle = tokio::spawn(async move {
        let mut last_count = 0u64;
        let mut last_progress_time = std::time::Instant::now();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));

        loop {
            interval.tick().await;
            let current = progress_counter.load(Ordering::Relaxed);
            let failed = progress_failed.load(Ordering::Relaxed);
            let elapsed = benchmark_start.elapsed().as_secs_f64();

            let delta = current.saturating_sub(last_count);
            let instant_rps = delta as f64 / 2.0;
            let avg_rps = if elapsed > 0.0 { current as f64 / elapsed } else { 0.0 };

            println!(
                "[{:.1}s] Completed: {}/{} | Failed: {} | Instant: {:.1} req/s | Avg: {:.1} req/s",
                elapsed, current, TARGET_REQUESTS, failed, instant_rps, avg_rps
            );

            if delta > 0 {
                last_progress_time = std::time::Instant::now();
            }
            last_count = current;

            let total_processed = current + failed;

            if total_processed >= TARGET_REQUESTS {
                println!("\n>>> All {} requests processed!", TARGET_REQUESTS);
                break;
            }

            if benchmark_start.elapsed().as_secs() >= MAX_WAIT_SECONDS {
                println!("\n>>> Timeout after {} seconds", MAX_WAIT_SECONDS);
                break;
            }

            if total_processed > 0 && last_progress_time.elapsed().as_secs() >= IDLE_TIMEOUT_SECONDS {
                println!("\n>>> No progress for {} seconds, assuming complete", IDLE_TIMEOUT_SECONDS);
                break;
            }
        }

        println!(">>> Shutting down engine...");
        engine_for_shutdown.shutdown().await;
    });

    tokio::select! {
        _ = engine.start() => {
            println!(">>> Engine stopped");
        }
        _ = signal::ctrl_c() => {
            println!("\n>>> Ctrl+C received, shutting down...");
            engine.shutdown().await;
        }
    }

    let _ = monitor_handle.await;

    let elapsed = benchmark_start.elapsed();
    let completed = completed_counter.load(Ordering::Relaxed);
    let failed = failed_counter.load(Ordering::Relaxed);

    println!("\n--- Prod-Like Results ---");
    println!("Target Requests: {}", TARGET_REQUESTS);
    println!("Completed: {}", completed);
    println!("Failed: {}", failed);
    println!("Total Duration: {:.2?}", elapsed);
    if elapsed.as_secs_f64() > 0.0 {
        let throughput = completed as f64 / elapsed.as_secs_f64();
        println!("Throughput: {:.2} req/s", throughput);
    }
}
