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

use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};
use std::fs;
use mocra::common::model::Priority;
use mocra::common::model::Request;
use mocra::common::model::request::RequestMethod;
use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering};
use serde_json::json;
use mocra::cacheable::CacheAble;
use mocra::common::model::ModuleConfig;
use mocra::downloader::Downloader;
use mocra::common::model::Response;
use mocra::common::model::download_config::DownloadConfig;
use mocra::common::interface::{DownloadMiddleware, DownloadMiddlewareHandle};
use mocra::common::model::config::Config;
use mocra::errors::Result;
use semver::Version;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;

async fn seed_database(state: &Arc<State>) {
    let db_backend = state.db.get_database_backend();
    let config_name = state.config.read().await.name.clone();

    // 1. Run Init SQL to reset schema
    let init_filename = if db_backend == DatabaseBackend::Sqlite {
        if config_name.contains("mock_pure") {
            "init_sqlite_pure.sql"
        } else {
            "init_sqlite.sql"
        }
    } else { "init.sql" };
    let init_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(init_filename);

    if let Ok(sql) = fs::read_to_string(&init_path) {
        println!("Initializing database from {}", init_path.display());
        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        if db_backend == DatabaseBackend::Sqlite {
            if let Ok(txn) = state.db.begin().await {
                for stmt in stmts {
                    if let Err(e) = txn
                        .execute(Statement::from_string(db_backend, stmt.to_string()))
                        .await
                    {
                        eprintln!("Failed to execute init statement: {} \nError: {}", stmt, e);
                    }
                }
                let _ = txn.commit().await;
            }
        } else {
            for stmt in stmts {
                if let Err(e) = state.db.execute(Statement::from_string(db_backend, stmt.to_string())).await {
                    eprintln!("Failed to execute init statement: {} \nError: {}", stmt, e);
                }
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

struct CounterDownloadMiddleware {
    counter: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl DownloadMiddleware for CounterDownloadMiddleware {
    fn name(&mut self) -> String {
        "counter_download_middleware".to_string()
    }

    async fn after_response(&mut self, response: Response, _config: &Option<ModuleConfig>) -> Option<Response> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        Some(response)
    }

    fn default_arc() -> DownloadMiddlewareHandle
    where
        Self: Sized,
    {
        Arc::new(tokio::sync::Mutex::new(Box::new(CounterDownloadMiddleware {
            counter: Arc::new(AtomicU64::new(0)),
        })))
    }
}

#[derive(Clone)]
struct MockDownloader;

#[async_trait::async_trait]
impl Downloader for MockDownloader {
    async fn set_config(&self, _id: &str, _config: DownloadConfig) {}
    async fn set_limit(&self, _id: &str, _limit: f32) {}
    fn name(&self) -> String {
        "mock_downloader".to_string()
    }
    fn version(&self) -> Version {
        Version::new(0, 1, 0)
    }
    async fn download(&self, request: Request) -> Result<Response> {
        let request_hash = request.hash();
        Ok(Response {
            id: request.id,
            platform: request.platform,
            account: request.account,
            module: request.module,
            status_code: 200,
            cookies: Default::default(),
            content: b"OK".to_vec(),
            storage_path: None,
            headers: vec![("Content-Type".to_string(), "text/plain".to_string())],
            task_retry_times: request.task_retry_times,
            metadata: request.meta,
            download_middleware: request.download_middleware,
            data_middleware: request.data_middleware,
            task_finished: request.task_finished,
            context: request.context,
            run_id: request.run_id,
            prefix_request: request.prefix_request,
            request_hash: Some(request_hash),
            priority: request.priority,
        })
    }
    async fn health_check(&self) -> Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    const TARGET_REQUESTS: u64 = 2000;
    const MAX_WAIT_SECONDS: u64 = 120;
    const IDLE_TIMEOUT_SECONDS: u64 = 10;

    let target_url = "mock://local/test".to_string();

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║        Mocra Pure Mock Benchmark (No I/O)                   ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Target URL: {}                               ║", target_url);
    println!("║  Total Requests: {}                                       ║", TARGET_REQUESTS);
    println!("║  Max Wait: {} seconds                                     ║", MAX_WAIT_SECONDS);
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let _ = tokio::fs::create_dir_all("/home/eason/mocra/mocra-mc/tests/tmp").await;
    let _ = tokio::fs::create_dir_all("/home/eason/mocra/mocra-mc/tmp/blob_storage_mock").await;

    let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.mock.pure.toml");
    let config = Config::load(config_path.to_str().unwrap()).expect("Failed to load config");
    let use_pure = config.name.contains("mock_pure");

    if use_pure {
        let concurrency = config.crawler.task_concurrency.unwrap_or(200);
        let downloader = Arc::new(MockDownloader);
        let counter = Arc::new(AtomicU64::new(0));
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let start = std::time::Instant::now();
        let mut tasks = FuturesUnordered::new();

        for i in 0..TARGET_REQUESTS {
            let url = format!("{}?_t={}&_i={}", target_url, std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(), i);
            let mut request = Request::new(url, RequestMethod::Get);
            request.account = "benchmark".to_string();
            request.platform = "mock".to_string();
            request.module = "mock.dev".to_string();
            request.priority = Priority::High;

            let dl = downloader.clone();
            let sem = semaphore.clone();
            let cnt = counter.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok();
                let _ = dl.download(request).await;
                cnt.fetch_add(1, Ordering::Relaxed);
            }));
        }

        while tasks.next().await.is_some() {}

        let duration = start.elapsed().as_secs_f64();
        let processed = counter.load(Ordering::Relaxed);

        println!("\n--- Pure Mock Results ---");
        println!("Total Requests: {TARGET_REQUESTS}");
        println!("Processed: {processed}");
        println!("Time Taken: {duration:.4} seconds");
        if duration > 0.0 {
            println!("Throughput: {:.2} items/second", processed as f64 / duration);
        }
        return;
    }

    let engine = Arc::new(build_engine(&config_path).await);

    let completed_counter = Arc::new(AtomicU64::new(0));
    let failed_counter = Arc::new(AtomicU64::new(0));

    engine
        .register_download_middleware(Arc::new(tokio::sync::Mutex::new(Box::new(CounterDownloadMiddleware {
            counter: completed_counter.clone(),
        }))))
        .await;

    // Register mock downloader (no network)
    engine
        .downloader_manager
        .register(Box::new(MockDownloader))
        .await;

    // Seed module config in cache to force mock downloader
    let mut module_config = ModuleConfig::default();
    module_config.module_config = json!({"downloader": "mock_downloader"});
    let module_id = "benchmark-mock-mock.dev";
    if let Err(e) = module_config.send(module_id, &engine.state.cache_service).await {
        eprintln!("Failed to seed module config cache: {e}");
    }
    match ModuleConfig::sync(module_id, &engine.state.cache_service).await {
        Ok(Some(_)) => println!("Module config cached for {module_id}"),
        Ok(None) => println!("Module config missing for {module_id}"),
        Err(e) => eprintln!("Module config sync failed: {e}"),
    }

    let queue_manager = engine.queue_manager.clone();
    let target_url_for_requests = target_url.clone();
    let gen_counter = completed_counter.clone();
    tokio::spawn(async move {
        println!(">>> Injecting requests: mock.dev");

        let sender = queue_manager.get_request_push_channel();
        for i in 0..TARGET_REQUESTS {
            let url = format!("{}?_t={}&_i={}", target_url_for_requests, std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(), i);
            let mut request = Request::new(url, RequestMethod::Get);
            request.account = "benchmark".to_string();
            request.platform = "mock".to_string();
            request.module = "mock.dev".to_string();
            request.priority = Priority::High;
            request.download_middleware.push("counter_download_middleware".to_string());

                if let Err(e) = sender.send(mocra::queue::QueuedItem::new(request)).await {
                eprintln!("Failed to inject request: {}", e);
            }
        }

        println!(">>> Requests injected successfully");
        gen_counter.store(0, Ordering::Relaxed);
    });

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
    let total_completed = completed_counter.load(Ordering::Relaxed);
    let total_failed = failed_counter.load(Ordering::Relaxed);
    let total_processed = total_completed + total_failed;

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    MOCK BENCHMARK RESULTS                    ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Target URL:        {}                          ║", target_url);
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
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("统计时间（UTC）: {}", Utc::now().to_rfc3339());
}
