mod modules;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Semaphore;

use mocra::common::interface::SyncBoxStream;
use mocra::common::model::{Request, Response, ModuleConfig};
use mocra::common::model::download_config::DownloadConfig;
use mocra::common::model::login_info::LoginInfo;
use mocra::downloader::Downloader;
use mocra::errors::Result;
use semver::Version;
use serde_json::Map;
use futures::stream::{FuturesUnordered, StreamExt};

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
    const CONCURRENCY: usize = 200;

    let target_url = "mock://local/test".to_string();
    unsafe {
        std::env::set_var("MOCK_DEV_URL", &target_url);
        std::env::set_var("MOCK_DEV_COUNT", TARGET_REQUESTS.to_string());
    }

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║      Mocra Pure Mock Full-Chain Benchmark (No I/O)          ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Target URL: {}                               ║", target_url);
    println!("║  Total Requests: {}                                       ║", TARGET_REQUESTS);
    println!("║  Concurrency: {}                                           ║", CONCURRENCY);
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let module = modules::register_modules()
        .into_iter()
        .find(|m| m.name() == "mock.dev")
        .expect("mock.dev module not found");

    let steps = module.add_step().await;
    let downloader = Arc::new(MockDownloader);
    let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
    let counter = Arc::new(AtomicU64::new(0));

    let bench_start = std::time::Instant::now();

    for step in steps {
        let config = Arc::new(ModuleConfig::default());
        let params: Map<String, serde_json::Value> = Map::new();
        let login_info: Option<LoginInfo> = None;

        let stream: SyncBoxStream<'static, Request> = step.generate(config.clone(), params, login_info).await.unwrap();
        let requests: Vec<Request> = stream.collect().await;

        let mut tasks = FuturesUnordered::new();
        for request in requests {
            let dl = downloader.clone();
            let sem = semaphore.clone();
            let cnt = counter.clone();
            let step_clone = step.clone();
            let cfg = config.clone();
            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.ok();
                if let Ok(resp) = dl.download(request).await {
                    let _ = step_clone.parser(resp, Some(cfg.clone())).await;
                }
                cnt.fetch_add(1, Ordering::Relaxed);
            }));
        }

        while tasks.next().await.is_some() {}
    }

    let duration = bench_start.elapsed().as_secs_f64();
    let processed = counter.load(Ordering::Relaxed);

    println!("\n--- Pure Mock Full-Chain Results ---");
    println!("Total Requests: {TARGET_REQUESTS}");
    println!("Processed: {processed}");
    println!("Time Taken: {duration:.4} seconds");
    if duration > 0.0 {
        println!("Throughput: {:.2} items/second", processed as f64 / duration);
    }
}
