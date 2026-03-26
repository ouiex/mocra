use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mocra::cacheable::CacheService;
use mocra::common::model::download_config::DownloadConfig;
use mocra::common::model::Request;
use mocra::common::state::State;
use mocra::downloader::request_downloader::RequestDownloader;
use mocra::downloader::Downloader;
use mocra::engine::engine::Engine;
use mocra::utils::distributed_rate_limit::{DistributedSlidingWindowRateLimiter, RateLimitConfig};
use mocra::utils::redis_lock::DistributedLockManager;
use tokio::signal;
use tokio::time::sleep;
use uuid::Uuid;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn create_downloader(namespace: &str, cache_ttl_secs: u64) -> Arc<RequestDownloader> {
    let cache_service = Arc::new(CacheService::new(
        None,
        namespace.to_string(),
        Some(Duration::from_secs(cache_ttl_secs)),
        None,
    ));

    let locker = Arc::new(DistributedLockManager::new(None, namespace));
    let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
        None,
        locker.clone(),
        namespace,
        RateLimitConfig::new(1000.0),
    ));

    Arc::new(RequestDownloader::new(
        limiter,
        locker,
        cache_service,
        64,
        1024 * 1024,
    ))
}

fn build_request(url: &str, idx: u64) -> Request {
    let mut req = Request::new(format!("{url}?idx={idx}"), "GET");
    req.account = "bench_acc".to_string();
    req.platform = "bench_platform".to_string();
    req.module = "httpbin.benchmark".to_string();
    req.run_id = Uuid::new_v4();
    req
}

#[tokio::main]
async fn main() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    if let Err(e) = std::env::set_current_dir(&workspace_root) {
        eprintln!(
            "failed to switch current dir to workspace root {}: {e}",
            workspace_root.display()
        );
        std::process::exit(1);
    }

    let config_path = env_string(
        "BENCH_CONFIG_PATH",
        &workspace_root
            .join("monitoring")
            .join("local_engine.toml")
            .to_string_lossy(),
    );

    let total_requests = env_u64("BENCH_TOTAL_REQUESTS", 1000);
    let interval_secs = env_u64("BENCH_INTERVAL_SECS", 1);
    let hold_secs = env_u64("BENCH_HOLD_SECS", 300);
    let target_url = env_string("BENCH_TARGET_URL", "https://httpbin.org/get");

    let state = match State::try_new(&config_path).await {
        Ok(state) => Arc::new(state),
        Err(e) => {
            eprintln!("failed to initialize state from {config_path}: {e}");
            std::process::exit(1);
        }
    };

    let engine = Engine::new(Arc::clone(&state), None).await;
    let api_port = {
        let cfg = state.config.read().await;
        cfg.api.as_ref().map(|a| a.port).unwrap_or(8905)
    };
    engine.start_api(api_port).await;

    let cache_ttl = {
        let cfg = state.config.read().await;
        cfg.cache.ttl
    };
    let downloader = create_downloader("tests-httpbin-benchmark", cache_ttl);

    let download_config = {
        let cfg = state.config.read().await;
        DownloadConfig::load(&None, &cfg.download_config)
    };
    downloader
        .set_config("bench_acc-bench_platform-httpbin.benchmark", download_config)
        .await;

    println!("httpbin benchmark started");
    println!("target_url={target_url}");
    println!("total_requests={total_requests}, interval_secs={interval_secs}");
    println!("metrics_url=http://127.0.0.1:{api_port}/metrics");

    let started = Instant::now();
    let mut ok_count: u64 = 0;
    let mut err_count: u64 = 0;

    for i in 1..=total_requests {
        let req = build_request(&target_url, i);
        match downloader.download(req).await {
            Ok(resp) if resp.status_code < 400 => ok_count += 1,
            Ok(_) => err_count += 1,
            Err(_) => err_count += 1,
        }

        if i % 20 == 0 || i == total_requests {
            println!(
                "progress: {i}/{total_requests}, ok={ok_count}, err={err_count}, elapsed={}s",
                started.elapsed().as_secs()
            );
        }

        if i < total_requests {
            sleep(Duration::from_secs(interval_secs)).await;
        }
    }

    println!(
        "benchmark completed: ok={ok_count}, err={err_count}, elapsed={}s",
        started.elapsed().as_secs()
    );
    println!(
        "keep process alive for Prometheus scraping: {}s (Ctrl+C to stop)",
        hold_secs
    );

    tokio::select! {
        _ = sleep(Duration::from_secs(hold_secs)) => {}
        _ = signal::ctrl_c() => {}
    }

    engine.shutdown().await;
}
