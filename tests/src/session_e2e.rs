use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::Router;
use mocra::cacheable::CacheService;
use mocra::common::model::config::Config;
use mocra::common::model::download_config::DownloadConfig;
use mocra::common::model::Request;
use mocra::downloader::request_downloader::RequestDownloader;
use mocra::downloader::Downloader;
use mocra::utils::connector::create_redis_pool;
use mocra::utils::distributed_rate_limit::{DistributedSlidingWindowRateLimiter, RateLimitConfig};
use mocra::utils::redis_lock::DistributedLockManager;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

fn main() {}

async fn login_handler() -> (StatusCode, HeaderMap, &'static str) {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::SET_COOKIE,
        "sid=valid-token; Domain=127.0.0.1; Path=/; HttpOnly"
            .parse()
            .expect("valid set-cookie"),
    );
    (StatusCode::OK, headers, "login-ok")
}

async fn data_handler(headers: HeaderMap) -> (StatusCode, &'static str) {
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if cookie_header.contains("sid=valid-token") {
        (StatusCode::OK, "secret-data")
    } else {
        (StatusCode::UNAUTHORIZED, "missing-cookie")
    }
}

async fn start_session_test_server() -> (SocketAddr, oneshot::Sender<()>) {
    let app = Router::new()
        .route("/login", get(login_handler))
        .route("/data", get(data_handler));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind axum listener");
    let addr = listener.local_addr().expect("read local addr");
    let (tx, rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await;
    });

    (addr, tx)
}

fn load_test_config() -> Config {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
    Config::load(path.to_str().expect("config path to str")).expect("load tests config.toml")
}

fn build_cache_service_from_config(config: &Config) -> Option<Arc<CacheService>> {
    let redis = config.cache.redis.as_ref()?;
    let pool = create_redis_pool(
        &redis.redis_host,
        redis.redis_port,
        redis.redis_db,
        &redis.redis_username,
        &redis.redis_password,
        redis.pool_size,
        redis.tls.unwrap_or(false),
    )?;

    Some(Arc::new(CacheService::new(
        Some(pool),
        "tests-session-e2e".to_string(),
        Some(Duration::from_secs(config.cache.ttl)),
        config.cache.compression_threshold,
    )))
}

fn create_downloader(cache_service: Arc<CacheService>) -> Arc<RequestDownloader> {
    let locker = Arc::new(DistributedLockManager::new(None, "tests-session-e2e"));
    let rate_limit_config = RateLimitConfig::new(50.0);
    let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
        None,
        locker.clone(),
        "tests-session-e2e",
        rate_limit_config,
    ));

    Arc::new(RequestDownloader::new(
        limiter,
        locker,
        cache_service,
        64,
        1024 * 1024,
    ))
}

fn make_request(url: String, run_id: Uuid) -> Request {
    let mut req = Request::new(url, "GET");
    req.account = "acc1".to_string();
    req.platform = "p1".to_string();
    req.module = "m1".to_string();
    req.run_id = run_id;
    req
}

#[tokio::test]
async fn enable_session_full_flow_with_axum_login_and_data() {
    let config = load_test_config();
    let Some(cache_service) = build_cache_service_from_config(&config) else {
        eprintln!("skip session e2e test: cache.redis is not configured");
        return;
    };

    if cache_service.ping().await.is_err() {
        eprintln!("skip session e2e test: redis is unavailable");
        return;
    }

    let downloader = create_downloader(cache_service);
    let (addr, shutdown_tx) = start_session_test_server().await;
    let base_url = format!("http://{}", addr);

    // Scenario 1: follow tests/config.toml (enable_session=false) => login cookie is not reused.
    let cfg_disabled = DownloadConfig::load(&None, &config.download_config);
    assert!(
        !cfg_disabled.enable_session,
        "tests/config.toml should default enable_session=false for this test"
    );
    downloader.set_config("acc1-p1-m1", cfg_disabled).await;

    let run_a = Uuid::new_v4();
    let login_req_a = make_request(format!("{}/login", base_url), run_a);
    let login_resp_a = downloader
        .download(login_req_a)
        .await
        .expect("login request should succeed");
    assert_eq!(login_resp_a.status_code, 200);

    let data_req_a = make_request(format!("{}/data", base_url), run_a);
    let data_resp_a = downloader
        .download(data_req_a)
        .await
        .expect("data request should return response");
    assert_eq!(data_resp_a.status_code, 401);

    // Scenario 2: enable_session=true => login cookie is persisted and reused.
    let mut cfg_enabled = DownloadConfig::load(&None, &config.download_config);
    cfg_enabled.enable_session = true;
    downloader.set_config("acc1-p1-m1", cfg_enabled).await;

    let run_b = Uuid::new_v4();
    let login_req_b = make_request(format!("{}/login", base_url), run_b);
    let login_resp_b = downloader
        .download(login_req_b)
        .await
        .expect("login request should succeed");
    assert_eq!(login_resp_b.status_code, 200);

    let data_req_b = make_request(format!("{}/data", base_url), run_b);
    let data_resp_b = downloader
        .download(data_req_b)
        .await
        .expect("data request should return response");
    assert_eq!(data_resp_b.status_code, 200);
    assert_eq!(String::from_utf8_lossy(&data_resp_b.content), "secret-data");

    let _ = shutdown_tx.send(());
}
