use crate::downloader::Downloader;
use crate::errors::{DownloadError, RequestError, Result};
use futures::StreamExt;
use log::{info, warn};
use reqwest::Client;
use reqwest::Method;
use reqwest::Proxy;
use reqwest::header::HeaderMap;
use semver::Version;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH, Instant};
use url::Url;
use crate::utils::distributed_rate_limit::DistributedSlidingWindowRateLimiter;
use crate::utils::redis_lock::DistributedLockManager;
use crate::cacheable::{CacheService,CacheAble};
use crate::common::model::{Cookies, Headers, Request, Response};
use crate::common::model::cookies::CookieItem;
use crate::common::model::download_config::DownloadConfig;
use crate::common::model::headers::HeaderItem;
use dashmap::DashMap;
use metrics::{counter, histogram};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SessionState {
    session_id: String,
    module_id: String,
    headers: Headers,
    cookies: Cookies,
    version: u64,
}

impl CacheAble for SessionState {
    fn field() -> impl AsRef<str> {
        "session_state"
    }
}

/// RequestDownloader implements the Downloader trait and provides HTTP download capability.
/// It supports cookie/header caching, rate limiting, and task locking.
/// The corresponding features can be enabled via `enable_session`, `enable_locker`,
/// and `enable_rate_limit`.
/// This downloader uses `reqwest` for HTTP requests and relies on `Arc`/`Mutex`-style
/// shared state patterns for thread safety.
/// Task locks are implemented with a distributed lock.

#[derive(Clone)]
pub struct RequestDownloader {
    /// Rate limiter.
    pub limit: Arc<DistributedSlidingWindowRateLimiter>,
    /// Distributed lock manager.
    pub locker: Arc<DistributedLockManager>,
    cache_service: Arc<CacheService>,
    enable_session: Arc<AtomicBool>,
    enable_locker: Arc<AtomicBool>,
    enable_rate_limit: Arc<AtomicBool>,
    proxy_clients: Arc<DashMap<String, (Client, Instant)>>,
    default_client: Client,
    pool_size: usize,
    max_response_size: usize,
}

impl RequestDownloader {
    #[inline]
    fn is_session_enabled(&self, request: &Request) -> bool {
        request.enable_session || self.enable_session.load(Ordering::Relaxed)
    }

    #[inline]
    fn session_scope_key(&self, request: &Request) -> String {
        format!("{}:{}", request.module_id(), request.run_id)
    }

    pub fn new(
        limit: Arc<DistributedSlidingWindowRateLimiter>,
        locker: Arc<DistributedLockManager>,
        sync: Arc<CacheService>,
        pool_size: usize,
        max_response_size: usize,
    ) -> Self {
        let default_client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            // Increase pool size to allow high concurrency
            // Default reqwest is usually unlimited? No, check docs. 
            // pool_max_idle_per_host default is 32. 
            // If we hit one host, we need this much higher.
            .pool_max_idle_per_host(pool_size) 
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Some(Duration::from_secs(30)))
            .build()
            .expect("Failed to create default client");

        let proxy_clients = Arc::new(DashMap::new());
        let proxy_clients_clone = proxy_clients.clone();

        // Background cleanup task for proxy clients
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let now = Instant::now();
                proxy_clients_clone.retain(|_, (_, last_access)| {
                    now.duration_since(*last_access) < Duration::from_secs(3600)
                });
            }
        });

        RequestDownloader {
            limit,
            locker,
            cache_service: sync,
            enable_session: Arc::new(AtomicBool::new(false)),
            enable_locker: Arc::new(AtomicBool::new(false)),
            enable_rate_limit: Arc::new(AtomicBool::new(true)),
            proxy_clients,
            default_client,
            pool_size,
            max_response_size,
        }
    }

    async fn process_request(&self, request: Request) -> Result<Request> {
        if !self.is_session_enabled(&request) {
            return Ok(request);
        }

        let session_key = self.session_scope_key(&request);
        let mut modified_request = request;

        if let Some(session_state) = self.load_session_state(&session_key).await? {
            modified_request.headers.merge_if_absent(&session_state.headers);
            modified_request.cookies.merge_if_absent(&session_state.cookies);
        }

        Ok(modified_request)
    }

    async fn load_session_state(&self, session_key: &str) -> Result<Option<SessionState>> {
        Ok(SessionState::sync(session_key, &self.cache_service).await?)
    }

    async fn save_session_state(
        &self,
        session_key: &str,
        module_id: String,
        request: &Request,
        response: &Response,
    ) {
        let mut session_state = match self.load_session_state(session_key).await {
            Ok(Some(v)) => v,
            Ok(None) => SessionState {
                session_id: session_key.to_string(),
                module_id,
                headers: Headers::default(),
                cookies: Cookies::default(),
                version: 0,
            },
            Err(err) => {
                warn!(
                    "Failed to load session state for {}: {:?}",
                    session_key, err
                );
                SessionState {
                    session_id: session_key.to_string(),
                    module_id,
                    headers: Headers::default(),
                    cookies: Cookies::default(),
                    version: 0,
                }
            }
        };

        let request_host = Url::parse(&request.url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()));

        session_state.cookies.merge_if_absent(&request.cookies);
        for item in &response.cookies.cookies {
            let mut normalized_item = item.clone();
            if normalized_item.domain.trim().is_empty()
                && let Some(host) = &request_host
            {
                normalized_item.domain = host.clone();
            }

            if let Some(existing) = session_state
                .cookies
                .cookies
                .iter_mut()
                .find(|c| c.name == normalized_item.name && c.domain == normalized_item.domain)
            {
                *existing = normalized_item;
            } else {
                session_state.cookies.cookies.push(normalized_item);
            }
        }

        if let Some(cache_headers) = &request.cache_headers
            && !cache_headers.is_empty()
        {
            let cache_headers_set: std::collections::HashSet<String> = cache_headers
                .iter()
                .map(|h| h.to_lowercase())
                .collect();

            for (name, value) in &response.headers {
                if !cache_headers_set.contains(&name.to_lowercase()) {
                    continue;
                }

                if let Some(existing) = session_state
                    .headers
                    .headers
                    .iter_mut()
                    .find(|h| h.key.eq_ignore_ascii_case(name))
                {
                    existing.value = value.clone();
                } else {
                    session_state.headers.headers.push(HeaderItem {
                        key: name.clone(),
                        value: value.clone(),
                    });
                }
            }
        }

        session_state.version = session_state.version.saturating_add(1);
        if let Err(err) = session_state.send(session_key, &self.cache_service).await {
            warn!(
                "Failed to cache session state for {}: {:?}",
                session_key, err
            );
        }
    }

    async fn process_response(
        &self,
        request: Request,
        response: reqwest::Response,
        pre_calculated_hash: Option<String>,
    ) -> Result<Response> {
        let session_enabled = self.is_session_enabled(&request);
        let request_id = request.id;
        // Use pre-calculated hash if available, otherwise calculate it
        let request_hash = if session_enabled {
            pre_calculated_hash.or_else(|| Some(request.hash()))
        } else {
            None
        };
        let status_code = response.status().as_u16();
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("").to_string()))
            .collect();
        let response_cookies: Vec<CookieItem> = response
            .cookies()
            .map(|cookie| CookieItem {
                name: cookie.name().to_string(),
                value: cookie.value().to_string(),
                domain: cookie.domain().unwrap_or("").to_string(),
                path: cookie.path().unwrap_or("/").to_string(),
                expires: cookie
                    .expires()
                    .and_then(|exp| exp.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs()),
                max_age: cookie.max_age().map(|d| d.as_secs()),
                secure: cookie.secure(),
                http_only: Some(cookie.http_only()),
            })
            .collect();

        let content_length = response.content_length();
        if let Some(len) = content_length {
            if len > self.max_response_size as u64 {
                warn!("Response size {} exceeds limit {}, aborting download for {}", len, self.max_response_size, request.url);
                return Err(DownloadError::DownloadFailed("Response too large".into()).into());
            }
        }

        // Stream the body to enforce size limit during download
        let mut content = Vec::new();
        let mut stream = response.bytes_stream();
        let limit = self.max_response_size;

        while let Some(item) = stream.next().await {
            let chunk = item.map_err(|e: reqwest::Error| DownloadError::DownloadFailed(e.into()))?;
            if content.len() + chunk.len() > limit {
                 warn!("Response size exceeds limit {}, aborting download for {}", limit, request.url);
                 return Err(DownloadError::DownloadFailed("Response too large".into()).into());
            }
            content.extend_from_slice(&chunk);
        }

        Ok(Response {
            id: request_id,
            platform: request.platform,
            account: request.account,
            module: request.module,
            status_code,
            cookies: Cookies {
                cookies: response_cookies,
            },
            content,
            storage_path: None,
            headers: response_headers,
            task_retry_times: request.task_retry_times,
            metadata: request.meta,
            download_middleware: request.download_middleware,
            data_middleware: request.data_middleware,
            task_finished: request.task_finished,
            context: request.context,
            run_id: request.run_id,
            prefix_request: request.prefix_request,
            request_hash,
            priority: request.priority,
        })
    }

    async fn get_client(&self, proxy: Option<&String>) -> Result<Client> {
        if let Some(proxy_url) = proxy {
            if let Some(mut entry) = self.proxy_clients.get_mut(proxy_url) {
                entry.1 = Instant::now();
                return Ok(entry.0.clone());
            }
            
            let reqwest_proxy = Proxy::all(proxy_url).map_err(|e| DownloadError::ClientError(e.into()))?;
            let client = Client::builder()
                .proxy(reqwest_proxy)
                .pool_idle_timeout(Duration::from_secs(90))
                .pool_max_idle_per_host(self.pool_size) // Increased from 32 to 200 for better proxy concurrency
                .tcp_keepalive(Duration::from_secs(60))
                .tcp_nodelay(true)
                .connect_timeout(Duration::from_secs(10))
                .http2_keep_alive_interval(Some(Duration::from_secs(30)))
                .build()
                .map_err(|e| DownloadError::ClientError(e.into()))?;
            
            // Limit cache size to prevent OOM with high-cardinality dynamic proxies
            if self.proxy_clients.len() < 1000 {
                self.proxy_clients.insert(proxy_url.clone(), (client.clone(), Instant::now()));
            }
            Ok(client)
        } else {
            Ok(self.default_client.clone())
        }
    }

    async fn do_download(&self, request: Request, pre_calculated_hash: Option<String>) -> Result<Response> {
        let _request_id = request.id;
        let session_enabled = self.is_session_enabled(&request);
        if let Some(seconds) = request.time_sleep_secs {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        }
        if self.enable_rate_limit.load(Ordering::Relaxed) {
             // Use module_id as default limit_id if not specified
            let limit_id = if request.limit_id.is_empty() {
                request.module_id()
            } else {
                request.limit_id.clone()
            };

            loop {
                let res = self.limit.check_and_update(&limit_id).await;
                match res {
                    Ok(u) => {
                        if u > 0 {
                            if u > 5000 {
                                warn!("Rate limit wait too long ({}ms) for {}, aborting request to yield resources", u, limit_id);
                                return Err(crate::errors::Error::from(crate::errors::RateLimitError::WaitTimeTooLong(u)));
                            }
                            // Rate limit exceeded, wait for the specified duration
                            tokio::time::sleep(Duration::from_millis(u)).await;
                        } else {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("Rate limit verify error: {:?}", e);
                        break;
                    },
                }
            }
        }

        let mut request = self.process_request(request).await?;

        // Get pooled client
        let proxy_url = request.proxy.as_ref().map(|p| p.to_string());
        let client = self.get_client(proxy_url.as_ref()).await?;

        let method = Method::from_str(&request.method).map_err(|e| RequestError::InvalidMethod(e.into()))?;
        let url = match &request.params {
            Some(params) => Url::parse_with_params(&request.url, params)
                .map_err(|e| RequestError::InvalidUrl(e.to_string()))?,
            None => {
                Url::parse(&request.url).map_err(|e| RequestError::InvalidUrl(e.to_string()))?
            }
        };

        let cookie_header = if request.cookies.cookies.is_empty() {
            None
        } else {
            request.cookies.cookie_header_for_url(&url)
        };

        let mut request_builder = client.request(method, url);
        
        // Set Headers
        let headers: HeaderMap = HeaderMap::from(&request.headers);
        request_builder = request_builder.headers(headers);
        
        // Set Cookies manually (filtered by target URL domain/path rules)
        if let Some(cookie_str) = cookie_header {
            request_builder = request_builder.header(reqwest::header::COOKIE, cookie_str);
        }

        // Set Timeout
        request_builder = request_builder.timeout(Duration::from_secs(request.timeout));

        // Consume body/form/json to avoid cloning
        if let Some(body) = request.body.take() {
            request_builder = request_builder.body(body);
        }

        if let Some(form) = request.form.take() {
            request_builder = request_builder.form(&form);
        }

        if let Some(json) = request.json.take() {
            request_builder = request_builder.json(&json);
        }
        
        let start = std::time::Instant::now();
        let result = request_builder
            .send()
            .await;

        let response = match result {
            Ok(res) => res,
            Err(e) => {
                // Circuit Breaker for Network Errors (Timeout, Connection Refused)
                if self.enable_rate_limit.load(Ordering::Relaxed) {
                    let limit_id = if request.limit_id.is_empty() {
                        request.module_id()
                    } else {
                        request.limit_id.clone()
                    };
                    
                    if e.is_connect() || e.is_timeout() {
                         // Use a shorter suspension for network errors (10s) compared to 429s (60s)
                         // This prevents a single proxy timeout from stalling the crawler for too long
                         warn!("Circuit Breaker triggered for {} due to network error: {}. Suspending for 10s.", limit_id, e);
                         self.limit.suspend(&limit_id, Duration::from_secs(10)).await.ok();
                    }
                }
                return Err(DownloadError::DownloadFailed(e.into()).into());
            }
        };

        if self.enable_rate_limit.load(Ordering::Relaxed) {
            let limit_id = if request.limit_id.is_empty() {
                request.module_id()
            } else {
                request.limit_id.clone()
            };
            
            let status = response.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                warn!("Circuit Breaker triggered for {}, suspending for 60s", limit_id);
                self.limit.suspend(&limit_id, Duration::from_secs(60)).await.ok();
            } else if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                warn!("Backpressure triggered for {}, decreasing limit", limit_id);
                self.limit.decrease_limit(&limit_id, 0.5).await.ok();
            } else if status.is_success() && rand::rng().random_bool(0.1) {
                 self.limit.try_restore_limit(&limit_id, 1.1).await.ok();
            }
        }
            
        let duration = start.elapsed().as_secs_f64();
        histogram!("downloader_request_duration_seconds", "module" => request.module.clone()).record(duration);
        counter!("downloader_requests_total", "status_code" => response.status().as_u16().to_string(), "module" => request.module.clone()).increment(1);

        let session_key_for_state_cache = if session_enabled {
            Some(self.session_scope_key(&request))
        } else {
            None
        };
        let request_for_state_cache = if session_enabled {
            Some(request.clone())
        } else {
            None
        };
        let module_id_for_state_cache = if session_enabled {
            Some(request.module_id())
        } else {
            None
        };

        let response_processed = self.process_response(request, response, pre_calculated_hash.clone()).await?;

        if let (Some(session_key), Some(module_id), Some(request_for_cache)) =
            (session_key_for_state_cache, module_id_for_state_cache, request_for_state_cache)
        {
            self
                .save_session_state(&session_key, module_id, &request_for_cache, &response_processed)
                .await;
        }

        Ok(response_processed)
    }
    pub async fn set_limit_config(&self, id: &str, limit: f32) {
        self.limit.set_limit(id, limit).await.ok();
    }
}
#[async_trait::async_trait]
impl Downloader for RequestDownloader {
    async fn set_config(&self, id: &str, config: DownloadConfig) {
        if config.enable_session {
             // Cache clearing logic if needed
        }
        if self.enable_session.load(Ordering::Relaxed) != config.enable_session {
            self.enable_session.store(config.enable_session, Ordering::Relaxed);
        }
        if self.enable_locker.load(Ordering::Relaxed) != config.enable_locker {
            self.enable_locker.store(config.enable_locker, Ordering::Relaxed);
        }
        if self.enable_rate_limit.load(Ordering::Relaxed) != config.enable_rate_limit {
            self.enable_rate_limit.store(config.enable_rate_limit, Ordering::Relaxed);
        }
        self.limit.set_limit(id, config.rate_limit).await.ok();
    }

    async fn set_limit(&self, id: &str, limit: f32) {
        self.set_limit_config(id, limit).await;
    }

    fn name(&self) -> String {
        "request_downloader".to_string()
    }

    fn version(&self) -> Version {
        Version::parse("0.1.0").unwrap()
    }
    async fn download(&self, request: Request) -> Result<Response> {
        let session_enabled = self.is_session_enabled(&request);
        let request_hash = if session_enabled {
            Some(request.hash())
        } else {
            None
        };
        if let Some(hash) = request_hash.as_ref()
            && let Ok(Some(response)) = Response::sync(hash, &self.cache_service).await {
                info!("Cache hit for request: {}", request.id);
                if session_enabled {
                    let session_key = self.session_scope_key(&request);
                    let module_id = request.module_id();
                    self
                        .save_session_state(&session_key, module_id, &request, &response)
                        .await;
                }
                return Ok(Response {
                    id: request.id,
                    platform: request.platform.clone(),
                    account: request.account.clone(),
                    module: request.module.clone(),
                    status_code: response.status_code,
                    cookies: Cookies {
                        cookies: response.cookies.cookies,
                    },
                    content: response.content,
                    storage_path: response.storage_path,
                    headers: response.headers,
                    task_retry_times: request.task_retry_times,
                    metadata: request.meta.clone(),
                    download_middleware: request.download_middleware.clone(),
                    data_middleware: request.data_middleware.clone(),
                    task_finished: request.task_finished,
                    context: request.context.clone(),
                    run_id: request.run_id,
                    prefix_request: request.id,
                    request_hash: request_hash.clone(),
                    priority: request.priority,
                });
            }
        
        // Determine if locking is enabled: prefer request-level config, fallback to global config
        let locker_enabled = request.enable_locker.unwrap_or(self.enable_locker.load(Ordering::Relaxed));
        
        if locker_enabled {
            // Use module_id + run_id as lock key to serialize requests within the same run execution
            // Reduced timeout to minimize blocking on high concurrency
            let key = format!("task-download-{}-{}", request.module_id(), request.run_id);

            // Attempt to acquire lock with short timeout
            match self
                .locker
                .acquire_lock(&key, 30, Duration::from_secs(10))
                .await
            {
                Ok(_) => {
                    let response = self.do_download(request, request_hash.clone()).await;
                    // Ensure lock release
                    self.locker.release_lock(&key).await.ok();
                    response
                }
                Err(_) => {
                    // Lock acquisition failed (timeout/contention), proceed without lock to avoid starvation
                    warn!(
                        "Failed to acquire download lock for task: {}, proceeding without lock",
                        request.task_id()
                    );
                    self.do_download(request, request_hash.clone()).await
                }
            }
        } else {
            self.do_download(request, request_hash.clone()).await
        }
    }
    async fn health_check(&self) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::utils::distributed_rate_limit::RateLimitConfig;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_downloader_creation() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test"));
        let config = RateLimitConfig::new(10.0);
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(None, lock_manager.clone(), "test", config));
        let cache_service = Arc::new(CacheService::new(None, "test".to_string(), None, None));

        let downloader = RequestDownloader::new(limiter, lock_manager, cache_service, 200, 1024*1024*10);
        assert_eq!(downloader.name(), "request_downloader");
    }

    #[tokio::test]
    async fn test_downloader_rate_limit_execution() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test_exec"));
        let config = RateLimitConfig::new(10.0); // 10 req/s = 100ms interval
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(None, lock_manager.clone(), "test_exec", config));
        let cache_service = Arc::new(CacheService::new(None, "test".to_string(), None, None));
        
        let downloader = RequestDownloader::new(limiter.clone(), lock_manager, cache_service, 200, 1024*1024*10);
        downloader.enable_rate_limit.store(true, Ordering::Relaxed);

        // Set a strict limit: 1 req per second
        downloader.set_limit("test_exec", 1.0).await;
        
        // Mock request
        let mut request = Request::new("http://example.com", "GET");
        request.id = Uuid::new_v4();
        request.limit_id = "test_exec".to_string();

        // First request - should consume token
        limiter.record("test_exec").await.unwrap();
        
        // Verify next request would be delayed
        // (We can't easily run do_download without a real network or mocking client, 
        // so we check the limiter state which RequestDownloader uses)
        let delay = limiter.verify("test_exec").await.unwrap();
        assert!(delay.is_some());
        assert!(delay.unwrap() > 0);
    }
}
