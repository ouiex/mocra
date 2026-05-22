use crate::cacheable::{CacheAble, CacheService};
use crate::common::model::cookies::CookieItem;
use crate::common::model::download_config::DownloadConfig;
use crate::common::model::headers::HeaderItem;
use crate::common::model::{Cookies, Headers, Request, Response};
use crate::common::response_cache::{
    RESPONSE_CACHE_LOOKUP_KEY, ResponseCachePersistRequest, ResponseCacheRemoteClient,
    annotate_response_cache_metadata as annotate_shared_response_cache_metadata,
    current_owner_api_base_url, current_time_ms as response_cache_current_time_ms,
    persist_response_cache_entry,
    resolve_response_cache_expires_at as resolve_response_cache_expires_at_with_fallback,
};
use crate::downloader::Downloader;
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use crate::errors::{CacheError, DownloadError, RequestError, Result};
use crate::utils::distributed_lock::DistributedLockManager;
use crate::utils::distributed_rate_limit::DistributedSlidingWindowRateLimiter;
use dashmap::DashMap;
use futures::StreamExt;
use log::{info, warn};
use rand::RngExt;
use reqwest::Client;
use reqwest::Method;
use reqwest::Proxy;
use reqwest::header::HeaderMap;
use semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, UNIX_EPOCH};
use url::Url;
const ACTIVE_NODE_HEARTBEAT_TTL_SECS: u64 = 30;
const REMOTE_CACHE_LOOKUP_TIMEOUT_SECS: u64 = 3;

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

#[derive(Debug, Deserialize)]
struct RemoteCachedResponsePayload {
    response: Response,
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
    owner_namespace: String,
    owner_node_id: Option<String>,
    profile_store: Arc<ProfileControlPlaneStore>,
    api_key: Option<String>,
    federation_response_cache_api_endpoints: HashMap<String, String>,
    remote_cache_client: Option<Arc<dyn ResponseCacheRemoteClient>>,
    response_cache_ttl_secs: Arc<RwLock<Option<u64>>>,
    proxy_clients: Arc<DashMap<String, (Client, Instant)>>,
    default_client: Client,
    pool_size: usize,
    max_response_size: usize,
}

#[derive(Clone)]
pub struct RequestDownloaderConfig {
    pub limiter: Arc<DistributedSlidingWindowRateLimiter>,
    pub locker: Arc<DistributedLockManager>,
    pub cache_service: Arc<CacheService>,
    pub owner_namespace: String,
    pub owner_node_id: Option<String>,
    pub profile_store: Arc<ProfileControlPlaneStore>,
    pub api_key: Option<String>,
    pub pool_size: usize,
    pub max_response_size: usize,
}

impl RequestDownloader {
    #[inline]
    fn is_session_enabled(&self, request: &Request) -> bool {
        request.enable_session || self.enable_session.load(Ordering::Relaxed)
    }

    #[inline]
    fn is_response_cache_enabled(&self, request: &Request) -> bool {
        request.enable_response_cache
    }

    #[inline]
    fn session_scope_key(&self, request: &Request) -> String {
        format!("{}:{}", request.module_id(), request.run_id)
    }

    pub fn new(config: RequestDownloaderConfig) -> Self {
        let owner_namespace = config.owner_namespace;
        let default_client = Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(config.pool_size)
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

        let remote_cache_client: Option<Arc<dyn ResponseCacheRemoteClient>> = Some(Arc::new(
            crate::downloader::response_cache_remote::HttpResponseCacheRemoteClient::new(
                crate::downloader::response_cache_remote::HttpResponseCacheRemoteClientConfig {
                    owner_namespace: owner_namespace.clone(),
                    owner_node_id: config.owner_node_id.clone(),
                    profile_store: config.profile_store.clone(),
                    api_key: config.api_key.clone(),
                    federation_endpoints: HashMap::new(),
                    http_client: default_client.clone(),
                },
            ),
        ));

        RequestDownloader {
            limit: config.limiter,
            locker: config.locker,
            cache_service: config.cache_service,
            enable_session: Arc::new(AtomicBool::new(false)),
            enable_locker: Arc::new(AtomicBool::new(false)),
            enable_rate_limit: Arc::new(AtomicBool::new(true)),
            owner_namespace,
            owner_node_id: config.owner_node_id,
            profile_store: config.profile_store,
            api_key: config.api_key,
            federation_response_cache_api_endpoints: HashMap::new(),
            remote_cache_client,
            response_cache_ttl_secs: Arc::new(RwLock::new(None)),
            proxy_clients,
            default_client,
            pool_size: config.pool_size,
            max_response_size: config.max_response_size,
        }
    }

    pub fn with_federation_response_cache_api_endpoints(
        mut self,
        endpoints: HashMap<String, String>,
    ) -> Self {
        self.federation_response_cache_api_endpoints = endpoints.clone();
        self.remote_cache_client = Some(Arc::new(
            crate::downloader::response_cache_remote::HttpResponseCacheRemoteClient::new(
                crate::downloader::response_cache_remote::HttpResponseCacheRemoteClientConfig {
                    owner_namespace: self.owner_namespace.clone(),
                    owner_node_id: self.owner_node_id.clone(),
                    profile_store: self.profile_store.clone(),
                    api_key: self.api_key.clone(),
                    federation_endpoints: endpoints,
                    http_client: self.default_client.clone(),
                },
            ),
        ));
        self
    }

    fn merge_json_value_preserving_base(base: &mut serde_json::Value, overlay: &serde_json::Value) {
        if overlay.is_null() {
            return;
        }

        if let (Some(base_map), Some(overlay_map)) = (base.as_object_mut(), overlay.as_object()) {
            for (key, value) in overlay_map {
                base_map.entry(key.clone()).or_insert_with(|| value.clone());
            }
            return;
        }

        if base.is_null() {
            *base = overlay.clone();
        }
    }

    fn merge_metadata_preserving_base(
        &self,
        mut base: crate::common::model::meta::MetaData,
        overlay: &crate::common::model::meta::MetaData,
    ) -> crate::common::model::meta::MetaData {
        Self::merge_json_value_preserving_base(&mut base.task, &overlay.task);
        Self::merge_json_value_preserving_base(&mut base.login_info, &overlay.login_info);
        Self::merge_json_value_preserving_base(&mut base.module_config, &overlay.module_config);
        Self::merge_json_value_preserving_base(&mut base.trait_meta, &overlay.trait_meta);
        base
    }

    fn response_cache_identity_metadata(
        &self,
        request_hash: Option<&str>,
    ) -> crate::common::model::meta::MetaData {
        let owner_api_base_url = self.local_owner_api_base_url();
        annotate_shared_response_cache_metadata(
            crate::common::model::meta::MetaData::default(),
            &self.owner_namespace,
            self.owner_node_id.as_deref(),
            owner_api_base_url.as_deref(),
            request_hash,
            self.resolve_response_cache_expires_at(&crate::common::model::meta::MetaData::default()),
        )
    }

    fn annotate_response_cache_metadata(
        &self,
        metadata: crate::common::model::meta::MetaData,
        request_hash: Option<&str>,
    ) -> crate::common::model::meta::MetaData {
        let expires_at = self.resolve_response_cache_expires_at(&metadata);
        let owner_api_base_url = self.local_owner_api_base_url();
        annotate_shared_response_cache_metadata(
            metadata,
            &self.owner_namespace,
            self.owner_node_id.as_deref(),
            owner_api_base_url.as_deref(),
            request_hash,
            expires_at,
        )
    }

    fn local_owner_api_base_url(&self) -> Option<String> {
        current_owner_api_base_url(
            &self.profile_store,
            self.owner_node_id.as_deref(),
            Duration::from_secs(ACTIVE_NODE_HEARTBEAT_TTL_SECS),
        )
    }

    fn resolve_response_cache_expires_at(
        &self,
        metadata: &crate::common::model::meta::MetaData,
    ) -> Option<i64> {
        resolve_response_cache_expires_at_with_fallback(
            metadata,
            self.response_cache_ttl_fallback(),
        )
    }

    fn response_cache_ttl_fallback(&self) -> Option<Duration> {
        self.response_cache_ttl_secs
            .read()
            .ok()
            .and_then(|guard| *guard)
            .map(Duration::from_secs)
            .or_else(|| self.cache_service.default_ttl())
    }

    async fn store_response_cache(&self, response: &Response) {
        let owner_api_base_url = self.local_owner_api_base_url();
        let _ = persist_response_cache_entry(ResponseCachePersistRequest {
            response,
            owner_namespace: &self.owner_namespace,
            owner_node_id: self.owner_node_id.as_deref(),
            owner_api_base_url: owner_api_base_url.as_deref(),
            fallback_ttl: self.response_cache_ttl_fallback(),
            cache_service: &self.cache_service,
            profile_store: &self.profile_store,
            context: "request_downloader",
        })
        .await;
    }

    async fn save_session_state_for_cached_response(&self, request: &Request, response: &Response) {
        if !self.is_session_enabled(request) {
            return;
        }

        let session_key = self.session_scope_key(request);
        let module_id = request.module_id();
        let existing_session = match SessionState::sync(&session_key, &self.cache_service).await {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    "Session load failed (cache hit path): session={} account={} platform={} url={} error={:?}",
                    session_key, request.account, request.platform, request.url, err
                );
                None
            }
        };
        self.save_session_state(&session_key, module_id, request, response, existing_session)
            .await;
    }

    fn build_cached_hit_response(
        &self,
        request: &Request,
        response: Response,
        request_hash: Option<String>,
    ) -> Response {
        Response {
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
            metadata: self.merge_metadata_preserving_base(
                self.merge_metadata_preserving_base(request.meta.clone(), &response.metadata),
                &self.response_cache_identity_metadata(request_hash.as_deref()),
            ),
            download_middleware: request.download_middleware.clone(),
            data_middleware: request.data_middleware.clone(),
            task_finished: request.task_finished,
            context: request.context.clone(),
            run_id: request.run_id,
            prefix_request: request.prefix_request,
            request_hash: response.request_hash.clone().or(request_hash),
            priority: request.priority,
        }
    }

    async fn clear_stale_response_cache_owner(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        owner_node_id: Option<&str>,
        reason: &str,
    ) {
        if let Err(err) = self
            .profile_store
            .remove_response_cache_owner_if_matches(cache_key, owner_namespace, owner_node_id)
            .await
        {
            warn!(
                "Failed to clear stale response cache owner: cache_key={} owner_namespace={} owner_node_id={:?} reason={} error={:?}",
                cache_key, owner_namespace, owner_node_id, reason, err
            );
        }
    }

    fn log_remote_cache_lookup_skip(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        owner_node_id: Option<&str>,
        reason: &str,
    ) {
        warn!(
            "Remote cached response lookup skipped: cache_key={} owner_namespace={} owner_node_id={:?} reason={}",
            cache_key, owner_namespace, owner_node_id, reason
        );
    }

    fn log_remote_cache_lookup_error<E: std::fmt::Debug>(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        endpoint: &str,
        reason: &str,
        error: E,
    ) {
        warn!(
            "Remote cached response lookup failed: cache_key={} owner_namespace={} endpoint={} reason={} error={:?}",
            cache_key, owner_namespace, endpoint, reason, error
        );
    }

    fn log_remote_cache_lookup_non_success(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        endpoint: &str,
        status: reqwest::StatusCode,
    ) {
        warn!(
            "Remote cached response lookup returned non-success: cache_key={} owner_namespace={} endpoint={} reason=http_status status={}",
            cache_key, owner_namespace, endpoint, status
        );
    }

    fn cached_response_matches_key(
        &self,
        cache_key: &str,
        response: &Response,
        source: &str,
        owner_namespace: Option<&str>,
        endpoint: Option<&str>,
    ) -> bool {
        if let Some(response_hash) = response.request_hash.as_deref()
            && response_hash != cache_key
        {
            warn!(
                "Cached response returned mismatched request_hash: source={} cache_key={} response_request_hash={} owner_namespace={:?} endpoint={:?}",
                source, cache_key, response_hash, owner_namespace, endpoint
            );
            return false;
        }

        if let Some(lookup_key) = response
            .metadata
            .get_trait_config::<String>(RESPONSE_CACHE_LOOKUP_KEY)
            .as_deref()
            && lookup_key != cache_key
        {
            warn!(
                "Cached response returned mismatched lookup_key: source={} cache_key={} response_lookup_key={} owner_namespace={:?} endpoint={:?}",
                source, cache_key, lookup_key, owner_namespace, endpoint
            );
            return false;
        }

        true
    }

    async fn try_load_remote_cached_response(&self, cache_key: &str) -> Option<Response> {
        let owner = self.profile_store.get_response_cache_owner(cache_key)?;
        if owner
            .expires_at
            .is_some_and(|expires_at| expires_at <= response_cache_current_time_ms())
        {
            self.clear_stale_response_cache_owner(
                cache_key,
                &owner.owner_namespace,
                owner.owner_node_id.as_deref(),
                "owner_record_expired",
            )
            .await;
            return None;
        }
        let Some(api_key) = self.api_key.as_deref() else {
            self.log_remote_cache_lookup_skip(
                cache_key,
                &owner.owner_namespace,
                owner.owner_node_id.as_deref(),
                "missing_api_key",
            );
            return None;
        };

        let encoded_cache_key: String =
            url::form_urlencoded::byte_serialize(cache_key.as_bytes()).collect();
        let mut remote_endpoints: Vec<(String, bool)> = Vec::new();
        let mut push_remote_endpoint = |endpoint: String, authoritative_not_found: bool| {
            if let Some((_, existing_authoritative_not_found)) = remote_endpoints
                .iter_mut()
                .find(|(existing_endpoint, _)| *existing_endpoint == endpoint)
            {
                *existing_authoritative_not_found |= authoritative_not_found;
            } else {
                remote_endpoints.push((endpoint, authoritative_not_found));
            }
        };

        if owner.owner_namespace == self.owner_namespace {
            let Some(owner_node_id) = owner.owner_node_id.as_deref() else {
                self.log_remote_cache_lookup_skip(
                    cache_key,
                    &owner.owner_namespace,
                    None,
                    "missing_owner_node_id",
                );
                return None;
            };
            if self.owner_node_id.as_deref() == Some(owner_node_id) {
                self.clear_stale_response_cache_owner(
                    cache_key,
                    &owner.owner_namespace,
                    owner.owner_node_id.as_deref(),
                    "local_owner_cache_miss",
                )
                .await;
                return None;
            }

            let node = self
                .profile_store
                .list_active_nodes(Duration::from_secs(ACTIVE_NODE_HEARTBEAT_TTL_SECS))
                .into_iter()
                .find(|candidate| candidate.id == owner_node_id);
            let Some(node) = node else {
                self.clear_stale_response_cache_owner(
                    cache_key,
                    &owner.owner_namespace,
                    owner.owner_node_id.as_deref(),
                    "same_namespace_owner_not_active",
                )
                .await;
                return None;
            };
            let Some(api_port) = node.api_port else {
                self.log_remote_cache_lookup_skip(
                    cache_key,
                    &owner.owner_namespace,
                    Some(owner_node_id),
                    "missing_api_port",
                );
                return None;
            };
            push_remote_endpoint(
                format!(
                    "http://{}:{}/debug/cache/response/{}",
                    node.ip, api_port, encoded_cache_key
                ),
                true,
            );
        } else {
            if let Some(base_url) = owner.owner_api_base_url.as_deref() {
                push_remote_endpoint(
                    format!(
                        "{}/debug/cache/response/{}",
                        base_url.trim_end_matches('/'),
                        encoded_cache_key
                    ),
                    false,
                );
            }

            if let Some(base_url) = self
                .federation_response_cache_api_endpoints
                .get(&owner.owner_namespace)
            {
                push_remote_endpoint(
                    format!(
                        "{}/debug/cache/response/{}",
                        base_url.trim_end_matches('/'),
                        encoded_cache_key
                    ),
                    true,
                );
            }
        }

        if remote_endpoints.is_empty() {
            let reason = if owner.owner_namespace == self.owner_namespace {
                "missing_same_namespace_endpoint"
            } else {
                "missing_foreign_namespace_endpoint_mapping"
            };
            self.log_remote_cache_lookup_skip(
                cache_key,
                &owner.owner_namespace,
                owner.owner_node_id.as_deref(),
                reason,
            );
            return None;
        }

        for (endpoint, authoritative_not_found) in remote_endpoints {
            let response = match self
                .default_client
                .get(&endpoint)
                .header("x-api-key", api_key)
                .timeout(Duration::from_secs(REMOTE_CACHE_LOOKUP_TIMEOUT_SECS))
                .send()
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    self.log_remote_cache_lookup_error(
                        cache_key,
                        &owner.owner_namespace,
                        &endpoint,
                        "request_failed",
                        err,
                    );
                    continue;
                }
            };

            if response.status() == reqwest::StatusCode::NOT_FOUND {
                if authoritative_not_found {
                    self.clear_stale_response_cache_owner(
                        cache_key,
                        &owner.owner_namespace,
                        owner.owner_node_id.as_deref(),
                        "remote_cache_not_found",
                    )
                    .await;
                }
                continue;
            }

            if !response.status().is_success() {
                self.log_remote_cache_lookup_non_success(
                    cache_key,
                    &owner.owner_namespace,
                    &endpoint,
                    response.status(),
                );
                continue;
            }

            match response.json::<RemoteCachedResponsePayload>().await {
                Ok(payload) => {
                    if self.cached_response_matches_key(
                        cache_key,
                        &payload.response,
                        "remote_cache_lookup",
                        Some(&owner.owner_namespace),
                        Some(&endpoint),
                    ) {
                        return Some(payload.response);
                    }
                }
                Err(err) => {
                    self.log_remote_cache_lookup_error(
                        cache_key,
                        &owner.owner_namespace,
                        &endpoint,
                        "decode_failed",
                        err,
                    );
                }
            }
        }

        None
    }

    async fn try_load_cached_response(&self, cache_key: &str) -> Option<Response> {
        match Response::sync(cache_key, &self.cache_service).await {
            Ok(Some(response)) => {
                if self.cached_response_matches_key(
                    cache_key,
                    &response,
                    "local_cache_lookup",
                    None,
                    None,
                ) {
                    return Some(response);
                }

                if let Err(err) = Response::delete(cache_key, &self.cache_service).await {
                    warn!(
                        "Failed to delete mismatched local cached response: cache_key={} error={:?}",
                        cache_key, err
                    );
                }
            }
            Ok(None) => {}
            Err(CacheError::Serde(err)) => {
                warn!(
                    "Failed to decode local cached response, deleting corrupt entry: cache_key={} error={:?}",
                    cache_key, err
                );
                if let Err(delete_err) = Response::delete(cache_key, &self.cache_service).await {
                    warn!(
                        "Failed to delete corrupt local cached response: cache_key={} error={:?}",
                        cache_key, delete_err
                    );
                }
            }
            Err(err) => {
                warn!(
                    "Local cached response lookup failed: cache_key={} error={:?}",
                    cache_key, err
                );
            }
        }

        let mut response = if let Some(ref client) = self.remote_cache_client {
            client.fetch_remote_cached_response(cache_key).await?
        } else {
            self.try_load_remote_cached_response(cache_key).await?
        };
        response.metadata =
            self.annotate_response_cache_metadata(response.metadata, Some(cache_key));
        if response.request_hash.is_none() {
            response.request_hash = Some(cache_key.to_string());
        }
        self.store_response_cache(&response).await;
        Some(response)
    }

    /// Load session state and apply it to the request.
    /// Returns the (modified request, loaded session) so callers can reuse the session
    /// in `save_session_state` without a second cache read.
    /// On cache read failure the error is logged and degraded to no-session rather than
    /// propagating, so a transient cache failure does not abort the download.
    async fn process_request(&self, request: Request) -> (Request, Option<SessionState>) {
        if !self.is_session_enabled(&request) {
            return (request, None);
        }

        let session_key = self.session_scope_key(&request);
        let session_state = match SessionState::sync(&session_key, &self.cache_service).await {
            Ok(v) => v,
            Err(err) => {
                warn!(
                    "Session load failed: session={} account={} platform={} url={} error={:?}",
                    session_key, request.account, request.platform, request.url, err
                );
                None
            }
        };

        let mut modified_request = request;
        if let Some(ref state) = session_state {
            modified_request.headers.merge_if_absent(&state.headers);
            modified_request.cookies.merge_if_absent(&state.cookies);
        }

        (modified_request, session_state)
    }

    // Pass the already-loaded session from `process_request` to avoid a second cache read.
    // `None` means the session did not exist before this request (fresh start).
    async fn save_session_state(
        &self,
        session_key: &str,
        module_id: String,
        request: &Request,
        response: &Response,
        existing_session: Option<SessionState>,
    ) {
        let mut session_state = existing_session.unwrap_or_else(|| SessionState {
            session_id: session_key.to_string(),
            module_id,
            headers: Headers::default(),
            cookies: Cookies::default(),
            version: 0,
        });

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Purge expired or deleted cookies from the carried-over session before merging.
        // max_age == 0 is the server's explicit deletion signal (RFC 6265 §4.1.2.2).
        // expires < now means the cookie has naturally expired.
        let before_len = session_state.cookies.cookies.len();
        session_state.cookies.cookies.retain(|c| {
            if c.max_age == Some(0) {
                return false;
            }
            if let Some(exp) = c.expires
                && exp > 0
                && exp < now_secs
            {
                return false;
            }
            true
        });
        let mut dirty = session_state.cookies.cookies.len() < before_len;

        let request_host = Url::parse(&request.url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_ascii_lowercase()));

        // Merge request cookies that are not already in the session.
        let cookies_before = session_state.cookies.cookies.len();
        session_state.cookies.merge_if_absent(&request.cookies);
        if session_state.cookies.cookies.len() != cookies_before {
            dirty = true;
        }

        // Upsert cookies from the response, normalising missing domains.
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
                if existing.value != normalized_item.value
                    || existing.expires != normalized_item.expires
                    || existing.max_age != normalized_item.max_age
                {
                    *existing = normalized_item;
                    dirty = true;
                }
            } else {
                session_state.cookies.cookies.push(normalized_item);
                dirty = true;
            }
        }

        if let Some(cache_headers) = &request.cache_headers
            && !cache_headers.is_empty()
        {
            let cache_headers_set: std::collections::HashSet<String> =
                cache_headers.iter().map(|h| h.to_lowercase()).collect();

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
                    if existing.value != *value {
                        existing.value = value.clone();
                        dirty = true;
                    }
                } else {
                    session_state.headers.headers.push(HeaderItem {
                        key: name.clone(),
                        value: value.clone(),
                    });
                    dirty = true;
                }
            }
        }

        // For a brand-new session with no collected state, skip the cache write entirely.
        // For an existing session, only write back when something actually changed.
        if !dirty {
            return;
        }

        session_state.version = session_state.version.saturating_add(1);
        if let Err(err) = session_state
            .send_persistent(session_key, &self.cache_service)
            .await
        {
            warn!(
                "Failed to cache session state: session={} account={} platform={} url={} error={:?}",
                session_key, request.account, request.platform, request.url, err
            );
        }
    }

    async fn process_response(
        &self,
        request: Request,
        response: reqwest::Response,
        pre_calculated_hash: Option<String>,
    ) -> Result<Response> {
        let response_cache_enabled = self.is_response_cache_enabled(&request);
        let request_id = request.id;
        // Use pre-calculated hash if available, otherwise calculate it
        let request_hash = if response_cache_enabled {
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
        if let Some(len) = content_length
            && len > self.max_response_size as u64
        {
            warn!(
                "Response size {} exceeds limit {}, aborting download for {}",
                len, self.max_response_size, request.url
            );
            return Err(DownloadError::DownloadFailed("Response too large".into()).into());
        }

        // Stream the body to enforce size limit during download.
        // reqwest's `.timeout()` only covers `send().await` (headers); the body
        // stream has no built-in timeout.  Wrap the entire read in a deadline so
        // that a stalled body transfer cannot block a download worker forever.
        let body_timeout = Duration::from_secs(request.timeout.max(30));
        let limit = self.max_response_size;
        let url_for_log = request.url.clone();

        let content = tokio::time::timeout(body_timeout, async {
            let mut buf = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(item) = stream.next().await {
                let chunk =
                    item.map_err(|e: reqwest::Error| DownloadError::DownloadFailed(e.into()))?;
                if buf.len() + chunk.len() > limit {
                    warn!(
                        "Response size exceeds limit {}, aborting download for {}",
                        limit, url_for_log
                    );
                    return Err(DownloadError::DownloadFailed("Response too large".into()).into());
                }
                buf.extend_from_slice(&chunk);
            }
            Ok::<Vec<u8>, crate::errors::Error>(buf)
        })
        .await
        .map_err(|_| {
            warn!(
                "Body read timed out after {}s for {}",
                body_timeout.as_secs(),
                url_for_log
            );
            crate::errors::Error::from(DownloadError::DownloadFailed(
                format!("body read timed out after {}s", body_timeout.as_secs()).into(),
            ))
        })??;

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
            metadata: self.annotate_response_cache_metadata(request.meta, request_hash.as_deref()),
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

            let reqwest_proxy =
                Proxy::all(proxy_url).map_err(|e| DownloadError::ClientError(e.into()))?;
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
                self.proxy_clients
                    .insert(proxy_url.clone(), (client.clone(), Instant::now()));
            }
            Ok(client)
        } else {
            Ok(self.default_client.clone())
        }
    }

    async fn do_download(
        &self,
        request: Request,
        pre_calculated_hash: Option<String>,
    ) -> Result<Response> {
        let _request_id = request.id;
        let session_enabled = self.is_session_enabled(&request);
        if let Some(seconds) = request.time_sleep_secs {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        }
        let rate_limit_enabled = self.enable_rate_limit.load(Ordering::Relaxed);
        info!(
            "[do_download] enable_rate_limit={} request_id={}",
            rate_limit_enabled, request.id
        );
        if rate_limit_enabled {
            // Use module_id as default limit_id if not specified
            let limit_id = if request.limit_id.is_empty() {
                request.module_id()
            } else {
                request.limit_id.clone()
            };

            // Reserve a rate-limit time slot and wait. Each concurrent download
            // gets a unique slot without polling.
            self.limit.wait_for_permit(&limit_id).await?;
        }

        let (mut request, loaded_session) = self.process_request(request).await;

        // Get pooled client
        let proxy_url = request.proxy.as_ref().map(|p| p.to_string());
        let client = self.get_client(proxy_url.as_ref()).await?;

        let method =
            Method::from_str(&request.method).map_err(|e| RequestError::InvalidMethod(e.into()))?;
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
        let result = request_builder.send().await;

        let response = match result {
            Ok(res) => res,
            Err(e) => {
                crate::common::metrics::inc_error(
                    "engine",
                    "downloader",
                    "network",
                    "send_failed",
                    1,
                );
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
                        warn!(
                            "Circuit Breaker triggered for {} due to network error: {}. Suspending for 10s.",
                            limit_id, e
                        );
                        self.limit
                            .suspend(&limit_id, Duration::from_secs(10))
                            .await
                            .ok();
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
                warn!(
                    "Circuit Breaker triggered for {}, suspending for 60s",
                    limit_id
                );
                self.limit
                    .suspend(&limit_id, Duration::from_secs(60))
                    .await
                    .ok();
            } else if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                warn!("Backpressure triggered for {}, decreasing limit", limit_id);
                self.limit.decrease_limit(&limit_id, 0.5).await.ok();
            } else if status.is_success() && rand::rng().random_bool(0.1) {
                self.limit.try_restore_limit(&limit_id, 1.1).await.ok();
            }
        }

        let duration = start.elapsed().as_secs_f64();
        crate::common::metrics::observe_latency(
            "engine",
            "downloader",
            "http_request",
            "success",
            duration,
        );
        crate::common::metrics::inc_throughput(
            "engine",
            "downloader",
            "http_request",
            "success",
            1,
        );

        let session_info = if session_enabled {
            Some((
                self.session_scope_key(&request),
                request.module_id(),
                request.clone(),
                loaded_session,
            ))
        } else {
            None
        };

        let response_processed = self
            .process_response(request, response, pre_calculated_hash.clone())
            .await?;

        if let Some((session_key, module_id, request_for_cache, existing_session)) = session_info {
            self.save_session_state(
                &session_key,
                module_id,
                &request_for_cache,
                &response_processed,
                existing_session,
            )
            .await;
        }

        if response_processed.request_hash.is_some() {
            self.store_response_cache(&response_processed).await;
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
            self.enable_session
                .store(config.enable_session, Ordering::Relaxed);
        }
        if self.enable_locker.load(Ordering::Relaxed) != config.enable_locker {
            self.enable_locker
                .store(config.enable_locker, Ordering::Relaxed);
        }
        info!(
            "[set_config] id={} config.enable_rate_limit={} current={}",
            id,
            config.enable_rate_limit,
            self.enable_rate_limit.load(Ordering::Relaxed)
        );
        if self.enable_rate_limit.load(Ordering::Relaxed) != config.enable_rate_limit {
            info!(
                "[set_config] changing enable_rate_limit from {} to {} for id={}",
                self.enable_rate_limit.load(Ordering::Relaxed),
                config.enable_rate_limit,
                id
            );
            self.enable_rate_limit
                .store(config.enable_rate_limit, Ordering::Relaxed);
        }
        if let Ok(mut guard) = self.response_cache_ttl_secs.write() {
            *guard = Some(config.cache_ttl);
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
        let response_cache_enabled = self.is_response_cache_enabled(&request);
        let request_hash = if response_cache_enabled {
            Some(request.hash())
        } else {
            None
        };
        if let Some(hash) = request_hash.as_ref()
            && let Some(response) = self.try_load_cached_response(hash).await
        {
            info!(
                "Cache hit: request_id={} account={} platform={} url={}",
                request.id, request.account, request.platform, request.url
            );
            self.save_session_state_for_cached_response(&request, &response)
                .await;
            return Ok(self.build_cached_hit_response(&request, response, request_hash.clone()));
        }

        // Determine if locking is enabled: prefer request-level config, fallback to global config
        let locker_enabled = request
            .enable_locker
            .unwrap_or(self.enable_locker.load(Ordering::Relaxed));

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
                        "Failed to acquire download lock: task_id={} account={} platform={} url={}, proceeding without lock",
                        request.task_id(),
                        request.account,
                        request.platform,
                        request.url
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
    use crate::cacheable::CacheServiceConfig;
    use crate::common::model::ExecutionMark;
    use crate::common::response_cache::{
        RESPONSE_CACHE_EXPIRES_AT_KEY, RESPONSE_CACHE_LOOKUP_KEY,
        RESPONSE_CACHE_OWNER_API_BASE_URL_KEY, RESPONSE_CACHE_OWNER_NAMESPACE_KEY,
        RESPONSE_CACHE_OWNER_NODE_ID_KEY,
    };
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use crate::utils::distributed_rate_limit::RateLimitConfig;
    use std::sync::Arc;
    use std::time::SystemTime;
    use uuid::Uuid;

    macro_rules! cache_service_config {
        ($pool:expr, $namespace:expr, $default_ttl:expr, $compression_threshold:expr $(,)?) => {
            CacheService::new(
                CacheServiceConfig::local($namespace)
                    .with_pool($pool)
                    .with_default_ttl($default_ttl)
                    .with_compression_threshold($compression_threshold),
            )
        };
    }

    macro_rules! test_downloader {
        (
            $limiter:expr,
            $locker:expr,
            $cache_service:expr,
            $owner_namespace:expr,
            $owner_node_id:expr,
            $profile_store:expr,
            $api_key:expr,
            $pool_size:expr,
            $max_response_size:expr $(,)?
        ) => {
            RequestDownloader::new(RequestDownloaderConfig {
                limiter: $limiter,
                locker: $locker,
                cache_service: $cache_service,
                owner_namespace: $owner_namespace.to_string(),
                owner_node_id: $owner_node_id,
                profile_store: $profile_store,
                api_key: $api_key,
                pool_size: $pool_size,
                max_response_size: $max_response_size,
            })
        };
    }

    #[tokio::test]
    async fn test_downloader_creation() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test"));
        let config = RateLimitConfig::new(10.0);
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "test",
            config,
        ));
        let cache_service = Arc::new(cache_service_config!(None, "test".to_string(), None, None));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("test").unwrap());

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "test",
            Some("node-a".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );
        assert_eq!(downloader.name(), "request_downloader");
    }

    #[tokio::test]
    async fn test_downloader_rate_limit_execution() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "test_exec"));
        let config = RateLimitConfig::new(10.0); // 10 req/s = 100ms interval
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "test_exec",
            config,
        ));
        let cache_service = Arc::new(cache_service_config!(None, "test".to_string(), None, None));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("test_exec").unwrap());

        let downloader = test_downloader!(
            limiter.clone(),
            lock_manager,
            cache_service,
            "test_exec",
            Some("node-a".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );
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

    fn sample_cached_response(request_hash: Option<String>) -> Response {
        Response {
            id: Uuid::new_v4(),
            platform: "platform-a".to_string(),
            account: "account-a".to_string(),
            module: "module-a".to_string(),
            status_code: 200,
            cookies: Cookies::default(),
            content: b"cached".to_vec(),
            storage_path: None,
            headers: Vec::new(),
            task_retry_times: 0,
            metadata: crate::common::model::meta::MetaData::default(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: ExecutionMark::default(),
            run_id: Uuid::new_v4(),
            prefix_request: Uuid::new_v4(),
            request_hash,
            priority: crate::common::model::Priority::Normal,
        }
    }

    #[tokio::test]
    async fn response_cache_write_stores_ownership_metadata() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "cache_write"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "cache_write",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("cache_write").unwrap());
        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-a".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let hash = "cache-key-1".to_string();
        let mut response = sample_cached_response(Some(hash.clone()));
        response.metadata =
            downloader.annotate_response_cache_metadata(response.metadata, Some(&hash));

        downloader.store_response_cache(&response).await;

        let cached = Response::sync(&hash, &cache_service)
            .await
            .expect("sync should succeed")
            .expect("cached response should exist");
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NAMESPACE_KEY)
                .as_deref(),
            Some("test")
        );
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NODE_ID_KEY)
                .as_deref(),
            Some("node-a")
        );
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_API_BASE_URL_KEY),
            None::<String>
        );
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_LOOKUP_KEY)
                .as_deref(),
            Some("cache-key-1")
        );
        let owner = profile_store
            .get_response_cache_owner(&hash)
            .expect("cache owner should be recorded");
        assert_eq!(owner.owner_namespace, "test");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(owner.owner_api_base_url, None);
        assert_eq!(owner.expires_at, None);
    }

    #[tokio::test]
    async fn response_cache_write_records_owner_api_endpoint_from_local_heartbeat() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "cache_write_endpoint"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "cache_write_endpoint",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("test").unwrap());
        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-a".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "host-a".to_string(),
                api_port: Some(18080),
                last_heartbeat: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                version: "test".to_string(),
            })
            .await
            .expect("node heartbeat should be recorded");
        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-a".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let hash = "cache-key-endpoint".to_string();
        let mut response = sample_cached_response(Some(hash.clone()));
        response.metadata =
            downloader.annotate_response_cache_metadata(response.metadata, Some(&hash));

        downloader.store_response_cache(&response).await;

        let owner = profile_store
            .get_response_cache_owner(&hash)
            .expect("cache owner should be recorded");
        assert_eq!(
            owner.owner_api_base_url.as_deref(),
            Some("http://127.0.0.1:18080")
        );

        let cached = Response::sync(&hash, &cache_service)
            .await
            .expect("sync should succeed")
            .expect("cached response should exist");
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_API_BASE_URL_KEY)
                .as_deref(),
            Some("http://127.0.0.1:18080")
        );
    }

    #[tokio::test]
    async fn response_cache_write_records_owner_expiry_from_configured_ttl() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "cache_write_ttl"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "cache_write_ttl",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("cache_write_ttl").unwrap());
        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-a".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );
        downloader
            .set_config(
                "cache_write_ttl",
                DownloadConfig {
                    enable_session: false,
                    enable_locker: false,
                    enable_rate_limit: true,
                    rate_limit: 10.0,
                    cache_ttl: 60,
                    default_timeout: 30,
                    downloader: "request_downloader".to_string(),
                },
            )
            .await;

        let before_write = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let hash = "cache-key-ttl".to_string();
        let mut response = sample_cached_response(Some(hash.clone()));
        response.metadata =
            downloader.annotate_response_cache_metadata(response.metadata, Some(&hash));

        downloader.store_response_cache(&response).await;

        let owner = profile_store
            .get_response_cache_owner(&hash)
            .expect("cache owner should be recorded");
        let expires_at = owner.expires_at.expect("owner expiry should be recorded");
        assert!(expires_at >= before_write + 60_000);
        assert!(expires_at <= before_write + 65_000);

        let cached = Response::sync(&hash, &cache_service)
            .await
            .expect("sync should succeed")
            .expect("cached response should exist");
        let cached_expires_at = cached
            .metadata
            .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY)
            .expect("cached response expiry should be recorded");
        assert_eq!(cached_expires_at, expires_at);
    }

    #[tokio::test]
    async fn response_cache_write_prefers_explicit_expiry_over_configured_ttl() {
        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "cache_write_explicit_ttl",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "cache_write_explicit_ttl",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("cache_write_explicit_ttl").unwrap());
        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-a".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );
        downloader
            .set_config(
                "cache_write_explicit_ttl",
                DownloadConfig {
                    enable_session: false,
                    enable_locker: false,
                    enable_rate_limit: true,
                    rate_limit: 10.0,
                    cache_ttl: 60,
                    default_timeout: 30,
                    downloader: "request_downloader".to_string(),
                },
            )
            .await;

        let explicit_expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
            + 10_000;
        let hash = "cache-key-explicit-ttl".to_string();
        let mut response = sample_cached_response(Some(hash.clone()));
        response.metadata = downloader.annotate_response_cache_metadata(
            response
                .metadata
                .add_trait_config(RESPONSE_CACHE_EXPIRES_AT_KEY, explicit_expires_at),
            Some(&hash),
        );

        downloader.store_response_cache(&response).await;

        let owner = profile_store
            .get_response_cache_owner(&hash)
            .expect("cache owner should be recorded");
        assert_eq!(owner.expires_at, Some(explicit_expires_at));

        let cached = Response::sync(&hash, &cache_service)
            .await
            .expect("sync should succeed")
            .expect("cached response should exist");
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY),
            Some(explicit_expires_at)
        );
    }

    #[tokio::test]
    async fn expired_owner_record_is_cleared_before_remote_lookup() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "expired_owner_record"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "expired_owner_record",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .upsert_response_cache_owner_with_expiry(
                "cache-key-1",
                "origin-app",
                Some("node-remote"),
                Some(1),
            )
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            None,
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_remote_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cache_hit_preserves_cached_response_metadata() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "cache_hit"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "cache_hit",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("cache_hit").unwrap());
        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-a".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let mut request = Request::new("http://example.com", "GET").enable_response_cache(true);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "module-a".to_string();
        request.prefix_request = Uuid::new_v4();
        request = request.add_meta("request_only", "live");

        let hash = request.hash();
        let mut cached_response = sample_cached_response(Some(hash.clone()));
        cached_response.metadata = downloader.annotate_response_cache_metadata(
            crate::common::model::meta::MetaData::default().add_trait_config("cached_only", "yes"),
            Some(&hash),
        );
        let cached_prefix = cached_response.prefix_request;
        cached_response
            .send(&hash, &cache_service)
            .await
            .expect("cache write should succeed");

        let response = downloader
            .download(request.clone())
            .await
            .expect("cache hit should succeed");
        assert_eq!(response.prefix_request, request.prefix_request);
        assert_ne!(cached_prefix, request.prefix_request);
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>("request_only")
                .as_deref(),
            Some("live")
        );
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>("cached_only")
                .as_deref(),
            Some("yes")
        );
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NAMESPACE_KEY)
                .as_deref(),
            Some("test")
        );
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NODE_ID_KEY)
                .as_deref(),
            Some("node-a")
        );
    }

    #[tokio::test]
    async fn local_cache_hit_rejects_mismatched_cached_response_payload() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "local_cache_mismatch"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "local_cache_mismatch",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("local_cache_mismatch").unwrap());
        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-a".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let cache_key = "cache-key-1".to_string();
        let mut cached_response = sample_cached_response(Some("other-cache-key".to_string()));
        cached_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "test")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "node-a")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "other-cache-key");
        cached_response
            .send(&cache_key, &cache_service)
            .await
            .expect("cache write should succeed");

        assert!(
            downloader
                .try_load_cached_response(&cache_key)
                .await
                .is_none()
        );
        assert!(
            Response::sync(&cache_key, &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn local_cache_hit_deletes_corrupt_cached_response_entry() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "local_cache_corrupt"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "local_cache_corrupt",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("local_cache_corrupt").unwrap());
        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-a".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let cache_key = "cache-key-1".to_string();
        let raw_cache_key = Response::cache_id(&cache_key, &cache_service);
        cache_service
            .set(&raw_cache_key, b"not-json", None)
            .await
            .expect("corrupt cache write should succeed");

        assert!(
            downloader
                .try_load_cached_response(&cache_key)
                .await
                .is_none()
        );
        assert!(
            cache_service
                .get(&raw_cache_key)
                .await
                .expect("raw cache lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn mismatched_self_owned_local_cache_clears_stale_owner_record() {
        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "mismatched_self_owned_local",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "mismatched_self_owned_local",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-local"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let cache_key = "cache-key-1".to_string();
        let mut cached_response = sample_cached_response(Some("other-cache-key".to_string()));
        cached_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "origin-app")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "node-local")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "other-cache-key");
        cached_response
            .send(&cache_key, &cache_service)
            .await
            .expect("cache write should succeed");

        assert!(
            downloader
                .try_load_cached_response(&cache_key)
                .await
                .is_none()
        );
        assert!(
            Response::sync(&cache_key, &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
        assert!(profile_store.get_response_cache_owner(&cache_key).is_none());
    }

    #[tokio::test]
    async fn corrupt_self_owned_local_cache_clears_stale_owner_record() {
        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "corrupt_self_owned_local",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "corrupt_self_owned_local",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-local"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let cache_key = "cache-key-1".to_string();
        let raw_cache_key = Response::cache_id(&cache_key, &cache_service);
        cache_service
            .set(&raw_cache_key, b"not-json", None)
            .await
            .expect("corrupt cache write should succeed");

        assert!(
            downloader
                .try_load_cached_response(&cache_key)
                .await
                .is_none()
        );
        assert!(
            cache_service
                .get(&raw_cache_key)
                .await
                .expect("raw cache lookup should succeed")
                .is_none()
        );
        assert!(profile_store.get_response_cache_owner(&cache_key).is_none());
    }

    #[tokio::test]
    async fn corrupt_local_cache_falls_back_to_remote_owner_and_rewarms_local_cache() {
        use axum::extract::Path;
        use axum::routing::get;
        use axum::{Json, Router};
        use tokio::net::TcpListener;

        #[derive(Clone)]
        struct RemoteResponseState {
            cache_key: String,
            response: Response,
        }

        async fn remote_cached_response(
            axum::extract::State(state): axum::extract::State<RemoteResponseState>,
            Path(cache_key): Path<String>,
        ) -> Json<serde_json::Value> {
            assert_eq!(cache_key, state.cache_key);
            Json(serde_json::json!({
                "cache_key": state.cache_key,
                "response": state.response,
            }))
        }

        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "corrupt_local_remote_recover",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "corrupt_local_remote_recover",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("corrupt_local_remote_recover").unwrap());

        let mut request = Request::new("http://example.com", "GET").enable_response_cache(true);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "module-a".to_string();
        let cache_key = request.hash();

        let raw_cache_key = Response::cache_id(&cache_key, &cache_service);
        cache_service
            .set(&raw_cache_key, b"not-json", None)
            .await
            .expect("corrupt cache write should succeed");

        let mut remote_response = sample_cached_response(Some(cache_key.clone()));
        remote_response.content = b"remote-recovered".to_vec();
        remote_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(
                RESPONSE_CACHE_OWNER_NAMESPACE_KEY,
                "corrupt_local_remote_recover",
            )
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "node-remote")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, cache_key.clone())
            .add_trait_config("remote_only", "yes");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let port = listener.local_addr().expect("listener local addr").port();
        let app = Router::new()
            .route(
                "/debug/cache/response/{cache_key}",
                get(remote_cached_response),
            )
            .with_state(RemoteResponseState {
                cache_key: cache_key.clone(),
                response: remote_response,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(port),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner(
                &cache_key,
                "corrupt_local_remote_recover",
                Some("node-remote"),
            )
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "corrupt_local_remote_recover",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let response = downloader
            .download(request)
            .await
            .expect("remote recovery should succeed");
        assert_eq!(response.content, b"remote-recovered".to_vec());
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>("remote_only")
                .as_deref(),
            Some("yes")
        );

        let warmed = Response::sync(&cache_key, &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("local cache should be rewarmed");
        assert_eq!(warmed.content, b"remote-recovered".to_vec());
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NODE_ID_KEY)
                .as_deref(),
            Some("node-local")
        );
        let owner = profile_store
            .get_response_cache_owner(&cache_key)
            .expect("local warmed owner should be recorded");
        assert_eq!(owner.owner_namespace, "corrupt_local_remote_recover");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-local"));
    }

    #[tokio::test]
    async fn remote_cache_hit_fetches_owner_response_and_warms_local_cache() {
        use axum::extract::Path;
        use axum::routing::get;
        use axum::{Json, Router};
        use tokio::net::TcpListener;

        #[derive(Clone)]
        struct RemoteResponseState {
            cache_key: String,
            response: Response,
        }

        async fn remote_cached_response(
            axum::extract::State(state): axum::extract::State<RemoteResponseState>,
            Path(cache_key): Path<String>,
        ) -> Json<serde_json::Value> {
            assert_eq!(cache_key, state.cache_key);
            Json(serde_json::json!({
                "cache_key": state.cache_key,
                "response": state.response,
            }))
        }

        let lock_manager = Arc::new(DistributedLockManager::new(None, "remote_cache_hit"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "remote_cache_hit",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("remote_cache_hit").unwrap());

        let mut request = Request::new("http://example.com", "GET").enable_response_cache(true);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "module-a".to_string();
        request = request.add_meta("request_only", "live");
        let cache_key = request.hash();

        let mut remote_response = sample_cached_response(Some(cache_key.clone()));
        remote_response.content = b"remote-cached".to_vec();
        remote_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "test")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "node-remote")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, cache_key.clone())
            .add_trait_config("remote_only", "yes");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let port = listener.local_addr().expect("listener local addr").port();
        let app = Router::new()
            .route(
                "/debug/cache/response/{cache_key}",
                get(remote_cached_response),
            )
            .with_state(RemoteResponseState {
                cache_key: cache_key.clone(),
                response: remote_response,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(port),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner(&cache_key, "test", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-local".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let response = downloader
            .download(request)
            .await
            .expect("remote cache hit should succeed");
        assert_eq!(response.content, b"remote-cached".to_vec());
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>("remote_only")
                .as_deref(),
            Some("yes")
        );
        let warmed = Response::sync(&cache_key, &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("local cache should be warmed");
        assert_eq!(warmed.content, b"remote-cached".to_vec());
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NAMESPACE_KEY)
                .as_deref(),
            Some("test")
        );
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NODE_ID_KEY)
                .as_deref(),
            Some("node-local")
        );
        let recorded_owner = downloader
            .profile_store
            .get_response_cache_owner(&cache_key)
            .expect("local warmed owner should be recorded");
        assert_eq!(recorded_owner.owner_namespace, "test");
        assert_eq!(recorded_owner.owner_node_id.as_deref(), Some("node-local"));
    }

    #[tokio::test]
    async fn remote_cache_hit_preserves_explicit_source_expiry_when_warming_local_cache() {
        use axum::extract::Path;
        use axum::routing::get;
        use axum::{Json, Router};
        use tokio::net::TcpListener;

        #[derive(Clone)]
        struct RemoteResponseState {
            cache_key: String,
            response: Response,
        }

        async fn remote_cached_response(
            axum::extract::State(state): axum::extract::State<RemoteResponseState>,
            Path(cache_key): Path<String>,
        ) -> Json<serde_json::Value> {
            assert_eq!(cache_key, state.cache_key);
            Json(serde_json::json!({
                "cache_key": state.cache_key,
                "response": state.response,
            }))
        }

        let lock_manager = Arc::new(DistributedLockManager::new(None, "remote_cache_hit_expiry"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "remote_cache_hit_expiry",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("remote_cache_hit_expiry").unwrap());

        let mut request = Request::new("http://example.com", "GET").enable_response_cache(true);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "module-a".to_string();
        let cache_key = request.hash();

        let explicit_expires_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
            + 10_000;
        let mut remote_response = sample_cached_response(Some(cache_key.clone()));
        remote_response.content = b"remote-explicit-expiry".to_vec();
        remote_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "test")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "node-remote")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, cache_key.clone())
            .add_trait_config(RESPONSE_CACHE_EXPIRES_AT_KEY, explicit_expires_at);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let port = listener.local_addr().expect("listener local addr").port();
        let app = Router::new()
            .route(
                "/debug/cache/response/{cache_key}",
                get(remote_cached_response),
            )
            .with_state(RemoteResponseState {
                cache_key: cache_key.clone(),
                response: remote_response,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(port),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner(&cache_key, "test", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "test",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );
        downloader
            .set_config(
                "remote_cache_hit_expiry",
                DownloadConfig {
                    enable_session: false,
                    enable_locker: false,
                    enable_rate_limit: true,
                    rate_limit: 10.0,
                    cache_ttl: 60,
                    default_timeout: 30,
                    downloader: "request_downloader".to_string(),
                },
            )
            .await;

        let response = downloader
            .download(request)
            .await
            .expect("remote cache hit should succeed");
        assert_eq!(response.content, b"remote-explicit-expiry".to_vec());
        assert_eq!(
            response
                .metadata
                .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY),
            Some(explicit_expires_at)
        );

        let warmed = Response::sync(&cache_key, &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("local cache should be warmed");
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY),
            Some(explicit_expires_at)
        );

        let recorded_owner = profile_store
            .get_response_cache_owner(&cache_key)
            .expect("local warmed owner should be recorded");
        assert_eq!(recorded_owner.expires_at, Some(explicit_expires_at));
    }

    #[tokio::test]
    async fn remote_cache_lookup_rejects_mismatched_response_payload() {
        use axum::extract::Path;
        use axum::routing::get;
        use axum::{Json, Router};
        use tokio::net::TcpListener;

        #[derive(Clone)]
        struct RemoteResponseState {
            cache_key: String,
            response: Response,
        }

        async fn remote_cached_response(
            axum::extract::State(state): axum::extract::State<RemoteResponseState>,
            Path(cache_key): Path<String>,
        ) -> Json<serde_json::Value> {
            assert_eq!(cache_key, state.cache_key);
            Json(serde_json::json!({
                "cache_key": state.cache_key,
                "response": state.response,
            }))
        }

        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "mismatched_remote_payload",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "mismatched_remote_payload",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store =
            Arc::new(ProfileControlPlaneStore::open_temp("mismatched_remote_payload").unwrap());

        let cache_key = "cache-key-1".to_string();
        let mut remote_response = sample_cached_response(Some("other-cache-key".to_string()));
        remote_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(
                RESPONSE_CACHE_OWNER_NAMESPACE_KEY,
                "mismatched_remote_payload",
            )
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "node-remote")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "other-cache-key")
            .add_trait_config("remote_only", "wrong");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let port = listener.local_addr().expect("listener local addr").port();
        let app = Router::new()
            .route(
                "/debug/cache/response/{cache_key}",
                get(remote_cached_response),
            )
            .with_state(RemoteResponseState {
                cache_key: cache_key.clone(),
                response: remote_response,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(port),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner(
                &cache_key,
                "mismatched_remote_payload",
                Some("node-remote"),
            )
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "mismatched_remote_payload",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_cached_response(&cache_key)
                .await
                .is_none()
        );
        assert!(
            Response::sync(&cache_key, &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner(&cache_key)
            .expect("mismatched payload should not clear owner record");
        assert_eq!(owner.owner_namespace, "mismatched_remote_payload");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-remote"));
    }

    #[tokio::test]
    async fn same_namespace_owner_without_node_id_skips_lookup_and_keeps_owner_record() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "missing_owner_node_id"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "missing_owner_node_id",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", None)
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_remote_cached_response("cache-key-1")
                .await
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert!(owner.owner_node_id.is_none());
    }

    #[tokio::test]
    async fn same_namespace_owner_without_api_port_skips_lookup_and_keeps_owner_record() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "missing_api_port"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "missing_api_port",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: None,
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_remote_cached_response("cache-key-1")
                .await
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-remote"));
    }

    #[tokio::test]
    async fn remote_cache_lookup_without_api_key_skips_lookup_and_keeps_owner_record() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "missing_api_key"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "missing_api_key",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(8080),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            None,
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-remote"));
    }

    #[tokio::test]
    async fn remote_cache_server_error_keeps_owner_record() {
        use axum::Router;
        use axum::http::StatusCode;
        use axum::routing::get;
        use tokio::net::TcpListener;

        async fn server_error() -> StatusCode {
            StatusCode::INTERNAL_SERVER_ERROR
        }

        let lock_manager = Arc::new(DistributedLockManager::new(None, "remote_cache_500"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "remote_cache_500",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let port = listener.local_addr().expect("listener local addr").port();
        let app = Router::new().route("/debug/cache/response/{cache_key}", get(server_error));
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(port),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-remote"));
    }

    #[tokio::test]
    async fn remote_cache_decode_failure_keeps_owner_record() {
        use axum::Router;
        use axum::response::IntoResponse;
        use axum::routing::get;
        use tokio::net::TcpListener;

        async fn invalid_json() -> impl IntoResponse {
            ([("content-type", "application/json")], "not-json")
        }

        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "remote_cache_decode_failure",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "remote_cache_decode_failure",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let port = listener.local_addr().expect("listener local addr").port();
        let app = Router::new().route("/debug/cache/response/{cache_key}", get(invalid_json));
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(port),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-remote"));
    }

    #[tokio::test]
    async fn remote_cache_connection_failure_keeps_owner_record() {
        use tokio::net::TcpListener;

        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "remote_cache_connect_failure",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "remote_cache_connect_failure",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let port = listener.local_addr().expect("listener local addr").port();
        drop(listener);

        profile_store
            .heartbeat_node(crate::common::registry::NodeInfo {
                id: "node-remote".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "remote-host".to_string(),
                api_port: Some(port),
                last_heartbeat: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                version: "0.2.16".to_string(),
            })
            .await
            .expect("heartbeat should succeed");
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-remote"));
    }

    #[tokio::test]
    async fn remote_cache_lookup_skips_foreign_namespace_owner_without_endpoint_mapping() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "foreign_owner"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "foreign_owner",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("foreign_owner").unwrap());
        profile_store
            .upsert_response_cache_owner("cache-key-1", "download-pool", Some("node-remote"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "origin-app",
            Some("node-local".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_remote_cached_response("cache-key-1")
                .await
                .is_none()
        );
        let owner = downloader
            .profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "download-pool");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-remote"));
    }

    #[tokio::test]
    async fn foreign_namespace_remote_cache_connection_failure_keeps_owner_record() {
        use std::collections::HashMap;
        use tokio::net::TcpListener;

        let lock_manager = Arc::new(DistributedLockManager::new(None, "foreign_connect_failure"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "foreign_connect_failure",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("download-pool").unwrap());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener local addr");
        drop(listener);

        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-a", Some("origin-node-1"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "download-pool",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        )
        .with_federation_response_cache_api_endpoints(HashMap::from([(
            "origin-a".to_string(),
            format!("http://{}:{}", addr.ip(), addr.port()),
        )]));

        assert!(
            downloader
                .try_load_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-a");
        assert_eq!(owner.owner_node_id.as_deref(), Some("origin-node-1"));
    }

    #[tokio::test]
    async fn stale_local_owner_record_is_cleared_when_local_cache_misses() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "stale_local_owner"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "stale_local_owner",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-local"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_remote_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
    }

    #[tokio::test]
    async fn inactive_same_namespace_owner_record_is_cleared() {
        let lock_manager = Arc::new(DistributedLockManager::new(None, "inactive_owner"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "inactive_owner",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-missing"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "origin-app",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        assert!(
            downloader
                .try_load_remote_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
    }

    #[tokio::test]
    async fn remote_cache_not_found_clears_stale_owner_record() {
        use axum::Router;
        use axum::http::StatusCode;
        use axum::routing::get;
        use std::collections::HashMap;
        use tokio::net::TcpListener;

        async fn not_found() -> StatusCode {
            StatusCode::NOT_FOUND
        }

        let lock_manager = Arc::new(DistributedLockManager::new(None, "stale_remote_owner"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "stale_remote_owner",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("download-pool").unwrap());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener local addr");
        let app = Router::new().route("/debug/cache/response/{cache_key}", get(not_found));
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-a", Some("origin-node-1"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service,
            "download-pool",
            Some("node-local".to_string()),
            profile_store.clone(),
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        )
        .with_federation_response_cache_api_endpoints(HashMap::from([(
            "origin-a".to_string(),
            format!("http://{}:{}", addr.ip(), addr.port()),
        )]));

        assert!(
            downloader
                .try_load_remote_cached_response("cache-key-1")
                .await
                .is_none()
        );
        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
    }

    #[tokio::test]
    async fn remote_cache_hit_fetches_foreign_namespace_response_via_configured_endpoint() {
        use axum::extract::Path;
        use axum::routing::get;
        use axum::{Json, Router};
        use std::collections::HashMap;
        use tokio::net::TcpListener;

        #[derive(Clone)]
        struct RemoteResponseState {
            cache_key: String,
            response: Response,
        }

        async fn remote_cached_response(
            axum::extract::State(state): axum::extract::State<RemoteResponseState>,
            Path(cache_key): Path<String>,
        ) -> Json<serde_json::Value> {
            assert_eq!(cache_key, state.cache_key);
            Json(serde_json::json!({
                "cache_key": state.cache_key,
                "response": state.response,
            }))
        }

        let lock_manager = Arc::new(DistributedLockManager::new(None, "foreign_namespace_hit"));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "foreign_namespace_hit",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("download-pool").unwrap());

        let mut request = Request::new("http://example.com", "GET").enable_response_cache(true);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "module-a".to_string();
        let cache_key = request.hash();

        let mut remote_response = sample_cached_response(Some(cache_key.clone()));
        remote_response.content = b"foreign-remote-cached".to_vec();
        remote_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "origin-a")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "origin-node-1")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, cache_key.clone())
            .add_trait_config("remote_only", "origin-a");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener local addr");
        let app = Router::new()
            .route(
                "/debug/cache/response/{cache_key}",
                get(remote_cached_response),
            )
            .with_state(RemoteResponseState {
                cache_key: cache_key.clone(),
                response: remote_response,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        profile_store
            .upsert_response_cache_owner(&cache_key, "origin-a", Some("origin-node-1"))
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "download-pool",
            Some("node-local".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        )
        .with_federation_response_cache_api_endpoints(HashMap::from([(
            "origin-a".to_string(),
            format!("http://{}:{}", addr.ip(), addr.port()),
        )]));

        let response = downloader
            .download(request)
            .await
            .expect("foreign remote cache hit should succeed");
        assert_eq!(response.content, b"foreign-remote-cached".to_vec());
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>("remote_only")
                .as_deref(),
            Some("origin-a")
        );

        let warmed = Response::sync(&cache_key, &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("foreign response should warm local cache");
        assert_eq!(warmed.content, b"foreign-remote-cached".to_vec());
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NAMESPACE_KEY)
                .as_deref(),
            Some("download-pool")
        );
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NODE_ID_KEY)
                .as_deref(),
            Some("node-local")
        );
        let recorded_owner = downloader
            .profile_store
            .get_response_cache_owner(&cache_key)
            .expect("local warmed owner should be recorded");
        assert_eq!(recorded_owner.owner_namespace, "download-pool");
        assert_eq!(recorded_owner.owner_node_id.as_deref(), Some("node-local"));
    }

    #[tokio::test]
    async fn remote_cache_hit_fetches_foreign_namespace_response_via_owner_endpoint() {
        use axum::extract::Path;
        use axum::routing::get;
        use axum::{Json, Router};
        use tokio::net::TcpListener;

        #[derive(Clone)]
        struct RemoteResponseState {
            cache_key: String,
            response: Response,
        }

        async fn remote_cached_response(
            axum::extract::State(state): axum::extract::State<RemoteResponseState>,
            Path(cache_key): Path<String>,
        ) -> Json<serde_json::Value> {
            assert_eq!(cache_key, state.cache_key);
            Json(serde_json::json!({
                "cache_key": state.cache_key,
                "response": state.response,
            }))
        }

        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "foreign_owner_endpoint_hit",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "foreign_owner_endpoint_hit",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("download-pool").unwrap());

        let mut request = Request::new("http://example.com", "GET").enable_response_cache(true);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "module-a".to_string();
        let cache_key = request.hash();

        let mut remote_response = sample_cached_response(Some(cache_key.clone()));
        remote_response.content = b"foreign-owner-endpoint".to_vec();
        remote_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "origin-a")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "origin-node-1")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, cache_key.clone())
            .add_trait_config("remote_only", "origin-a");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener local addr");
        let app = Router::new()
            .route(
                "/debug/cache/response/{cache_key}",
                get(remote_cached_response),
            )
            .with_state(RemoteResponseState {
                cache_key: cache_key.clone(),
                response: remote_response,
            });
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server should run");
        });

        let owner_api_base_url = format!("http://{}:{}", addr.ip(), addr.port());
        profile_store
            .upsert_response_cache_owner_with_details(
                &cache_key,
                "origin-a",
                Some("origin-node-1"),
                Some(&owner_api_base_url),
                None,
            )
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "download-pool",
            Some("node-local".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        );

        let response = downloader
            .download(request)
            .await
            .expect("foreign remote cache hit should succeed via owner endpoint");
        assert_eq!(response.content, b"foreign-owner-endpoint".to_vec());
        assert_eq!(
            response
                .metadata
                .get_trait_config::<String>("remote_only")
                .as_deref(),
            Some("origin-a")
        );

        let warmed = Response::sync(&cache_key, &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("foreign response should warm local cache");
        assert_eq!(warmed.content, b"foreign-owner-endpoint".to_vec());
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NAMESPACE_KEY)
                .as_deref(),
            Some("download-pool")
        );
    }

    #[tokio::test]
    async fn remote_cache_hit_falls_back_to_configured_endpoint_when_owner_endpoint_is_stale() {
        use axum::extract::Path;
        use axum::http::StatusCode;
        use axum::routing::get;
        use axum::{Json, Router};
        use std::collections::HashMap;
        use tokio::net::TcpListener;

        #[derive(Clone)]
        struct RemoteResponseState {
            cache_key: String,
            response: Response,
        }

        async fn missing_cached_response(Path(_cache_key): Path<String>) -> StatusCode {
            StatusCode::NOT_FOUND
        }

        async fn remote_cached_response(
            axum::extract::State(state): axum::extract::State<RemoteResponseState>,
            Path(cache_key): Path<String>,
        ) -> Json<serde_json::Value> {
            assert_eq!(cache_key, state.cache_key);
            Json(serde_json::json!({
                "cache_key": state.cache_key,
                "response": state.response,
            }))
        }

        let lock_manager = Arc::new(DistributedLockManager::new(
            None,
            "foreign_owner_endpoint_fallback",
        ));
        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            None,
            lock_manager.clone(),
            "foreign_owner_endpoint_fallback",
            RateLimitConfig::new(10.0),
        ));
        let cache_service = Arc::new(cache_service_config!(
            None,
            "test:cache".to_string(),
            None,
            None,
        ));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("download-pool").unwrap());

        let mut request = Request::new("http://example.com", "GET").enable_response_cache(true);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "module-a".to_string();
        let cache_key = request.hash();

        let stale_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stale listener should bind");
        let stale_addr = stale_listener
            .local_addr()
            .expect("stale listener local addr");
        let stale_app = Router::new().route(
            "/debug/cache/response/{cache_key}",
            get(missing_cached_response),
        );
        tokio::spawn(async move {
            axum::serve(stale_listener, stale_app)
                .await
                .expect("stale server should run");
        });

        let mut remote_response = sample_cached_response(Some(cache_key.clone()));
        remote_response.content = b"foreign-owner-endpoint-fallback".to_vec();
        remote_response.metadata = crate::common::model::meta::MetaData::default()
            .add_trait_config(RESPONSE_CACHE_OWNER_NAMESPACE_KEY, "origin-a")
            .add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, "origin-node-1")
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, cache_key.clone())
            .add_trait_config("remote_only", "origin-a");

        let mapping_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mapping listener should bind");
        let mapping_addr = mapping_listener
            .local_addr()
            .expect("mapping listener local addr");
        let mapping_app = Router::new()
            .route(
                "/debug/cache/response/{cache_key}",
                get(remote_cached_response),
            )
            .with_state(RemoteResponseState {
                cache_key: cache_key.clone(),
                response: remote_response,
            });
        tokio::spawn(async move {
            axum::serve(mapping_listener, mapping_app)
                .await
                .expect("mapping server should run");
        });

        profile_store
            .upsert_response_cache_owner_with_details(
                &cache_key,
                "origin-a",
                Some("origin-node-1"),
                Some(&format!("http://{}:{}", stale_addr.ip(), stale_addr.port())),
                None,
            )
            .await
            .expect("cache owner should be recorded");

        let downloader = test_downloader!(
            limiter,
            lock_manager,
            cache_service.clone(),
            "download-pool",
            Some("node-local".to_string()),
            profile_store,
            Some("local-dev".to_string()),
            200,
            1024 * 1024 * 10,
        )
        .with_federation_response_cache_api_endpoints(HashMap::from([(
            "origin-a".to_string(),
            format!("http://{}:{}", mapping_addr.ip(), mapping_addr.port()),
        )]));

        let response = downloader
            .download(request)
            .await
            .expect("foreign remote cache hit should fall back to configured endpoint");
        assert_eq!(
            response.content,
            b"foreign-owner-endpoint-fallback".to_vec()
        );

        let warmed = Response::sync(&cache_key, &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("foreign response should warm local cache");
        assert_eq!(
            warmed
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_NAMESPACE_KEY)
                .as_deref(),
            Some("download-pool")
        );
    }
}
