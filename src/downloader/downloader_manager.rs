use crate::cacheable::{CacheAble, CacheService};
use crate::common::model::download_config::DownloadConfig;
use crate::common::model::{Request, Response};
use crate::common::response_cache::{
    RESPONSE_CACHE_EXPIRES_AT_KEY, ResponseCachePersistRequest, current_owner_api_base_url,
    persist_response_cache_entry,
};
use crate::common::state::State;
use crate::downloader::request_downloader::{RequestDownloader, RequestDownloaderConfig};
use crate::downloader::{Downloader, WebSocketDownloader};
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use crate::errors::CacheError;
use dashmap::DashMap;
use log::warn;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const RESPONSE_CACHE_LOOKUP_KEY: &str = "response_cache_lookup_key";

fn cached_response_matches_owner_record(cache_key: &str, response: &Response) -> bool {
    if let Some(request_hash) = response.request_hash.as_deref()
        && request_hash != cache_key
    {
        return false;
    }

    if let Some(lookup_key) = response
        .metadata
        .get_trait_config::<String>(RESPONSE_CACHE_LOOKUP_KEY)
        .as_deref()
        && lookup_key != cache_key
    {
        return false;
    }

    true
}

fn current_node_response_cache_owner_api_base_url(
    profile_store: &Arc<ProfileControlPlaneStore>,
    owner_node_id: Option<&str>,
) -> Option<String> {
    current_owner_api_base_url(
        profile_store,
        owner_node_id,
        std::time::Duration::from_secs(30),
    )
}

async fn cleanup_local_response_cache_owner_records(
    cache_service: &Arc<CacheService>,
    profile_store: &Arc<ProfileControlPlaneStore>,
    owner_namespace: &str,
    owner_node_id: Option<&str>,
) {
    let owned_records: Vec<_> = profile_store
        .list_response_cache_owners()
        .into_iter()
        .filter(|record| {
            record.owner_namespace == owner_namespace
                && record.owner_node_id.as_deref() == owner_node_id
        })
        .collect();

    for record in owned_records {
        let expired = record.expires_at.is_some_and(|expires_at| {
            expires_at
                <= SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64
        });
        if expired && let Err(err) = Response::delete(&record.cache_key, cache_service).await {
            warn!(
                "Failed to delete expired local cached response during owner cleanup: cache_key={} error={:?}",
                record.cache_key, err
            );
        }

        let should_clear_owner = match Response::sync(&record.cache_key, cache_service).await {
            Ok(Some(response)) => {
                if expired {
                    true
                } else if cached_response_matches_owner_record(&record.cache_key, &response) {
                    let expected_owner_api_base_url =
                        current_node_response_cache_owner_api_base_url(
                            profile_store,
                            owner_node_id,
                        );
                    if record.owner_api_base_url != expected_owner_api_base_url {
                        let mut refreshed_response = response.clone();
                        if refreshed_response
                            .metadata
                            .get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY)
                            .is_none()
                            && let Some(expires_at) = record.expires_at
                        {
                            refreshed_response.metadata = refreshed_response
                                .metadata
                                .add_trait_config(RESPONSE_CACHE_EXPIRES_AT_KEY, expires_at);
                        }

                        if persist_response_cache_entry(ResponseCachePersistRequest {
                            response: &refreshed_response,
                            owner_namespace,
                            owner_node_id,
                            owner_api_base_url: expected_owner_api_base_url.as_deref(),
                            fallback_ttl: None,
                            cache_service,
                            profile_store,
                            context: "background_owner_endpoint_refresh",
                        })
                        .await
                        .is_none()
                        {
                            warn!(
                                "Failed to refresh response cache owner endpoint during owner cleanup: cache_key={} owner_namespace={} owner_node_id={:?} expected_owner_api_base_url={:?}",
                                record.cache_key,
                                owner_namespace,
                                owner_node_id,
                                expected_owner_api_base_url
                            );
                        }
                    }
                    false
                } else {
                    warn!(
                        "Removing stale response cache owner after local cache key mismatch: cache_key={} owner_namespace={} owner_node_id={:?}",
                        record.cache_key, record.owner_namespace, record.owner_node_id
                    );
                    if let Err(err) = Response::delete(&record.cache_key, cache_service).await {
                        warn!(
                            "Failed to delete mismatched local cached response during owner cleanup: cache_key={} error={:?}",
                            record.cache_key, err
                        );
                    }
                    true
                }
            }
            Ok(None) => true,
            Err(CacheError::Serde(err)) => {
                warn!(
                    "Removing stale response cache owner after local cache decode failure: cache_key={} owner_namespace={} owner_node_id={:?} error={:?}",
                    record.cache_key, record.owner_namespace, record.owner_node_id, err
                );
                if let Err(delete_err) = Response::delete(&record.cache_key, cache_service).await {
                    warn!(
                        "Failed to delete corrupt local cached response during owner cleanup: cache_key={} error={:?}",
                        record.cache_key, delete_err
                    );
                }
                true
            }
            Err(err) => {
                warn!(
                    "Skipping local response cache owner cleanup after cache read failure: cache_key={} owner_namespace={} owner_node_id={:?} error={:?}",
                    record.cache_key, record.owner_namespace, record.owner_node_id, err
                );
                false
            }
        };

        if !should_clear_owner {
            continue;
        }

        if let Err(err) = profile_store
            .remove_response_cache_owner_if_matches(
                &record.cache_key,
                &record.owner_namespace,
                record.owner_node_id.as_deref(),
            )
            .await
        {
            warn!(
                "Failed to clear stale response cache owner during background cleanup: cache_key={} owner_namespace={} owner_node_id={:?} error={:?}",
                record.cache_key, record.owner_namespace, record.owner_node_id, err
            );
        }
    }
}

pub struct DownloaderManager {
    pub state: Arc<State>,
    pub default_downloader: Box<dyn Downloader>,
    // Registered downloader factories.
    pub downloader: Arc<DashMap<String, Box<dyn Downloader>>>,
    // Task downloader configuration.
    pub config: Arc<DashMap<String, DownloadConfig>>,
    // Task downloader instances.
    pub task_downloader: Arc<DashMap<String, Box<dyn Downloader>>>,
    pub wss_downloader: Arc<WebSocketDownloader>,
    // Records the last expiration update timestamp to reduce churn.
    pub expire_update_cache: Arc<DashMap<String, u64>>,
}

impl DownloaderManager {
    /// Create a new DownloaderManager instance.
    ///
    /// # Arguments
    /// * `state` - Shared application state containing configuration, rate limiter, etc.
    pub async fn new(state: Arc<State>) -> Self {
        let (pool_size, max_response_size) = {
            let cfg = state.config.read().await;
            (
                cfg.download_config.pool_size.unwrap_or(200),
                cfg.download_config
                    .max_response_size
                    .unwrap_or(10 * 1024 * 1024),
            )
        };
        let (namespace, node_id) = {
            let cfg = state.config.read().await;
            (cfg.name.clone(), cfg.crawler.node_id.clone())
        };
        let api_key = {
            let cfg = state.config.read().await;
            cfg.api.as_ref().and_then(|api| api.api_key.clone())
        };
        let federation_response_cache_api_endpoints = {
            let cfg = state.config.read().await;
            cfg.channel_config
                .federation_response_cache_api_endpoints
                .clone()
                .into_iter()
                .collect()
        };

        DownloaderManager {
            state: state.clone(),
            default_downloader: Box::new(
                RequestDownloader::new(RequestDownloaderConfig {
                    limiter: Arc::clone(&state.limiter),
                    locker: Arc::clone(&state.locker),
                    cache_service: Arc::clone(&state.cache_service),
                    owner_namespace: namespace,
                    owner_node_id: node_id,
                    profile_store: Arc::clone(&state.profile_store),
                    api_key,
                    pool_size,
                    max_response_size,
                })
                .with_federation_response_cache_api_endpoints(
                    federation_response_cache_api_endpoints,
                ),
            ),
            // Downloader factory list.
            downloader: Arc::new(DashMap::new()),
            // Task downloader configuration.
            config: Arc::new(DashMap::new()),
            // Task downloader instances.
            task_downloader: Arc::new(DashMap::new()),
            wss_downloader: Arc::new(WebSocketDownloader::new()),
            expire_update_cache: Arc::new(DashMap::new()),
        }
    }

    /// Register a custom downloader implementation.
    ///
    /// The downloader will be available to use by its `name()`.
    pub async fn register(&self, downloader: Box<dyn Downloader>) {
        self.downloader.insert(downloader.name(), downloader);
    }

    /// Set rate limit for a specific limit_id dynamically.
    ///
    /// This updates the rate limit configuration for an active downloader instance.
    pub async fn set_limit(&self, limit_id: &str, limit: f32) {
        let downloader = self
            .task_downloader
            .get(limit_id)
            .map(|d| dyn_clone::clone_box(d.value().as_ref()));
        // guard dropped here — never hold DashMap Ref across .await
        if let Some(d) = downloader {
            d.set_limit(limit_id, limit).await;
        }
    }

    // Helper function to get the current timestamp.
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    // Cleans up expired downloaders.
    async fn cleanup_expired_downloader(&self) {
        let downloader_expire_time = self
            .state
            .config
            .read()
            .await
            .download_config
            .downloader_expire;

        let current_time = Self::current_timestamp();
        let max_score = current_time.saturating_sub(downloader_expire_time);

        let expired_keys: Vec<String> = self
            .expire_update_cache
            .iter()
            .filter_map(|entry| {
                if *entry.value() <= max_score {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        for key in &expired_keys {
            self.task_downloader.remove(key);
            self.config.remove(key);
            self.expire_update_cache.remove(key);
        }

        // Check health status.
        let check_list: Vec<(String, Box<dyn Downloader>)> = self
            .task_downloader
            .iter()
            .map(|r| (r.key().clone(), dyn_clone::clone_box(r.value().as_ref())))
            .collect();

        for (key, downloader) in check_list {
            if downloader.health_check().await.is_err() {
                self.task_downloader.remove(&key);
                self.config.remove(&key);
                self.expire_update_cache.remove(&key);
            }
        }
    }

    async fn cleanup_stale_response_cache_owners(&self) {
        let (namespace, node_id) = {
            let cfg = self.state.config.read().await;
            (cfg.name.clone(), cfg.crawler.node_id.clone())
        };

        cleanup_local_response_cache_owner_records(
            &self.state.cache_service,
            &self.state.profile_store,
            &namespace,
            node_id.as_deref(),
        )
        .await;
    }

    /// Start the background cleanup task.
    /// Should be called once at startup.
    pub fn start_background_cleaner(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                self.cleanup_expired_downloader().await;
                self.cleanup_stale_response_cache_owners().await;
            }
        });
    }

    /// Get or create a downloader for the given request.
    ///
    /// This method manages the lifecycle of downloader instances for specific tasks (modules).
    /// It handles creation, configuration updates, and expiration of idle downloaders.
    ///
    /// # Arguments
    /// * `request` - The request containing module ID and limit ID.
    /// * `download_config` - Configuration for the downloader.
    pub async fn get_downloader(
        &self,
        request: &Request,
        download_config: DownloadConfig,
    ) -> Box<dyn Downloader> {
        let current_time = Self::current_timestamp();
        let module_id = request.module_id();

        // Check whether expiration time needs to be updated (once every 60 seconds).
        let should_update = if let Some(last_update) = self.expire_update_cache.get(&module_id) {
            current_time > *last_update + 60
        } else {
            true
        };

        if should_update {
            // Optimistically update cache to prevent spamming the spawn
            self.expire_update_cache
                .insert(module_id.clone(), current_time);
        }

        // Get or insert configuration.
        // Extract the cached value and drop the DashMap guard BEFORE any
        // potential .insert() on the same map — holding a Ref (read guard)
        // while calling .insert() (write lock) on the same shard is a
        // parking_lot self-deadlock.
        let cached_config = self.config.get(&module_id).map(|existing| existing.clone());
        // guard dropped here
        let config = if let Some(cached) = cached_config {
            if cached != download_config {
                self.config
                    .insert(module_id.clone(), download_config.clone());
                download_config.clone()
            } else {
                cached
            }
        } else {
            self.config
                .insert(module_id.clone(), download_config.clone());
            download_config.clone()
        };

        // Determine effective limit_id here to ensure rate limiter gets correct key
        let limit_id = if request.limit_id.is_empty() {
            request.module_id()
        } else {
            request.limit_id.clone()
        };

        // Get or create downloader.
        // Clone the cached downloader and drop the DashMap guard before any .await.
        // Holding a DashMap Ref across an await can deadlock the Tokio runtime
        // (parking_lot RwLock blocks the OS thread).
        let cached_downloader = self
            .task_downloader
            .get(&module_id)
            .map(|d| dyn_clone::clone_box(d.value().as_ref()));
        // guard dropped here
        if let Some(d) = cached_downloader {
            d.set_config(&limit_id, config).await;
            return d;
        }

        let new_downloader = {
            if let Some(registered) = self.downloader.get(&config.downloader) {
                dyn_clone::clone_box(registered.value().as_ref())
            } else {
                dyn_clone::clone_box(self.default_downloader.as_ref())
            }
        };

        // Ensure the configuration is up to date.
        new_downloader.set_config(&limit_id, config).await;
        self.task_downloader.insert(
            module_id.clone(),
            dyn_clone::clone_box(new_downloader.as_ref()),
        );
        new_downloader
    }
    /// Clear all configurations and downloaders.
    ///
    /// This removes all registered task downloaders and their configurations.
    pub async fn clear(&self) {
        self.config.clear();
        self.task_downloader.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::cleanup_local_response_cache_owner_records;
    use crate::cacheable::{CacheAble, CacheService, CacheServiceConfig};
    use crate::common::model::meta::MetaData;
    use crate::common::model::{Cookies, ExecutionMark, Response};
    use crate::common::registry::NodeInfo;
    use crate::common::response_cache::RESPONSE_CACHE_OWNER_API_BASE_URL_KEY;
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use uuid::Uuid;

    const RESPONSE_CACHE_LOOKUP_KEY: &str = "response_cache_lookup_key";

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
            metadata: MetaData::default(),
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
    async fn cleanup_local_response_cache_owner_records_removes_missing_entries() {
        let cache_service = Arc::new(CacheService::new(CacheServiceConfig::local("test:cache")));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-local"))
            .await
            .expect("cache owner should be recorded");

        cleanup_local_response_cache_owner_records(
            &cache_service,
            &profile_store,
            "origin-app",
            Some("node-local"),
        )
        .await;

        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cleanup_local_response_cache_owner_records_removes_corrupt_entries() {
        let cache_service = Arc::new(CacheService::new(CacheServiceConfig::local("test:cache")));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-local"))
            .await
            .expect("cache owner should be recorded");
        cache_service
            .set(
                &Response::cache_id("cache-key-1", &cache_service),
                b"not-json",
                None,
            )
            .await
            .expect("corrupt cache entry should be stored");

        cleanup_local_response_cache_owner_records(
            &cache_service,
            &profile_store,
            "origin-app",
            Some("node-local"),
        )
        .await;

        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cleanup_local_response_cache_owner_records_removes_mismatched_entries() {
        let cache_service = Arc::new(CacheService::new(CacheServiceConfig::local("test:cache")));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-local"))
            .await
            .expect("cache owner should be recorded");

        let mut response = sample_cached_response(Some("other-cache-key".to_string()));
        response.metadata = response
            .metadata
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "other-cache-key");
        response
            .send_persistent("cache-key-1", &cache_service)
            .await
            .expect("cached response should be stored");

        cleanup_local_response_cache_owner_records(
            &cache_service,
            &profile_store,
            "origin-app",
            Some("node-local"),
        )
        .await;

        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cleanup_local_response_cache_owner_records_preserves_valid_entries() {
        let cache_service = Arc::new(CacheService::new(CacheServiceConfig::local("test:cache")));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        profile_store
            .upsert_response_cache_owner("cache-key-1", "origin-app", Some("node-local"))
            .await
            .expect("cache owner should be recorded");

        let mut response = sample_cached_response(Some("cache-key-1".to_string()));
        response.metadata = response
            .metadata
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "cache-key-1");
        response
            .send_persistent("cache-key-1", &cache_service)
            .await
            .expect("cached response should be stored");

        cleanup_local_response_cache_owner_records(
            &cache_service,
            &profile_store,
            "origin-app",
            Some("node-local"),
        )
        .await;

        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_namespace, "origin-app");
        assert_eq!(owner.owner_node_id.as_deref(), Some("node-local"));
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_some()
        );
    }

    #[tokio::test]
    async fn cleanup_local_response_cache_owner_records_removes_expired_entries() {
        let cache_service = Arc::new(CacheService::new(CacheServiceConfig::local("test:cache")));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());

        profile_store
            .upsert_response_cache_owner_with_expiry(
                "cache-key-1",
                "origin-app",
                Some("node-local"),
                Some(1),
            )
            .await
            .expect("cache owner should be recorded");

        let mut response = sample_cached_response(Some("cache-key-1".to_string()));
        response.metadata = response
            .metadata
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "cache-key-1");
        response
            .send_persistent("cache-key-1", &cache_service)
            .await
            .expect("cached response should be stored");

        cleanup_local_response_cache_owner_records(
            &cache_service,
            &profile_store,
            "origin-app",
            Some("node-local"),
        )
        .await;

        assert!(
            profile_store
                .get_response_cache_owner("cache-key-1")
                .is_none()
        );
        assert!(
            Response::sync("cache-key-1", &cache_service)
                .await
                .expect("cache lookup should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn cleanup_local_response_cache_owner_records_refreshes_owner_endpoint_from_heartbeat() {
        let cache_service = Arc::new(CacheService::new(CacheServiceConfig::local("test:cache")));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .heartbeat_node(NodeInfo {
                id: "node-local".to_string(),
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

        profile_store
            .upsert_response_cache_owner_with_details(
                "cache-key-1",
                "origin-app",
                Some("node-local"),
                None,
                None,
            )
            .await
            .expect("cache owner should be recorded");

        let mut response = sample_cached_response(Some("cache-key-1".to_string()));
        response.metadata = response
            .metadata
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "cache-key-1");
        response
            .send_persistent("cache-key-1", &cache_service)
            .await
            .expect("cached response should be stored");

        cleanup_local_response_cache_owner_records(
            &cache_service,
            &profile_store,
            "origin-app",
            Some("node-local"),
        )
        .await;

        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(
            owner.owner_api_base_url.as_deref(),
            Some("http://127.0.0.1:18080")
        );

        let cached = Response::sync("cache-key-1", &cache_service)
            .await
            .expect("cache lookup should succeed")
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
    async fn cleanup_local_response_cache_owner_records_clears_stale_owner_endpoint_when_api_port_missing()
     {
        let cache_service = Arc::new(CacheService::new(CacheServiceConfig::local("test:cache")));
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("origin-app").unwrap());
        profile_store
            .heartbeat_node(NodeInfo {
                id: "node-local".to_string(),
                ip: "127.0.0.1".to_string(),
                hostname: "host-a".to_string(),
                api_port: None,
                last_heartbeat: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                version: "test".to_string(),
            })
            .await
            .expect("node heartbeat should be recorded");

        profile_store
            .upsert_response_cache_owner_with_details(
                "cache-key-1",
                "origin-app",
                Some("node-local"),
                Some("http://127.0.0.1:18080"),
                None,
            )
            .await
            .expect("cache owner should be recorded");

        let mut response = sample_cached_response(Some("cache-key-1".to_string()));
        response.metadata = response
            .metadata
            .add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, "cache-key-1")
            .add_trait_config(
                RESPONSE_CACHE_OWNER_API_BASE_URL_KEY,
                "http://127.0.0.1:18080",
            );
        response
            .send_persistent("cache-key-1", &cache_service)
            .await
            .expect("cached response should be stored");

        cleanup_local_response_cache_owner_records(
            &cache_service,
            &profile_store,
            "origin-app",
            Some("node-local"),
        )
        .await;

        let owner = profile_store
            .get_response_cache_owner("cache-key-1")
            .expect("owner record should be preserved");
        assert_eq!(owner.owner_api_base_url, None);

        let cached = Response::sync("cache-key-1", &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("cached response should exist");
        assert_eq!(
            cached
                .metadata
                .get_trait_config::<String>(RESPONSE_CACHE_OWNER_API_BASE_URL_KEY),
            None::<String>
        );
    }
}
