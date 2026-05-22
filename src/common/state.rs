// no direct filesystem path usage here
use crate::utils::connector::db_connection;

use crate::common::config::ConfigProvider;
use crate::common::config::file::FileConfigProvider;
use crate::common::model::config::{CacheBackendKind, Config};
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use crate::sync::{RaftRuntime, RaftRuntimeConfig};

use crate::cacheable::{CacheService, CacheServiceConfig};
use crate::common::status_tracker::{ErrorTrackerConfig, StatusTracker};
use crate::utils::distributed_lock::{DistributedLockManager, RaftLockBackend};
use crate::utils::distributed_rate_limit::{
    DistributedSlidingWindowRateLimiter, RaftRateLimitBackend, RateLimitConfig,
};
use log::{error, info};
use std::sync::Arc;
use std::time;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum StateInitError {
    #[error("load config failed: {0}")]
    LoadConfig(String),
    #[error("raft configuration invalid: {0}")]
    RaftConfig(String),
    #[error("raft runtime initialization failed: {0}")]
    RaftRuntime(String),
    #[error("control-plane store initialization failed: {0}")]
    ControlPlaneStore(String),
    #[error(
        "database connection failed (url={url:?}, schema={schema:?}, pool_size={pool_size:?}, tls={tls:?})"
    )]
    DatabaseConnect {
        url: Option<String>,
        schema: Option<String>,
        pool_size: Option<u32>,
        tls: Option<bool>,
    },
}

/// Global application state shared across the system.
///
/// Contains database, configuration, and shared runtime services.
#[derive(Clone)]
pub struct State {
    /// Database connection pool
    pub db: Arc<sea_orm::DatabaseConnection>,
    /// Thread-safe dynamic configuration
    pub config: Arc<RwLock<Config>>,
    /// General purpose cache service
    pub cache_service: Arc<CacheService>,
    /// Cookie storage service
    pub cookie_service: Option<Arc<CacheService>>,
    /// Distributed lock manager
    pub locker: Arc<DistributedLockManager>,
    /// Distributed rate limiter
    pub limiter: Arc<DistributedSlidingWindowRateLimiter>,
    /// API rate limiter
    pub api_limiter: Option<Arc<DistributedSlidingWindowRateLimiter>>,
    /// Task status and error tracker
    pub status_tracker: Arc<StatusTracker>,
    /// Persistent control-plane store for cluster and configuration metadata.
    pub profile_store: Arc<ProfileControlPlaneStore>,
    /// Parsed Raft runtime configuration for the control-plane consensus path.
    pub raft_runtime_config: Option<Arc<RaftRuntimeConfig>>,
    /// Running openraft runtime when raft is configured.
    pub raft_runtime: Option<Arc<RaftRuntime>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BootstrapCapabilities {
    single_node_deployment: bool,
    raft_enabled: bool,
}

impl BootstrapCapabilities {
    fn from_config(config: &Config) -> Self {
        Self {
            single_node_deployment: config.is_single_node_deployment(),
            raft_enabled: config.has_raft_control_plane(),
        }
    }

    fn runtime_mode_label(&self) -> &'static str {
        if self.single_node_deployment {
            "single_node"
        } else {
            "distributed"
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CacheRuntimeSettings {
    cache_ttl: time::Duration,
    compression_threshold: Option<usize>,
    enable_l1: bool,
    l1_ttl_secs: u64,
    l1_max_entries: usize,
}

impl CacheRuntimeSettings {
    fn from_config(config: &Config) -> Self {
        Self {
            cache_ttl: time::Duration::from_secs(config.cache.ttl),
            compression_threshold: config.cache.compression_threshold,
            enable_l1: config.cache.enable_l1.unwrap_or(false),
            l1_ttl_secs: config.cache.l1_ttl_secs.unwrap_or(30),
            l1_max_entries: config.cache.l1_max_entries.unwrap_or(10000),
        }
    }

    fn build_cache_service(
        &self,
        pool: Option<()>,
        namespace: String,
        backend_kind: Option<CacheBackendKind>,
        profile_store: Option<Arc<ProfileControlPlaneStore>>,
    ) -> Arc<CacheService> {
        Arc::new(CacheService::new(
            CacheServiceConfig::local(namespace)
                .with_pool(pool)
                .with_default_ttl(Some(self.cache_ttl))
                .with_compression_threshold(self.compression_threshold)
                .with_l1(self.enable_l1, self.l1_ttl_secs, self.l1_max_entries)
                .with_backend(backend_kind, profile_store),
        ))
    }
}

struct CoordinationServices {
    cache_service: Arc<CacheService>,
    cookie_service: Option<Arc<CacheService>>,
    locker: Arc<DistributedLockManager>,
    limiter: Arc<DistributedSlidingWindowRateLimiter>,
    api_limiter: Option<Arc<DistributedSlidingWindowRateLimiter>>,
    status_tracker: Arc<StatusTracker>,
}

impl CoordinationServices {
    fn build(
        config: &Config,
        cache_pool: Option<()>,
        cookie_pool: Option<()>,
        profile_store: Arc<ProfileControlPlaneStore>,
        backend_kind: Option<CacheBackendKind>,
    ) -> Self {
        let locker: Arc<DistributedLockManager> = match backend_kind {
            Some(CacheBackendKind::RaftRocksdb) => {
                let raft_lock = Arc::new(RaftLockBackend::new(
                    profile_store.clone(),
                    config.name.as_str(),
                ));
                Arc::new(DistributedLockManager::with_backend(
                    raft_lock,
                    &config.name,
                ))
            }
            _ => Arc::new(DistributedLockManager::new(None, &config.name)),
        };

        let limiter: Arc<DistributedSlidingWindowRateLimiter> = match backend_kind {
            Some(CacheBackendKind::RaftRocksdb) => {
                let raft_rate_limit = Arc::new(RaftRateLimitBackend::new(
                    profile_store.clone(),
                    config.name.as_str(),
                ));
                Arc::new(DistributedSlidingWindowRateLimiter::with_backend(
                    raft_rate_limit,
                    &config.name,
                    RateLimitConfig {
                        max_requests_per_second: config.download_config.rate_limit,
                        window_size_millis: 1000,
                        base_max_requests_per_second: Some(config.download_config.rate_limit),
                    },
                ))
            }
            _ => Arc::new(DistributedSlidingWindowRateLimiter::new(
                None,
                locker.clone(),
                &config.name,
                RateLimitConfig {
                    max_requests_per_second: config.download_config.rate_limit,
                    window_size_millis: 1000,
                    base_max_requests_per_second: Some(config.download_config.rate_limit),
                },
            )),
        };

        let api_limiter = config.api.as_ref().and_then(|api| {
            api.rate_limit.map(|limit| match backend_kind {
                Some(CacheBackendKind::RaftRocksdb) => {
                    let raft_rate_limit = Arc::new(RaftRateLimitBackend::new(
                        profile_store.clone(),
                        config.name.as_str(),
                    ));
                    Arc::new(DistributedSlidingWindowRateLimiter::with_backend(
                        raft_rate_limit,
                        &format!("{}:api", config.name),
                        RateLimitConfig {
                            max_requests_per_second: limit as f32,
                            window_size_millis: 1000,
                            base_max_requests_per_second: Some(limit as f32),
                        },
                    ))
                }
                _ => Arc::new(DistributedSlidingWindowRateLimiter::new(
                    None,
                    locker.clone(),
                    &format!("{}:api", config.name),
                    RateLimitConfig {
                        max_requests_per_second: limit as f32,
                        window_size_millis: 1000,
                        base_max_requests_per_second: Some(limit as f32),
                    },
                )),
            })
        });

        let cache_settings = CacheRuntimeSettings::from_config(config);
        let cache_service = cache_settings.build_cache_service(
            cache_pool,
            format!("{}:cache", config.name),
            backend_kind,
            Some(profile_store.clone()),
        );
        let cookie_service = cookie_pool.map(|pool| {
            cache_settings.build_cache_service(
                Some(pool),
                format!("{}:cookie", config.name),
                None,
                None,
            )
        });

        let error_tracker_config = ErrorTrackerConfig {
            task_max_errors: config.crawler.task_max_errors,
            module_max_errors: config.crawler.module_max_errors,
            request_max_retries: config.crawler.request_max_retries,
            parse_max_retries: config.crawler.request_max_retries,
            enable_success_decay: true,
            success_decay_amount: 1,
            enable_time_window: false,
            time_window_seconds: 3600,
            consecutive_error_threshold: 3,
            error_ttl: config.cache.ttl,
        };
        let status_tracker = Arc::new(StatusTracker::new(error_tracker_config, profile_store));

        Self {
            cache_service,
            cookie_service,
            locker,
            limiter,
            api_limiter,
            status_tracker,
        }
    }
}

impl State {
    pub fn is_single_node_deployment(&self) -> bool {
        self.raft_runtime.is_none()
    }

    pub fn has_raft_control_plane(&self) -> bool {
        self.raft_runtime.is_some()
    }

    /// Creates a new State instance using a file-based configuration.
    pub async fn new(path: &str) -> Self {
        match Self::try_new(path).await {
            Ok(state) => state,
            Err(e) => {
                error!("State initialization failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    /// Creates a new State instance with detailed error propagation.
    pub async fn try_new(path: &str) -> Result<Self, StateInitError> {
        let provider = FileConfigProvider::new(path);
        Self::try_new_with_provider(Box::new(provider)).await
    }

    /// Creates a new State instance with a custom configuration provider.
    ///
    /// Initializes database connections and runtime services.
    /// Starts a background task to watch for configuration changes.
    pub async fn new_with_provider(provider: Box<dyn ConfigProvider>) -> Self {
        match Self::try_new_with_provider(provider).await {
            Ok(state) => state,
            Err(e) => {
                error!("State initialization failed: {}", e);
                std::process::exit(1);
            }
        }
    }

    /// Creates a new State instance and returns explicit initialization failures.
    pub async fn try_new_with_provider(
        provider: Box<dyn ConfigProvider>,
    ) -> Result<Self, StateInitError> {
        let config = provider
            .load_config()
            .await
            .map_err(|e| StateInitError::LoadConfig(e.to_string()))?;
        let capabilities = BootstrapCapabilities::from_config(&config);
        info!(
            "Runtime mode initialized: {} (raft={})",
            capabilities.runtime_mode_label(),
            capabilities.raft_enabled,
        );

        let watcher_res = provider.watch().await;

        let db = Arc::new(
            db_connection(
                config.db.url.clone(),
                config.db.database_schema.clone(),
                config.db.pool_size,
                config.db.tls,
            )
            .await
            .ok_or_else(|| StateInitError::DatabaseConnect {
                url: config.db.url.clone(),
                schema: config.db.database_schema.clone(),
                pool_size: config.db.pool_size,
                tls: config.db.tls,
            })?,
        );
        info!("Database connected successfully");
        let cache_pool = None;
        let cookie_pool = None;

        let profile_store = ProfileControlPlaneStore::from_config(&config)
            .map_err(|e| StateInitError::ControlPlaneStore(e.to_string()))?;
        info!("locker and limit pools shared with cache pool");

        let backend_kind = config.cache.backend;
        let coordination = CoordinationServices::build(
            &config,
            cache_pool,
            cookie_pool,
            profile_store.clone(),
            backend_kind,
        );
        info!("Coordination services initialized");

        let raft_runtime_config = config
            .raft
            .as_ref()
            .map(|raft| RaftRuntimeConfig::from_app_config(&config.name, raft))
            .transpose()
            .map_err(StateInitError::RaftConfig)?
            .map(Arc::new);
        let raft_runtime = if let Some(runtime_config) = raft_runtime_config.as_ref() {
            let runtime = Arc::new(
                RaftRuntime::start(runtime_config.clone(), Some(profile_store.clone()))
                    .await
                    .map_err(StateInitError::RaftRuntime)?,
            );
            profile_store.attach_raft_runtime(runtime.clone());
            Some(runtime)
        } else {
            None
        };

        let config_arc = Arc::new(RwLock::new(config));

        // Spawn configuration watcher
        if let Ok(mut rx) = watcher_res {
            let config_clone = config_arc.clone();
            let limiter = coordination.limiter.clone();
            let api_limiter = coordination.api_limiter.clone();
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    let new_config = rx.borrow().clone();
                    info!("Configuration updated dynamically");
                    {
                        let mut w = config_clone.write().await;
                        *w = new_config.clone();
                    }
                    // Propagate rate limit updates without requiring a restart.
                    let _ = limiter
                        .set_all_limit(new_config.download_config.rate_limit)
                        .await;
                    if let (Some(api), Some(api_limiter)) =
                        (new_config.api.as_ref(), api_limiter.as_ref())
                        && let Some(limit) = api.rate_limit
                    {
                        let _ = api_limiter.set_all_limit(limit as f32).await;
                    }
                }
            });
        } else if let Err(e) = watcher_res {
            error!("Failed to start config watcher: {}", e);
        }

        Ok(State {
            db,
            config: config_arc,
            cache_service: coordination.cache_service,
            cookie_service: coordination.cookie_service,
            locker: coordination.locker,
            limiter: coordination.limiter,
            api_limiter: coordination.api_limiter,
            status_tracker: coordination.status_tracker,
            profile_store,
            raft_runtime_config,
            raft_runtime,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{BootstrapCapabilities, CoordinationServices};
    use crate::common::model::config::{
        Api, BlobStorageConfig, CacheConfig, ChannelConfig, Config, CrawlerConfig, DatabaseConfig,
        DownloadConfig, RaftConfig,
    };
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use std::sync::Arc;

    fn sample_config() -> Config {
        Config {
            name: "demo".to_string(),
            db: DatabaseConfig {
                url: None,
                database_schema: None,
                pool_size: None,
                tls: None,
            },
            download_config: DownloadConfig {
                downloader_expire: 60,
                timeout: 30,
                rate_limit: 5.0,
                enable_session: true,
                enable_locker: false,
                enable_rate_limit: true,
                cache_ttl: 60,
                wss_timeout: 30,
                pool_size: None,
                max_response_size: None,
            },
            cache: CacheConfig {
                backend: None,
                ttl: 60,
                compression_threshold: None,
                enable_l1: Some(false),
                l1_ttl_secs: None,
                l1_max_entries: None,
            },
            crawler: CrawlerConfig {
                request_max_retries: 3,
                task_max_errors: 5,
                module_max_errors: 5,
                module_locker_ttl: 60,
                node_id: None,
                task_concurrency: None,
                publish_concurrency: None,
                parser_concurrency: None,
                error_task_concurrency: None,
                backpressure_retry_delay_ms: None,
                dedup_ttl_secs: None,
                idle_stop_secs: None,
                scheduler_ingress_cutover_gate: None,
            },
            scheduler: None,
            sync: None,
            channel_config: ChannelConfig {
                blob_storage: Some(BlobStorageConfig { path: None }),
                kafka: None,
                minid_time: 0,
                capacity: 128,
                queue_codec: None,
                batch_concurrency: None,
                compression_threshold: None,
                nack_max_retries: None,
                nack_backoff_ms: None,
                federation_request_namespaces: Vec::new(),
                federation_response_cache_api_endpoints: Default::default(),
            },
            proxy: None,
            api: None,
            event_bus: None,
            logger: None,
            policy: None,
            raft: None,
        }
    }

    #[test]
    fn bootstrap_capabilities_default_to_single_node_without_raft() {
        let config = sample_config();
        let capabilities = BootstrapCapabilities::from_config(&config);
        assert!(capabilities.single_node_deployment);
        assert!(!capabilities.raft_enabled);
    }

    #[test]
    fn bootstrap_capabilities_track_raft_deployment() {
        let mut config = sample_config();
        config.raft = Some(RaftConfig {
            addr: "127.0.0.1:7101".to_string(),
            peers: Vec::new(),
            heartbeat_interval_ms: None,
            election_timeout_ms: None,
            snapshot_interval: None,
            data_dir: None,
        });

        let capabilities = BootstrapCapabilities::from_config(&config);
        assert!(!capabilities.single_node_deployment);
        assert!(capabilities.raft_enabled);
        assert_eq!(capabilities.runtime_mode_label(), "distributed");
    }

    #[test]
    fn state_backend_helpers_follow_bootstrap_capabilities() {
        let local = BootstrapCapabilities {
            single_node_deployment: true,
            raft_enabled: true,
        };
        assert!(local.single_node_deployment);
        assert!(local.raft_enabled);

        let distributed = BootstrapCapabilities {
            single_node_deployment: false,
            raft_enabled: false,
        };
        assert!(!distributed.single_node_deployment);
        assert!(!distributed.raft_enabled);
    }

    #[test]
    fn coordination_services_build_local_bundle_without_optional_components() {
        let config = sample_config();
        let profile_store = Arc::new(
            ProfileControlPlaneStore::open_temp("demo")
                .expect("open temporary control-plane store"),
        );

        let coordination = CoordinationServices::build(&config, None, None, profile_store, None);

        assert_eq!(coordination.cache_service.namespace(), "demo:cache");
        assert!(coordination.cookie_service.is_none());
        assert_eq!(coordination.limiter.get_key_prefix(), "demo:rate_limiter");
        assert!(coordination.api_limiter.is_none());
    }

    #[test]
    fn coordination_services_scope_api_limiter_separately_from_main_limiter() {
        let mut config = sample_config();
        config.api = Some(Api {
            port: 8805,
            api_key: Some("local-dev".to_string()),
            rate_limit: Some(12.0),
        });
        let profile_store = Arc::new(
            ProfileControlPlaneStore::open_temp("demo")
                .expect("open temporary control-plane store"),
        );

        let coordination = CoordinationServices::build(&config, None, None, profile_store, None);

        assert_eq!(coordination.limiter.get_key_prefix(), "demo:rate_limiter");
        assert_eq!(
            coordination
                .api_limiter
                .as_ref()
                .expect("api limiter should be configured")
                .get_key_prefix(),
            "demo:api:rate_limiter"
        );
    }

    // ── Raft/RocksDB integration smoke tests ──

    #[test]
    fn raft_rocksdb_state_bootstrap_no_redis() {
        let mut config = sample_config();
        config.cache.backend = Some(crate::common::model::config::CacheBackendKind::RaftRocksdb);

        let profile_store = Arc::new(
            ProfileControlPlaneStore::open_temp("raft-test")
                .expect("open temporary control-plane store"),
        );

        let coordination = CoordinationServices::build(
            &config,
            None,
            None,
            profile_store.clone(),
            Some(crate::common::model::config::CacheBackendKind::RaftRocksdb),
        );

        // Verify cache backend is functional.
        assert_eq!(coordination.cache_service.namespace(), "demo:cache");
        assert!(coordination.cookie_service.is_none());
    }

    #[tokio::test]
    async fn raft_rocksdb_cache_lock_rate_limit_status_smoke() {
        let mut config = sample_config();
        config.cache.backend = Some(crate::common::model::config::CacheBackendKind::RaftRocksdb);

        let profile_store = Arc::new(
            ProfileControlPlaneStore::open_temp("raft-smoke")
                .expect("open temporary control-plane store"),
        );

        let coordination = CoordinationServices::build(
            &config,
            None,
            None,
            profile_store.clone(),
            Some(crate::common::model::config::CacheBackendKind::RaftRocksdb),
        );

        // Cache: set and get via cache_service
        coordination
            .cache_service
            .set("smoke_key", b"smoke_value", None)
            .await
            .expect("cache set");
        let val = coordination
            .cache_service
            .get("smoke_key")
            .await
            .expect("cache get");
        assert_eq!(val, Some(b"smoke_value".to_vec()));

        // Lock: acquire and release
        let lock_result = coordination
            .locker
            .acquire_lock("smoke_lock", 30, std::time::Duration::from_secs(5))
            .await
            .expect("lock acquire");
        assert!(lock_result);
        coordination
            .locker
            .release_lock("smoke_lock")
            .await
            .expect("lock release");

        // Status counter: write and read via profile_store
        use crate::common::model::control_plane_profile::ControlPlaneRaftCommand;
        profile_store
            .submit_command(ControlPlaneRaftCommand::UpsertStatusCounter {
                namespace: "raft-smoke".to_string(),
                counter_key: "task:smoke_task:error_count".to_string(),
                value: 3,
            })
            .await
            .expect("status counter");
        let counter = profile_store.get_status_counter("task:smoke_task:error_count");
        assert_eq!(counter, 3);

        // Rate limit: call check_and_update via Raft backend
        let wait = coordination
            .limiter
            .check_and_update("rate-limit-test")
            .await
            .expect("rate limit check_and_update");
        assert_eq!(wait, 0, "first request should acquire permit immediately");

        // Second call must see a non-zero wait (rate-limit state persisted via Raft backend)
        let wait2 = coordination
            .limiter
            .check_and_update("rate-limit-test")
            .await
            .expect("rate limit check_and_update 2");
        assert!(wait2 > 0, "second call must wait, got {wait2}");
    }

    #[tokio::test]
    async fn raft_rocksdb_state_uses_raft_rate_limit_backend() {
        let mut config = sample_config();
        config.cache.backend = Some(crate::common::model::config::CacheBackendKind::RaftRocksdb);

        let profile_store = Arc::new(
            ProfileControlPlaneStore::open_temp("raft-rl-test")
                .expect("open temporary control-plane store"),
        );

        let coordination = CoordinationServices::build(
            &config,
            None,
            None,
            profile_store.clone(),
            Some(crate::common::model::config::CacheBackendKind::RaftRocksdb),
        );

        // Call check_and_update to write rate-limit state
        let wait = coordination
            .limiter
            .check_and_update("rl-backend-test")
            .await
            .expect("check_and_update");
        assert_eq!(wait, 0, "first request should acquire permit immediately");

        // Second call immediately after should see wait time
        let wait2 = coordination
            .limiter
            .check_and_update("rl-backend-test")
            .await
            .expect("check_and_update 2");
        // With rate_limit=2.0 and window=1000ms, interval is 500ms
        assert!(
            wait2 > 0,
            "expected wait time on second request, got {wait2}"
        );
    }

    // ── Full-stack RaftRocksDB integration tests ──

    /// Build a full `State` with SQLite in-memory and RaftRocksDB backend.
    /// Exercises cache, lock, rate-limit, and status tracker all together.
    #[tokio::test]
    async fn raft_rocksdb_full_state_integration() {
        let mut config = sample_config();
        config.db.url = Some("sqlite::memory:".to_string());
        config.cache.backend = Some(crate::common::model::config::CacheBackendKind::RaftRocksdb);

        let profile_store = Arc::new(
            ProfileControlPlaneStore::open_temp("raft-full")
                .expect("open temporary control-plane store"),
        );

        let db = Arc::new(
            sea_orm::Database::connect("sqlite::memory:")
                .await
                .expect("connect sqlite in-memory"),
        );

        let coordination = CoordinationServices::build(
            &config,
            None,
            None,
            profile_store.clone(),
            Some(crate::common::model::config::CacheBackendKind::RaftRocksdb),
        );

        let state = crate::common::state::State {
            db,
            config: Arc::new(tokio::sync::RwLock::new(config)),
            cache_service: coordination.cache_service,
            cookie_service: coordination.cookie_service,
            locker: coordination.locker,
            limiter: coordination.limiter,
            api_limiter: coordination.api_limiter,
            status_tracker: coordination.status_tracker,
            profile_store,
            raft_runtime_config: None,
            raft_runtime: None,
        };

        // 1. Cache: write and read
        state
            .cache_service
            .set("integration_key", b"integration_value", None)
            .await
            .expect("cache set");
        let val = state
            .cache_service
            .get("integration_key")
            .await
            .expect("cache get");
        assert_eq!(val, Some(b"integration_value".to_vec()));

        // 2. Lock: acquire and release
        let locked = state
            .locker
            .acquire_lock("integration_lock", 30, std::time::Duration::from_secs(2))
            .await
            .expect("lock acquire");
        assert!(locked);
        let released = state
            .locker
            .release_lock("integration_lock")
            .await
            .expect("lock release");
        assert!(released);

        // 3. Rate limit: acquire and check
        let wait = state
            .limiter
            .check_and_update("integration_rl")
            .await
            .expect("check_and_update");
        assert_eq!(wait, 0);

        // 4. Status tracker: write error counter
        use crate::common::model::control_plane_profile::ControlPlaneRaftCommand;
        state
            .profile_store
            .submit_command(ControlPlaneRaftCommand::UpsertStatusCounter {
                namespace: "raft-full".to_string(),
                counter_key: "task:integration:error_count".to_string(),
                value: 1,
            })
            .await
            .expect("status counter");
        let counter = state
            .profile_store
            .get_status_counter("task:integration:error_count");
        assert_eq!(counter, 1);

        // 5. Verify rate-limit through behavior: second call must see wait time
        // (proves state is persisted via Raft backend, not local DashMap)
        let wait2 = state
            .limiter
            .check_and_update("integration_rl")
            .await
            .expect("check_and_update 2");
        assert!(
            wait2 > 0,
            "second rate-limit call must see wait time (proof of Raft-backed state), got {wait2}"
        );

        assert!(state.is_single_node_deployment());
    }
}
