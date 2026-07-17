// no direct filesystem path usage here
#[cfg(feature = "store")]
use crate::utils::connector::db_connection;

use crate::common::config::ConfigProvider;
use crate::common::config::file::FileConfigProvider;
use crate::common::model::config::Config;

use crate::cacheable::CacheService;
use crate::common::status_tracker::{ErrorTrackerConfig, StatusTracker};
use crate::utils::distributed_rate_limit::{DistributedSlidingWindowRateLimiter, RateLimitConfig};
use crate::utils::lock::DistributedLockManager;
use log::{error, info};
use std::sync::Arc;
use std::time;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum StateInitError {
    #[error("load config failed: {0}")]
    LoadConfig(String),
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
/// Contains connections to database, configuration, and shared services.
/// DB handle: a real connection when the `store` feature is enabled, otherwise a placeholder type
/// (always `None`).
#[cfg(feature = "store")]
pub type DbHandle = Option<Arc<sea_orm::DatabaseConnection>>;
#[cfg(not(feature = "store"))]
pub type DbHandle = Option<()>;

#[derive(Clone)]
pub struct State {
    /// Database connection pool (`None` = no DB / standalone mode; always a placeholder `None`
    /// without the `store` feature).
    pub db: DbHandle,
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
    /// Optional coordination backend (e.g. the embedded redb+Raft one). When set, the engine's
    /// `LeaderElector` / locks / rate limiting go through it for strongly consistent cross-node
    /// coordination; when unset, the runtime is in single-node in-process mode.
    /// Injected by the facade when starting a cluster (see `cluster-embedded`).
    pub coordination: Option<Arc<dyn crate::common::coordination::CoordinationBackend>>,
}

impl State {
    /// Creates a new State instance from a file-based configuration.
    ///
    /// Returns a `Result`; library code never calls `process::exit` — callers
    /// decide how to handle initialization failures.
    pub async fn try_new(path: &str) -> Result<Self, StateInitError> {
        let provider = FileConfigProvider::new(path);
        Self::try_new_with_provider(Box::new(provider)).await
    }

    /// Creates a new State instance from a custom configuration provider,
    /// returning explicit initialization failures.
    ///
    /// Initializes the DB connection and services (Lock, Limit, Tracker).
    /// Starts a background task to watch for configuration changes.
    pub async fn try_new_with_provider(
        provider: Box<dyn ConfigProvider>,
    ) -> Result<Self, StateInitError> {
        Self::try_new_with_provider_and_coordination(provider, None).await
    }

    /// Same as [`try_new_with_provider`](Self::try_new_with_provider), but injects an optional
    /// coordination backend (e.g. the embedded redb+Raft one). Once injected, distributed locks /
    /// leader election / rate limiting go through that backend **from construction onwards**
    /// (rather than the in-process local implementation), making coordination strongly consistent
    /// across nodes in cluster mode.
    pub async fn try_new_with_provider_and_coordination(
        provider: Box<dyn ConfigProvider>,
        coordination: Option<Arc<dyn crate::common::coordination::CoordinationBackend>>,
    ) -> Result<Self, StateInitError> {
        let config = provider
            .load_config()
            .await
            .map_err(|e| StateInitError::LoadConfig(e.to_string()))?;
        // Single-node vs. distributed is decided by whether a coordination backend was injected.
        let single_node_mode = coordination.is_none();
        info!(
            "Runtime mode initialized: {}",
            if single_node_mode {
                "single_node"
            } else {
                "distributed (coordination backend)"
            }
        );

        let watcher_res = provider.watch().await;

        #[cfg(feature = "store")]
        let db: DbHandle = if config.db.url.is_some() {
            let conn = db_connection(
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
            })?;
            info!("Database connected successfully");
            Some(Arc::new(conn))
        } else {
            info!("No database configured; running in standalone (in-memory metadata) mode");
            None
        };
        #[cfg(not(feature = "store"))]
        let db: DbHandle = {
            if config.db.url.is_some() {
                info!(
                    "db.url is set but the `store` feature is disabled; ignoring (standalone mode)"
                );
            }
            None
        };

        let cache_ttl = time::Duration::from_secs(config.cache.ttl);

        let locker = Arc::new(DistributedLockManager::new_with_coordination(
            coordination.clone(),
            &config.name,
        ));

        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new_with_coordination(
            locker.clone(),
            coordination.clone(),
            &config.name,
            RateLimitConfig {
                max_requests_per_second: config.download_config.rate_limit,
                window_size_millis: 1000,
                base_max_requests_per_second: Some(config.download_config.rate_limit),
            },
        ));

        let api_limiter = if let Some(api) = &config.api {
            if let Some(limit) = api.rate_limit {
                Some(Arc::new(
                    DistributedSlidingWindowRateLimiter::new_with_coordination(
                        locker.clone(),
                        coordination.clone(),
                        &format!("{}:api", config.name),
                        RateLimitConfig {
                            max_requests_per_second: limit as f32,
                            window_size_millis: 1000,
                            base_max_requests_per_second: Some(limit as f32),
                        },
                    ),
                ))
            } else {
                None
            }
        } else {
            None
        };

        let cache_service = Arc::new(CacheService::new(
            format!("{}:cache", config.name),
            Some(cache_ttl),
            config.cache.compression_threshold,
        ));

        // Cookie storage uses the in-process cache (single-node).
        let cookie_service: Option<Arc<CacheService>> = None;
        info!("Cache service initialized (in-memory)");

        // Initialize error tracking subsystem.
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
        let error_tracker = Arc::new(StatusTracker::new(
            cache_service.clone(),
            error_tracker_config,
            locker.clone(),
        ));

        let config_arc = Arc::new(RwLock::new(config));

        // Spawn configuration watcher
        if let Ok(mut rx) = watcher_res {
            let config_clone = config_arc.clone();
            let limiter = limiter.clone();
            let api_limiter = api_limiter.clone();
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
            cache_service,
            cookie_service,
            locker,
            limiter,
            api_limiter,
            status_tracker: error_tracker,
            coordination,
        })
    }

    /// Builds the [focused context](crate::common::context::PipelineContext) required by the
    /// collection pipeline.
    ///
    /// Clones only the four shared services the pipeline actually uses (config / cache_service /
    /// status_tracker / locker), letting chains depend on the narrowed `PipelineContext` rather than
    /// the whole `State` — decoupling the core pipeline from optional subsystems such as the
    /// database / API rate limiter / cookies / coordination backend. The four `Arc`s share the same
    /// instances as `State`.
    pub fn pipeline_ctx(&self) -> Arc<crate::common::context::PipelineContext> {
        Arc::new(crate::common::context::PipelineContext {
            config: self.config.clone(),
            cache_service: self.cache_service.clone(),
            status_tracker: self.status_tracker.clone(),
            locker: self.locker.clone(),
        })
    }
}
