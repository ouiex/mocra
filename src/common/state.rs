// no direct filesystem path usage here
use crate::utils::connector::{create_redis_pool, db_connection};

use crate::common::model::config::Config;
use crate::common::config::ConfigProvider;
use crate::common::config::file::FileConfigProvider;

use crate::common::status_tracker::{ErrorTrackerConfig, StatusTracker};
use crate::cacheable::CacheService;
use log::{info, error};
use std::sync::Arc;
use std::time;
use tokio::sync::RwLock;
use crate::utils::distributed_rate_limit::{DistributedSlidingWindowRateLimiter, RateLimitConfig};
use crate::utils::redis_lock::DistributedLockManager;
use deadpool_redis::Pool;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateInitError {
    #[error("load config failed: {0}")]
    LoadConfig(String),
    #[error("database connection failed (url={url:?}, schema={schema:?}, pool_size={pool_size:?}, tls={tls:?})")]
    DatabaseConnect {
        url: Option<String>,
        schema: Option<String>,
        pool_size: Option<u32>,
        tls: Option<bool>,
    },
    #[error("redis pool creation failed ({name}): host={host}, port={port}, db={db}, tls={tls}, pool_size={pool_size:?}")]
    RedisPoolCreate {
        name: &'static str,
        host: String,
        port: u16,
        db: u16,
        tls: bool,
        pool_size: Option<usize>,
    },
    #[error("redis ping failed (cache): {0}")]
    CachePing(String),
    #[error("redis connection borrow failed (cache): {0}")]
    CacheConn(String),
}

/// Global application state shared across the system.
///
/// Contains connections to database, Redis, configuration, and shared services.
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
    /// Underlying Redis pool (exposed for specialized components like LeaderElector)
    pub redis: Option<Pool>,
}
impl State {
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
    /// Initializes all connections (DB, Redis) and services (Lock, Limit, Tracker).
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
        let single_node_mode = config.is_single_node_mode();
        info!(
            "Runtime mode initialized: {}",
            if single_node_mode { "single_node" } else { "distributed" }
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
        let cache_pool = if single_node_mode {
            None
        } else {
            match config.cache.redis.as_ref() {
                Some(redis) => {
                    let pool = create_redis_pool(
                        &redis.redis_host,
                        redis.redis_port,
                        redis.redis_db,
                        &redis.redis_username,
                        &redis.redis_password,
                        redis.pool_size,
                        redis.tls.unwrap_or(false),
                    )
                    .ok_or_else(|| StateInitError::RedisPoolCreate {
                        name: "cache",
                        host: redis.redis_host.clone(),
                        port: redis.redis_port,
                        db: redis.redis_db,
                        tls: redis.tls.unwrap_or(false),
                        pool_size: redis.pool_size,
                    })?;
                    Some(pool)
                }
                None => None,
            }
        };
        {
            if let Some(pool) = cache_pool.as_ref() {
                let mut cnn = pool
                    .get()
                    .await
                    .map_err(|e| StateInitError::CacheConn(e.to_string()))?;
                let _pong: String = deadpool_redis::redis::cmd("PING")
                    .query_async(&mut *cnn)
                    .await
                    .map_err(|e| StateInitError::CachePing(e.to_string()))?;
            }
        }
        info!("cache pool connect successfully");
        let cookie_pool = if single_node_mode {
            None
        } else {
            match config.cookie.as_ref() {
                Some(redis) => {
                    let pool = create_redis_pool(
                        &redis.redis_host,
                        redis.redis_port,
                        redis.redis_db,
                        &redis.redis_username,
                        &redis.redis_password,
                        redis.pool_size,
                        redis.tls.unwrap_or(false),
                    )
                    .ok_or_else(|| StateInitError::RedisPoolCreate {
                        name: "cookie",
                        host: redis.redis_host.clone(),
                        port: redis.redis_port,
                        db: redis.redis_db,
                        tls: redis.tls.unwrap_or(false),
                        pool_size: redis.pool_size,
                    })?;
                    Some(pool)
                }
                None => None,
            }
        };
        info!("cookie pool connect successfully");

        // Reuse cache_pool for locker and limiter since they use the same config
        let locker_pool = cache_pool.clone().map(Arc::new);
        let limit_pool = cache_pool.clone().map(Arc::new);

        info!("locker and limit pools shared with cache pool");

        let locker = Arc::new(DistributedLockManager::new(
            locker_pool.clone(),
            &config.name,
        ));

        let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
            limit_pool.clone(),
            locker.clone(),
            &config.name,
            RateLimitConfig {
                max_requests_per_second: config.download_config.rate_limit,
                window_size_millis: 1000,
                base_max_requests_per_second: Some(config.download_config.rate_limit),
            },
        ));

        let api_limiter = if let Some(api) = &config.api {
            if let Some(limit) = api.rate_limit {
                Some(Arc::new(DistributedSlidingWindowRateLimiter::new(
                    limit_pool.clone(),
                    locker.clone(),
                    &format!("{}:api", config.name),
                    RateLimitConfig {
                        max_requests_per_second: limit as f32,
                        window_size_millis: 1000,
                        base_max_requests_per_second: Some(limit as f32),
                    },
                )))
            } else {
                None
            }
        } else {
            None
        };

        let cache_ttl = time::Duration::from_secs(config.cache.ttl);
        let enable_l1 = config.cache.enable_l1.unwrap_or(false);
        let l1_ttl_secs = config.cache.l1_ttl_secs.unwrap_or(30);
        let l1_max_entries = config.cache.l1_max_entries.unwrap_or(10000);
        
        let cache_service = if let Some(pool) = cache_pool.clone() {
            Arc::new(CacheService::new_with_l1_config(
                Some(pool),
                format!("{}:cache", config.name),
                Some(cache_ttl),
                config.cache.compression_threshold,
                enable_l1,
                l1_ttl_secs,
                l1_max_entries,
            ))
        } else {
            Arc::new(CacheService::new_with_l1_config(
                None,
                format!("{}:cache", config.name),
                Some(cache_ttl),
                config.cache.compression_threshold,
                false, // No L1 for local-only mode
                l1_ttl_secs,
                l1_max_entries,
            ))
        };

        let cookie_service = cookie_pool.map(|pool|
            Arc::new(CacheService::new_with_l1_config(
                Some(pool),
                format!("{}:cookie", config.name),
                Some(cache_ttl),
                config.cache.compression_threshold,
                enable_l1,
                l1_ttl_secs,
                l1_max_entries,
            ))
        );
        info!("Redis connection pool created successfully");

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
                    let _ = limiter.set_all_limit(new_config.download_config.rate_limit).await;
                    if let (Some(api), Some(api_limiter)) = (new_config.api.as_ref(), api_limiter.as_ref())
                        && let Some(limit) = api.rate_limit {
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
            redis: cache_pool,
        })
    }
}
