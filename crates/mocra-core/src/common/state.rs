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
/// DB 句柄:开启 `store` 特性时为真实连接,否则为占位类型(始终 `None`)。
#[cfg(feature = "store")]
pub type DbHandle = Option<Arc<sea_orm::DatabaseConnection>>;
#[cfg(not(feature = "store"))]
pub type DbHandle = Option<()>;

#[derive(Clone)]
pub struct State {
    /// Database connection pool(`None` = 无 DB / standalone 模式;无 `store` 特性时恒为占位 `None`)。
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
    /// 可选的协调后端(如内嵌 redb+Raft)。设置后,引擎的 `LeaderElector` / 锁 /
    /// 限流等走它做跨节点强一致协调;未设置即为单机进程内模式。
    /// 由 facade 在启动集群时注入(见 `cluster-embedded`)。
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

    /// 同 [`try_new_with_provider`](Self::try_new_with_provider),但注入一个可选的
    /// 协调后端(如内嵌 redb+Raft)。注入后,分布式锁 / 选举 / 限流等**从构造起**即走该
    /// 后端(而非进程内本地实现),使集群模式下的协调跨节点强一致。
    pub async fn try_new_with_provider_and_coordination(
        provider: Box<dyn ConfigProvider>,
        coordination: Option<Arc<dyn crate::common::coordination::CoordinationBackend>>,
    ) -> Result<Self, StateInitError> {
        let config = provider
            .load_config()
            .await
            .map_err(|e| StateInitError::LoadConfig(e.to_string()))?;
        // 单机 vs 分布式由是否注入协调后端决定。
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

        // cookie 存储走进程内缓存(单机)。
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

    /// 构造采集管线所需的[聚焦上下文](crate::common::context::PipelineContext)。
    ///
    /// 只克隆管线真正用到的三个共享服务(config / cache_service / status_tracker),
    /// 让 chains 依赖窄化的 `PipelineContext` 而非整个 `State` —— 把核心管线与数据库 /
    /// 协调后端 / 限流器等可选子系统解耦。三个 `Arc` 与 `State` 共享同一实例。
    pub fn pipeline_ctx(&self) -> Arc<crate::common::context::PipelineContext> {
        Arc::new(crate::common::context::PipelineContext {
            config: self.config.clone(),
            cache_service: self.cache_service.clone(),
            status_tracker: self.status_tracker.clone(),
            locker: self.locker.clone(),
        })
    }
}
