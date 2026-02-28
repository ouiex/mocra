// #![allow(unused)]
use crate::utils::device_info::get_primary_local_ip;
use crate::engine::zombie;
use crate::engine::monitor::SystemMonitor;
use crate::engine::events::{
    EventBus, RedisEventHandler,
};
use crate::downloader::DownloaderManager;
use crate::queue::Identifiable;

use crate::engine::chain::{
    create_download_chain, create_parser_chain, create_unified_task_ingress_chain,
    UnifiedTaskIngressChain,
};
use crate::engine::events::{EventEnvelope, EventPhase, EventType, HealthCheckEvent};
use crate::common::policy::{DlqPolicy, PolicyResolver};
use metrics::counter;

use crate::engine::chain::stream_chain::create_wss_download_chain;
use futures::{StreamExt, FutureExt};
use crate::common::state::State;
use log::{error, info, warn};
use crate::queue::{QueueManager, QueuedItem};

use crate::common::processors::processor::{ProcessorContext, RetryPolicy};
use crate::common::model::message::UnifiedTaskInput;
use crate::common::model::chain_key;
use crate::proxy::ProxyManager;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use crate::common::interface::{
    DataMiddlewareHandle, DataStoreMiddlewareHandle, DownloadMiddlewareHandle, MiddlewareManager,
    ModuleTrait,
};
use crate::common::registry::NodeRegistry;
use crate::utils::connector::create_redis_pool;
use crate::engine::task::TaskManager;
use crate::engine::runner::ProcessorRunner;
use crate::engine::lua::LuaScriptRegistry;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use crate::engine::scheduler::CronScheduler;
use crate::sync::{LeaderElector, RedisBackend};
use uuid::Uuid;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::utils::logger as app_logger;
use crate::utils::logger::{LogOutputConfig as AppLogOutputConfig, LoggerConfig as AppLoggerConfig, LogSender as AppLogSender, PrometheusConfig as AppPrometheusConfig};

mod processors;
mod runtime;
#[cfg(test)]
mod tests;

/// Engine is the core component that orchestrates the crawling process.
/// It manages the lifecycle of Tasks, Requests, Responses, and Parsers via a chain of responsibilities.
///
/// # Architecture
/// The data flow is pipeline-based:
/// 1. `TaskProcessor`: Consumes Tasks -> Generates Requests (via `TaskModelChain`).
/// 2. `DownloadProcessor`: Consumes Requests -> Downloads Content -> Generates Responses (via `DownloadChain`).
/// 3. `ResponseProcessor`: Consumes Responses -> Parses Data -> Generates ParserTasks (via `ParserChain`).
/// 4. `ParserProcessor`: Consumes ParserTasks -> Extracts Data/New Tasks (via `ParserTaskChain`).
/// 5. `ErrorProcessor`: Handles errors and retries (via `ErrorChain`).
///
/// # Distributed Coordination
/// - **Queues**: Redis/Kafka are used for passing messages between processors, enabling horizontal scaling.
/// - **Locking**: Optional distributed locking (Redis-based) ensures serial execution for state-sensitive tasks.
/// - **Rate Limiting**: Distributed sliding window rate limiter protects target sites and manages concurrency.
/// - **Cron**: `CronScheduler` handles distributed timing tasks with de-duplication.
pub struct Engine {
    /// Message transport abstraction for all engine queues (task/request/response/parser/error).
    pub queue_manager: Arc<QueueManager>,
    /// Downloader registry and connection lifecycle manager.
    pub downloader_manager: Arc<DownloaderManager>,
    /// Task loading and module materialization service.
    pub task_manager: Arc<TaskManager>,
    /// Optional proxy provider used by download stages.
    pub proxy_manager: Option<Arc<ProxyManager>>,
    /// Runtime middleware registry for request/response/data stages.
    pub middleware_manager: Arc<MiddlewareManager>,
    /// Optional pub/sub event bus for observability and operational workflows.
    pub event_bus: Option<Arc<EventBus>>,
    /// Global shared state (config, cache, status tracker, lock/rate services).
    pub state: Arc<State>,
    /// Broadcast shutdown signal consumed by all long-running processor loops.
    shutdown_tx: broadcast::Sender<()>,
    /// Pause switch propagated to workers via watch channel.
    pause_tx: watch::Sender<bool>,
    /// Optional Prometheus exporter handle exposed to API endpoints.
    pub prometheus_handle: Option<PrometheusHandle>,
    /// Node registry used for heartbeats and cluster visibility.
    pub node_registry: Arc<NodeRegistry>,
    /// Distributed cron scheduler.
    pub cron_scheduler: Arc<CronScheduler>,
    /// Lua script registry used for atomic distributed coordination paths.
    pub lua_registry: Arc<LuaScriptRegistry>,
}

impl Engine {
    /// Interval for node heartbeats.
    const NODE_HEARTBEAT_INTERVAL_SECS: u64 = 10;
    /// TTL attached to heartbeat records.
    const NODE_HEARTBEAT_TTL_SECS: u64 = Self::NODE_HEARTBEAT_INTERVAL_SECS * 3;

    /// Normalizes engine event labels for low-cardinality metrics dimensions.
    fn policy_event_label(event_type: &str) -> &'static str {
        match event_type {
            "task_model" => "task_model",
            "download" => "download",
            "parser_task_model" => "parser_task_model",
            "system_error" => "system_error",
            "parser" => "parser",
            _ => "unknown",
        }
    }

    fn policy_kind_label(kind: &crate::errors::ErrorKind) -> &'static str {
        match kind {
            crate::errors::ErrorKind::Request => "request",
            crate::errors::ErrorKind::Response => "response",
            crate::errors::ErrorKind::Command => "command",
            crate::errors::ErrorKind::Service => "service",
            crate::errors::ErrorKind::Proxy => "proxy",
            crate::errors::ErrorKind::Download => "download",
            crate::errors::ErrorKind::Queue => "queue",
            crate::errors::ErrorKind::Orm => "orm",
            crate::errors::ErrorKind::Task => "task",
            crate::errors::ErrorKind::Module => "module",
            crate::errors::ErrorKind::RateLimit => "rate_limit",
            crate::errors::ErrorKind::ProcessorChain => "processor_chain",
            crate::errors::ErrorKind::Parser => "parser",
            crate::errors::ErrorKind::DataMiddleware => "data_middleware",
            crate::errors::ErrorKind::DataStore => "data_store",
            crate::errors::ErrorKind::DynamicLibrary => "dynamic_library",
            crate::errors::ErrorKind::CacheService => "cache_service",
        }
    }

    /// Applies policy resolution for fatal failures and performs queue side-effects.
    ///
    /// The method emits `policy_decisions_total` metrics and then executes one of:
    /// - retry via `nack` with reason,
    /// - DLQ handoff + `ack`,
    /// - direct `ack`.
    async fn handle_policy_failure<T>(
        policy_resolver: &PolicyResolver,
        queue_manager: &QueueManager,
        topic: &str,
        event_type: &str,
        item: &T,
        err: &crate::errors::Error,
        ack_fn: &mut Option<crate::queue::AckFn>,
        nack_fn: &mut Option<crate::queue::NackFn>,
    ) where
        T: serde::Serialize + Identifiable + Send + Sync,
    {
        let decision = policy_resolver.resolve_with_error(
            "engine",
            Some(event_type),
            Some("failed"),
            err,
        );
        let action = if decision.policy.retryable {
            "retry"
        } else if decision.policy.dlq == DlqPolicy::Never {
            "ack"
        } else {
            "dlq"
        };

        let event_label = Self::policy_event_label(event_type);
        let kind_label = Self::policy_kind_label(err.kind());

        counter!(
            "policy_decisions_total",
            "domain" => "engine",
            "event_type" => event_label,
            "phase" => "failed",
            "kind" => kind_label,
            "action" => action
        )
        .increment(1);

        let reason = format!("{}: {}", decision.reason, err);

        match action {
            "retry" => {
                if let Some(f) = nack_fn.take() {
                    let _ = f(reason).await;
                }
            }
            "dlq" => {
                let _ = queue_manager.send_to_dlq(topic, item, &reason).await;
                if let Some(f) = ack_fn.take() {
                    let _ = f().await;
                }
            }
            _ => {
                if let Some(f) = ack_fn.take() {
                    let _ = f().await;
                }
            }
        }
    }

    /// Applies policy resolution for retryable failures surfaced by processor chains.
    ///
    /// Retryable outcomes are converted into a synthetic `ProcessorChain` error shape
    /// so policy matching stays consistent with normal error handling.
    async fn handle_policy_retry<T>(
        policy_resolver: &PolicyResolver,
        queue_manager: &QueueManager,
        topic: &str,
        event_type: &str,
        item: &T,
        retry_policy: &RetryPolicy,
        ack_fn: &mut Option<crate::queue::AckFn>,
        nack_fn: &mut Option<crate::queue::NackFn>,
    ) where
        T: serde::Serialize + Identifiable + Send + Sync,
    {
        let reason = retry_policy
            .reason
            .clone()
            .unwrap_or_else(|| "retryable failure".to_string());
        let err = crate::errors::Error::new(
            crate::errors::ErrorKind::ProcessorChain,
            Some(std::io::Error::new(std::io::ErrorKind::Other, reason.clone())),
        );

        let decision = policy_resolver.resolve_with_error(
            "engine",
            Some(event_type),
            Some("retry"),
            &err,
        );
        let action = if decision.policy.retryable {
            "retry"
        } else if decision.policy.dlq == DlqPolicy::Never {
            "ack"
        } else {
            "dlq"
        };

        let event_label = Self::policy_event_label(event_type);
        let kind_label = Self::policy_kind_label(err.kind());

        counter!(
            "policy_decisions_total",
            "domain" => "engine",
            "event_type" => event_label,
            "phase" => "retry",
            "kind" => kind_label,
            "action" => action
        )
        .increment(1);

        let reason = format!("{}: {}", decision.reason, reason);

        match action {
            "retry" => {
                if let Some(f) = nack_fn.take() {
                    let _ = f(reason).await;
                }
            }
            "dlq" => {
                let _ = queue_manager.send_to_dlq(topic, item, &reason).await;
                if let Some(f) = ack_fn.take() {
                    let _ = f().await;
                }
            }
            _ => {
                if let Some(f) = ack_fn.take() {
                    let _ = f().await;
                }
            }
        }
    }

    /// Initializes queue manager with optional log topic derived from logger outputs.
    fn init_queue_manager(cfg: &crate::common::model::config::Config) -> Arc<QueueManager> {
        let log_topic = cfg
            .logger
            .as_ref()
            .and_then(|logger| Self::first_mq_topic(logger));
        QueueManager::from_config_with_log_topic(cfg, log_topic.as_deref())
    }

    fn first_mq_topic(
        logger: &crate::common::model::logger_config::LoggerConfig,
    ) -> Option<String> {
        logger.outputs.iter().find_map(|output| {
            match output {
                crate::common::model::logger_config::LogOutputConfig::Mq { topic, .. } => {
                    Some(topic.clone())
                }
                _ => None,
            }
        })
    }

    fn build_app_logger_config(
        logger: &crate::common::model::logger_config::LoggerConfig,
        namespace: &str,
    ) -> AppLoggerConfig {
        let mut config = AppLoggerConfig::for_app(namespace);
        if let Some(enabled) = logger.enabled {
            config.enabled = enabled;
        }
        if let Some(level) = &logger.level {
            config.level = level.clone();
        }
        if let Some(format) = &logger.format {
            if format.to_lowercase() != "text" {
                eprintln!("logger.format only supports text for console/file, got {format}");
            }
            config.format = "text".to_string();
        }
        if let Some(include) = &logger.include {
            config.include = include.clone();
        }
        if let Some(buffer) = logger.buffer {
            config.buffer = buffer;
        }
        if let Some(interval) = logger.flush_interval_ms {
            config.flush_interval_ms = interval;
        }

        config.outputs = logger
            .outputs
            .iter()
            .filter_map(|output| match output {
                crate::common::model::logger_config::LogOutputConfig::Console {} => {
                    Some(AppLogOutputConfig::Console)
                }
                crate::common::model::logger_config::LogOutputConfig::File {
                    path,
                    rotation,
                    ..
                } => Some(AppLogOutputConfig::File {
                    path: PathBuf::from(path),
                    rotation: rotation.clone(),
                }),
                crate::common::model::logger_config::LogOutputConfig::Mq { format, .. } => {
                    if let Some(format) = format
                        && format.to_lowercase() != "json"
                    {
                        eprintln!("logger.outputs.mq.format only supports json, got {format}");
                    }
                    Some(AppLogOutputConfig::Mq)
                }
            })
            .collect();

        if config.outputs.is_empty() {
            config.outputs = AppLoggerConfig::default().outputs;
        }

        if let Some(prometheus) = &logger.prometheus
            && prometheus.enabled
        {
            config.prometheus = Some(AppPrometheusConfig { enabled: true });
        }

        config
    }

    fn base_level_from_filter(level: &str) -> Option<&str> {
        level
            .split(|ch| ch == ',' || ch == ';')
            .map(|value| value.trim())
            .find(|value| !value.is_empty())
    }

    async fn setup_mq_log_sender(
        logger: &crate::common::model::logger_config::LoggerConfig,
        queue_manager: Arc<QueueManager>,
    ) -> Option<AppLogSender> {
        let mq_output = logger.outputs.iter().find_map(|output| {
            match output {
                crate::common::model::logger_config::LogOutputConfig::Mq { buffer, .. } => {
                    Some(*buffer)
                }
                _ => None,
            }
        })?;

        let buffer = mq_output.or(logger.buffer).unwrap_or(10000);
        let level = logger
            .level
            .as_deref()
            .and_then(Self::base_level_from_filter)
            .unwrap_or("info")
            .to_string();

        let (sender, mut receiver) = tokio::sync::mpsc::channel(buffer);
        let log_sender = AppLogSender::with_capacity(sender, level, buffer);
        let queue_sender = queue_manager.get_log_push_channel();

        tokio::spawn(async move {
            while let Some(log) = receiver.recv().await {
                let item = QueuedItem::new(log);
                if let Err(e) = queue_sender.send(item).await {
                    eprintln!("Failed to forward log to queue: {e}");
                }
            }
        });

        Some(log_sender)
    }

    /// Create a new Engine instance.
    ///
    /// Initializes all core components including queue manager, downloader manager,
    /// task manager, middleware manager, and event bus.
    ///
    /// # Arguments
    /// * `state` - Shared application state.
    pub async fn new(state: Arc<State>, queue_manager: Option<Arc<QueueManager>>) -> Self {
        // Initialize Prometheus recorder
        let builder = PrometheusBuilder::new();
        // Ignore error if recorder is already installed (e.g. in tests)
        let prometheus_handle = builder.install_recorder().ok();

        // Create event bus when enabled by configuration.
        let event_bus = if let Some(conf) = &state.config.read().await.event_bus {
            Some(Arc::new(EventBus::new(conf.capacity, conf.concurrency)))
        } else {
            None
        };
        // Create global shutdown signal channel.
        let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);
        
        let (pause_tx, _) = watch::channel(false);
        // Pause Poller
        let state_clone = Arc::clone(&state);
        let pause_tx_clone = pause_tx.clone();
        let mut shutdown_rx_poller = shutdown_tx.subscribe();
        let pause_key = {
            let ns = state_clone.cache_service.namespace();
            if ns.is_empty() {
                warn!("Cache namespace is empty; set config.name to avoid cross-app pause collisions");
                "engine:pause".to_string()
            } else {
                format!("{ns}:engine:pause")
            }
        };
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let is_paused = matches!(state_clone.cache_service.get(&pause_key).await, Ok(Some(_)));
                        
                        if *pause_tx_clone.borrow() != is_paused {
                            let _ = pause_tx_clone.send(is_paused);
                            if is_paused {
                                 info!("Engine paused by global signal");
                            } else {
                                 info!("Engine resumed by global signal");
                            }
                        }
                    }
                    _ = shutdown_rx_poller.recv() => {
                        info!("Engine pause poller shutting down");
                        break;
                    }
                }
            }
        });

        let task_manager = Arc::new(TaskManager::new(Arc::clone(&state)));
        let cfg = state.config.read().await.clone();
        let _channel_config = cfg.channel_config.clone();
        let namespace = cfg.name.clone();

        let queue_manager = if let Some(qm) = queue_manager {
            qm
        } else {
            Self::init_queue_manager(&cfg)
        };

        if let Some(logger_config) = &cfg.logger {
            if logger_config.enabled.unwrap_or(true) {
                let app_config = Self::build_app_logger_config(logger_config, &namespace);
                let log_sender = Self::setup_mq_log_sender(logger_config, queue_manager.clone()).await;
                let _ = app_logger::init_logger(app_config).await;
                if let Some(sender) = log_sender {
                    let _ = app_logger::set_log_sender(sender);
                }
            }
        }

        // Initialize Logger/Event Handlers based on Config
        if let (Some(log_config), Some(event_bus)) = (&cfg.logger, event_bus.as_ref()) {
            if log_config.enabled == Some(false) {
                info!("Logger disabled; skipping EventBus log handlers");
            } else {
            use crate::common::model::logger_config::LogOutputConfig;
            use crate::engine::events::handlers::{queue_handler::QueueLogHandler, console_handler::ConsoleLogHandler};

            for output in &log_config.outputs {
                match output {
                    LogOutputConfig::Mq { .. } => {
                        let rx = event_bus.subscribe("*".to_string()).await;
                        QueueLogHandler::start(rx, queue_manager.clone(), "mq".to_string()).await;
                        info!("Registered MQ Logger for EventBus");
                    }
                    LogOutputConfig::Console { .. } => {
                        let rx = event_bus.subscribe("*".to_string()).await;
                        let level = log_config
                            .level
                            .as_deref()
                            .and_then(Self::base_level_from_filter)
                            .unwrap_or("info")
                            .to_string();
                        ConsoleLogHandler::start(rx, level).await;
                        info!("Registered Console Logger for EventBus");
                    }
                    LogOutputConfig::File { .. } => {
                        info!("Registered File Logger for EventBus (Handled by Global Tracing)");
                    }
                }
            }
            }
        } else if cfg.logger.is_some() {
            info!("EventBus disabled; skipping logger EventBus handlers");
        }

        // Initialize DownloaderManager

        // Initialize DownloaderManager
        let downloader_manager = DownloaderManager::new(Arc::clone(&state)).await;
        let proxy_manager = if let Some(proxy_config) = state.config.read().await.proxy.clone() {
            Some(Arc::new(
                ProxyManager::from_proxy_config(&proxy_config)
                    .await
                    .expect("Failed to create ProxyManager"),
            ))
        } else {
            None
        };

        let middleware_manager = MiddlewareManager::new(state.clone());
        let node_id = state
            .config
            .read()
            .await
            .crawler
            .node_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let node_registry = Arc::new(NodeRegistry::new(
            state.cache_service.clone(),
            node_id,
            Duration::from_secs(Self::NODE_HEARTBEAT_TTL_SECS),
        ));

        let redis_pool = state.redis.clone();
        let leader_elector = if let Some(pool) = redis_pool {
            let backend = Arc::new(RedisBackend::new(pool));
            let (elector, _) = LeaderElector::new(
                Some(backend),
                format!("{}:leader:cron", namespace),
                5000,
            );
            elector
        } else {
            // Single node mode or no redis - always leader
            let (elector, _) = LeaderElector::new(None, "".to_string(), 5000);
            elector
        };

        let cron_scheduler = Arc::new(CronScheduler::new(
            task_manager.clone(),
            state.clone(),
            queue_manager.clone(),
            shutdown_tx.subscribe(),
            leader_elector,
        ).await);

        let lua_registry = Arc::new(LuaScriptRegistry::new_default());

        Self {
            queue_manager,
            downloader_manager: Arc::new(downloader_manager),
            task_manager,
            proxy_manager,
            middleware_manager: Arc::new(middleware_manager),
            event_bus,
            state,
            shutdown_tx,
            pause_tx,
            prometheus_handle,
            node_registry,
            cron_scheduler,
            lua_registry,
        }
    }
    /// Register a download middleware.
    ///
    /// Download middleware intercepts requests before they are sent and responses after they are received.
    pub async fn register_download_middleware(&self, middleware: DownloadMiddlewareHandle) {
        self.middleware_manager
            .register_download_middleware(middleware)
            .await;
    }
    /// Register a data processing middleware.
    ///
    /// Data middleware processes structured data extracted from responses.
    pub async fn register_data_middleware(&self, middleware: DataMiddlewareHandle) {
        self.middleware_manager
            .register_data_middleware(middleware)
            .await;
    }
    /// Register a data storage middleware.
    ///
    /// Store middleware handles persistence of processed data.
    pub async fn register_store_middleware(&self, middleware: DataStoreMiddlewareHandle) {
        self.middleware_manager
            .register_store_middleware(middleware)
            .await;
    }

    /// Register a functional module.
    ///
    /// Modules define the crawling logic, including task generation and response parsing.
    pub async fn register_module(&self, module: Arc<dyn ModuleTrait>) {
        self.task_manager.add_module(module).await;
    }
}
