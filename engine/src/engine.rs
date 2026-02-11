// #![allow(unused)]
use utils::device_info::get_primary_local_ip;
use crate::zombie;
use crate::monitor::SystemMonitor;
use crate::events::{
    EventBus, RedisEventHandler,
};
use downloader::DownloaderManager;
use queue::Identifiable;

use crate::chain::{
    create_download_chain, create_error_task_chain, create_parser_chain, create_parser_task_chain,
    create_task_model_chain,
};
use crate::events::{EventEnvelope, EventPhase, EventType, HealthCheckEvent};
use common::policy::{DlqPolicy, PolicyResolver};
use metrics::counter;

use crate::chain::stream_chain::create_wss_download_chain;
use futures::{StreamExt, FutureExt};
use common::state::State;
use log::{error, info, warn};
use queue::{QueueManager, QueuedItem};

use common::processors::processor::{ProcessorContext, RetryPolicy};
use proxy::ProxyManager;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{broadcast, watch};
use common::interface::{DataMiddleware, DataStoreMiddleware, DownloadMiddleware, MiddlewareManager, ModuleTrait};
use common::registry::NodeRegistry;
use utils::connector::create_redis_pool;
use crate::task::TaskManager;
use crate::runner::ProcessorRunner;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use crate::scheduler::CronScheduler;
use sync::{LeaderElector, RedisBackend};
use uuid::Uuid;
use std::time::{Duration, Instant};
use utils::logger as app_logger;
use utils::logger::{LogOutputConfig as AppLogOutputConfig, LoggerConfig as AppLoggerConfig, LogSender as AppLogSender, PrometheusConfig as AppPrometheusConfig};

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
    pub queue_manager: Arc<QueueManager>,
    pub downloader_manager: Arc<DownloaderManager>,
    pub task_manager: Arc<TaskManager>,
    pub proxy_manager: Option<Arc<ProxyManager>>,
    pub middleware_manager: Arc<MiddlewareManager>,
    pub event_bus: Option<Arc<EventBus>>,
    pub state: Arc<State>,
    // 广播型关闭信号，所有处理器都能订阅到
    shutdown_tx: broadcast::Sender<()>,
    pause_tx: watch::Sender<bool>,
    pub prometheus_handle: Option<PrometheusHandle>,
    pub node_registry: Arc<NodeRegistry>,
    pub cron_scheduler: Arc<CronScheduler>,
}

impl Engine {
    const NODE_HEARTBEAT_INTERVAL_SECS: u64 = 10;
    const NODE_HEARTBEAT_TTL_SECS: u64 = Self::NODE_HEARTBEAT_INTERVAL_SECS * 3;

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

    fn policy_kind_label(kind: &errors::ErrorKind) -> &'static str {
        match kind {
            errors::ErrorKind::Request => "request",
            errors::ErrorKind::Response => "response",
            errors::ErrorKind::Command => "command",
            errors::ErrorKind::Service => "service",
            errors::ErrorKind::Proxy => "proxy",
            errors::ErrorKind::Download => "download",
            errors::ErrorKind::Queue => "queue",
            errors::ErrorKind::Orm => "orm",
            errors::ErrorKind::Task => "task",
            errors::ErrorKind::Module => "module",
            errors::ErrorKind::RateLimit => "rate_limit",
            errors::ErrorKind::ProcessorChain => "processor_chain",
            errors::ErrorKind::Parser => "parser",
            errors::ErrorKind::DataMiddleware => "data_middleware",
            errors::ErrorKind::DataStore => "data_store",
            errors::ErrorKind::DynamicLibrary => "dynamic_library",
            errors::ErrorKind::CacheService => "cache_service",
        }
    }

    async fn handle_policy_failure<T>(
        policy_resolver: &PolicyResolver,
        queue_manager: &QueueManager,
        topic: &str,
        event_type: &str,
        item: &T,
        err: &errors::Error,
        ack_fn: &mut Option<queue::AckFn>,
        nack_fn: &mut Option<queue::NackFn>,
    ) where
        T: serde::Serialize + queue::Identifiable + Send + Sync,
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

    async fn handle_policy_retry<T>(
        policy_resolver: &PolicyResolver,
        queue_manager: &QueueManager,
        topic: &str,
        event_type: &str,
        item: &T,
        retry_policy: &RetryPolicy,
        ack_fn: &mut Option<queue::AckFn>,
        nack_fn: &mut Option<queue::NackFn>,
    ) where
        T: serde::Serialize + queue::Identifiable + Send + Sync,
    {
        let reason = retry_policy
            .reason
            .clone()
            .unwrap_or_else(|| "retryable failure".to_string());
        let err = errors::Error::new(
            errors::ErrorKind::ProcessorChain,
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
    fn init_queue_manager(cfg: &common::model::config::Config) -> Arc<QueueManager> {
        let log_topic = cfg
            .logger
            .as_ref()
            .and_then(|logger| Self::first_mq_topic(logger));
        QueueManager::from_config_with_log_topic(cfg, log_topic.as_deref())
    }

    fn first_mq_topic(
        logger: &common::model::logger_config::LoggerConfig,
    ) -> Option<String> {
        logger.outputs.iter().find_map(|output| {
            match output {
                common::model::logger_config::LogOutputConfig::Mq { topic, .. } => {
                    Some(topic.clone())
                }
                _ => None,
            }
        })
    }

    fn build_app_logger_config(
        logger: &common::model::logger_config::LoggerConfig,
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
                common::model::logger_config::LogOutputConfig::Console {} => {
                    Some(AppLogOutputConfig::Console {})
                }
                common::model::logger_config::LogOutputConfig::File {
                    path,
                    rotation,
                    ..
                } => Some(AppLogOutputConfig::File {
                    path: PathBuf::from(path),
                    rotation: rotation.clone(),
                }),
                common::model::logger_config::LogOutputConfig::Mq { format, .. } => {
                    if let Some(format) = format
                        && format.to_lowercase() != "json"
                    {
                        eprintln!("logger.outputs.mq.format only supports json, got {format}");
                    }
                    Some(AppLogOutputConfig::Mq {})
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
        logger: &common::model::logger_config::LoggerConfig,
        queue_manager: Arc<QueueManager>,
    ) -> Option<AppLogSender> {
        let mq_output = logger.outputs.iter().find_map(|output| {
            match output {
                common::model::logger_config::LogOutputConfig::Mq { buffer, .. } => {
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

        // 创建事件总线
        let event_bus = if let Some(conf) = &state.config.read().await.event_bus {
            Some(Arc::new(EventBus::new(conf.capacity, conf.concurrency)))
        } else {
            None
        };
        // 创建关闭信号通道
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
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
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
            use common::model::logger_config::LogOutputConfig;
            use crate::events::handlers::{queue_handler::QueueLogHandler, console_handler::ConsoleLogHandler};

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
        let proxy_manager = if let Some(path) = state.config.read().await.crawler.proxy_path.clone() {
            let proxy_config = fs::read_to_string(path)
                .await
                .expect("Failed to read proxy config");
            Some(Arc::new(
                ProxyManager::from_config(&proxy_config)
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
        }
    }
    /// Register a download middleware.
    ///
    /// Download middleware intercepts requests before they are sent and responses after they are received.
    pub async fn register_download_middleware(&self, middleware: Arc<dyn DownloadMiddleware>) {
        self.middleware_manager
            .register_download_middleware(middleware)
            .await;
    }
    /// Register a data processing middleware.
    ///
    /// Data middleware processes structured data extracted from responses.
    pub async fn register_data_middleware(&self, middleware: Arc<dyn DataMiddleware>) {
        self.middleware_manager
            .register_data_middleware(middleware)
            .await;
    }
    /// Register a data storage middleware.
    ///
    /// Store middleware handles persistence of processed data.
    pub async fn register_store_middleware(&self, middleware: Arc<dyn DataStoreMiddleware>) {
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

    /// Initialize event handlers.
    async fn setup_event_handlers(&self) {
        let Some(event_bus) = &self.event_bus else {
            info!("EventBus disabled; skipping event handlers");
            return;
        };
        // Register default event handlers
        
        // Console Log Handler
        let log_rx = event_bus.subscribe("*".to_string()).await;
        crate::events::handlers::console_handler::ConsoleLogHandler::start(log_rx, "INFO".to_string()).await;

        // Metrics Handler
        // self.event_bus.subscribe("*".to_string(), MetricsEventHandler::new()).await;

        // Redis Event Handler
        let config = self.state.config.read().await;
        if let Some(redis_config) = &config.cookie {
            if let Some(pool) = create_redis_pool(&redis_config.redis_host,
                                                  redis_config.redis_port,
                                                  redis_config.redis_db,
                                                  &redis_config.redis_username,
                                                  &redis_config.redis_password,
                                                  redis_config.pool_size,
                                                  redis_config.tls.unwrap_or(false))
            {
                let redis_rx = event_bus.subscribe("*".to_string()).await;
                let redis_handler = RedisEventHandler::new(
                    Arc::new(pool),
                    config.name.clone(),
                    3600, // 1 hour TTL
                );
                redis_handler.start(redis_rx).await;
    
                info!("Redis event handler registered successfully (TLS: {})", redis_config.tls.unwrap_or(false));
            } else {
                 info!("Redis pool creation failed");
            }
        } else {
            info!("Redis not configured, skipping Redis event handler");
        }

        info!("Event handlers registered successfully");
    }

    /// Start the engine and all its background processors.
    ///
    /// This method starts:
    /// - API Server (if configured)
    /// - Event Bus
    /// - Cron Scheduler
    /// - Task Processor
    /// - Download Processor
    /// - Parser Processors
    /// - Error Processor
    /// - Health Monitor
    pub async fn start(&self) {
        info!("Starting Schedule with event-driven architecture");
        let api_config = self.state.config.read().await.api.clone();
        if let Some(api) = api_config {
            self.start_api(api.port).await;
            info!("API server started on host:  http://127.0.0.1:{}", api.port);
            if api.api_key.is_none() {
                 warn!("No API Key configured; API requests will be rejected. Set 'api.api_key' in config to enable access.");
            }
        }

        if self.event_bus.is_some() {
            // 设置事件处理器
            self.setup_event_handlers().await;

            // 启动事件总线
            if let Some(event_bus) = &self.event_bus {
                event_bus.start().await;
            }
        } else {
            info!("EventBus disabled; skipping setup and start");
        }

        // Start DownloaderManager background cleaner
        self.downloader_manager.clone().start_background_cleaner();

        // 发布系统启动事件
        if let Some(event_bus) = &self.event_bus {
            if let Err(e) = event_bus
                .publish(EventEnvelope::engine(
                    EventType::SystemHealth,
                    EventPhase::Started,
                    serde_json::json!({ "event": "system_started" }),
                ))
                .await
            {
                error!("Failed to publish system started event: {e}");
            }
        }

        // Spawn signal handler
        let shutdown_tx = self.shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(()) = tokio::signal::ctrl_c().await {
                 info!("Received Ctrl+C, initiating shutdown...");
                 let _ = shutdown_tx.send(());
            }
        });

        // Start Zombie Task Cleaner in background
        let state_for_zombie = self.state.clone();
        tokio::spawn(async move {
            zombie::start_zombie_cleaner(state_for_zombie, 600).await; // 10 minutes timeout default
        });

        // Start System Monitor
        let state_for_monitor = self.state.clone();
        tokio::spawn(async move {
             SystemMonitor::new(15).run(state_for_monitor).await; // 15 seconds interval
        });

        // Idle Stop Monitor (based on local queue pending data)
        let idle_stop_secs = self
            .state
            .config
            .read()
            .await
            .crawler
            .idle_stop_secs
            .unwrap_or(0);
        if idle_stop_secs > 0 {
            let queue_manager = self.queue_manager.clone();
            let shutdown_tx = self.shutdown_tx.clone();
            let mut shutdown_rx = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                let mut last_active = Instant::now();

                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Idle stop monitor shutting down");
                            break;
                        }
                        _ = interval.tick() => {
                            let pending = queue_manager.local_pending_count().await;
                            if pending > 0 {
                                last_active = Instant::now();
                                continue;
                            }

                            if last_active.elapsed().as_secs() >= idle_stop_secs {
                                info!("No local queue data for {}s, initiating shutdown", idle_stop_secs);
                                let _ = shutdown_tx.send(());
                                break;
                            }
                        }
                    }
                }
            });
        }

        // Start Cron Scheduler
        self.start_cron_scheduler();

        info!("Starting all processors concurrently...");

        // 并发启动所有处理器
        macro_rules! run_processor {
            ($name:expr, $fut:expr) => {
                async {
                    while let Err(e) = std::panic::AssertUnwindSafe($fut).catch_unwind().await {
                         error!("{} panicked: {:?}. Restarting...", $name, e);
                         tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                }
            };
        }

        tokio::join!(
            run_processor!("TaskProcessor", self.start_task_processor()),
            run_processor!("DownloadProcessor", self.start_download_processor()),
            run_processor!("ParserProcessor", self.start_parser_model_processor()),
            run_processor!("ErrorProcessor", self.start_error_processor()),
            run_processor!("HealthMonitor", self.start_health_monitor()),
            run_processor!("ResponseProcessor", self.start_response_parser_processor()),
            run_processor!("NodeRegistry", self.start_node_registry()),
        );
    }

    async fn start_node_registry(&self) {
        info!("Starting node registry heartbeat");
        let registry = self.node_registry.clone();
        let mut shutdown = self.shutdown_tx.subscribe();
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(Self::NODE_HEARTBEAT_INTERVAL_SECS));
        
        let hostname = std::env::var("COMPUTERNAME").or(std::env::var("HOSTNAME")).unwrap_or("unknown".to_string());
        let ip = get_primary_local_ip().map(|ip| ip.to_string()).unwrap_or_else(|_| "127.0.0.1".to_string());
        
        loop {
             tokio::select! {
                _ = shutdown.recv() => {
                    info!("Node registry heartbeat received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = registry.heartbeat(&ip, &hostname, env!("CARGO_PKG_VERSION")).await {
                         error!("Failed to send heartbeat: {}", e);
                    }
                }
             }
        }
    }

    async fn run_processor_loop<T, F, Fut>(
        &self,
        name: &str,
        receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<T>>>,
        execute_fn: F,
    )
    where
        T: queue::Identifiable + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let concurrency = self.state.config.read().await.crawler.task_concurrency.unwrap_or(2048);
        
        let runner = ProcessorRunner::new(
            name,
            self.shutdown_tx.subscribe(),
            self.pause_tx.subscribe(),
            concurrency,
        );

        runner.run(receiver, execute_fn).await;
    }

    /// 启动任务处理器
    async fn start_task_processor(&self) {
        let task_model_chain = Arc::new(
            create_task_model_chain(
                self.task_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.clone(),
            )
                .await,
        );
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());

        self.run_processor_loop(
            "Task",
            self.queue_manager.get_task_pop_channel(),
            move |task_item| {
                let chain = task_model_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (task, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let task_for_dlq = task.clone();
                    let id = task.get_id();
                    let result = chain.execute(task, ProcessorContext::default()).await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(mut stream) => {
                            while stream.next().await.is_some() {}
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("task", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack task {}: {}", id, e);
                                }
                            }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "task",
                                "task_model",
                                &task_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "task",
                                "task_model",
                                &task_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        ).await;
    }

    /// 启动下载处理器
    async fn start_download_processor(&self) {
        info!("Starting download processor");
        let download_chain = Arc::new(
            create_download_chain(
                self.state.clone(),
                self.downloader_manager.clone(),
                self.queue_manager.clone(),
                self.middleware_manager.clone(),
                self.event_bus.clone(),
                self.proxy_manager.clone(),
            )
                .await,
        );
        let wss_download_chain = Arc::new(
            create_wss_download_chain(
                self.state.clone(),
                self.downloader_manager.clone(),
                self.queue_manager.clone(),
                self.middleware_manager.clone(),
                self.state.cache_service.clone(),
                self.event_bus.clone(),
                self.proxy_manager.clone(),
            )
                .await,
        );
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());

        self.run_processor_loop(
            "Download",
            self.queue_manager.get_request_pop_channel(),
            move |request_item| {
                let download_chain = download_chain.clone();
                let wss_chain = wss_download_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (request, mut ack_fn, mut nack_fn) = request_item.into_parts();
                    let request_for_dlq = request.clone();
                    let id = request.get_id();
                    
                    let result = if request.downloader.eq_ignore_ascii_case("wss_downloader") {
                        wss_chain.execute(request, ProcessorContext::default()).await
                    } else {
                        download_chain.execute(request, ProcessorContext::default()).await
                    };

                    match result {
                        common::processors::processor::ProcessorResult::Success(_) => {
                        if let Some(comp) = &queue_manager.compensator {
                            let _ = comp.remove_task("request", &id).await;
                        }
                        if let Some(f) = ack_fn.take() {
                            if let Err(e) = f().await {
                                error!("Failed to ack request {}: {}", id, e);
                            }
                        }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "request",
                                "download",
                                &request_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "request",
                                "download",
                                &request_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        ).await;
    }

    /// 启动解析处理器
    async fn start_parser_model_processor(&self) {
        info!("Starting parser processor");
        let parser_model_chain = Arc::new(
            create_parser_task_chain(
                self.task_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.clone(),
            )
                .await,
        );
        let queue_manager = self.queue_manager.clone();
        let state = self.state.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());

        self.run_processor_loop(
            "Parser",
            self.queue_manager.get_parser_task_pop_channel(),
            move |task_item| {
                let chain = parser_model_chain.clone();
                let queue_manager = queue_manager.clone();
                let state = state.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (task, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let task_for_dlq = task.clone();
                    let id = task.get_id();
                    
                    // Idempotency check: Prevent duplicate processing
                    let processed_key = format!("{}:processed:parser:{}", state.cache_service.namespace(), id);
                    let lock_key = format!("{}:lock:parser:{}", state.cache_service.namespace(), id);
                    
                    // 1. Check if already processed
                    if let Ok(Some(_)) = state.cache_service.get(&processed_key).await {
                         info!("ParserTask {} already processed, skipping", id);
                         if let Some(f) = ack_fn.take() {
                             let _ = f().await;
                         }
                        return;
                    }
                    
                    // 2. Try to acquire lock to prevent concurrent processing
                    match state.cache_service.set_nx(&lock_key, "1".as_bytes(), Some(std::time::Duration::from_secs(300))).await {
                        Ok(false) => {
                            // Locked by another worker
                            return;
                        },
                        Err(e) => {
                            error!("Failed to acquire lock for task {}: {}", id, e);
                            return;
                        },
                        Ok(true) => {}
                    }
                    
                    // Ensure lock is released even if panic?
                    // Rust async drop doesn't guarantee this easily without a Guard struct.
                    // For now, relies on TTL (5m).

                    let result = chain.execute(task, ProcessorContext::default()).await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(mut stream) => {
                        while stream.next().await.is_some() {}
                        if let Some(comp) = &queue_manager.compensator {
                            let _ = comp.remove_task("parser_task", &id).await;
                        }
                        
                        // Mark as processed (24h TTL)
                        let _ = state.cache_service.set(&processed_key, "1".as_bytes(), Some(std::time::Duration::from_secs(86400))).await;
                        // Release lock
                        let _ = state.cache_service.del(&lock_key).await;

                        if let Some(f) = ack_fn.take() {
                            if let Err(e) = f().await {
                                error!("Failed to ack parser task {}: {}", id, e);
                            }
                        }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            let _ = state.cache_service.del(&lock_key).await;
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "parser_task",
                                "parser_task_model",
                                &task_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                        // Failed, release lock immediately so it can be retried
                            let _ = state.cache_service.del(&lock_key).await;
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "parser_task",
                                "parser_task_model",
                                &task_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        ).await;
    }

    /// 启动错误处理器
    async fn start_error_processor(&self) {
        info!("Starting error processor");
        let error_chain = Arc::new(
            create_error_task_chain(
                self.task_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.clone(),
            )
                .await,
        );
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());

        self.run_processor_loop(
            "Error",
            self.queue_manager.get_error_pop_channel(),
            move |task_item| {
                let chain = error_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (task, mut ack_fn, mut nack_fn) = task_item.into_parts();
                    let task_for_dlq = task.clone();
                    let id = task.get_id();
                    let result = chain.execute(task, ProcessorContext::default()).await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(mut stream) => {
                            while stream.next().await.is_some() {}
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("error_task", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                    error!("Failed to ack error task {}: {}", id, e);
                                }
                            }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "error_task",
                                "system_error",
                                &task_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "error_task",
                                "system_error",
                                &task_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        ).await;
    }
    async fn start_response_parser_processor(&self) {
        info!("Starting response processor");
        let parser_chain = Arc::new(
            create_parser_chain(
                self.state.clone(),
                self.task_manager.clone(),
                self.middleware_manager.clone(),
                self.queue_manager.clone(),
                self.event_bus.clone(),
                self.state.cache_service.clone(),
            )
                .await,
        );
        let queue_manager = self.queue_manager.clone();
        let policy_resolver = PolicyResolver::new(self.state.config.read().await.policy.as_ref());

        self.run_processor_loop(
            "Response",
            self.queue_manager.get_response_pop_channel(),
            move |response_item| {
                let chain = parser_chain.clone();
                let queue_manager = queue_manager.clone();
                let policy_resolver = policy_resolver.clone();
                async move {
                    let (response, mut ack_fn, mut nack_fn) = response_item.into_parts();
                    let response_for_dlq = response.clone();
                    let id = response.get_id();
                    let result = chain.execute(response, ProcessorContext::default()).await;
                    match result {
                        common::processors::processor::ProcessorResult::Success(_) => {
                            if let Some(comp) = &queue_manager.compensator {
                                let _ = comp.remove_task("response", &id).await;
                            }
                            if let Some(f) = ack_fn.take() {
                                if let Err(e) = f().await {
                                     error!("Failed to ack response {}: {}", id, e);
                                }
                            }
                        }
                        common::processors::processor::ProcessorResult::RetryableFailure(retry_policy) => {
                            Self::handle_policy_retry(
                                &policy_resolver,
                                &queue_manager,
                                "response",
                                "parser",
                                &response_for_dlq,
                                &retry_policy,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                        common::processors::processor::ProcessorResult::FatalFailure(err) => {
                            Self::handle_policy_failure(
                                &policy_resolver,
                                &queue_manager,
                                "response",
                                "parser",
                                &response_for_dlq,
                                &err,
                                &mut ack_fn,
                                &mut nack_fn,
                            )
                            .await;
                        }
                    }
                }
            },
        ).await;
    }
    /// 启动健康监控
    async fn start_health_monitor(&self) {
        info!("Starting health monitor");

        let event_bus = self.event_bus.clone();
        let mut shutdown = self.shutdown_tx.subscribe();

        // 每30秒进行一次健康检查
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Health monitor received shutdown signal, exiting loop");
                    break;
                }
                _ = interval.tick() => {
                    // Clean up expired rate limit keys
                    match self.state.limiter.cleanup().await {
                         Ok(count) => if count > 0 { info!("Cleaned up {} expired rate limit keys", count); },
                         Err(e) => error!("Failed to cleanup rate limit keys: {:?}", e),
                    }

                    // 发布健康检查事件
                    let health_event = EventEnvelope::system_health(
                        HealthCheckEvent {
                            component: "schedule".to_string(),
                            status: "healthy".to_string(),
                            metrics: serde_json::json!({
                                "uptime_seconds": std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs(),
                                "processors": {
                                    "task": "running",
                                    "download": "running",
                                    "parser": "running",
                                    "error": "running"
                                }
                            }),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        },
                        EventPhase::Completed,
                    );

                    if let Some(event_bus) = &event_bus {
                        if let Err(e) = event_bus.publish(health_event).await {
                            error!("Failed to publish health check event: {e}");
                        }
                    }
                }
            }
        }
    }

    /// Shutdown the engine gracefully.
    ///
    /// Sends shutdown signals to all processors and waits for cleanup.
    pub async fn shutdown(&self) {
        info!("Shutting down Schedule");

        if let Err(e) = self.node_registry.deregister().await {
            warn!("Failed to deregister node: {}", e);
        }
        // 通知所有后台处理器优雅退出
        let _ = self.shutdown_tx.send(());

        // 发布系统关闭事件
        if let Some(event_bus) = &self.event_bus {
            if let Err(e) = event_bus
                .publish(EventEnvelope::engine(
                    EventType::SystemHealth,
                    EventPhase::Completed,
                    serde_json::json!({ "event": "system_shutdown" }),
                ))
                .await
            {
                error!("Failed to publish system shutdown event: {e}");
            }

            // 停止事件总线
            event_bus.stop();
        }

        info!("Schedule shutdown completed");
    }

    /// 获取系统统计信息
    pub async fn get_system_stats(&self) -> serde_json::Value {
        serde_json::json!({
            "processors": {
                "task": "active",
                "download": "active",
                "parser": "active",
                "error": "active"
            },
            "event_bus": if self.event_bus.is_some() { "active" } else { "disabled" },
            "uptime": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        })
    }

    /// Start the API server on the specified port.
    ///
    /// # Arguments
    /// * `port` - The port number to listen on.
    pub async fn start_api(&self, port: u16) {
        let queue_manager = self.queue_manager.clone();
        let prometheus_handle = self.prometheus_handle.clone();
        let state = self.state.clone();
        let node_registry = self.node_registry.clone();
        tokio::spawn(async move {
            let api_state = crate::api::state::ApiState { 
                queue_manager, 
                prometheus_handle,
                state,
                node_registry,
            };
            let app = crate::api::router::router(api_state);
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    info!("Successfully bound to address {:?}", listener);
                    listener
                }
                Err(e) => {
                    error!("Failed to bind to address {:?}", e);
                    std::process::exit(1);
                }
            };
            match axum::serve(listener, app.into_make_service()).await {
                Ok(_) => {
                    info!("API server stopped gracefully");
                }
                Err(e) => {
                    error!("API server error: {:?}", e);
                    std::process::exit(1);
                }
            }
        });
    }

    /// 启动Cron调度器
    pub fn start_cron_scheduler(&self) {
        info!("Starting cron scheduler");
        self.cron_scheduler.clone().start();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use common::model::Request;
    use common::policy::{PolicyConfig, PolicyOverride};
    use errors::ErrorKind;
    use queue::{Message, MqBackend};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::mpsc;

    #[derive(Clone, Default)]
    struct TestBackend {
        dlq_entries: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[async_trait]
    impl MqBackend for TestBackend {
        async fn publish(&self, _topic: &str, _key: Option<&str>, _payload: &[u8]) -> errors::Result<()> {
            Ok(())
        }

        async fn publish_with_headers(
            &self,
            _topic: &str,
            _key: Option<&str>,
            _payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> errors::Result<()> {
            Ok(())
        }

        async fn publish_batch_with_headers(
            &self,
            _topic: &str,
            _items: &[(Option<String>, Vec<u8>, HashMap<String, String>)],
        ) -> errors::Result<()> {
            Ok(())
        }

        async fn subscribe(&self, _topic: &str, _sender: mpsc::Sender<Message>) -> errors::Result<()> {
            Ok(())
        }

        async fn clean_storage(&self) -> errors::Result<()> {
            Ok(())
        }

        async fn send_to_dlq(
            &self,
            topic: &str,
            _id: &str,
            _payload: &[u8],
            reason: &str,
        ) -> errors::Result<()> {
            self.dlq_entries
                .lock()
                .unwrap()
                .push((topic.to_string(), reason.to_string()));
            Ok(())
        }

        async fn read_dlq(&self, _topic: &str, _count: usize) -> errors::Result<Vec<(String, Vec<u8>, String, String)>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn retryable_failure_policy_can_route_to_dlq() {
        let backend = Arc::new(TestBackend::default());
        let queue_manager = QueueManager::new(Some(backend.clone()), 10);

        let policy_cfg = PolicyConfig {
            overrides: vec![PolicyOverride {
                domain: Some("engine".to_string()),
                event_type: Some("download".to_string()),
                phase: Some("retry".to_string()),
                kind: ErrorKind::ProcessorChain,
                retryable: Some(false),
                backoff: None,
                dlq: Some(DlqPolicy::Always),
                alert: None,
                max_retries: Some(0),
                backoff_ms: Some(0),
            }],
        };

        let policy_resolver = PolicyResolver::new(Some(&policy_cfg));
        let request = Request::new("http://example.com", "GET");

        let acked = Arc::new(AtomicBool::new(false));
        let nacked = Arc::new(AtomicBool::new(false));

        let ack_flag = acked.clone();
        let mut ack_fn: Option<queue::AckFn> = Some(Box::new(move || {
            let ack_flag = ack_flag.clone();
            Box::pin(async move {
                ack_flag.store(true, Ordering::SeqCst);
                Ok(())
            })
        }));

        let nack_flag = nacked.clone();
        let mut nack_fn: Option<queue::NackFn> = Some(Box::new(move |_reason| {
            let nack_flag = nack_flag.clone();
            Box::pin(async move {
                nack_flag.store(true, Ordering::SeqCst);
                Ok(())
            })
        }));

        let retry_policy = RetryPolicy::default().with_reason("unit test".to_string());

        Engine::handle_policy_retry(
            &policy_resolver,
            &queue_manager,
            "request",
            "download",
            &request,
            &retry_policy,
            &mut ack_fn,
            &mut nack_fn,
        )
        .await;

        assert!(acked.load(Ordering::SeqCst));
        assert!(!nacked.load(Ordering::SeqCst));

        let dlq_entries = backend.dlq_entries.lock().unwrap();
        assert_eq!(dlq_entries.len(), 1);
        assert_eq!(dlq_entries[0].0, "request");
    }

    use queue::{QueuedItem, Identifiable};


    #[derive(Clone)]
    struct TestItem {
        id: String,
    }

    impl Identifiable for TestItem {
        fn get_id(&self) -> String {
            self.id.clone()
        }
    }

    #[tokio::test]
    async fn test_processor_failure_triggers_nack() {
        let ack_count = Arc::new(Mutex::new(0u32));
        let nack_reason = Arc::new(Mutex::new(None::<String>));

        let ack_count_clone = ack_count.clone();
        let nack_reason_clone = nack_reason.clone();

        let item = QueuedItem::with_ack(
            TestItem { id: "test-1".to_string() },
            move || {
                let ack_count_clone = ack_count_clone.clone();
                Box::pin(async move {
                    *ack_count_clone.lock().unwrap() += 1;
                    Ok(())
                })
            },
            move |reason| {
                let nack_reason_clone = nack_reason_clone.clone();
                Box::pin(async move {
                    *nack_reason_clone.lock().unwrap() = Some(reason);
                    Ok(())
                })
            },
        );

        let (task, mut ack_fn, mut nack_fn) = item.into_parts();
        let _id = task.get_id();
        let failed = true;

        if !failed {
            if let Some(f) = ack_fn.take() {
                let _ = f().await;
            }
        } else if let Some(f) = nack_fn.take() {
            let _ = f("processor failed".to_string()).await;
        }

        assert_eq!(*ack_count.lock().unwrap(), 0);
        assert_eq!(
            nack_reason.lock().unwrap().as_deref(),
            Some("processor failed")
        );
    }
}
