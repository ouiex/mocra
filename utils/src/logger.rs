#![allow(unused)]

use chrono;
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use time::{UtcOffset, format_description::well_known::Rfc3339};
use tokio::sync::mpsc::Sender;
use tokio::task;
use tracing::Level;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber, error};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_log::LogTracer;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::{EnvFilter, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
pub struct LogModel {
    pub task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    pub status: String,
    pub level: String,
    pub message: String,
    pub timestamp: String, // 使用字符串格式化时间戳
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>, // 可选的错误追踪信息
}

// Hold the non-blocking writer guard to keep the background logging thread alive
static FILE_GUARD: OnceCell<WorkerGuard> = OnceCell::new();

/// Log sender configuration for message queue
#[derive(Debug, Clone)]
pub struct LogSender {
    /// Sender channel for sending logs to message queue
    pub sender: Sender<LogModel>,
    /// Minimum log level for queue sender (only logs at this level or higher will be sent to queue)
    pub level: String,
}

impl LogSender {
    /// Create a new LogSender with sender and level
    pub fn new(sender: Sender<LogModel>, level: impl AsRef<str>) -> Self {
        Self {
            sender,
            level: level.as_ref().into(),
        }
    }

    /// Create a new LogSender with default warn level
    pub fn with_warn_level(sender: Sender<LogModel>) -> Self {
        Self::new(sender, "warn")
    }
}

/// Logger configuration structure
///
/// # Examples
///
/// Basic usage with console and file output:
/// ```
/// use std::path::PathBuf;
/// use crate::utils::logger::LoggerConfig;
///
/// let config = LoggerConfig::new()
///     .with_level("debug")
///     .with_file_path(PathBuf::from("./logs/app.log"))
///     .with_console(true);
/// ```
///
/// Using with message queue:
/// ```
/// use tokio::sync::mpsc;
/// use crate::utils::logger::{LoggerConfig, LogSender};
/// use crate::core::model::log::LogModel;
///
/// let (sender, receiver) = mpsc::channel::<LogModel>(1000);
/// let log_sender = LogSender::new(sender, "warn"); // Only send warn, error levels to queue
/// let config = LoggerConfig::new()
///     .with_level("info")
///     .with_console(true)
///     .with_log_sender(log_sender);
/// ```
#[derive(Debug)]
pub struct LoggerConfig {
    /// Log level filter (trace, debug, info, warn, error)
    pub level: String,
    /// Optional file path for log output
    pub file_path: Option<PathBuf>,
    /// Whether to enable console output
    pub enable_console: bool,
    /// Optional log sender for sending logs to message queue
    pub log_sender: Option<LogSender>,
}
impl LoggerConfig {
    /// Initialize the logger with this configuration
    pub async fn init(self) -> Result<(), Box<dyn std::error::Error>> {
        init_logger(self).await
    }

    /// Create a new logger configuration with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the log level
    pub fn with_level(mut self, level: impl AsRef<str>) -> Self {
        self.level = level.as_ref().into();
        self
    }

    /// Set the file path for log output
    pub fn with_file_path(mut self, path: PathBuf) -> Self {
        self.file_path = Some(path);
        self
    }

    /// Enable or disable console output
    pub fn with_console(mut self, enable: bool) -> Self {
        self.enable_console = enable;
        self
    }

    /// Set queue sender for sending logs to message queue
    pub fn with_queue_sender(mut self, sender: Sender<LogModel>) -> Self {
        self.log_sender = Some(LogSender::with_warn_level(sender));
        self
    }

    /// Set log sender for sending logs to message queue
    pub fn with_log_sender(mut self, log_sender: LogSender) -> Self {
        self.log_sender = Some(log_sender);
        self
    }

    /// Set minimum log level for queue sender (e.g., "warn" to send only warn, error, fatal to queue)
    /// This method is deprecated, use with_log_sender instead
    pub fn with_queue_level(mut self, level: impl AsRef<str>) -> Self {
        if let Some(ref mut log_sender) = self.log_sender {
            log_sender.level = level.as_ref().into();
        }
        self
    }
}

// Logger initialization flag
static LOGGER_INITIALIZED: AtomicBool = AtomicBool::new(false);

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            level: "error".to_string(),
            file_path: Some(PathBuf::from("./logs/app.log")),
            enable_console: true,
            log_sender: None,
        }
    }
}

// ---------------- Dynamic queue sender support (方案3) ----------------

struct DynamicSender {
    sender: Sender<LogModel>,
    queue_level: Level,
}

static DYNAMIC_SENDER: Lazy<RwLock<Option<DynamicSender>>> = Lazy::new(|| RwLock::new(None));

/// 设置 / 更新日志队列发送者（可在引擎初始化后调用）
pub fn set_log_sender(log_sender: LogSender) -> Result<(), Box<dyn std::error::Error>> {
    let queue_level = log_sender
        .level
        .parse::<Level>()
        .map_err(|_| format!("Invalid queue log level: {}", log_sender.level))?;
    let mut guard = DYNAMIC_SENDER
        .write()
        .expect("DYNAMIC_SENDER write lock poisoned");
    *guard = Some(DynamicSender {
        sender: log_sender.sender,
        queue_level,
    });
    Ok(())
}

/// 清除队列发送者（可选）
#[allow(dead_code)]
pub fn clear_log_sender() {
    if let Ok(mut guard) = DYNAMIC_SENDER.write() {
        *guard = None;
    }
}

/// 动态队列 Layer：初始化时添加一次，之后可随时 set_log_sender
pub struct DynamicQueueLayer;

impl<S> Layer<S> for DynamicQueueLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // 如果还没有注入 sender，直接返回
        let Some(dynamic) = DYNAMIC_SENDER
            .read()
            .ok()
            .and_then(|g| g.as_ref().map(|d| (d.sender.clone(), d.queue_level)))
        else {
            return;
        };

        // 过滤级别（只有达到 queue_level 或更高级别才发送）
        if *event.metadata().level() > dynamic.1 {
            return;
        }

        let mut visitor = LogVisitor::new();
        event.record(&mut visitor);

        let log_model = LogModel {
            task_id: visitor
                .get_field("task_id")
                .unwrap_or_else(|| "unknown".to_string()),
            request_id: visitor.get_field("request_id").and_then(|s| s.parse().ok()),
            status: visitor
                .get_field("status")
                .unwrap_or_else(|| "info".to_string()),
            level: event.metadata().level().to_string(),
            message: visitor.get_message(),
            timestamp: chrono::Local::now().to_rfc3339(),
            traceback: visitor.get_field("traceback"),
        };

        if let Err(e) = dynamic.0.try_send(log_model) {
            // Use eprintln! instead of error! to avoid recursive logging loop
            // if the queue is full or closed.
            eprintln!("Failed to enqueue log for message queue: {e}");
        }
    }
}

/// Visitor for extracting log fields
struct LogVisitor {
    fields: BTreeMap<String, String>,
    message: String,
}

impl LogVisitor {
    fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
            message: String::new(),
        }
    }

    fn get_field(&self, name: &str) -> Option<String> {
        self.fields.get(name).cloned()
    }

    fn get_message(&self) -> String {
        if !self.message.is_empty() {
            self.message.clone()
        } else if let Some(msg) = self.fields.get("message") {
            msg.clone()
        } else {
            // 构建一个包含所有字段的消息
            self.fields
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    }
}

impl Visit for LogVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let debug_str = format!("{value:?}");
        if field.name() == "message" {
            self.message = debug_str;
        } else {
            self.fields.insert(field.name().to_string(), debug_str);
        }
    }
}

/// Initialize and configure tracing logger
pub async fn init_logger(config: LoggerConfig) -> Result<(), Box<dyn std::error::Error>> {
    if LOGGER_INITIALIZED.swap(true, Ordering::SeqCst) {
        tracing::warn!("Logger already initialized, skipping re-initialization");
        return Ok(());
    }

    // bridge log crate
    let _ = LogTracer::builder()
        .with_max_level(log::LevelFilter::Trace)
        .init();

    // Normalize provided level and don't force sqlx to error so we can see SQL & params when desired
    let default_level = config.level.to_lowercase();
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // timer
    let local_offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    let timer = OffsetTime::new(local_offset, Rfc3339);

    // prepare optional console layer
    let console_layer = if config.enable_console {
        Some(fmt::layer().pretty().with_timer(timer.clone()))
    } else {
        None
    };

    // prepare optional file layer
    let file_layer = if let Some(file_path) = &config.file_path {
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_path_prefix = file_path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| "app".to_string());
        let file_appender = tracing_appender::rolling::Builder::new()
            .rotation(Rotation::DAILY)
            .filename_prefix(file_path_prefix)
            .filename_suffix("log")
            .build(
                file_path
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new(".")),
            )
            .expect("Failed to create rolling file appender");

        let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
        let _ = FILE_GUARD.set(guard);
        Some(
            fmt::layer()
                .with_ansi(false)
                .with_writer(file_writer)
                .with_timer(timer.clone()),
        )
    } else {
        None
    };

    // Build registry with Option layers (Option<T: Layer> implements Layer)
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .with(DynamicQueueLayer);

    // If initial config already 提供了 log_sender 则立即设置
    if let Some(log_sender) = config.log_sender {
        let _ = set_log_sender(log_sender);
    }

    let _ = registry.try_init();
    Ok(())
}

/// Initialize a simple logger with default configuration
/// Useful for quick setup in development or testing environments
pub async fn init_simple_logger() -> Result<(), Box<dyn std::error::Error>> {
    let config = LoggerConfig::default();
    init_logger(config).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tokio::sync::mpsc;
    use tracing::{debug, error, info, warn};

    /// Test the logger configuration builder pattern
    #[test]
    fn test_logger_config_builder() {
        let config = LoggerConfig::new()
            .with_level("debug")
            .with_file_path(PathBuf::from("./test.log"))
            .with_console(false);

        assert_eq!(config.level, "debug");
        assert_eq!(config.file_path, Some(PathBuf::from("./test.log")));
        assert!(!config.enable_console);
    }

    /// Test simple logger initialization
    #[tokio::test]
    async fn test_simple_logger_init() {
        let config = LoggerConfig::new().with_level("info");
        // This should not panic
        let _ = init_logger(config).await;
    }

    /// Test different log levels
    #[tokio::test]
    async fn test_log_levels() {
        let config = LoggerConfig::new().with_level("debug");
        let _ = init_logger(config).await;

        debug!("Debug message");
        info!("Info message");
        warn!("Warning message");
        error!("Error message");
    }

    /// Test queue logger functionality
    #[tokio::test]
    async fn test_queue_logger() {
        let (sender, mut receiver) = mpsc::channel(100);
        let log_sender = LogSender::new(sender, "warn"); // Only send warn and error to queue

        let config = LoggerConfig::new()
            .with_level("info")
            .with_console(false)
            .with_log_sender(log_sender);

        let _ = init_logger(config).await;

        // Test info level - should NOT be sent to queue (below warn level)
        info!(
            task_id = "test-task",
            status = "success",
            "Info message should not go to queue"
        );

        // Test warn level - should be sent to queue
        warn!(
            task_id = "test-task",
            status = "warning",
            "Warning message for queue"
        );

        // Wait a bit for the async task to process
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Check if we received a log in the queue (should only get the warn message)
        if let Ok(log_model) = receiver.try_recv() {
            assert_eq!(log_model.task_id, "test-task");
            assert_eq!(log_model.status, "warning");
            assert_eq!(log_model.level, "WARN");
            assert!(log_model.message.contains("Warning message for queue"));
        }

        // Verify no more messages (info should not have been sent)
        assert!(receiver.try_recv().is_err());
    }
}
