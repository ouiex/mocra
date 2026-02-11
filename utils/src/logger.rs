#![allow(unused)]

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc::Sender;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::Rotation;
use tracing_log::LogTracer;
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::{EnvFilter, util::SubscriberInitExt};
use uuid::Uuid;

use crate::storage::{BlobStorage, Offloadable};

#[derive(Serialize, Deserialize)]
pub struct LogModel {
    pub task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Uuid>,
    pub status: String,
    pub level: String,
    pub message: String,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,
}

#[async_trait]
impl Offloadable for LogModel {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
}

impl crate::priority::Prioritizable for LogModel {
    fn get_priority(&self) -> crate::priority::Priority {
        match self.level.to_lowercase().as_str() {
            "error" | "fatal" => crate::priority::Priority::High,
            _ => crate::priority::Priority::Low,
        }
    }
}

#[derive(Debug)]
pub enum LogError {
    Io(std::io::Error),
    Send(String),
    Init(tracing_appender::rolling::InitError),
}

impl From<std::io::Error> for LogError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<tracing_appender::rolling::InitError> for LogError {
    fn from(err: tracing_appender::rolling::InitError) -> Self {
        Self::Init(err)
    }
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::Io(err) => write!(f, "{err}"),
            LogError::Send(msg) => write!(f, "{msg}"),
            LogError::Init(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for LogError {}

#[derive(Debug, Clone, Serialize)]
pub struct LogRecord {
    pub time: String,
    #[serde(skip)]
    pub level: Level,
    #[serde(rename = "level")]
    pub level_name: String,
    pub module: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,
}

impl LogRecord {
    fn new(level: Level, module: impl Into<String>, message: impl Into<String>) -> Self {
        let time = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let level_name = level.to_string();
        Self {
            time,
            level,
            level_name,
            module: module.into(),
            message: message.into(),
            status: None,
            event_type: None,
            phase: None,
            error_kind: None,
            trace_id: None,
            task_id: None,
            request_id: None,
            queue_topic: None,
            policy_action: None,
            policy_reason: None,
            retry_count: None,
            traceback: None,
        }
    }
}

pub trait LogSink: Send + Sync {
    fn name(&self) -> &'static str;
    fn enabled(&self) -> bool {
        true
    }
    fn min_level(&self) -> Level;
    fn emit(&self, record: &LogRecord) -> Result<(), LogError>;
    fn flush(&self) -> Result<(), LogError> {
        Ok(())
    }
}

struct LogDispatcher {
    sinks: Vec<Arc<dyn LogSink>>,
}

impl LogDispatcher {
    fn new(sinks: Vec<Arc<dyn LogSink>>) -> Self {
        Self { sinks }
    }

    fn emit(&self, record: LogRecord) {
        if self.sinks.is_empty() {
            return;
        }

        for sink in &self.sinks {
            if !sink.enabled() || record.level > sink.min_level() {
                continue;
            }
            if sink.emit(&record).is_err() {
                metrics::counter!("log_sink_errors_total", "sink" => sink.name()).increment(1);
            }
        }
    }
}

struct LogSinkLayer {
    dispatcher: Arc<LogDispatcher>,
}

impl LogSinkLayer {
    fn new(dispatcher: Arc<LogDispatcher>) -> Self {
        Self { dispatcher }
    }
}

impl<S> Layer<S> for LogSinkLayer
where
    S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = LogVisitor::new();
        event.record(&mut visitor);

        let message = if visitor.message.is_empty() {
            metadata.name().to_string()
        } else {
            visitor.message
        };

        let mut record = LogRecord::new(*metadata.level(), metadata.target(), message);
        record.status = visitor.status;
        record.event_type = visitor.event_type;
        record.phase = visitor.phase;
        record.error_kind = visitor.error_kind;
        record.trace_id = visitor.trace_id;
        record.task_id = visitor.task_id;
        record.request_id = visitor.request_id;
        record.queue_topic = visitor.queue_topic;
        record.policy_action = visitor.policy_action;
        record.policy_reason = visitor.policy_reason;
        record.retry_count = visitor.retry_count;
        record.traceback = visitor.traceback;

        self.dispatcher.emit(record);
    }
}

struct ConsoleSink {
    min_level: Level,
    writer: Mutex<tracing_appender::non_blocking::NonBlocking>,
    _guard: WorkerGuard,
}

impl ConsoleSink {
    fn new(min_level: Level) -> Self {
        let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());
        Self {
            min_level,
            writer: Mutex::new(writer),
            _guard: guard,
        }
    }
}

impl LogSink for ConsoleSink {
    fn name(&self) -> &'static str {
        "console"
    }

    fn min_level(&self) -> Level {
        self.min_level
    }

    fn emit(&self, record: &LogRecord) -> Result<(), LogError> {
        let line = format_log_record_text(record);
        if let Ok(mut writer) = self.writer.lock() {
            use std::io::Write;
            writeln!(writer, "{}", line)?;
        }
        metrics::counter!("log_events_total", "sink" => self.name(), "level" => record.level.as_str()).increment(1);
        Ok(())
    }
}

struct FileSink {
    min_level: Level,
    writer: Mutex<tracing_appender::non_blocking::NonBlocking>,
    _guard: WorkerGuard,
}

impl FileSink {
    fn new(path: &Path, min_level: Level, rotation: Rotation) -> Result<Self, LogError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file_prefix = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("app");
        let file_appender = tracing_appender::rolling::Builder::new()
            .rotation(rotation)
            .filename_prefix(file_prefix)
            .filename_suffix("log")
            .build(path.parent().unwrap_or_else(|| Path::new(".")))?;
        let (writer, guard) = tracing_appender::non_blocking(file_appender);
        Ok(Self {
            min_level,
            writer: Mutex::new(writer),
            _guard: guard,
        })
    }
}

impl LogSink for FileSink {
    fn name(&self) -> &'static str {
        "file"
    }

    fn min_level(&self) -> Level {
        self.min_level
    }

    fn emit(&self, record: &LogRecord) -> Result<(), LogError> {
        let line = format_log_record_text(record);
        if let Ok(mut writer) = self.writer.lock() {
            use std::io::Write;
            writeln!(writer, "{}", line)?;
        }
        metrics::counter!("log_events_total", "sink" => self.name(), "level" => record.level.as_str()).increment(1);
        Ok(())
    }
}

struct DynamicMqSink {
    min_level: Level,
}

impl DynamicMqSink {
    fn new(min_level: Level) -> Self {
        Self { min_level }
    }
}

impl LogSink for DynamicMqSink {
    fn name(&self) -> &'static str {
        "mq"
    }

    fn min_level(&self) -> Level {
        self.min_level
    }

    fn emit(&self, record: &LogRecord) -> Result<(), LogError> {
        let Some(dynamic) = DYNAMIC_SENDER
            .read()
            .ok()
            .and_then(|g| g.as_ref().map(|d| (d.sender.clone(), d.queue_level, d.capacity)))
        else {
            metrics::counter!("log_dropped_total", "sink" => self.name(), "reason" => "sender_unset").increment(1);
            return Ok(());
        };

        if record.level > dynamic.1 {
            return Ok(());
        }

        let status = record
            .status
            .clone()
            .or_else(|| record.phase.clone())
            .unwrap_or_else(|| "info".to_string());
        let log_model = LogModel {
            task_id: record.task_id.clone().unwrap_or_else(|| "unknown".to_string()),
            request_id: record.request_id.as_ref().and_then(|s| s.parse().ok()),
            status,
            level: record.level_name.clone(),
            message: record.message.clone(),
            timestamp: record.time.clone(),
            traceback: record.traceback.clone(),
        };

        if dynamic.0.try_send(log_model).is_err() {
            metrics::counter!("log_dropped_total", "sink" => self.name(), "reason" => "channel_full").increment(1);
        } else {
            metrics::counter!("log_events_total", "sink" => self.name(), "level" => record.level.as_str()).increment(1);
            metrics::gauge!("log_batch_size", "sink" => self.name()).set(1.0);
        }

        if let Some(capacity) = dynamic.2 {
            let remaining = dynamic.0.capacity();
            let lag = capacity.saturating_sub(remaining) as f64;
            metrics::gauge!("log_queue_lag", "sink" => self.name()).set(lag);
        }
        Ok(())
    }
}

struct PrometheusSink {
    min_level: Level,
}

impl PrometheusSink {
    fn new(min_level: Level) -> Self {
        Self { min_level }
    }
}

impl LogSink for PrometheusSink {
    fn name(&self) -> &'static str {
        "prometheus"
    }

    fn min_level(&self) -> Level {
        self.min_level
    }

    fn emit(&self, record: &LogRecord) -> Result<(), LogError> {
        metrics::counter!("log_events_total", "sink" => self.name(), "level" => record.level.as_str()).increment(1);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LogSender {
    pub sender: Sender<LogModel>,
    pub level: String,
    pub capacity: Option<usize>,
}

impl LogSender {
    pub fn new(sender: Sender<LogModel>, level: impl AsRef<str>) -> Self {
        Self {
            sender,
            level: level.as_ref().into(),
            capacity: None,
        }
    }

    pub fn with_capacity(
        sender: Sender<LogModel>,
        level: impl AsRef<str>,
        capacity: usize,
    ) -> Self {
        Self {
            sender,
            level: level.as_ref().into(),
            capacity: Some(capacity),
        }
    }

    pub fn with_warn_level(sender: Sender<LogModel>) -> Self {
        Self::new(sender, "warn")
    }
}

struct DynamicSender {
    sender: Sender<LogModel>,
    queue_level: Level,
    capacity: Option<usize>,
}

static DYNAMIC_SENDER: Lazy<RwLock<Option<DynamicSender>>> = Lazy::new(|| RwLock::new(None));

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
        capacity: log_sender.capacity,
    });
    Ok(())
}

#[allow(dead_code)]
pub fn clear_log_sender() {
    if let Ok(mut guard) = DYNAMIC_SENDER.write() {
        *guard = None;
    }
}

#[derive(Debug, Clone)]
pub enum LogOutputConfig {
    Console {
    },
    File {
        path: PathBuf,
        rotation: Option<String>,
    },
    Mq {
    },
}

#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct LoggerConfig {
    pub enabled: bool,
    pub level: String,
    pub format: String,
    pub include: Vec<String>,
    pub buffer: usize,
    pub flush_interval_ms: u64,
    pub outputs: Vec<LogOutputConfig>,
    pub prometheus: Option<PrometheusConfig>,
}

impl LoggerConfig {
    pub async fn init(self) -> Result<(), Box<dyn std::error::Error>> {
        init_logger(self).await
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_level(mut self, level: impl AsRef<str>) -> Self {
        self.level = level.as_ref().into();
        self
    }

    pub fn with_output(mut self, output: LogOutputConfig) -> Self {
        self.outputs.push(output);
        self
    }

    pub fn for_app(namespace: &str) -> Self {
        let mut config = Self::default();
        config.outputs = vec![
            LogOutputConfig::Console {},
            LogOutputConfig::File {
                path: PathBuf::from("logs").join(format!("mocra.{namespace}.log")),
                rotation: Some("daily".to_string()),
            },
        ];
        config
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            level: DEFAULT_APP_LOG_LEVEL.to_string(),
            format: "text".to_string(),
            include: vec![],
            buffer: 10000,
            flush_interval_ms: 500,
            outputs: vec![LogOutputConfig::Console {}],
            prometheus: None,
        }
    }
}

const DEFAULT_APP_LOG_LEVEL: &str =
    "info,engine=debug;sqlx=warn,sea_orm=warn";

static LOGGER_INITIALIZED: AtomicBool = AtomicBool::new(false);

pub fn is_logging_disabled() -> bool {
    let value = env::var("DISABLE_LOGS")
        .or_else(|_| env::var("MOCRA_DISABLE_LOGS"))
        .unwrap_or_default();
    matches!(
        value.trim().to_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

pub async fn init_app_logger(namespace: &str) -> Result<bool, Box<dyn std::error::Error>> {
    if is_logging_disabled() {
        return Ok(false);
    }

    let config = LoggerConfig::for_app(namespace);
    init_logger(config).await?;
    Ok(true)
}

pub async fn init_logger(config: LoggerConfig) -> Result<(), Box<dyn std::error::Error>> {
    if is_logging_disabled() {
        let _ = LOGGER_INITIALIZED.swap(true, Ordering::SeqCst);
        return Ok(());
    }
    if LOGGER_INITIALIZED.swap(true, Ordering::SeqCst) {
        tracing::warn!("Logger already initialized, skipping re-initialization");
        return Ok(());
    }

    let _ = LogTracer::builder()
        .with_max_level(log::LevelFilter::Trace)
        .init();

    let configured_filter = normalize_filter_string(&config.level);
    let filter = if configured_filter != DEFAULT_APP_LOG_LEVEL {
        EnvFilter::try_new(&configured_filter).unwrap_or_else(|_| EnvFilter::new("info"))
    } else {
        EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new(&configured_filter))
            .unwrap_or_else(|_| EnvFilter::new("info"))
    };

    let sinks = build_sinks(&config)?;
    let dispatcher = Arc::new(LogDispatcher::new(sinks));
    let layer = LogSinkLayer::new(dispatcher);

    let _ = tracing_subscriber::registry()
        .with(layer)
        .with(filter)
        .try_init();

    Ok(())
}

pub async fn init_simple_logger() -> Result<(), Box<dyn std::error::Error>> {
    let config = LoggerConfig::default();
    init_logger(config).await
}

fn build_sinks(config: &LoggerConfig) -> Result<Vec<Arc<dyn LogSink>>, LogError> {
    if !config.enabled {
        return Ok(Vec::new());
    }

    let mut sinks: Vec<Arc<dyn LogSink>> = Vec::new();
    let base_level = base_level_from_filter(&config.level).unwrap_or(Level::INFO);

    for output in &config.outputs {
        match output {
            LogOutputConfig::Console {} => {
                sinks.push(Arc::new(ConsoleSink::new(base_level)));
            }
            LogOutputConfig::File { path, rotation } => {
                let rotation = match rotation.as_deref() {
                    Some("daily") | None => Rotation::DAILY,
                    Some("hourly") => Rotation::HOURLY,
                    Some("never") => Rotation::NEVER,
                    Some("minutely") => Rotation::MINUTELY,
                    _ => Rotation::DAILY,
                };
                sinks.push(Arc::new(FileSink::new(path.as_path(), base_level, rotation)?));
            }
            LogOutputConfig::Mq {} => {
                sinks.push(Arc::new(DynamicMqSink::new(base_level)));
            }
        }
    }

    if let Some(prometheus) = &config.prometheus
        && prometheus.enabled
    {
        sinks.push(Arc::new(PrometheusSink::new(base_level)));
    }

    Ok(sinks)
}

fn normalize_filter_string(filter: &str) -> String {
    let trimmed = filter.trim();
    if trimmed.contains('=') || trimmed.contains(',') || trimmed.contains(';') {
        return trimmed.to_string();
    }
    let lower = trimmed.to_lowercase();
    let normalized = match lower.as_str() {
        "all" => "trace",
        "fatal" => "error",
        "warning" => "warn",
        other => other,
    };
    build_allowlist_filter(normalized)
}

fn build_allowlist_filter(level: &str) -> String {
    format!(
        "off,cacheable={level},common={level},downloader={level},engine={level},errors={level},js_v8={level},mocra={level},proxy={level},queue={level},sync={level},utils={level},tests={level},python_mocra={level},sqlx=warn,sea_orm=warn"
    )
}

fn base_level_from_filter(level: &str) -> Option<Level> {
    let candidate = level
        .split(|ch| ch == ',' || ch == ';')
        .next()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())?;
    candidate.parse::<Level>().ok()
}

fn format_log_record_text(record: &LogRecord) -> String {
    let mut line = format!(
        "{} [{}] {} - {}",
        record.time, record.level_name, record.module, record.message
    );

    if let Some(value) = &record.event_type {
        line.push_str(&format!(" event_type={value}"));
    }
    if let Some(value) = &record.status {
        line.push_str(&format!(" status={value}"));
    }
    if let Some(value) = &record.phase {
        line.push_str(&format!(" phase={value}"));
    }
    if let Some(value) = &record.error_kind {
        line.push_str(&format!(" error_kind={value}"));
    }
    if let Some(value) = &record.trace_id {
        line.push_str(&format!(" trace_id={value}"));
    }
    if let Some(value) = &record.task_id {
        line.push_str(&format!(" task_id={value}"));
    }
    if let Some(value) = &record.request_id {
        line.push_str(&format!(" request_id={value}"));
    }
    if let Some(value) = &record.queue_topic {
        line.push_str(&format!(" queue_topic={value}"));
    }
    if let Some(value) = &record.policy_action {
        line.push_str(&format!(" policy.action={value}"));
    }
    if let Some(value) = &record.policy_reason {
        line.push_str(&format!(" policy.reason={value}"));
    }
    if let Some(value) = &record.retry_count {
        line.push_str(&format!(" retry.count={value}"));
    }
    if let Some(value) = &record.traceback {
        line.push_str(&format!(" traceback={value}"));
    }

    line
}

struct LogVisitor {
    message: String,
    status: Option<String>,
    event_type: Option<String>,
    phase: Option<String>,
    error_kind: Option<String>,
    trace_id: Option<String>,
    task_id: Option<String>,
    request_id: Option<String>,
    queue_topic: Option<String>,
    policy_action: Option<String>,
    policy_reason: Option<String>,
    retry_count: Option<u32>,
    traceback: Option<String>,
}

impl LogVisitor {
    fn new() -> Self {
        Self {
            message: String::with_capacity(64),
            status: None,
            event_type: None,
            phase: None,
            error_kind: None,
            trace_id: None,
            task_id: None,
            request_id: None,
            queue_topic: None,
            policy_action: None,
            policy_reason: None,
            retry_count: None,
            traceback: None,
        }
    }
}

impl Visit for LogVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            use std::fmt::Write;
            let _ = write!(self.message, "{:?}", value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message.push_str(value),
            "status" => self.status = Some(value.to_string()),
            "event_type" => self.event_type = Some(value.to_string()),
            "phase" => self.phase = Some(value.to_string()),
            "error_kind" => self.error_kind = Some(value.to_string()),
            "trace_id" => self.trace_id = Some(value.to_string()),
            "task_id" => self.task_id = Some(value.to_string()),
            "request_id" => self.request_id = Some(value.to_string()),
            "queue_topic" => self.queue_topic = Some(value.to_string()),
            "policy_action" => self.policy_action = Some(value.to_string()),
            "policy_reason" => self.policy_reason = Some(value.to_string()),
            "traceback" => self.traceback = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "retry_count" {
            self.retry_count = Some(value as u32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tracing::{debug, error, info, warn};

    #[test]
    fn test_logger_config_builder() {
        let config = LoggerConfig::new()
            .with_level("debug")
            .with_output(LogOutputConfig::Console {});

        assert_eq!(config.level, "debug");
        assert!(!config.outputs.is_empty());
    }

    #[tokio::test]
    async fn test_simple_logger_init() {
        let config = LoggerConfig::new().with_level("info");
        let _ = init_logger(config).await;
    }

    #[tokio::test]
    async fn test_log_levels() {
        let config = LoggerConfig::new().with_level("debug");
        let _ = init_logger(config).await;

        debug!("Debug message");
        info!("Info message");
        warn!("Warning message");
        error!("Error message");
    }

    #[tokio::test]
    async fn test_queue_logger() {
        let (sender, mut receiver) = mpsc::channel(100);
        let log_sender = LogSender::new(sender, "warn");
        let _ = set_log_sender(log_sender);

        let config = LoggerConfig::new()
            .with_level("info")
            .with_output(LogOutputConfig::Mq {});

        let _ = init_logger(config).await;

        info!(task_id = "test-task", status = "success", "Info message should not go to queue");
        warn!(task_id = "test-task", status = "warning", "Warning message for queue");

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        if let Ok(log_model) = receiver.try_recv() {
            assert_eq!(log_model.task_id, "test-task");
            assert_eq!(log_model.status, "warning");
            assert_eq!(log_model.level, "WARN");
            assert!(log_model.message.contains("Warning message for queue"));
        }

        assert!(receiver.try_recv().is_err());
    }
}
