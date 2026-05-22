use crate::common::policy::PolicyConfig;
use crate::proxy::ProxyConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// API Configuration
#[derive(Serialize, Deserialize, Clone)]
pub struct Api {
    /// Port number for the API server
    pub port: u16,
    /// Optional API key for authentication
    pub api_key: Option<String>,
    /// Optional Rate Limit (requests per second)
    pub rate_limit: Option<f64>,
}

impl fmt::Debug for Api {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Api")
            .field("port", &self.port)
            .field("api_key", &self.api_key.as_ref().map(|_| "***REDACTED***"))
            .field("rate_limit", &self.rate_limit)
            .finish()
    }
}

/// Database Configuration
#[derive(Serialize, Deserialize, Clone)]
pub struct DatabaseConfig {
    /// Connection URL
    pub url: Option<String>,
    /// Database schema
    pub database_schema: Option<String>,
    /// Connection pool size
    pub pool_size: Option<u32>,
    /// Enable TLS (sslmode=require)
    pub tls: Option<bool>,
}

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("url", &self.url.as_ref().map(|_| "***REDACTED***"))
            .field("database_schema", &self.database_schema)
            .field("pool_size", &self.pool_size)
            .field("tls", &self.tls)
            .finish()
    }
}

/// Downloader Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownloadConfig {
    /// Downloader expiration time in seconds
    pub downloader_expire: u64,
    /// Request timeout in seconds
    pub timeout: u32,
    /// Rate limit in requests per second
    pub rate_limit: f32,
    /// Enable session cache/sync behavior
    pub enable_session: bool,
    /// Enable distributed locking
    pub enable_locker: bool,
    /// Enable rate limiting
    pub enable_rate_limit: bool,
    /// Cache TTL in seconds
    pub cache_ttl: u64,
    /// WebSocket timeout in seconds
    pub wss_timeout: u32,
    /// Connection pool size for HTTP client (default: 200)
    pub pool_size: Option<usize>,
    /// Maximum response size in bytes (default: 10MB)
    pub max_response_size: Option<usize>,
}

/// Synchronization Configuration
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SyncConfig {
    /// Kafka configuration for synchronization
    pub kafka: Option<KafkaConfig>,
    /// Enable versioned envelope for sync payloads (default: false)
    #[serde(default = "default_sync_envelope_enabled")]
    pub envelope_enabled: bool,
}

fn default_sync_envelope_enabled() -> bool {
    false
}

/// Explicit cache backend kind.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CacheBackendKind {
    Local,
    RaftRocksdb,
}

/// Cache Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CacheConfig {
    /// Default TTL for cache items
    pub ttl: u64,
    /// Explicit backend selection.
    pub backend: Option<CacheBackendKind>,
    /// Compression threshold in bytes (payloads larger than this will be compressed)
    pub compression_threshold: Option<usize>,
    /// Enable L1 local cache layer (default: false)
    pub enable_l1: Option<bool>,
    /// L1 cache TTL in seconds (default: 30)
    pub l1_ttl_secs: Option<u64>,
    /// L1 cache max entries before eviction (default: 10000)
    pub l1_max_entries: Option<usize>,
}

/// Crawler Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CrawlerConfig {
    /// Maximum retries for failed requests
    pub request_max_retries: usize,
    /// Maximum allowed errors per task
    pub task_max_errors: usize,
    /// Maximum allowed errors per module
    pub module_max_errors: usize,
    /// TTL for module locks
    pub module_locker_ttl: u64,
    /// Optional node ID (useful for stable node identity across restarts)
    pub node_id: Option<String>,
    /// Concurrency for task processor (JS execution)
    pub task_concurrency: Option<usize>,
    /// Concurrency for request publishing
    pub publish_concurrency: Option<usize>,
    /// Concurrency for parser task processor
    pub parser_concurrency: Option<usize>,
    /// Concurrency for error task processor
    pub error_task_concurrency: Option<usize>,
    /// Delay in milliseconds before retrying when backpressure is detected
    pub backpressure_retry_delay_ms: Option<u64>,
    /// Request deduplication TTL in seconds (default: 3600)
    pub dedup_ttl_secs: Option<u64>,
    /// Idle stop timeout in seconds (stop engine if local queues have no data for this duration)
    pub idle_stop_secs: Option<u64>,
    /// Scheduler ingress cutover fallback gate configuration.
    pub scheduler_ingress_cutover_gate: Option<SchedulerIngressCutoverGateConfig>,
}

/// Scheduler ingress cutover fallback gate configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchedulerIngressCutoverGateConfig {
    /// Consecutive scheduler failures required before blocking cutover.
    pub failure_threshold: Option<usize>,
    /// Cooldown window in seconds before allowing one probing retry.
    pub recovery_window_secs: Option<u64>,
    /// Cutover gray ratio in [0, 1], where 1 means full cutover.
    pub gray_ratio: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchedulerIngressCutoverGateConfigResolved {
    pub failure_threshold: usize,
    pub recovery_window_secs: u64,
    pub gray_ratio: f64,
}

impl Default for SchedulerIngressCutoverGateConfigResolved {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            recovery_window_secs: 60,
            gray_ratio: 1.0,
        }
    }
}

impl CrawlerConfig {
    pub fn scheduler_ingress_cutover_gate_config(
        &self,
    ) -> SchedulerIngressCutoverGateConfigResolved {
        let mut resolved = SchedulerIngressCutoverGateConfigResolved::default();
        if let Some(cfg) = self.scheduler_ingress_cutover_gate.as_ref() {
            resolved.failure_threshold = cfg
                .failure_threshold
                .unwrap_or(resolved.failure_threshold)
                .max(1);
            resolved.recovery_window_secs = cfg
                .recovery_window_secs
                .unwrap_or(resolved.recovery_window_secs);
            resolved.gray_ratio = cfg
                .gray_ratio
                .unwrap_or(resolved.gray_ratio)
                .clamp(0.0, 1.0);
        }
        resolved
    }
}

/// Scheduler Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum tolerance in seconds for missed cron jobs (default: 300)
    pub misfire_tolerance_secs: Option<i64>,
    /// Max concurrency for processing scheduled contexts (default: 100)
    pub concurrency: Option<usize>,
    /// Refresh interval in seconds for scheduler cache (default: 60)
    pub refresh_interval_secs: Option<u64>,
    /// Max allowed staleness in seconds before forcing refresh (default: 120)
    pub max_staleness_secs: Option<u64>,
}

/// Kafka Configuration
#[derive(Serialize, Deserialize, Clone)]
pub struct KafkaConfig {
    /// Comma-separated list of brokers
    pub brokers: String,
    /// SASL username
    pub username: Option<String>,
    /// SASL password
    pub password: Option<String>,
    /// Enable TLS
    pub tls: Option<bool>,
}

impl fmt::Debug for KafkaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaConfig")
            .field("brokers", &self.brokers)
            .field("username", &self.username)
            .field(
                "password",
                &self.password.as_ref().map(|_| "***REDACTED***"),
            )
            .field("tls", &self.tls)
            .finish()
    }
}

/// Blob Storage Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BlobStorageConfig {
    /// Local file system path for blob storage
    pub path: Option<String>,
}

/// Channel (Queue) Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChannelConfig {
    /// Blob storage configuration
    pub blob_storage: Option<BlobStorageConfig>,
    /// Kafka configuration for queue
    pub kafka: Option<KafkaConfig>,
    /// Minimum ID time (snowflake/uuid related)
    pub minid_time: u64,
    /// Channel capacity
    pub capacity: usize,
    /// Queue codec: json | msgpack
    pub queue_codec: Option<String>,
    /// Concurrency limit for batch flushing (default: 10)
    pub batch_concurrency: Option<usize>,
    /// Compression threshold in bytes (payloads larger than this will be compressed)
    pub compression_threshold: Option<usize>,
    /// Max retries for NACK before sending to DLQ (default: 0)
    pub nack_max_retries: Option<u32>,
    /// Backoff in milliseconds before retrying NACK (default: 0)
    pub nack_backoff_ms: Option<u64>,
    /// Additional request namespaces this node should subscribe to for federated download.
    #[serde(default)]
    pub federation_request_namespaces: Vec<String>,
    /// Optional namespace to protected API base URL mapping for remote cached-response fallback.
    #[serde(default)]
    pub federation_response_cache_api_endpoints: BTreeMap<String, String>,
}

use super::logger_config::LoggerConfig;

/// Event Bus Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventBusConfig {
    /// Channel capacity for events (default: 1024)
    pub capacity: usize,
    /// Concurrency limit for event handlers (default: 64)
    pub concurrency: usize,
}

/// Raft Consensus Configuration
///
/// When present, the node participates in Raft-based consensus for its own namespace
/// (`config.name`). Each namespace forms an independent Raft group; a new node only
/// needs ONE reachable peer address to join the cluster.
///
/// The downloader is the sole cross-namespace component and does NOT use Raft for
/// coordination — it is shared via the federation download pool instead.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftConfig {
    /// This node's Raft gRPC listen address, e.g. "0.0.0.0:7001" or "192.168.1.10:7001".
    /// Other nodes will connect to this address.
    pub addr: String,

    /// Peer Raft addresses for cluster bootstrapping / joining.
    /// A new node needs only ONE valid peer to discover and join the Raft group.
    /// Leave empty only for a brand-new single-node cluster bootstrapping itself.
    /// Example: ["192.168.1.1:7001", "192.168.1.2:7001"]
    #[serde(default)]
    pub peers: Vec<String>,

    /// Heartbeat interval in milliseconds (default: 500).
    pub heartbeat_interval_ms: Option<u64>,

    /// Election timeout in milliseconds (default: 1500).
    /// Should be significantly larger than heartbeat_interval_ms.
    pub election_timeout_ms: Option<u64>,

    /// Number of log entries between automatic snapshots (default: 500).
    pub snapshot_interval: Option<u64>,

    /// RocksDB data directory for Raft log and state machine storage.
    /// Defaults to "./raft_data/{config.name}" if not set.
    pub data_dir: Option<String>,
}

// Logger Queue Configuration (Deprecated, replaced by LoggerConfig in separate file)
// #[derive(Serialize, Deserialize, Debug, Clone)]
// pub struct LoggerConfig {
//     ...
// }

/// Main Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// Application instance name
    pub name: String,
    /// Database configuration
    pub db: DatabaseConfig,
    /// Download configuration
    pub download_config: DownloadConfig,
    /// Cache configuration
    pub cache: CacheConfig,
    /// Crawler behavior configuration
    pub crawler: CrawlerConfig,
    /// Scheduler configuration
    pub scheduler: Option<SchedulerConfig>,
    /// Synchronization configuration
    pub sync: Option<SyncConfig>,
    /// Message channel configuration
    pub channel_config: ChannelConfig,
    /// Proxy configuration
    pub proxy: Option<ProxyConfig>,
    /// API server configuration
    pub api: Option<Api>,
    /// Event Bus configuration
    pub event_bus: Option<EventBusConfig>,
    /// Logger configuration
    pub logger: Option<LoggerConfig>,
    /// Policy configuration (override default error strategies)
    pub policy: Option<PolicyConfig>,
    /// Raft consensus configuration.
    ///
    /// When set, this node joins the Raft group for `config.name` namespace.
    /// Only ONE peer address is required to bootstrap or join an existing cluster.
    /// Omit entirely to use local-only coordination.
    pub raft: Option<RaftConfig>,
}
impl Config {
    pub fn cache_backend_kind(&self) -> Option<CacheBackendKind> {
        self.cache.backend
    }

    pub fn has_raft_control_plane(&self) -> bool {
        self.raft.is_some()
    }

    /// Whether the derived deployment label is `single_node`.
    ///
    pub fn is_single_node_deployment(&self) -> bool {
        self.raft.is_none()
    }

    /// Loads configuration from a TOML file
    pub fn load(path: &str) -> Result<Self, String> {
        // Read `config.toml` file content.
        let config_str = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let config: Config = toml::from_str(&config_str).map_err(|e| e.to_string())?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use std::io::Write;
    #[allow(unused_imports)]
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_deserialization() {
        let toml_str = r#"
            name = "test_app"
            
            [db]
            url = "postgres://user:password@localhost:5432/db"
            database_schema = "public"
            
            [download_config]
            downloader_expire = 3600
            timeout = 30
            rate_limit = 10.0
            enable_session = true
            enable_locker = false
            enable_rate_limit = true
            cache_ttl = 600
            wss_timeout = 60
            
            [cache]
            ttl = 3600
            
            [crawler]
            request_max_retries = 3
            task_max_errors = 5
            module_max_errors = 10
            module_locker_ttl = 60
            
            [channel_config]
            minid_time = 0
            capacity = 1000
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.name, "test_app");
        assert_eq!(
            config.db.url.as_deref(),
            Some("postgres://user:password@localhost:5432/db")
        );
        assert_eq!(config.crawler.request_max_retries, 3);
        assert!(config.proxy.is_none());
    }

    #[test]
    fn runtime_capability_helpers_follow_configured_backends() {
        let mut config: Config = toml::from_str(
            r#"
            name = "test_app"

            [db]
            url = "postgres://user:password@localhost:5432/db"
            database_schema = "public"

            [download_config]
            downloader_expire = 3600
            timeout = 30
            rate_limit = 10.0
            enable_session = true
            enable_locker = false
            enable_rate_limit = true
            cache_ttl = 600
            wss_timeout = 60

            [cache]
            ttl = 3600

            [crawler]
            request_max_retries = 3
            task_max_errors = 5
            module_max_errors = 10
            module_locker_ttl = 60

            [channel_config]
            minid_time = 0
            capacity = 1000
        "#,
        )
        .unwrap();

        assert!(config.is_single_node_deployment());
        assert!(!config.has_raft_control_plane());

        config.raft = Some(RaftConfig {
            addr: "127.0.0.1:7101".to_string(),
            peers: Vec::new(),
            heartbeat_interval_ms: None,
            election_timeout_ms: None,
            snapshot_interval: None,
            data_dir: None,
        });

        assert!(!config.is_single_node_deployment());
        assert!(config.has_raft_control_plane());
    }

    #[test]
    fn scheduler_ingress_cutover_gate_defaults_and_overrides() {
        let config: Config = toml::from_str(
            r#"
            name = "test_app"

            [db]
            url = "postgres://user:password@localhost:5432/db"
            database_schema = "public"

            [download_config]
            downloader_expire = 3600
            timeout = 30
            rate_limit = 10.0
            enable_session = true
            enable_locker = false
            enable_rate_limit = true
            cache_ttl = 600
            wss_timeout = 60

            [cache]
            ttl = 3600

            [crawler]
            request_max_retries = 3
            task_max_errors = 5
            module_max_errors = 10
            module_locker_ttl = 60

            [crawler.scheduler_ingress_cutover_gate]
            failure_threshold = 0
            recovery_window_secs = 120
            gray_ratio = 1.5

            [channel_config]
            minid_time = 0
            capacity = 1000
        "#,
        )
        .unwrap();

        let gate = config.crawler.scheduler_ingress_cutover_gate_config();
        assert_eq!(gate.failure_threshold, 1);
        assert_eq!(gate.recovery_window_secs, 120);
        assert_eq!(gate.gray_ratio, 1.0);

        let mut without_override = config.clone();
        without_override.crawler.scheduler_ingress_cutover_gate = None;
        let default_gate = without_override
            .crawler
            .scheduler_ingress_cutover_gate_config();
        assert_eq!(default_gate.failure_threshold, 3);
        assert_eq!(default_gate.recovery_window_secs, 60);
        assert_eq!(default_gate.gray_ratio, 1.0);
    }

    #[test]
    fn channel_config_deserializes_federation_request_namespaces() {
        let config: Config = toml::from_str(
            r#"
            name = "test_app"

            [db]
            url = "postgres://user:password@localhost:5432/db"
            database_schema = "public"

            [download_config]
            downloader_expire = 3600
            timeout = 30
            rate_limit = 10.0
            enable_session = true
            enable_locker = false
            enable_rate_limit = true
            cache_ttl = 600
            wss_timeout = 60

            [cache]
            ttl = 3600

            [crawler]
            request_max_retries = 3
            task_max_errors = 5
            module_max_errors = 10
            module_locker_ttl = 60

            [channel_config]
            minid_time = 0
            capacity = 1000
            federation_request_namespaces = ["origin-a", "origin-b", "test_app"]
            federation_response_cache_api_endpoints = { origin-a = "http://127.0.0.1:7101", origin-b = "http://127.0.0.1:7102" }
        "#,
        )
        .unwrap();

        assert_eq!(
            config.channel_config.federation_request_namespaces,
            vec![
                "origin-a".to_string(),
                "origin-b".to_string(),
                "test_app".to_string()
            ]
        );
        assert_eq!(
            config
                .channel_config
                .federation_response_cache_api_endpoints
                .get("origin-a")
                .map(String::as_str),
            Some("http://127.0.0.1:7101")
        );
    }

    #[test]
    fn cache_backend_kind_deserializes_all_variants() {
        // Test the enum directly via serde
        assert_eq!(
            serde_json::from_str::<CacheBackendKind>("\"local\"").unwrap(),
            CacheBackendKind::Local
        );
        assert_eq!(
            serde_json::from_str::<CacheBackendKind>("\"raft_rocksdb\"").unwrap(),
            CacheBackendKind::RaftRocksdb
        );
    }

    #[test]
    fn cache_config_defaults_backend_to_none() {
        let config = CacheConfig {
            backend: None,
            ttl: 60,
            compression_threshold: None,
            enable_l1: None,
            l1_ttl_secs: None,
            l1_max_entries: None,
        };
        assert!(config.backend.is_none());
    }
}
