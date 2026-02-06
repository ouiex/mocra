use serde::{Deserialize, Serialize};
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

/// Redis Configuration
#[derive(Serialize, Deserialize, Clone)]
pub struct RedisConfig {
    /// Redis server hostname
    pub redis_host: String,
    /// Redis server port
    pub redis_port: u16,
    /// Redis database index
    pub redis_db: u16,
    /// Optional Redis username
    pub redis_username: Option<String>,
    /// Optional Redis password
    pub redis_password: Option<String>,
    /// Connection pool size
    pub pool_size: Option<usize>,
    /// Number of shards for stream operations
    pub shards: Option<usize>,
    /// Enable TLS for connection
    pub tls: Option<bool>,
    /// Minimum idle time in milliseconds for claiming stuck messages (default: 600000)
    pub claim_min_idle: Option<u64>,
    /// Number of messages to claim at once (default: 10)
    pub claim_count: Option<usize>,
    /// Interval in milliseconds for claiming stuck messages check (default: 60000)
    pub claim_interval: Option<u64>,
    /// Number of listener tasks for sharded multiplexing (default: 4)
    pub listener_count: Option<usize>,
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("redis_host", &self.redis_host)
            .field("redis_port", &self.redis_port)
            .field("redis_db", &self.redis_db)
            .field("redis_username", &self.redis_username)
            .field("redis_password", &self.redis_password.as_ref().map(|_| "***REDACTED***"))
            .field("pool_size", &self.pool_size)
            .field("shards", &self.shards)
            .field("tls", &self.tls)
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
    /// Enable caching of responses
    pub enable_cache: bool,
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SyncConfig {
    /// Redis configuration for synchronization
    pub redis: Option<RedisConfig>,
    /// Kafka configuration for synchronization
    pub kafka: Option<KafkaConfig>,
}

/// Cache Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CacheConfig {
    /// Default TTL for cache items
    pub ttl: u64,
    /// Redis configuration for cache storage
    pub redis: Option<RedisConfig>,
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
    /// Path to proxy configuration file
    pub proxy_path: Option<String>,
    /// Concurrency for task processor (JS execution)
    pub task_concurrency: Option<usize>,
    /// Concurrency for request publishing
    pub publish_concurrency: Option<usize>,
    /// Request deduplication TTL in seconds (default: 3600)
    pub dedup_ttl_secs: Option<u64>,
    /// Idle stop timeout in seconds (stop engine if local queues have no data for this duration)
    pub idle_stop_secs: Option<u64>,
}

/// Scheduler Configuration
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum tolerance in seconds for missed cron jobs (default: 300)
    pub misfire_tolerance_secs: Option<i64>,
    /// Max concurrency for processing scheduled contexts (default: 100)
    pub concurrency: Option<usize>,
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
            .field("password", &self.password.as_ref().map(|_| "***REDACTED***"))
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
    /// Redis configuration for queue
    pub redis: Option<RedisConfig>,
    /// Kafka configuration for queue
    pub kafka: Option<KafkaConfig>,
    /// Compensator Redis configuration (for dead letter or retry)
    pub compensator: Option<RedisConfig>,
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

/// Logger Queue Configuration (Deprecated, replaced by LoggerConfig in separate file)
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
    /// Cookie storage configuration
    pub cookie: Option<RedisConfig>,
    /// Message channel configuration
    pub channel_config: ChannelConfig,
    /// API server configuration
    pub api: Option<Api>,
    /// Event Bus configuration
    pub event_bus: Option<EventBusConfig>,
    /// Logger configuration
    pub logger: Option<LoggerConfig>,
}
impl Config {
    /// Loads configuration from a TOML file
    pub fn load(path: &str) -> Result<Self, String> {
        // 读取config.toml文件
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
            enable_cache = true
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
        assert_eq!(config.db.url.as_deref(), Some("postgres://user:password@localhost:5432/db"));
        assert_eq!(config.crawler.request_max_retries, 3);
    }
}
