use serde::{Deserialize, Serialize};
use super::config::{KafkaConfig, RedisConfig};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogOutputConfig {
    File {
        /// Log file path
        path: String,
        /// Log level (debug, info, warn, error)
        level: String,
        /// Log format (json, text)
        format: String,
        /// Rotation policy (daily, none)
        rotation: Option<String>,
    },
    RedisStream {
        /// Redis connection config
        config: RedisConfig,
        /// Stream key name
        key: String,
        /// Log level
        level: String,
        /// Log format (json, text)
        format: String,
        /// Batch size for sending logs
        batch_size: Option<usize>,
    },
    Kafka {
        /// Kafka connection config
        config: KafkaConfig,
        /// Topic name
        topic: String,
        /// Log level
        level: String,
        /// Log format (json, text)
        format: String,
        /// Batch size for sending logs
        batch_size: Option<usize>,
    },
    Console {
        /// Log level
        level: String,
        /// Log format (json, text)
        format: String,
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LoggerConfig {
    /// List of output configurations
    pub outputs: Vec<LogOutputConfig>,
    /// Global channel capacity for the main log channel
    pub channel_capacity: usize,
}
