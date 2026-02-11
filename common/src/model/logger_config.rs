use serde::{Deserialize, Serialize};
use super::config::{KafkaConfig, RedisConfig};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogOutputConfig {
    Console {
    },
    File {
        path: String,
        rotation: Option<String>,
        max_size_mb: Option<u64>,
        max_files: Option<u32>,
    },
    Mq {
        backend: String,
        topic: String,
        format: Option<String>,
        buffer: Option<usize>,
        batch_size: Option<usize>,
        compression: Option<String>,
        kafka: Option<KafkaConfig>,
        redis: Option<RedisConfig>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PrometheusLogConfig {
    pub enabled: bool,
    pub port: Option<u16>,
    pub path: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LoggerConfig {
    #[serde(alias = "enable")]
    pub enabled: Option<bool>,
    pub level: Option<String>,
    pub format: Option<String>,
    pub include: Option<Vec<String>>,
    pub buffer: Option<usize>,
    pub flush_interval_ms: Option<u64>,
    pub outputs: Vec<LogOutputConfig>,
    pub prometheus: Option<PrometheusLogConfig>,
}
