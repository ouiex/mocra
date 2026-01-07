use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Api {
    pub port: u16,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RedisConfig {
    pub redis_host: String,
    pub redis_port: u16,
    pub redis_db: u16,
    pub redis_username: Option<String>,
    pub redis_password: Option<String>,
    pub pool_size: Option<usize>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PostgresConfig {
    pub database_host: String,
    pub database_port: u16,
    pub database_user: String,
    pub database_password: String,
    pub database_name: String,
    pub database_schema: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownloadConfig {
    pub downloader_expire: u64,
    pub timeout: u32,
    pub rate_limit: f32,
    pub enable_cache: bool,
    pub enable_locker: bool,
    pub enable_rate_limit: bool,
    pub cache_ttl: u64,
    pub wss_timeout: u32,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SyncConfig {
    pub redis: Option<RedisConfig>,
    pub kafka: Option<KafkaConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CacheConfig {
    pub ttl: u64,
    pub redis: RedisConfig,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CrawlerConfig {
    pub request_max_retries: usize,
    pub task_max_errors: usize,
    pub module_max_errors: usize,
    pub module_locker_ttl: u64,
    pub proxy_path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub username: Option<String>,
    pub password: Option<String>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChannelConfig {
    pub redis: Option<RedisConfig>,
    pub kafka: Option<KafkaConfig>,
    pub compensator: Option<RedisConfig>,
    pub minid_time: u64,
    pub capacity: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    pub name: String,
    pub db: PostgresConfig,
    pub download_config: DownloadConfig,
    pub cache: CacheConfig,
    pub crawler: CrawlerConfig,
    pub redis: Option<RedisConfig>,
    pub channel_config: ChannelConfig,
    pub api: Option<Api>,
}
impl Config {
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

    use std::fs;
    use toml;
    #[test]
    fn test() {
        let config_str = fs::read_to_string("/Users/eason/crawler/crawler/config.toml")
            .expect("Failed to read config file");
        let config: Config = toml::from_str(&config_str).expect("Failed to parse config");
        println!("{config:#?}");
    }
}
