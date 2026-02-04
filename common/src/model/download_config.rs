use crate::model::ModuleConfig;
use crate::model::config::DownloadConfig as DefaultDownloadConfig;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DownloadConfig {
    pub enable_cache: bool,
    pub enable_locker: bool,
    pub enable_rate_limit: bool,
    pub rate_limit: f32,
    pub cache_ttl: u64,
    pub default_timeout: u32,
    pub downloader: String,
}

impl DownloadConfig {
    pub fn load(
        module_config: &Option<ModuleConfig>,
        default_config: &DefaultDownloadConfig,
    ) -> Self {
        if let Some(module_config) = module_config {
            Self {
                enable_cache: module_config
                    .get_config_value("enable_cache")
                    .is_some_and(|x| x.as_bool().unwrap_or(default_config.enable_cache)),
                enable_locker: module_config
                    .get_config_value("enable_locker")
                    .is_some_and(|x| x.as_bool().unwrap_or(default_config.enable_locker)),
                enable_rate_limit: module_config
                    .get_config_value("enable_rate_limit")
                    .is_some_and(|v| v.as_bool().unwrap_or(default_config.enable_rate_limit)),
                rate_limit: module_config
                    .get_config_value("rate_limit")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(default_config.rate_limit as f64) as f32,
                cache_ttl: module_config
                    .get_config_value("cache_ttl")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_config.cache_ttl),
                default_timeout: module_config
                    .get_config_value("default_timeout")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(default_config.timeout as u64)
                    as u32,
                downloader: module_config
                    .get_config_value("downloader")
                    .and_then(|v| v.as_str())
                    .unwrap_or("request_downloader")
                    .to_string(),
            }
        } else {
            Self {
                enable_cache: default_config.enable_cache,
                enable_locker: default_config.enable_locker,
                enable_rate_limit: default_config.enable_rate_limit,
                rate_limit: default_config.rate_limit,
                cache_ttl: default_config.cache_ttl,
                default_timeout: default_config.timeout,
                downloader: "request_downloader".to_string(),
            }
        }
    }
}
