use crate::common::model::ModuleConfig;
use crate::common::model::config::DownloadConfig as DefaultDownloadConfig;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DownloadConfig {
    pub enable_session: bool,
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
                enable_session: module_config
                    .get_config_value("enable_session")
                    .and_then(|x| x.as_bool())
                    .unwrap_or(default_config.enable_session),
                enable_locker: module_config
                    .get_config_value("enable_locker")
                    .and_then(|x| x.as_bool())
                    .unwrap_or(default_config.enable_locker),
                enable_rate_limit: module_config
                    .get_config_value("enable_rate_limit")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(default_config.enable_rate_limit),
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
                enable_session: default_config.enable_session,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::config::DownloadConfig as DefaultDownloadConfig;

    #[test]
    fn load_should_fallback_to_default_when_module_value_missing() {
        let default_cfg = DefaultDownloadConfig {
            downloader_expire: 3600,
            timeout: 30,
            rate_limit: 12.5,
            enable_session: true,
            enable_locker: true,
            enable_rate_limit: true,
            cache_ttl: 60,
            wss_timeout: 30,
            pool_size: Some(10),
            max_response_size: Some(1024),
        };

        let module_cfg = ModuleConfig::default();
        let loaded = DownloadConfig::load(&Some(module_cfg), &default_cfg);

        assert!(loaded.enable_session);
        assert!(loaded.enable_locker);
        assert!(loaded.enable_rate_limit);
    }

    #[test]
    fn load_should_use_module_bool_when_present() {
        let default_cfg = DefaultDownloadConfig {
            downloader_expire: 3600,
            timeout: 30,
            rate_limit: 12.5,
            enable_session: true,
            enable_locker: true,
            enable_rate_limit: true,
            cache_ttl: 60,
            wss_timeout: 30,
            pool_size: Some(10),
            max_response_size: Some(1024),
        };

        let mut module_cfg = ModuleConfig::default();
        module_cfg.module_config = serde_json::json!({
            "enable_session": false,
            "enable_locker": false,
            "enable_rate_limit": false
        });

        let loaded = DownloadConfig::load(&Some(module_cfg), &default_cfg);

        assert!(!loaded.enable_session);
        assert!(!loaded.enable_locker);
        assert!(!loaded.enable_rate_limit);
    }
}
