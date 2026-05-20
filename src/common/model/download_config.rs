use crate::common::model::config::DownloadConfig as DefaultDownloadConfig;
use crate::common::model::workflow_profile::TaskProfileSnapshot;
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
    pub fn load_from_profile(
        profile: &TaskProfileSnapshot,
        default_config: &DefaultDownloadConfig,
    ) -> Self {
        let common = &profile.common;
        Self {
            enable_session: common.enable_session,
            enable_locker: common.module_locker,
            enable_rate_limit: common.rate_limit.is_some(),
            rate_limit: common.rate_limit.unwrap_or(default_config.rate_limit),
            cache_ttl: common
                .response_cache_ttl_secs
                .unwrap_or(default_config.cache_ttl),
            default_timeout: common.timeout_secs as u32,
            downloader: if common.downloader.is_empty() {
                "request_downloader".to_string()
            } else {
                common.downloader.clone()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::config::DownloadConfig as DefaultDownloadConfig;

    fn default_cfg() -> DefaultDownloadConfig {
        DefaultDownloadConfig {
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
        }
    }

    #[test]
    fn load_from_profile_uses_profile_fields() {
        let mut profile = TaskProfileSnapshot::default();
        profile.common.enable_session = true;
        profile.common.module_locker = true;
        profile.common.rate_limit = Some(5.0);
        profile.common.response_cache_ttl_secs = Some(120);
        profile.common.timeout_secs = 45;
        profile.common.downloader = "custom_downloader".to_string();

        let loaded = DownloadConfig::load_from_profile(&profile, &default_cfg());

        assert!(loaded.enable_session);
        assert!(loaded.enable_locker);
        assert!(loaded.enable_rate_limit);
        assert_eq!(loaded.rate_limit, 5.0);
        assert_eq!(loaded.cache_ttl, 120);
        assert_eq!(loaded.default_timeout, 45);
        assert_eq!(loaded.downloader, "custom_downloader");
    }

    #[test]
    fn load_from_profile_uses_profile_defaults() {
        let profile = TaskProfileSnapshot::default();
        let loaded = DownloadConfig::load_from_profile(&profile, &default_cfg());

        // Profile defaults are used directly — no fallback to DefaultDownloadConfig
        assert!(!loaded.enable_session);
        assert!(!loaded.enable_locker);
        assert!(!loaded.enable_rate_limit);
        assert_eq!(loaded.default_timeout, 30); // from profile.common.timeout_secs default
    }
}
