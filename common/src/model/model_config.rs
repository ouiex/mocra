use crate::model::config::DatabaseConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use cacheable::CacheAble;

/// Module-level configuration snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleConfig {
    pub account_config: serde_json::Value,  // Configuration loaded from account table.
    pub platform_config: serde_json::Value, // Configuration loaded from platform table.
    pub module_config: serde_json::Value,   // Configuration defined on the module itself.
    pub data_middleware_config: HashMap<String, serde_json::Value>, // Data middleware configs.
    pub download_middleware_config: HashMap<String, serde_json::Value>, // Download middleware configs.
    pub rel_account_platform_config: serde_json::Value,
    pub rel_module_account_config: serde_json::Value, // Module-account relation config.
    pub rel_module_platform_config: serde_json::Value, // Module-platform relation config.
    pub rel_module_data_middleware_config: HashMap<String, serde_json::Value>, // Module-data-middleware relation config.
    pub rel_module_download_middleware_config: HashMap<String, serde_json::Value>, // Module-download-middleware relation config.
}
impl Default for ModuleConfig {
    fn default() -> Self {
        ModuleConfig {
            account_config: serde_json::Value::Object(serde_json::Map::new()),
            platform_config: serde_json::Value::Object(serde_json::Map::new()),
            module_config: serde_json::Value::Object(serde_json::Map::new()),
            data_middleware_config: HashMap::new(),
            download_middleware_config: HashMap::new(),
            rel_account_platform_config: serde_json::Value::Object(serde_json::Map::new()),
            rel_module_account_config: serde_json::Value::Object(serde_json::Map::new()),
            rel_module_platform_config: serde_json::Value::Object(serde_json::Map::new()),
            rel_module_data_middleware_config: HashMap::new(),
            rel_module_download_middleware_config: HashMap::new(),
        }
    }
}
impl ModuleConfig {
    /// Returns merged configuration with priority: `module_config > rel_platform_config >`
    /// rel_account_config > rel_data_middleware_config > rel_download_middleware_config >
    /// platform_config > account_config
    pub fn get_merged_config(&self) -> serde_json::Value {
        let mut merged = serde_json::Map::new();

        // Merge from lower priority to higher priority.

        // 1. Lowest priority: account base config.
        if let serde_json::Value::Object(account_config) = &self.account_config {
            for (key, value) in account_config {
                merged.insert(key.clone(), value.clone());
            }
        }

        // 2. Platform base config.
        if let serde_json::Value::Object(platform_config) = &self.platform_config {
            for (key, value) in platform_config {
                merged.insert(key.clone(), value.clone());
            }
        }

        // 3. Download middleware base config.
        for (_, config) in self.download_middleware_config.iter() {
            if let serde_json::Value::Object(download_config) = config {
                for (key, value) in download_config {
                    merged.insert(key.clone(), value.clone());
                }
            }
        }

        // 4. Data middleware base config.
        for (_, config) in self.data_middleware_config.iter() {
            if let serde_json::Value::Object(data_config) = config {
                for (key, value) in data_config {
                    merged.insert(key.clone(), value.clone());
                }
            }
        }

        if let serde_json::Value::Object(module_config) = &self.rel_account_platform_config {
            for (key, value) in module_config {
                merged.insert(key.clone(), value.clone());
            }
        }

        // 5. Module-download-middleware relation config.
        for (_, config) in self.rel_module_download_middleware_config.iter() {
            if let serde_json::Value::Object(rel_download_config) = config {
                for (key, value) in rel_download_config {
                    merged.insert(key.clone(), value.clone());
                }
            }
        }

        // 6. Module-data-middleware relation config.
        for (_, config) in self.rel_module_data_middleware_config.iter() {
            if let serde_json::Value::Object(rel_data_config) = config {
                for (key, value) in rel_data_config {
                    merged.insert(key.clone(), value.clone());
                }
            }
        }

        // 7. Module-account relation config.
        if let serde_json::Value::Object(rel_account_config) = &self.rel_module_account_config {
            for (key, value) in rel_account_config {
                merged.insert(key.clone(), value.clone());
            }
        }

        // 8. Module-platform relation config.
        if let serde_json::Value::Object(rel_platform_config) = &self.rel_module_platform_config {
            for (key, value) in rel_platform_config {
                merged.insert(key.clone(), value.clone());
            }
        }

        // 9. Highest priority: module self config.
        if let serde_json::Value::Object(module_config) = &self.module_config {
            for (key, value) in module_config {
                merged.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(merged)
    }

    /// Returns the configuration value for a key using priority-based lookup.
    pub fn get_config_value(&self, key: &str) -> Option<&serde_json::Value> {
        // 1. Highest priority: module self config.
        if let serde_json::Value::Object(module_config) = &self.module_config
            && let Some(value) = module_config.get(key) {
                return Some(value);
            }

        // 2. Module-platform relation config.
        if let serde_json::Value::Object(rel_platform_config) = &self.rel_module_platform_config
            && let Some(value) = rel_platform_config.get(key) {
                return Some(value);
            }

        // 3. Module-account relation config.
        if let serde_json::Value::Object(rel_account_config) = &self.rel_module_account_config
            && let Some(value) = rel_account_config.get(key) {
                return Some(value);
            }

        // 4. Module-data-middleware relation config.
        for (_, config) in self.rel_module_data_middleware_config.iter() {
            if let serde_json::Value::Object(rel_data_config) = config
                && let Some(value) = rel_data_config.get(key) {
                    return Some(value);
                }
        }

        // 5. Module-download-middleware relation config.
        for (_, config) in self.rel_module_download_middleware_config.iter() {
            if let serde_json::Value::Object(rel_download_config) = config
                && let Some(value) = rel_download_config.get(key) {
                    return Some(value);
                }
        }

        if let serde_json::Value::Object(rel_account_platform_config) =
            &self.rel_account_platform_config
            && let Some(value) = rel_account_platform_config.get(key) {
                return Some(value);
            }
        // 6. Platform base config.
        if let serde_json::Value::Object(platform_config) = &self.platform_config
            && let Some(value) = platform_config.get(key) {
                return Some(value);
            }
        // Account base config.
        if let serde_json::Value::Object(account_config) = &self.account_config
            && let Some(value) = account_config.get(key) {
                return Some(value);
            }
        // 7. Data middleware base config.
        for (_, config) in self.download_middleware_config.iter() {
            if let serde_json::Value::Object(data_config) = config
                && let Some(value) = data_config.get(key) {
                    return Some(value);
                }
        }

        // 8. Download middleware base config.
        for (_, config) in self.data_middleware_config.iter() {
            if let serde_json::Value::Object(download_config) = config
                && let Some(value) = download_config.get(key) {
                    return Some(value);
                }
        }
        None
    }
    pub fn get_middleware_weight(&self, middleware_name: &str) -> Option<u32> {
        if let Some(serde_json::Value::Object(obj)) = self.download_middleware_config.get(middleware_name)
            && let Some(weight) = obj.get("weight") {
                return weight.as_u64().map(|v| v as u32);
            }
        if let Some(serde_json::Value::Object(obj)) = self
            .rel_module_download_middleware_config
            .get(middleware_name)
            && let Some(weight) = obj.get("weight") {
                return weight.as_u64().map(|v| v as u32);
            }
        None
    }
    pub fn get_task_config(&self, key: &str) -> Option<&serde_json::Value> {
        if let serde_json::Value::Object(account_config) = &self.account_config
            && let Some(value) = account_config.get(key) {
                return Some(value);
            }
        if let serde_json::Value::Object(platform_config) = &self.platform_config
            && let Some(value) = platform_config.get(key) {
                return Some(value);
            }
        if let serde_json::Value::Object(rel_account_platform_config) =
            &self.rel_account_platform_config
            && let Some(value) = rel_account_platform_config.get(key) {
                return Some(value);
            }
        None
    }
    pub fn get_postgres_config(&self) -> Option<DatabaseConfig> {
        for (_, rel_module_data_middleware_config) in self.rel_module_data_middleware_config.iter()
        {
            if let Some(value) = rel_module_data_middleware_config.get("postgres")
                && let Ok(config) = serde_json::from_value::<DatabaseConfig>(value.clone()) {
                    return Some(config);
                }
        }
        if let Some(module_config) = self.module_config.get("postgres")
            && let Ok(config) = serde_json::from_value::<DatabaseConfig>(module_config.clone()) {
                return Some(config);
            }

        for (_, data_middleware_config) in self.data_middleware_config.iter() {
            if let Some(value) = data_middleware_config.get("postgres")
                && let Ok(config) = serde_json::from_value::<DatabaseConfig>(value.clone()) {
                    return Some(config);
                }
        }
        if let Some(account_config) = self.account_config.get("postgres")
            && let Ok(config) = serde_json::from_value::<DatabaseConfig>(account_config.clone()) {
                return Some(config);
            }
        if let Some(platform_config) = self.platform_config.get("postgres")
            && let Ok(config) = serde_json::from_value::<DatabaseConfig>(platform_config.clone()) {
                return Some(config);
            }

        None
    }
    pub fn get_config<T: serde::de::DeserializeOwned>(&self, key: &str) -> Option<T> {
        if let Some(value) = self.get_config_value(key)
            && let Ok(config) = serde_json::from_value::<T>(value.clone()) {
                return Some(config);
            }
        None
    }
}

impl CacheAble for ModuleConfig {
    fn field() -> impl AsRef<str> {
        "module_config"
    }
}
