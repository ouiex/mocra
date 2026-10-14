use crate::common::model::ModuleConfig;
use crate::common::model::login_info::LoginInfo;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct MetaData {
    /// Task metadata. Provenance: Task definition → ParserModel/ErrorModel processing →
    /// TaskFactory merge → Module.generate() → Request.meta
    /// Stored by serializing the task config into a Value via `add_task_config()`.
    #[serde(with = "crate::common::model::serde_value::value")]
    pub task: Value,
    /// Login information. Provenance: the LoginInfo.extra field → user authentication/authorization
    /// system → Module.generate() → Request.meta
    /// Extracted from LoginInfo's extra field and serialized via `add_login_info()`.
    #[serde(with = "crate::common::model::serde_value::value")]
    pub login_info: Value,
    /// Module configuration. Provenance: database (account/platform/middleware relation tables) →
    /// ModuleConfig instance → module initialization → Module.generate() → Request.meta
    /// Includes: account table config, platform table config, the module's own config,
    /// rel_module_account config, rel_module_platform config, middleware config, and so on.
    /// Stored by serializing the whole ModuleConfig struct via `add_module_config()`.
    #[serde(with = "crate::common::model::serde_value::value")]
    pub module_config: Value,
    /// Trait metadata. Provenance: individual trait implementations → trait type definitions →
    /// business code calls → add_trait_config() → Request.meta
    /// Stores trait-level custom configuration and extension data via `add_trait_config()`.
    #[serde(with = "crate::common::model::serde_value::value")]
    pub trait_meta: Value,
}

/// Type-safe selector for MetaData storage slots.
#[derive(Debug, Clone, Copy)]
pub enum MetaSource {
    Task,
    LoginInfo,
    ModuleConfig,
    TraitMeta,
}

impl MetaData {
    pub fn add(
        mut self,
        key: impl AsRef<str>,
        value: serde_json::Value,
        source: MetaSource,
    ) -> Self {
        let target = match source {
            MetaSource::Task => &mut self.task,
            MetaSource::LoginInfo => &mut self.login_info,
            MetaSource::ModuleConfig => &mut self.module_config,
            MetaSource::TraitMeta => &mut self.trait_meta,
        };

        if !target.is_object() {
            *target = Value::Object(serde_json::Map::new());
        }

        if let Some(map) = target.as_object_mut() {
            map.insert(key.as_ref().into(), value);
        }
        self
    }
    pub fn add_task_config<T>(mut self, task_meta: T) -> Self
    where
        T: Serialize,
    {
        if let Ok(value) = serde_json::to_value(task_meta) {
            self.task = value;
        }
        self
    }
    pub fn add_login_info(mut self, login_info: &LoginInfo) -> Self {
        if let Ok(value) = serde_json::to_value(&login_info.extra) {
            self.login_info = value;
        }
        self
    }
    pub fn add_module_config(mut self, module_config: &ModuleConfig) -> Self {
        if let Ok(value) = serde_json::to_value(module_config) {
            self.module_config = value;
        };

        self
    }
    pub fn add_trait_config<T>(mut self, key: impl AsRef<str>, value: T) -> Self
    where
        T: Serialize,
    {
        if !self.trait_meta.is_object() {
            self.trait_meta = Value::Object(serde_json::Map::new());
        }

        if let Some(map) = self.trait_meta.as_object_mut() {
            if let Ok(value) = serde_json::to_value(value) {
                map.insert(key.as_ref().into(), value);
            }
        }

        self
    }
    pub fn get_trait_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.trait_meta
            .get(key)
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
    }
    pub fn get_login_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.login_info
            .get(key)
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
    }
    pub fn get_module_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.module_config
            .get(key)
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
    }
    pub fn get_task_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.task
            .get(key)
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
    }
}

impl From<MetaData> for Value {
    fn from(value: MetaData) -> Self {
        serde_json::to_value(value).unwrap_or_default()
    }
}
