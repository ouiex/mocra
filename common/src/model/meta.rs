use crate::model::ModuleConfig;
use crate::model::login_info::LoginInfo;


use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(
    Default,
    Debug,
    Clone,
    Serialize,
    Deserialize,
)]
pub struct MetaData {
    #[serde(with = "crate::model::serde_value::value")]
    pub task: Value,
    #[serde(with = "crate::model::serde_value::value")]
    pub login_info: Value,
    #[serde(with = "crate::model::serde_value::value")]
    pub module_config: Value,
    #[serde(with = "crate::model::serde_value::value")]
    pub trait_meta: Value,
}

impl MetaData {
    pub fn add(mut self, key: impl AsRef<str>, value: serde_json::Value, source: &str) -> Self {
        let target = match source {
            "task" => &mut self.task,
            "login_info" => &mut self.login_info,
            "module_config" => &mut self.module_config,
            "trait_meta" => &mut self.trait_meta,
            _ => return self,
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
        self.trait_meta.get(key).cloned().and_then(|v| serde_json::from_value(v).ok())
    }
    pub fn get_login_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.login_info.get(key).cloned().and_then(|v| serde_json::from_value(v).ok())
    }
    pub fn get_module_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.module_config.get(key).cloned().and_then(|v| serde_json::from_value(v).ok())
    }
    pub fn get_task_config<T>(&self, key: &str) -> Option<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.task.get(key).cloned().and_then(|v| serde_json::from_value(v).ok())
    }
}

impl From<MetaData> for Value {
    fn from(value: MetaData) -> Self {
        serde_json::to_value(value).unwrap_or_default()
    }
}
