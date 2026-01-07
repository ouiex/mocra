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
    pub task: Vec<u8>,
    pub login_info: Vec<u8>,
    pub module_config: Vec<u8>,
    pub trait_meta: Vec<u8>,
}

impl MetaData {
    pub fn add(mut self, key: impl AsRef<str>, value: serde_json::Value, source: &str) -> Self {
        match source {
            "task" => {
                if let Ok(mut map) = serde_json::to_value(&self.task)
                    && let Some(map) = map.as_object_mut()
                {
                    map.insert(key.as_ref().into(), value);
                    self.task = serde_json::to_vec(&map).unwrap_or_default();
                }
            }
            "login_info" => {
                if let Ok(mut map) = serde_json::to_value(&self.login_info)
                    && let Some(map) = map.as_object_mut()
                {
                    map.insert(key.as_ref().into(), value);
                    self.login_info = serde_json::to_vec(&map).unwrap_or_default();
                }
            }
            "module_config" => {
                if let Ok(mut map) = serde_json::to_value(&self.module_config)
                    && let Some(map) = map.as_object_mut()
                {
                    map.insert(key.as_ref().into(), value);
                }
            }
            "trait_meta" => {
                if let Ok(mut map) = serde_json::to_value(&self.trait_meta)
                    && let Some(map) = map.as_object_mut()
                {
                    map.insert(key.as_ref().into(), value);
                }
            }
            _ => {}
        }
        self
    }
    pub fn add_task_config<T>(mut self, task_meta: T) -> Self
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        self.task = serde_json::to_vec(&task_meta).unwrap_or_default();
        self
    }
    pub fn add_login_info(mut self, login_info: &LoginInfo) -> Self {
        self.login_info = serde_json::to_vec(&login_info.extra).unwrap_or_default();
        self
    }
    pub fn add_module_config(mut self, module_config: &ModuleConfig) -> Self {
        if let Ok(value) = serde_json::to_vec(module_config) {
            self.module_config = value;
        };

        self
    }
    pub fn add_trait_config<T>(mut self, key: impl AsRef<str>, value: T) -> Self
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Ok(mut map) = serde_json::to_value(&self.trait_meta)
            && let Some(meta) = map.as_object_mut()
        {
            let value = serde_json::to_value(value).unwrap();
            meta.insert(key.as_ref().into(), value);
            self.trait_meta = serde_json::to_vec(&meta).unwrap_or_default();
        }

        self
    }
    pub fn get_trait_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Ok(meta) = serde_json::to_value(&self.trait_meta)
            && let Some(value) = meta.get(key)
            && let Ok(value) = serde_json::from_value(value.clone())
        {
            return value;
        }
        None
    }
    pub fn get_login_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Ok(meta) = serde_json::to_value(&self.login_info)
            && let Some(value) = meta.get(key)
            && let Ok(value) = serde_json::from_value(value.clone())
        {
            return value;
        }
        None
    }
    pub fn get_module_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Ok(meta) = serde_json::to_value(&self.module_config)
            && let Some(value) = meta.get(key)
            && let Ok(value) = serde_json::from_value(value.clone())
        {
            return value;
        }
        None
    }
    pub fn get_task_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Ok(meta) = serde_json::to_value(&self.task)
            && let Some(value) = meta.get(key)
            && let Ok(value) = serde_json::from_value(value.clone())
        {
            return value;
        }
        None
    }
}

impl From<MetaData> for Value {
    fn from(value: MetaData) -> Self {
        serde_json::to_value(value).unwrap_or_default()
    }
}
