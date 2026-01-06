use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use crate::model::ModuleConfig;
use crate::model::login_info::LoginInfo;
use sea_orm::JsonValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize,Archive,RkyvDeserialize,RkyvSerialize,CheckBytes)]
pub struct MetaData {
    pub task: Vec<u8>,
    pub login_info: Vec<u8>,
    pub module_config: Vec<u8>,
    pub trait_meta: Vec<u8>,
}

impl Default for MetaData {
    fn default() -> Self {
        Self {
            task: vec![],
            login_info:  vec![],
            module_config:  vec![],
            trait_meta:  vec![],
        }
    }
}
impl MetaData {
    pub fn add(mut self, key: impl AsRef<str>, value: serde_json::Value, source: &str) -> Self {
        match source {
            "task" => {
                if let Ok(map) = serde_json::to_value(self.task.clone()) && let Some(map) = map.as_object_mut() {
                    map.insert(key.as_ref().into(), value);
                    self.task = serde_json::to_vec(&map).unwrap_or_default();
                }
            }
            "login_info" => {
                if let Some(map) = self.login_info.as_object_mut() {
                    map.insert(key.as_ref().into(), value);
                }
            }
            "module_config" => {
                if let Some(map) = self.module_config.as_object_mut() {
                    map.insert(key.as_ref().into(), value);
                }
            }
            "trait_meta" => {
                if let Some(map) = self.trait_meta.as_object_mut() {
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
        self.task = serde_json::to_value(task_meta).unwrap();
        self
    }
    pub fn add_login_info(mut self, login_info: &LoginInfo) -> Self {
        self.login_info = login_info.extra.clone();
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
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(meta) = self.trait_meta.as_object_mut() {
            let value = serde_json::to_value(value).unwrap();
            meta.insert(key.as_ref().into(), value);
        }

        self
    }
    pub fn get_trait_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(meta) = self.trait_meta.as_object() {
            if let Some(value) = meta.get(key) {
                if let Ok(value) = serde_json::from_value(value.clone()) {
                    return value;
                }
            }
        }
        None
    }
    pub fn get_login_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(meta) = self.login_info.as_object() {
            if let Some(value) = meta.get(key) {
                if let Ok(value) = serde_json::from_value(value.clone()) {
                    return value;
                }
            }
        }
        None
    }
    pub fn get_module_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(meta) = self.module_config.as_object() {
            if let Some(value) = meta.get(key) {
                if let Ok(value) = serde_json::from_value(value.clone()) {
                    return value;
                }
            }
        }
        None
    }
    pub fn get_task_config<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        if let Some(meta) = self.task.as_object() {
            if let Some(value) = meta.get(key) {
                if let Ok(value) = serde_json::from_value(value.clone()) {
                    return value;
                }
            }
        }
        None
    }
}

impl From<MetaData> for JsonValue {
    fn from(value: MetaData) -> Self {
        serde_json::to_value(value).unwrap_or_default()
    }
}
