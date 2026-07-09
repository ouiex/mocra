use crate::common::model::ModuleConfig;
use crate::common::model::login_info::LoginInfo;


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
    /// 任务元数据。来源链路：Task定义 → ParserModel/ErrorModel 处理 → TaskFactory合并 → Module.generate() → Request.meta
    /// 通过 `add_task_config()` 方法将任务配置序列化为 Value 存储
    #[serde(with = "crate::common::model::serde_value::value")]
    pub task: Value,
    /// 登录信息。来源链路：LoginInfo.extra 字段 → 用户认证/授权系统 → Module.generate() → Request.meta
    /// 通过 `add_login_info()` 方法从 LoginInfo 的 extra 字段提取并序列化
    #[serde(with = "crate::common::model::serde_value::value")]
    pub login_info: Value,
    /// 模块配置。来源链路：数据库(account/platform/middleware关系表) → ModuleConfig实例 → 模块初始化 → Module.generate() → Request.meta
    /// 包含: account表配置、platform表配置、module自身配置、rel_module_account配置、rel_module_platform配置、中间件配置等
    /// 通过 `add_module_config()` 方法将整个 ModuleConfig 结构序列化存储
    #[serde(with = "crate::common::model::serde_value::value")]
    pub module_config: Value,
    /// trait元数据。来源链路：各trait实现 → trait类型定义 → 业务代码调用 → add_trait_config() → Request.meta
    /// 通过 `add_trait_config()` 方法存储trait级别的自定义配置和扩展数据
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
    pub fn add(mut self, key: impl AsRef<str>, value: serde_json::Value, source: MetaSource) -> Self {
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
