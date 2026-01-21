#![allow(unused)]
use common::interface::ModuleTrait;
use common::model::ModuleConfig;
use common::model::entity::*;
use common::model::entity::{RelModuleDataMiddlewareModel, RelModuleDownloadMiddlewareModel};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// 配置组装器，负责将数据库实体组装成模块配置
pub struct ConfigAssembler;

impl ConfigAssembler {
    /// 构建关联配置映射
    pub fn build_relation_config<T, F, G>(
        relations: &[T],
        entities: &[G],
        extract_entity_id: F,
        extract_name: fn(&G) -> &str,
    ) -> HashMap<String, serde_json::Value>
    where
        T: AsRef<RelModuleDataMiddlewareModel> + AsRef<RelModuleDownloadMiddlewareModel>,
        F: Fn(&T) -> i32,
        G: Clone,
    {
        let config_map = HashMap::new();
        for relation in relations {
            let entity_id = extract_entity_id(relation);
            if let Some(entity) = entities.iter().find(|e| {
                // 这里需要根据具体类型来匹配ID
                // 由于泛型限制，这个函数可能需要拆分为具体的实现
                false // 临时占位
            }) {
                let name = extract_name(entity);
                // 需要获取关联配置，这里也需要具体实现
                // config_map.insert(name.to_string(), relation.config.clone());
            }
        }
        config_map
    }

    /// 构建数据中间件关联配置
    pub fn build_data_middleware_relation_config(
        relations: &[RelModuleDataMiddlewareModel],
        middlewares: &[DataMiddlewareModel],
    ) -> HashMap<String, serde_json::Value> {
        let mut config_map = HashMap::new();
        for relation in relations {
            if let Some(middleware) = middlewares
                .iter()
                .find(|m| m.id == relation.data_middleware_id)
            {
                config_map.insert(middleware.name.clone(), relation.config.clone());
            }
        }
        config_map
    }

    /// 构建下载中间件关联配置
    pub fn build_download_middleware_relation_config(
        relations: &[RelModuleDownloadMiddlewareModel],
        middlewares: &[DownloadMiddlewareModel],
    ) -> HashMap<String, serde_json::Value> {
        let mut config_map = HashMap::new();
        for relation in relations {
            if let Some(middleware) = middlewares
                .iter()
                .find(|m| m.id == relation.download_middleware_id)
            {
                config_map.insert(middleware.name.clone(), relation.config.clone());
            }
        }
        config_map
    }

    /// 构建中间件基础配置
    pub fn build_middleware_base_config<T: HasNameAndConfig>(
        middlewares: &[T],
    ) -> HashMap<String, serde_json::Value> {
        middlewares
            .iter()
            .map(|m| (m.name().clone(), m.config().clone()))
            .collect()
    }

    /// 组装完整的模块配置
    pub fn assemble_module_config(
        account: &AccountModel,
        platform: &PlatformModel,
        module: &ModuleModel,
        rel_account_platform: &RelAccountPlatformModel,
        rel_module_platform: &RelModulePlatformModel,
        rel_module_account: &RelModuleAccountModel,
        data_middleware: &[DataMiddlewareModel],
        download_middleware: &[DownloadMiddlewareModel],
        rel_module_data_middleware: &[RelModuleDataMiddlewareModel],
        rel_module_download_middleware: &[RelModuleDownloadMiddlewareModel],
    ) -> ModuleConfig {
        let data_middleware_config = Self::build_middleware_base_config(data_middleware);
        let download_middleware_config = Self::build_middleware_base_config(download_middleware);
        let rel_module_data_middleware_config = Self::build_data_middleware_relation_config(
            rel_module_data_middleware,
            data_middleware,
        );
        let rel_module_download_middleware_config = Self::build_download_middleware_relation_config(
            rel_module_download_middleware,
            download_middleware,
        );

        ModuleConfig {
            account_config: account.config.clone(),
            platform_config: platform.config.clone(),
            module_config: module.config.clone(),
            data_middleware_config,
            download_middleware_config,
            rel_account_platform_config: rel_account_platform.config.clone(),
            rel_module_account_config: rel_module_account.config.clone(),
            rel_module_platform_config: rel_module_platform.config.clone(),
            rel_module_data_middleware_config,
            rel_module_download_middleware_config,
        }
    }
}

/// 用于统一处理具有名称和配置的实体的trait
pub trait HasNameAndConfig {
    fn name(&self) -> &String;
    fn config(&self) -> &serde_json::Value;
}

impl HasNameAndConfig for DataMiddlewareModel {
    fn name(&self) -> &String {
        &self.name
    }

    fn config(&self) -> &serde_json::Value {
        &self.config
    }
}

impl HasNameAndConfig for DownloadMiddlewareModel {
    fn name(&self) -> &String {
        &self.name
    }

    fn config(&self) -> &serde_json::Value {
        &self.config
    }
}

pub struct ModuleItem {
    pub module: Arc<dyn ModuleTrait>,
    pub origin: Option<PathBuf>,
}
impl From<Arc<dyn ModuleTrait>> for ModuleItem {
    fn from(value: Arc<dyn ModuleTrait>) -> Self {
        Self {
            module: value,
            origin: None,
        }
    }
}

/// 模块组装器，负责创建完整的Module实例
pub struct ModuleAssembler {
    modules: HashMap<String, ModuleItem>,
}

impl ModuleAssembler {
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
        }
    }

    /// 注册模块
    pub fn register_module(&mut self, module: Arc<dyn ModuleTrait>) {
        self.modules.insert(module.name().clone(), module.into());
    }
    pub fn remove_module(&mut self, name: &str) {
        self.modules.remove(name);
    }
    pub fn remove_by_origin(&mut self, origin: &Path) {
        self.modules
            .retain(|_, w| w.origin.as_deref().map(|p| p != origin).unwrap_or(true));
    }
    pub fn module_names(&self) -> Vec<String> { self.modules.keys().cloned().collect() }
    pub fn set_origin(&mut self, names: &[String], origin: &Path) {
        for n in names {
            if let Some(item) = self.modules.get_mut(n) { item.origin = Some(origin.to_path_buf()); }
        }
    }
    /// 根据名称获取模块
    pub fn get_module(&self, name: &str) -> Option<Arc<dyn ModuleTrait>> {
        self.modules.get(name).map(|x| x.module.clone())
    }
    
    pub fn get_all_modules(&self) -> Vec<Arc<dyn ModuleTrait>> {
        self.modules.values().map(|x| x.module.clone()).collect()
    }

}

impl Default for ModuleAssembler {
    fn default() -> Self {
        Self::new()
    }
}
