#![allow(unused)]
use crate::common::interface::ModuleTrait;
use crate::common::model::ModuleConfig;
use crate::common::model::entity::*;
use crate::common::model::entity::{RelModuleDataMiddlewareModel, RelModuleDownloadMiddlewareModel};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Builds module configuration objects from database entities and relations.
pub struct ConfigAssembler;

impl ConfigAssembler {
    /// Builds a relation config map for middleware-like entities.
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
                // Generic ID matching is intentionally left as a placeholder.
                let _ = entity_id;
                false
            }) {
                let name = extract_name(entity);
                let _ = name;
            }
        }
        config_map
    }

    /// Builds relation config map for data middleware.
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

    /// Builds relation config map for download middleware.
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

    /// Builds base middleware config map (`name -> config`).
    pub fn build_middleware_base_config<T: HasNameAndConfig>(
        middlewares: &[T],
    ) -> HashMap<String, serde_json::Value> {
        middlewares
            .iter()
            .map(|m| (m.name().clone(), m.config().clone()))
            .collect()
    }

    /// Assembles the complete `ModuleConfig` from account/platform/module entities.
    #[allow(clippy::too_many_arguments)]
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

/// Shared abstraction for entities exposing a name and config payload.
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

/// Registry-like assembler for loaded module implementations.
pub struct ModuleAssembler {
    modules: HashMap<String, ModuleItem>,
}

impl ModuleAssembler {
    pub fn new() -> Self {
        Self {
            modules: HashMap::new(),
        }
    }

    /// Registers a module implementation by its name.
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
    /// Returns a module by name.
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
