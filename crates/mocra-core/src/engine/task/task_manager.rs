#![allow(unused)]
#[cfg(feature = "store")]
use super::repository::{MetadataStore, TaskRepository};
use super::{assembler::ModuleAssembler, factory::TaskFactory, module::Module, task::Task};
use crate::cacheable::{CacheAble, CacheService};
use crate::common::interface::ModuleTrait;
use crate::common::model::config::Config;
use crate::common::model::login_info::LoginInfo;
use crate::common::model::message::{TaskErrorEvent, TaskEvent, TaskParserEvent};
use crate::common::model::{Request, Response};
use crate::common::state::DbHandle;
use crate::errors::Result;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub struct TaskManager {
    factory: TaskFactory,
    pub cache_service: Arc<CacheService>,
    module_assembler: Arc<RwLock<ModuleAssembler>>,
}

impl TaskManager {
    /// Creates a task manager with repository, factory, and module assembler wiring.
    /// Narrowed dependencies: only a DB handle + cache / cookies / config are needed (it used to
    /// take the entire `Arc<State>` — refactor Phase 2).
    /// With `db = None` (or without the `store` feature) it enters no-DB mode, synthesizing tasks
    /// from the in-memory module registry.
    pub fn new(
        db: &DbHandle,
        cache_service: Arc<CacheService>,
        cookie_service: Option<Arc<CacheService>>,
        config: Arc<RwLock<Config>>,
    ) -> Self {
        #[cfg(feature = "store")]
        let repository = db
            .as_ref()
            .map(|db| Arc::new(TaskRepository::new((**db).clone())) as Arc<dyn MetadataStore>);
        #[cfg(not(feature = "store"))]
        let repository = ();

        let module_assembler = Arc::new(RwLock::new(ModuleAssembler::new()));
        let factory = TaskFactory::new(
            repository,
            Arc::clone(&cache_service),
            cookie_service,
            Arc::clone(&module_assembler),
            Arc::clone(&config),
        );

        Self {
            factory,
            cache_service,
            module_assembler,
        }
    }

    /// Registers one module implementation.
    pub async fn add_module(&self, work: Arc<dyn ModuleTrait>) {
        let name = work.name();
        {
            let mut assembler = self.module_assembler.write().await;
            assembler.register_module(work.clone());
        }
    }

    /// Registers multiple module implementations.
    pub async fn add_modules(&self, works: Vec<Arc<dyn ModuleTrait>>) {
        {
            let mut assembler = self.module_assembler.write().await;
            for work in &works {
                assembler.register_module(work.clone());
            }
        }
        for work in works {
            let name = work.name();
        }
    }

    /// Returns true when module name is registered.
    pub async fn exists_module(&self, name: &str) -> bool {
        let assembler = self.module_assembler.read().await;
        assembler.get_module(name).is_some()
    }
    /// Unregisters module by name.
    pub async fn remove_work(&self, name: &str) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_module(name);
        drop(assembler);
    }
    /// Removes all modules loaded from a given origin path.
    pub async fn remove_by_origin(&self, origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_by_origin(origin);
    }
    /// Returns all registered module names.
    pub async fn module_names(&self) -> Vec<String> {
        let assembler = self.module_assembler.read().await;
        assembler.module_names()
    }
    /// Tags module names with origin path for hot-reload bookkeeping.
    pub async fn set_origin(&self, names: &[String], origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.set_origin(names, origin);
    }
    /// Loads task from TaskModel and synchronizes initial state.
    pub async fn load_with_model(&self, task_model: &TaskEvent) -> Result<Task> {
        self.factory.load_with_model(task_model).await
    }

    /// Loads task from ParserTaskModel with historical state restoration.
    pub async fn load_parser(&self, parser_model: &TaskParserEvent) -> Result<Task> {
        self.factory.load_parser_model(parser_model).await
    }

    /// Loads task from ErrorTaskModel and applies error accounting.
    pub async fn load_error(&self, error_model: &TaskErrorEvent) -> Result<Task> {
        self.factory.load_error_model(error_model).await
    }

    /// Loads task context from response metadata.
    pub async fn load_with_response(&self, response: &Response) -> Result<Task> {
        self.factory.load_with_response(response).await
    }

    /// Loads resolved module and optional login info for parser flow.
    pub async fn load_module_with_response(
        &self,
        response: &Response,
    ) -> Result<(Arc<Module>, Option<LoginInfo>)> {
        self.factory.load_module_with_response(response).await
    }

    /// Clears internal factory caches.
    pub async fn clear_factory_cache(&self) {
        self.factory.clear_cache().await;
    }

    /// Returns all registered module implementations.
    pub async fn get_all_modules(&self) -> Vec<Arc<dyn ModuleTrait>> {
        let assembler = self.module_assembler.read().await;
        assembler.get_all_modules()
    }
}
