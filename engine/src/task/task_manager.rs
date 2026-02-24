#![allow(unused)]
use super::{
    assembler::ModuleAssembler, factory::TaskFactory, repository::TaskRepository, task::Task,
    module::Module,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use common::model::{Request, Response};
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use common::state::State;
use errors::Result;
use cacheable::{CacheAble,CacheService};
use common::interface::ModuleTrait;
use common::model::login_info::LoginInfo;

pub struct TaskManager {
    factory: TaskFactory,
    pub cache_service: Arc<CacheService>,
    module_assembler: Arc<RwLock<ModuleAssembler>>,
}

impl TaskManager {
    /// Creates a task manager with repository, factory, and module assembler wiring.
    pub fn new(state: Arc<State>) -> Self {
        let repository = TaskRepository::new((*state.db).clone());

        let module_assembler = Arc::new(RwLock::new(ModuleAssembler::new()));
        let factory = TaskFactory::new(
            repository,
            Arc::clone(&state.cache_service),
            state.cookie_service.clone(),
            Arc::clone(&module_assembler),
            Arc::clone(&state),
        );

        Self {
            factory,
            cache_service: Arc::clone(&state.cache_service),
            module_assembler,
        }
    }
    

    /// Registers one module implementation.
    pub async fn add_module(&self, work: Arc<dyn ModuleTrait>) {
        let mut assembler = self.module_assembler.write().await;
        assembler.register_module(work);
    }

    /// Registers multiple module implementations.
    pub async fn add_modules(&self, works: Vec<Arc<dyn ModuleTrait>>) {
        let mut assembler = self.module_assembler.write().await;
        for work in works {
            assembler.register_module(work);
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
    pub async fn load_with_model(&self, task_model: &TaskModel) -> Result<Task> {
        self.factory.load_with_model(task_model).await
    }

    /// Loads task from ParserTaskModel with historical state restoration.
    pub async fn load_parser(&self, parser_model: &ParserTaskModel) -> Result<Task> {
        self.factory.load_parser_model(parser_model).await
    }

    /// Loads task from ErrorTaskModel and applies error accounting.
    pub async fn load_error(&self, error_model: &ErrorTaskModel) -> Result<Task> {
        self.factory.load_error_model(error_model).await
    }

    /// Loads task context from response metadata.
    pub async fn load_with_response(&self, response: &Response) -> Result<Task> {
        self.factory.load_with_response(response).await
    }

    /// Loads resolved module and optional login info for parser flow.
    pub async fn load_module_with_response(&self, response: &Response) -> Result<(Arc<Module>, Option<LoginInfo>)> {
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
