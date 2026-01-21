#![allow(unused)]
use super::{
    assembler::ModuleAssembler, factory::TaskFactory, repository::TaskRepository, task::Task,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use common::model::{Request, Response};
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use common::state::State;
use errors::Result;
use cacheable::{CacheAble,CacheService};
use common::interface::ModuleTrait;

pub struct TaskManager {
    factory: TaskFactory,
    pub cache_service: Arc<CacheService>,
    module_assembler: Arc<RwLock<ModuleAssembler>>,
}

impl TaskManager {
    /// 创建新的任务管理器
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
    

    /// 添加工作模块
    pub async fn add_module(&self, work: Arc<dyn ModuleTrait>) {
        let mut assembler = self.module_assembler.write().await;
        assembler.register_module(work);
    }
    pub async fn add_modules(&self, works: Vec<Arc<dyn ModuleTrait>>) {
        let mut assembler = self.module_assembler.write().await;
        for work in works {
            assembler.register_module(work);
        }
    }
    pub async fn exists_module(&self, name: &str) -> bool {
        let assembler = self.module_assembler.read().await;
        assembler.get_module(name).is_some()
    }
    pub async fn remove_work(&self, name: &str) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_module(name);
    }
    pub async fn remove_by_origin(&self, origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.remove_by_origin(origin);
    }
    pub async fn module_names(&self) -> Vec<String> {
        let assembler = self.module_assembler.read().await;
        assembler.module_names()
    }
    pub async fn set_origin(&self, names: &[String], origin: &std::path::Path) {
        let mut assembler = self.module_assembler.write().await;
        assembler.set_origin(names, origin);
    }
    /// 根据任务模型加载任务，并同步初始状态
    pub async fn load_with_model(&self, task_model: &TaskModel) -> Result<Task> {
        self.factory.load_with_model(task_model).await
    }

    /// 根据解析任务模型加载任务，并加载历史状态或初始状态
    pub async fn load_parser(&self, parser_model: &ParserTaskModel) -> Result<Task> {
        self.factory.load_parser_model(parser_model).await
    }

    /// 根据解析错误模型加载任务，并增加错误次数
    pub async fn load_error(&self, error_model: &ErrorTaskModel) -> Result<Task> {
        self.factory.load_error_model(error_model).await
    }
    pub async fn load_with_response(&self, response: &Response) -> Result<Task> {
        self.factory.load_with_response(response).await
    }

    pub async fn clear_factory_cache(&self) {
        self.factory.clear_cache().await;
    }
    
    pub async fn get_all_modules(&self) -> Vec<Arc<dyn ModuleTrait>> {
        let assembler = self.module_assembler.read().await;
        assembler.get_all_modules()
    }

}
