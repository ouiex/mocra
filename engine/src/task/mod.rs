pub mod assembler;
pub mod factory;
pub mod middleware;
pub mod module;
pub mod module_processor_with_chain;
pub mod repository;
pub mod task;
pub mod task_manager;

// 重导出主要的类型和 trait
pub use task::Task;
pub use task_manager::TaskManager;
