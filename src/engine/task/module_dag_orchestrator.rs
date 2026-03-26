use std::sync::Arc;

use crate::common::interface::ModuleTrait;
use crate::engine::task::module_dag_compiler::{ModuleDagCompiler, ModuleDagDefinition};
use crate::schedule::dag::{Dag, DagError, DagExecutionReport, DagScheduler, DagSchedulerOptions};

#[derive(Debug, Clone, Default)]
pub struct ModuleDagOrchestratorOptions {
    pub scheduler_options: DagSchedulerOptions,
}

pub struct ModuleDagOrchestrator {
    options: ModuleDagOrchestratorOptions,
}

impl Default for ModuleDagOrchestrator {
    fn default() -> Self {
        Self {
            options: ModuleDagOrchestratorOptions::default(),
        }
    }
}

impl ModuleDagOrchestrator {
    pub fn new(options: ModuleDagOrchestratorOptions) -> Self {
        Self { options }
    }

    /// Compiles a DAG from explicit module definition.
    pub fn compile_definition(&self, definition: ModuleDagDefinition) -> Result<Dag, DagError> {
        ModuleDagCompiler::compile(definition)
    }

    /// Compiles a linear-compatible DAG by adapting ModuleTrait.add_step().
    pub async fn compile_linear_compat(&self, module: Arc<dyn ModuleTrait>) -> Result<Dag, DagError> {
        let steps = module.add_step().await;
        let definition = ModuleDagDefinition::from_linear_steps(steps);
        ModuleDagCompiler::compile(definition)
    }

    /// Executes a precompiled DAG with DagScheduler defaults from orchestrator options.
    pub async fn execute_dag(&self, dag: Dag) -> Result<DagExecutionReport, DagError> {
        DagScheduler::new(dag)
            .with_options(self.options.scheduler_options.clone())
            .execute_parallel()
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::Map;

    use crate::common::interface::{
        ModuleNodeTrait, ModuleTrait, SyncBoxStream, ToSyncBoxStream,
    };
    use crate::common::model::login_info::LoginInfo;
    use crate::common::model::message::TaskOutputEvent;
    use crate::common::model::{ModuleConfig, Request, Response};
    use crate::errors::Result;

    use super::ModuleDagOrchestrator;

    struct DummyNode;

    #[async_trait]
    impl ModuleNodeTrait for DummyNode {
        async fn generate(
            &self,
            _config: Arc<ModuleConfig>,
            _params: Map<String, serde_json::Value>,
            _login_info: Option<LoginInfo>,
        ) -> Result<SyncBoxStream<'static, Request>> {
            Ok(Vec::<Request>::new().to_stream())
        }

        async fn parser(
            &self,
            _response: Response,
            _config: Option<Arc<ModuleConfig>>,
        ) -> Result<TaskOutputEvent> {
            Ok(TaskOutputEvent::default())
        }
    }

    struct DummyModule;

    #[async_trait]
    impl ModuleTrait for DummyModule {
        fn should_login(&self) -> bool {
            false
        }

        fn name(&self) -> String {
            "dummy_module_for_orchestrator".to_string()
        }

        fn version(&self) -> i32 {
            1
        }

        fn default_arc() -> Arc<dyn ModuleTrait>
        where
            Self: Sized,
        {
            Arc::new(Self)
        }

        async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
            vec![Arc::new(DummyNode)]
        }
    }

    #[tokio::test]
    async fn compile_linear_compat_succeeds() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_linear_compat(module)
            .await
            .expect("linear compat compile should succeed");
        assert_eq!(dag.node_ptrs().len(), 1);
    }

    #[tokio::test]
    async fn execute_compiled_linear_compat_succeeds() {
        let orchestrator = ModuleDagOrchestrator::default();
        let module: Arc<dyn ModuleTrait> = Arc::new(DummyModule);

        let dag = orchestrator
            .compile_linear_compat(module)
            .await
            .expect("linear compat compile should succeed");
        let report = orchestrator
            .execute_dag(dag)
            .await
            .expect("execution should succeed");

        assert!(report.outputs.contains_key("step_0"));
    }
}
