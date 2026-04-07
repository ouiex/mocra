use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use serde_json::{json, Map, Value};

use mocra::common::interface::{ModuleNodeTrait, ModuleTrait, SyncBoxStream};
use mocra::common::model::login_info::LoginInfo;
use mocra::common::model::message::{TaskOutputEvent, TaskParserEvent};
use mocra::common::model::{ModuleConfig, Request, RequestMethod, Response};
use mocra::common::state::State;
use mocra::engine::engine::Engine;
use mocra::engine::task::module_dag_compiler::{
    ModuleDagDefinition, ModuleDagEdgeDef, ModuleDagNodeDef,
};
use mocra::errors::Result;

struct StartNode;

#[async_trait]
impl ModuleNodeTrait for StartNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut req = Request::new("https://httpbin.org/get", RequestMethod::Get.as_ref());
        req.account = "demo_account".to_string();
        req.platform = "demo_platform".to_string();
        req.module = "demo_dag_module".to_string();
        req.meta = json!({"source": "start_node"}).into();

        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(&self, response: Response, _config: Option<Arc<ModuleConfig>>) -> Result<TaskOutputEvent> {
        // 这里演示将解析结果转成下游任务（可选）。
        let next_task = TaskParserEvent::from(&response).add_meta("from_node", "start_node");
        Ok(TaskOutputEvent::default().with_task(next_task))
    }
}

struct FollowNode;

#[async_trait]
impl ModuleNodeTrait for FollowNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut req = Request::new("https://httpbin.org/anything", RequestMethod::Get.as_ref());
        req.account = "demo_account".to_string();
        req.platform = "demo_platform".to_string();
        req.module = "demo_dag_module".to_string();

        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(&self, _response: Response, _config: Option<Arc<ModuleConfig>>) -> Result<TaskOutputEvent> {
        Ok(TaskOutputEvent::default())
    }
}

struct BranchANode;

#[async_trait]
impl ModuleNodeTrait for BranchANode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut req = Request::new("https://httpbin.org/uuid", RequestMethod::Get.as_ref());
        req.account = "demo_account".to_string();
        req.platform = "demo_platform".to_string();
        req.module = "demo_dag_module".to_string();

        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(&self, _response: Response, _config: Option<Arc<ModuleConfig>>) -> Result<TaskOutputEvent> {
        Ok(TaskOutputEvent::default())
    }
}

struct BranchBNode;

#[async_trait]
impl ModuleNodeTrait for BranchBNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut req = Request::new("https://httpbin.org/ip", RequestMethod::Get.as_ref());
        req.account = "demo_account".to_string();
        req.platform = "demo_platform".to_string();
        req.module = "demo_dag_module".to_string();

        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(&self, _response: Response, _config: Option<Arc<ModuleConfig>>) -> Result<TaskOutputEvent> {
        Ok(TaskOutputEvent::default())
    }
}

struct MergeNode;

#[async_trait]
impl ModuleNodeTrait for MergeNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut req = Request::new("https://httpbin.org/headers", RequestMethod::Get.as_ref());
        req.account = "demo_account".to_string();
        req.platform = "demo_platform".to_string();
        req.module = "demo_dag_module".to_string();

        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(&self, _response: Response, _config: Option<Arc<ModuleConfig>>) -> Result<TaskOutputEvent> {
        Ok(TaskOutputEvent::default())
    }
}

struct DemoDagModule;

#[async_trait]
impl ModuleTrait for DemoDagModule {
    fn should_login(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        "demo_dag_module".to_string()
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

    /// 自定义多链路 DAG：start -> branch_a/branch_b -> merge。
    async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
        Some(build_multi_route_definition())
    }

    /// 线性步骤：step_0(StartNode) -> step_1(FollowNode)。
    /// 当 dag_definition 同时存在时，调度器会自动把两者合并为一个多链路 DAG。
    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(StartNode), Arc::new(FollowNode)]
    }
}

fn build_multi_route_definition() -> ModuleDagDefinition {
    // 多链路示例：start 分叉到 branch_a/branch_b，最后汇合到 merge。
    ModuleDagDefinition {
        nodes: vec![
            ModuleDagNodeDef {
                node_id: "start".to_string(),
                node: Arc::new(StartNode),
                placement_override: None,
                policy_override: None,
                tags: vec!["entry".to_string()],
            },
            ModuleDagNodeDef {
                node_id: "branch_a".to_string(),
                node: Arc::new(BranchANode),
                placement_override: None,
                policy_override: None,
                tags: vec!["branch".to_string(), "a".to_string()],
            },
            ModuleDagNodeDef {
                node_id: "branch_b".to_string(),
                node: Arc::new(BranchBNode),
                placement_override: None,
                policy_override: None,
                tags: vec!["branch".to_string(), "b".to_string()],
            },
            ModuleDagNodeDef {
                node_id: "merge".to_string(),
                node: Arc::new(MergeNode),
                placement_override: None,
                policy_override: None,
                tags: vec!["merge".to_string()],
            },
        ],
        edges: vec![
            ModuleDagEdgeDef {
                from: "start".to_string(),
                to: "branch_a".to_string(),
            },
            ModuleDagEdgeDef {
                from: "start".to_string(),
                to: "branch_b".to_string(),
            },
            ModuleDagEdgeDef {
                from: "branch_a".to_string(),
                to: "merge".to_string(),
            },
            ModuleDagEdgeDef {
                from: "branch_b".to_string(),
                to: "merge".to_string(),
            },
        ],
        entry_nodes: vec!["start".to_string()],
        default_policy: None,
        metadata: Default::default(),
    }
}

#[tokio::main]
async fn main() {
    // 1) 初始化 State + Engine。
    let state = Arc::new(State::new("tests/config.mock.pure.engine.toml").await);
    let engine = Engine::new(state, None).await.expect("Failed to initialize engine");

    // 2) 注册模块到引擎（自动编译 DAG）。
    let module: Arc<dyn ModuleTrait> = Arc::new(DemoDagModule);
    engine.register_module(module.clone()).await;

    // 3) 直接从引擎获取已编译的 DAG，无需手动编译。
    let dag = engine
        .get_module_dag("demo_dag_module")
        .expect("module DAG should be pre-compiled on registration");
    println!("DAG node count: {}", dag.node_ptrs().len());
    let topo = dag.topological_sort().expect("topological sort");
    println!("DAG topological order: {:?}", topo);
}
