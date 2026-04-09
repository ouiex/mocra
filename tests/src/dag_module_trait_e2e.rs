use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map, Value};
use tokio::signal;
use mocra::common::model::CronConfig;
use mocra::prelude::*;
use mocra::prelude::common::ToSyncBoxStream;
use mocra::common::state::State;
use mocra::prelude::engine::Engine;
use mocra::engine::task::module_dag_compiler::{ModuleDagDefinition, ModuleDagNodeDef};

// ---------------------------------------------------------------------------
// DAG topology (7 nodes, 7 edges):
//
//   login_node ──► cate_list_node ──┬──► cate_sales_trend_node   (leaf, 4 reqs)
//                                   ├──► brand_rank_downloader ──► download_url_node ──► download_file_node
//                                   └──► goods_cate_downloader ──┘       (fan-in)
// ---------------------------------------------------------------------------

// ── LoginNode (1 request) ───────────────────────────────────────────────────

struct LoginNode;

#[async_trait]
impl ModuleNodeTrait for LoginNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new("http://127.0.0.1:8000", RequestMethod::Post.as_ref());
        vec![req].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let task = TaskParserEvent::from(&response).add_meta("from", "login_node");
        Ok(TaskOutputEvent::default().with_task(task))
    }

    fn stable_node_key(&self) -> &'static str {
        "login_node"
    }
}

// ── CateListNode (1 request) ────────────────────────────────────────────────

struct CateListNode;

#[async_trait]
impl ModuleNodeTrait for CateListNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new("http://127.0.0.1:8000", RequestMethod::Get.as_ref());
        vec![req].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let task = TaskParserEvent::from(&response).add_meta("from", "cate_list_node");
        Ok(TaskOutputEvent::default().with_task(task))
    }

    fn stable_node_key(&self) -> &'static str {
        "cate_list_node"
    }
}

// ── CateSalesTrendNode (4 requests, leaf) ───────────────────────────────────

struct CateSalesTrendNode;

#[async_trait]
impl ModuleNodeTrait for CateSalesTrendNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let requests = (0..4)
            .map(|i| Request::new(format!("http://127.0.0.1:8000"), RequestMethod::Get.as_ref()))
            .collect::<Vec<_>>();
        requests.into_stream_ok()
    }

    async fn parser(
        &self,
        _response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        Ok(TaskOutputEvent::default())
    }

    fn stable_node_key(&self) -> &'static str {
        "cate_sales_trend_node"
    }
}

// ── BrandRankDownloader (10 requests) ───────────────────────────────────────

struct BrandRankDownloader;

#[async_trait]
impl ModuleNodeTrait for BrandRankDownloader {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let requests = (0..10)
            .map(|i| Request::new(format!("http://127.0.0.1:8000"), RequestMethod::Get.as_ref()))
            .collect::<Vec<_>>();
        requests.into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let task = TaskParserEvent::from(&response).add_meta("from", "brand_rank_downloader");
        Ok(TaskOutputEvent::default().with_task(task))
    }

    fn stable_node_key(&self) -> &'static str {
        "brand_rank_downloader"
    }
}

// ── GoodsCateDownloader (10 requests) ───────────────────────────────────────

struct GoodsCateDownloader;

#[async_trait]
impl ModuleNodeTrait for GoodsCateDownloader {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let requests = (0..10)
            .map(|i| Request::new(format!("http://127.0.0.1:8000"), RequestMethod::Get.as_ref()))
            .collect::<Vec<_>>();
        requests.into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let task = TaskParserEvent::from(&response).add_meta("from", "goods_cate_downloader");
        Ok(TaskOutputEvent::default().with_task(task))
    }

    fn stable_node_key(&self) -> &'static str {
        "goods_cate_downloader"
    }
}

// ── DownloadUrlNode (1 request per upstream item, fan-in) ───────────────────

struct DownloadUrlNode;

#[async_trait]
impl ModuleNodeTrait for DownloadUrlNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new("http://127.0.0.1:8000", RequestMethod::Get.as_ref());
        vec![req].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let task = TaskParserEvent::from(&response).add_meta("from", "download_url_node");
        Ok(TaskOutputEvent::default().with_task(task))
    }

    fn stable_node_key(&self) -> &'static str {
        "download_url_node"
    }
}

// ── DownloadFileNode (leaf, 1 request per upstream item) ────────────────────

struct DownloadFileNode;

#[async_trait]
impl ModuleNodeTrait for DownloadFileNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new("http://127.0.0.1:8000", RequestMethod::Get.as_ref());
        vec![req].into_stream_ok()
    }

    async fn parser(
        &self,
        _response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        Ok(TaskOutputEvent::default())
    }

    fn stable_node_key(&self) -> &'static str {
        "download_file_node"
    }
}

// ── Module definition ───────────────────────────────────────────────────────

struct DagModuleE2ETest;

#[async_trait]
impl ModuleTrait for DagModuleE2ETest {
    fn should_login(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        "dag_module_e2e_test".to_string()
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

    async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
        Some(build_dag())
    }
    fn cron(&self) -> Option<CronConfig> {
        Some(CronConfig::right_now())
    }
}

fn build_dag() -> ModuleDagDefinition {
    let login_node = ModuleDagNodeDef::new(Arc::new(LoginNode));
    let cate_list_node = ModuleDagNodeDef::new(Arc::new(CateListNode));
    let cate_sales_trend_node = ModuleDagNodeDef::new(Arc::new(CateSalesTrendNode));
    let brand_rank_downloader = ModuleDagNodeDef::new(Arc::new(BrandRankDownloader));
    let goods_cate_downloader = ModuleDagNodeDef::new(Arc::new(GoodsCateDownloader));
    let download_url_node = ModuleDagNodeDef::new(Arc::new(DownloadUrlNode));
    let download_file_node = ModuleDagNodeDef::new(Arc::new(DownloadFileNode));

    ModuleDagDefinition::builder()
        .edge(&login_node, &cate_list_node)
        .edge(&cate_list_node, &cate_sales_trend_node)
        .edge(&cate_list_node, &brand_rank_downloader)
        .edge(&cate_list_node, &goods_cate_downloader)
        .edge(&brand_rank_downloader, &download_url_node)
        .edge(&goods_cate_downloader, &download_url_node)
        .edge(&download_url_node, &download_file_node)
        .build()
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
    eprintln!("Using config: {}", config_path.display());
    let state = Arc::new(
        State::try_new(config_path.to_str().unwrap())
            .await
            .expect("State initialization failed"),
    );
    let engine = Engine::new(state, None)
        .await
        .expect("Failed to initialize engine");

    let module: Arc<dyn ModuleTrait> = Arc::new(DagModuleE2ETest);
    engine.register_module(module.clone()).await;

    // Start the engine – it drives the full download/parse pipeline.
    let start_fut = engine.start();
    tokio::pin!(start_fut);
    tokio::select! {
        _ = &mut start_fut => {}
        _ = signal::ctrl_c() => {
            engine.shutdown().await;
            let _ = (&mut start_fut).await;
        }
    }
}