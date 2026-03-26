use async_trait::async_trait;
use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::Router;
use mocra::cacheable::{CacheAble, CacheService};
use mocra::common::interface::{ModuleNodeTrait, ModuleTrait, SyncBoxStream, ToSyncBoxStream};
use mocra::common::model::config::Config;
use mocra::common::model::download_config::DownloadConfig;
use mocra::common::model::login_info::LoginInfo;
use mocra::common::model::message::{TaskOutputEvent, TaskParserEvent};
use mocra::common::model::{ExecutionMark, Headers, ModuleConfig, Request, Response};
use mocra::common::state::State;
use mocra::downloader::request_downloader::RequestDownloader;
use mocra::downloader::Downloader;
use mocra::engine::engine::Engine;
use mocra::engine::events::{DownloadEvent, EventEnvelope, EventPhase, EventType};
use mocra::engine::task::module_dag_orchestrator::ModuleDagOrchestrator;
use mocra::errors::{Error, ErrorKind, Result};
use mocra::queue::QueuedItem;
use mocra::utils::connector::create_redis_pool;
use mocra::utils::distributed_rate_limit::{DistributedSlidingWindowRateLimiter, RateLimitConfig};
use mocra::utils::redis_lock::DistributedLockManager;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;
use uuid::Uuid;

fn main() {}

#[derive(Deserialize)]
struct SumQuery {
    a: i64,
    b: i64,
}

async fn login_handler() -> (StatusCode, HeaderMap, &'static str) {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::SET_COOKIE,
        "sid=valid-token; Domain=127.0.0.1; Path=/; HttpOnly"
            .parse()
            .expect("valid set-cookie"),
    );
    (StatusCode::OK, headers, "login-ok")
}

async fn data_handler(headers: HeaderMap) -> (StatusCode, &'static str) {
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if cookie_header.contains("sid=valid-token") {
        (StatusCode::OK, "secret-data")
    } else {
        (StatusCode::UNAUTHORIZED, "missing-cookie")
    }
}

async fn sum_handler(Query(query): Query<SumQuery>) -> (StatusCode, String) {
    let result = query.a + query.b;
    (StatusCode::OK, json!({ "result": result }).to_string())
}

async fn start_test_server() -> (SocketAddr, oneshot::Sender<()>) {
    let app = Router::new()
        .route("/login", get(login_handler))
        .route("/data", get(data_handler))
        .route("/sum", get(sum_handler));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind axum listener");
    let addr = listener.local_addr().expect("read local addr");
    let (tx, rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await;
    });

    (addr, tx)
}

fn load_config_by_file(file_name: &str) -> Config {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(file_name);
    Config::load(path.to_str().expect("config path to str")).expect("load tests config.toml")
}

fn load_test_config() -> Config {
    load_config_by_file("config.toml")
}

fn build_cache_service_from_config(config: &Config) -> Option<Arc<CacheService>> {
    let redis = config.cache.redis.as_ref()?;
    let pool = create_redis_pool(
        &redis.redis_host,
        redis.redis_port,
        redis.redis_db,
        &redis.redis_username,
        &redis.redis_password,
        redis.pool_size,
        redis.tls.unwrap_or(false),
    )?;

    Some(Arc::new(CacheService::new(
        Some(pool),
        "tests-dag-module-e2e".to_string(),
        Some(Duration::from_secs(config.cache.ttl)),
        config.cache.compression_threshold,
    )))
}

fn build_cache_service_with_single_node_fallback(config: &Config, namespace: &str) -> Arc<CacheService> {
    if let Some(cache) = build_cache_service_from_config(config) {
        return cache;
    }
    Arc::new(CacheService::new(
        None,
        namespace.to_string(),
        Some(Duration::from_secs(config.cache.ttl)),
        config.cache.compression_threshold,
    ))
}

fn build_local_cache_service() -> Arc<CacheService> {
    Arc::new(CacheService::new(
        None,
        "tests-dag-module-local".to_string(),
        Some(Duration::from_secs(60)),
        None,
    ))
}

fn create_downloader(cache_service: Arc<CacheService>, namespace: &str) -> Arc<RequestDownloader> {
    let locker = Arc::new(DistributedLockManager::new(None, namespace));
    let rate_limit_config = RateLimitConfig::new(200.0);
    let limiter = Arc::new(DistributedSlidingWindowRateLimiter::new(
        None,
        locker.clone(),
        namespace,
        rate_limit_config,
    ));

    Arc::new(RequestDownloader::new(
        limiter,
        locker,
        cache_service,
        64,
        1024 * 1024,
    ))
}

fn make_request(url: String, module: &str, run_id: Uuid) -> Request {
    let mut req = Request::new(url, "GET");
    req.account = "acc1".to_string();
    req.platform = "p1".to_string();
    req.module = module.to_string();
    req.run_id = run_id;
    req
}

fn parse_sum(content: &[u8]) -> Result<i64> {
    let value = serde_json::from_slice::<Value>(content)
        .map_err(|e| Error::new(ErrorKind::Parser, Some(e)))?;
    value
        .get("result")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| Error::new(ErrorKind::Parser, Some(std::io::Error::other("missing result field"))))
}

struct DagHttpAddNode {
    index: usize,
    delta: i64,
    base_url: String,
}

#[async_trait]
impl ModuleNodeTrait for DagHttpAddNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let current_sum = params
            .get("current_sum")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let req = make_request(
            format!("{}/sum?a={}&b={}", self.base_url, current_sum, self.delta),
            "dag_module_trait",
            Uuid::new_v4(),
        );
        vec![req].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let sum = parse_sum(&response.content)?;
        let next_task = TaskParserEvent::from(&response)
            .add_meta("current_sum", sum)
            .add_meta("step_index", self.index + 1);

        Ok(TaskOutputEvent::default().with_task(next_task))
    }
}

struct DagModuleTraitTestModule {
    base_url: String,
}

struct EngineDagHttpAddNode {
    index: usize,
    total_steps: usize,
    delta: i64,
    base_url: String,
}

#[async_trait]
impl ModuleNodeTrait for EngineDagHttpAddNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let current_sum = params
            .get("current_sum")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let req = make_request(
            format!("{}/sum?a={}&b={}", self.base_url, current_sum, self.delta),
            "engine_dag_module_trait",
            Uuid::new_v4(),
        );
        vec![req].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let _ = parse_sum(&response.content)?;
        let _ = self.index;
        let _ = self.total_steps;
        Ok(TaskOutputEvent::default())
    }
}

struct EngineDagModuleTraitTestModule {
    base_url: String,
}

struct TaskQueueDagHttpAddNode {
    index: usize,
    total_steps: usize,
    delta: i64,
    base_url: String,
}

#[async_trait]
impl ModuleNodeTrait for TaskQueueDagHttpAddNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let current_sum = params
            .get("current_sum")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let req = make_request(
            format!("{}/sum?a={}&b={}", self.base_url, current_sum, self.delta),
            "engine_task_queue_module_trait",
            Uuid::new_v4(),
        );
        vec![req].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        let sum = parse_sum(&response.content)?;
        if self.index + 1 >= self.total_steps {
            return Ok(TaskOutputEvent::default());
        }

        let next_task = TaskParserEvent::from(&response).add_meta("current_sum", sum);
        Ok(TaskOutputEvent::default().with_task(next_task))
    }
}

struct TaskQueueDagModuleTraitTestModule {
    base_url: String,
}

#[async_trait]
impl ModuleTrait for TaskQueueDagModuleTraitTestModule {
    fn should_login(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        "engine.task.queue.module.trait.e2e".to_string()
    }

    fn version(&self) -> i32 {
        1
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(Self {
            base_url: "http://127.0.0.1:0".to_string(),
        })
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        let total_steps = 3usize;
        vec![
            Arc::new(TaskQueueDagHttpAddNode {
                index: 0,
                total_steps,
                delta: 1,
                base_url: self.base_url.clone(),
            }),
            Arc::new(TaskQueueDagHttpAddNode {
                index: 1,
                total_steps,
                delta: 2,
                base_url: self.base_url.clone(),
            }),
            Arc::new(TaskQueueDagHttpAddNode {
                index: 2,
                total_steps,
                delta: 3,
                base_url: self.base_url.clone(),
            }),
        ]
    }
}

#[async_trait]
impl ModuleTrait for EngineDagModuleTraitTestModule {
    fn should_login(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        "engine.dag.module.trait.e2e".to_string()
    }

    fn version(&self) -> i32 {
        1
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(Self {
            base_url: "http://127.0.0.1:0".to_string(),
        })
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        let total_steps = 3usize;
        vec![
            Arc::new(EngineDagHttpAddNode {
                index: 0,
                total_steps,
                delta: 1,
                base_url: self.base_url.clone(),
            }),
            Arc::new(EngineDagHttpAddNode {
                index: 1,
                total_steps,
                delta: 2,
                base_url: self.base_url.clone(),
            }),
            Arc::new(EngineDagHttpAddNode {
                index: 2,
                total_steps,
                delta: 3,
                base_url: self.base_url.clone(),
            }),
        ]
    }
}

fn prepare_single_node_engine_config() -> String {
    let config_template = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.mock.pure.engine.toml");
    let config_raw = std::fs::read_to_string(config_template).expect("read engine config template");

    let tmp_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tmp");
    std::fs::create_dir_all(&tmp_dir).expect("create tests/tmp");
    let db_path = tmp_dir.join(format!("engine_e2e_{}.sqlite", Uuid::new_v4()));
    let db_path_url = db_path.to_string_lossy().replace('\\', "/");
    let sqlite_url = format!("sqlite://{}?mode=rwc", db_path_url);

    let mut patched = String::with_capacity(config_raw.len() + 128);
    for line in config_raw.lines() {
        if line.trim_start().starts_with("url = ") {
            patched.push_str(&format!("url = \"{}\"\n", sqlite_url));
        } else {
            patched.push_str(line);
            patched.push('\n');
        }
    }

    let config_path = tmp_dir.join(format!("engine_e2e_{}.toml", Uuid::new_v4()));
    std::fs::write(&config_path, patched).expect("write temporary engine config");
    config_path.to_string_lossy().to_string()
}

async fn init_single_node_sqlite_schema(state: &Arc<State>) {
    let db_backend = state.db.get_database_backend();
    if db_backend != DatabaseBackend::Sqlite {
        return;
    }

    let init_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("init_sqlite_pure.sql");
    let init_sql = std::fs::read_to_string(&init_path).expect("read init_sqlite_pure.sql");
    for stmt in init_sql.split(';').filter(|s| !s.trim().is_empty()) {
        if let Err(err) = state
            .db
            .execute(Statement::from_string(db_backend, stmt.to_string()))
            .await
        {
            eprintln!("skip sqlite schema statement error: {err}");
        }
    }
}

async fn ensure_task_queue_support_tables(state: &Arc<State>) {
    let db_backend = state.db.get_database_backend();
    if db_backend != DatabaseBackend::Sqlite {
        return;
    }

    let sql = [
        "CREATE TABLE IF NOT EXISTS base.data_middleware (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS base.download_middleware (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS base.rel_module_data_middleware (module_id INTEGER NOT NULL, data_middleware_id INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS base.rel_module_download_middleware (module_id INTEGER NOT NULL, download_middleware_id INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    ];

    for stmt in sql {
        if let Err(err) = state
            .db
            .execute(Statement::from_string(db_backend, stmt.to_string()))
            .await
        {
            eprintln!("skip task support table statement error: {err}");
        }
    }
}

async fn seed_task_queue_module_scope(state: &Arc<State>, module_name: &str) {
    let db_backend = state.db.get_database_backend();
    if db_backend != DatabaseBackend::Sqlite {
        return;
    }

    let stmts = [
        format!("INSERT OR IGNORE INTO base.module (name, enabled, config, priority, version) VALUES ('{}', 1, '{{}}', 10, 1)", module_name),
        "INSERT OR IGNORE INTO base.account (name, modules, enabled, config, priority) VALUES ('acc1', '{}', 1, '{}', 10)".to_string(),
        "INSERT OR IGNORE INTO base.platform (name, description, base_url, enabled, config) VALUES ('p1', 'task queue e2e', '', 1, '{}')".to_string(),
        format!(
            "INSERT INTO base.rel_module_account (module_id, account_id, priority, enabled, config) SELECT m.id, a.id, 10, 1, '{{}}' FROM base.module m, base.account a WHERE m.name='{}' AND a.name='acc1'",
            module_name
        ),
        format!(
            "INSERT INTO base.rel_module_platform (module_id, platform_id, priority, enabled, config) SELECT m.id, p.id, 10, 1, '{{}}' FROM base.module m, base.platform p WHERE m.name='{}' AND p.name='p1'",
            module_name
        ),
        "INSERT INTO base.rel_account_platform (account_id, platform_id, enabled, config) SELECT a.id, p.id, 1, '{}' FROM base.account a, base.platform p WHERE a.name='acc1' AND p.name='p1'".to_string(),
    ];

    for stmt in stmts {
        if let Err(err) = state
            .db
            .execute(Statement::from_string(db_backend, stmt))
            .await
        {
            eprintln!("skip seed task queue statement error: {err}");
        }
    }
}

async fn update_task_queue_module_config(state: &Arc<State>, module_name: &str, module_config: Value) {
    let db_backend = state.db.get_database_backend();
    if db_backend != DatabaseBackend::Sqlite {
        return;
    }

    let cfg = module_config.to_string();
    let stmt = format!(
        "UPDATE base.module SET config='{}' WHERE name='{}'",
        cfg, module_name
    );
    state
        .db
        .execute(Statement::from_string(db_backend, stmt))
        .await
        .expect("update module config for task-queue cutover e2e");
}

async fn wait_download_completed_count(
    receiver: &mut tokio::sync::mpsc::Receiver<EventEnvelope>,
    module_name: &str,
    expected: usize,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    let mut completed = 0usize;

    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(Some(event)) = tokio::time::timeout(remaining, receiver.recv()).await else {
            break;
        };

        if event.event_type != EventType::Download || event.phase != EventPhase::Completed {
            continue;
        }

        let Ok(payload) = serde_json::from_value::<DownloadEvent>(event.payload.clone()) else {
            continue;
        };

        if payload.module == module_name {
            completed += 1;
            if completed >= expected {
                break;
            }
        }
    }

    completed
}

async fn enqueue_engine_step_requests(
    engine: &Arc<Engine>,
    module: Arc<dyn ModuleTrait>,
    module_name: &str,
    run_id: Uuid,
) {
    let sender = engine.queue_manager.get_request_push_channel();
    let mut current_sum = 0i64;
    let module_steps = module.add_step().await;
    for (idx, step) in module_steps.into_iter().enumerate() {
        let mut params = Map::new();
        params.insert("current_sum".to_string(), json!(current_sum));

        let mut request_stream = step
            .generate(Arc::new(ModuleConfig::default()), params, None)
            .await
            .expect("engine e2e step generate should succeed");
        let mut request = futures::StreamExt::next(&mut request_stream)
            .await
            .expect("engine e2e request stream should yield one request");

        request.account = "acc1".to_string();
        request.platform = "p1".to_string();
        request.module = module_name.to_string();
        request.run_id = run_id;
        request = request.with_context(ExecutionMark::default().with_step_idx(idx as u32));

        sender
            .send(QueuedItem::new(request))
            .await
            .expect("enqueue engine e2e request should succeed");

        current_sum += (idx + 1) as i64;
    }
}

#[async_trait]
impl ModuleTrait for DagModuleTraitTestModule {
    fn should_login(&self) -> bool {
        false
    }

    fn name(&self) -> String {
        "dag.module.trait.e2e".to_string()
    }

    fn version(&self) -> i32 {
        1
    }

    async fn headers(&self) -> Headers {
        Headers::new().add("accept", "application/json")
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(Self {
            base_url: "http://127.0.0.1:0".to_string(),
        })
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![
            Arc::new(DagHttpAddNode {
                index: 0,
                delta: 1,
                base_url: self.base_url.clone(),
            }),
            Arc::new(DagHttpAddNode {
                index: 1,
                delta: 2,
                base_url: self.base_url.clone(),
            }),
            Arc::new(DagHttpAddNode {
                index: 2,
                delta: 3,
                base_url: self.base_url.clone(),
            }),
        ]
    }
}

async fn execute_module_steps(
    module: Arc<dyn ModuleTrait>,
    downloader: Arc<RequestDownloader>,
) -> Result<Vec<i64>> {
    let mut params = Map::new();
    let mut sums = Vec::new();
    let config = Arc::new(ModuleConfig::default());

    for step in module.add_step().await {
        let mut request_stream = step
            .generate(config.clone(), params.clone(), None)
            .await?;
        let request = futures::StreamExt::next(&mut request_stream)
            .await
            .ok_or_else(|| Error::new(ErrorKind::Parser, Some(std::io::Error::other("empty request stream"))))?;

        let response = downloader.download(request).await?;
        let output = step.parser(response.clone(), None).await?;
        let current_sum = parse_sum(&response.content)?;
        sums.push(current_sum);

        if let Some(next_task) = output.parser_task.first() {
            params = next_task.metadata.clone();
        } else {
            params.insert("current_sum".to_string(), json!(current_sum));
        }
    }

    Ok(sums)
}

#[tokio::test]
async fn module_trait_steps_execute_with_axum_http_server() {
    let (addr, shutdown_tx) = start_test_server().await;
    let base_url = format!("http://{}", addr);

    let module: Arc<dyn ModuleTrait> = Arc::new(DagModuleTraitTestModule { base_url });
    let downloader = create_downloader(build_local_cache_service(), "tests-dag-module-local");

    assert_eq!(module.name(), "dag.module.trait.e2e");
    assert_eq!(module.version(), 1);

    let sums = execute_module_steps(module, downloader)
        .await
        .expect("module steps should execute against axum server");

    assert_eq!(sums, vec![1, 3, 6]);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn module_trait_linear_compat_dag_with_multiple_nodes_executes() {
    let (addr, shutdown_tx) = start_test_server().await;
    let base_url = format!("http://{}", addr);
    let module: Arc<dyn ModuleTrait> = Arc::new(DagModuleTraitTestModule { base_url });

    let orchestrator = ModuleDagOrchestrator::default();
    let dag = orchestrator
        .compile_linear_compat(module)
        .await
        .expect("dag compile should succeed for multi-node module");
    let report = orchestrator
        .execute_dag(dag)
        .await
        .expect("dag execute should succeed for multi-node module");

    assert!(report.outputs.contains_key("step_0"));
    assert!(report.outputs.contains_key("step_1"));
    assert!(report.outputs.contains_key("step_2"));

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_behavior_toggle_works() {
    let config = load_test_config();
    let Some(cache_service) = build_cache_service_from_config(&config) else {
        eprintln!("skip session test: cache.redis is not configured");
        return;
    };

    if cache_service.ping().await.is_err() {
        eprintln!("skip session test: redis is unavailable");
        return;
    }

    let downloader = create_downloader(cache_service, "tests-dag-module-session");
    let (addr, shutdown_tx) = start_test_server().await;
    let base_url = format!("http://{}", addr);

    let module_key = "acc1-p1-dag_session";

    let cfg_disabled = DownloadConfig::load(&None, &config.download_config);
    assert!(
        !cfg_disabled.enable_session,
        "tests/config.toml should default enable_session=false"
    );
    downloader.set_config(module_key, cfg_disabled).await;

    let run_a = Uuid::new_v4();
    let login_resp_a = downloader
        .download(make_request(format!("{}/login", base_url), "dag_session", run_a))
        .await
        .expect("login should succeed with session disabled");
    assert_eq!(login_resp_a.status_code, 200);

    let data_resp_a = downloader
        .download(make_request(format!("{}/data", base_url), "dag_session", run_a))
        .await
        .expect("data should respond with session disabled");
    assert_eq!(data_resp_a.status_code, 401);

    let mut cfg_enabled = DownloadConfig::load(&None, &config.download_config);
    cfg_enabled.enable_session = true;
    downloader.set_config(module_key, cfg_enabled).await;

    let run_b = Uuid::new_v4();
    let login_resp_b = downloader
        .download(make_request(format!("{}/login", base_url), "dag_session", run_b))
        .await
        .expect("login should succeed with session enabled");
    assert_eq!(login_resp_b.status_code, 200);

    let data_resp_b = downloader
        .download(make_request(format!("{}/data", base_url), "dag_session", run_b))
        .await
        .expect("data should respond with session enabled");
    assert_eq!(data_resp_b.status_code, 200);
    assert_eq!(String::from_utf8_lossy(&data_resp_b.content), "secret-data");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn distributed_multi_node_session_sharing_works() {
    let config = load_test_config();
    let Some(cache_service) = build_cache_service_from_config(&config) else {
        eprintln!("skip distributed session test: cache.redis is not configured");
        return;
    };

    if cache_service.ping().await.is_err() {
        eprintln!("skip distributed session test: redis is unavailable");
        return;
    }

    let downloader_a = create_downloader(cache_service.clone(), "tests-dag-node-a");
    let downloader_b = create_downloader(cache_service.clone(), "tests-dag-node-b");
    let downloader_c = create_downloader(cache_service, "tests-dag-node-c");

    let mut cfg_enabled = DownloadConfig::load(&None, &config.download_config);
    cfg_enabled.enable_session = true;
    let module_key = "acc1-p1-dag_dist_session";
    downloader_a.set_config(module_key, cfg_enabled.clone()).await;
    downloader_b.set_config(module_key, cfg_enabled.clone()).await;
    downloader_c.set_config(module_key, cfg_enabled).await;

    let (addr, shutdown_tx) = start_test_server().await;
    let base_url = format!("http://{}", addr);
    let shared_run_id = Uuid::new_v4();

    let login_resp = downloader_a
        .download(make_request(
            format!("{}/login", base_url),
            "dag_dist_session",
            shared_run_id,
        ))
        .await
        .expect("login should succeed on node A");
    assert_eq!(login_resp.status_code, 200);

    let fut_b = downloader_b.download(make_request(
        format!("{}/data", base_url),
        "dag_dist_session",
        shared_run_id,
    ));
    let fut_c = downloader_c.download(make_request(
        format!("{}/data", base_url),
        "dag_dist_session",
        shared_run_id,
    ));

    let (resp_b, resp_c) = tokio::join!(fut_b, fut_c);
    let resp_b = resp_b.expect("node B request should succeed");
    let resp_c = resp_c.expect("node C request should succeed");

    assert_eq!(resp_b.status_code, 200);
    assert_eq!(resp_c.status_code, 200);
    assert_eq!(String::from_utf8_lossy(&resp_b.content), "secret-data");
    assert_eq!(String::from_utf8_lossy(&resp_c.content), "secret-data");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn single_node_mode_without_redis_runs_full_chain() {
    let config = load_config_by_file("config.mock.pure.engine.toml");
    assert!(
        config.is_single_node_mode(),
        "config without cache.redis should default to single-node mode"
    );
    assert!(
        config.cache.redis.is_none(),
        "single-node test config should not provide cache.redis"
    );

    let cache_service = build_cache_service_with_single_node_fallback(
        &config,
        "tests-dag-module-single-node",
    );
    let downloader = create_downloader(cache_service, "tests-dag-single-node-a");

    let (addr, shutdown_tx) = start_test_server().await;
    let base_url = format!("http://{}", addr);
    let module: Arc<dyn ModuleTrait> = Arc::new(DagModuleTraitTestModule {
        base_url: base_url.clone(),
    });

    let sums = execute_module_steps(module.clone(), downloader.clone())
        .await
        .expect("single-node ModuleTrait steps should execute");
    assert_eq!(sums, vec![1, 3, 6]);

    let orchestrator = ModuleDagOrchestrator::default();
    let dag = orchestrator
        .compile_linear_compat(module)
        .await
        .expect("single-node dag compile should succeed");
    let report = orchestrator
        .execute_dag(dag)
        .await
        .expect("single-node dag execute should succeed");
    assert!(report.outputs.contains_key("step_0"));
    assert!(report.outputs.contains_key("step_1"));
    assert!(report.outputs.contains_key("step_2"));

    let mut cfg_enabled = DownloadConfig::load(&None, &config.download_config);
    cfg_enabled.enable_session = true;
    downloader
        .set_config("acc1-p1-single_node", cfg_enabled.clone())
        .await;

    let shared_run_id = Uuid::new_v4();
    let login_resp = downloader
        .download(make_request(
            format!("{}/login", base_url),
            "single_node",
            shared_run_id,
        ))
        .await
        .expect("single-node login should succeed");
    assert_eq!(login_resp.status_code, 200);

    let same_node_data_resp = downloader
        .download(make_request(
            format!("{}/data", base_url),
            "single_node",
            shared_run_id,
        ))
        .await
        .expect("single-node session reuse should succeed in same instance");
    assert_eq!(same_node_data_resp.status_code, 200);

    let second_node_cache = build_cache_service_with_single_node_fallback(
        &config,
        "tests-dag-module-single-node-b",
    );
    let downloader_b = create_downloader(second_node_cache, "tests-dag-single-node-b");
    downloader_b
        .set_config("acc1-p1-single_node", cfg_enabled)
        .await;

    let cross_node_data_resp = downloader_b
        .download(make_request(
            format!("{}/data", base_url),
            "single_node",
            shared_run_id,
        ))
        .await
        .expect("second single-node instance request should return response");
    assert_eq!(
        cross_node_data_resp.status_code,
        401,
        "without redis, session should not be shared across different node instances"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn engine_driven_single_node_full_chain_runs() {
    let (addr, server_shutdown) = start_test_server().await;
    let base_url = format!("http://{}", addr);
    let module_name = "engine.dag.module.trait.e2e";

    let config_path = prepare_single_node_engine_config();
    let state = Arc::new(State::new(&config_path).await);
    assert!(
        state.config.read().await.is_single_node_mode(),
        "engine e2e config should run as single-node mode"
    );
    init_single_node_sqlite_schema(&state).await;
    let engine = Arc::new(Engine::new(state, None).await);
    let module = Arc::new(EngineDagModuleTraitTestModule { base_url });
    engine.register_module(module.clone()).await;

    let mut module_cfg = ModuleConfig::default();
    module_cfg.module_config = json!({"downloader": "request_downloader"});
    module_cfg
        .send(&format!("acc1-p1-{module_name}"), &engine.state.cache_service)
        .await
        .expect("seed module config cache for engine e2e");

    let mut event_rx = engine
        .event_bus
        .as_ref()
        .expect("event bus should be enabled in engine e2e config")
        .subscribe("*".to_string())
        .await;

    let engine_for_start = engine.clone();
    let start_handle = tokio::spawn(async move {
        engine_for_start.start().await;
    });

    let run_id = Uuid::new_v4();
    enqueue_engine_step_requests(&engine, module.clone(), module_name, run_id).await;

    let completed_downloads =
        wait_download_completed_count(&mut event_rx, module_name, 3, Duration::from_secs(20)).await;

    engine.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), start_handle).await;
    let _ = server_shutdown.send(());

    assert!(
        completed_downloads >= 3,
        "engine full chain should execute at least three module download steps"
    );
}

#[tokio::test]
async fn engine_dual_node_concurrent_full_chain_runs() {
    let (addr, server_shutdown) = start_test_server().await;
    let base_url = format!("http://{}", addr);
    let module_name = "engine.dag.module.trait.e2e";

    let config_path_a = prepare_single_node_engine_config();
    let state_a = Arc::new(State::new(&config_path_a).await);
    init_single_node_sqlite_schema(&state_a).await;
    let engine_a = Arc::new(Engine::new(state_a, None).await);

    let config_path_b = prepare_single_node_engine_config();
    let state_b = Arc::new(State::new(&config_path_b).await);
    init_single_node_sqlite_schema(&state_b).await;
    let engine_b = Arc::new(Engine::new(state_b, None).await);

    let module_a = Arc::new(EngineDagModuleTraitTestModule {
        base_url: base_url.clone(),
    });
    let module_b = Arc::new(EngineDagModuleTraitTestModule {
        base_url: base_url.clone(),
    });
    engine_a.register_module(module_a.clone()).await;
    engine_b.register_module(module_b.clone()).await;

    let mut module_cfg_a = ModuleConfig::default();
    module_cfg_a.module_config = json!({"downloader": "request_downloader"});
    module_cfg_a
        .send(&format!("acc1-p1-{module_name}"), &engine_a.state.cache_service)
        .await
        .expect("seed module config cache for engine A");

    let mut module_cfg_b = ModuleConfig::default();
    module_cfg_b.module_config = json!({"downloader": "request_downloader"});
    module_cfg_b
        .send(&format!("acc1-p1-{module_name}"), &engine_b.state.cache_service)
        .await
        .expect("seed module config cache for engine B");

    let mut event_rx_a = engine_a
        .event_bus
        .as_ref()
        .expect("event bus should be enabled for engine A")
        .subscribe("*".to_string())
        .await;
    let mut event_rx_b = engine_b
        .event_bus
        .as_ref()
        .expect("event bus should be enabled for engine B")
        .subscribe("*".to_string())
        .await;

    let engine_a_for_start = engine_a.clone();
    let start_handle_a = tokio::spawn(async move {
        engine_a_for_start.start().await;
    });
    let engine_b_for_start = engine_b.clone();
    let start_handle_b = tokio::spawn(async move {
        engine_b_for_start.start().await;
    });

    enqueue_engine_step_requests(&engine_a, module_a, module_name, Uuid::new_v4()).await;
    enqueue_engine_step_requests(&engine_b, module_b, module_name, Uuid::new_v4()).await;

    let completed_a =
        wait_download_completed_count(&mut event_rx_a, module_name, 3, Duration::from_secs(20)).await;
    let completed_b =
        wait_download_completed_count(&mut event_rx_b, module_name, 3, Duration::from_secs(20)).await;

    engine_a.shutdown().await;
    engine_b.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), start_handle_a).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), start_handle_b).await;
    let _ = server_shutdown.send(());

    assert!(
        completed_a >= 3,
        "engine A should execute at least three module download steps"
    );
    assert!(
        completed_b >= 3,
        "engine B should execute at least three module download steps"
    );
}

#[tokio::test]
async fn engine_task_queue_driven_single_node_full_chain_runs() {
    let (addr, server_shutdown) = start_test_server().await;
    let base_url = format!("http://{}", addr);
    let module_name = "engine.task.queue.module.trait.e2e";

    let config_path = prepare_single_node_engine_config();
    let state = Arc::new(State::new(&config_path).await);
    assert!(
        state.config.read().await.is_single_node_mode(),
        "task-queue e2e config should run as single-node mode"
    );
    init_single_node_sqlite_schema(&state).await;
    ensure_task_queue_support_tables(&state).await;
    seed_task_queue_module_scope(&state, module_name).await;

    let engine = Arc::new(Engine::new(state, None).await);
    engine
        .register_module(Arc::new(TaskQueueDagModuleTraitTestModule { base_url }))
        .await;

    let mut event_rx = engine
        .event_bus
        .as_ref()
        .expect("event bus should be enabled in task-queue e2e config")
        .subscribe("*".to_string())
        .await;

    let engine_for_start = engine.clone();
    let start_handle = tokio::spawn(async move {
        engine_for_start.start().await;
    });

    let task = mocra::common::model::message::TaskEvent {
        account: "acc1".to_string(),
        platform: "p1".to_string(),
        module: Some(vec![module_name.to_string()]),
        priority: Default::default(),
        run_id: Uuid::new_v4(),
    };
    engine
        .queue_manager
        .get_task_push_channel()
        .send(QueuedItem::new(task))
        .await
        .expect("enqueue task event should succeed");

    let completed_downloads =
        wait_download_completed_count(&mut event_rx, module_name, 3, Duration::from_secs(25)).await;

    engine.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), start_handle).await;
    let _ = server_shutdown.send(());

    assert!(
        completed_downloads >= 3,
        "task-queue full chain should execute at least three module download steps"
    );
}

#[tokio::test]
async fn engine_task_queue_primary_cutover_emits_preview_ok_metric() {
    let (addr, server_shutdown) = start_test_server().await;
    let base_url = format!("http://{}", addr);
    let module_name = "engine.task.queue.module.trait.e2e";

    let config_path = prepare_single_node_engine_config();
    let state = Arc::new(State::new(&config_path).await);
    init_single_node_sqlite_schema(&state).await;
    ensure_task_queue_support_tables(&state).await;
    seed_task_queue_module_scope(&state, module_name).await;
    update_task_queue_module_config(
        &state,
        module_name,
        json!({
            "downloader": "request_downloader",
            "module_dag_enabled": true,
            "module_dag_force_linear_compat": true,
            "module_dag_whitelist": [module_name],
            "module_dag_execute_primary": true,
            "module_dag_execute_primary_cutover": true,
            "module_dag_cutover_whitelist": [module_name],
            "module_dag_cutover_require_shadow_match": false,
            "module_dag_cutover_require_warmup": false,
            "module_dag_cutover_fail_threshold": 2,
            "module_dag_cutover_recovery_window_secs": 300
        }),
    )
    .await;

    let engine = Arc::new(Engine::new(state, None).await);
    engine
        .register_module(Arc::new(TaskQueueDagModuleTraitTestModule { base_url }))
        .await;

    let mut event_rx = engine
        .event_bus
        .as_ref()
        .expect("event bus should be enabled in task-queue cutover e2e config")
        .subscribe("*".to_string())
        .await;

    let engine_for_start = engine.clone();
    let start_handle = tokio::spawn(async move {
        engine_for_start.start().await;
    });

    let module_scope = format!("acc1-p1-{module_name}");
    engine
        .task_manager
        .record_module_dag_cutover_failure(&module_scope);
    assert!(
        engine
            .task_manager
            .should_allow_module_dag_cutover(&module_scope, 2, 300),
        "single pre-seeded failure should not block cutover when threshold=2"
    );

    let task = mocra::common::model::message::TaskEvent {
        account: "acc1".to_string(),
        platform: "p1".to_string(),
        module: Some(vec![module_name.to_string()]),
        priority: Default::default(),
        run_id: Uuid::new_v4(),
    };
    engine
        .queue_manager
        .get_task_push_channel()
        .send(QueuedItem::new(task))
        .await
        .expect("enqueue task event should succeed");

    let completed_downloads =
        wait_download_completed_count(&mut event_rx, module_name, 3, Duration::from_secs(25)).await;

    engine.shutdown().await;
    let _ = tokio::time::timeout(Duration::from_secs(10), start_handle).await;
    let _ = server_shutdown.send(());

    assert!(
        completed_downloads >= 3,
        "task-queue primary cutover flow should execute at least three module download steps"
    );

    let cutover_failure_cleared = engine
        .task_manager
        .should_allow_module_dag_cutover(&module_scope, 1, 0);
    assert!(
        cutover_failure_cleared,
        "primary cutover success path should clear pre-seeded failure state for scope={module_scope}"
    );
}
