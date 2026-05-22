use async_trait::async_trait;
use sea_orm::{ConnectionTrait, DbBackend, Statement};
use std::path::Path;
use std::sync::Arc;
use tokio::signal;

use mocra::common::interface::{
    ModuleNodeTrait, ModuleTrait, NodeGenerateContext, NodeParseContext, SyncBoxStream,
    ToSyncBoxStream,
};
use mocra::common::model::request::RequestMethod;
use mocra::common::model::{CronConfig, NodeParseOutput, Request, Response};
use mocra::common::state::State;
use mocra::engine::api::{ApiState, build_router};
use mocra::engine::engine::Engine;
use mocra::errors::Result;

struct LocalProbeStartNode {
    health_url: String,
}

#[async_trait]
impl ModuleNodeTrait for LocalProbeStartNode {
    async fn generate(
        &self,
        _ctx: NodeGenerateContext<'_>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let mut request = Request::new(self.health_url.clone(), RequestMethod::Get.as_ref());
        request.account = "local".to_string();
        request.platform = "monitoring".to_string();
        request.module = "local_probe_module".to_string();
        request.context = request.context.with_node_id("step_0").with_step_idx(0);
        vec![request].into_stream_ok()
    }

    async fn parser(
        &self,
        _response: Response,
        _ctx: NodeParseContext<'_>,
    ) -> Result<NodeParseOutput> {
        Ok(NodeParseOutput::default().finish())
    }

    fn stable_node_key(&self) -> &'static str {
        "step_0"
    }
}

struct LocalProbeModule {
    health_url: String,
}

impl LocalProbeModule {
    const fn new(health_url: String) -> Self {
        Self { health_url }
    }
}

#[async_trait]
impl ModuleTrait for LocalProbeModule {
    fn should_login(&self) -> bool {
        false
    }

    fn name(&self) -> &'static str {
        "local_probe_module"
    }

    fn version(&self) -> i32 {
        1
    }

    fn default_arc() -> Arc<dyn ModuleTrait>
    where
        Self: Sized,
    {
        Arc::new(Self::new("http://127.0.0.1:8805/health".to_string()))
    }

    fn cron(&self) -> Option<CronConfig> {
        None
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(LocalProbeStartNode {
            health_url: self.health_url.clone(),
        })]
    }
}

async fn ensure_local_sqlite_task_catalog(state: &State) -> std::result::Result<(), String> {
    if state.db.get_database_backend() != DbBackend::Sqlite {
        return Ok(());
    }

    let base_dir = Path::new("monitoring").join("tmp");
    std::fs::create_dir_all(&base_dir).map_err(|err| err.to_string())?;
    let base_path = std::env::current_dir()
        .map_err(|err| err.to_string())?
        .join(base_dir)
        .join("mocra_metrics_base.sqlite");
    let base_path_sql = base_path
        .to_string_lossy()
        .replace('\\', "/")
        .replace('\'', "''");

    let attach_sql = format!("ATTACH DATABASE '{base_path_sql}' AS base");
    if let Err(err) = state
        .db
        .execute(Statement::from_string(DbBackend::Sqlite, attach_sql))
        .await
    {
        let msg = err.to_string();
        if !msg.contains("database base is already in use") {
            return Err(msg);
        }
    }

    let statements = [
        "CREATE TABLE IF NOT EXISTS base.module (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', priority INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, version INTEGER NOT NULL DEFAULT 1)",
        "CREATE TABLE IF NOT EXISTS base.data_middleware (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS base.rel_module_data_middleware (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, data_middleware_id INTEGER NOT NULL REFERENCES data_middleware(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, data_middleware_id))",
        "CREATE TABLE IF NOT EXISTS base.download_middleware (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS base.rel_module_download_middleware (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, download_middleware_id INTEGER NOT NULL REFERENCES download_middleware(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, download_middleware_id))",
        "CREATE TABLE IF NOT EXISTS base.account (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, modules TEXT NOT NULL DEFAULT '{}', enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', priority INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS base.platform (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255) UNIQUE NOT NULL, description TEXT, base_url VARCHAR(255), enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
        "CREATE TABLE IF NOT EXISTS base.rel_account_platform (account_id INTEGER NOT NULL REFERENCES account(id) ON DELETE CASCADE, platform_id INTEGER NOT NULL REFERENCES platform(id) ON DELETE CASCADE, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (account_id, platform_id))",
        "CREATE TABLE IF NOT EXISTS base.rel_module_account (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, account_id INTEGER NOT NULL REFERENCES account(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, account_id))",
        "CREATE TABLE IF NOT EXISTS base.rel_module_platform (module_id INTEGER NOT NULL REFERENCES module(id) ON DELETE CASCADE, platform_id INTEGER NOT NULL REFERENCES platform(id) ON DELETE CASCADE, priority INTEGER NOT NULL DEFAULT 0, enabled BOOLEAN NOT NULL DEFAULT 1, config TEXT NOT NULL DEFAULT '{}', created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (module_id, platform_id))",
        "INSERT OR IGNORE INTO base.module (name, version, priority, enabled) VALUES ('local_probe_module', 1, 5, 1)",
        "INSERT OR IGNORE INTO base.account (name, enabled, priority) VALUES ('local', 1, 5)",
        "INSERT OR IGNORE INTO base.platform (name, description, enabled) VALUES ('monitoring', 'Local monitoring runtime', 1)",
        "INSERT OR IGNORE INTO base.rel_module_account (module_id, account_id, priority, enabled) SELECT m.id, a.id, 5, 1 FROM base.module m, base.account a WHERE m.name = 'local_probe_module' AND a.name = 'local'",
        "INSERT OR IGNORE INTO base.rel_module_platform (module_id, platform_id, priority, enabled) SELECT m.id, p.id, 5, 1 FROM base.module m, base.platform p WHERE m.name = 'local_probe_module' AND p.name = 'monitoring'",
        "INSERT OR IGNORE INTO base.rel_account_platform (account_id, platform_id, enabled) SELECT a.id, p.id, 1 FROM base.account a, base.platform p WHERE a.name = 'local' AND p.name = 'monitoring'",
    ];

    for sql in statements {
        state
            .db
            .execute(Statement::from_string(DbBackend::Sqlite, sql.to_string()))
            .await
            .map_err(|err| format!("{err}; sql={sql}"))?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let config_path = Path::new("monitoring").join("local_engine.toml");
    eprintln!(
        "metrics_node: initializing state from {}",
        config_path.display()
    );
    let state = match State::try_new(config_path.to_str().expect("config path")).await {
        Ok(state) => Arc::new(state),
        Err(e) => {
            eprintln!("metrics_node failed to initialize state: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("metrics_node: state initialized");
    if let Err(err) = ensure_local_sqlite_task_catalog(state.as_ref()).await {
        eprintln!("metrics_node failed to initialize local sqlite task catalog: {err}");
        std::process::exit(1);
    }
    eprintln!("metrics_node: local sqlite task catalog ready");
    let engine = match Engine::new(Arc::clone(&state), None).await {
        Ok(e) => e,
        Err(e) => {
            eprintln!("metrics_node failed to initialize engine: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("metrics_node: engine initialized");

    let api_port = {
        let cfg = state.config.read().await;
        cfg.api.as_ref().map(|api| api.port)
    };
    if let Some(port) = api_port {
        eprintln!("metrics_node: starting API on port {port}");
        let health_url = format!("http://127.0.0.1:{port}/health");
        let module: Arc<dyn ModuleTrait> = Arc::new(LocalProbeModule::new(health_url));
        engine.register_module(module).await;
        eprintln!("metrics_node: local probe module registered");
        let api_state = ApiState::new(
            engine.queue_manager.clone(),
            engine.prometheus_handle.clone(),
            engine.state.clone(),
            engine.task_manager.clone(),
        );
        tokio::spawn(async move {
            let app = build_router(api_state);
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    eprintln!("metrics_node: API listener bound on {addr}");
                    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
                        eprintln!("metrics_node: API server error: {err}");
                    }
                }
                Err(err) => {
                    eprintln!("metrics_node: failed to bind API listener on {addr}: {err}");
                }
            }
        });
    }

    eprintln!("metrics_node: starting engine runtime");
    engine.start().await;
    eprintln!("metrics_node: engine runtime started; waiting for ctrl+c");

    if signal::ctrl_c().await.is_ok() {
        eprintln!("metrics_node: ctrl+c received; shutting down");
        engine.shutdown().await;
    }
}
