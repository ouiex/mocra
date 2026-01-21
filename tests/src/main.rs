mod modules;
mod middleware;
use tokio::signal;
use std::path::Path;
use std::sync::Arc;
use common::state::State;
use engine::engine::Engine;
use utils::logger;
use utils::logger::{set_log_sender, LogSender, LoggerConfig};

async fn build_engine() -> Engine {
    let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.dev.toml");
    let state = Arc::new(State::new(config_path.to_str().unwrap()).await);
    println!("Config loaded from {}", config_path.display());

    // logger init
    let namespace = state.config.read().await.name.clone();
    let mut logger_config = LoggerConfig::default();
    logger_config.level = "info,sqlx=error,sea_orm=error".to_string();
    logger_config.file_path = Some(Path::new("logs").join(format!("mocra.{}", namespace)));
    logger::init_logger(logger_config).await.ok();

    let engine = Engine::new(Arc::clone(&state)).await;
    for middleware in middleware::register_data_middlewares(){
        engine.register_data_middleware(middleware).await;
    }
    for middleware in middleware::register_download_middlewares(){
        engine.register_download_middleware(middleware).await;
    }
    for middleware in middleware::register_data_store_middlewares(){
        engine.register_store_middleware(middleware).await;
    }

    for module in modules::register_modules(){
        engine.register_module(module).await;
    }

    let _ = set_log_sender(LogSender {
        sender: engine.queue_manager.get_log_pop_channel(),
        level: "WARN".to_string(),
    });

    engine
}

#[tokio::main]
async fn main() {

    // Build engine (includes logger init inside library)
    let engine = build_engine().await;
    // 并行等待：
    // 1) 引擎运行完成（例如内部收到关闭信号后返回）
    // 2) 收到 Ctrl+C，触发优雅关闭
    let start_fut = engine.start();
    tokio::pin!(start_fut);
    tokio::select! {
        _ = &mut start_fut => {
            // 引擎正常退出
        }
        _ = signal::ctrl_c() => {
            // 收到中断信号，通知引擎优雅退出
            engine.shutdown().await;
            // 等待引擎完成（可选：再次等待 start 完成）
        }
    }
}