mod modules;
mod middleware;
use tokio::signal;
use std::path::Path;
use std::sync::Arc;
use mocra::common::state::State;
use mocra::engine::engine::Engine;
use mocra::utils::logger;


async fn build_engine_with_logger(enable_logger: bool) -> Engine {
    let config_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
    let state = Arc::new(State::new(config_path.to_str().unwrap()).await);
    println!("Config loaded from {}", config_path.display());

    let _logging_enabled = enable_logger;

    let engine: Engine = Engine::new(Arc::clone(&state), None).await;
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

    engine
}

#[tokio::main]
async fn main() {
    let engine = build_engine_with_logger(true).await;
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
