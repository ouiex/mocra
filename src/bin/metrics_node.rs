use std::path::Path;
use std::sync::Arc;
use tokio::signal;

use mocra::common::state::State;
use mocra::engine::engine::Engine;

#[tokio::main]
async fn main() {
    let config_path = Path::new("monitoring").join("local_engine.toml");
    let state = match State::try_new(config_path.to_str().expect("config path")).await {
        Ok(state) => Arc::new(state),
        Err(e) => {
            eprintln!("metrics_node failed to initialize state: {e}");
            std::process::exit(1);
        }
    };
    let engine = Engine::new(Arc::clone(&state), None).await;

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
