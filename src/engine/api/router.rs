
use axum::extract::State;
use axum::{Json, Router, middleware};
use axum::routing::{post, get};
use crate::common::model::message::TaskModel;
use crate::queue::QueuedItem;
use crate::engine::api::state::ApiState;
use crate::engine::api::control::{get_nodes, pause_engine, resume_engine};
use crate::engine::api::dlq::get_dlq;
use crate::engine::api::auth::auth_middleware;
use crate::engine::api::health;
use crate::engine::api::limit;

/// Configures and returns the Axum router for the Engine API.
///
/// # Routes
/// - Public:
///   - `GET /metrics`: Prometheus metrics
///   - `GET /health`: Health check (Excluded from Rate Limit)
/// - Protected (Requires API Key, Rate Limited):
///   - `POST /start_work`: Inject a manual task
///   - `GET /nodes`: List active nodes
///   - `GET /dlq`: Inspect Dead Letter Queue
///   - `POST /control/pause`: Pause global engine
///   - `POST /control/resume`: Resume global engine
pub fn router(state: ApiState) -> Router {
    let protected_routes = Router::new()
        .route("/start_work", post(start_work))
        .route("/nodes", get(get_nodes))
        .route("/dlq", get(get_dlq))
        .route("/control/pause", post(pause_engine))
        .route("/control/resume", post(resume_engine))
        .route_layer(middleware::from_fn_with_state(state.clone(), auth_middleware));

    let rate_limited_routes = Router::new()
        .route("/metrics", get(metrics_handler))
        .merge(protected_routes)
        .route_layer(middleware::from_fn_with_state(state.clone(), limit::rate_limit_middleware));

    let health_route = Router::new()
        .route("/health", get(health::health_check));

    rate_limited_routes.merge(health_route).with_state(state)
}

/// Handler for Prometheus metrics endpoint.
/// Returns the rendered metrics in text format.
pub async fn metrics_handler(
    State(state): State<ApiState>,
) -> String {
    if let Some(handle) = &state.prometheus_handle {
        handle.render()
    } else {
        "Prometheus metrics not available (recorder not initialized)".to_string()
    }
}


/// Handler to manually inject a `TaskModel` into the engine's processing queue.
/// Useful for testing or triggering specific tasks.
pub async fn start_work(
    State(app_state): State<ApiState>,
    Json(task): Json<TaskModel>,
){
    let task_pop_chain = app_state.queue_manager.get_task_push_channel().clone();
    if let Err(e) = task_pop_chain.send(QueuedItem::new(task)).await {
        log::error!("Failed to send task to processing channel: {}", e);
    }
}