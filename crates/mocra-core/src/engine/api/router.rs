use crate::common::model::message::TaskEvent;
use crate::engine::api::auth::auth_middleware;
use crate::engine::api::control::{get_nodes, pause_engine, resume_engine};
use crate::engine::api::dlq::get_dlq;
use crate::engine::api::health;
use crate::engine::api::limit;
use crate::engine::api::observability;
use crate::engine::api::state::ApiState;
use crate::queue::QueuedItem;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router, middleware};
use tower_http::cors::CorsLayer;

/// Configures and returns the Axum router for the Engine API.
///
/// # Routes
/// - Static / health (no API key, no rate limit):
///   - `GET /`: built-in single-file admin dashboard (served with the `dashboard` feature)
///   - `GET /health`: Health check
/// - Monitoring (no API key, rate-limited) — for the dashboard / scrapers:
///   - `GET /metrics`: Prometheus metrics
///   - `GET /observability/cluster`: Raft cluster status (`null` when standalone)
///   - `GET /observability/engine`: engine / queue runtime snapshot
///   - `GET /observability/system`: host CPU / memory / swap snapshot
///   - `GET /observability/logs?limit=N`: recent structured logs (newest first)
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
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // Read-only monitoring endpoints: no API key required (consistent with /metrics), rate limiting
    // only. Polled by the admin page.
    let rate_limited_routes = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/observability/cluster", get(observability::cluster_status))
        .route("/observability/engine", get(observability::engine_stats))
        .route("/observability/system", get(observability::system_stats))
        .route("/observability/logs", get(observability::recent_logs))
        .merge(protected_routes)
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            limit::rate_limit_middleware,
        ));

    let health_route = Router::new()
        .route("/", get(dashboard_page))
        .route("/health", get(health::health_check));

    rate_limited_routes
        .merge(health_route)
        .with_state(state)
        // Allow a standalone frontend (browser) to access the dashboard API cross-origin.
        .layer(CorsLayer::permissive())
}

/// Built-in admin page (a single-file, zero-build frontend), compiled into the binary with the
/// `dashboard` feature.
///
/// Opening the engine address in a browser (`GET /`) shows the metrics / logs / tasks / performance
/// panels; when the page is served from the same origin it points at this engine automatically, with
/// no endpoint to fill in by hand. The same file can also be distributed on its own and pointed at
/// any endpoint.
async fn dashboard_page() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("../../../dashboard/index.html"))
}

/// Handler for Prometheus metrics endpoint.
/// Returns the rendered metrics in text format.
pub async fn metrics_handler(State(state): State<ApiState>) -> String {
    if let Some(handle) = &state.prometheus_handle {
        handle.render()
    } else {
        "Prometheus metrics not available (recorder not initialized)".to_string()
    }
}

/// Handler to manually inject a `TaskModel` into the engine's processing queue.
/// Useful for testing or triggering specific tasks.
pub async fn start_work(State(app_state): State<ApiState>, Json(task): Json<TaskEvent>) {
    let task_pop_chain = app_state.queue_manager.get_task_push_channel().clone();
    if let Err(e) = task_pop_chain.send(QueuedItem::new(task)).await {
        log::error!("Failed to send task to processing channel: {}", e);
    }
}
