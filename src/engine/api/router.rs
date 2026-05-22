use crate::common::model::message::TaskEvent;
use crate::engine::api::auth::auth_middleware;
use crate::engine::api::config::{
    create_account, create_middleware, create_module, create_platform, create_profile,
    delete_account, delete_middleware, delete_module, delete_platform, get_account, get_middleware,
    get_module, get_platform, get_profile, list_accounts, list_middlewares, list_modules,
    list_platforms, list_profiles, patch_account, patch_middleware, patch_module, patch_platform,
    patch_profile,
};
use crate::engine::api::control::{
    get_cluster_leader, get_module_fallback_gate, get_nodes, pause_engine, resume_engine,
};
use crate::engine::api::debug::{
    get_debug_cached_response, get_debug_config, get_debug_profile, get_debug_status,
    get_debug_status_counts, list_debug_status_by_stage,
};
use crate::engine::api::dlq::{delete_dlq_message, get_dlq_messages, requeue_dlq_message};
use crate::engine::api::health;
use crate::engine::api::limit;
use crate::engine::api::state::ApiState;
use crate::queue::QueuedItem;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router, middleware};
use std::time::Instant;

/// Configures and returns the Axum router for the Engine API.
///
/// # Routes
/// - Public:
///   - `GET /metrics`: Prometheus metrics
///   - `GET /health`: Health check (Excluded from Rate Limit)
/// - Protected (Requires API Key, Rate Limited):
///   - `POST /tasks/dispatch`: Inject a manual task
///   - `GET /cluster/nodes`: List active nodes
///   - `GET /cluster/leader`: Inspect the current control-plane leader
///   - `GET /config/profiles`: List task profiles
///   - `POST /config/profiles`: Upsert a task profile
///   - `GET /config/profiles/{account}/{platform}/{module}`: Read one task profile
///   - `PATCH /config/profiles/{account}/{platform}/{module}`: Partially update one task profile
///   - `GET|POST|PATCH|DELETE /config/accounts*`: Account default config control plane
///   - `GET|POST|PATCH|DELETE /config/platforms*`: Platform default config control plane
///   - `GET|POST|PATCH|DELETE /config/modules*`: Module default config control plane
///   - `GET|POST|PATCH|DELETE /config/middlewares*`: Middleware config control plane
///   - `GET /dlq/messages`: Inspect Dead Letter Queue
///   - `POST /dlq/messages/{id}/requeue`: Replay one DLQ record
///   - `DELETE /dlq/messages/{id}`: Delete one DLQ record
///   - `GET /debug/status/{task_id}`: Inspect one persisted task status
///   - `GET /debug/status/stage/{stage}?limit=N`: List persisted task statuses by stage
///   - `GET /debug/status/counts`: Inspect persisted task status aggregates
///   - `GET /debug/cache/response/{cache_key}`: Read one locally cached response payload
///   - `GET /debug/config/{account}/{platform}/{module}`: Explain merged config layers
///   - `GET /debug/profile/{account}/{platform}/{module}`: Inspect stored task profile snapshot
///   - `POST /control/pause`: Pause global engine
///   - `POST /control/resume`: Resume global engine
///   - `GET /control/fallback-gates/{module}`: Inspect scheduler ingress fallback gate state
pub fn router(state: ApiState) -> Router {
    let protected_routes = Router::new()
        .route("/tasks/dispatch", post(dispatch_task))
        .route("/cluster/nodes", get(get_nodes))
        .route("/cluster/leader", get(get_cluster_leader))
        .route("/config/accounts", get(list_accounts).post(create_account))
        .route(
            "/config/accounts/{name}",
            get(get_account).patch(patch_account).delete(delete_account),
        )
        .route(
            "/config/platforms",
            get(list_platforms).post(create_platform),
        )
        .route(
            "/config/platforms/{name}",
            get(get_platform)
                .patch(patch_platform)
                .delete(delete_platform),
        )
        .route("/config/modules", get(list_modules).post(create_module))
        .route(
            "/config/modules/{name}",
            get(get_module).patch(patch_module).delete(delete_module),
        )
        .route(
            "/config/middlewares",
            get(list_middlewares).post(create_middleware),
        )
        .route(
            "/config/middlewares/{name}",
            get(get_middleware)
                .patch(patch_middleware)
                .delete(delete_middleware),
        )
        .route("/config/profiles", get(list_profiles).post(create_profile))
        .route(
            "/config/profiles/{account}/{platform}/{module}",
            get(get_profile).patch(patch_profile),
        )
        .route("/dlq/messages", get(get_dlq_messages))
        .route("/dlq/messages/{id}/requeue", post(requeue_dlq_message))
        .route("/dlq/messages/{id}", delete(delete_dlq_message))
        .route("/debug/status/counts", get(get_debug_status_counts))
        .route(
            "/debug/status/stage/{stage}",
            get(list_debug_status_by_stage),
        )
        .route("/debug/status/{task_id}", get(get_debug_status))
        .route(
            "/debug/cache/response/{cache_key}",
            get(get_debug_cached_response),
        )
        .route(
            "/debug/config/{account}/{platform}/{module}",
            get(get_debug_config),
        )
        .route(
            "/debug/profile/{account}/{platform}/{module}",
            get(get_debug_profile),
        )
        .route("/control/pause", post(pause_engine))
        .route("/control/resume", post(resume_engine))
        .route(
            "/control/fallback-gates/{module}",
            get(get_module_fallback_gate),
        )
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    let rate_limited_protected_routes = protected_routes.route_layer(
        middleware::from_fn_with_state(state.clone(), limit::rate_limit_middleware),
    );

    let public_routes = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health::health_check));

    public_routes
        .merge(rate_limited_protected_routes)
        .with_state(state)
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

/// Handler for `POST /tasks/dispatch`.
/// Pushes a typed task dispatch envelope into the task ingress queue.
pub async fn dispatch_task(
    State(app_state): State<ApiState>,
    Json(task): Json<TaskEvent>,
) -> StatusCode {
    let started = Instant::now();
    let task_pop_chain = app_state.queue_manager.get_task_push_channel().clone();
    let dispatch = match build_task_dispatch(&task, app_state.queue_manager.namespace.clone()) {
        Ok(dispatch) => dispatch,
        Err(e) => {
            log::error!("Failed to build task dispatch envelope: {}", e);
            record_dispatch_status("dispatch_task", StatusCode::INTERNAL_SERVER_ERROR, started);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    if let Err(e) = task_pop_chain.send(QueuedItem::new(dispatch)).await {
        log::error!("Failed to send task to processing channel: {}", e);
        record_dispatch_status("dispatch_task", StatusCode::INTERNAL_SERVER_ERROR, started);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    record_dispatch_status("dispatch_task", StatusCode::ACCEPTED, started);
    StatusCode::ACCEPTED
}

fn record_dispatch_status(action: &str, status: StatusCode, started: Instant) {
    let result = match status {
        StatusCode::OK | StatusCode::ACCEPTED | StatusCode::CREATED | StatusCode::NO_CONTENT => {
            "success"
        }
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::BAD_REQUEST => "bad_request",
        _ => "error",
    };
    crate::common::metrics::inc_throughput("control_plane", "dispatch", action, result, 1);
    crate::common::metrics::observe_latency(
        "control_plane",
        "dispatch",
        action,
        result,
        started.elapsed().as_secs_f64(),
    );
    if result == "error" {
        crate::common::metrics::inc_error("control_plane", "dispatch", "http", status.as_str(), 1);
    }
}
use crate::engine::task::task_dispatch_adapter::build_task_dispatch;
