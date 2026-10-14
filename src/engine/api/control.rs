use crate::common::registry::NodeInfo;
use crate::engine::api::state::ApiState;
use axum::http::StatusCode;
use axum::{Json, extract::State};

pub async fn get_nodes(State(state): State<ApiState>) -> Json<Vec<NodeInfo>> {
    match state.node_registry.get_active_nodes().await {
        Ok(nodes) => Json(nodes),
        Err(e) => {
            log::error!("Failed to get nodes: {}", e);
            Json(vec![])
        }
    }
}

pub async fn pause_engine(State(state): State<ApiState>) -> StatusCode {
    let key = pause_key(&state);
    let result = if let Some(coordination) = &state.state.coordination {
        coordination
            .set(&key, b"1")
            .await
            .map_err(|e| e.to_string())
    } else {
        state
            .state
            .cache_service
            .set(&key, b"1", None)
            .await
            .map_err(|e| e.to_string())
    };
    if let Err(e) = result {
        log::error!("Failed to set pause flag: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

pub async fn resume_engine(State(state): State<ApiState>) -> StatusCode {
    let key = pause_key(&state);
    let result = if let Some(coordination) = &state.state.coordination {
        coordination
            .set(&key, b"0")
            .await
            .map_err(|e| e.to_string())
    } else {
        state
            .state
            .cache_service
            .del(&key)
            .await
            .map_err(|e| e.to_string())
    };
    if let Err(e) = result {
        log::error!("Failed to remove pause flag: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

fn pause_key(state: &ApiState) -> String {
    let ns = state.state.cache_service.namespace();
    if ns.is_empty() {
        log::warn!("Cache namespace is empty; set config.name for safe global pause");
        "engine:pause".to_string()
    } else {
        format!("{ns}:engine:pause")
    }
}
