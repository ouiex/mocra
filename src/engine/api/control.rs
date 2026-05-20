use crate::engine::api::state::ApiState;
use axum::http::StatusCode;
use axum::{Json, extract::{Path, State}};
use serde::Serialize;
use std::time::Duration;
use std::time::Instant;

use crate::common::registry::NodeInfo;
use crate::sync::RaftLeaderView;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ClusterLeaderResponse {
    pub mode: String,
    pub local_node_id: Option<u64>,
    pub local_node_addr: Option<String>,
    pub leader_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub is_local_leader: bool,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FallbackGateConfigResponse {
    pub failure_threshold: usize,
    pub recovery_window_secs: u64,
    pub gray_ratio: f64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ModuleFallbackGateResponse {
    pub module: String,
    pub gate: FallbackGateConfigResponse,
    pub blocked: bool,
    pub failure_streak: usize,
    pub last_failure_ms: Option<u64>,
}

impl ClusterLeaderResponse {
    fn from_raft_view(view: RaftLeaderView) -> Self {
        Self {
            mode: "raft".to_string(),
            local_node_id: Some(view.local_node_id),
            local_node_addr: Some(view.local_node_addr),
            leader_id: view.leader_id,
            leader_addr: view.leader_addr,
            is_local_leader: view.is_local_leader,
        }
    }

    fn local() -> Self {
        Self {
            mode: "local".to_string(),
            local_node_id: None,
            local_node_addr: None,
            leader_id: None,
            leader_addr: None,
            is_local_leader: true,
        }
    }
}

pub async fn get_nodes(State(state): State<ApiState>) -> Json<Vec<NodeInfo>> {
    let started = Instant::now();
    let nodes = state
        .state
        .profile_store
        .list_active_nodes(Duration::from_secs(30));
    crate::common::metrics::inc_throughput(
        "control_plane",
        "cluster",
        "get_nodes",
        "success",
        1,
    );
    crate::common::metrics::observe_latency(
        "control_plane",
        "cluster",
        "get_nodes",
        "success",
        started.elapsed().as_secs_f64(),
    );
    Json(nodes)
}

pub async fn get_cluster_leader(
    State(state): State<ApiState>,
) -> Result<Json<ClusterLeaderResponse>, StatusCode> {
    let started = Instant::now();

    if let Some(runtime) = state.state.raft_runtime.as_ref() {
        let leader = runtime.leader_view();
        if leader.leader_id.is_none() {
            record_control_status(
                "get_cluster_leader",
                StatusCode::SERVICE_UNAVAILABLE,
                started,
            );
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }

        record_control_status("get_cluster_leader", StatusCode::OK, started);
        return Ok(Json(ClusterLeaderResponse::from_raft_view(leader)));
    }

    record_control_status("get_cluster_leader", StatusCode::OK, started);
    Ok(Json(ClusterLeaderResponse::local()))
}

pub async fn pause_engine(State(state): State<ApiState>) -> StatusCode {
    let started = Instant::now();
    if let Err(e) = state.state.profile_store.set_pause_state(true).await {
        log::error!("Failed to set pause flag: {}", e);
        record_control_status("pause_engine", StatusCode::INTERNAL_SERVER_ERROR, started);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    record_control_status("pause_engine", StatusCode::OK, started);
    StatusCode::OK
}

pub async fn resume_engine(State(state): State<ApiState>) -> StatusCode {
    let started = Instant::now();
    if let Err(e) = state.state.profile_store.set_pause_state(false).await {
        log::error!("Failed to remove pause flag: {}", e);
        record_control_status("resume_engine", StatusCode::INTERNAL_SERVER_ERROR, started);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    record_control_status("resume_engine", StatusCode::OK, started);
    StatusCode::OK
}

pub async fn get_module_fallback_gate(
    State(state): State<ApiState>,
    Path(module): Path<String>,
) -> Json<ModuleFallbackGateResponse> {
    let started = Instant::now();
    let gate = {
        let config = state.state.config.read().await;
        config.crawler.scheduler_ingress_cutover_gate_config()
    };

    let snapshot = state.task_manager.module_dag_cutover_gate_state(
        module.as_str(),
        gate.failure_threshold,
        gate.recovery_window_secs,
    );

    record_control_status("get_module_fallback_gate", StatusCode::OK, started);
    Json(ModuleFallbackGateResponse {
        module,
        gate: FallbackGateConfigResponse {
            failure_threshold: gate.failure_threshold,
            recovery_window_secs: gate.recovery_window_secs,
            gray_ratio: gate.gray_ratio,
        },
        blocked: snapshot.blocked,
        failure_streak: snapshot.failure_streak,
        last_failure_ms: snapshot.last_failure_ms,
    })
}

fn record_control_status(action: &str, status: StatusCode, started: Instant) {
    let result = if status == StatusCode::OK { "success" } else { "error" };
    crate::common::metrics::inc_throughput("control_plane", "cluster", action, result, 1);
    crate::common::metrics::observe_latency(
        "control_plane",
        "cluster",
        action,
        result,
        started.elapsed().as_secs_f64(),
    );
    if result == "error" {
        crate::common::metrics::inc_error(
            "control_plane",
            "cluster",
            "http",
            status.as_str(),
            1,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterLeaderResponse, FallbackGateConfigResponse};
    use crate::sync::RaftLeaderView;

    #[test]
    fn cluster_leader_response_maps_raft_view() {
        let response = ClusterLeaderResponse::from_raft_view(RaftLeaderView {
            local_node_id: 11,
            local_node_addr: "127.0.0.1:3101".to_string(),
            leader_id: Some(12),
            leader_addr: Some("127.0.0.1:3102".to_string()),
            is_local_leader: false,
        });

        assert_eq!(response.mode, "raft");
        assert_eq!(response.local_node_id, Some(11));
        assert_eq!(response.leader_id, Some(12));
        assert_eq!(response.leader_addr.as_deref(), Some("127.0.0.1:3102"));
        assert!(!response.is_local_leader);
    }

    #[test]
    fn cluster_leader_response_local_mode_is_self_led() {
        let response = ClusterLeaderResponse::local();

        assert_eq!(response.mode, "local");
        assert_eq!(response.local_node_id, None);
        assert_eq!(response.leader_id, None);
        assert!(response.is_local_leader);
    }

    #[test]
    fn fallback_gate_config_response_keeps_values() {
        let response = FallbackGateConfigResponse {
            failure_threshold: 3,
            recovery_window_secs: 60,
            gray_ratio: 0.25,
        };

        assert_eq!(response.failure_threshold, 3);
        assert_eq!(response.recovery_window_secs, 60);
        assert_eq!(response.gray_ratio, 0.25);
    }
}
