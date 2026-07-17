//! Observability endpoints: cluster / engine runtime status, consumed by admin / monitoring pages.
//!
//! - `GET /observability/cluster` → [`ClusterStatusView`]; only populated with embedded Raft,
//!   otherwise `null`.
//! - `GET /observability/engine`  → [`EngineStats`] (engine namespace, mode, and per-queue pending
//!   counts).
//!
//! Metrics (latency / throughput / error rate) go through the existing `GET /metrics` (Prometheus
//! text format).

use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};

use crate::engine::api::state::ApiState;
use crate::engine::monitor::{SystemSnapshot, latest_system_snapshot};
use crate::sync::ClusterStatusView;
use crate::utils::logger::LogRecord;

/// Cluster status: returns a snapshot when coordinating via embedded Raft, otherwise `null`.
pub async fn cluster_status(State(state): State<ApiState>) -> Json<Option<ClusterStatusView>> {
    Json(
        state
            .state
            .coordination
            .as_ref()
            .and_then(|c| c.cluster_status()),
    )
}

/// Pending counts per queue (task/request/response/parser/error/remote-task).
#[derive(Debug, Clone, Serialize)]
pub struct PendingBreakdown {
    pub task: usize,
    pub download: usize,
    pub response: usize,
    pub parser: usize,
    pub error: usize,
    pub remote_task: usize,
    pub total: usize,
}

/// Engine / queue runtime snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct EngineStats {
    /// Namespace (`config.name`).
    pub namespace: String,
    /// Whether the runtime is in single-node mode.
    pub single_node: bool,
    /// Whether cluster coordination is wired up (embedded Raft, etc.).
    pub clustered: bool,
    /// Local pending counts per queue.
    pub pending: PendingBreakdown,
}

pub async fn engine_stats(State(state): State<ApiState>) -> Json<EngineStats> {
    let (task, download, response, parser, error, remote_task) =
        state.queue_manager.local_pending_breakdown().await;
    let total = task + download + response + parser + error + remote_task;
    let cfg = state.state.config.read().await;
    Json(EngineStats {
        namespace: cfg.name.clone(),
        single_node: state.state.coordination.is_none(),
        clustered: state.state.coordination.is_some(),
        pending: PendingBreakdown {
            task,
            download,
            response,
            parser,
            error,
            remote_task,
            total,
        },
    })
}

/// Most recent snapshot of host resources (CPU / memory / swap). The first snapshot appears as soon
/// as the engine starts.
pub async fn system_stats() -> Json<Option<SystemSnapshot>> {
    Json(latest_system_snapshot())
}

/// Log query parameters.
#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    /// How many recent records to return (default 200, capped at 1000).
    pub limit: Option<usize>,
}

/// Recent logs (newest first). Consumed by the admin page's log panel.
pub async fn recent_logs(Query(q): Query<LogsQuery>) -> Json<Vec<LogRecord>> {
    let limit = q.limit.unwrap_or(200).min(1000);
    Json(crate::utils::logger::recent_logs(limit))
}
