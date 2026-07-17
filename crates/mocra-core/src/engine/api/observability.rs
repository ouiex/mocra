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

/// Counts per queue (task/request/response/parser/error/remote-task).
#[derive(Debug, Clone, Serialize)]
pub struct QueueBreakdown {
    pub task: usize,
    pub download: usize,
    pub response: usize,
    pub parser: usize,
    pub error: usize,
    pub remote_task: usize,
    pub total: usize,
}

/// Cumulative outcomes for one pipeline stage, since engine start.
#[derive(Debug, Clone, Serialize)]
pub struct StageOutcome {
    /// Items that finished successfully.
    pub success: u64,
    /// Items that failed.
    pub failure: u64,
    /// `success + failure` — items that finished either way.
    pub total: u64,
    /// `success / total`, or `null` before anything has finished.
    pub success_rate: Option<f64>,
}

impl StageOutcome {
    fn new(counter: &crate::engine::runner::StageCounter) -> Self {
        let (success, failure) = counter.snapshot();
        let total = success + failure;
        Self {
            success,
            failure,
            total,
            success_rate: (total > 0).then(|| success as f64 / total as f64),
        }
    }
}

/// Cumulative throughput per stage, since engine start.
///
/// Counts **terminal outcomes**: an item retried three times and then succeeding is one success,
/// not three failures plus a success. A retry storm therefore shows as the counters stalling rather
/// than as a failure spike — pair these with `outstanding` to tell "stuck" from "idle".
///
/// `parse` merges both parser processors (responses and parser tasks). `store` is the exception to
/// the terminal-outcome rule and counts per store operation; see `DataStoreProcessor` for why.
#[derive(Debug, Clone, Serialize)]
pub struct ThroughputStats {
    pub task: StageOutcome,
    pub request: StageOutcome,
    pub parse: StageOutcome,
    pub store: StageOutcome,
}

/// Engine / queue runtime snapshot.
///
/// The three breakdowns are different questions. A task is counted in exactly one of `pending` or
/// `inflight`, so `outstanding` is their sum:
///
/// - `pending` — queued, not yet picked up. Processors drain their channels as fast as items
///   arrive, so this sits at 0 on a healthy run; a non-zero value is **backpressure**, not
///   throughput.
/// - `inflight` — picked up and executing right now. This is the throughput signal.
/// - `outstanding` — the work the engine still owes, and what the idle-stop monitor treats as
///   "busy". Use this to answer "is anything happening?".
#[derive(Debug, Clone, Serialize)]
pub struct EngineStats {
    /// Namespace (`config.name`).
    pub namespace: String,
    /// Whether the runtime is in single-node mode.
    pub single_node: bool,
    /// Whether cluster coordination is wired up (embedded Raft, etc.).
    pub clustered: bool,
    /// Queued but not yet picked up (non-zero means consumers are falling behind).
    pub pending: QueueBreakdown,
    /// Currently executing.
    pub inflight: QueueBreakdown,
    /// `pending + inflight` — the work still owed.
    pub outstanding: QueueBreakdown,
    /// Cumulative per-stage success/failure since engine start. Poll twice and divide by the
    /// elapsed time for a rate.
    pub throughput: ThroughputStats,
}

pub async fn engine_stats(State(state): State<ApiState>) -> Json<EngineStats> {
    let (p_task, p_download, p_response, p_parser, p_error, p_remote) =
        state.queue_manager.local_pending_breakdown().await;
    // `remote_task` is the outbound queue for remote dispatch; nothing consumes it locally, so it
    // has a pending depth but never an in-flight count.
    let (i_task, i_download, i_response, i_parser, i_error) = state.inflight.breakdown();

    let pending = QueueBreakdown {
        task: p_task,
        download: p_download,
        response: p_response,
        parser: p_parser,
        error: p_error,
        remote_task: p_remote,
        total: p_task + p_download + p_response + p_parser + p_error + p_remote,
    };
    let inflight = QueueBreakdown {
        task: i_task,
        download: i_download,
        response: i_response,
        parser: i_parser,
        error: i_error,
        remote_task: 0,
        total: i_task + i_download + i_response + i_parser + i_error,
    };
    let outstanding = QueueBreakdown {
        task: p_task + i_task,
        download: p_download + i_download,
        response: p_response + i_response,
        parser: p_parser + i_parser,
        error: p_error + i_error,
        remote_task: p_remote,
        total: pending.total + inflight.total,
    };

    let throughput = ThroughputStats {
        task: StageOutcome::new(&state.outcomes.task),
        request: StageOutcome::new(&state.outcomes.request),
        parse: StageOutcome::new(&state.outcomes.parse),
        store: StageOutcome::new(&state.outcomes.store),
    };

    let cfg = state.state.config.read().await;
    Json(EngineStats {
        namespace: cfg.name.clone(),
        single_node: state.state.coordination.is_none(),
        clustered: state.state.coordination.is_some(),
        pending,
        inflight,
        outstanding,
        throughput,
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
