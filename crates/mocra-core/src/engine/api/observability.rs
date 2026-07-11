//! 可观测端点:集群 / 引擎运行时状态,供后台管理 / 监控页面消费。
//!
//! - `GET /observability/cluster` → [`ClusterStatusView`];仅内嵌 Raft 时有值,否则 `null`。
//! - `GET /observability/engine`  → [`EngineStats`](引擎命名空间、模式、各队列待处理数)。
//!
//! 指标(延迟 / 吞吐 / 错误率)走已有的 `GET /metrics`(Prometheus 文本)。

use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};

use crate::engine::api::state::ApiState;
use crate::engine::monitor::{SystemSnapshot, latest_system_snapshot};
use crate::sync::ClusterStatusView;
use crate::utils::logger::LogRecord;

/// 集群状态:内嵌 Raft 协调时返回快照,否则 `null`。
pub async fn cluster_status(State(state): State<ApiState>) -> Json<Option<ClusterStatusView>> {
    Json(
        state
            .state
            .coordination
            .as_ref()
            .and_then(|c| c.cluster_status()),
    )
}

/// 各队列(task/request/response/parser/error/remote-task)待处理数。
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

/// 引擎 / 队列运行时快照。
#[derive(Debug, Clone, Serialize)]
pub struct EngineStats {
    /// 命名空间(`config.name`)。
    pub namespace: String,
    /// 是否单节点模式。
    pub single_node: bool,
    /// 是否接入集群协调(内嵌 Raft 等)。
    pub clustered: bool,
    /// 本地各队列待处理数。
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

/// 主机资源(CPU / 内存 / swap)最近快照。首个快照在引擎启动后即出。
pub async fn system_stats() -> Json<Option<SystemSnapshot>> {
    Json(latest_system_snapshot())
}

/// 日志查询参数。
#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    /// 返回最近多少条(默认 200,上限 1000)。
    pub limit: Option<usize>,
}

/// 最近日志(最新在前)。供后台管理页面日志面板消费。
pub async fn recent_logs(Query(q): Query<LogsQuery>) -> Json<Vec<LogRecord>> {
    let limit = q.limit.unwrap_or(200).min(1000);
    Json(crate::utils::logger::recent_logs(limit))
}
