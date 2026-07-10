//! DAG 引擎已抽为独立 crate [`mocra_dag`]。
//!
//! 此模块保留为 re-export shim(顶层 + `dag` 子模块,兼容既有 `crate::schedule::*`
//! 与 `crate::schedule::dag::*` 路径),并在此提供宿主 adapter:把主 crate 的
//! [`CacheService`](crate::cacheable::CacheService) / [`SyncService`] 适配为 dag 的
//! [`DagStore`] / [`DagEventSink`]。

pub use mocra_dag::*;

/// 兼容旧路径 `crate::schedule::dag::*`。
pub mod dag {
    pub use mocra_dag::*;
}

use async_trait::async_trait;

use crate::sync::{SyncAble, SyncService};

// `impl DagStore for CacheService` 已随 `CacheService` 迁入 `mocra-core`(孤儿规则:
// trait 与类型均在外部 crate,impl 必须在拥有其一的 crate 里)。

// SyncAble 是本地 trait、DagNodeSyncState 是外部类型 —— 孤儿规则允许在此 impl。
impl SyncAble for DagNodeSyncState {
    fn topic() -> String {
        "dag_node_sync_state".to_string()
    }
}

/// 把宿主的分布式同步服务适配为 dag 的节点状态事件出口。
pub struct DagSyncSink(pub SyncService);

#[async_trait]
impl DagEventSink for DagSyncSink {
    async fn emit(&self, event: &DagNodeSyncState) -> Result<(), String> {
        self.0.send(event).await
    }
}

#[cfg(test)]
mod dag_tests;
