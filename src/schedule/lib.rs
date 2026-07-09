//! DAG 引擎已抽为独立 crate [`mocra_dag`]。
//!
//! 此模块保留为 re-export shim(顶层 + `dag` 子模块,兼容既有 `crate::schedule::*`
//! 与 `crate::schedule::dag::*` 路径),并在此提供宿主 adapter:把主 crate 的
//! [`CacheService`] / [`SyncService`] 适配为 dag 的 [`DagStore`] / [`DagEventSink`]。

pub use mocra_dag::*;

/// 兼容旧路径 `crate::schedule::dag::*`。
pub mod dag {
    pub use mocra_dag::*;
}

use std::time::Duration;

use async_trait::async_trait;
use deadpool_redis::redis::Value as RedisValue;

use crate::cacheable::CacheService;
use crate::sync::{SyncAble, SyncService};

/// 把宿主的多级缓存服务适配为 dag 的分布式 KV / 原子后端。
#[async_trait]
impl DagStore for CacheService {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        CacheService::get(self, key).await.map_err(|e| e.to_string())
    }
    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), String> {
        CacheService::set(self, key, value, ttl)
            .await
            .map_err(|e| e.to_string())
    }
    async fn del(&self, key: &str) -> Result<(), String> {
        CacheService::del(self, key).await.map_err(|e| e.to_string())
    }
    async fn incr(&self, key: &str, delta: i64) -> Result<i64, String> {
        CacheService::incr(self, key, delta)
            .await
            .map_err(|e| e.to_string())
    }
    async fn eval_lua(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<RedisValue, String> {
        CacheService::eval_lua(self, script, keys, args)
            .await
            .map_err(|e| e.to_string())
    }
}

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
