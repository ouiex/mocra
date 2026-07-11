//! dag 运行时依赖的抽象 trait —— 由宿主 crate 注入实现,使 mocra-dag 不反依赖宿主。

use async_trait::async_trait;

use crate::types::DagNodeSyncState;

/// 分布式 KV / 原子操作后端(宿主用嵌入式集群 KV 实现:`get`/`del`/`incr`)。
///
/// 语义:`get` 取字节、`set` 写入(可选 TTL)、`del` 删键、`incr` 原子自增。
/// 错误统一为 `String`(dag 侧多为 best-effort,忽略或降级处理)。
#[async_trait]
pub trait DagStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    async fn set(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<std::time::Duration>,
    ) -> Result<(), String>;
    async fn del(&self, key: &str) -> Result<(), String>;
    async fn incr(&self, key: &str, delta: i64) -> Result<i64, String>;
}

/// DAG 节点状态事件出口(宿主用分布式 pub/sub 实现)。可选:未注入则不发事件。
#[async_trait]
pub trait DagEventSink: Send + Sync {
    async fn emit(&self, event: &DagNodeSyncState) -> Result<(), String>;
}
