use tokio::sync::mpsc;

#[async_trait::async_trait]
pub trait CoordinationBackend: Send + Sync {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String>;
    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String>;
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    // Optimistic Lock (CAS)
    async fn cas(&self, key: &str, old_val: Option<&[u8]>, new_val: &[u8]) -> Result<bool, String>;
    // Distributed Lock
    async fn acquire_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String>;
    async fn renew_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String>;
    /// 释放锁:仅当仍由 `value` 持有时删除(CAS-del)。
    ///
    /// 默认实现依赖 TTL 自然过期(返回 `Ok(false)`,表示未主动释放);
    /// 有原生释放语义的后端(Redis / Raft)应覆写以支持提前释放。
    async fn release_lock(&self, key: &str, value: &[u8]) -> Result<bool, String> {
        let _ = (key, value);
        Ok(false)
    }

    /// 集群当前成员数(用于限流按成员分摊等**近似**分布式语义)。
    ///
    /// 默认 1(单机 / 无法感知成员的后端,如 Redis 协调)。感知成员的后端
    /// (内嵌 Raft)覆写为真实成员数。
    fn cluster_size(&self) -> usize {
        1
    }

    /// 给定分区键(账号 / 会话键)是否归**本节点**处理。
    ///
    /// 用于把采集工作按分区分摊到集群节点:每个账号只由其归属节点处理,
    /// 避免重复抓取、保持会话粘性。默认 `true`(单机 / 无分区能力的后端,全归本地);
    /// 感知成员的后端(内嵌 Raft)按 rendezvous 归属覆写。
    fn owns_partition_key(&self, key: &str) -> bool {
        let _ = key;
        true
    }

    /// 本节点当前是否是集群 leader。
    ///
    /// 用于「集群里只由一个节点做一次」的动作(如注入种子任务,避免每节点重复注入
    /// 导致 N× 重复抓取)。默认 `true`(单机 / 无 leader 概念的后端);内嵌 Raft
    /// 覆写为「本节点 == 当前 Raft leader」。
    fn is_leader(&self) -> bool {
        true
    }

    /// 优雅关闭本后端(引擎停机时调用):释放持有的资源(如内嵌 Raft 的 redb 句柄
    /// 与后台任务)。默认无操作(无状态后端如 Redis / Kafka 无需特殊处理)。
    async fn shutdown(&self) {}
}
