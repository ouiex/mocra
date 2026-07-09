//! `CoordinationBackend` over the embedded redb+Raft control plane(重构 Phase 3)。
//!
//! 用自组网的 Raft 集群替换 Redis 协调:
//! - **锁 / 选举**(`LeaderElector` 走 acquire/renew_lock)/ **KV** / **CAS** —— Raft 强一致。
//! - **pub/sub**(`SyncService` 用)—— 用 Raft KV 的「每 topic 单调 seq append + 轮询」实现,
//!   跨节点正确且持久(仅依赖 get/cas/set)。控制面流量小,轮询间隔足够。

use std::time::Duration;

use async_trait::async_trait;
use mocra_cluster::{ControlPlane, RaftControlPlane};
use tokio::sync::mpsc;

use crate::sync::backend::CoordinationBackend;

/// 把内嵌 Raft 控制面适配为 [`CoordinationBackend`]。
pub struct RaftCoordinationBackend {
    cp: RaftControlPlane,
}

impl RaftCoordinationBackend {
    /// 包装一个已启动的 Raft 控制面。
    pub fn new(cp: RaftControlPlane) -> Self {
        Self { cp }
    }
}

fn u64_from(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    let n = b.len().min(8);
    a[..n].copy_from_slice(&b[..n]);
    u64::from_be_bytes(a)
}

#[async_trait]
impl CoordinationBackend for RaftCoordinationBackend {
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.cp.set(key.as_bytes(), value).await.map_err(|e| e.to_string())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.cp.get(key.as_bytes()).await.map_err(|e| e.to_string())
    }

    async fn cas(&self, key: &str, old_val: Option<&[u8]>, new_val: &[u8]) -> Result<bool, String> {
        self.cp
            .cas(key.as_bytes(), old_val, new_val)
            .await
            .map_err(|e| e.to_string())
    }

    async fn acquire_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        let holder = String::from_utf8_lossy(value);
        let token = self
            .cp
            .acquire_lock(key, holder.as_ref(), ttl_ms)
            .await
            .map_err(|e| e.to_string())?;
        Ok(token.is_some())
    }

    async fn renew_lock(&self, key: &str, value: &[u8], ttl_ms: u64) -> Result<bool, String> {
        let holder = String::from_utf8_lossy(value);
        self.cp
            .renew_lock(key, holder.as_ref(), ttl_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn release_lock(&self, key: &str, value: &[u8]) -> Result<bool, String> {
        let holder = String::from_utf8_lossy(value);
        // ControlPlane::release_lock 仅当持有者匹配时删除,返回 `()`;成功即视为已释放。
        self.cp
            .release_lock(key, holder.as_ref())
            .await
            .map(|_| true)
            .map_err(|e| e.to_string())
    }

    fn cluster_size(&self) -> usize {
        self.cp.member_count()
    }

    fn owns_partition_key(&self, key: &str) -> bool {
        self.cp.owns_key(key)
    }

    fn is_leader(&self) -> bool {
        self.cp.current_leader() == Some(self.cp.node_id())
    }

    async fn shutdown(&self) {
        if let Err(e) = self.cp.shutdown().await {
            log::warn!("cluster: raft control plane shutdown error: {e}");
        }
    }

    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String> {
        let seq_key = format!("__psq/{topic}");
        // 对每 topic 序号做 cas 原子自增,再写消息(Raft 强一致 + 持久)。
        loop {
            let cur = self.cp.get(seq_key.as_bytes()).await.map_err(|e| e.to_string())?;
            let next = cur.as_deref().map(u64_from).unwrap_or(0) + 1;
            let ok = self
                .cp
                .cas(seq_key.as_bytes(), cur.as_deref(), &next.to_be_bytes())
                .await
                .map_err(|e| e.to_string())?;
            if ok {
                let msg_key = format!("__psm/{topic}/{next}");
                self.cp
                    .set(msg_key.as_bytes(), payload)
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
            // cas 失败(并发发布)→ 重试。
        }
    }

    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        let (tx, rx) = mpsc::channel(1024);
        let cp = self.cp.clone();
        let seq_key = format!("__psq/{topic}");
        let topic = topic.to_string();
        // 从当前序号起(只投递订阅之后的新消息)。
        let start = cp
            .get(seq_key.as_bytes())
            .await
            .ok()
            .flatten()
            .as_deref()
            .map(u64_from)
            .unwrap_or(0);
        tokio::spawn(async move {
            let _ = &topic;
            let mut last = start;
            // 同一 seq 连续缺失的轮询次数;超过阈值(~5s)判为「真丢失」而非复制滞后。
            let mut stall = 0u32;
            const STALL_SKIP: u32 = 25; // 25 * 200ms ≈ 5s
            loop {
                let cur = cp
                    .get(seq_key.as_bytes())
                    .await
                    .ok()
                    .flatten()
                    .as_deref()
                    .map(u64_from)
                    .unwrap_or(last);
                while last < cur {
                    // 只在成功读到消息后推进 `last`:follower 上 seq(前一条日志)可能
                    // 先于消息(后一条日志)被 apply,若此时贸然自增会永久跳过该消息。
                    let next = last + 1;
                    let msg_key = format!("__psm/{topic}/{next}");
                    match cp.get(msg_key.as_bytes()).await {
                        Ok(Some(p)) => {
                            if tx.send(p).await.is_err() {
                                return; // 订阅端已释放。
                            }
                            last = next;
                            stall = 0;
                        }
                        _ => {
                            // 消息未就绪。通常是 follower apply 滞后 → 下轮重试同一 seq;
                            // 若长时间仍缺失(如 publish 两次写之间 leader 崩溃,seq 已占位
                            // 但消息永远不会写入),跳过以免永久阻塞后续消息。
                            stall += 1;
                            if stall >= STALL_SKIP {
                                last = next;
                                stall = 0;
                                continue;
                            }
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mocra_cluster::RaftControlPlane;

    #[tokio::test]
    async fn raft_backend_covers_coordination() {
        let dir = tempfile::tempdir().unwrap();
        let cp = RaftControlPlane::start_single_node(1, dir.path())
            .await
            .unwrap();
        cp.wait_leader(Duration::from_secs(10)).await.unwrap();
        let be = RaftCoordinationBackend::new(cp);

        // KV + CAS。
        be.set("k", b"v").await.unwrap();
        assert_eq!(be.get("k").await.unwrap(), Some(b"v".to_vec()));
        assert!(be.cas("k", Some(b"v"), b"v2").await.unwrap());
        assert!(!be.cas("k", Some(b"v"), b"v3").await.unwrap());
        assert_eq!(be.get("k").await.unwrap(), Some(b"v2".to_vec()));

        // 分布式锁(LeaderElector 走这条)。
        assert!(be.acquire_lock("L", b"owner1", 5000).await.unwrap());
        assert!(!be.acquire_lock("L", b"owner2", 5000).await.unwrap());
        assert!(be.renew_lock("L", b"owner1", 5000).await.unwrap());
        assert!(!be.renew_lock("L", b"owner2", 5000).await.unwrap());

        // pub/sub。
        let mut rx = be.subscribe("topic").await.unwrap();
        be.publish("topic", b"m1").await.unwrap();
        be.publish("topic", b"m2").await.unwrap();
        let m1 = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let m2 = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m1, b"m1".to_vec());
        assert_eq!(m2, b"m2".to_vec());

        // 集群语义(单节点):自己是 leader、成员数 1、任何分区键都归本地。
        assert!(be.is_leader(), "single node must be leader");
        assert_eq!(be.cluster_size(), 1);
        assert!(be.owns_partition_key("any-account"));
        assert!(be.release_lock("L", b"owner1").await.unwrap());
        // 释放后可被他人抢到(验证 release 走原生 CAS-del)。
        assert!(be.acquire_lock("L", b"owner2", 5000).await.unwrap());
    }
}

