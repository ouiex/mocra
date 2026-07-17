//! `CoordinationBackend` over the embedded redb+Raft control plane (refactor Phase 3).
//!
//! Coordination is handled by a self-forming Raft cluster:
//! - **locks / leader election** (`LeaderElector` goes through acquire/renew_lock) / **KV** /
//!   **CAS** — strongly consistent via Raft.
//! - **pub/sub** (used by `SyncService`) — implemented on Raft KV as "monotonic per-topic seq
//!   append + polling", which is correct across nodes and durable (it only relies on
//!   get/cas/set). Control-plane traffic is light, so the polling interval is good enough.

use std::time::Duration;

use async_trait::async_trait;
use mocra_cluster::{ControlPlane, RaftControlPlane};
use tokio::sync::mpsc;

use crate::sync::backend::CoordinationBackend;

/// Adapts the embedded Raft control plane to a [`CoordinationBackend`].
pub struct RaftCoordinationBackend {
    cp: RaftControlPlane,
}

impl RaftCoordinationBackend {
    /// Wrap an already started Raft control plane.
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
        self.cp
            .set(key.as_bytes(), value)
            .await
            .map_err(|e| e.to_string())
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
        // ControlPlane::release_lock only deletes when the holder matches and returns `()`; a
        // success therefore means the lock was released.
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

    fn cluster_status(&self) -> Option<crate::sync::ClusterStatusView> {
        let s = self.cp.status();
        Some(crate::sync::ClusterStatusView {
            node_id: s.node_id,
            is_leader: s.is_leader,
            current_leader: s.current_leader,
            term: s.term,
            last_applied_index: s.last_applied_index,
            member_count: s.member_count,
            voter_count: s.voter_count,
        })
    }

    async fn shutdown(&self) {
        if let Err(e) = self.cp.shutdown().await {
            log::warn!("cluster: raft control plane shutdown error: {e}");
        }
    }

    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String> {
        let seq_key = format!("__psq/{topic}");
        // Atomically bump the per-topic sequence number with cas, then write the message
        // (strongly consistent and durable via Raft).
        loop {
            let cur = self
                .cp
                .get(seq_key.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
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
            // cas failed (a concurrent publish), so retry.
        }
    }

    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        let (tx, rx) = mpsc::channel(1024);
        let cp = self.cp.clone();
        let seq_key = format!("__psq/{topic}");
        let topic = topic.to_string();
        // Start from the current sequence number (only deliver messages published after the
        // subscription).
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
            // Number of consecutive polls where the same seq was missing; past the threshold
            // (~5s) we treat it as genuinely lost rather than replication lag.
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
                    // Only advance `last` once the message has been read: on a follower the
                    // seq (the earlier log entry) can be applied before the message (the later
                    // one), and bumping eagerly here would skip that message forever.
                    let next = last + 1;
                    let msg_key = format!("__psm/{topic}/{next}");
                    match cp.get(msg_key.as_bytes()).await {
                        Ok(Some(p)) => {
                            if tx.send(p).await.is_err() {
                                return; // The subscriber has been dropped.
                            }
                            last = next;
                            stall = 0;
                        }
                        _ => {
                            // The message is not ready yet. Usually this is follower apply lag,
                            // so retry the same seq on the next round; if it stays missing for
                            // a long time (e.g. the leader crashed between publish's two
                            // writes, so the seq is reserved but the message will never be
                            // written), skip it rather than block every later message forever.
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

        // Distributed lock (this is the path LeaderElector takes).
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

        // Cluster semantics (single node): we are the leader, the member count is 1, and every
        // partition key is owned locally.
        assert!(be.is_leader(), "single node must be leader");
        assert_eq!(be.cluster_size(), 1);
        assert!(be.owns_partition_key("any-account"));
        assert!(be.release_lock("L", b"owner1").await.unwrap());
        // Once released, someone else can take it (verifies that release uses native CAS-del).
        assert!(be.acquire_lock("L", b"owner2", 5000).await.unwrap());
    }
}
