//! End-to-end: [`LeaderElector`], the engine's real coordination consumer, electing a leader on
//! a **multi-node Raft cluster**.
//!
//! This is the closing proof of the control-plane integration — it exercises the whole
//! `LeaderElector → RaftCoordinationBackend → RaftControlPlane → Raft → redb`
//! chain: two nodes each run an elector contending for the same lock, Raft globally serializes
//! `acquire_lock`, so **exactly one** becomes leader; once the original leader stops renewing
//! its lease, the other takes over within the TTL.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use mocra_cluster::{NodeId, RaftControlPlane};
use tokio::time::sleep;

use crate::sync::{CoordinationBackend, LeaderElector, RaftCoordinationBackend};

async fn two_node_cluster(
    a1: &str,
    a2: &str,
    d1: &std::path::Path,
    d2: &std::path::Path,
) -> (RaftControlPlane, RaftControlPlane) {
    let cp1 = RaftControlPlane::start_cluster_node(1, d1, a1)
        .await
        .unwrap();
    let cp2 = RaftControlPlane::start_cluster_node(2, d2, a2)
        .await
        .unwrap();
    // Give the HTTP server time to start.
    sleep(Duration::from_millis(300)).await;

    let mut init = BTreeMap::new();
    init.insert(1u64, a1.to_string());
    cp1.init_cluster(init).await.unwrap();
    cp1.wait_leader(Duration::from_secs(10)).await.unwrap();
    cp1.add_learner(2, a2).await.unwrap();
    let voters: BTreeSet<NodeId> = [1, 2].into_iter().collect();
    cp1.change_membership(voters).await.unwrap();
    (cp1, cp2)
}

/// Cross-node pub/sub: publish on node 1, and a subscriber on node 2 must receive it — the
/// message becomes visible to a local read on every node through the Raft-replicated KV
/// (monotonic per-topic seq + message key), verifying that the `CoordinationBackend` pub/sub
/// contract holds in a cluster (so a user's `SyncService` works across nodes).
#[tokio::test]
async fn raft_backend_pubsub_across_nodes() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let (cp1, cp2) =
        two_node_cluster("127.0.0.1:27841", "127.0.0.1:27842", d1.path(), d2.path()).await;

    let be1: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp1));
    let be2: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp2));

    // Basis for seed deduplication: exactly one node in the cluster reports `is_leader()` (the
    // facade only lets the leader inject seeds).
    let leaders = [be1.is_leader(), be2.is_leader()]
        .iter()
        .filter(|&&b| b)
        .count();
    assert_eq!(
        leaders, 1,
        "exactly one node must report is_leader (seed-dedup basis)"
    );

    // Node 2 subscribes; node 1 publishes (the write is forwarded to the leader and replicated
    // into node 2's local redb).
    let mut rx = be2.subscribe("evt").await.unwrap();
    be1.publish("evt", b"hello").await.unwrap();
    be1.publish("evt", b"world").await.unwrap();

    let m1 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for cross-node message 1")
        .unwrap();
    let m2 = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for cross-node message 2")
        .unwrap();
    assert_eq!(m1, b"hello".to_vec());
    assert_eq!(m2, b"world".to_vec());
}

#[tokio::test]
async fn raft_cluster_elects_single_cron_leader() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let (cp1, cp2) =
        two_node_cluster("127.0.0.1:27811", "127.0.0.1:27812", d1.path(), d2.path()).await;

    // Each node adapts its own control plane into a coordination backend and runs one cron
    // leader elector (contending for the same lock).
    let be1: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp1));
    let be2: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp2));
    let key = "mocra:leader:cron".to_string();
    let (e1, _rx1) = LeaderElector::new(Some(be1), key.clone(), 3000);
    let (e2, _rx2) = LeaderElector::new(Some(be2), key.clone(), 3000);

    let h1 = tokio::spawn(e1.clone().start());
    let h2 = tokio::spawn(e2.clone().start());

    // Election convergence: poll until exactly one node becomes leader (up to ~8s).
    let mut settled = false;
    for _ in 0..80 {
        sleep(Duration::from_millis(100)).await;
        let n = [e1.is_leader(), e2.is_leader()]
            .iter()
            .filter(|&&b| b)
            .count();
        if n == 1 {
            settled = true;
            break;
        }
    }
    assert!(settled, "cluster failed to elect exactly one cron leader");

    // Failover: kill the current leader's lease-renewal task, and the other node should take
    // over once the TTL expires.
    let (leader_handle, follower) = if e1.is_leader() {
        (h1, e2.clone())
    } else {
        (h2, e1.clone())
    };
    leader_handle.abort();

    let mut failover = false;
    for _ in 0..120 {
        sleep(Duration::from_millis(100)).await;
        if follower.is_leader() {
            failover = true;
            break;
        }
    }
    assert!(
        failover,
        "follower did not take over leadership after leader stopped renewing"
    );
    // The remaining elector task is reclaimed by the tokio runtime when the test ends.
}
