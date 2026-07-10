//! 端到端:引擎真正的协调消费者 [`LeaderElector`] 在**多节点 Raft 集群**上选主。
//!
//! 这是控制面集成的收口证明 —— 打通
//! `LeaderElector → RaftCoordinationBackend → RaftControlPlane → Raft → redb`
//! 全链路:两个节点各跑一个 elector 抢同一把锁,Raft 全局串行化 `acquire_lock`,
//! 故**恰好一个**成为 leader;原 leader 停止续租后,另一个在 TTL 内接管。

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
    // 给 HTTP server 起动时间。
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

/// 跨节点 pub/sub:在节点 1 publish,节点 2 订阅应收到 —— 消息经 Raft 复制的 KV
/// (每 topic 单调 seq + 消息键)在各节点本地读可见,验证 `CoordinationBackend`
/// 的 pub/sub 契约在集群里成立(用户的 `SyncService` 跨节点可用)。
#[tokio::test]
async fn raft_backend_pubsub_across_nodes() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let (cp1, cp2) =
        two_node_cluster("127.0.0.1:27841", "127.0.0.1:27842", d1.path(), d2.path()).await;

    let be1: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp1));
    let be2: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp2));

    // 种子去重基础:集群里恰好一个节点 `is_leader()`(facade 只让 leader 注入种子)。
    let leaders = [be1.is_leader(), be2.is_leader()]
        .iter()
        .filter(|&&b| b)
        .count();
    assert_eq!(
        leaders, 1,
        "exactly one node must report is_leader (seed-dedup basis)"
    );

    // 节点 2 订阅;节点 1 发布(写经转发到 leader,复制到节点 2 本地 redb)。
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

    // 两个节点各把自己的控制面适配为协调后端,各跑一个 cron leader elector(同一把锁)。
    let be1: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp1));
    let be2: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp2));
    let key = "mocra:leader:cron".to_string();
    let (e1, _rx1) = LeaderElector::new(Some(be1), key.clone(), 3000);
    let (e2, _rx2) = LeaderElector::new(Some(be2), key.clone(), 3000);

    let h1 = tokio::spawn(e1.clone().start());
    let h2 = tokio::spawn(e2.clone().start());

    // 选举收敛:轮询直到恰好一个成为 leader(最多 ~8s)。
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

    // 失效接管:掐掉当前 leader 的续租任务,另一节点应在 TTL 过期后接管。
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
    // 剩余 elector 任务由 tokio runtime 在测试结束时回收。
}
