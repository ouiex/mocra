//! 端到端:`DistributedLockManager`(现 `mocra-core::utils`)→ `CoordinationBackend` →
//! Raft → redb。证明无 Redis 集群下,锁跨「持有者」互斥且可提前释放(不靠 TTL)。
//!
//! 该测试原在 `utils::redis_lock` 内;`utils` 迁入 `mocra-core` 后它需要 host 侧的
//! `RaftCoordinationBackend`,故移到 `sync` 的测试套件(同时能访问锁管理器与 Raft 后端)。

use std::sync::Arc;
use std::time::Duration;

use mocra_cluster::RaftControlPlane;

use crate::sync::{CoordinationBackend, RaftCoordinationBackend};
use crate::utils::redis_lock::DistributedLockManager;

#[tokio::test]
async fn lock_manager_routes_through_raft_coordination() {
    let dir = tempfile::tempdir().unwrap();
    let cp = RaftControlPlane::start_single_node(1, dir.path())
        .await
        .unwrap();
    cp.wait_leader(Duration::from_secs(10)).await.unwrap();
    let backend: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp));

    // 两个「节点」共享同一后端(单进程内模拟两方争锁)。
    let node_a =
        DistributedLockManager::new_with_coordination(None, Some(backend.clone()), "cluster");
    let node_b =
        DistributedLockManager::new_with_coordination(None, Some(backend.clone()), "cluster");

    // A 抢到锁;B 抢同名锁应失败(强一致互斥,非进程内)。
    assert!(node_a
        .acquire_lock("job", 30, Duration::from_millis(200))
        .await
        .unwrap());
    assert!(!node_b
        .acquire_lock("job", 30, Duration::from_millis(200))
        .await
        .unwrap());
    assert!(node_a.is_lock_valid("job").await.unwrap());

    // A 提前释放(不等 TTL),B 随即可抢到。
    assert!(node_a.release_lock("job").await.unwrap());
    assert!(node_b
        .acquire_lock("job", 30, Duration::from_millis(500))
        .await
        .unwrap());
    assert!(node_b.is_lock_valid("job").await.unwrap());
    assert!(!node_a.is_lock_valid("job").await.unwrap());
}
