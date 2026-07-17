//! End-to-end: `DistributedLockManager` (now `mocra-core::utils`) → `CoordinationBackend` →
//! Raft → redb. Proves that in a cluster, locks are mutually exclusive across "holders" and
//! can be released early (without waiting for the TTL).
//!
//! This test used to live in `utils::lock`; once `utils` moved into `mocra-core` it needed the
//! host-side `RaftCoordinationBackend`, so it was moved to the `sync` test suite (which can
//! reach both the lock manager and the Raft backend).

use std::sync::Arc;
use std::time::Duration;

use mocra_cluster::RaftControlPlane;

use crate::sync::{CoordinationBackend, RaftCoordinationBackend};
use crate::utils::lock::DistributedLockManager;

#[tokio::test]
async fn lock_manager_routes_through_raft_coordination() {
    let dir = tempfile::tempdir().unwrap();
    let cp = RaftControlPlane::start_single_node(1, dir.path())
        .await
        .unwrap();
    cp.wait_leader(Duration::from_secs(10)).await.unwrap();
    let backend: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp));

    // Two "nodes" sharing one backend (simulating two contenders inside a single process).
    let node_a = DistributedLockManager::new_with_coordination(Some(backend.clone()), "cluster");
    let node_b = DistributedLockManager::new_with_coordination(Some(backend.clone()), "cluster");

    // A takes the lock; B must fail to take the same one (strongly consistent mutual
    // exclusion, not merely in-process).
    assert!(
        node_a
            .acquire_lock("job", 30, Duration::from_millis(200))
            .await
            .unwrap()
    );
    assert!(
        !node_b
            .acquire_lock("job", 30, Duration::from_millis(200))
            .await
            .unwrap()
    );
    assert!(node_a.is_lock_valid("job").await.unwrap());

    // A releases early (without waiting for the TTL), and B can take it right away.
    assert!(node_a.release_lock("job").await.unwrap());
    assert!(
        node_b
            .acquire_lock("job", 30, Duration::from_millis(500))
            .await
            .unwrap()
    );
    assert!(node_b.is_lock_valid("job").await.unwrap());
    assert!(!node_a.is_lock_valid("job").await.unwrap());
}
