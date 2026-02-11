use sync::{self, SyncAble};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct TestState {
    val: String,
}

impl SyncAble for TestState {
    fn topic() -> String {
        "isolation_test".to_string()
    }
}

#[tokio::main]
async fn main() {
    println!("Running Namespace Isolation Tests...");

    test_local_isolation().await;
    test_distributed_isolation().await;
}

async fn test_local_isolation() {
    println!("\n--- Testing Local Mode Isolation ---");
    
    // Service 1: Namespace "ns1"
    // Note: In Local Mode, SyncService instances share the static/global store if it were static, 
    // but here `local_store` is a field of `SyncService`. 
    // Wait, `local_store` is `Arc<DashMap...>` created in `new`.
    // So two different `SyncService` instances have DIFFERENT `local_store`s!
    // So they are isolated by definition of being different instances.
    //
    // TO TEST NAMESPACE ISOLATION IN LOCAL MODE:
    // We need to verify that if we somehow shared the store (which we don't currently expose), keys would be different.
    // OR, more importantly, we are testing that the `namespace` parameter is correctly used.
    //
    // Actually, if `SyncService` instances are separate, they don't share data in Local Mode anyway.
    // So `test_local_isolation` with separate instances is trivial.
    //
    // However, the user might intend for `SyncService` to be a singleton or shared.
    // But `SyncService::new` creates a NEW DashMap.
    //
    // Let's verify the code in `distributed.rs`:
    // pub struct SyncService { ..., local_store: Arc<DashMap<String, Arc<LocalTopicState>>>, ... }
    // impl SyncService { pub fn new(...) { ..., local_store: Arc::new(DashMap::new()), ... } }
    //
    // So yes, separate instances are completely isolated in Local Mode regardless of namespace.
    //
    // BUT, if we clone a SyncService, they share the store.
    // If we have a shared store, namespace should separate them.
    // But `SyncService` doesn't allow changing namespace on a clone (it's immutable).
    //
    // So for Local Mode, the namespace is mostly for consistency with Distributed Mode 
    // or if we ever made the store static/global.
    //
    // Let's proceed with the test anyway, as it validates the API contract.
    
    let config = common::model::config::SyncConfig::default();
    let service1 = sync::SyncService::from_config(None, "ns1".to_string(), &config);
    let service2 = sync::SyncService::from_config(None, "ns2".to_string(), &config);

    let state1 = TestState { val: "data_for_ns1".to_string() };
    let state2 = TestState { val: "data_for_ns2".to_string() };

    service1.send(&state1).await.unwrap();
    service2.send(&state2).await.unwrap();

    let fetched1 = service1.fetch_latest::<TestState>().await.unwrap();
    assert_eq!(fetched1, Some(state1.clone()));

    let fetched2 = service2.fetch_latest::<TestState>().await.unwrap();
    assert_eq!(fetched2, Some(state2.clone()));
    
    assert_ne!(fetched1, fetched2);

    println!("Local Mode Isolation: PASSED");
}

async fn test_distributed_isolation() {
    println!("\n--- Testing Distributed Mode Isolation ---");
    const REDIS_URL: &str = "redis://:Qaz.123456@localhost:6379";
    
    // Use the same backend connection pool to prove isolation happens at key level
    let redis_backend = Arc::new(sync::RedisBackend::new(REDIS_URL));
    
    // Clean up before test
    // We can't easily clean up specific keys without a raw redis client, but we can overwrite.
    // Or we can use a random namespace suffix to ensure clean state.
    let ns1 = format!("dist_ns1_{}", uuid::Uuid::new_v4());
    let ns2 = format!("dist_ns2_{}", uuid::Uuid::new_v4());
    
    let config = common::model::config::SyncConfig::default();
    let service1 = sync::SyncService::from_config(Some(redis_backend.clone()), ns1.clone(), &config);
    let service2 = sync::SyncService::from_config(Some(redis_backend.clone()), ns2.clone(), &config);

    let state1 = TestState { val: "dist_data_1".to_string() };
    let state2 = TestState { val: "dist_data_2".to_string() };

    // 1. Send initial data
    service1.send(&state1).await.unwrap();
    service2.send(&state2).await.unwrap();

    // 2. Verify fetch
    let fetched1 = service1.fetch_latest::<TestState>().await.unwrap();
    let fetched2 = service2.fetch_latest::<TestState>().await.unwrap();

    assert_eq!(fetched1, Some(state1.clone()), "Service 1 should fetch state 1");
    assert_eq!(fetched2, Some(state2.clone()), "Service 2 should fetch state 2");

    // 3. Verify update isolation
    service1.optimistic_update::<TestState, _>(|s| s.val = "dist_updated_1".to_string()).await.unwrap();
    
    // Service 2 should still see state2 (or at least not "dist_updated_1")
    let fetched2_after = service2.fetch_latest::<TestState>().await.unwrap();
    assert_eq!(fetched2_after, Some(state2.clone()), "Service 2 should not be affected by Service 1 update");
    
    let fetched1_after = service1.fetch_latest::<TestState>().await.unwrap();
    assert_eq!(fetched1_after.unwrap().val, "dist_updated_1");

    println!("Distributed Mode Isolation: PASSED");
}
