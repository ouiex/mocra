use super::{CacheAble, CacheService};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize, Debug)]
struct MyConfig {
    name: String,
    value: i32,
}

impl CacheAble for MyConfig {
    fn field() -> impl AsRef<str> {
        "global_config".to_string()
    }
}

#[tokio::test]
async fn cacheable_send_and_sync_roundtrip() {
    let sync_service = CacheService::new(
        None,
        "myapp".to_string(),
        Some(Duration::from_secs(60)),
        None,
    );

    let config = MyConfig {
        name: "test".to_string(),
        value: 123,
    };

    if let Err(e) = config.send(&"test", &sync_service).await {
        eprintln!("Failed to send: {}", e);
    }

    match MyConfig::sync(&"test", &sync_service).await {
        Ok(Some(fetched)) => {
            assert_eq!(fetched.name, "test");
            assert_eq!(fetched.value, 123);
        }
        Ok(None) => panic!("Expected cached data but got None"),
        Err(e) => eprintln!("Error syncing: {}", e),
    }
}

#[tokio::test]
async fn local_backend_kv_ttl_and_nx_work() {
    let cache = CacheService::new(
        None,
        "single-node".to_string(),
        Some(Duration::from_secs(60)),
        None,
    );

    cache
        .set("k1", b"v1", Some(Duration::from_millis(30)))
        .await
        .expect("set should succeed");
    let immediate = cache.get("k1").await.expect("get should succeed");
    assert_eq!(immediate, Some(b"v1".to_vec()));

    tokio::time::sleep(Duration::from_millis(50)).await;
    let expired = cache.get("k1").await.expect("get should succeed after ttl");
    assert_eq!(expired, None);

    let first = cache
        .set_nx("nx-key", b"one", Some(Duration::from_secs(1)))
        .await
        .expect("set_nx should succeed");
    let second = cache
        .set_nx("nx-key", b"two", Some(Duration::from_secs(1)))
        .await
        .expect("set_nx should succeed");
    assert!(first);
    assert!(!second);
}

#[tokio::test]
async fn local_backend_zset_interfaces_behave_as_expected() {
    let cache = CacheService::new(
        None,
        "single-node".to_string(),
        Some(Duration::from_secs(60)),
        None,
    );

    cache
        .zadd("z:key", 2.0, b"m2")
        .await
        .expect("zadd should succeed");
    cache
        .zadd("z:key", 1.0, b"m1")
        .await
        .expect("zadd should succeed");
    cache
        .zadd("z:key", 3.0, b"m3")
        .await
        .expect("zadd should succeed");

    let members = cache
        .zrangebyscore("z:key", 1.0, 2.0)
        .await
        .expect("zrangebyscore should succeed");
    assert_eq!(members, vec![b"m1".to_vec(), b"m2".to_vec()]);

}
