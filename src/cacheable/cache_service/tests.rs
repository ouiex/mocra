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
    let sync_service = CacheService::new("myapp".to_string(), Some(Duration::from_secs(60)), None);

    let config = MyConfig {
        name: "test".to_string(),
        value: 123,
    };

    config.send("test", &sync_service).await.unwrap();
    let fetched = MyConfig::sync("test", &sync_service)
        .await
        .unwrap()
        .expect("cached config should exist");
    assert_eq!(fetched.name, "test");
    assert_eq!(fetched.value, 123);
}

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
struct LargeConfig {
    value: String,
    cloneable: bool,
}

impl CacheAble for LargeConfig {
    fn field() -> impl AsRef<str> {
        "large_config"
    }

    fn serialized_size_hint(&self) -> Option<usize> {
        Some(self.value.len())
    }

    fn clone_for_serialize(&self) -> Option<Self> {
        self.cloneable.then(|| self.clone())
    }
}

#[tokio::test]
async fn cacheable_write_modes_preserve_values_ttl_and_nx() {
    for cloneable in [false, true] {
        let cache = CacheService::new("write-modes".into(), Some(Duration::ZERO), None);
        let value = LargeConfig {
            value: "x".repeat(70 * 1024),
            cloneable,
        };

        value.send("default", &cache).await.unwrap();
        assert_eq!(LargeConfig::sync("default", &cache).await.unwrap(), None);

        value.send_persistent("persistent", &cache).await.unwrap();
        assert_eq!(
            LargeConfig::sync("persistent", &cache).await.unwrap(),
            Some(value.clone())
        );

        value
            .send_with_ttl("explicit", &cache, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            LargeConfig::sync("explicit", &cache).await.unwrap(),
            Some(value.clone())
        );

        assert!(value.send_nx("nx", &cache, None).await.unwrap());
        let replacement = LargeConfig {
            value: "y".repeat(70 * 1024),
            cloneable,
        };
        assert!(!replacement.send_nx("nx", &cache, None).await.unwrap());
        assert_eq!(LargeConfig::sync("nx", &cache).await.unwrap(), Some(value));
    }
}

#[tokio::test]
async fn local_backend_kv_ttl_and_nx_work() {
    let cache = CacheService::new(
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
async fn local_backend_zset_behaves_as_expected() {
    let cache = CacheService::new(
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
