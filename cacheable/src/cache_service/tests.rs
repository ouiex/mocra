use super::{CacheAble, CacheService};
use deadpool_redis::Pool;
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

fn redis_test_pool() -> Option<Pool> {
    let url = env::var("MOCRA_REDIS_TEST_URL")
        .ok()
        .or_else(|| env::var("REDIS_URL").ok())?;
    let cfg = deadpool_redis::Config::from_url(url);
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)).ok()
}

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
async fn local_backend_zset_and_script_interfaces_behave_as_expected() {
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

    let script_err = cache
        .script_load("return {0, 'ok', '{}'}")
        .await
        .expect_err("local backend should not support lua script loading");
    assert!(
        script_err
            .to_string()
            .contains("Lua scripts are only supported in distributed Redis mode")
    );
}

#[tokio::test]
async fn redis_and_single_node_set_get_behave_consistently() {
    let Some(pool) = redis_test_pool() else {
        eprintln!("skip redis consistency test: REDIS_URL/MOCRA_REDIS_TEST_URL not set");
        return;
    };

    let local_cache = CacheService::new(
        None,
        "single-node".to_string(),
        Some(Duration::from_secs(60)),
        None,
    );
    let redis_cache = CacheService::new(
        Some(pool),
        "distributed".to_string(),
        Some(Duration::from_secs(60)),
        None,
    );

    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let key = format!("t08:consistency:{}", now_ns);

    local_cache
        .set(&key, b"value-a", Some(Duration::from_secs(60)))
        .await
        .expect("local set should succeed");
    redis_cache
        .set(&key, b"value-a", Some(Duration::from_secs(60)))
        .await
        .expect("redis set should succeed");

    let local_value = local_cache.get(&key).await.expect("local get should succeed");
    let redis_value = redis_cache.get(&key).await.expect("redis get should succeed");
    assert_eq!(local_value, redis_value);

    local_cache
        .set(&key, b"value-b", Some(Duration::from_secs(60)))
        .await
        .expect("local overwrite should succeed");
    redis_cache
        .set(&key, b"value-b", Some(Duration::from_secs(60)))
        .await
        .expect("redis overwrite should succeed");

    let local_updated = local_cache
        .get(&key)
        .await
        .expect("local updated get should succeed");
    let redis_updated = redis_cache
        .get(&key)
        .await
        .expect("redis updated get should succeed");
    assert_eq!(local_updated, redis_updated);
    assert_eq!(local_updated, Some(b"value-b".to_vec()));

    redis_cache
        .del(&key)
        .await
        .expect("redis cleanup should succeed");
}
