use crate::cacheable::cache_service::backend::CacheBackend;
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use crate::errors::error::CacheError;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Raft/RocksDB-backed cache implementation.
///
/// All writes go through `ProfileControlPlaneStore::submit_command()` (Raft consensus
/// when a Raft runtime is active, local apply otherwise). Reads go directly to the
/// local RocksDB read model.
pub struct RaftRocksDbCacheBackend {
    store: Arc<ProfileControlPlaneStore>,
    namespace: String,
    default_ttl: Option<Duration>,
}

impl RaftRocksDbCacheBackend {
    pub fn new(
        store: Arc<ProfileControlPlaneStore>,
        namespace: impl Into<String>,
        default_ttl: Option<Duration>,
    ) -> Self {
        Self {
            store,
            namespace: namespace.into(),
            default_ttl,
        }
    }

    fn resolve_ttl(&self, ttl: Option<Duration>) -> Option<u64> {
        ttl.or(self.default_ttl)
            .map(|d| d.as_millis() as u64)
    }
}

#[async_trait]
impl CacheBackend for RaftRocksDbCacheBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(self.store.read_cache_value(&self.namespace, key))
    }

    async fn set(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        self.store
            .submit_cache_set(&self.namespace, key, value.to_vec(), self.resolve_ttl(ttl))
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))
    }

    async fn del(&self, key: &str) -> Result<(), CacheError> {
        self.store
            .submit_cache_del(&self.namespace, key)
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))
    }

    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        self.store
            .submit_cache_del_batch(&self.namespace, keys)
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))
    }

    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        self.keys_with_limit(pattern, usize::MAX).await
    }

    async fn keys_with_limit(
        &self,
        pattern: &str,
        limit: usize,
    ) -> Result<Vec<String>, CacheError> {
        Ok(self
            .store
            .list_cache_keys(&self.namespace, pattern, limit))
    }

    async fn set_nx(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<bool, CacheError> {
        self.store
            .submit_cache_set_nx(&self.namespace, key, value.to_vec(), self.resolve_ttl(ttl))
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))
    }

    async fn set_nx_batch(
        &self,
        keys: &[&str],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<Vec<bool>, CacheError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.set_nx(key, value, ttl).await?);
        }
        Ok(results)
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.store.read_cache_value(&self.namespace, key));
        }
        Ok(results)
    }

    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        self.store
            .submit_cache_incr_by(&self.namespace, key, delta, self.resolve_ttl(None))
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))
    }

    async fn ping(&self) -> Result<(), CacheError> {
        Ok(())
    }

    async fn zadd(
        &self,
        key: &str,
        score: f64,
        member: &[u8],
    ) -> Result<i64, CacheError> {
        self.store
            .submit_cache_zadd(&self.namespace, key, score, member.to_vec())
            .await
            .map(|_| 1)
            .map_err(|e| CacheError::Pool(e.to_string()))
    }

    async fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<Vec<u8>>, CacheError> {
        Ok(self.store.read_cache_zrange_by_score(&self.namespace, key, min, max, None))
    }

    async fn zremrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<i64, CacheError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let outcome_ns = self.namespace.clone();
        self.store
            .submit_command(
                crate::common::model::control_plane_profile::ControlPlaneRaftCommand::CacheZRemRangeByScore {
                    namespace: self.namespace.clone(),
                    zset_key: key.to_string(),
                    min,
                    max,
                    request_id: request_id.clone(),
                },
            )
            .await
            .map_err(|e| CacheError::Pool(e.to_string()))?;
        // Read outcome to get the real count
        let count = self
            .store
            .read_command_outcome(&outcome_ns, &request_id)
            .map(|(_, v)| v)
            .unwrap_or(0);
        Ok(count)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use std::time::Duration;

    fn test_backend() -> RaftRocksDbCacheBackend {
        let store = Arc::new(ProfileControlPlaneStore::default());
        RaftRocksDbCacheBackend::new(store, "test-ns", Some(Duration::from_secs(3600)))
    }

    #[tokio::test]
    async fn raft_backend_kv_set_get() {
        let backend = test_backend();
        backend.set("k1", b"hello", None).await.unwrap();
        let val = backend.get("k1").await.unwrap();
        assert_eq!(val, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn raft_backend_kv_ttl_expires() {
        let backend = RaftRocksDbCacheBackend::new(
            Arc::new(ProfileControlPlaneStore::default()),
            "test-ttl",
            None,
        );
        backend
            .set("k2", b"data", Some(Duration::from_millis(1)))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let val = backend.get("k2").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test]
    async fn raft_backend_set_nx() {
        let backend = test_backend();
        let ok = backend.set_nx("nx1", b"first", None).await.unwrap();
        assert!(ok);
        let not_ok = backend.set_nx("nx1", b"second", None).await.unwrap();
        assert!(!not_ok);
        assert_eq!(backend.get("nx1").await.unwrap(), Some(b"first".to_vec()));
    }

    #[tokio::test]
    async fn raft_backend_set_nx_batch() {
        let backend = test_backend();
        let results = backend
            .set_nx_batch(&["a", "b", "c"], b"val", None)
            .await
            .unwrap();
        assert_eq!(results, vec![true, true, true]);
        // Second call: all should fail
        let results2 = backend
            .set_nx_batch(&["a", "b", "c"], b"val2", None)
            .await
            .unwrap();
        assert_eq!(results2, vec![false, false, false]);
    }

    #[tokio::test]
    async fn raft_backend_mget() {
        let backend = test_backend();
        backend.set("a", b"1", None).await.unwrap();
        backend.set("b", b"2", None).await.unwrap();
        let vals = backend.mget(&["a", "missing", "b"]).await.unwrap();
        assert_eq!(vals, vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]);
    }

    #[tokio::test]
    async fn raft_backend_incr() {
        let backend = test_backend();
        let v = backend.incr("counter", 5).await.unwrap();
        assert_eq!(v, 5);
        let v = backend.incr("counter", 3).await.unwrap();
        assert_eq!(v, 8);
    }

    #[tokio::test]
    async fn raft_backend_zset_add_and_range() {
        let backend = test_backend();
        backend.zadd("zs", 1.0, b"a").await.unwrap();
        backend.zadd("zs", 2.0, b"b").await.unwrap();
        backend.zadd("zs", 3.0, b"c").await.unwrap();
        let members = backend.zrangebyscore("zs", 1.0, 2.0).await.unwrap();
        assert_eq!(members, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[tokio::test]
    async fn raft_backend_zset_remrangebyscore() {
        let backend = test_backend();
        backend.zadd("zs2", 1.0, b"low").await.unwrap();
        backend.zadd("zs2", 5.0, b"mid").await.unwrap();
        backend.zadd("zs2", 10.0, b"high").await.unwrap();
        backend.zremrangebyscore("zs2", 1.0, 5.0).await.unwrap();
        let remaining = backend.zrangebyscore("zs2", 0.0, 100.0).await.unwrap();
        assert_eq!(remaining, vec![b"high".to_vec()]);
    }

    #[tokio::test]
    async fn raft_backend_keys_prefix() {
        let backend = test_backend();
        backend.set("prefix:a", b"1", None).await.unwrap();
        backend.set("prefix:b", b"2", None).await.unwrap();
        backend.set("other", b"3", None).await.unwrap();
        let keys = backend.keys_with_limit("prefix:*", 10).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"prefix:a".to_string()));
        assert!(keys.contains(&"prefix:b".to_string()));
    }

    #[tokio::test]
    async fn raft_backend_del_and_del_batch() {
        let backend = test_backend();
        backend.set("d1", b"1", None).await.unwrap();
        backend.set("d2", b"2", None).await.unwrap();
        backend.set("d3", b"3", None).await.unwrap();
        backend.del("d1").await.unwrap();
        assert_eq!(backend.get("d1").await.unwrap(), None);
        let count = backend.del_batch(&["d2", "d3"]).await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(backend.get("d2").await.unwrap(), None);
        assert_eq!(backend.get("d3").await.unwrap(), None);
    }

    #[tokio::test]
    async fn raft_backend_default_ttl_applied() {
        let backend = RaftRocksDbCacheBackend::new(
            Arc::new(ProfileControlPlaneStore::default()),
            "ns-ttl",
            Some(Duration::from_millis(1)),
        );
        backend.set("k", b"val", None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(backend.get("k").await.unwrap(), None);
    }
}
