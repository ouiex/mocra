use super::backend::CacheBackend;
use super::local_backend::LocalBackend;
use crate::errors::CacheError;
use std::sync::Arc;
use std::time::Duration;

pub struct CacheService {
    pub(crate) backend: Arc<dyn CacheBackend>,
    pub(crate) namespace: String,
    pub(crate) default_ttl: Option<Duration>,
    pub(crate) serialize_blocking_threshold: usize,
}

impl CacheService {
    /// Builds an in-process (local in-memory) cache service.
    ///
    /// The cache backend is always the in-process `LocalBackend`.
    /// The `compression_threshold` parameter is kept for compatibility with existing callers, but
    /// the local backend performs no compression.
    pub fn new(
        namespace: String,
        default_ttl: Option<Duration>,
        compression_threshold: Option<usize>,
    ) -> Self {
        let _ = compression_threshold;
        CacheService {
            backend: Arc::new(LocalBackend::new()),
            namespace,
            default_ttl,
            serialize_blocking_threshold: 64 * 1024,
        }
    }

    pub async fn set_nx(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<bool, CacheError> {
        self.backend.set_nx(key, value, ttl).await
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        self.backend.zadd(key, score, member).await
    }

    pub async fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
    ) -> Result<Vec<Vec<u8>>, CacheError> {
        self.backend.zrangebyscore(key, min, max).await
    }

    pub async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        self.backend.zremrangebyscore(key, min, max).await
    }

    pub async fn set_nx_batch(
        &self,
        keys: &[&str],
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<Vec<bool>, CacheError> {
        self.backend.set_nx_batch(keys, value, ttl).await
    }

    pub async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        self.backend.mget(keys).await
    }

    pub async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        self.backend.incr(key, delta).await
    }

    pub async fn set(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(), CacheError> {
        self.backend.set(key, value, ttl).await
    }

    pub async fn del(&self, key: &str) -> Result<(), CacheError> {
        self.backend.del(key).await
    }

    pub async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError> {
        self.backend.del_batch(keys).await
    }

    pub async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError> {
        self.backend.keys(pattern).await
    }

    pub async fn keys_with_limit(
        &self,
        pattern: &str,
        limit: usize,
    ) -> Result<Vec<String>, CacheError> {
        self.backend.keys_with_limit(pattern, limit).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        self.backend.get(key).await
    }

    pub async fn ping(&self) -> Result<(), CacheError> {
        self.backend.ping().await
    }

    pub fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl
    }
}
