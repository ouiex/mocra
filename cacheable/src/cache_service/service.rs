use super::backend::CacheBackend;
use super::local_backend::LocalBackend;
use super::redis_backend::RedisBackend;
use super::two_level_backend::TwoLevelCacheBackend;
use deadpool_redis::redis::Value as RedisValue;
use deadpool_redis::Pool;
use errors::CacheError;
use std::sync::Arc;
use std::time::Duration;

pub struct CacheService {
    pub(crate) backend: Arc<dyn CacheBackend>,
    pub(crate) namespace: String,
    pub(crate) default_ttl: Option<Duration>,
    pub(crate) serialize_blocking_threshold: usize,
}

impl CacheService {
    pub fn new(
        pool: Option<Pool>,
        namespace: String,
        default_ttl: Option<Duration>,
        compression_threshold: Option<usize>,
    ) -> Self {
        Self::new_with_l1_config(pool, namespace, default_ttl, compression_threshold, false, 30, 10000)
    }

    pub fn new_with_l1_config(
        pool: Option<Pool>,
        namespace: String,
        default_ttl: Option<Duration>,
        compression_threshold: Option<usize>,
        enable_l1: bool,
        l1_ttl_secs: u64,
        l1_max_entries: usize,
    ) -> Self {
        let threshold = compression_threshold.unwrap_or(1024);

        let backend: Arc<dyn CacheBackend> = match pool {
            Some(p) => {
                if enable_l1 {
                    Arc::new(TwoLevelCacheBackend::new(p, threshold, l1_ttl_secs, l1_max_entries))
                } else {
                    Arc::new(RedisBackend::new(p, threshold))
                }
            }
            None => Arc::new(LocalBackend::new()),
        };

        CacheService {
            backend,
            namespace,
            default_ttl,
            serialize_blocking_threshold: 64 * 1024,
        }
    }

    pub async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError> {
        self.backend.set_nx(key, value, ttl).await
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError> {
        self.backend.zadd(key, score, member).await
    }

    pub async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError> {
        self.backend.zrangebyscore(key, min, max).await
    }

    pub async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError> {
        self.backend.zremrangebyscore(key, min, max).await
    }

    pub async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError> {
        self.backend.set_nx_batch(keys, value, ttl).await
    }

    pub async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError> {
        self.backend.mget(keys).await
    }

    pub async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError> {
        self.backend.incr(key, delta).await
    }

    pub async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError> {
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

    pub async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError> {
        self.backend.keys_with_limit(pattern, limit).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        self.backend.get(key).await
    }

    pub async fn ping(&self) -> Result<(), CacheError> {
        self.backend.ping().await
    }

    pub async fn script_load(&self, script: &str) -> Result<String, CacheError> {
        self.backend.script_load(script).await
    }

    pub async fn evalsha(&self, sha: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        self.backend.evalsha(sha, keys, args).await
    }

    pub async fn eval_lua(&self, script: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError> {
        self.backend.eval_lua(script, keys, args).await
    }

    pub fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl
    }
}
