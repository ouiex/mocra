use deadpool_redis::redis::Value as RedisValue;
use errors::CacheError;
use std::time::Duration;

#[async_trait::async_trait]
pub trait CacheBackend: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), CacheError>;
    async fn del(&self, key: &str) -> Result<(), CacheError>;
    async fn del_batch(&self, keys: &[&str]) -> Result<u64, CacheError>;
    async fn keys(&self, pattern: &str) -> Result<Vec<String>, CacheError>;
    async fn keys_with_limit(&self, pattern: &str, limit: usize) -> Result<Vec<String>, CacheError>;
    async fn set_nx(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<bool, CacheError>;
    async fn set_nx_batch(&self, keys: &[&str], value: &[u8], ttl: Option<Duration>) -> Result<Vec<bool>, CacheError>;
    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, CacheError>;
    async fn incr(&self, key: &str, delta: i64) -> Result<i64, CacheError>;
    async fn ping(&self) -> Result<(), CacheError>;
    async fn zadd(&self, key: &str, score: f64, member: &[u8]) -> Result<i64, CacheError>;
    async fn zrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<Vec<Vec<u8>>, CacheError>;
    async fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64, CacheError>;
    async fn script_load(&self, script: &str) -> Result<String, CacheError>;
    async fn evalsha(&self, sha: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError>;
    async fn eval_lua(&self, script: &str, keys: &[&str], args: &[&str]) -> Result<RedisValue, CacheError>;
}
