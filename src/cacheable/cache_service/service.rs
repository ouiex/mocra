use super::backend::CacheBackend;
use super::local_backend::LocalBackend;
use super::raft_backend::RaftRocksDbCacheBackend;
use crate::common::model::config::CacheBackendKind;
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use crate::errors::CacheError;
use std::sync::Arc;
use std::time::Duration;

pub struct CacheService {
    pub(crate) backend: Arc<dyn CacheBackend>,
    pub(crate) namespace: String,
    pub(crate) default_ttl: Option<Duration>,
    pub(crate) serialize_blocking_threshold: usize,
}

#[derive(Clone)]
pub struct CacheServiceConfig {
    pub pool: Option<()>,
    pub namespace: String,
    pub default_ttl: Option<Duration>,
    pub compression_threshold: Option<usize>,
    pub enable_l1: bool,
    pub l1_ttl_secs: u64,
    pub l1_max_entries: usize,
    pub backend_kind: Option<CacheBackendKind>,
    pub profile_store: Option<Arc<ProfileControlPlaneStore>>,
}

impl CacheServiceConfig {
    pub fn local(namespace: impl Into<String>) -> Self {
        Self {
            pool: None,
            namespace: namespace.into(),
            default_ttl: None,
            compression_threshold: None,
            enable_l1: false,
            l1_ttl_secs: 30,
            l1_max_entries: 10000,
            backend_kind: Some(CacheBackendKind::Local),
            profile_store: None,
        }
    }

    pub fn raft_rocksdb(
        namespace: impl Into<String>,
        profile_store: Arc<ProfileControlPlaneStore>,
    ) -> Self {
        Self {
            backend_kind: Some(CacheBackendKind::RaftRocksdb),
            profile_store: Some(profile_store),
            ..Self::local(namespace)
        }
    }

    pub fn with_pool(mut self, pool: Option<()>) -> Self {
        self.pool = pool;
        self
    }

    pub fn with_default_ttl(mut self, default_ttl: Option<Duration>) -> Self {
        self.default_ttl = default_ttl;
        self
    }

    pub fn with_compression_threshold(mut self, compression_threshold: Option<usize>) -> Self {
        self.compression_threshold = compression_threshold;
        self
    }

    pub fn with_l1(mut self, enable_l1: bool, l1_ttl_secs: u64, l1_max_entries: usize) -> Self {
        self.enable_l1 = enable_l1;
        self.l1_ttl_secs = l1_ttl_secs;
        self.l1_max_entries = l1_max_entries;
        self
    }

    pub fn with_backend(
        mut self,
        backend_kind: Option<CacheBackendKind>,
        profile_store: Option<Arc<ProfileControlPlaneStore>>,
    ) -> Self {
        self.backend_kind = backend_kind;
        self.profile_store = profile_store;
        self
    }
}

impl CacheService {
    pub fn new(config: CacheServiceConfig) -> Self {
        let threshold = config.compression_threshold.unwrap_or(1024);

        let backend: Arc<dyn CacheBackend> = match config.backend_kind {
            Some(CacheBackendKind::Local) => Arc::new(LocalBackend::new()),
            Some(CacheBackendKind::RaftRocksdb) => {
                let store = config
                    .profile_store
                    .expect("ProfileControlPlaneStore required for raft_rocksdb backend");
                Arc::new(RaftRocksDbCacheBackend::new(
                    store,
                    config.namespace.clone(),
                    config.default_ttl,
                ))
            }
            None => Arc::new(LocalBackend::new()),
        };
        let _ = (
            config.pool,
            threshold,
            config.enable_l1,
            config.l1_ttl_secs,
            config.l1_max_entries,
        );

        CacheService {
            backend,
            namespace: config.namespace,
            default_ttl: config.default_ttl,
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
