use dashmap::DashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug)]
pub enum LockError {
    Timeout,
    InvalidOperation(String),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Timeout => write!(f, "Lock operation timed out"),
            LockError::InvalidOperation(msg) => write!(f, "Invalid operation: {msg}"),
        }
    }
}

impl std::error::Error for LockError {}

#[derive(Debug, Clone)]
pub struct LockInfo {
    key: String,
    value: String,
    ttl: u64,
}

pub struct AdvancedDistributedLock {
    local_map: Arc<DashMap<String, (String, Instant)>>,
    lock_info: LockInfo,
    renewal_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AdvancedDistributedLock {
    pub async fn acquire_with_retry(
        local_map: Arc<DashMap<String, (String, Instant)>>,
        lock_key: String,
        ttl_seconds: u64,
        retry_interval: Duration,
        max_wait: Duration,
    ) -> Result<Option<Self>, LockError> {
        let unique_value = Uuid::now_v7().to_string();
        let start_time = Instant::now();

        loop {
            let acquired = Self::try_acquire(&local_map, &lock_key, &unique_value, ttl_seconds)?;

            if acquired {
                let lock_info = LockInfo {
                    key: lock_key,
                    value: unique_value,
                    ttl: ttl_seconds,
                };

                let mut lock = Self {
                    local_map,
                    lock_info,
                    renewal_handle: None,
                };
                lock.start_renewal().await;
                return Ok(Some(lock));
            }

            if start_time.elapsed() >= max_wait {
                return Ok(None);
            }

            sleep(retry_interval).await;
        }
    }

    fn try_acquire(
        map: &Arc<DashMap<String, (String, Instant)>>,
        key: &str,
        value: &str,
        ttl: u64,
    ) -> Result<bool, LockError> {
        let now = Instant::now();
        match map.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if entry.get().1 < now {
                    entry.insert((value.to_string(), now + Duration::from_secs(ttl)));
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert((value.to_string(), now + Duration::from_secs(ttl)));
                Ok(true)
            }
        }
    }

    async fn start_renewal(&mut self) {
        let local_map = self.local_map.clone();
        let key = self.lock_info.key.clone();
        let value = self.lock_info.value.clone();
        let ttl = self.lock_info.ttl;

        let handle = tokio::spawn(async move {
            let renewal_interval = Duration::from_millis((ttl * 1000 / 3).max(1));

            loop {
                sleep(renewal_interval).await;
                if let Some(mut entry) = local_map.get_mut(&key) {
                    if entry.0 == value {
                        entry.1 = Instant::now() + Duration::from_secs(ttl);
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        });

        self.renewal_handle = Some(handle);
    }

    pub async fn release(mut self) -> Result<bool, LockError> {
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }

        match self.local_map.entry(self.lock_info.key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if entry.get().0 == self.lock_info.value {
                    entry.remove();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(_) => Ok(false),
        }
    }

    pub async fn is_valid(&self) -> Result<bool, LockError> {
        if let Some(entry) = self.local_map.get(&self.lock_info.key) {
            Ok(entry.0 == self.lock_info.value && entry.1 > Instant::now())
        } else {
            Ok(false)
        }
    }
}

impl std::fmt::Debug for AdvancedDistributedLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdvancedDistributedLock")
            .field("lock_info", &self.lock_info)
            .field("renewal_handle", &self.renewal_handle.is_some())
            .finish()
    }
}

impl Drop for AdvancedDistributedLock {
    fn drop(&mut self) {
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }
    }
}

#[derive(Debug)]
pub struct DistributedLockManager {
    locks: Arc<DashMap<String, AdvancedDistributedLock>>,
    local_locks: Arc<DashMap<String, (String, Instant)>>,
    backend: Option<Arc<dyn LockBackend>>,
    prefix: String,
}

impl DistributedLockManager {
    pub fn new(_pool: Option<()>, prefix: &str) -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
            local_locks: Arc::new(DashMap::new()),
            backend: None,
            prefix: prefix.to_string(),
        }
    }

    pub fn with_backend(backend: Arc<dyn LockBackend>, prefix: &str) -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
            local_locks: Arc::new(DashMap::new()),
            backend: Some(backend),
            prefix: prefix.to_string(),
        }
    }

    fn format_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}:{}", self.prefix, key)
        }
    }

    pub async fn acquire_lock(
        &self,
        lock_name: &str,
        ttl_seconds: u64,
        max_wait: Duration,
    ) -> Result<bool, LockError> {
        let full_lock_name = self.format_key(lock_name);

        if let Some(backend) = &self.backend {
            let token = uuid::Uuid::now_v7().to_string();
            let start = Instant::now();
            loop {
                match backend
                    .acquire(&full_lock_name, &token, ttl_seconds * 1000)
                    .await
                {
                    Ok(true) => {
                        self.local_locks.insert(
                            lock_name.to_string(),
                            (
                                token.clone(),
                                Instant::now() + Duration::from_secs(ttl_seconds),
                            ),
                        );
                        return Ok(true);
                    }
                    Ok(false) => {
                        if start.elapsed() >= max_wait {
                            return Ok(false);
                        }
                        sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => return Err(LockError::InvalidOperation(e)),
                }
            }
        }

        if let Some(lock) = AdvancedDistributedLock::acquire_with_retry(
            self.local_locks.clone(),
            full_lock_name,
            ttl_seconds,
            Duration::from_millis(1),
            max_wait,
        )
        .await?
        {
            self.locks.insert(lock_name.to_string(), lock);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn acquire_lock_default(&self, lock_name: &str) -> Result<bool, LockError> {
        self.acquire_lock(lock_name, 5, Duration::from_secs(10))
            .await
    }

    pub async fn release_lock(&self, lock_name: &str) -> Result<bool, LockError> {
        let full_lock_name = self.format_key(lock_name);

        if let Some((_, lock)) = self.locks.remove(lock_name) {
            return lock.release().await;
        }

        if let Some(backend) = &self.backend
            && let Some((_, (token, _))) = self.local_locks.remove(lock_name)
        {
            return backend
                .release(&full_lock_name, &token)
                .await
                .map_err(LockError::InvalidOperation);
        }

        Ok(false)
    }

    pub async fn with_lock<F, R>(
        &self,
        lock_name: &str,
        ttl_seconds: u64,
        max_wait: Duration,
        f: F,
    ) -> Result<Option<R>, LockError>
    where
        F: Future<Output = R>,
    {
        if self.acquire_lock(lock_name, ttl_seconds, max_wait).await? {
            let result = f.await;
            self.release_lock(lock_name).await?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub async fn is_lock_valid(&self, lock_name: &str) -> Result<bool, LockError> {
        if let Some(lock) = self.locks.get(lock_name) {
            lock.is_valid().await
        } else {
            Ok(false)
        }
    }
}

#[async_trait::async_trait]
pub trait LockBackend: Send + Sync + std::fmt::Debug {
    async fn acquire(&self, key: &str, owner_token: &str, ttl_ms: u64) -> Result<bool, String>;
    async fn renew(&self, key: &str, owner_token: &str, ttl_ms: u64) -> Result<bool, String>;
    async fn release(&self, key: &str, owner_token: &str) -> Result<bool, String>;
}

#[derive(Debug)]
pub struct LocalLockBackend {
    locks: Arc<DashMap<String, (String, Instant)>>,
}

impl Default for LocalLockBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalLockBackend {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl LockBackend for LocalLockBackend {
    async fn acquire(&self, key: &str, owner_token: &str, ttl_ms: u64) -> Result<bool, String> {
        let now = Instant::now();
        let ttl = Duration::from_millis(ttl_ms);
        match self.locks.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if entry.get().1 < now {
                    entry.insert((owner_token.to_string(), now + ttl));
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert((owner_token.to_string(), now + ttl));
                Ok(true)
            }
        }
    }

    async fn renew(&self, key: &str, owner_token: &str, ttl_ms: u64) -> Result<bool, String> {
        if let Some(mut entry) = self.locks.get_mut(key)
            && entry.0 == *owner_token
        {
            entry.1 = Instant::now() + Duration::from_millis(ttl_ms);
            return Ok(true);
        }
        Ok(false)
    }

    async fn release(&self, key: &str, owner_token: &str) -> Result<bool, String> {
        match self.locks.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                if entry.get().0 == owner_token {
                    entry.remove();
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            dashmap::mapref::entry::Entry::Vacant(_) => Ok(false),
        }
    }
}

use crate::engine::api::profile_store::ProfileControlPlaneStore;

#[derive(Debug)]
pub struct RaftLockBackend {
    store: Arc<ProfileControlPlaneStore>,
    namespace: String,
}

impl RaftLockBackend {
    pub fn new(store: Arc<ProfileControlPlaneStore>, namespace: impl Into<String>) -> Self {
        Self {
            store,
            namespace: namespace.into(),
        }
    }
}

#[async_trait::async_trait]
impl LockBackend for RaftLockBackend {
    async fn acquire(&self, key: &str, owner_token: &str, ttl_ms: u64) -> Result<bool, String> {
        self.store
            .submit_lock_acquire(&self.namespace, key, owner_token, ttl_ms)
            .await
            .map_err(|e| format!("{e}"))
    }

    async fn renew(&self, key: &str, owner_token: &str, ttl_ms: u64) -> Result<bool, String> {
        self.store
            .submit_lock_renew(&self.namespace, key, owner_token, ttl_ms)
            .await
            .map_err(|e| format!("{e}"))
    }

    async fn release(&self, key: &str, owner_token: &str) -> Result<bool, String> {
        self.store
            .submit_lock_release(&self.namespace, key, owner_token)
            .await
            .map_err(|e| format!("{e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_lock_acquire_release() {
        let backend = LocalLockBackend::new();
        assert!(backend.acquire("k1", "owner1", 5000).await.unwrap());
        assert!(!backend.acquire("k1", "owner1", 5000).await.unwrap());
        assert!(!backend.acquire("k1", "owner2", 5000).await.unwrap());
        assert!(backend.release("k1", "owner1").await.unwrap());
        assert!(backend.acquire("k1", "owner2", 5000).await.unwrap());
    }

    #[tokio::test]
    async fn local_lock_renew_wrong_owner_fails() {
        let backend = LocalLockBackend::new();
        backend.acquire("k1", "owner1", 5000).await.unwrap();
        assert!(backend.renew("k1", "owner1", 10000).await.unwrap());
        assert!(!backend.renew("k1", "owner2", 10000).await.unwrap());
    }

    #[tokio::test]
    async fn local_lock_release_wrong_owner_fails() {
        let backend = LocalLockBackend::new();
        backend.acquire("k1", "owner1", 5000).await.unwrap();
        assert!(!backend.release("k1", "owner2").await.unwrap());
        assert!(backend.release("k1", "owner1").await.unwrap());
    }

    #[tokio::test]
    async fn raft_lock_acquire_release() {
        let store = crate::engine::api::profile_store::ProfileControlPlaneStore::default();
        let backend = RaftLockBackend::new(Arc::new(store), "ns");
        assert!(backend.acquire("k1", "owner1", 5000).await.unwrap());
        assert!(!backend.acquire("k1", "owner2", 5000).await.unwrap());
        assert!(backend.release("k1", "owner1").await.unwrap());
        assert!(backend.acquire("k1", "owner2", 5000).await.unwrap());
    }

    #[tokio::test]
    async fn raft_lock_renew_owner_check() {
        let store = crate::engine::api::profile_store::ProfileControlPlaneStore::default();
        let backend = RaftLockBackend::new(Arc::new(store), "ns");
        backend.acquire("k2", "owner1", 5000).await.unwrap();
        assert!(backend.renew("k2", "owner1", 10000).await.unwrap());
        assert!(!backend.renew("k2", "owner2", 10000).await.unwrap());
    }

    #[tokio::test]
    async fn raft_lock_expired_preemption() {
        let store = crate::engine::api::profile_store::ProfileControlPlaneStore::default();
        let backend = RaftLockBackend::new(Arc::new(store), "ns");
        backend.acquire("k3", "owner1", 1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(backend.acquire("k3", "owner2", 5000).await.unwrap());
    }
}
