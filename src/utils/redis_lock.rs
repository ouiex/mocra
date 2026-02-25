#![allow(unused)]
use dashmap::DashMap;
use deadpool_redis::{Config, Runtime,redis::{AsyncCommands,Script}};
use log::trace;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::error;
use uuid::Uuid;

#[derive(Debug)]
pub enum LockError {
    Redis(deadpool_redis::redis::RedisError),
    Pool(deadpool_redis::PoolError),
    Timeout,
    InvalidOperation(String),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Redis(e) => write!(f, "Redis error: {e}"),
            LockError::Pool(e) => write!(f, "Pool error: {e}"),
            LockError::Timeout => write!(f, "Lock operation timed out"),
            LockError::InvalidOperation(msg) => write!(f, "Invalid operation: {msg}"),
        }
    }
}

impl std::error::Error for LockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LockError::Redis(e) => Some(e),
            LockError::Pool(e) => Some(e),
            _ => None,
        }
    }
}

impl From<deadpool_redis::redis::RedisError> for LockError {
    fn from(error: deadpool_redis::redis::RedisError) -> Self {
        LockError::Redis(error)
    }
}

impl From<deadpool_redis::PoolError> for LockError {
    fn from(error: deadpool_redis::PoolError) -> Self {
        LockError::Pool(error)
    }
}

// Simplified lock metadata structure.
#[derive(Debug, Clone)]
pub struct LockInfo {
    key: String,
    value: String,
    ttl: u64,
    created_at: Instant,
}

pub struct AdvancedDistributedLock {
    pool: Option<Arc<deadpool_redis::Pool>>,
    local_map: Option<Arc<DashMap<String, (String, Instant)>>>,
    lock_info: LockInfo,
    renewal_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AdvancedDistributedLock {
    /// Tries to acquire a lock with retry support.
    pub async fn acquire_with_retry(
        pool: Option<Arc<deadpool_redis::Pool>>,
        local_map: Option<Arc<DashMap<String, (String, Instant)>>>,
        lock_key: String,
        ttl_seconds: u64,
        retry_interval: Duration,
        max_wait: Duration,
    ) -> Result<Option<Self>, LockError> {
        let unique_value = Uuid::now_v7().to_string();
        let start_time = Instant::now();

        loop {
            let acquired =
                Self::try_acquire(&pool, &local_map, &lock_key, &unique_value, ttl_seconds).await?;

            if acquired {
                let lock_info = LockInfo {
                    key: lock_key,
                    value: unique_value,
                    ttl: ttl_seconds,
                    created_at: Instant::now(),
                };

                let mut lock = Self {
                    pool: pool.clone(),
                    local_map: local_map.clone(),
                    lock_info,
                    renewal_handle: None,
                };

                // Start automatic renewal.
                lock.start_renewal().await;
                return Ok(Some(lock));
            }

            if start_time.elapsed() >= max_wait {
                return Ok(None); // Timed out.
            }

            sleep(retry_interval).await;
        }
    }

    async fn try_acquire(
        pool: &Option<Arc<deadpool_redis::Pool>>,
        local_map: &Option<Arc<DashMap<String, (String, Instant)>>>,
        key: &str,
        value: &str,
        ttl: u64,
    ) -> Result<bool, LockError> {
        if let Some(pool) = pool {
            let mut conn = pool.get().await?;

            // Use `SET NX EX` to perform atomic lock acquisition.
            let script = r#"
            return redis.call("SET", KEYS[1], ARGV[1], "NX", "EX", ARGV[2])
        "#;

            let result: Option<String> = deadpool_redis::redis::Script::new(script)
                .key(key)
                .arg(value)
                .arg(ttl)
                .invoke_async(&mut conn)
                .await?;

            Ok(result.is_some())
        } else if let Some(map) = local_map {
            let now = Instant::now();
            // Optimistic check
            if let Some(mut entry) = map.get_mut(key) {
                if entry.1 < now {
                    // Expired, we can take it
                    *entry = (value.to_string(), now + Duration::from_secs(ttl));
                    return Ok(true);
                }
                Ok(false)
            } else {
                // Not exists, try to insert
                // DashMap's entry API would be better but let's keep it simple with a double check
                // Using try_insert if available or just insert if we are sure it's new?
                // DashMap doesn't have atomic "insert if not exists" that returns success easily without entry API
                // But we can use entry
                match map.entry(key.to_string()) {
                    dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                        if entry.get().1 < now {
                            entry.insert((value.to_string(), now + Duration::from_secs(ttl)));
                            return Ok(true);
                        }
                        Ok(false)
                    }
                    dashmap::mapref::entry::Entry::Vacant(entry) => {
                        entry.insert((value.to_string(), now + Duration::from_secs(ttl)));
                        Ok(true)
                    }
                }
            }
        } else {
            Err(LockError::InvalidOperation(
                "No pool or local map provided".to_string(),
            ))
        }
    }

    /// Starts the automatic renewal task.
    async fn start_renewal(&mut self) {
        let pool = self.pool.clone();
        let local_map = self.local_map.clone();
        let key = self.lock_info.key.clone();
        let value = self.lock_info.value.clone();
        let ttl = self.lock_info.ttl;

        let handle = tokio::spawn(async move {
            let renewal_interval = Duration::from_millis(ttl * 1000 / 3); // Renew once every 1/3 TTL.

            loop {
                sleep(renewal_interval).await;

                if let Some(pool) = &pool {
                    let script = r#"
                    if redis.call("GET", KEYS[1]) == ARGV[1] then
                        return redis.call("EXPIRE", KEYS[1], ARGV[2])
                    else
                        return 0
                    end
                "#;

                    match pool.get().await {
                        Ok(mut conn) => {
                            let result: Result<i32, _> =  deadpool_redis::redis::Script::new(script)
                                .key(&key)
                                .arg(&value)
                                .arg(ttl)
                                .invoke_async(&mut *conn)
                                .await;

                            match result {
                                Ok(1) => {
                                    trace!("Lock renewed successfully: {key}");
                                }
                                Ok(0) => {
                                    trace!("Lock is no longer valid, stopping renewal: {key}");
                                    break;
                                }
                                Ok(_) => {
                                    trace!("Renewal returned unexpected value: {key}");
                                    break;
                                }
                                Err(e) => {
                                    trace!("Renewal failed: {e}");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            trace!("Failed to get Redis connection, stopping renewal: {e}");
                            break;
                        }
                    }
                } else if let Some(map) = &local_map {
                    if let Some(mut entry) = map.get_mut(&key) {
                        if entry.0 == value {
                            entry.1 = Instant::now() + Duration::from_secs(ttl);
                            trace!("Local lock renewed successfully: {key}");
                        } else {
                            trace!("Local lock invalid (value mismatch), stopping renewal: {key}");
                            break;
                        }
                    } else {
                        trace!("Local lock invalid (missing), stopping renewal: {key}");
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
        // Stop renewal task.
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }

        if let Some(pool) = &self.pool {
            let script = r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#;

            let mut conn = pool.get().await?;
            let result: i32 =  deadpool_redis::redis::Script::new(script)
                .key(&self.lock_info.key)
                .arg(&self.lock_info.value)
                .invoke_async(&mut *conn)
                .await?;

            Ok(result == 1)
        } else if let Some(map) = &self.local_map {
            match map.entry(self.lock_info.key.clone()) {
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
        } else {
            Err(LockError::InvalidOperation(
                "No pool or local map provided".to_string(),
            ))
        }
    }

    /// Checks whether the lock is still valid.
    pub async fn is_valid(&self) -> Result<bool, LockError> {
        if let Some(pool) = &self.pool {
            let mut conn = pool.get().await?;
            let current_value: Option<String> = conn.get(&self.lock_info.key).await?;
            Ok(current_value.as_ref() == Some(&self.lock_info.value))
        } else if let Some(map) = &self.local_map {
            if let Some(entry) = map.get(&self.lock_info.key) {
                Ok(entry.0 == self.lock_info.value && entry.1 > Instant::now())
            } else {
                Ok(false)
            }
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
    redis_pool: Option<Arc<deadpool_redis::Pool>>,
    locks: Arc<DashMap<String, AdvancedDistributedLock>>,
    local_locks: Arc<DashMap<String, (String, Instant)>>, // Key -> (Value, Expiration Time)
    prefix: String,
}

impl DistributedLockManager {
    pub fn new(pool: Option<Arc<deadpool_redis::Pool>>, prefix: &str) -> Self {
        Self {
            redis_pool: pool,
            locks: Arc::new(DashMap::new()),
            local_locks: Arc::new(DashMap::new()),
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

        if let Some(lock) = AdvancedDistributedLock::acquire_with_retry(
            self.redis_pool.clone(),
            Some(self.local_locks.clone()),
            full_lock_name,
            ttl_seconds,
            if self.redis_pool.is_some() { Duration::from_millis(50) } else { Duration::from_millis(1) },
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
        if let Some((_, lock)) = self.locks.remove(lock_name) {
            let released = lock.release().await?;
            Ok(released)
        } else {
            Ok(false) // Lock does not exist.
        }
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

    // Checks whether lock is still valid.
    pub async fn is_lock_valid(&self, lock_name: &str) -> Result<bool, LockError> {
        if let Some(lock) = self.locks.get(lock_name) {
            lock.is_valid().await
        } else {
            Ok(false)
        }
    }

    // Returns the connection pool reference (for external consumers).
    pub fn get_pool(&self) -> Option<&deadpool_redis::Pool> {
        self.redis_pool.as_ref().map(|p| p.as_ref())
    }
}

// DistributedLockManager is Send + Sync via its fields (Arc<Pool>, Arc<RwLock<...>>)
// Rely on compiler to enforce/derive thread-safety without unsafe impls.
