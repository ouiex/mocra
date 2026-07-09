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

use crate::sync::CoordinationBackend;

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
/// 由 [`CoordinationBackend`] 支撑的分布式锁句柄。
///
/// 集群(如内嵌 redb+Raft)模式下,锁经共识落库,跨节点强一致 —— 取代无 Redis
/// 时退化的**进程内**本地锁。自带续租任务;`release` 走 CAS-del(仅持有者可删)。
pub struct CoordinationLock {
    backend: Arc<dyn CoordinationBackend>,
    key: String,   // 完整格式化后的 key
    value: String, // 唯一持有者 token
    ttl_ms: u64,   // 校验 / 续租用的 TTL
    renewal_handle: Option<tokio::task::JoinHandle<()>>,
}

impl CoordinationLock {
    async fn release(mut self) -> bool {
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }
        self.backend
            .release_lock(&self.key, self.value.as_bytes())
            .await
            .unwrap_or(false)
    }
}

impl Drop for CoordinationLock {
    fn drop(&mut self) {
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }
    }
}

/// 后台续租:每 `ttl_ms/3` 续一次;丢锁 / 出错即停。
fn spawn_coord_renewal(
    backend: Arc<dyn CoordinationBackend>,
    key: String,
    value: String,
    ttl_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_millis((ttl_ms / 3).max(1));
        loop {
            sleep(interval).await;
            match backend.renew_lock(&key, value.as_bytes(), ttl_ms).await {
                Ok(true) => trace!("Coordination lock renewed: {key}"),
                Ok(false) => {
                    trace!("Coordination lock lost, stopping renewal: {key}");
                    break;
                }
                Err(e) => {
                    trace!("Coordination lock renewal failed ({key}): {e}");
                    break;
                }
            }
        }
    })
}

pub struct DistributedLockManager {
    redis_pool: Option<Arc<deadpool_redis::Pool>>,
    /// 可选的协调后端(内嵌 Raft 等);置位后**优先**于 Redis / 本地锁,
    /// 使无 Redis 集群的锁仍跨节点强一致。
    coordination: Option<Arc<dyn CoordinationBackend>>,
    locks: Arc<DashMap<String, AdvancedDistributedLock>>,
    coord_locks: Arc<DashMap<String, CoordinationLock>>,
    local_locks: Arc<DashMap<String, (String, Instant)>>, // Key -> (Value, Expiration Time)
    prefix: String,
}

impl std::fmt::Debug for DistributedLockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedLockManager")
            .field("has_redis", &self.redis_pool.is_some())
            .field("has_coordination", &self.coordination.is_some())
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl DistributedLockManager {
    pub fn new(pool: Option<Arc<deadpool_redis::Pool>>, prefix: &str) -> Self {
        Self::new_with_coordination(pool, None, prefix)
    }

    /// 带协调后端构造:集群模式下所有锁经 `coordination`(如 Raft)协商。
    pub fn new_with_coordination(
        pool: Option<Arc<deadpool_redis::Pool>>,
        coordination: Option<Arc<dyn CoordinationBackend>>,
        prefix: &str,
    ) -> Self {
        Self {
            redis_pool: pool,
            coordination,
            locks: Arc::new(DashMap::new()),
            coord_locks: Arc::new(DashMap::new()),
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
        // 集群协调后端优先(Raft 强一致锁),取代无 Redis 时的进程内本地锁。
        if let Some(backend) = self.coordination.clone() {
            return self
                .acquire_lock_via_coordination(backend, lock_name, ttl_seconds, max_wait)
                .await;
        }

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

    /// 经协调后端(Raft 等)获取锁:重试直至成功或超过 `max_wait`,
    /// 成功后登记续租任务与句柄。
    async fn acquire_lock_via_coordination(
        &self,
        backend: Arc<dyn CoordinationBackend>,
        lock_name: &str,
        ttl_seconds: u64,
        max_wait: Duration,
    ) -> Result<bool, LockError> {
        let full = self.format_key(lock_name);
        let value = Uuid::now_v7().to_string();
        let ttl_ms = ttl_seconds.saturating_mul(1000).max(1);
        let start = Instant::now();
        loop {
            match backend.acquire_lock(&full, value.as_bytes(), ttl_ms).await {
                Ok(true) => {
                    let handle = spawn_coord_renewal(
                        backend.clone(),
                        full.clone(),
                        value.clone(),
                        ttl_ms,
                    );
                    self.coord_locks.insert(
                        lock_name.to_string(),
                        CoordinationLock {
                            backend,
                            key: full,
                            value,
                            ttl_ms,
                            renewal_handle: Some(handle),
                        },
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

    pub async fn acquire_lock_default(&self, lock_name: &str) -> Result<bool, LockError> {
        self.acquire_lock(lock_name, 5, Duration::from_secs(10))
            .await
    }

    pub async fn release_lock(&self, lock_name: &str) -> Result<bool, LockError> {
        // 协调后端(集群)锁优先释放。
        if let Some((_, lock)) = self.coord_locks.remove(lock_name) {
            return Ok(lock.release().await);
        }
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
        // 协调后端(集群)锁:锁存于独立 LOCKS 命名空间,`get`(读 KV)看不到;
        // 用 `renew_lock` 校验 —— 仅当仍由我们的 token 持有时返回 true(顺带续租,无害)。
        if self.coordination.is_some() {
            // 快照后再 await,避免跨 await 持有 DashMap guard。
            let snapshot = self
                .coord_locks
                .get(lock_name)
                .map(|l| (l.backend.clone(), l.key.clone(), l.value.clone(), l.ttl_ms));
            return if let Some((backend, key, value, ttl_ms)) = snapshot {
                backend
                    .renew_lock(&key, value.as_bytes(), ttl_ms)
                    .await
                    .map_err(LockError::InvalidOperation)
            } else {
                Ok(false)
            };
        }
        // Extract only what we need for validation, then drop the DashMap guard.
        // Holding a DashMap Ref across .await can deadlock the Tokio runtime
        // (parking_lot RwLock blocks the OS thread).
        let snapshot = self.locks.get(lock_name).map(|l| {
            let lock = l.value();
            (lock.pool.clone(), lock.local_map.clone(), lock.lock_info.clone())
        });
        // guard dropped here
        if let Some((pool, local_map, lock_info)) = snapshot {
            if let Some(pool) = &pool {
                let mut conn = pool.get().await?;
                let current_value: Option<String> = conn.get(&lock_info.key).await?;
                Ok(current_value.as_ref() == Some(&lock_info.value))
            } else if let Some(map) = &local_map {
                if let Some(entry) = map.get(&lock_info.key) {
                    Ok(entry.0 == lock_info.value && entry.1 > Instant::now())
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    // Returns the connection pool reference (for external consumers).
    pub fn get_pool(&self) -> Option<&deadpool_redis::Pool> {
        self.redis_pool.as_ref().map(|p| p.as_ref())
    }
}

#[cfg(all(test, feature = "cluster-embedded"))]
mod coordination_tests {
    use super::*;
    use crate::sync::RaftCoordinationBackend;
    use mocra_cluster::RaftControlPlane;

    /// 端到端:LockManager → CoordinationBackend → Raft → redb。
    /// 证明无 Redis 集群下,锁跨「持有者」互斥且可提前释放(不靠 TTL)。
    #[tokio::test]
    async fn lock_manager_routes_through_raft_coordination() {
        let dir = tempfile::tempdir().unwrap();
        let cp = RaftControlPlane::start_single_node(1, dir.path())
            .await
            .unwrap();
        cp.wait_leader(Duration::from_secs(10)).await.unwrap();
        let backend: Arc<dyn CoordinationBackend> = Arc::new(RaftCoordinationBackend::new(cp));

        // 两个「节点」共享同一后端(单进程内模拟两方争锁)。
        let node_a = DistributedLockManager::new_with_coordination(
            None,
            Some(backend.clone()),
            "cluster",
        );
        let node_b =
            DistributedLockManager::new_with_coordination(None, Some(backend.clone()), "cluster");

        // A 抢到锁;B 抢同名锁应失败(强一致互斥,非进程内)。
        assert!(node_a
            .acquire_lock("job", 30, Duration::from_millis(200))
            .await
            .unwrap());
        assert!(!node_b
            .acquire_lock("job", 30, Duration::from_millis(200))
            .await
            .unwrap());
        assert!(node_a.is_lock_valid("job").await.unwrap());

        // A 提前释放(不等 TTL),B 随即可抢到。
        assert!(node_a.release_lock("job").await.unwrap());
        assert!(node_b
            .acquire_lock("job", 30, Duration::from_millis(500))
            .await
            .unwrap());
        assert!(node_b.is_lock_valid("job").await.unwrap());
        assert!(!node_a.is_lock_valid("job").await.unwrap());
    }
}

// DistributedLockManager is Send + Sync via its fields (Arc<Pool>, Arc<RwLock<...>>)
// Rely on compiler to enforce/derive thread-safety without unsafe impls.
