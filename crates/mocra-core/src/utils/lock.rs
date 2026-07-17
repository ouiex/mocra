#![allow(unused)]
//! Distributed lock manager.
//!
//! Locks take one of two paths —
//! - **coordination backend** ([`CoordinationBackend`], e.g. embedded redb+Raft): strongly
//!   consistent across nodes, the default in cluster mode;
//! - **in-process local lock** (`DashMap`): the single-node fallback used when no coordination
//!   backend is injected.
use dashmap::DashMap;
use log::trace;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

use crate::utils::coordination::CoordinationBackend;

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

// Simplified lock metadata structure.
#[derive(Debug, Clone)]
pub struct LockInfo {
    key: String,
    value: String,
    ttl: u64,
    created_at: Instant,
}

/// In-process local lock handle (the default when no coordination backend is injected; comes
/// with its own lease-renewal task).
pub struct AdvancedDistributedLock {
    local_map: Option<Arc<DashMap<String, (String, Instant)>>>,
    lock_info: LockInfo,
    renewal_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AdvancedDistributedLock {
    /// Tries to acquire a lock with retry support.
    pub async fn acquire_with_retry(
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
                Self::try_acquire(&local_map, &lock_key, &unique_value, ttl_seconds).await?;

            if acquired {
                let lock_info = LockInfo {
                    key: lock_key,
                    value: unique_value,
                    ttl: ttl_seconds,
                    created_at: Instant::now(),
                };

                let mut lock = Self {
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
        local_map: &Option<Arc<DashMap<String, (String, Instant)>>>,
        key: &str,
        value: &str,
        ttl: u64,
    ) -> Result<bool, LockError> {
        if let Some(map) = local_map {
            let now = Instant::now();
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
        } else {
            Err(LockError::InvalidOperation(
                "No local map provided".to_string(),
            ))
        }
    }

    /// Starts the automatic renewal task.
    async fn start_renewal(&mut self) {
        let local_map = self.local_map.clone();
        let key = self.lock_info.key.clone();
        let value = self.lock_info.value.clone();
        let ttl = self.lock_info.ttl;

        let handle = tokio::spawn(async move {
            let renewal_interval = Duration::from_millis(ttl * 1000 / 3); // Renew once every 1/3 TTL.

            loop {
                sleep(renewal_interval).await;

                if let Some(map) = &local_map {
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

        if let Some(map) = &self.local_map {
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
                "No local map provided".to_string(),
            ))
        }
    }

    /// Checks whether the lock is still valid.
    pub async fn is_valid(&self) -> Result<bool, LockError> {
        if let Some(map) = &self.local_map {
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

/// Distributed lock handle backed by a [`CoordinationBackend`].
///
/// In cluster mode (e.g. embedded redb+Raft) the lock is persisted through consensus and is
/// strongly consistent across nodes — replacing the **in-process** local lock it falls back to
/// when no coordination backend is present. Comes with its own lease-renewal task; `release`
/// goes through CAS-del (only the holder can delete it).
pub struct CoordinationLock {
    backend: Arc<dyn CoordinationBackend>,
    key: String,   // fully formatted key
    value: String, // unique holder token
    ttl_ms: u64,   // TTL used for validation / lease renewal
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

/// Background lease renewal: renews every `ttl_ms/3`; stops as soon as the lock is lost or an
/// error occurs.
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
    /// Optional coordination backend (embedded Raft, etc.); when set it takes **precedence**
    /// over the in-process local lock, making locks strongly consistent across nodes in
    /// cluster mode.
    coordination: Option<Arc<dyn CoordinationBackend>>,
    locks: Arc<DashMap<String, AdvancedDistributedLock>>,
    coord_locks: Arc<DashMap<String, CoordinationLock>>,
    local_locks: Arc<DashMap<String, (String, Instant)>>, // Key -> (Value, Expiration Time)
    prefix: String,
}

impl std::fmt::Debug for DistributedLockManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedLockManager")
            .field("has_coordination", &self.coordination.is_some())
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl DistributedLockManager {
    pub fn new(prefix: &str) -> Self {
        Self::new_with_coordination(None, prefix)
    }

    /// Construct with a coordination backend: in cluster mode every lock is negotiated through
    /// `coordination` (e.g. Raft).
    pub fn new_with_coordination(
        coordination: Option<Arc<dyn CoordinationBackend>>,
        prefix: &str,
    ) -> Self {
        Self {
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
        // The cluster coordination backend wins (strongly consistent Raft lock), replacing the
        // in-process local lock used when no coordination backend is present.
        if let Some(backend) = self.coordination.clone() {
            return self
                .acquire_lock_via_coordination(backend, lock_name, ttl_seconds, max_wait)
                .await;
        }

        let full_lock_name = self.format_key(lock_name);

        if let Some(lock) = AdvancedDistributedLock::acquire_with_retry(
            Some(self.local_locks.clone()),
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

    /// Acquire a lock through the coordination backend (Raft, etc.): retries until it succeeds
    /// or `max_wait` elapses, then registers the lease-renewal task and the handle.
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
                    let handle =
                        spawn_coord_renewal(backend.clone(), full.clone(), value.clone(), ttl_ms);
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
        // Release the coordination-backend (cluster) lock first.
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
        // Coordination-backend (cluster) lock: it lives in a separate LOCKS namespace that
        // `get` (a KV read) cannot see, so validate with `renew_lock` — it returns true only
        // while our token still holds the lock (renewing the lease as a harmless side effect).
        if self.coordination.is_some() {
            // Snapshot before awaiting, so we never hold a DashMap guard across an await.
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
        let snapshot = self
            .locks
            .get(lock_name)
            .map(|l| (l.local_map.clone(), l.lock_info.clone()));
        // guard dropped here
        if let Some((local_map, lock_info)) = snapshot {
            if let Some(map) = &local_map {
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
}

// DistributedLockManager is Send + Sync via its fields (Arc<DashMap<...>>).
// Rely on compiler to enforce/derive thread-safety without unsafe impls.
