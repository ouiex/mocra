use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use redis::Script;

use super::types::{DagError, DagRunGuard, DagRunGuardAcquireOutcome};

#[derive(Default)]
pub struct InMemoryDagRunGuard {
    locks: DashMap<String, (String, u64)>,
    pub renew_count: AtomicUsize,
    next_fencing_token: AtomicU64,
}

#[async_trait]
impl DagRunGuard for InMemoryDagRunGuard {
    async fn try_acquire(
        &self,
        lock_key: &str,
        owner: &str,
        _ttl_ms: u64,
    ) -> Result<DagRunGuardAcquireOutcome, DagError> {
        match self.locks.entry(lock_key.to_string()) {
            Entry::Occupied(existing) => {
                let (current_owner, token) = existing.get();
                Ok(DagRunGuardAcquireOutcome {
                    acquired: current_owner == owner,
                    fencing_token: Some(*token),
                })
            }
            Entry::Vacant(slot) => {
                let token = self
                    .next_fencing_token
                    .fetch_add(1, Ordering::SeqCst)
                    .saturating_add(1);
                slot.insert((owner.to_string(), token));
                Ok(DagRunGuardAcquireOutcome {
                    acquired: true,
                    fencing_token: Some(token),
                })
            }
        }
    }

    async fn renew(&self, lock_key: &str, owner: &str, _ttl_ms: u64) -> Result<bool, DagError> {
        self.renew_count.fetch_add(1, Ordering::SeqCst);
        let ok = self
            .locks
            .get(lock_key)
            .map(|entry| entry.value().0 == owner)
            .unwrap_or(false);
        Ok(ok)
    }

    async fn release(&self, lock_key: &str, owner: &str) -> Result<(), DagError> {
        if let Some(current) = self.locks.get(lock_key)
            && current.value().0 != owner
        {
            return Ok(());
        }
        self.locks.remove(lock_key);
        Ok(())
    }
}

#[derive(Clone)]
pub struct RedisDagRunGuard {
    client: redis::Client,
}

impl RedisDagRunGuard {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl DagRunGuard for RedisDagRunGuard {
    async fn try_acquire(
        &self,
        lock_key: &str,
        owner: &str,
        ttl_ms: u64,
    ) -> Result<DagRunGuardAcquireOutcome, DagError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::RunGuardAcquireFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        let token_key = format!("{lock_key}:token");
        let holder_token_key = format!("{lock_key}:holder_token");
        let script = Script::new(
            r#"
            if redis.call('GET', KEYS[1]) then
                return 0
            end
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            local t = redis.call('INCR', KEYS[2])
            redis.call('SET', KEYS[3], t, 'PX', ARGV[2])
            return t
            "#,
        );
        let token: i64 = script
            .key(lock_key)
            .key(&token_key)
            .key(&holder_token_key)
            .arg(owner)
            .arg(ttl_ms as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| DagError::RunGuardAcquireFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        if token <= 0 {
            return Ok(DagRunGuardAcquireOutcome {
                acquired: false,
                fencing_token: None,
            });
        }

        Ok(DagRunGuardAcquireOutcome {
            acquired: true,
            fencing_token: Some(token as u64),
        })
    }

    async fn renew(&self, lock_key: &str, owner: &str, ttl_ms: u64) -> Result<bool, DagError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::RunGuardRenewFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        let script = Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0
            "#,
        );
        let renewed: i64 = script
            .key(lock_key)
            .arg(owner)
            .arg(ttl_ms as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| DagError::RunGuardRenewFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        Ok(renewed == 1)
    }

    async fn release(&self, lock_key: &str, owner: &str) -> Result<(), DagError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::RunGuardReleaseFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        let script = Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                redis.call('DEL', KEYS[2])
                return redis.call('DEL', KEYS[1])
            end
            return 0
            "#,
        );
        let holder_token_key = format!("{lock_key}:holder_token");
        let _: i64 = script
            .key(lock_key)
            .key(&holder_token_key)
            .arg(owner)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| DagError::RunGuardReleaseFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        Ok(())
    }
}
