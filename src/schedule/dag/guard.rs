use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;

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

