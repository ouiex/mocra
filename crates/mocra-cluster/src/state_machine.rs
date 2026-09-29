//! A redb-backed replicated state machine.
//!
//! Commands mutate the persistent state deterministically through [`apply`](StateMachine::apply).
//! This is exactly the state machine Raft replicates: every node calls `apply` once a log entry is
//! committed and arrives at the same result.
//!
//! Tables: `kv` (general-purpose KV), `locks` (distributed locks), `meta` (the fencing counter),
//! and `raft_state` (OpenRaft state-machine progress and the latest snapshot).

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::cmd::{Cmd, CmdResult, Lock};

const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");
const LOCKS: TableDefinition<&str, &[u8]> = TableDefinition::new("locks");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const RAFT_STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_state");
const FENCING_COUNTER: &str = "fencing_counter";

/// State machine errors.
#[derive(Debug, thiserror::Error)]
pub enum StateMachineError {
    #[error("redb: {0}")]
    Redb(String),
    #[error("codec: {0}")]
    Codec(String),
}

fn redb_err<E: std::fmt::Display>(e: E) -> StateMachineError {
    StateMachineError::Redb(e.to_string())
}

fn encode_lock(l: &Lock) -> Result<Vec<u8>, StateMachineError> {
    rmp_serde::to_vec(l).map_err(|e| StateMachineError::Codec(e.to_string()))
}

fn decode_lock(b: &[u8]) -> Result<Lock, StateMachineError> {
    rmp_serde::from_slice(b).map_err(|e| StateMachineError::Codec(e.to_string()))
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StateMachineError> {
    rmp_serde::to_vec(value).map_err(|e| StateMachineError::Codec(e.to_string()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StateMachineError> {
    rmp_serde::from_slice(bytes).map_err(|e| StateMachineError::Codec(e.to_string()))
}

fn apply_in_transaction(w: &WriteTransaction, cmd: &Cmd) -> Result<CmdResult, StateMachineError> {
    let result = match cmd {
        Cmd::Set { key, value } => {
            let mut t = w.open_table(KV).map_err(redb_err)?;
            t.insert(key.as_slice(), value.as_slice())
                .map_err(redb_err)?;
            CmdResult::Ok
        }
        Cmd::Delete { key } => {
            let mut t = w.open_table(KV).map_err(redb_err)?;
            t.remove(key.as_slice()).map_err(redb_err)?;
            CmdResult::Ok
        }
        Cmd::Cas { key, expect, value } => {
            let mut t = w.open_table(KV).map_err(redb_err)?;
            let cur = t
                .get(key.as_slice())
                .map_err(redb_err)?
                .map(|g| g.value().to_vec());
            if cur.as_deref() == expect.as_deref() {
                t.insert(key.as_slice(), value.as_slice())
                    .map_err(redb_err)?;
                CmdResult::Bool(true)
            } else {
                CmdResult::Bool(false)
            }
        }
        Cmd::AcquireLock {
            key,
            holder,
            now_ms,
            ttl_ms,
        } => {
            let mut locks = w.open_table(LOCKS).map_err(redb_err)?;
            let cur = match locks.get(key.as_str()).map_err(redb_err)? {
                Some(g) => Some(decode_lock(g.value())?),
                None => None,
            };
            let free = match &cur {
                None => true,
                Some(l) => l.expire_at_ms <= *now_ms || l.holder == *holder,
            };
            if free {
                let token = {
                    let mut meta = w.open_table(META).map_err(redb_err)?;
                    let n = meta
                        .get(FENCING_COUNTER)
                        .map_err(redb_err)?
                        .map(|g| g.value())
                        .unwrap_or(0)
                        + 1;
                    meta.insert(FENCING_COUNTER, n).map_err(redb_err)?;
                    n
                };
                let lock = Lock {
                    holder: holder.clone(),
                    expire_at_ms: now_ms + ttl_ms,
                    fencing_token: token,
                };
                locks
                    .insert(key.as_str(), encode_lock(&lock)?.as_slice())
                    .map_err(redb_err)?;
                CmdResult::Fencing(Some(token))
            } else {
                CmdResult::Fencing(None)
            }
        }
        Cmd::RenewLock {
            key,
            holder,
            now_ms,
            ttl_ms,
        } => {
            let mut locks = w.open_table(LOCKS).map_err(redb_err)?;
            let cur = match locks.get(key.as_str()).map_err(redb_err)? {
                Some(g) => Some(decode_lock(g.value())?),
                None => None,
            };
            match cur {
                Some(mut l) if l.holder == *holder && l.expire_at_ms > *now_ms => {
                    l.expire_at_ms = now_ms + ttl_ms;
                    locks
                        .insert(key.as_str(), encode_lock(&l)?.as_slice())
                        .map_err(redb_err)?;
                    CmdResult::Bool(true)
                }
                _ => CmdResult::Bool(false),
            }
        }
        Cmd::ReleaseLock { key, holder } => {
            let mut locks = w.open_table(LOCKS).map_err(redb_err)?;
            let held_by_holder = match locks.get(key.as_str()).map_err(redb_err)? {
                Some(g) => decode_lock(g.value())?.holder == *holder,
                None => false,
            };
            if held_by_holder {
                locks.remove(key.as_str()).map_err(redb_err)?;
            }
            CmdResult::Ok
        }
    };
    Ok(result)
}

fn restore_in_transaction(w: &WriteTransaction, dump: &SmDump) -> Result<(), StateMachineError> {
    {
        let mut t = w.open_table(KV).map_err(redb_err)?;
        t.retain(|_, _| false).map_err(redb_err)?;
        for (k, v) in &dump.kv {
            t.insert(k.as_slice(), v.as_slice()).map_err(redb_err)?;
        }
    }
    {
        let mut t = w.open_table(LOCKS).map_err(redb_err)?;
        t.retain(|_, _| false).map_err(redb_err)?;
        for (k, v) in &dump.locks {
            t.insert(k.as_str(), v.as_slice()).map_err(redb_err)?;
        }
    }
    {
        let mut t = w.open_table(META).map_err(redb_err)?;
        t.retain(|_, _| false).map_err(redb_err)?;
        t.insert(FENCING_COUNTER, dump.fencing_counter)
            .map_err(redb_err)?;
    }
    Ok(())
}

/// A redb-backed replicated state machine.
#[derive(Clone)]
pub struct StateMachine {
    db: Arc<Database>,
}

impl StateMachine {
    /// Open (or create) a redb-backed state machine, creating the required tables up front.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateMachineError> {
        let db = Database::create(path).map_err(redb_err)?;
        let w = db.begin_write().map_err(redb_err)?;
        {
            w.open_table(KV).map_err(redb_err)?;
            w.open_table(LOCKS).map_err(redb_err)?;
            w.open_table(META).map_err(redb_err)?;
            w.open_table(RAFT_STATE).map_err(redb_err)?;
        }
        w.commit().map_err(redb_err)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Apply a single command deterministically (called by every node once Raft has committed it).
    pub fn apply(&self, cmd: &Cmd) -> Result<CmdResult, StateMachineError> {
        let w = self.db.begin_write().map_err(redb_err)?;
        let result = apply_in_transaction(&w, cmd)?;
        w.commit().map_err(redb_err)?;
        Ok(result)
    }

    /// Apply a Raft command and persist its state-machine metadata in the same redb transaction.
    pub(crate) fn apply_with_raft_state<T: Serialize>(
        &self,
        cmd: &Cmd,
        key: &str,
        state: &T,
    ) -> Result<CmdResult, StateMachineError> {
        let state = encode(state)?;
        let w = self.db.begin_write().map_err(redb_err)?;
        let result = apply_in_transaction(&w, cmd)?;
        {
            let mut t = w.open_table(RAFT_STATE).map_err(redb_err)?;
            t.insert(key, state.as_slice()).map_err(redb_err)?;
        }
        w.commit().map_err(redb_err)?;
        Ok(result)
    }

    /// Load a value from the internal persistent Raft state table.
    pub(crate) fn raft_state<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, StateMachineError> {
        let r = self.db.begin_read().map_err(redb_err)?;
        let t = r.open_table(RAFT_STATE).map_err(redb_err)?;
        match t.get(key).map_err(redb_err)? {
            Some(value) => Ok(Some(decode(value.value())?)),
            None => Ok(None),
        }
    }

    /// Store a value in the internal persistent Raft state table.
    pub(crate) fn set_raft_state<T: Serialize>(
        &self,
        key: &str,
        state: &T,
    ) -> Result<(), StateMachineError> {
        let state = encode(state)?;
        let w = self.db.begin_write().map_err(redb_err)?;
        {
            let mut t = w.open_table(RAFT_STATE).map_err(redb_err)?;
            t.insert(key, state.as_slice()).map_err(redb_err)?;
        }
        w.commit().map_err(redb_err)?;
        Ok(())
    }

    pub(crate) fn has_business_data(&self) -> Result<bool, StateMachineError> {
        let r = self.db.begin_read().map_err(redb_err)?;
        Ok(r.open_table(KV)
            .map_err(redb_err)?
            .first()
            .map_err(redb_err)?
            .is_some()
            || r.open_table(LOCKS)
                .map_err(redb_err)?
                .first()
                .map_err(redb_err)?
                .is_some()
            || r.open_table(META)
                .map_err(redb_err)?
                .get(FENCING_COUNTER)
                .map_err(redb_err)?
                .is_some())
    }

    /// Read a KV pair (a local read; linearizability is guaranteed by the Raft read-index above).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateMachineError> {
        let r = self.db.begin_read().map_err(redb_err)?;
        let t = r.open_table(KV).map_err(redb_err)?;
        Ok(t.get(key).map_err(redb_err)?.map(|g| g.value().to_vec()))
    }

    /// For snapshots: serialize the entire replicated business state into bytes.
    pub fn dump(&self) -> Result<Vec<u8>, StateMachineError> {
        let r = self.db.begin_read().map_err(redb_err)?;
        let mut kv = Vec::new();
        {
            let t = r.open_table(KV).map_err(redb_err)?;
            for item in t.iter().map_err(redb_err)? {
                let (k, v) = item.map_err(redb_err)?;
                kv.push((k.value().to_vec(), v.value().to_vec()));
            }
        }
        let mut locks = Vec::new();
        {
            let t = r.open_table(LOCKS).map_err(redb_err)?;
            for item in t.iter().map_err(redb_err)? {
                let (k, v) = item.map_err(redb_err)?;
                locks.push((k.value().to_string(), v.value().to_vec()));
            }
        }
        let fencing_counter = r
            .open_table(META)
            .map_err(redb_err)?
            .get(FENCING_COUNTER)
            .map_err(redb_err)?
            .map(|g| g.value())
            .unwrap_or(0);
        encode(&SmDump {
            kv,
            locks,
            fencing_counter,
        })
    }

    /// Restore from a snapshot: clear kv / locks, then load the contents.
    pub fn restore(&self, bytes: &[u8]) -> Result<(), StateMachineError> {
        let dump: SmDump = decode(bytes)?;
        let w = self.db.begin_write().map_err(redb_err)?;
        restore_in_transaction(&w, &dump)?;
        w.commit().map_err(redb_err)?;
        Ok(())
    }

    /// Install a snapshot and its OpenRaft metadata atomically.
    pub(crate) fn restore_with_raft_state<M: Serialize, S: Serialize>(
        &self,
        bytes: &[u8],
        meta_key: &str,
        meta: &M,
        snapshot_key: &str,
        snapshot: &S,
    ) -> Result<(), StateMachineError> {
        let dump: SmDump = decode(bytes)?;
        let meta = encode(meta)?;
        let snapshot = encode(snapshot)?;
        let w = self.db.begin_write().map_err(redb_err)?;
        restore_in_transaction(&w, &dump)?;
        {
            let mut t = w.open_table(RAFT_STATE).map_err(redb_err)?;
            t.insert(meta_key, meta.as_slice()).map_err(redb_err)?;
            t.insert(snapshot_key, snapshot.as_slice())
                .map_err(redb_err)?;
        }
        w.commit().map_err(redb_err)?;
        Ok(())
    }
}

/// The serializable representation of the complete replicated business state.
#[derive(serde::Serialize, serde::Deserialize)]
struct SmDump {
    kv: Vec<(Vec<u8>, Vec<u8>)>,
    locks: Vec<(String, Vec<u8>)>,
    fencing_counter: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sm(dir: &tempfile::TempDir) -> StateMachine {
        StateMachine::open(dir.path().join("sm.redb")).unwrap()
    }

    #[test]
    fn kv_and_cas() {
        let dir = tempfile::tempdir().unwrap();
        let sm = sm(&dir);
        sm.apply(&Cmd::Set {
            key: b"x".to_vec(),
            value: b"1".to_vec(),
        })
        .unwrap();
        assert_eq!(sm.get(b"x").unwrap(), Some(b"1".to_vec()));

        // CAS fails (expect does not match).
        assert_eq!(
            sm.apply(&Cmd::Cas {
                key: b"x".to_vec(),
                expect: Some(b"9".to_vec()),
                value: b"2".to_vec()
            })
            .unwrap(),
            CmdResult::Bool(false)
        );
        // CAS succeeds.
        assert_eq!(
            sm.apply(&Cmd::Cas {
                key: b"x".to_vec(),
                expect: Some(b"1".to_vec()),
                value: b"2".to_vec()
            })
            .unwrap(),
            CmdResult::Bool(true)
        );
        assert_eq!(sm.get(b"x").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn lock_fencing_and_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let sm = sm(&dir);

        // a acquires the lock -> fencing token 1.
        assert_eq!(
            sm.apply(&Cmd::AcquireLock {
                key: "k".into(),
                holder: "a".into(),
                now_ms: 1000,
                ttl_ms: 5000
            })
            .unwrap(),
            CmdResult::Fencing(Some(1))
        );
        // b is rejected while the lock has not expired.
        assert_eq!(
            sm.apply(&Cmd::AcquireLock {
                key: "k".into(),
                holder: "b".into(),
                now_ms: 2000,
                ttl_ms: 5000
            })
            .unwrap(),
            CmdResult::Fencing(None)
        );
        // After expiry b acquires it -> the fencing token increments to 2.
        assert_eq!(
            sm.apply(&Cmd::AcquireLock {
                key: "k".into(),
                holder: "b".into(),
                now_ms: 7000,
                ttl_ms: 5000
            })
            .unwrap(),
            CmdResult::Fencing(Some(2))
        );
        // a's lease renewal fails (it is no longer the holder).
        assert_eq!(
            sm.apply(&Cmd::RenewLock {
                key: "k".into(),
                holder: "a".into(),
                now_ms: 8000,
                ttl_ms: 5000
            })
            .unwrap(),
            CmdResult::Bool(false)
        );
        // b's lease renewal succeeds.
        assert_eq!(
            sm.apply(&Cmd::RenewLock {
                key: "k".into(),
                holder: "b".into(),
                now_ms: 9000,
                ttl_ms: 5000
            })
            .unwrap(),
            CmdResult::Bool(true)
        );
        // Once b releases it, a can acquire it.
        sm.apply(&Cmd::ReleaseLock {
            key: "k".into(),
            holder: "b".into(),
        })
        .unwrap();
        assert_eq!(
            sm.apply(&Cmd::AcquireLock {
                key: "k".into(),
                holder: "a".into(),
                now_ms: 10000,
                ttl_ms: 5000
            })
            .unwrap(),
            CmdResult::Fencing(Some(3))
        );
    }

    #[test]
    fn snapshot_restores_fencing_counter() {
        let dir = tempfile::tempdir().unwrap();
        let source = StateMachine::open(dir.path().join("source.redb")).unwrap();

        assert_eq!(
            source
                .apply(&Cmd::AcquireLock {
                    key: "first".into(),
                    holder: "a".into(),
                    now_ms: 1,
                    ttl_ms: 10,
                })
                .unwrap(),
            CmdResult::Fencing(Some(1))
        );
        source
            .apply(&Cmd::ReleaseLock {
                key: "first".into(),
                holder: "a".into(),
            })
            .unwrap();

        let snapshot = source.dump().unwrap();
        let restored = StateMachine::open(dir.path().join("restored.redb")).unwrap();
        restored.restore(&snapshot).unwrap();

        assert_eq!(
            restored
                .apply(&Cmd::AcquireLock {
                    key: "second".into(),
                    holder: "b".into(),
                    now_ms: 20,
                    ttl_ms: 10,
                })
                .unwrap(),
            CmdResult::Fencing(Some(2))
        );
    }

    #[test]
    fn legacy_snapshot_without_fencing_counter_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let sm = sm(&dir);
        let legacy = rmp_serde::to_vec(&(
            Vec::<(Vec<u8>, Vec<u8>)>::new(),
            Vec::<(String, Vec<u8>)>::new(),
        ))
        .unwrap();

        assert!(sm.restore(&legacy).is_err());
    }
}
