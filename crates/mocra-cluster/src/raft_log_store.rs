//! redb-persisted Raft log storage.
//!
//! Replaces the in-memory log so that the control plane is **fully persisted in redb** (state
//! machine + log) — the key to a self-contained embedded cluster.
//! Modelled on the `LogStore` from the official `raft-kv-rocksdb` example, with rocksdb swapped
//! for redb.
//!
//! Tables:
//! - `raft_logs`: `u64` (log index) → serialized [`Entry`]
//! - `raft_meta`: `&str` → serialized `last_purged` / `committed` / `vote`

// openraft fixes the storage traits' error type to `StorageError` (~224B), which cannot be boxed
// locally; these `StorageResult` helpers must keep using it to match the traits, hence the
// result_large_err exemption.
#![allow(clippy::result_large_err)]

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote};
use redb::{Database, ReadableTable, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::raft::{NodeId, TypeConfig};

const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const KEY_LAST_PURGED: &str = "last_purged";
const KEY_COMMITTED: &str = "committed";
const KEY_VOTE: &str = "vote";

type StorageResult<T> = Result<T, StorageError<NodeId>>;

/// Maps any displayable error into openraft's `StorageError`.
fn sto<E: std::fmt::Display>(e: E) -> StorageError<NodeId> {
    let io = std::io::Error::other(e.to_string());
    StorageIOError::read(&io).into()
}

fn enc<T: Serialize>(v: &T) -> StorageResult<Vec<u8>> {
    rmp_serde::to_vec(v).map_err(sto)
}

fn dec<T: DeserializeOwned>(b: &[u8]) -> StorageResult<T> {
    rmp_serde::from_slice(b).map_err(sto)
}

/// redb-persisted log storage.
#[derive(Clone)]
pub struct RedbLogStore {
    db: Arc<Database>,
}

impl RedbLogStore {
    /// Open (or create) a redb-backed log store.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateMachineIo> {
        let db = Database::create(path).map_err(|e| StateMachineIo(e.to_string()))?;
        let w = db
            .begin_write()
            .map_err(|e| StateMachineIo(e.to_string()))?;
        {
            w.open_table(LOGS)
                .map_err(|e| StateMachineIo(e.to_string()))?;
            w.open_table(META)
                .map_err(|e| StateMachineIo(e.to_string()))?;
        }
        w.commit().map_err(|e| StateMachineIo(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    fn get_meta<T: DeserializeOwned>(&self, key: &str) -> StorageResult<Option<T>> {
        let r = self.db.begin_read().map_err(sto)?;
        let t = r.open_table(META).map_err(sto)?;
        match t.get(key).map_err(sto)? {
            Some(g) => Ok(Some(dec(g.value())?)),
            None => Ok(None),
        }
    }

    fn put_meta<T: Serialize>(&self, key: &str, val: &T) -> StorageResult<()> {
        let bytes = enc(val)?;
        let w = self.db.begin_write().map_err(sto)?;
        {
            let mut t = w.open_table(META).map_err(sto)?;
            t.insert(key, bytes.as_slice()).map_err(sto)?;
        }
        w.commit().map_err(sto)?;
        Ok(())
    }
}

/// An IO error raised while opening the log store (used at startup so it can be `?`-ed into an
/// application error).
#[derive(Debug, thiserror::Error)]
#[error("redb log store: {0}")]
pub struct StateMachineIo(pub String);

impl RaftLogReader<TypeConfig> for RedbLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(x) => *x,
            std::ops::Bound::Excluded(x) => x + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(x) => Some(*x),
            std::ops::Bound::Excluded(x) => Some(x.saturating_sub(1)),
            std::ops::Bound::Unbounded => None,
        };
        let r = self.db.begin_read().map_err(sto)?;
        let t = r.open_table(LOGS).map_err(sto)?;
        let mut out = Vec::new();
        let iter = match end {
            Some(e) => t.range(start..=e).map_err(sto)?,
            None => t.range(start..).map_err(sto)?,
        };
        for item in iter {
            let (_, v) = item.map_err(sto)?;
            out.push(dec::<Entry<TypeConfig>>(v.value())?);
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last = {
            let r = self.db.begin_read().map_err(sto)?;
            let t = r.open_table(LOGS).map_err(sto)?;
            match t.last().map_err(sto)? {
                Some((_, v)) => Some(dec::<Entry<TypeConfig>>(v.value())?.log_id),
                None => None,
            }
        };
        let last_purged: Option<LogId<NodeId>> = self.get_meta(KEY_LAST_PURGED)?;
        let last_log_id = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> StorageResult<()> {
        self.put_meta(KEY_COMMITTED, &committed)
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        self.get_meta(KEY_COMMITTED)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.put_meta(KEY_VOTE, vote)
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        self.get_meta(KEY_VOTE)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let w = self.db.begin_write().map_err(sto)?;
            {
                let mut t = w.open_table(LOGS).map_err(sto)?;
                for e in entries {
                    let bytes = enc(&e)?;
                    t.insert(e.log_id.index, bytes.as_slice()).map_err(sto)?;
                }
            }
            w.commit().map_err(sto)?;
        }
        // The redb commit has already hit disk -> tell openraft the IO is complete.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        // Delete [index, +oo): redb 2.x has no drain, so collect the keys first and remove them
        // one by one.
        let w = self.db.begin_write().map_err(sto)?;
        {
            let mut t = w.open_table(LOGS).map_err(sto)?;
            let keys: Vec<u64> = t
                .range(log_id.index..)
                .map_err(sto)?
                .map(|item| item.map(|(k, _)| k.value()).map_err(sto))
                .collect::<StorageResult<Vec<_>>>()?;
            for k in keys {
                t.remove(k).map_err(sto)?;
            }
        }
        w.commit().map_err(sto)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        self.put_meta(KEY_LAST_PURGED, &Some(log_id))?;
        // Delete [0, index].
        let w = self.db.begin_write().map_err(sto)?;
        {
            let mut t = w.open_table(LOGS).map_err(sto)?;
            let keys: Vec<u64> = t
                .range(..=log_id.index)
                .map_err(sto)?
                .map(|item| item.map(|(k, _)| k.value()).map_err(sto))
                .collect::<StorageResult<Vec<_>>>()?;
            for k in keys {
                t.remove(k).map_err(sto)?;
            }
        }
        w.commit().map_err(sto)?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
