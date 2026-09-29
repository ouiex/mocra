//! Raft storage: in-memory test log + fully persisted redb state machine.
//!
//! - [`LogStore`]: the Raft log (currently an **in-memory** implementation; a redb-persisted log
//!   is a follow-up item).
//! - [`StateMachineStore`]: wires openraft's state machine to the redb [`StateMachine`] — applied
//!   progress, membership, snapshots, and application state are persisted together.

// OpenRaft fixes the storage traits' error type to `StorageError` (~224B), so helper methods used
// by the trait implementations must return it unchanged.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::cmd::CmdResult;
use crate::raft::{Node, NodeId, SnapshotData, TypeConfig};
use crate::state_machine::{StateMachine, StateMachineError};

type StorageResult<T> = Result<T, StorageError<NodeId>>;

const RAFT_META_KEY: &str = "state_machine_meta";
const RAFT_SNAPSHOT_KEY: &str = "current_snapshot";

// ============ Log storage (in-memory) ============

#[derive(Clone, Default)]
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

#[derive(Default)]
struct LogStoreInner {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StorageResult<Vec<Entry<TypeConfig>>> {
        let inner = self.inner.lock().await;
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let inner = self.inner.lock().await;
        let last = inner.log.values().next_back().map(|e| e.log_id);
        let last_log_id = last.or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> StorageResult<()> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self.inner.lock().await.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self.inner.lock().await.vote)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self.inner.lock().await;
            for e in entries {
                inner.log.insert(e.log_id.index, e);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        // Delete [index, +oo): split_off keeps everything < index.
        let mut inner = self.inner.lock().await;
        let _ = inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        // Keep (index, +oo): split_off returns everything >= index+1.
        let mut inner = self.inner.lock().await;
        inner.last_purged = Some(log_id);
        let keep = inner.log.split_off(&(log_id.index + 1));
        inner.log = keep;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

// ============ State machine storage (redb) ============

/// The persistent representation of a state machine snapshot.
#[derive(Serialize, Deserialize, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,
    pub data: Vec<u8>,
}

/// Wires the openraft state machine to the redb [`StateMachine`].
#[derive(Clone)]
pub struct StateMachineStore {
    sm: Arc<StateMachine>,
    operation_lock: Arc<Mutex<()>>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct SmMeta {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, Node>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedSnapshotState {
    current: StoredSnapshot,
    snapshot_idx: u64,
}

impl StateMachineStore {
    pub fn new(sm: Arc<StateMachine>) -> Self {
        Self {
            sm,
            operation_lock: Arc::new(Mutex::new(())),
        }
    }

    pub(crate) fn has_persisted_meta(&self) -> Result<bool, StateMachineError> {
        self.sm
            .raft_state::<SmMeta>(RAFT_META_KEY)
            .map(|meta| meta.is_some())
    }

    pub(crate) fn initialize_persisted_meta(&self) -> Result<(), StateMachineError> {
        self.sm.set_raft_state(RAFT_META_KEY, &SmMeta::default())
    }

    fn load_meta(&self) -> StorageResult<SmMeta> {
        let meta = self
            .sm
            .raft_state(RAFT_META_KEY)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;
        meta.ok_or_else(|| {
            StorageIOError::read_state_machine(&std::io::Error::other(
                "missing persisted Raft state-machine metadata",
            ))
            .into()
        })
    }

    fn store_meta(&self, meta: &SmMeta) -> StorageResult<()> {
        self.sm
            .set_raft_state(RAFT_META_KEY, meta)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        Ok(())
    }

    fn load_snapshot_state(&self) -> StorageResult<Option<PersistedSnapshotState>> {
        self.sm
            .raft_state(RAFT_SNAPSHOT_KEY)
            .map_err(|e| StorageIOError::read_state_machine(&e).into())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let _guard = self.operation_lock.lock().await;
        let sm_meta = self.load_meta()?;
        let last_applied = sm_meta.last_applied;
        let last_membership = sm_meta.last_membership;
        let data = self
            .sm
            .dump()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let idx = self
            .load_snapshot_state()?
            .map(|state| state.snapshot_idx)
            .unwrap_or(0)
            + 1;
        let snapshot_id = match last_applied {
            Some(last) => format!("{}-{}-{}", last.leader_id, last.index, idx),
            None => format!("--{}", idx),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        let current = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        self.sm
            .set_raft_state(
                RAFT_SNAPSHOT_KEY,
                &PersistedSnapshotState {
                    current,
                    snapshot_idx: idx,
                },
            )
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>> {
        let _guard = self.operation_lock.lock().await;
        let meta = self.load_meta()?;
        Ok((meta.last_applied, meta.last_membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CmdResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let _guard = self.operation_lock.lock().await;
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);
        let mut meta = self.load_meta()?;
        for ent in entries {
            meta.last_applied = Some(ent.log_id);
            let reply = match ent.payload {
                EntryPayload::Blank => {
                    self.store_meta(&meta)?;
                    CmdResult::Ok
                }
                EntryPayload::Normal(cmd) => self
                    .sm
                    .apply_with_raft_state(&cmd, RAFT_META_KEY, &meta)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?,
                EntryPayload::Membership(mem) => {
                    meta.last_membership = StoredMembership::new(Some(ent.log_id), mem);
                    self.store_meta(&meta)?;
                    CmdResult::Ok
                }
            };
            replies.push(reply);
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let _guard = self.operation_lock.lock().await;
        let data = snapshot.into_inner();
        let sm_meta = SmMeta {
            last_applied: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
        };
        let snapshot_idx = self
            .load_snapshot_state()?
            .map(|state| state.snapshot_idx)
            .unwrap_or(0)
            + 1;
        let snapshot_state = PersistedSnapshotState {
            current: StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            },
            snapshot_idx,
        };
        self.sm
            .restore_with_raft_state(
                &data,
                RAFT_META_KEY,
                &sm_meta,
                RAFT_SNAPSHOT_KEY,
                &snapshot_state,
            )
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let _guard = self.operation_lock.lock().await;
        let current = self.load_snapshot_state()?.map(|state| state.current);
        Ok(current.map(|snapshot| Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(Cursor::new(snapshot.data)),
        }))
    }
}
