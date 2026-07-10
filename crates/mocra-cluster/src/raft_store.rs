//! Raft 存储:内存日志 + redb 状态机。
//!
//! - [`LogStore`]:Raft 日志(当前**内存**实现;redb 持久化日志为后续项)。
//! - [`StateMachineStore`]:把 openraft 的状态机接到 redb [`StateMachine`] ——
//!   `apply` 时把 `Cmd` 交给 redb;快照 = dump/restore 整个业务状态。

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::cmd::CmdResult;
use crate::raft::{Node, NodeId, SnapshotData, TypeConfig};
use crate::state_machine::StateMachine;

type StorageResult<T> = Result<T, StorageError<NodeId>>;

// ============ 日志存储(内存) ============

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
        // 删除 [index, +oo):split_off 保留 < index。
        let mut inner = self.inner.lock().await;
        let _ = inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        // 保留 (index, +oo):split_off 返回 >= index+1。
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

// ============ 状态机存储(redb) ============

/// 状态机快照的持久表示。
#[derive(Serialize, Deserialize, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, Node>,
    pub data: Vec<u8>,
}

/// 把 openraft 状态机接到 redb [`StateMachine`]。
#[derive(Clone)]
pub struct StateMachineStore {
    sm: Arc<StateMachine>,
    meta: Arc<Mutex<SmMeta>>,
    current_snapshot: Arc<Mutex<Option<StoredSnapshot>>>,
    snapshot_idx: Arc<AtomicU64>,
}

#[derive(Default, Clone)]
struct SmMeta {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, Node>,
}

impl StateMachineStore {
    pub fn new(sm: Arc<StateMachine>) -> Self {
        Self {
            sm,
            meta: Arc::new(Mutex::new(SmMeta::default())),
            current_snapshot: Arc::new(Mutex::new(None)),
            snapshot_idx: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied, last_membership) = {
            let m = self.meta.lock().await;
            (m.last_applied, m.last_membership.clone())
        };
        let data = self
            .sm
            .dump()
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = match last_applied {
            Some(last) => format!("{}-{}-{}", last.leader_id, last.index, idx),
            None => format!("--{}", idx),
        };
        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };
        *self.current_snapshot.lock().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
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
        let m = self.meta.lock().await;
        Ok((m.last_applied, m.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CmdResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut replies = Vec::with_capacity(entries.size_hint().0);
        let mut meta = self.meta.lock().await;
        for ent in entries {
            meta.last_applied = Some(ent.log_id);
            let reply = match ent.payload {
                EntryPayload::Blank => CmdResult::Ok,
                EntryPayload::Normal(cmd) => self
                    .sm
                    .apply(&cmd)
                    .map_err(|e| StorageIOError::write_state_machine(&e))?,
                EntryPayload::Membership(mem) => {
                    meta.last_membership = StoredMembership::new(Some(ent.log_id), mem);
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
        let data = snapshot.into_inner();
        self.sm
            .restore(&data)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        {
            let mut m = self.meta.lock().await;
            m.last_applied = meta.last_log_id;
            m.last_membership = meta.last_membership.clone();
        }
        *self.current_snapshot.lock().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let cur = self.current_snapshot.lock().await;
        Ok(cur.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
