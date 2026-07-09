//! redb 持久化的 Raft 日志存储。
//!
//! 取代内存日志,使控制面**完全 redb 持久化**(状态机 + 日志)—— 自包含嵌入式集群的关键。
//! 以官方 `raft-kv-rocksdb` 的 `LogStore` 为模板,把 rocksdb 换成 redb。
//!
//! 表:
//! - `raft_logs`: `u64`(日志 index)→ 序列化的 [`Entry`]
//! - `raft_meta`: `&str` → 序列化的 `last_purged` / `committed` / `vote`

// 存储 trait 的错误类型由 openraft 固定为 `StorageError`(~224B),无法在本地装箱;
// 这些 `StorageResult` 助手必须沿用它以对接 trait,故豁免 result_large_err。
#![allow(clippy::result_large_err)]

use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{
    Entry, LogId, OptionalSend, RaftLogReader, StorageError, StorageIOError, Vote,
};
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

/// 把任意可显示错误映射为 openraft 的 `StorageError`。
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

/// redb 持久化日志存储。
#[derive(Clone)]
pub struct RedbLogStore {
    db: Arc<Database>,
}

impl RedbLogStore {
    /// 打开(或创建)一个 redb 支撑的日志存储。
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateMachineIo> {
        let db = Database::create(path).map_err(|e| StateMachineIo(e.to_string()))?;
        let w = db.begin_write().map_err(|e| StateMachineIo(e.to_string()))?;
        {
            w.open_table(LOGS).map_err(|e| StateMachineIo(e.to_string()))?;
            w.open_table(META).map_err(|e| StateMachineIo(e.to_string()))?;
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

/// 打开日志存储时的 IO 错误(启动期用,便于 `?` 到应用错误)。
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
        // redb commit 已落盘 -> 通知 openraft IO 完成。
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        // 删除 [index, +oo):redb 2.x 无 drain,先收集键再逐个删除。
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
        // 删除 [0, index]。
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
