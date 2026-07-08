//! redb 支撑的复制状态机。
//!
//! 命令经 [`apply`](StateMachine::apply) 确定性地改变持久状态。这正是 Raft 将要复制的状态机:
//! 每个节点在日志条目提交后调用 `apply`,得到一致的结果。
//!
//! 表:`kv`(通用 KV)、`locks`(分布式锁)、`meta`(fencing 计数器)。

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};

use crate::cmd::{Cmd, CmdResult, Lock};

const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");
const LOCKS: TableDefinition<&str, &[u8]> = TableDefinition::new("locks");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const FENCING_COUNTER: &str = "fencing_counter";

/// 状态机错误。
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

/// redb 支撑的复制状态机。
#[derive(Clone)]
pub struct StateMachine {
    db: Arc<Database>,
}

impl StateMachine {
    /// 打开(或创建)一个 redb 支撑的状态机,并预建所需的表。
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StateMachineError> {
        let db = Database::create(path).map_err(redb_err)?;
        let w = db.begin_write().map_err(redb_err)?;
        {
            w.open_table(KV).map_err(redb_err)?;
            w.open_table(LOCKS).map_err(redb_err)?;
            w.open_table(META).map_err(redb_err)?;
        }
        w.commit().map_err(redb_err)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// 确定性地应用一条命令(Raft 提交后由每个节点调用)。
    pub fn apply(&self, cmd: &Cmd) -> Result<CmdResult, StateMachineError> {
        let w = self.db.begin_write().map_err(redb_err)?;
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
            Cmd::AcquireLock { key, holder, now_ms, ttl_ms } => {
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
            Cmd::RenewLock { key, holder, now_ms, ttl_ms } => {
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
        w.commit().map_err(redb_err)?;
        Ok(result)
    }

    /// 读取一个 KV(本地读;线性一致由上层 Raft read-index 保证)。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateMachineError> {
        let r = self.db.begin_read().map_err(redb_err)?;
        let t = r.open_table(KV).map_err(redb_err)?;
        Ok(t.get(key).map_err(redb_err)?.map(|g| g.value().to_vec()))
    }

    /// 快照用:把整个业务状态(kv + locks)序列化为字节。
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
        rmp_serde::to_vec(&SmDump { kv, locks }).map_err(|e| StateMachineError::Codec(e.to_string()))
    }

    /// 从快照恢复:清空 kv / locks 并载入。
    pub fn restore(&self, bytes: &[u8]) -> Result<(), StateMachineError> {
        let dump: SmDump =
            rmp_serde::from_slice(bytes).map_err(|e| StateMachineError::Codec(e.to_string()))?;
        let w = self.db.begin_write().map_err(redb_err)?;
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
        w.commit().map_err(redb_err)?;
        Ok(())
    }
}

/// 状态机快照的可序列化表示(kv + locks 全量)。
#[derive(serde::Serialize, serde::Deserialize)]
struct SmDump {
    kv: Vec<(Vec<u8>, Vec<u8>)>,
    locks: Vec<(String, Vec<u8>)>,
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
        sm.apply(&Cmd::Set { key: b"x".to_vec(), value: b"1".to_vec() }).unwrap();
        assert_eq!(sm.get(b"x").unwrap(), Some(b"1".to_vec()));

        // CAS 失败(expect 不匹配)。
        assert_eq!(
            sm.apply(&Cmd::Cas { key: b"x".to_vec(), expect: Some(b"9".to_vec()), value: b"2".to_vec() }).unwrap(),
            CmdResult::Bool(false)
        );
        // CAS 成功。
        assert_eq!(
            sm.apply(&Cmd::Cas { key: b"x".to_vec(), expect: Some(b"1".to_vec()), value: b"2".to_vec() }).unwrap(),
            CmdResult::Bool(true)
        );
        assert_eq!(sm.get(b"x").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn lock_fencing_and_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let sm = sm(&dir);

        // a 获取锁 -> fencing token 1。
        assert_eq!(
            sm.apply(&Cmd::AcquireLock { key: "k".into(), holder: "a".into(), now_ms: 1000, ttl_ms: 5000 }).unwrap(),
            CmdResult::Fencing(Some(1))
        );
        // b 在未过期时被拒。
        assert_eq!(
            sm.apply(&Cmd::AcquireLock { key: "k".into(), holder: "b".into(), now_ms: 2000, ttl_ms: 5000 }).unwrap(),
            CmdResult::Fencing(None)
        );
        // 过期后 b 获取 -> fencing token 递增到 2。
        assert_eq!(
            sm.apply(&Cmd::AcquireLock { key: "k".into(), holder: "b".into(), now_ms: 7000, ttl_ms: 5000 }).unwrap(),
            CmdResult::Fencing(Some(2))
        );
        // a 续租失败(已非持有者)。
        assert_eq!(
            sm.apply(&Cmd::RenewLock { key: "k".into(), holder: "a".into(), now_ms: 8000, ttl_ms: 5000 }).unwrap(),
            CmdResult::Bool(false)
        );
        // b 续租成功。
        assert_eq!(
            sm.apply(&Cmd::RenewLock { key: "k".into(), holder: "b".into(), now_ms: 9000, ttl_ms: 5000 }).unwrap(),
            CmdResult::Bool(true)
        );
        // b 释放后 a 可获取。
        sm.apply(&Cmd::ReleaseLock { key: "k".into(), holder: "b".into() }).unwrap();
        assert_eq!(
            sm.apply(&Cmd::AcquireLock { key: "k".into(), holder: "a".into(), now_ms: 10000, ttl_ms: 5000 }).unwrap(),
            CmdResult::Fencing(Some(3))
        );
    }
}
