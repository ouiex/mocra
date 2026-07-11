//! 控制面 API:在状态机之上提供强一致的 KV + 分布式锁。
//!
//! [`LocalControlPlane`] 是**单节点**实现(命令直接 apply 到本地状态机,无共识)。
//! 分布式部署时由 `RaftControlPlane` 替换 —— 命令先经 Raft 复制到多数派再 apply(后续增量)。
//!
//! 该 trait 的形状对应主 crate 的 `CoordinationBackend`;两者的适配在 `mocra` 侧完成,
//! 从而用内嵌控制面做协调。

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::cmd::{Cmd, CmdResult};
use crate::state_machine::{StateMachine, StateMachineError};

/// 控制面错误:状态机错误 + 共识 / 配置错误。
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error(transparent)]
    StateMachine(#[from] StateMachineError),
    #[error("raft: {0}")]
    Raft(String),
    #[error("config: {0}")]
    Config(String),
}

/// 控制面:强一致的 KV + 分布式锁(+ 将来的成员/归属)。
#[async_trait]
pub trait ControlPlane: Send + Sync {
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), ControlError>;
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ControlError>;
    async fn delete(&self, key: &[u8]) -> Result<(), ControlError>;
    /// 比较并交换,返回是否成功。
    async fn cas(
        &self,
        key: &[u8],
        expect: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, ControlError>;
    /// 获取锁,成功返回 fencing token。
    async fn acquire_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<Option<u64>, ControlError>;
    async fn renew_lock(&self, key: &str, holder: &str, ttl_ms: u64) -> Result<bool, ControlError>;
    async fn release_lock(&self, key: &str, holder: &str) -> Result<(), ControlError>;
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 单节点控制面:命令直接 apply 到本地 redb 状态机(无共识)。
#[derive(Clone)]
pub struct LocalControlPlane {
    sm: Arc<StateMachine>,
}

impl LocalControlPlane {
    pub fn new(sm: Arc<StateMachine>) -> Self {
        Self { sm }
    }

    /// 打开一个 redb 支撑的单节点控制面。
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ControlError> {
        Ok(Self {
            sm: Arc::new(StateMachine::open(path)?),
        })
    }
}

#[async_trait]
impl ControlPlane for LocalControlPlane {
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), ControlError> {
        self.sm.apply(&Cmd::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        })?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ControlError> {
        Ok(self.sm.get(key)?)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), ControlError> {
        self.sm.apply(&Cmd::Delete { key: key.to_vec() })?;
        Ok(())
    }

    async fn cas(
        &self,
        key: &[u8],
        expect: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, ControlError> {
        match self.sm.apply(&Cmd::Cas {
            key: key.to_vec(),
            expect: expect.map(|e| e.to_vec()),
            value: value.to_vec(),
        })? {
            CmdResult::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    async fn acquire_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<Option<u64>, ControlError> {
        match self.sm.apply(&Cmd::AcquireLock {
            key: key.to_string(),
            holder: holder.to_string(),
            now_ms: now_ms(),
            ttl_ms,
        })? {
            CmdResult::Fencing(t) => Ok(t),
            _ => Ok(None),
        }
    }

    async fn renew_lock(&self, key: &str, holder: &str, ttl_ms: u64) -> Result<bool, ControlError> {
        match self.sm.apply(&Cmd::RenewLock {
            key: key.to_string(),
            holder: holder.to_string(),
            now_ms: now_ms(),
            ttl_ms,
        })? {
            CmdResult::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    async fn release_lock(&self, key: &str, holder: &str) -> Result<(), ControlError> {
        self.sm.apply(&Cmd::ReleaseLock {
            key: key.to_string(),
            holder: holder.to_string(),
        })?;
        Ok(())
    }
}
