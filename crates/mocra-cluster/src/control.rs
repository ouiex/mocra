//! Control plane API: strongly consistent KV + distributed locks on top of the state machine.
//!
//! [`LocalControlPlane`] is the **single-node** implementation (commands are applied directly to
//! the local state machine, with no consensus). In a distributed deployment it is replaced by
//! `RaftControlPlane` — commands are first replicated to a majority through Raft and only then
//! applied (a follow-up increment).
//!
//! This trait's shape mirrors the main crate's `CoordinationBackend`; the adapter between the two
//! lives on the `mocra` side, so the embedded control plane can be used for coordination.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::cmd::{Cmd, CmdResult};
use crate::state_machine::{StateMachine, StateMachineError};

/// Control plane errors: state machine errors plus consensus / configuration errors.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error(transparent)]
    StateMachine(#[from] StateMachineError),
    #[error("raft: {0}")]
    Raft(String),
    #[error("config: {0}")]
    Config(String),
}

/// Control plane: strongly consistent KV + distributed locks (+ membership/ownership later).
#[async_trait]
pub trait ControlPlane: Send + Sync {
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), ControlError>;
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ControlError>;
    async fn delete(&self, key: &[u8]) -> Result<(), ControlError>;
    /// Compare-and-swap; returns whether it succeeded.
    async fn cas(
        &self,
        key: &[u8],
        expect: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, ControlError>;
    /// Acquire a lock; returns a fencing token on success.
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

/// Single-node control plane: commands are applied directly to the local redb state machine
/// (no consensus).
#[derive(Clone)]
pub struct LocalControlPlane {
    sm: Arc<StateMachine>,
}

impl LocalControlPlane {
    pub fn new(sm: Arc<StateMachine>) -> Self {
        Self { sm }
    }

    /// Open a redb-backed single-node control plane.
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
