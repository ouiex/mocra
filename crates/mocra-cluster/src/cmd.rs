//! State machine commands and results.
//!
//! `Cmd` is **serializable**: it will be replicated to a majority through the Raft log and then
//! applied deterministically by every node. That is why time (`now_ms`) is carried inside the
//! command by the caller rather than read from the clock inside `apply` — that is what keeps
//! `apply` deterministic.

use serde::{Deserialize, Serialize};

/// A replicated state machine command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Cmd {
    /// Write a key/value pair.
    Set { key: Vec<u8>, value: Vec<u8> },
    /// Delete a key.
    Delete { key: Vec<u8> },
    /// Compare-and-swap: write `value` only if the current value equals `expect`. Returns whether
    /// the swap succeeded.
    Cas {
        key: Vec<u8>,
        expect: Option<Vec<u8>>,
        value: Vec<u8>,
    },
    /// Acquire a lock: granted if it is free, expired, or already held by the same holder; returns
    /// a monotonically increasing fencing token.
    AcquireLock {
        key: String,
        holder: String,
        now_ms: u64,
        ttl_ms: u64,
    },
    /// Lease renewal: extend the deadline if the caller still holds the lock and it has not expired.
    RenewLock {
        key: String,
        holder: String,
        now_ms: u64,
        ttl_ms: u64,
    },
    /// Release a lock (only the holder may release it).
    ReleaseLock { key: String, holder: String },
}

/// The result of an `apply`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CmdResult {
    /// Success with no return value (set/delete/release).
    Ok,
    /// Whether a cas / renew succeeded.
    Bool(bool),
    /// The fencing token from acquire_lock (`None` = the lock was not acquired).
    Fencing(Option<u64>),
}

/// A lock record (stored in the state machine). The fencing token increases monotonically and
/// prevents an expired holder from processing the same work again after coming back to life.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lock {
    pub holder: String,
    pub expire_at_ms: u64,
    pub fencing_token: u64,
}
