//! mocra-cluster: the embedded control plane (refactor Phase 3).
//!
//! The **control plane** uses a redb state machine + Raft consensus to provide strongly consistent
//! membership / locks / KV / partition ownership; the **data plane** (queues) still goes through
//! the main crate's pluggable `MqBackend`.
//!
//! # Current progress
//! - ✅ redb replicated state machine ([`StateMachine`]): `kv` / `locks` (with monotonic fencing
//!   tokens).
//! - ✅ Single-node control plane ([`LocalControlPlane`]):
//!   `set/get/cas/acquire_lock/renew_lock/release_lock`.
//! - ✅ openraft consensus layered on top of the state machine ([`RaftControlPlane`]): persistent
//!   log + snapshots.
//! - ✅ Membership / join API + partition ownership ([`partition`]: rendezvous assignment + Raft
//!   fencing leases).
//! - ✅ On the main crate side, `RaftCoordinationBackend` adapts `CoordinationBackend` to provide
//!   cluster coordination.

pub mod cmd;
pub mod control;
pub mod partition;
pub mod raft;
pub mod raft_http;
pub mod raft_log_store;
pub mod raft_network;
pub mod raft_node;
pub mod raft_store;
pub mod state_machine;

pub use cmd::{Cmd, CmdResult, Lock};
pub use control::{ControlError, ControlPlane, LocalControlPlane};
pub use partition::{
    DEFAULT_PARTITIONS, owner_of_partition, owns_key, partition_of, partitions_owned_by,
};
pub use raft::{MocraRaft, Node, NodeId, TypeConfig};
pub use raft_http::JoinRequest;
pub use raft_node::{ClusterStatus, RaftControlPlane, RaftTuning};
pub use state_machine::{StateMachine, StateMachineError};
