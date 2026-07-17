//! openraft type configuration: wires the control plane's [`Cmd`] / [`CmdResult`] into Raft
//! (openraft 0.9.24).
//!
//! The application data Raft replicates (`D`) is [`Cmd`] and the response (`R`) is [`CmdResult`];
//! once a log entry is committed, every node hands the `Cmd` to the redb state machine's
//! [`apply`](crate::state_machine::StateMachine::apply).

use std::io::Cursor;

use crate::cmd::{Cmd, CmdResult};

/// Node identifier.
pub type NodeId = u64;

/// Node information (openraft's built-in `BasicNode` is used here; it carries an address string).
pub type Node = openraft::BasicNode;

/// Snapshot data carrier (the default cursor).
pub type SnapshotData = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    /// Raft type configuration for the mocra control plane.
    pub TypeConfig:
        D = Cmd,
        R = CmdResult,
);

/// Type alias for the Raft instance.
pub type MocraRaft = openraft::Raft<TypeConfig>;

/// Common openraft type aliases (mirrors the `typ` module from the official examples).
pub mod typ {
    use openraft::error::Infallible;

    use super::{Node, NodeId, TypeConfig};

    pub type Entry = openraft::Entry<TypeConfig>;

    pub type RaftError<E = Infallible> = openraft::error::RaftError<NodeId, E>;
    pub type RPCError<E = Infallible> = openraft::error::RPCError<NodeId, Node, RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, Node>;
    pub type InitializeError = openraft::error::InitializeError<NodeId, Node>;
    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}
