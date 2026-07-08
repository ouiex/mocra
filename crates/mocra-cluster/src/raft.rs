//! openraft 类型配置:把控制面的 [`Cmd`] / [`CmdResult`] 接入 Raft(openraft 0.9.24)。
//!
//! Raft 复制的应用数据(`D`)就是 [`Cmd`],响应(`R`)是 [`CmdResult`];
//! 每个节点在日志条目提交后,把 `Cmd` 交给 redb 状态机 [`apply`](crate::state_machine::StateMachine::apply)。

use std::io::Cursor;

use crate::cmd::{Cmd, CmdResult};

/// 节点标识。
pub type NodeId = u64;

/// 节点信息(此处用 openraft 内置的 `BasicNode`,携带一个地址字符串)。
pub type Node = openraft::BasicNode;

/// 快照数据载体(默认游标)。
pub type SnapshotData = Cursor<Vec<u8>>;

openraft::declare_raft_types!(
    /// mocra 控制面的 Raft 类型配置。
    pub TypeConfig:
        D = Cmd,
        R = CmdResult,
);

/// Raft 实例类型别名。
pub type MocraRaft = openraft::Raft<TypeConfig>;

/// 常用 openraft 类型别名(仿官方示例的 `typ` 模块)。
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
