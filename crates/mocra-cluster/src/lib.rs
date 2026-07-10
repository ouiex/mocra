//! mocra-cluster:内嵌控制面(重构 Phase 3)。
//!
//! **控制面**用 redb 状态机 + Raft 共识提供强一致的成员 / 锁 / KV / 分区归属;
//! **数据面**(队列)仍走主 crate 可插拔的 `MqBackend`。
//! 设计见 `docs/refactor/03-cluster-architecture.md`。
//!
//! # 当前进度
//! - ✅ redb 复制状态机([`StateMachine`]):`kv` / `locks`(含单调 fencing token)。
//! - ✅ 单节点控制面([`LocalControlPlane`]):`set/get/cas/acquire_lock/renew_lock/release_lock`。
//! - ✅ openraft 共识分层于状态机之上([`RaftControlPlane`]):持久化日志 + 快照。
//! - ✅ 成员 / join API + 分区归属([`partition`]:rendezvous 分配 + Raft fencing 租约)。
//! - ✅ 主 crate 侧 `RaftCoordinationBackend` 适配 `CoordinationBackend`,替换 Redis 协调。

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
    owner_of_partition, owns_key, partition_of, partitions_owned_by, DEFAULT_PARTITIONS,
};
pub use raft::{MocraRaft, Node, NodeId, TypeConfig};
pub use raft_http::JoinRequest;
pub use raft_node::{ClusterStatus, RaftControlPlane, RaftTuning};
pub use state_machine::{StateMachine, StateMachineError};
