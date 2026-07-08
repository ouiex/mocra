//! openraft 类型配置:把控制面的 [`Cmd`] / [`CmdResult`] 接入 Raft(openraft 0.9.24)。
//!
//! Raft 复制的应用数据(`D`)就是我们的 [`Cmd`],响应(`R`)是 [`CmdResult`];
//! 每个节点在日志条目提交后,把 `Cmd` 交给 redb 状态机 [`apply`](crate::state_machine::StateMachine::apply)。
//!
//! # 已完成
//! - openraft 0.9.24 依赖集成;`TypeConfig`(D=Cmd, R=CmdResult,其余用默认:
//!   `NodeId=u64`、`Node=BasicNode`、`Entry=Entry<Self>`、`SnapshotData=Cursor<Vec<u8>>`、
//!   `AsyncRuntime=TokioRuntime`)编译通过。
//!
//! # 待实现(约 500 行精细集成,建议专注增量)
//!
//! 1. **`RaftStateMachine<TypeConfig>`**(在 redb 状态机上实现;签名已核对,0.9 用 `impl Future` 风格):
//!    - `applied_state() -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, Node>), StorageError<NodeId>>`
//!      —— 需在 redb 新增 `raft_meta` 表持久化 `last_applied` 与 `last_membership`。
//!    - `apply<I: IntoIterator<Item = Entry>>(entries) -> Result<Vec<CmdResult>, StorageError>`
//!      —— 逐条:更新 `last_applied = entry.log_id`;`match entry.payload`:
//!         `Blank => CmdResult::Ok`、`Normal(cmd) => sm.apply(cmd)`、`Membership(m) => 更新 last_membership`。
//!    - `type SnapshotBuilder` + 快照四法:`get_snapshot_builder / begin_receiving_snapshot /
//!      install_snapshot / get_current_snapshot` —— 简单实现:快照 = 序列化整个 kv+locks 表。
//! 2. **`RaftLogStorage<TypeConfig>` + `RaftLogReader`**:append / truncate / purge /
//!    get_log_state / save_vote / read_vote / try_get_log_entries —— 日志条目存 redb(或内存起步)。
//! 3. **`RaftNetwork<TypeConfig>` + `RaftNetworkFactory`**:append_entries / vote /
//!    install_snapshot 的节点间 RPC(用主 crate 的 axum,或 tonic)。
//! 4. **节点装配 + join API**:`Raft::new(node_id, config, network, log_store, sm)`;
//!    `POST /cluster/join` → leader `add_learner` + `change_membership`(方案 A:小投票核心 + 多 worker)。
//! 5. **`RaftControlPlane: ControlPlane`**:`client_write(Cmd)` 提交 → 等提交 → 返回 `CmdResult`;
//!    读走 read-index。最后主 crate 侧 `impl CoordinationBackend for RaftControlPlane`,替换 Redis 协调。

use std::io::Cursor;

use crate::cmd::{Cmd, CmdResult};

/// 节点标识。
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// mocra 控制面的 Raft 类型配置。
    pub TypeConfig:
        D = Cmd,
        R = CmdResult,
);
