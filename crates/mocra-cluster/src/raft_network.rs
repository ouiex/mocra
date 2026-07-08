//! 单节点占位网络。
//!
//! 实现 `RaftNetwork` / `RaftNetworkFactory` 以满足 [`Raft::new`](openraft::Raft::new) 的类型要求。
//! 单一投票者的集群从不发起节点间 RPC,故这些方法不会被调用;
//! 多节点 RPC(HTTP / gRPC)是后续项(方案 A:小投票核心 + 多 worker)。

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::raft::{Node, NodeId, TypeConfig};

/// 占位网络工厂。
#[derive(Clone, Default)]
pub struct StubNetwork;

/// 占位连接。
pub struct StubConnection;

fn stub_err() -> std::io::Error {
    std::io::Error::other("stub network: multi-node RPC not implemented (single-node control plane)")
}

impl RaftNetworkFactory<TypeConfig> for StubNetwork {
    type Network = StubConnection;

    async fn new_client(&mut self, _target: NodeId, _node: &Node) -> Self::Network {
        StubConnection
    }
}

impl RaftNetwork<TypeConfig> for StubConnection {
    async fn append_entries(
        &mut self,
        _req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        Err(RPCError::Network(NetworkError::new(&stub_err())))
    }

    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(RPCError::Network(NetworkError::new(&stub_err())))
    }

    async fn vote(
        &mut self,
        _req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        Err(RPCError::Network(NetworkError::new(&stub_err())))
    }
}
