//! Stub network for single-node deployments.
//!
//! Implements `RaftNetwork` / `RaftNetworkFactory` to satisfy the type requirements of
//! [`Raft::new`](openraft::Raft::new). A cluster with a single voter never issues node-to-node
//! RPCs, so these methods are never called; multi-node RPC (HTTP / gRPC) is a follow-up item
//! (plan A: a small voting core + many workers).

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::raft::{Node, NodeId, TypeConfig};

/// Stub network factory.
#[derive(Clone, Default)]
pub struct StubNetwork;

/// Stub connection.
pub struct StubConnection;

fn stub_err() -> std::io::Error {
    std::io::Error::other(
        "stub network: multi-node RPC not implemented (single-node control plane)",
    )
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
