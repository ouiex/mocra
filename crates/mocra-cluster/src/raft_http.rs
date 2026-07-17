//! Node-to-node control plane RPC: HTTP + msgpack.
//!
//! - **Server**: [`raft_router`] exposes openraft's append_entries / vote / install_snapshot as
//!   HTTP endpoints.
//! - **Client**: [`HttpNetwork`] / [`HttpConnection`] implement `RaftNetworkFactory` /
//!   `RaftNetwork`, issuing RPCs with reqwest to the `addr` of the peer [`BasicNode`].
//!
//! Control plane traffic is small (membership / locks / configuration / heartbeats), so
//! HTTP/msgpack is sufficient and self-contained (no protobuf codegen required).

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::routing::post;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::cmd::{Cmd, CmdResult};
use crate::raft::{MocraRaft, Node, NodeId, TypeConfig};

/// The join request body: a new node carries its own id and HTTP address.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub addr: String,
}

// ============ Server ============

/// Exposes a Raft instance's RPC + cluster management as HTTP routes. Mounted on the node's own
/// HTTP server.
pub fn raft_router(raft: MocraRaft) -> Router {
    Router::new()
        .route("/raft/append", post(append))
        .route("/raft/vote", post(vote))
        .route("/raft/snapshot", post(snapshot))
        .route("/cluster/join", post(join))
        .route("/cluster/write", post(cluster_write))
        .with_state(raft)
}

/// The client write-forwarding endpoint: when any node receives a write request, its
/// `client_write` fails if that node is not the leader; on the follower side,
/// [`RaftControlPlane::write`](crate::RaftControlPlane) forwards the [`Cmd`] to this endpoint on
/// the leader. The leader commits it locally here and returns a [`CmdResult`].
/// This is what makes "register against any node" writes land.
async fn cluster_write(State(raft): State<MocraRaft>, body: Bytes) -> Vec<u8> {
    let cmd: Cmd = match rmp_serde::from_slice(&body) {
        Ok(c) => c,
        Err(e) => {
            let out: Result<CmdResult, String> = Err(format!("decode cmd: {e}"));
            return rmp_serde::to_vec(&out).unwrap_or_default();
        }
    };
    let out: Result<CmdResult, String> = raft
        .client_write(cmd)
        .await
        .map(|r| r.data)
        .map_err(|e| e.to_string());
    rmp_serde::to_vec(&out).unwrap_or_default()
}

/// Cluster join: adds the new node as a **learner** (plan A: a worker starts out as a learner;
/// promoting it to a voter is an explicit operation).
/// Should be sent to the leader; a non-leader returns openraft's ForwardToLeader error
/// (serialized into the result).
async fn join(State(raft): State<MocraRaft>, body: Bytes) -> Vec<u8> {
    let req: JoinRequest = match rmp_serde::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let res = raft
        .add_learner(req.node_id, BasicNode::new(req.addr), true)
        .await;
    let out: Result<(), String> = res.map(|_| ()).map_err(|e| e.to_string());
    rmp_serde::to_vec(&out).unwrap_or_default()
}

async fn append(State(raft): State<MocraRaft>, body: Bytes) -> Vec<u8> {
    let req: AppendEntriesRequest<TypeConfig> = match rmp_serde::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let res = raft.append_entries(req).await;
    rmp_serde::to_vec(&res).unwrap_or_default()
}

async fn vote(State(raft): State<MocraRaft>, body: Bytes) -> Vec<u8> {
    let req: VoteRequest<NodeId> = match rmp_serde::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let res = raft.vote(req).await;
    rmp_serde::to_vec(&res).unwrap_or_default()
}

async fn snapshot(State(raft): State<MocraRaft>, body: Bytes) -> Vec<u8> {
    let req: InstallSnapshotRequest<TypeConfig> = match rmp_serde::from_slice(&body) {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    let res = raft.install_snapshot(req).await;
    rmp_serde::to_vec(&res).unwrap_or_default()
}

// ============ Client ============

/// RaftNetwork factory: holds one shared reqwest client.
#[derive(Clone)]
pub struct HttpNetwork {
    client: reqwest::Client,
}

impl HttpNetwork {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for HttpNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// A connection to a peer node.
pub struct HttpConnection {
    client: reqwest::Client,
    target: NodeId,
    addr: String,
}

impl HttpConnection {
    /// POSTs raw bytes to one of the peer's endpoints and returns the response bytes (only
    /// network errors are produced).
    async fn post(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, NetworkError> {
        let url = format!("http://{}{}", self.addr, path);
        let resp = self
            .client
            .post(url)
            .body(body)
            .send()
            .await
            .map_err(|e| NetworkError::new(&e))?;
        let bytes = resp.bytes().await.map_err(|e| NetworkError::new(&e))?;
        Ok(bytes.to_vec())
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetwork {
    type Network = HttpConnection;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        HttpConnection {
            client: self.client.clone(),
            target,
            addr: node.addr.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for HttpConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let body = rmp_serde::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let bytes = self
            .post("/raft/append", body)
            .await
            .map_err(RPCError::Network)?;
        let res: Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>> =
            rmp_serde::from_slice(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let body = rmp_serde::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let bytes = self
            .post("/raft/vote", body)
            .await
            .map_err(RPCError::Network)?;
        let res: Result<VoteResponse<NodeId>, RaftError<NodeId>> =
            rmp_serde::from_slice(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let body = rmp_serde::to_vec(&req).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let bytes = self
            .post("/raft/snapshot", body)
            .await
            .map_err(RPCError::Network)?;
        let res: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>> =
            rmp_serde::from_slice(&bytes).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        res.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
