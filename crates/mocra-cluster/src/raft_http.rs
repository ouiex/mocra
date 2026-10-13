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
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::post;
use openraft::BasicNode;
use openraft::error::{
    ClientWriteError, InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::cmd::{Cmd, CmdResult};
use crate::raft::{MocraRaft, Node, NodeId, TypeConfig};

// axum spawns connection tasks separately from the server task. Wake their IO during forced
// shutdown so incomplete headers/bodies cannot keep sockets and Raft handles alive.
pub(crate) struct ShutdownListener {
    pub listener: TcpListener,
    pub shutdown: watch::Receiver<()>,
}

impl axum::serve::Listener for ShutdownListener {
    type Io = ShutdownStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (stream, addr) = axum::serve::Listener::accept(&mut self.listener).await;
        let mut shutdown = self.shutdown.clone();
        (
            ShutdownStream {
                stream: Some(stream),
                shutdown: Box::pin(async move {
                    let _ = shutdown.changed().await;
                }),
            },
            addr,
        )
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

pub(crate) struct ShutdownStream {
    stream: Option<TcpStream>,
    shutdown: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl ShutdownStream {
    fn poll_stream(&mut self, cx: &mut Context<'_>) -> Option<&mut TcpStream> {
        if self.stream.is_some() && self.shutdown.as_mut().poll(cx).is_ready() {
            self.stream.take();
        }
        self.stream.as_mut()
    }
}

impl AsyncRead for ShutdownStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_read(cx, buf),
            None => Poll::Ready(Ok(())),
        }
    }
}

impl AsyncWrite for ShutdownStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_write(cx, buf),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "HTTP server shut down",
            ))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_flush(cx),
            None => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_stream(cx) {
            Some(stream) => Pin::new(stream).poll_shutdown(cx),
            None => Poll::Ready(Ok(())),
        }
    }
}

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
/// A follower redirects to its known leader with HTTP 307, preserving the POST body and the
/// existing msgpack request / success response format.
async fn join(State(raft): State<MocraRaft>, body: Bytes) -> Response {
    let req: JoinRequest = match rmp_serde::from_slice(&body) {
        Ok(r) => r,
        Err(error) => {
            let out: Result<(), String> = Err(format!("decode join: {error}"));
            return rmp_serde::to_vec(&out).unwrap_or_default().into_response();
        }
    };
    let res = raft
        .add_learner(req.node_id, BasicNode::new(req.addr), true)
        .await;
    let out: Result<(), String> = match res {
        Ok(_) => Ok(()),
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(forward))) => {
            if let Some(leader) = forward.leader_node {
                return Redirect::temporary(&format!("http://{}/cluster/join", leader.addr))
                    .into_response();
            }
            Err("join failed: no leader is currently known".to_string())
        }
        Err(error) => Err(error.to_string()),
    };
    rmp_serde::to_vec(&out).unwrap_or_default().into_response()
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
