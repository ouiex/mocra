//! 节点间控制面 RPC:HTTP + msgpack。
//!
//! - **服务端**:[`raft_router`] 把 openraft 的 append_entries / vote / install_snapshot
//!   暴露为 HTTP 端点。
//! - **客户端**:[`HttpNetwork`] / [`HttpConnection`] 实现 `RaftNetworkFactory` / `RaftNetwork`,
//!   用 reqwest 向对端 [`BasicNode`] 的 `addr` 发起 RPC。
//!
//! 控制面流量小(成员 / 锁 / 配置 / 心跳),HTTP/msgpack 足够且自包含(无需 protobuf codegen)。

use axum::body::Bytes;
use axum::extract::State;
use axum::routing::post;
use axum::Router;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::cmd::{Cmd, CmdResult};
use crate::raft::{MocraRaft, Node, NodeId, TypeConfig};

/// join 请求体:新节点携带自己的 id 与 HTTP 地址。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: NodeId,
    pub addr: String,
}

// ============ 服务端 ============

/// 把一个 Raft 实例的 RPC + 集群管理暴露为 HTTP 路由。挂到节点自己的 HTTP server 上。
pub fn raft_router(raft: MocraRaft) -> Router {
    Router::new()
        .route("/raft/append", post(append))
        .route("/raft/vote", post(vote))
        .route("/raft/snapshot", post(snapshot))
        .route("/cluster/join", post(join))
        .route("/cluster/write", post(cluster_write))
        .with_state(raft)
}

/// 客户端写转发端点:任意节点收到写请求后,若自己不是 leader,其 `client_write`
/// 会失败;follower 侧的 [`RaftControlPlane::write`](crate::RaftControlPlane) 会把
/// [`Cmd`] 转发到 leader 的本端点。leader 在此本地提交并回 [`CmdResult`]。
/// 这样「注册到任意节点」的写请求都能落地。
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

/// 集群 join:把新节点加为 **learner**(方案 A:worker 先作 learner,提升为 voter 是显式操作)。
/// 应打到 leader;非 leader 会返回 openraft 的 ForwardToLeader 错误(序列化在结果里)。
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

// ============ 客户端 ============

/// RaftNetwork 工厂:持有一个共享的 reqwest 客户端。
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

/// 到某个对端节点的连接。
pub struct HttpConnection {
    client: reqwest::Client,
    target: NodeId,
    addr: String,
}

impl HttpConnection {
    /// POST 原始字节到对端某端点,返回响应字节(仅产生网络错误)。
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
        let bytes = self.post("/raft/append", body).await.map_err(RPCError::Network)?;
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
        let bytes = self.post("/raft/vote", body).await.map_err(RPCError::Network)?;
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
