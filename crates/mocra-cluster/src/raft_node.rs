//! 单节点 Raft 控制面:装配 openraft 节点,命令经 Raft 提交后 apply 到 redb 状态机。
//!
//! 多节点(join / 成员变更 / 网络 RPC)为后续项;此处先跑通单节点共识路径:
//! `client_write(Cmd)` → Raft 复制提交 → `StateMachineStore::apply` → redb。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use openraft::Config;

use crate::cmd::{Cmd, CmdResult};
use crate::control::{ControlError, ControlPlane};
use crate::raft::{MocraRaft, Node, NodeId};
use crate::raft_http::{HttpNetwork, JoinRequest, raft_router};
use crate::raft_log_store::RedbLogStore;
use crate::raft_network::StubNetwork;
use crate::raft_store::StateMachineStore;
use crate::state_machine::StateMachine;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 集群状态快照,由 [`RaftControlPlane::status`] 返回,供运维监控 / 健康检查。
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    /// 本节点 id。
    pub node_id: NodeId,
    /// 本节点当前是否是 leader。
    pub is_leader: bool,
    /// 当前已知 leader(选举中为 `None`)。
    pub current_leader: Option<NodeId>,
    /// 当前 Raft 任期。
    pub term: u64,
    /// 已应用到状态机的最高日志位点(`None` = 尚无)。
    pub last_applied_index: Option<u64>,
    /// 成员总数(投票者 + learner)。
    pub member_count: usize,
    /// 投票核心成员数。
    pub voter_count: usize,
}

/// Raft 时序调参:默认适配局域网;高延迟 / 广域网可放大以避免误判 leader 失联。
#[derive(Debug, Clone)]
pub struct RaftTuning {
    /// leader 心跳间隔(ms)。
    pub heartbeat_ms: u64,
    /// 选举超时下界(ms)。应远大于心跳间隔。
    pub election_timeout_min_ms: u64,
    /// 选举超时上界(ms)。
    pub election_timeout_max_ms: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        // 局域网默认:心跳 250ms,选举 600~1200ms。
        Self {
            heartbeat_ms: 250,
            election_timeout_min_ms: 600,
            election_timeout_max_ms: 1200,
        }
    }
}

/// 基于 openraft 的控制面。
#[derive(Clone)]
pub struct RaftControlPlane {
    node_id: NodeId,
    raft: MocraRaft,
    sm: Arc<StateMachine>,
    /// 共享 HTTP 客户端(复用连接池),用于写转发热路径。
    client: reqwest::Client,
}

impl RaftControlPlane {
    /// 起一个单节点 Raft 控制面。
    ///
    /// `dir` 下放两个 redb 文件:`sm.redb`(状态机)与 `log.redb`(Raft 日志)——
    /// 状态机与日志均持久化,整个控制面自包含、可崩溃恢复。
    pub async fn start_single_node(
        node_id: NodeId,
        dir: impl AsRef<Path>,
    ) -> Result<Self, ControlError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).ok();
        let sm = Arc::new(StateMachine::open(dir.join("sm.redb"))?);

        let config = Config {
            heartbeat_interval: 250,
            election_timeout_min: 299,
            ..Default::default()
        };
        let config = Arc::new(
            config
                .validate()
                .map_err(|e| ControlError::Config(e.to_string()))?,
        );

        let log_store = RedbLogStore::open(dir.join("log.redb"))
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        let sm_store = StateMachineStore::new(sm.clone());
        let raft = MocraRaft::new(node_id, config, StubNetwork, log_store, sm_store)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;

        // 初始化单节点集群(自己为唯一投票者)。已初始化则忽略。
        let mut members = BTreeMap::new();
        members.insert(node_id, Node::default());
        let _ = raft.initialize(members).await;

        Ok(Self {
            node_id,
            raft,
            sm,
            client: reqwest::Client::new(),
        })
    }

    /// 起一个**集群**节点:HTTP 网络 + 自带 HTTP server(暴露 Raft RPC 与 `/cluster/join`)。
    ///
    /// - `dir`:redb 状态机 + 日志目录。
    /// - `http_addr`:本节点绑定并对外的地址(如 `127.0.0.1:7001`),即它在集群里的标识地址。
    pub async fn start_cluster_node(
        node_id: NodeId,
        dir: impl AsRef<Path>,
        http_addr: impl Into<String>,
    ) -> Result<Self, ControlError> {
        Self::start_cluster_node_with(node_id, dir, http_addr, RaftTuning::default()).await
    }

    /// 同 [`start_cluster_node`](Self::start_cluster_node),但可自定义 Raft 时序
    /// ([`RaftTuning`]),用于高延迟 / 广域网集群。
    pub async fn start_cluster_node_with(
        node_id: NodeId,
        dir: impl AsRef<Path>,
        http_addr: impl Into<String>,
        tuning: RaftTuning,
    ) -> Result<Self, ControlError> {
        let http_addr = http_addr.into();
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).ok();
        let sm = Arc::new(StateMachine::open(dir.join("sm.redb"))?);

        let config = Arc::new(
            Config {
                heartbeat_interval: tuning.heartbeat_ms,
                election_timeout_min: tuning.election_timeout_min_ms,
                election_timeout_max: tuning.election_timeout_max_ms,
                ..Default::default()
            }
            .validate()
            .map_err(|e| ControlError::Config(e.to_string()))?,
        );

        let log_store = RedbLogStore::open(dir.join("log.redb"))
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        let sm_store = StateMachineStore::new(sm.clone());
        let raft = MocraRaft::new(node_id, config, HttpNetwork::new(), log_store, sm_store)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;

        // 起 HTTP server 暴露 Raft RPC + join。
        let router = raft_router(raft.clone());
        let listener = tokio::net::TcpListener::bind(&http_addr)
            .await
            .map_err(|e| ControlError::Raft(format!("bind {http_addr}: {e}")))?;
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Ok(Self {
            node_id,
            raft,
            sm,
            client: reqwest::Client::new(),
        })
    }

    /// 初始化一个新集群(把给定 `{id: addr}` 作为初始投票集;通常只在第一个核心节点上调用一次)。
    pub async fn init_cluster(
        &self,
        members: BTreeMap<NodeId, String>,
    ) -> Result<(), ControlError> {
        let m: BTreeMap<NodeId, Node> = members
            .into_iter()
            .map(|(id, addr)| (id, Node::new(addr)))
            .collect();
        self.raft
            .initialize(m)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// 把一个节点加为 learner(方案 A:worker 先作 learner)。
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        addr: impl Into<String>,
    ) -> Result<(), ControlError> {
        self.raft
            .add_learner(node_id, Node::new(addr.into()), true)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// 变更投票成员(方案 A:小投票核心 3~5;learner/worker 不受影响)。
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), ControlError> {
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// 本节点向种子节点发起 join(把自己加进集群,作 learner)。
    /// 「注册任意节点即入网」:`seed_addr` 是集群里任意一个已知节点的地址(应为当前 leader)。
    pub async fn join_cluster(
        seed_addr: &str,
        node_id: NodeId,
        my_addr: &str,
    ) -> Result<(), ControlError> {
        let req = JoinRequest {
            node_id,
            addr: my_addr.to_string(),
        };
        let body = rmp_serde::to_vec(&req).map_err(|e| ControlError::Raft(e.to_string()))?;
        let bytes = reqwest::Client::new()
            .post(format!("http://{seed_addr}/cluster/join"))
            .body(body)
            .send()
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        let res: Result<(), String> =
            rmp_serde::from_slice(&bytes).map_err(|e| ControlError::Raft(e.to_string()))?;
        res.map_err(ControlError::Raft)
    }

    /// 底层 openraft 句柄(成员变更 / 状态查询)。
    pub fn raft(&self) -> &MocraRaft {
        &self.raft
    }

    /// 集群当前配置的所有成员(投票者 + learner)及其地址。
    ///
    /// 基于 Raft membership 配置(强一致),而非实时存活探测 —— 足够支撑
    /// 「按成员数分摊限流」「分区归属」等近似分布式语义。
    pub fn members(&self) -> Vec<(NodeId, String)> {
        let mc = self.raft.metrics().borrow().membership_config.clone();
        mc.membership()
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
    }

    /// 集群成员总数(投票者 + learner)。至少为 1。
    pub fn member_count(&self) -> usize {
        let mc = self.raft.metrics().borrow().membership_config.clone();
        mc.membership().nodes().count().max(1)
    }

    /// 投票核心成员数(方案 A 的小投票集,通常 3~5)。
    pub fn voter_count(&self) -> usize {
        let mc = self.raft.metrics().borrow().membership_config.clone();
        mc.membership().voter_ids().count()
    }

    /// 当前已知 leader(未知 / 选举中返回 `None`)。
    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// 集群状态快照(可观测性 / 运维监控:leader、任期、已应用位点、成员数)。
    pub fn status(&self) -> ClusterStatus {
        let m = self.raft.metrics().borrow().clone();
        let mc = m.membership_config.clone();
        ClusterStatus {
            node_id: self.node_id,
            is_leader: m.current_leader == Some(self.node_id),
            current_leader: m.current_leader,
            term: m.current_term,
            last_applied_index: m.last_applied.map(|l| l.index),
            member_count: mc.membership().nodes().count().max(1),
            voter_count: mc.membership().voter_ids().count(),
        }
    }

    /// 本节点 id。
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// 本节点在当前成员视图下负责的分区(rendezvous 分配,默认分区数)。
    pub fn owned_partitions(&self) -> Vec<u32> {
        let members: Vec<NodeId> = self.members().into_iter().map(|(id, _)| id).collect();
        crate::partition::partitions_owned_by(
            self.node_id,
            &members,
            crate::partition::DEFAULT_PARTITIONS,
        )
    }

    /// 某账号 / 会话键是否归本节点处理(rendezvous 分配,默认分区数)。
    ///
    /// 无需协商:成员来自 Raft 强一致视图,哈希确定性,各节点结论一致。
    pub fn owns_key(&self, key: &str) -> bool {
        let members: Vec<NodeId> = self.members().into_iter().map(|(id, _)| id).collect();
        crate::partition::owns_key(
            self.node_id,
            key,
            &members,
            crate::partition::DEFAULT_PARTITIONS,
        )
    }

    /// 抢占某分区的**归属租约**,返回单调 fencing token。
    ///
    /// 用于成员切换瞬间需要强保证的场景:两个节点视图短暂不一致时,只有持最新
    /// token 的节点能安全处理该分区;陈旧属主的下游写入可凭更小 token 被拒。
    /// 稳态归属由 [`owns_key`](Self::owns_key) 决定,本方法在其上加 Raft 强保证。
    pub async fn acquire_partition(
        &self,
        partition: u32,
        ttl_ms: u64,
    ) -> Result<Option<u64>, ControlError> {
        let key = format!("__part/{partition}");
        self.acquire_lock(&key, &self.node_id.to_string(), ttl_ms)
            .await
    }

    /// 释放分区归属租约(仅当仍由本节点持有时)。
    pub async fn release_partition(&self, partition: u32) -> Result<(), ControlError> {
        let key = format!("__part/{partition}");
        self.release_lock(&key, &self.node_id.to_string()).await
    }

    /// 优雅关闭本节点的 Raft 运行时:停止后台任务并释放存储(含 redb 文件锁)。
    ///
    /// 崩溃恢复 / 重启前应调用它,确保 redb 数据库句柄释放,重开同一目录不再报
    /// 「Database already open」。
    pub async fn shutdown(&self) -> Result<(), ControlError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// 等待本节点成为 leader(单节点极快;多节点选举需一个选举超时窗口)。
    pub async fn wait_leader(&self, timeout: std::time::Duration) -> Result<(), ControlError> {
        self.raft
            .wait(Some(timeout))
            .state(openraft::ServerState::Leader, "wait leader")
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// 把一条命令经 Raft 复制提交,返回状态机应用结果。
    ///
    /// - 本节点是 leader → 直接提交。
    /// - 本节点是 follower 且已知 leader → 把 [`Cmd`] 经 HTTP 转发到 leader 的
    ///   `/cluster/write`(「注册到任意节点即可用」)。
    /// - **选举中、暂无 leader** → 命令确定尚未提交,短暂退避后**重试本地 client_write**
    ///   (只重试这一种确定未提交的情况,故对非幂等命令 CAS / AcquireLock 无双应用风险;
    ///   转发的 HTTP 失败则不重试 —— leader 可能已应用,结果不确定)。
    async fn write(&self, cmd: Cmd) -> Result<CmdResult, ControlError> {
        use openraft::error::{ClientWriteError, RaftError};

        const MAX_ATTEMPTS: u32 = 6;
        for attempt in 0..MAX_ATTEMPTS {
            match self.raft.client_write(cmd.clone()).await {
                Ok(resp) => return Ok(resp.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                    match &f.leader_node {
                        // 已知 leader:转发一次(HTTP 失败即返回,不重试 —— 结果不确定)。
                        Some(node) => {
                            return forward_write_to_leader(&self.client, &node.addr, &cmd).await;
                        }
                        // 选举中无 leader:命令未提交,退避后重试(安全)。
                        None => {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                150 * (attempt as u64 + 1),
                            ))
                            .await;
                        }
                    }
                }
                Err(e) => return Err(ControlError::Raft(e.to_string())),
            }
        }
        Err(ControlError::Raft(
            "write failed: no leader elected after retries".to_string(),
        ))
    }
}

/// 把一条命令转发到 leader 的 `/cluster/write` 端点并解析结果(复用共享客户端)。
async fn forward_write_to_leader(
    client: &reqwest::Client,
    leader_addr: &str,
    cmd: &Cmd,
) -> Result<CmdResult, ControlError> {
    let body = rmp_serde::to_vec(cmd).map_err(|e| ControlError::Raft(e.to_string()))?;
    let bytes = client
        .post(format!("http://{leader_addr}/cluster/write"))
        .body(body)
        .send()
        .await
        .map_err(|e| ControlError::Raft(e.to_string()))?
        .bytes()
        .await
        .map_err(|e| ControlError::Raft(e.to_string()))?;
    let res: Result<CmdResult, String> =
        rmp_serde::from_slice(&bytes).map_err(|e| ControlError::Raft(e.to_string()))?;
    res.map_err(ControlError::Raft)
}

#[async_trait]
impl ControlPlane for RaftControlPlane {
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), ControlError> {
        self.write(Cmd::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        })
        .await?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ControlError> {
        // 本地读(线性一致读可先 ensure_linearizable 再读 —— 后续)。
        Ok(self.sm.get(key)?)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), ControlError> {
        self.write(Cmd::Delete { key: key.to_vec() }).await?;
        Ok(())
    }

    async fn cas(
        &self,
        key: &[u8],
        expect: Option<&[u8]>,
        value: &[u8],
    ) -> Result<bool, ControlError> {
        match self
            .write(Cmd::Cas {
                key: key.to_vec(),
                expect: expect.map(|e| e.to_vec()),
                value: value.to_vec(),
            })
            .await?
        {
            CmdResult::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    async fn acquire_lock(
        &self,
        key: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<Option<u64>, ControlError> {
        match self
            .write(Cmd::AcquireLock {
                key: key.to_string(),
                holder: holder.to_string(),
                now_ms: now_ms(),
                ttl_ms,
            })
            .await?
        {
            CmdResult::Fencing(t) => Ok(t),
            _ => Ok(None),
        }
    }

    async fn renew_lock(&self, key: &str, holder: &str, ttl_ms: u64) -> Result<bool, ControlError> {
        match self
            .write(Cmd::RenewLock {
                key: key.to_string(),
                holder: holder.to_string(),
                now_ms: now_ms(),
                ttl_ms,
            })
            .await?
        {
            CmdResult::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    async fn release_lock(&self, key: &str, holder: &str) -> Result<(), ControlError> {
        self.write(Cmd::ReleaseLock {
            key: key.to_string(),
            holder: holder.to_string(),
        })
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn single_node_raft_applies_commands() {
        let dir = tempfile::tempdir().unwrap();
        let cp = RaftControlPlane::start_single_node(1, dir.path())
            .await
            .unwrap();

        // 单节点很快成为 leader。
        cp.raft()
            .wait(Some(Duration::from_secs(10)))
            .state(openraft::ServerState::Leader, "become leader")
            .await
            .unwrap();

        // set / get 经 Raft。
        cp.set(b"k", b"v").await.unwrap();
        assert_eq!(cp.get(b"k").await.unwrap(), Some(b"v".to_vec()));

        // cas 经 Raft。
        assert!(cp.cas(b"k", Some(b"v"), b"v2").await.unwrap());
        assert_eq!(cp.get(b"k").await.unwrap(), Some(b"v2".to_vec()));

        // 分布式锁 + fencing 经 Raft。
        assert_eq!(cp.acquire_lock("lock", "a", 5000).await.unwrap(), Some(1));
        assert_eq!(cp.acquire_lock("lock", "b", 5000).await.unwrap(), None);

        // 状态快照:单节点自己是 leader、成员 1、已应用位点随写入前进。
        let st = cp.status();
        assert!(st.is_leader);
        assert_eq!(st.node_id, 1);
        assert_eq!(st.current_leader, Some(1));
        assert_eq!(st.member_count, 1);
        assert_eq!(st.voter_count, 1);
        assert!(st.last_applied_index.unwrap_or(0) > 0);
    }

    #[tokio::test]
    async fn single_node_recovers_state_after_restart() {
        // 崩溃恢复:控制面(状态机 + 日志)全 redb 持久化,重开同一目录应恢复已提交状态。
        let dir = tempfile::tempdir().unwrap();

        // 第一次生命周期:写入并提交(client_write 返回即已持久化)。
        {
            let cp = RaftControlPlane::start_single_node(1, dir.path())
                .await
                .unwrap();
            cp.wait_leader(Duration::from_secs(10)).await.unwrap();
            cp.set(b"persist", b"value").await.unwrap();
            assert!(cp.cas(b"persist", Some(b"value"), b"v2").await.unwrap());
            assert_eq!(
                cp.acquire_lock("L", "owner", 60_000).await.unwrap(),
                Some(1)
            );
            // 优雅关闭以释放 redb 句柄;已提交数据落盘。cp 随块结束 drop。
            cp.shutdown().await.unwrap();
        }
        // 让后台任务完全退出、文件锁释放。
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 第二次生命周期:重开同一目录 → 从 redb 恢复(无需重放外部日志)。
        {
            let cp = RaftControlPlane::start_single_node(1, dir.path())
                .await
                .unwrap();
            cp.wait_leader(Duration::from_secs(10)).await.unwrap();
            // KV / CAS 结果恢复。
            assert_eq!(cp.get(b"persist").await.unwrap(), Some(b"v2".to_vec()));
            // 锁状态恢复:同 key 仍被 owner 持有,他人抢不到。
            assert_eq!(cp.acquire_lock("L", "other", 60_000).await.unwrap(), None);
            // 恢复后仍可继续写入(日志可追加、fencing 计数单调延续)。
            cp.set(b"after", b"restart").await.unwrap();
            assert_eq!(cp.get(b"after").await.unwrap(), Some(b"restart".to_vec()));
        }
    }

    #[tokio::test]
    async fn three_node_cluster_replicates_over_http() {
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let d3 = tempfile::tempdir().unwrap();
        let (a1, a2, a3) = ("127.0.0.1:27801", "127.0.0.1:27802", "127.0.0.1:27803");

        let cp1 = RaftControlPlane::start_cluster_node(1, d1.path(), a1)
            .await
            .unwrap();
        let cp2 = RaftControlPlane::start_cluster_node(2, d2.path(), a2)
            .await
            .unwrap();
        let cp3 = RaftControlPlane::start_cluster_node(3, d3.path(), a3)
            .await
            .unwrap();

        // 给 HTTP server 一点起动时间。
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 节点 1 初始化单节点集群 → 成为 leader。
        let mut init = BTreeMap::new();
        init.insert(1u64, a1.to_string());
        cp1.init_cluster(init).await.unwrap();
        cp1.raft()
            .wait(Some(Duration::from_secs(10)))
            .state(openraft::ServerState::Leader, "become leader")
            .await
            .unwrap();

        // 加节点 2、3 为 learner(add_learner 阻塞至追平),再提升为 voter(方案 A 核心)。
        cp1.add_learner(2, a2).await.unwrap();
        cp1.add_learner(3, a3).await.unwrap();
        let voters: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        cp1.change_membership(voters).await.unwrap();

        // 成员 API:三节点全在册且均为投票者;leader 已知。
        assert_eq!(cp1.member_count(), 3);
        assert_eq!(cp1.voter_count(), 3);
        assert_eq!(cp1.current_leader(), Some(1));
        let mut addrs: Vec<String> = cp1.members().into_iter().map(|(_, a)| a).collect();
        addrs.sort();
        assert_eq!(addrs, vec![a1.to_string(), a2.to_string(), a3.to_string()]);

        // 在 leader 写入 → 经 Raft 复制到多数派。
        cp1.set(b"hello", b"world").await.unwrap();

        // follower 本地读应最终一致(轮询等待 apply)。
        for cp in [&cp2, &cp3] {
            let mut ok = false;
            for _ in 0..50 {
                if cp.get(b"hello").await.unwrap() == Some(b"world".to_vec()) {
                    ok = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(ok, "follower did not replicate the write");
        }

        // 分布式锁 + fencing 经 Raft(在 leader 提交)。
        // (锁在 LOCKS 表;KV 已验证复制,锁走同一条 Raft 日志,故不另查 follower。)
        assert_eq!(cp1.acquire_lock("L", "a", 5000).await.unwrap(), Some(1));
        assert_eq!(cp1.acquire_lock("L", "b", 5000).await.unwrap(), None);

        // 等 follower 也看到完整成员视图(成员经 Raft 提交后复制)。
        for cp in [&cp2, &cp3] {
            for _ in 0..50 {
                if cp.member_count() == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(cp.member_count(), 3);
        }

        // 分区归属:三节点各自负责的分区互不重叠且并集覆盖全部(无需协商即一致)。
        let p1: BTreeSet<u32> = cp1.owned_partitions().into_iter().collect();
        let p2: BTreeSet<u32> = cp2.owned_partitions().into_iter().collect();
        let p3: BTreeSet<u32> = cp3.owned_partitions().into_iter().collect();
        assert!(p1.is_disjoint(&p2) && p1.is_disjoint(&p3) && p2.is_disjoint(&p3));
        assert_eq!(
            p1.len() + p2.len() + p3.len(),
            crate::partition::DEFAULT_PARTITIONS as usize
        );
        // 任一账号恰好被一个节点认领(三节点视图一致)。
        let owners = [
            cp1.owns_key("account-42"),
            cp2.owns_key("account-42"),
            cp3.owns_key("account-42"),
        ];
        assert_eq!(owners.iter().filter(|&&b| b).count(), 1);

        // fencing 归属租约:cp1 抢占分区 7 → 拿到 token;cp2 抢同分区被拒;
        // cp1 释放后 cp2 可得,且 token 单调递增(防脑裂)。
        let t1 = cp1.acquire_partition(7, 5000).await.unwrap();
        assert!(t1.is_some());
        assert_eq!(cp2.acquire_partition(7, 5000).await.unwrap(), None);
        cp1.release_partition(7).await.unwrap();
        let t2 = cp2.acquire_partition(7, 5000).await.unwrap();
        assert!(t2 > t1, "fencing token must increase: {t2:?} !> {t1:?}");
    }

    #[tokio::test]
    async fn cluster_handles_voter_removal() {
        // 缩容 / 下线:3 投票节点移除一个 → 集群按新多数派继续提交(quorum 重算)。
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let d3 = tempfile::tempdir().unwrap();
        let (a1, a2, a3) = ("127.0.0.1:27871", "127.0.0.1:27872", "127.0.0.1:27873");

        let cp1 = RaftControlPlane::start_cluster_node(1, d1.path(), a1)
            .await
            .unwrap();
        let cp2 = RaftControlPlane::start_cluster_node(2, d2.path(), a2)
            .await
            .unwrap();
        let cp3 = RaftControlPlane::start_cluster_node(3, d3.path(), a3)
            .await
            .unwrap();
        let _ = &cp3;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut init = BTreeMap::new();
        init.insert(1u64, a1.to_string());
        cp1.init_cluster(init).await.unwrap();
        cp1.wait_leader(Duration::from_secs(10)).await.unwrap();
        cp1.add_learner(2, a2).await.unwrap();
        cp1.add_learner(3, a3).await.unwrap();
        cp1.change_membership([1, 2, 3].into_iter().collect())
            .await
            .unwrap();
        assert_eq!(cp1.member_count(), 3);
        cp1.set(b"before", b"3nodes").await.unwrap();

        // 移除节点 3(缩为 {1,2});新多数派 = 2/2。
        cp1.change_membership([1u64, 2].into_iter().collect())
            .await
            .unwrap();
        assert_eq!(cp1.member_count(), 2);
        assert_eq!(cp1.voter_count(), 2);

        // 缩容后仍可提交,并复制到剩余成员。
        cp1.set(b"after", b"2nodes").await.unwrap();
        let mut ok = false;
        for _ in 0..50 {
            if cp2.get(b"after").await.unwrap() == Some(b"2nodes".to_vec()) {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(ok, "shrunk cluster failed to replicate a new write");
        assert_eq!(cp2.get(b"before").await.unwrap(), Some(b"3nodes".to_vec()));
    }

    #[tokio::test]
    async fn new_node_catches_up_via_snapshot_install() {
        // 日志压缩 + 快照恢复:leader 写入若干条 → 触发快照 → 清除已快照日志 →
        // 新节点加入时日志已被 purge,只能经 **install_snapshot** 追平(而非重放日志)。
        // 验证 RaftSnapshotBuilder → install_snapshot RPC → redb restore 全链路。
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let (a1, a2) = ("127.0.0.1:27861", "127.0.0.1:27862");

        let cp1 = RaftControlPlane::start_cluster_node(1, d1.path(), a1)
            .await
            .unwrap();
        let cp2 = RaftControlPlane::start_cluster_node(2, d2.path(), a2)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut init = BTreeMap::new();
        init.insert(1u64, a1.to_string());
        cp1.init_cluster(init).await.unwrap();
        cp1.wait_leader(Duration::from_secs(10)).await.unwrap();

        // 写入若干条(单节点期间快速提交)。
        for i in 0..30u32 {
            cp1.set(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }

        // 触发快照并等待其构建完成(metrics.snapshot 出现)。
        cp1.raft().trigger().snapshot().await.unwrap();
        let mut snap_idx = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Some(s) = cp1.raft().metrics().borrow().snapshot.clone() {
                snap_idx = Some(s.index);
                break;
            }
        }
        let snap_idx = snap_idx.expect("snapshot should have been built");

        // 清除已快照日志(之后新节点无法靠重放追平,必须走快照安装)。
        cp1.raft().trigger().purge_log(snap_idx).await.unwrap();

        // 新节点加入:add_learner 阻塞至追平 —— 追平只能经 install_snapshot。
        cp1.add_learner(2, a2).await.unwrap();

        // 节点 2 应已具备快照里的全部 KV。
        for i in [0u32, 15, 29] {
            let mut ok = false;
            for _ in 0..50 {
                if cp2.get(format!("k{i}").as_bytes()).await.unwrap() == Some(b"v".to_vec()) {
                    ok = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(ok, "node 2 did not receive k{i} via snapshot install");
        }
    }

    #[tokio::test]
    async fn cluster_rebalances_partitions_as_nodes_join() {
        // 动态自组网:节点逐个加入,分区归属随成员增长重平衡。
        // HRW 关键性质在**活集群**里成立 —— 既有节点的归属集合只缩小,新增节点分走一部分。
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let d3 = tempfile::tempdir().unwrap();
        let (a1, a2, a3) = ("127.0.0.1:27851", "127.0.0.1:27852", "127.0.0.1:27853");

        let cp1 = RaftControlPlane::start_cluster_node(1, d1.path(), a1)
            .await
            .unwrap();
        let cp2 = RaftControlPlane::start_cluster_node(2, d2.path(), a2)
            .await
            .unwrap();
        let cp3 = RaftControlPlane::start_cluster_node(3, d3.path(), a3)
            .await
            .unwrap();
        let _ = (&cp2, &cp3); // 需存活以便 add_learner 复制。
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut init = BTreeMap::new();
        init.insert(1u64, a1.to_string());
        cp1.init_cluster(init).await.unwrap();
        cp1.wait_leader(Duration::from_secs(10)).await.unwrap();

        // 单节点:独占全部分区。
        assert_eq!(cp1.member_count(), 1);
        let set1: BTreeSet<u32> = cp1.owned_partitions().into_iter().collect();
        assert_eq!(set1.len(), crate::partition::DEFAULT_PARTITIONS as usize);

        // 节点 2 入网:归属分裂,节点 1 让出一部分给节点 2(集合真子集)。
        cp1.add_learner(2, a2).await.unwrap();
        assert_eq!(cp1.member_count(), 2);
        let set2: BTreeSet<u32> = cp1.owned_partitions().into_iter().collect();
        assert!(
            set2.is_subset(&set1),
            "node 1's partitions must only shrink"
        );
        assert!(
            set2.len() < set1.len(),
            "node 2 must take a share from node 1"
        );

        // 节点 3 入网:HRW 关键不变量 —— 节点 1 的归属只会失去,**绝不重新获得**分区
        //(set3 ⊆ set2);节点 3 从节点 1/2 各分走一部分。
        cp1.add_learner(3, a3).await.unwrap();
        assert_eq!(cp1.member_count(), 3);
        let set3: BTreeSet<u32> = cp1.owned_partitions().into_iter().collect();
        assert!(
            set3.is_subset(&set2),
            "node 1 must never regain a partition when node 3 joins"
        );
        assert!(
            set3.len() < set1.len(),
            "node 1 sheds partitions overall as cluster grows"
        );
    }

    #[tokio::test]
    async fn cluster_survives_leader_failure() {
        // 3 投票节点;干掉 leader 后,剩余多数派应重新选主并继续提交(写经转发到新 leader)。
        let d1 = tempfile::tempdir().unwrap();
        let d2 = tempfile::tempdir().unwrap();
        let d3 = tempfile::tempdir().unwrap();
        let (a1, a2, a3) = ("127.0.0.1:27821", "127.0.0.1:27822", "127.0.0.1:27823");

        let cp1 = RaftControlPlane::start_cluster_node(1, d1.path(), a1)
            .await
            .unwrap();
        let cp2 = RaftControlPlane::start_cluster_node(2, d2.path(), a2)
            .await
            .unwrap();
        let cp3 = RaftControlPlane::start_cluster_node(3, d3.path(), a3)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut init = BTreeMap::new();
        init.insert(1u64, a1.to_string());
        cp1.init_cluster(init).await.unwrap();
        cp1.wait_leader(Duration::from_secs(10)).await.unwrap();
        cp1.add_learner(2, a2).await.unwrap();
        cp1.add_learner(3, a3).await.unwrap();
        let voters: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        cp1.change_membership(voters).await.unwrap();

        // 崩溃前写入,经 Raft 提交。
        cp1.set(b"before", b"crash").await.unwrap();

        // 干掉 leader(节点 1)。
        cp1.shutdown().await.unwrap();

        // 剩余两节点应在若干个选举超时窗口内选出新 leader(2 或 3)。
        let mut new_leader = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let l2 = cp2.current_leader();
            let l3 = cp3.current_leader();
            // 新 leader 必须是存活节点之一,且两存活节点看法一致。
            if let Some(l) = l2 {
                if l != 1 && Some(l) == l3 {
                    new_leader = Some(l);
                    break;
                }
            }
        }
        let leader = new_leader.expect("cluster failed to elect a new leader after leader crash");
        assert!(leader == 2 || leader == 3, "unexpected new leader {leader}");

        // 经存活节点写入(非 leader 会转发到新 leader);选举抖动期重试直至成功。
        let mut wrote = false;
        for _ in 0..100 {
            if cp2.set(b"after", b"failover").await.is_ok() {
                wrote = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            wrote,
            "surviving majority could not commit a write after failover"
        );

        // 另一存活节点最终读到新值(经 Raft 复制),且崩溃前的数据仍在。
        let mut replicated = false;
        for _ in 0..50 {
            if cp3.get(b"after").await.unwrap() == Some(b"failover".to_vec()) {
                replicated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            replicated,
            "post-failover write did not replicate to the other survivor"
        );
        assert_eq!(cp3.get(b"before").await.unwrap(), Some(b"crash".to_vec()));
    }
}
