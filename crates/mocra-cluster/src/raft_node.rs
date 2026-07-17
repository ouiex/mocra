//! Single-node Raft control plane: assembles the openraft node; commands are committed through
//! Raft and then applied to the redb state machine.
//!
//! Multi-node support (join / membership changes / network RPC) is a follow-up item; this gets the
//! single-node consensus path working first:
//! `client_write(Cmd)` → Raft replicates and commits → `StateMachineStore::apply` → redb.

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

/// A cluster status snapshot, returned by [`RaftControlPlane::status`] for operational monitoring
/// / health checks.
#[derive(Debug, Clone)]
pub struct ClusterStatus {
    /// This node's id.
    pub node_id: NodeId,
    /// Whether this node is currently the leader.
    pub is_leader: bool,
    /// The currently known leader (`None` during a leader election).
    pub current_leader: Option<NodeId>,
    /// The current Raft term.
    pub term: u64,
    /// The highest log position applied to the state machine (`None` = nothing yet).
    pub last_applied_index: Option<u64>,
    /// Total number of members (voters + learners).
    pub member_count: usize,
    /// The number of members in the voting core.
    pub voter_count: usize,
}

/// Raft timing knobs: the defaults suit a LAN; on high-latency / wide-area networks, scale them up
/// to avoid falsely concluding that the leader is unreachable.
#[derive(Debug, Clone)]
pub struct RaftTuning {
    /// Leader heartbeat interval (ms).
    pub heartbeat_ms: u64,
    /// Lower bound of the election timeout (ms). Should be far larger than the heartbeat interval.
    pub election_timeout_min_ms: u64,
    /// Upper bound of the election timeout (ms).
    pub election_timeout_max_ms: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        // LAN defaults: 250ms heartbeat, 600~1200ms election.
        Self {
            heartbeat_ms: 250,
            election_timeout_min_ms: 600,
            election_timeout_max_ms: 1200,
        }
    }
}

/// An openraft-based control plane.
#[derive(Clone)]
pub struct RaftControlPlane {
    node_id: NodeId,
    raft: MocraRaft,
    sm: Arc<StateMachine>,
    /// A shared HTTP client (reusing the connection pool) for the write-forwarding hot path.
    client: reqwest::Client,
}

impl RaftControlPlane {
    /// Starts a single-node Raft control plane.
    ///
    /// Two redb files live under `dir`: `sm.redb` (the state machine) and `log.redb` (the Raft
    /// log) — both the state machine and the log are persisted, so the whole control plane is
    /// self-contained and recoverable after a crash.
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

        // Initialize the single-node cluster (this node is the only voter). Ignored if it was
        // already initialized.
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

    /// Starts a **cluster** node: the HTTP network plus its own HTTP server (exposing the Raft RPC
    /// and `/cluster/join`).
    ///
    /// - `dir`: the directory for the redb state machine + log.
    /// - `http_addr`: the address this node binds and advertises (e.g. `127.0.0.1:7001`), i.e. the
    ///   address that identifies it within the cluster.
    pub async fn start_cluster_node(
        node_id: NodeId,
        dir: impl AsRef<Path>,
        http_addr: impl Into<String>,
    ) -> Result<Self, ControlError> {
        Self::start_cluster_node_with(node_id, dir, http_addr, RaftTuning::default()).await
    }

    /// Same as [`start_cluster_node`](Self::start_cluster_node), but with customizable Raft timing
    /// ([`RaftTuning`]) for high-latency / wide-area clusters.
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

        // Start the HTTP server exposing the Raft RPC + join.
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

    /// Initializes a new cluster (the given `{id: addr}` becomes the initial voting set; normally
    /// called exactly once, on the first core node).
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

    /// Adds a node as a learner (plan A: a worker starts out as a learner).
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

    /// Changes the voting membership (plan A: a small voting core of 3~5; learners/workers are
    /// unaffected).
    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), ControlError> {
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Sends a join request from this node to a seed node (adding itself to the cluster as a
    /// learner).
    /// "Register against any node to join": `seed_addr` is the address of any known node in the
    /// cluster (it should be the current leader).
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

    /// The underlying openraft handle (membership changes / status queries).
    pub fn raft(&self) -> &MocraRaft {
        &self.raft
    }

    /// All members currently configured in the cluster (voters + learners) and their addresses.
    ///
    /// Based on the Raft membership configuration (strongly consistent) rather than live liveness
    /// probing — good enough for approximate distributed semantics such as "spread rate limits
    /// across the member count" and "partition ownership".
    pub fn members(&self) -> Vec<(NodeId, String)> {
        let mc = self.raft.metrics().borrow().membership_config.clone();
        mc.membership()
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
    }

    /// Total number of cluster members (voters + learners). At least 1.
    pub fn member_count(&self) -> usize {
        let mc = self.raft.metrics().borrow().membership_config.clone();
        mc.membership().nodes().count().max(1)
    }

    /// The number of members in the voting core (plan A's small voting set, typically 3~5).
    pub fn voter_count(&self) -> usize {
        let mc = self.raft.metrics().borrow().membership_config.clone();
        mc.membership().voter_ids().count()
    }

    /// The currently known leader (returns `None` when unknown / during a leader election).
    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// A cluster status snapshot (observability / operational monitoring: leader, term, applied
    /// position, member count).
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

    /// This node's id.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The partitions this node is responsible for under the current membership view (rendezvous
    /// assignment, default partition count).
    pub fn owned_partitions(&self) -> Vec<u32> {
        let members: Vec<NodeId> = self.members().into_iter().map(|(id, _)| id).collect();
        crate::partition::partitions_owned_by(
            self.node_id,
            &members,
            crate::partition::DEFAULT_PARTITIONS,
        )
    }

    /// Whether a given account / session key is handled by this node (rendezvous assignment,
    /// default partition count).
    ///
    /// No negotiation needed: membership comes from Raft's strongly consistent view and the hash
    /// is deterministic, so every node reaches the same conclusion.
    pub fn owns_key(&self, key: &str) -> bool {
        let members: Vec<NodeId> = self.members().into_iter().map(|(id, _)| id).collect();
        crate::partition::owns_key(
            self.node_id,
            key,
            &members,
            crate::partition::DEFAULT_PARTITIONS,
        )
    }

    /// Claims a partition's **ownership lease** and returns a monotonic fencing token.
    ///
    /// For situations that need a strong guarantee at the moment membership changes: while two
    /// nodes' views briefly disagree, only the node holding the newest token can safely handle the
    /// partition; downstream writes from a stale owner can be rejected on their smaller token.
    /// Steady-state ownership is decided by [`owns_key`](Self::owns_key); this method layers a
    /// Raft-backed strong guarantee on top of it.
    pub async fn acquire_partition(
        &self,
        partition: u32,
        ttl_ms: u64,
    ) -> Result<Option<u64>, ControlError> {
        let key = format!("__part/{partition}");
        self.acquire_lock(&key, &self.node_id.to_string(), ttl_ms)
            .await
    }

    /// Releases the partition ownership lease (only if it is still held by this node).
    pub async fn release_partition(&self, partition: u32) -> Result<(), ControlError> {
        let key = format!("__part/{partition}");
        self.release_lock(&key, &self.node_id.to_string()).await
    }

    /// Gracefully shuts down this node's Raft runtime: stops background tasks and releases storage
    /// (including the redb file lock).
    ///
    /// Call it before crash recovery / restart to make sure the redb database handle is released,
    /// so reopening the same directory no longer reports "Database already open".
    pub async fn shutdown(&self) -> Result<(), ControlError> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Waits for this node to become the leader (near-instant on a single node; a multi-node
    /// leader election takes one election-timeout window).
    pub async fn wait_leader(&self, timeout: std::time::Duration) -> Result<(), ControlError> {
        self.raft
            .wait(Some(timeout))
            .state(openraft::ServerState::Leader, "wait leader")
            .await
            .map_err(|e| ControlError::Raft(e.to_string()))?;
        Ok(())
    }

    /// Replicates and commits a command through Raft, returning the state machine's apply result.
    ///
    /// - This node is the leader → commit directly.
    /// - This node is a follower and the leader is known → forward the [`Cmd`] over HTTP to the
    ///   leader's `/cluster/write` ("register against any node and it works").
    /// - **A leader election is in progress, with no leader yet** → the command is definitely not
    ///   committed, so back off briefly and **retry the local client_write** (this is the only
    ///   case that is retried, precisely because it is known to be uncommitted, so the
    ///   non-idempotent CAS / AcquireLock commands cannot be applied twice; a failed forwarded
    ///   HTTP call is not retried — the leader may already have applied it, leaving the result
    ///   uncertain).
    async fn write(&self, cmd: Cmd) -> Result<CmdResult, ControlError> {
        use openraft::error::{ClientWriteError, RaftError};

        const MAX_ATTEMPTS: u32 = 6;
        for attempt in 0..MAX_ATTEMPTS {
            match self.raft.client_write(cmd.clone()).await {
                Ok(resp) => return Ok(resp.data),
                Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                    match &f.leader_node {
                        // Leader known: forward once (an HTTP failure returns immediately with no
                        // retry — the result is uncertain).
                        Some(node) => {
                            return forward_write_to_leader(&self.client, &node.addr, &cmd).await;
                        }
                        // No leader during the election: the command is uncommitted, so back off
                        // and retry (safe).
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

/// Forwards a command to the leader's `/cluster/write` endpoint and parses the result (reusing the
/// shared client).
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
        // Local read (for a linearizable read, call ensure_linearizable first — a follow-up item).
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

        // A single node becomes the leader quickly.
        cp.raft()
            .wait(Some(Duration::from_secs(10)))
            .state(openraft::ServerState::Leader, "become leader")
            .await
            .unwrap();

        // set / get through Raft.
        cp.set(b"k", b"v").await.unwrap();
        assert_eq!(cp.get(b"k").await.unwrap(), Some(b"v".to_vec()));

        // cas through Raft.
        assert!(cp.cas(b"k", Some(b"v"), b"v2").await.unwrap());
        assert_eq!(cp.get(b"k").await.unwrap(), Some(b"v2".to_vec()));

        // Distributed lock + fencing through Raft.
        assert_eq!(cp.acquire_lock("lock", "a", 5000).await.unwrap(), Some(1));
        assert_eq!(cp.acquire_lock("lock", "b", 5000).await.unwrap(), None);

        // Status snapshot: a single node is its own leader, has 1 member, and the applied position
        // advances with each write.
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
        // Crash recovery: the control plane (state machine + log) is fully persisted in redb, so
        // reopening the same directory should restore the committed state.
        let dir = tempfile::tempdir().unwrap();

        // First lifetime: write and commit (once client_write returns, the data is persisted).
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
            // Shut down gracefully to release the redb handle; committed data is already on disk.
            // cp is dropped when the block ends.
            cp.shutdown().await.unwrap();
        }
        // Let the background tasks exit fully and the file lock be released.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Second lifetime: reopen the same directory → recover from redb (no external log replay
        // needed).
        {
            let cp = RaftControlPlane::start_single_node(1, dir.path())
                .await
                .unwrap();
            cp.wait_leader(Duration::from_secs(10)).await.unwrap();
            // KV / CAS results are restored.
            assert_eq!(cp.get(b"persist").await.unwrap(), Some(b"v2".to_vec()));
            // Lock state is restored: the same key is still held by owner, so nobody else can
            // take it.
            assert_eq!(cp.acquire_lock("L", "other", 60_000).await.unwrap(), None);
            // Writes can continue after recovery (the log is appendable and the fencing counter
            // continues monotonically).
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

        // Give the HTTP server a moment to start up.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Node 1 initializes a single-node cluster → becomes the leader.
        let mut init = BTreeMap::new();
        init.insert(1u64, a1.to_string());
        cp1.init_cluster(init).await.unwrap();
        cp1.raft()
            .wait(Some(Duration::from_secs(10)))
            .state(openraft::ServerState::Leader, "become leader")
            .await
            .unwrap();

        // Add nodes 2 and 3 as learners (add_learner blocks until they catch up), then promote
        // them to voters (plan A's core).
        cp1.add_learner(2, a2).await.unwrap();
        cp1.add_learner(3, a3).await.unwrap();
        let voters: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        cp1.change_membership(voters).await.unwrap();

        // Membership API: all three nodes are registered and all are voters; the leader is known.
        assert_eq!(cp1.member_count(), 3);
        assert_eq!(cp1.voter_count(), 3);
        assert_eq!(cp1.current_leader(), Some(1));
        let mut addrs: Vec<String> = cp1.members().into_iter().map(|(_, a)| a).collect();
        addrs.sort();
        assert_eq!(addrs, vec![a1.to_string(), a2.to_string(), a3.to_string()]);

        // Write on the leader → replicated to a majority through Raft.
        cp1.set(b"hello", b"world").await.unwrap();

        // A local read on a follower should be eventually consistent (poll until it applies).
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

        // Distributed lock + fencing through Raft (committed on the leader).
        // (Locks live in the LOCKS table; KV replication is already verified and locks travel the
        // same Raft log, so the followers are not queried separately.)
        assert_eq!(cp1.acquire_lock("L", "a", 5000).await.unwrap(), Some(1));
        assert_eq!(cp1.acquire_lock("L", "b", 5000).await.unwrap(), None);

        // Wait for the followers to see the full membership view too (membership is replicated
        // after being committed through Raft).
        for cp in [&cp2, &cp3] {
            for _ in 0..50 {
                if cp.member_count() == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(cp.member_count(), 3);
        }

        // Partition ownership: the partitions each of the three nodes is responsible for do not
        // overlap, and their union covers everything (consistent without negotiation).
        let p1: BTreeSet<u32> = cp1.owned_partitions().into_iter().collect();
        let p2: BTreeSet<u32> = cp2.owned_partitions().into_iter().collect();
        let p3: BTreeSet<u32> = cp3.owned_partitions().into_iter().collect();
        assert!(p1.is_disjoint(&p2) && p1.is_disjoint(&p3) && p2.is_disjoint(&p3));
        assert_eq!(
            p1.len() + p2.len() + p3.len(),
            crate::partition::DEFAULT_PARTITIONS as usize
        );
        // Any given account is claimed by exactly one node (all three views agree).
        let owners = [
            cp1.owns_key("account-42"),
            cp2.owns_key("account-42"),
            cp3.owns_key("account-42"),
        ];
        assert_eq!(owners.iter().filter(|&&b| b).count(), 1);

        // Fencing ownership lease: cp1 claims partition 7 → gets a token; cp2 is rejected for the
        // same partition; once cp1 releases it, cp2 can take it, and the token increases
        // monotonically (split-brain protection).
        let t1 = cp1.acquire_partition(7, 5000).await.unwrap();
        assert!(t1.is_some());
        assert_eq!(cp2.acquire_partition(7, 5000).await.unwrap(), None);
        cp1.release_partition(7).await.unwrap();
        let t2 = cp2.acquire_partition(7, 5000).await.unwrap();
        assert!(t2 > t1, "fencing token must increase: {t2:?} !> {t1:?}");
    }

    #[tokio::test]
    async fn cluster_handles_voter_removal() {
        // Scale-down / decommission: remove one of the 3 voting nodes → the cluster keeps
        // committing under the new majority (the quorum is recomputed).
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

        // Remove node 3 (shrinking to {1,2}); the new majority = 2/2.
        cp1.change_membership([1u64, 2].into_iter().collect())
            .await
            .unwrap();
        assert_eq!(cp1.member_count(), 2);
        assert_eq!(cp1.voter_count(), 2);

        // Commits still work after the scale-down and replicate to the remaining members.
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
        // Log compaction + snapshot recovery: the leader writes a number of entries → triggers a
        // snapshot → purges the snapshotted log → by the time the new node joins the log has been
        // purged, so it can only catch up via **install_snapshot** (not by replaying the log).
        // Exercises the whole RaftSnapshotBuilder → install_snapshot RPC → redb restore chain.
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

        // Write a number of entries (committed quickly while still single-node).
        for i in 0..30u32 {
            cp1.set(format!("k{i}").as_bytes(), b"v").await.unwrap();
        }

        // Trigger a snapshot and wait for it to finish building (metrics.snapshot appears).
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

        // Purge the snapshotted log (after this a new node cannot catch up by replay and must go
        // through snapshot installation).
        cp1.raft().trigger().purge_log(snap_idx).await.unwrap();

        // A new node joins: add_learner blocks until it catches up — and it can only catch up via
        // install_snapshot.
        cp1.add_learner(2, a2).await.unwrap();

        // Node 2 should now hold every KV from the snapshot.
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
        // Dynamic self-organizing network: nodes join one by one and partition ownership
        // rebalances as membership grows.
        // The key HRW property holds in a **live cluster** — an existing node's ownership set only
        // shrinks, and a newly added node takes a share.
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
        let _ = (&cp2, &cp3); // Must stay alive so that add_learner can replicate.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let mut init = BTreeMap::new();
        init.insert(1u64, a1.to_string());
        cp1.init_cluster(init).await.unwrap();
        cp1.wait_leader(Duration::from_secs(10)).await.unwrap();

        // Single node: owns every partition.
        assert_eq!(cp1.member_count(), 1);
        let set1: BTreeSet<u32> = cp1.owned_partitions().into_iter().collect();
        assert_eq!(set1.len(), crate::partition::DEFAULT_PARTITIONS as usize);

        // Node 2 joins: ownership splits and node 1 gives up a share to node 2 (a proper subset).
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

        // Node 3 joins: the key HRW invariant — node 1's ownership can only shrink and it
        // **never regains** a partition (set3 ⊆ set2); node 3 takes a share from both node 1
        // and node 2.
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
        // 3 voting nodes; after the leader is killed, the surviving majority should elect a new
        // leader and keep committing (writes are forwarded to the new leader).
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

        // Write before the crash, committed through Raft.
        cp1.set(b"before", b"crash").await.unwrap();

        // Kill the leader (node 1).
        cp1.shutdown().await.unwrap();

        // The two surviving nodes should elect a new leader (2 or 3) within a few
        // election-timeout windows.
        let mut new_leader = None;
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let l2 = cp2.current_leader();
            let l3 = cp3.current_leader();
            // The new leader must be one of the surviving nodes, and both survivors must agree.
            if let Some(l) = l2 {
                if l != 1 && Some(l) == l3 {
                    new_leader = Some(l);
                    break;
                }
            }
        }
        let leader = new_leader.expect("cluster failed to elect a new leader after leader crash");
        assert!(leader == 2 || leader == 3, "unexpected new leader {leader}");

        // Write via a surviving node (a non-leader forwards to the new leader); retry through the
        // election churn until it succeeds.
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

        // The other survivor eventually reads the new value (replicated through Raft), and the
        // pre-crash data is still there.
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
