use crate::common::model::config::RaftConfig as AppRaftConfig;
use crate::common::model::control_plane_profile::ControlPlaneRaftCommand;
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use async_trait::async_trait;
use futures::StreamExt;
use axum::extract::State as AxumState;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use log::{error, info};
use openraft::async_runtime::WatchReceiver;
use openraft::error::{InitializeError, RaftError, RPCError, StreamingError};
use openraft::errors::{NetworkError, ReplicationClosed};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::storage::{IOFlushed, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf};
use openraft::{BasicNode, EntryPayload, LogState, Raft};
use rocksdb::{DB, Direction, IteratorMode, Options, WriteBatch};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::io::{self, Cursor};
use std::net::SocketAddr;
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use uuid::Uuid;

use crate::sync::LeadershipGate;

pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 500;
pub const DEFAULT_ELECTION_TIMEOUT_MS: u64 = 1500;
pub const DEFAULT_SNAPSHOT_INTERVAL: u64 = 500;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlPlaneRaftRequest {
    Apply(ControlPlaneRaftCommand),
}

impl Display for ControlPlaneRaftRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ControlPlaneRaftResponse {
    pub accepted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftLeaderView {
    pub local_node_id: u64,
    pub local_node_addr: String,
    pub leader_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub is_local_leader: bool,
}

openraft::declare_raft_types!(
    pub MocraRaftTypeConfig:
        D = ControlPlaneRaftRequest,
        R = ControlPlaneRaftResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

#[derive(Debug, Clone)]
pub struct RaftRuntimeConfig {
    pub node_id: u64,
    pub node: BasicNode,
    pub peers: BTreeMap<u64, BasicNode>,
    pub cluster_nodes: BTreeMap<u64, BasicNode>,
    pub data_dir: PathBuf,
    pub openraft_config: Arc<openraft::Config>,
}

impl RaftRuntimeConfig {
    pub fn from_app_config(namespace: &str, config: &AppRaftConfig) -> Result<Self, String> {
        let local_addr = config.addr.trim();
        if local_addr.is_empty() {
            return Err("raft.addr must not be empty".to_string());
        }

        let node_id = node_id_from_addr(local_addr);
        let node = BasicNode::new(local_addr);
        let mut peers = BTreeMap::new();

        for peer_addr in &config.peers {
            let peer_addr = peer_addr.trim();
            if peer_addr.is_empty() {
                return Err("raft.peers must not contain empty addresses".to_string());
            }
            if peer_addr == local_addr {
                continue;
            }

            let peer_id = node_id_from_addr(peer_addr);
            if peer_id == node_id {
                return Err(format!(
                    "raft node id collision between '{}' and '{}'",
                    local_addr, peer_addr
                ));
            }

            let replaced = peers.insert(peer_id, BasicNode::new(peer_addr));
            if replaced.is_some() {
                return Err(format!("duplicate raft peer id generated for '{}': {}", peer_addr, peer_id));
            }
        }

        let mut cluster_nodes = peers.clone();
        cluster_nodes.insert(node_id, node.clone());

        let heartbeat_interval = config
            .heartbeat_interval_ms
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let election_timeout = config
            .election_timeout_ms
            .unwrap_or(DEFAULT_ELECTION_TIMEOUT_MS);
        if election_timeout <= heartbeat_interval {
            return Err(format!(
                "raft.election_timeout_ms ({election_timeout}) must be greater than raft.heartbeat_interval_ms ({heartbeat_interval})"
            ));
        }

        let snapshot_interval = config
            .snapshot_interval
            .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL);
        let data_dir = config
            .data_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(format!("./raft_data/{namespace}")));

        let election_timeout_max = election_timeout.saturating_mul(2).max(election_timeout + 1);
        let openraft_config = openraft::Config {
            cluster_name: namespace.to_string(),
            heartbeat_interval,
            election_timeout_min: election_timeout,
            election_timeout_max,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(snapshot_interval),
            ..Default::default()
        }
        .validate()
        .map_err(|err| err.to_string())?;

        Ok(Self {
            node_id,
            node,
            peers,
            cluster_nodes,
            data_dir,
            openraft_config: Arc::new(openraft_config),
        })
    }

    pub fn bootstrap_members(&self) -> BTreeMap<u64, BasicNode> {
        self.cluster_nodes.clone()
    }

    pub fn is_single_node_bootstrap(&self) -> bool {
        self.peers.is_empty()
    }
}

pub fn node_id_from_addr(addr: &str) -> u64 {
    let mut hasher = Sha1::new();
    hasher.update(addr.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    let node_id = u64::from_be_bytes(bytes);
    if node_id == 0 { 1 } else { node_id }
}

type MocraEntry = EntryOf<MocraRaftTypeConfig>;
type MocraLogId = LogIdOf<MocraRaftTypeConfig>;
type MocraVote = VoteOf<MocraRaftTypeConfig>;
type MocraStoredMembership = StoredMembershipOf<MocraRaftTypeConfig>;
type MocraSnapshot = SnapshotOf<MocraRaftTypeConfig>;
type MocraSnapshotMeta = SnapshotMetaOf<MocraRaftTypeConfig>;
type MocraRaft = Raft<MocraRaftTypeConfig, RocksRaftStore>;
type MocraAppendEntriesRequest = openraft::raft::AppendEntriesRequest<MocraRaftTypeConfig>;
type MocraAppendEntriesResponse = openraft::raft::AppendEntriesResponse<MocraRaftTypeConfig>;
type MocraVoteRequest = openraft::raft::VoteRequest<MocraRaftTypeConfig>;
type MocraVoteResponse = openraft::raft::VoteResponse<MocraRaftTypeConfig>;
type MocraSnapshotResponse = openraft::raft::SnapshotResponse<MocraRaftTypeConfig>;

const KEY_VOTE: &[u8] = b"raft/meta/vote";
const KEY_COMMITTED: &[u8] = b"raft/meta/committed";
const KEY_LAST_PURGED: &[u8] = b"raft/meta/last_purged";
const KEY_SM_STATE: &[u8] = b"raft/sm/state";
const KEY_SNAPSHOT_META: &[u8] = b"raft/snapshot/meta";
const KEY_SNAPSHOT_DATA: &[u8] = b"raft/snapshot/data";
const LOG_PREFIX: &str = "raft/log/";
const RAFT_APPEND_PATH: &str = "/raft/append";
const RAFT_VOTE_PATH: &str = "/raft/vote";
const RAFT_SNAPSHOT_PATH: &str = "/raft/snapshot";
const RAFT_JOIN_PATH: &str = "/raft/admin/join";
const RAFT_CLIENT_WRITE_PATH: &str = "/raft/client-write";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotTransport {
    vote: MocraVote,
    meta: MocraSnapshotMeta,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinClusterRequest {
    node_id: u64,
    node: BasicNode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JoinClusterStatus {
    Joined,
    ForwardToLeader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JoinClusterResponse {
    status: JoinClusterStatus,
    leader_id: Option<u64>,
    leader_addr: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientWriteRequest {
    command: ControlPlaneRaftCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ClientWriteStatus {
    Applied,
    ForwardToLeader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientWriteResponse {
    status: ClientWriteStatus,
    log_index: Option<u64>,
    leader_id: Option<u64>,
    leader_addr: Option<String>,
}

#[derive(Clone)]
struct RaftRpcState {
    raft: MocraRaft,
    cluster_nodes: BTreeMap<u64, BasicNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppliedRequestRecord {
    log_id: MocraLogId,
    request: ControlPlaneRaftRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateMachineData {
    last_applied_log: Option<MocraLogId>,
    last_membership: MocraStoredMembership,
    applied_requests: Vec<AppliedRequestRecord>,
}

impl Default for StateMachineData {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: MocraStoredMembership::default(),
            applied_requests: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct RocksRaftStoreInner {
    db: Arc<DB>,
    write_lock: std::sync::Mutex<()>,
    profile_store: Option<Weak<ProfileControlPlaneStore>>,
}

#[derive(Debug, Clone)]
pub struct RocksRaftStore {
    inner: Arc<RocksRaftStoreInner>,
}

impl RocksRaftStore {
    pub fn open(path: &PathBuf, profile_store: Option<Arc<ProfileControlPlaneStore>>) -> Result<Self, String> {
        std::fs::create_dir_all(path).map_err(|err| err.to_string())?;

        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, path).map_err(|err| err.to_string())?;

        Ok(Self {
            inner: Arc::new(RocksRaftStoreInner {
                db: Arc::new(db),
                write_lock: std::sync::Mutex::new(()),
                profile_store: profile_store.as_ref().map(Arc::downgrade),
            }),
        })
    }

    fn db(&self) -> &DB {
        self.inner.db.as_ref()
    }

    fn lock_write(&self) -> Result<std::sync::MutexGuard<'_, ()>, io::Error> {
        self.inner
            .write_lock
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))
    }

    fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, io::Error> {
        serde_json::to_vec(value).map_err(io::Error::other)
    }

    fn deserialize<T>(bytes: &[u8]) -> Result<T, io::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_slice(bytes).map_err(io::Error::other)
    }

    fn get_optional<T>(&self, key: &[u8]) -> Result<Option<T>, io::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        match self.db().get(key).map_err(io::Error::other)? {
            Some(bytes) => Self::deserialize(&bytes).map(Some),
            None => Ok(None),
        }
    }

    fn put_serialized<T: Serialize>(batch: &mut WriteBatch, key: &[u8], value: &T) -> Result<(), io::Error> {
        batch.put(key, Self::serialize(value)?);
        Ok(())
    }

    fn log_key(index: u64) -> Vec<u8> {
        format!("{LOG_PREFIX}{index:020}").into_bytes()
    }

    fn read_state_machine_data(&self) -> Result<StateMachineData, io::Error> {
        self.get_optional(KEY_SM_STATE)
            .map(|value| value.unwrap_or_default())
    }

    fn write_state_machine_data(&self, data: &StateMachineData) -> Result<(), io::Error> {
        self.db()
            .put(KEY_SM_STATE, Self::serialize(data)?)
            .map_err(io::Error::other)
    }

    fn read_snapshot_meta(&self) -> Result<Option<MocraSnapshotMeta>, io::Error> {
        self.get_optional(KEY_SNAPSHOT_META)
    }

    fn read_snapshot_data(&self) -> Result<Option<Vec<u8>>, io::Error> {
        self.db().get(KEY_SNAPSHOT_DATA).map_err(io::Error::other).map(|opt| opt.map(|v| v.to_vec()))
    }

    fn save_snapshot(&self, meta: &MocraSnapshotMeta, bytes: &[u8]) -> Result<(), io::Error> {
        let mut batch = WriteBatch::default();
        Self::put_serialized(&mut batch, KEY_SNAPSHOT_META, meta)?;
        batch.put(KEY_SNAPSHOT_DATA, bytes);
        self.db().write(batch).map_err(io::Error::other)
    }

    fn iter_log_entries(&self) -> impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + '_ {
        self.db()
            .iterator(IteratorMode::From(LOG_PREFIX.as_bytes(), Direction::Forward))
            .take_while(|entry| match entry {
                Ok((key, _)) => key.starts_with(LOG_PREFIX.as_bytes()),
                Err(_) => true,
            })
    }

    fn range_bounds_to_start_end<RB: RangeBounds<u64>>(range: &RB) -> (u64, Option<u64>) {
        let start = match range.start_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(value) => Some(value.saturating_add(1)),
            Bound::Excluded(value) => Some(*value),
            Bound::Unbounded => None,
        };
        (start, end)
    }

    fn read_log_entries_in_range<RB>(&self, range: RB) -> Result<Vec<MocraEntry>, io::Error>
    where
        RB: RangeBounds<u64>,
    {
        let (start, end) = Self::range_bounds_to_start_end(&range);
        let mut entries = Vec::new();

        for item in self.iter_log_entries() {
            let (key, value) = item.map_err(io::Error::other)?;
            if !key.starts_with(LOG_PREFIX.as_bytes()) {
                continue;
            }

            let suffix = std::str::from_utf8(&key[LOG_PREFIX.len()..]).map_err(io::Error::other)?;
            let index: u64 = suffix.parse().map_err(io::Error::other)?;
            if index < start {
                continue;
            }
            if let Some(end) = end
                && index >= end
            {
                break;
            }

            entries.push(Self::deserialize(&value)?);
        }

        Ok(entries)
    }

    fn last_log_id(&self) -> Result<Option<MocraLogId>, io::Error> {
        let mut last = None;
        for item in self.iter_log_entries() {
            let (_, value) = item.map_err(io::Error::other)?;
            let entry: MocraEntry = Self::deserialize(&value)?;
            last = Some(entry.log_id.clone());
        }
        Ok(last)
    }
}

impl RaftLogReader<MocraRaftTypeConfig> for RocksRaftStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<MocraEntry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    {
        self.read_log_entries_in_range(range)
    }

    async fn read_vote(&mut self) -> Result<Option<MocraVote>, io::Error> {
        self.get_optional(KEY_VOTE)
    }
}

impl RaftLogStorage<MocraRaftTypeConfig> for RocksRaftStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MocraRaftTypeConfig>, io::Error> {
        Ok(LogState {
            last_purged_log_id: self.get_optional(KEY_LAST_PURGED)?,
            last_log_id: self.last_log_id()?,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &MocraVote) -> Result<(), io::Error> {
        let _guard = self.lock_write()?;
        self.db()
            .put(KEY_VOTE, Self::serialize(vote)?)
            .map_err(io::Error::other)
    }

    async fn save_committed(&mut self, committed: Option<MocraLogId>) -> Result<(), io::Error> {
        let _guard = self.lock_write()?;
        self.db()
            .put(KEY_COMMITTED, Self::serialize(&committed)?)
            .map_err(io::Error::other)
    }

    async fn read_committed(&mut self) -> Result<Option<MocraLogId>, io::Error> {
        self.get_optional(KEY_COMMITTED)
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<MocraRaftTypeConfig>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = MocraEntry> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let _guard = self.lock_write()?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            batch.put(Self::log_key(entry.log_id.index), Self::serialize(&entry)?);
        }

        let result = self.db().write(batch).map_err(io::Error::other);
        callback.io_completed(result.as_ref().map(|_| ()).map_err(|err| io::Error::other(err.to_string())));
        result
    }

    async fn truncate_after(&mut self, last_log_id: Option<MocraLogId>) -> Result<(), io::Error> {
        let _guard = self.lock_write()?;
        let mut batch = WriteBatch::default();
        let keep_until = last_log_id.map(|log_id| log_id.index).unwrap_or(0);
        for item in self.iter_log_entries() {
            let (key, _) = item.map_err(io::Error::other)?;
            let suffix = std::str::from_utf8(&key[LOG_PREFIX.len()..]).map_err(io::Error::other)?;
            let index: u64 = suffix.parse().map_err(io::Error::other)?;
            if last_log_id.is_none() || index > keep_until {
                batch.delete(key);
            }
        }
        self.db().write(batch).map_err(io::Error::other)
    }

    async fn purge(&mut self, log_id: MocraLogId) -> Result<(), io::Error> {
        let _guard = self.lock_write()?;
        let mut batch = WriteBatch::default();
        for item in self.iter_log_entries() {
            let (key, _) = item.map_err(io::Error::other)?;
            let suffix = std::str::from_utf8(&key[LOG_PREFIX.len()..]).map_err(io::Error::other)?;
            let index: u64 = suffix.parse().map_err(io::Error::other)?;
            if index <= log_id.index {
                batch.delete(key);
            } else {
                break;
            }
        }
        Self::put_serialized(&mut batch, KEY_LAST_PURGED, &Some(log_id))?;
        self.db().write(batch).map_err(io::Error::other)
    }
}

#[derive(Debug, Clone)]
pub struct RocksSnapshotBuilder {
    store: RocksRaftStore,
}

impl RaftSnapshotBuilder<MocraRaftTypeConfig> for RocksSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<MocraSnapshot, io::Error> {
        let data = self.store.read_state_machine_data()?;
        let bytes = RocksRaftStore::serialize(&data)?;
        let meta = MocraSnapshotMeta {
            last_log_id: data.last_applied_log.clone(),
            last_membership: data.last_membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                data.last_applied_log.as_ref().map(|log_id| log_id.index).unwrap_or_default(),
                Uuid::new_v4()
            ),
        };
        self.store.save_snapshot(&meta, &bytes)?;
        Ok(openraft::Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}

impl RaftStateMachine<MocraRaftTypeConfig> for RocksRaftStore {
    type SnapshotBuilder = RocksSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<MocraLogId>, MocraStoredMembership), io::Error> {
        let data = self.read_state_machine_data()?;
        Ok((data.last_applied_log, data.last_membership))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<openraft::storage::EntryResponder<MocraRaftTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend,
    {
        let mut buffered = Vec::new();
        while let Some(item) = entries.next().await {
            buffered.push(item?);
        }

        let _guard = self.lock_write()?;
        let mut data = self.read_state_machine_data()?;

        for (entry, responder) in buffered {
            data.last_applied_log = Some(entry.log_id.clone());

            let response = match &entry.payload {
                EntryPayload::Blank => ControlPlaneRaftResponse::default(),
                EntryPayload::Normal(request) => {
                    match request {
                        ControlPlaneRaftRequest::Apply(command) => {
                            if let Some(profile_store) = self
                                .inner
                                .profile_store
                                .as_ref()
                                .and_then(Weak::upgrade)
                            {
                                profile_store
                                    .apply_replicated_command(command.clone(), entry.log_id.index)
                                    .map_err(io::Error::other)?;
                            }
                        }
                    }
                    data.applied_requests.push(AppliedRequestRecord {
                        log_id: entry.log_id.clone(),
                        request: request.clone(),
                    });
                    ControlPlaneRaftResponse { accepted: true }
                }
                EntryPayload::Membership(membership) => {
                    data.last_membership = MocraStoredMembership::new(
                        Some(entry.log_id.clone()),
                        membership.clone(),
                    );
                    ControlPlaneRaftResponse::default()
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
        }

        self.write_state_machine_data(&data)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RocksSnapshotBuilder {
            store: self.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(&mut self, meta: &MocraSnapshotMeta, snapshot: Cursor<Vec<u8>>) -> Result<(), io::Error> {
        let _guard = self.lock_write()?;
        let bytes = snapshot.into_inner();
        let data: StateMachineData = RocksRaftStore::deserialize(&bytes)?;
        self.write_state_machine_data(&data)?;
        self.save_snapshot(meta, &bytes)?;

        if let Some(profile_store) = self.inner.profile_store.as_ref().and_then(Weak::upgrade) {
            profile_store.reset_replicated_state().map_err(io::Error::other)?;
            for record in &data.applied_requests {
                match &record.request {
                    ControlPlaneRaftRequest::Apply(command) => {
                        profile_store
                            .apply_replicated_command(command.clone(), record.log_id.index)
                            .map_err(io::Error::other)?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<MocraSnapshot>, io::Error> {
        match (self.read_snapshot_meta()?, self.read_snapshot_data()?) {
            (Some(meta), Some(bytes)) => Ok(Some(openraft::Snapshot {
                meta,
                snapshot: Cursor::new(bytes),
            })),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
struct HttpRaftNetworkFactory {
    client: reqwest::Client,
}

impl RaftNetworkFactory<MocraRaftTypeConfig> for HttpRaftNetworkFactory {
    type Network = HttpRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let _ = target;
        HttpRaftNetwork {
            client: self.client.clone(),
            target_addr: node.addr.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct HttpRaftNetwork {
    client: reqwest::Client,
    target_addr: String,
}

impl HttpRaftNetwork {
    fn target_url(&self, path: &str) -> String {
        format!("{}{}", http_base_url(&self.target_addr), path)
    }

    async fn post_json<Req, Resp>(&self, path: &str, payload: &Req) -> Result<Resp, reqwest::Error>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        self.client
            .post(self.target_url(path))
            .json(payload)
            .send()
            .await?
            .error_for_status()?
            .json::<Resp>()
            .await
    }
}

impl RaftNetworkV2<MocraRaftTypeConfig> for HttpRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: MocraAppendEntriesRequest,
        _option: RPCOption,
    ) -> Result<MocraAppendEntriesResponse, RPCError<MocraRaftTypeConfig>> {
        self.post_json::<_, MocraAppendEntriesResponse>(RAFT_APPEND_PATH, &rpc)
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))
    }

    async fn vote(
        &mut self,
        rpc: MocraVoteRequest,
        _option: RPCOption,
    ) -> Result<MocraVoteResponse, RPCError<MocraRaftTypeConfig>> {
        self.post_json::<_, MocraVoteResponse>(RAFT_VOTE_PATH, &rpc)
            .await
            .map_err(|err| RPCError::Network(NetworkError::new(&err)))
    }

    async fn full_snapshot(
        &mut self,
        vote: MocraVote,
        snapshot: MocraSnapshot,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + openraft::OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<MocraSnapshotResponse, StreamingError<MocraRaftTypeConfig>> {
        let payload = SnapshotTransport {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };
        self.post_json::<_, MocraSnapshotResponse>(RAFT_SNAPSHOT_PATH, &payload)
            .await
            .map_err(|err| StreamingError::Network(NetworkError::new(&err)))
    }
}

#[derive(Debug, Clone)]
pub struct RaftRuntime {
    config: Arc<RaftRuntimeConfig>,
    raft: MocraRaft,
    store: RocksRaftStore,
}

impl RaftRuntime {
    pub async fn start(
        config: Arc<RaftRuntimeConfig>,
        profile_store: Option<Arc<ProfileControlPlaneStore>>,
    ) -> Result<Self, String> {
        let store = RocksRaftStore::open(&config.data_dir, profile_store)?;
        let raft = Raft::new(
            config.node_id,
            config.openraft_config.clone(),
            HttpRaftNetworkFactory {
                client: reqwest::Client::new(),
            },
            store.clone(),
            store.clone(),
        )
        .await
        .map_err(|err| err.to_string())?;

        let runtime = Self { config, raft, store };
        runtime.start_rpc_server().await?;
        runtime.bootstrap_single_node().await?;
        runtime.join_existing_cluster().await?;

        Ok(runtime)
    }

    async fn start_rpc_server(&self) -> Result<(), String> {
        let addr: SocketAddr = self
            .config
            .node
            .addr
            .parse()
            .map_err(|err| format!("invalid raft.addr '{}': {err}", self.config.node.addr))?;

        let state = RaftRpcState {
            raft: self.raft.clone(),
            cluster_nodes: self.config.cluster_nodes.clone(),
        };
        let app = Router::new()
            .route(RAFT_APPEND_PATH, post(raft_append_entries_handler))
            .route(RAFT_VOTE_PATH, post(raft_vote_handler))
            .route(RAFT_SNAPSHOT_PATH, post(raft_snapshot_handler))
            .route(RAFT_JOIN_PATH, post(raft_join_handler))
            .route(RAFT_CLIENT_WRITE_PATH, post(raft_client_write_handler))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|err| format!("failed to bind raft rpc listener on {addr}: {err}"))?;
        info!("Raft RPC server listening on {}", addr);

        tokio::spawn(async move {
            if let Err(err) = axum::serve(listener, app.into_make_service()).await {
                error!("Raft RPC server stopped with error: {}", err);
            }
        });

        Ok(())
    }

    async fn bootstrap_single_node(&self) -> Result<(), String> {
        if !self.config.is_single_node_bootstrap() {
            return Ok(());
        }

        match self.raft.initialize(self.config.bootstrap_members()).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(err) => Err(err.to_string()),
        }
    }

    async fn join_existing_cluster(&self) -> Result<(), String> {
        if self.config.is_single_node_bootstrap() {
            return Ok(());
        }

        let client = reqwest::Client::new();
        let mut targets: Vec<String> = self
            .config
            .peers
            .values()
            .map(|node| node.addr.clone())
            .collect();
        let mut visited = BTreeSet::new();
        let request = JoinClusterRequest {
            node_id: self.config.node_id,
            node: self.config.node.clone(),
        };
        let mut last_error = None;

        while let Some(target) = targets.pop() {
            if !visited.insert(target.clone()) {
                continue;
            }

            match client
                .post(format!("{}{}", http_base_url(&target), RAFT_JOIN_PATH))
                .json(&request)
                .send()
                .await
            {
                Ok(response) => match response.error_for_status() {
                    Ok(response) => match response.json::<JoinClusterResponse>().await {
                        Ok(payload) => match payload.status {
                            JoinClusterStatus::Joined => {
                                info!(
                                    "Raft node {} joined cluster via {}",
                                    self.config.node_id, target
                                );
                                return Ok(());
                            }
                            JoinClusterStatus::ForwardToLeader => {
                                if let Some(leader_addr) = payload.leader_addr {
                                    targets.push(leader_addr);
                                }
                            }
                        },
                        Err(err) => {
                            last_error = Some(err.to_string());
                        }
                    },
                    Err(err) => {
                        last_error = Some(err.to_string());
                    }
                },
                Err(err) => {
                    last_error = Some(err.to_string());
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            format!(
                "failed to join raft cluster for node {} using peers {:?}",
                self.config.node_id,
                self.config.peers.values().map(|node| node.addr.clone()).collect::<Vec<_>>()
            )
        }))
    }

    pub fn raft(&self) -> &MocraRaft {
        &self.raft
    }

    pub async fn client_write(&self, command: ControlPlaneRaftCommand) -> Result<u64, String> {
        self.client_write_with_forwarding(command, 0).await
    }

    async fn client_write_with_forwarding(
        &self,
        command: ControlPlaneRaftCommand,
        redirect_count: usize,
    ) -> Result<u64, String> {
        let redirect_limit = self.config.cluster_nodes.len().saturating_add(1);
        if redirect_count > redirect_limit {
            return Err(format!(
                "raft client write exceeded leader forwarding limit ({redirect_limit})"
            ));
        }

        match self
            .raft
            .client_write(ControlPlaneRaftRequest::Apply(command.clone()))
            .await
        {
            Ok(response) => Ok(response.log_id.index),
            Err(err) => {
                if let Some(forward) = err.forward_to_leader().cloned()
                    && let Some(leader_addr) =
                        leader_addr_from_forward(&forward, &self.config.cluster_nodes)
                {
                    return self
                        .forward_client_write_http(command, leader_addr, redirect_count + 1)
                        .await;
                }
                Err(err.to_string())
            }
        }
    }

    async fn forward_client_write_http(
        &self,
        command: ControlPlaneRaftCommand,
        initial_target: String,
        mut redirect_count: usize,
    ) -> Result<u64, String> {
        let redirect_limit = self.config.cluster_nodes.len().saturating_add(1);
        let client = reqwest::Client::new();
        let mut target = initial_target;

        loop {
            if redirect_count > redirect_limit {
                return Err(format!(
                    "raft client write exceeded leader forwarding limit ({redirect_limit})"
                ));
            }

            let response = client
                .post(format!("{}{}", http_base_url(&target), RAFT_CLIENT_WRITE_PATH))
                .json(&ClientWriteRequest {
                    command: command.clone(),
                })
                .send()
                .await
                .map_err(|err| err.to_string())?
                .error_for_status()
                .map_err(|err| err.to_string())?
                .json::<ClientWriteResponse>()
                .await
                .map_err(|err| err.to_string())?;

            match response.status {
                ClientWriteStatus::Applied => {
                    return response.log_index.ok_or_else(|| {
                        "raft client write completed without a log index".to_string()
                    });
                }
                ClientWriteStatus::ForwardToLeader => {
                    target = response.leader_addr.ok_or_else(|| {
                        format!(
                            "raft leader forwarding target missing while forwarding command via {}",
                            target
                        )
                    })?;
                    redirect_count += 1;
                }
            }
        }
    }

    pub fn config(&self) -> Arc<RaftRuntimeConfig> {
        self.config.clone()
    }

    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow_watched().state.is_leader()
    }

    pub fn leader_view(&self) -> RaftLeaderView {
        let metrics = self.raft.metrics();
        let borrowed = metrics.borrow_watched();
        let leader_id = borrowed.current_leader;

        RaftLeaderView {
            local_node_id: self.config.node_id,
            local_node_addr: self.config.node.addr.clone(),
            leader_id,
            leader_addr: leader_id
                .and_then(|node_id| self.config.cluster_nodes.get(&node_id))
                .map(|node| node.addr.clone()),
            is_local_leader: leader_id == Some(self.config.node_id),
        }
    }

    pub fn store(&self) -> &RocksRaftStore {
        &self.store
    }
}

#[derive(Debug, Clone)]
pub struct RaftLeadershipGate {
    runtime: Arc<RaftRuntime>,
}

impl RaftLeadershipGate {
    pub fn new(runtime: Arc<RaftRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl LeadershipGate for RaftLeadershipGate {
    async fn start(self: Arc<Self>) {
        let _ = self;
    }

    fn is_leader(&self) -> bool {
        self.runtime.is_leader()
    }
}

fn http_base_url(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.trim_end_matches('/').to_string()
    } else {
        format!("http://{}", addr.trim_end_matches('/'))
    }
}

async fn raft_append_entries_handler(
    AxumState(state): AxumState<RaftRpcState>,
    Json(request): Json<MocraAppendEntriesRequest>,
) -> Result<Json<MocraAppendEntriesResponse>, (StatusCode, String)> {
    state
        .raft
        .append_entries(request)
        .await
        .map(Json)
        .map_err(internal_raft_error)
}

async fn raft_vote_handler(
    AxumState(state): AxumState<RaftRpcState>,
    Json(request): Json<MocraVoteRequest>,
) -> Result<Json<MocraVoteResponse>, (StatusCode, String)> {
    state
        .raft
        .vote(request)
        .await
        .map(Json)
        .map_err(internal_raft_error)
}

async fn raft_snapshot_handler(
    AxumState(state): AxumState<RaftRpcState>,
    Json(request): Json<SnapshotTransport>,
) -> Result<Json<MocraSnapshotResponse>, (StatusCode, String)> {
    let snapshot = openraft::Snapshot {
        meta: request.meta,
        snapshot: Cursor::new(request.data),
    };

    state
        .raft
        .install_full_snapshot(request.vote, snapshot)
        .await
        .map(Json)
        .map_err(internal_raft_fatal)
}

async fn raft_join_handler(
    AxumState(state): AxumState<RaftRpcState>,
    Json(request): Json<JoinClusterRequest>,
) -> Result<Json<JoinClusterResponse>, (StatusCode, String)> {
    match state
        .raft
        .add_learner(request.node_id, request.node.clone(), true)
        .await
    {
        Ok(_) => {}
        Err(err) => {
            if let Some(forward) = err.forward_to_leader().cloned() {
                return Ok(Json(join_redirect_response(forward, &state.cluster_nodes)));
            }
            return Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()));
        }
    }

    let metrics = state.raft.metrics();
    let mut members: BTreeSet<u64> = {
        let borrowed = metrics.borrow_watched();
        borrowed.membership_config.voter_ids().collect()
    };

    if !members.insert(request.node_id) {
        return Ok(Json(JoinClusterResponse {
            status: JoinClusterStatus::Joined,
            leader_id: None,
            leader_addr: None,
        }));
    }

    match state.raft.change_membership(members, false).await {
        Ok(_) => Ok(Json(JoinClusterResponse {
            status: JoinClusterStatus::Joined,
            leader_id: None,
            leader_addr: None,
        })),
        Err(err) => {
            if let Some(forward) = err.forward_to_leader().cloned() {
                Ok(Json(join_redirect_response(forward, &state.cluster_nodes)))
            } else {
                Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
            }
        }
    }
}

async fn raft_client_write_handler(
    AxumState(state): AxumState<RaftRpcState>,
    Json(request): Json<ClientWriteRequest>,
) -> Result<Json<ClientWriteResponse>, (StatusCode, String)> {
    match state
        .raft
        .client_write(ControlPlaneRaftRequest::Apply(request.command))
        .await
    {
        Ok(response) => Ok(Json(ClientWriteResponse {
            status: ClientWriteStatus::Applied,
            log_index: Some(response.log_id.index),
            leader_id: None,
            leader_addr: None,
        })),
        Err(err) => {
            if let Some(forward) = err.forward_to_leader().cloned() {
                Ok(Json(client_write_redirect_response(
                    forward,
                    &state.cluster_nodes,
                )))
            } else {
                Err((StatusCode::SERVICE_UNAVAILABLE, err.to_string()))
            }
        }
    }
}

fn join_redirect_response(
    forward: openraft::errors::ForwardToLeader<MocraRaftTypeConfig>,
    cluster_nodes: &BTreeMap<u64, BasicNode>,
) -> JoinClusterResponse {
    JoinClusterResponse {
        status: JoinClusterStatus::ForwardToLeader,
        leader_id: forward.leader_id,
        leader_addr: leader_addr_from_forward(&forward, cluster_nodes),
    }
}

fn client_write_redirect_response(
    forward: openraft::errors::ForwardToLeader<MocraRaftTypeConfig>,
    cluster_nodes: &BTreeMap<u64, BasicNode>,
) -> ClientWriteResponse {
    ClientWriteResponse {
        status: ClientWriteStatus::ForwardToLeader,
        log_index: None,
        leader_id: forward.leader_id,
        leader_addr: leader_addr_from_forward(&forward, cluster_nodes),
    }
}

fn leader_addr_from_forward(
    forward: &openraft::errors::ForwardToLeader<MocraRaftTypeConfig>,
    cluster_nodes: &BTreeMap<u64, BasicNode>,
) -> Option<String> {
    forward
        .leader_node
        .as_ref()
        .map(|node| node.addr.clone())
        .or_else(|| {
            forward
                .leader_id
                .and_then(|leader_id| cluster_nodes.get(&leader_id).map(|node| node.addr.clone()))
        })
}

fn internal_raft_error(err: RaftError<MocraRaftTypeConfig>) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

fn internal_raft_fatal(err: openraft::errors::Fatal<MocraRaftTypeConfig>) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::ServerState;
    use tempfile::tempdir;
    use tokio::time::Duration;

    #[test]
    fn node_id_derivation_is_stable() {
        let first = node_id_from_addr("127.0.0.1:7001");
        let second = node_id_from_addr("127.0.0.1:7001");
        let third = node_id_from_addr("127.0.0.1:7002");

        assert_eq!(first, second);
        assert_ne!(first, third);
    }

    #[test]
    fn runtime_config_translates_app_config() {
        let runtime = RaftRuntimeConfig::from_app_config(
            "test-namespace",
            &AppRaftConfig {
                addr: "127.0.0.1:7001".to_string(),
                peers: vec!["127.0.0.1:7002".to_string(), "127.0.0.1:7003".to_string()],
                heartbeat_interval_ms: Some(600),
                election_timeout_ms: Some(1800),
                snapshot_interval: Some(321),
                data_dir: None,
            },
        )
        .expect("runtime config should build");

        assert_eq!(runtime.openraft_config.cluster_name, "test-namespace");
        assert_eq!(runtime.openraft_config.heartbeat_interval, 600);
        assert_eq!(runtime.openraft_config.election_timeout_min, 1800);
        assert!(matches!(
            runtime.openraft_config.snapshot_policy,
            openraft::SnapshotPolicy::LogsSinceLast(321)
        ));
        assert_eq!(runtime.cluster_nodes.len(), 3);
        assert_eq!(runtime.data_dir, PathBuf::from("./raft_data/test-namespace"));
    }

    #[test]
    fn runtime_config_rejects_invalid_timing() {
        let err = RaftRuntimeConfig::from_app_config(
            "test-namespace",
            &AppRaftConfig {
                addr: "127.0.0.1:7001".to_string(),
                peers: vec![],
                heartbeat_interval_ms: Some(1500),
                election_timeout_ms: Some(1500),
                snapshot_interval: None,
                data_dir: None,
            },
        )
        .expect_err("invalid timing should fail");

        assert!(err.contains("must be greater"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_bootstraps_single_node_cluster() {
        let temp_dir = tempdir().expect("temp dir should create");
        let runtime = RaftRuntime::start(Arc::new(
            RaftRuntimeConfig::from_app_config(
                "raft-single-node-test",
                &AppRaftConfig {
                    addr: "127.0.0.1:7901".to_string(),
                    peers: vec![],
                    heartbeat_interval_ms: Some(50),
                    election_timeout_ms: Some(150),
                    snapshot_interval: Some(64),
                    data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
                },
            )
            .expect("runtime config should build"),
        ), None)
        .await
        .expect("runtime should start");

        runtime
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .state(ServerState::Leader, "single-node bootstrap should elect a leader")
            .await
            .expect("single node should become leader");

        assert!(runtime.is_leader());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_joins_existing_cluster() {
        let leader_dir = tempdir().expect("leader dir should create");
        let follower_dir = tempdir().expect("follower dir should create");
        let leader_addr = allocate_local_addr();
        let follower_addr = allocate_local_addr();

        let leader = Arc::new(
            RaftRuntime::start(Arc::new(
                RaftRuntimeConfig::from_app_config(
                    "raft-join-test",
                    &AppRaftConfig {
                        addr: leader_addr.clone(),
                        peers: vec![],
                        heartbeat_interval_ms: Some(50),
                        election_timeout_ms: Some(150),
                        snapshot_interval: Some(64),
                        data_dir: Some(leader_dir.path().to_string_lossy().to_string()),
                    },
                )
                .expect("leader config should build"),
            ), None)
            .await
            .expect("leader runtime should start"),
        );

        leader
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .state(ServerState::Leader, "leader should become leader before join")
            .await
            .expect("leader should become leader");

        let _follower = RaftRuntime::start(Arc::new(
            RaftRuntimeConfig::from_app_config(
                "raft-join-test",
                &AppRaftConfig {
                    addr: follower_addr.clone(),
                    peers: vec![leader_addr.clone()],
                    heartbeat_interval_ms: Some(50),
                    election_timeout_ms: Some(150),
                    snapshot_interval: Some(64),
                    data_dir: Some(follower_dir.path().to_string_lossy().to_string()),
                },
            )
            .expect("follower config should build"),
        ), None)
        .await
        .expect("follower runtime should join cluster");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let contains_follower = {
                    let metrics = leader.raft().metrics();
                    let borrowed = metrics.borrow_watched();
                    borrowed.membership_config.voter_ids().any(|id| id == node_id_from_addr(&follower_addr))
                };
                if contains_follower {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("leader membership should include joined follower");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn client_write_updates_profile_store_state() {
        let temp_dir = tempdir().expect("temp dir should create");
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("demo").expect("open temp store"));
        let runtime = Arc::new(
            RaftRuntime::start(
                Arc::new(
                    RaftRuntimeConfig::from_app_config(
                        "raft-profile-store-test",
                        &AppRaftConfig {
                            addr: allocate_local_addr(),
                            peers: vec![],
                            heartbeat_interval_ms: Some(50),
                            election_timeout_ms: Some(150),
                            snapshot_interval: Some(64),
                            data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
                        },
                    )
                    .expect("runtime config should build"),
                ),
                Some(profile_store.clone()),
            )
            .await
            .expect("runtime should start"),
        );
        profile_store.attach_raft_runtime(runtime.clone());

        runtime
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .state(ServerState::Leader, "single-node bootstrap should elect a leader")
            .await
            .expect("single node should become leader");

        profile_store
            .set_pause_state(true)
            .await
            .expect("pause flag should replicate");

        assert!(profile_store.is_paused());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn follower_client_write_forwards_to_leader() {
        let leader_dir = tempdir().expect("leader dir should create");
        let follower_dir = tempdir().expect("follower dir should create");
        let leader_addr = allocate_local_addr();
        let follower_addr = allocate_local_addr();
        let leader_store = Arc::new(
            ProfileControlPlaneStore::open_temp("demo").expect("open leader profile store"),
        );
        let follower_store = Arc::new(
            ProfileControlPlaneStore::open_temp("demo").expect("open follower profile store"),
        );

        let leader = Arc::new(
            RaftRuntime::start(
                Arc::new(
                    RaftRuntimeConfig::from_app_config(
                        "demo",
                        &AppRaftConfig {
                            addr: leader_addr.clone(),
                            peers: vec![],
                            heartbeat_interval_ms: Some(50),
                            election_timeout_ms: Some(150),
                            snapshot_interval: Some(64),
                            data_dir: Some(leader_dir.path().to_string_lossy().to_string()),
                        },
                    )
                    .expect("leader config should build"),
                ),
                Some(leader_store.clone()),
            )
            .await
            .expect("leader runtime should start"),
        );
        leader_store.attach_raft_runtime(leader.clone());

        leader
            .raft()
            .wait(Some(Duration::from_secs(5)))
            .state(ServerState::Leader, "leader should become leader before forwarding test")
            .await
            .expect("leader should become leader");

        let follower = Arc::new(
            RaftRuntime::start(
                Arc::new(
                    RaftRuntimeConfig::from_app_config(
                        "demo",
                        &AppRaftConfig {
                            addr: follower_addr.clone(),
                            peers: vec![leader_addr.clone()],
                            heartbeat_interval_ms: Some(50),
                            election_timeout_ms: Some(150),
                            snapshot_interval: Some(64),
                            data_dir: Some(follower_dir.path().to_string_lossy().to_string()),
                        },
                    )
                    .expect("follower config should build"),
                ),
                Some(follower_store.clone()),
            )
            .await
            .expect("follower runtime should start"),
        );
        follower_store.attach_raft_runtime(follower.clone());

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let leader_has_follower = {
                    let metrics = leader.raft().metrics();
                    let borrowed = metrics.borrow_watched();
                    borrowed
                        .membership_config
                        .voter_ids()
                        .any(|id| id == node_id_from_addr(&follower_addr))
                };
                let follower_has_leader = {
                    let metrics = follower.raft().metrics();
                    let borrowed = metrics.borrow_watched();
                    borrowed.current_leader == Some(node_id_from_addr(&leader_addr))
                        && borrowed.state == ServerState::Follower
                };

                if leader_has_follower && follower_has_leader {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("follower should observe the elected leader");

        follower_store
            .set_pause_state(true)
            .await
            .expect("follower write should forward to leader");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if leader_store.is_paused() && follower_store.is_paused() {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("forwarded write should replicate to both nodes");
    }

    fn allocate_local_addr() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("port should allocate");
        let addr = listener.local_addr().expect("local addr should exist");
        drop(listener);
        addr.to_string()
    }
}