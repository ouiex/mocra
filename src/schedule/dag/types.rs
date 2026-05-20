use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::sync::SyncAble;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagError {
    DuplicateNode(String),
    NodeNotFound(String),
    PrecedingNodeNotFound(String),
    CycleDetected,
    EmptyGraph,
    MissingNodeCompute(String),
    NodeExecutionFailed {
        node_id: String,
        reason: String,
    },
    RetryExhausted {
        node_id: String,
        attempts: usize,
        max_retries: usize,
        retryable: bool,
        last_error: String,
    },
    TaskJoinFailed {
        node_id: String,
        reason: String,
    },
    RunAlreadyInProgress {
        lock_key: String,
    },
    RunGuardAcquireFailed {
        lock_key: String,
        reason: String,
    },
    RunGuardRenewFailed {
        lock_key: String,
        reason: String,
    },
    RunGuardReleaseFailed {
        lock_key: String,
        reason: String,
    },
    MissingRunFencingToken {
        resource: String,
    },
    FencingTokenRejected {
        resource: String,
        token: u64,
        reason: String,
    },
    InvalidPayloadEnvelope(String),
    UnsupportedPayloadVersion(u8),
    ReservedControlNode(String),
    InvalidPrecedingControlNode(String),
    RemoteDispatchNotConfigured(String),
    NodeTimeout {
        node_id: String,
        timeout_ms: u64,
    },
    ExecutionTimeout {
        run_id: String,
        timeout_ms: u64,
    },
    ExecutionIncomplete {
        run_id: String,
        unfinished_nodes: usize,
    },
    RunStopped {
        run_id: String,
        reason: String,
    },
    InvalidStateTransition {
        node_id: String,
        from: DagNodeStatus,
        to: DagNodeStatus,
    },
}

impl Display for DagError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::DuplicateNode(id) => write!(f, "duplicate node id: {id}"),
            DagError::NodeNotFound(id) => write!(f, "node not found: {id}"),
            DagError::PrecedingNodeNotFound(id) => write!(f, "preceding node not found: {id}"),
            DagError::CycleDetected => write!(f, "cycle detected in dag"),
            DagError::EmptyGraph => write!(f, "dag graph is empty"),
            DagError::MissingNodeCompute(id) => {
                write!(f, "missing compute function for node: {id}")
            }
            DagError::NodeExecutionFailed { node_id, reason } => {
                write!(f, "node execution failed: node={node_id} reason={reason}")
            }
            DagError::RetryExhausted {
                node_id,
                attempts,
                max_retries,
                retryable,
                last_error,
            } => {
                write!(
                    f,
                    "node retry exhausted: node={node_id} attempts={attempts} max_retries={max_retries} retryable={retryable} last_error={last_error}"
                )
            }
            DagError::TaskJoinFailed { node_id, reason } => {
                write!(f, "node task join failed: node={node_id} reason={reason}")
            }
            DagError::RunAlreadyInProgress { lock_key } => {
                write!(f, "dag run already in progress: lock_key={lock_key}")
            }
            DagError::RunGuardAcquireFailed { lock_key, reason } => {
                write!(
                    f,
                    "dag run guard acquire failed: lock_key={lock_key} reason={reason}"
                )
            }
            DagError::RunGuardRenewFailed { lock_key, reason } => {
                write!(
                    f,
                    "dag run guard renew failed: lock_key={lock_key} reason={reason}"
                )
            }
            DagError::RunGuardReleaseFailed { lock_key, reason } => {
                write!(
                    f,
                    "dag run guard release failed: lock_key={lock_key} reason={reason}"
                )
            }
            DagError::MissingRunFencingToken { resource } => {
                write!(f, "missing run fencing token for resource: {resource}")
            }
            DagError::FencingTokenRejected {
                resource,
                token,
                reason,
            } => {
                write!(
                    f,
                    "fencing token rejected: resource={resource} token={token} reason={reason}"
                )
            }
            DagError::InvalidPayloadEnvelope(reason) => {
                write!(f, "invalid payload envelope: {reason}")
            }
            DagError::UnsupportedPayloadVersion(version) => {
                write!(f, "unsupported payload version: {version}")
            }
            DagError::ReservedControlNode(id) => {
                write!(f, "node id is reserved as control node: {id}")
            }
            DagError::InvalidPrecedingControlNode(id) => {
                write!(f, "invalid preceding control node: {id}")
            }
            DagError::RemoteDispatchNotConfigured(id) => {
                write!(f, "remote dispatch not configured for node: {id}")
            }
            DagError::NodeTimeout {
                node_id,
                timeout_ms,
            } => {
                write!(
                    f,
                    "node execution timeout: node={node_id} timeout_ms={timeout_ms}"
                )
            }
            DagError::ExecutionTimeout { run_id, timeout_ms } => {
                write!(
                    f,
                    "dag execution timeout: run_id={run_id} timeout_ms={timeout_ms}"
                )
            }
            DagError::ExecutionIncomplete {
                run_id,
                unfinished_nodes,
            } => {
                write!(
                    f,
                    "dag execution incomplete: run_id={run_id} unfinished_nodes={unfinished_nodes}"
                )
            }
            DagError::RunStopped { run_id, reason } => {
                write!(f, "dag run stopped: run_id={run_id} reason={reason}")
            }
            DagError::InvalidStateTransition { node_id, from, to } => {
                write!(
                    f,
                    "invalid node state transition: node={node_id} from={from:?} to={to:?}"
                )
            }
        }
    }
}

impl Error for DagError {}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskPayload {
    pub bytes: Vec<u8>,
    pub content_type: Option<String>,
    pub encoding: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl TaskPayload {
    const ENVELOPE_MAGIC: [u8; 4] = *b"MDAG";
    const ENVELOPE_VERSION: u8 = 1;

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            content_type: None,
            encoding: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_content_type(mut self, content_type: impl AsRef<str>) -> Self {
        self.content_type = Some(content_type.as_ref().to_string());
        self
    }

    pub fn with_encoding(mut self, encoding: impl AsRef<str>) -> Self {
        self.encoding = Some(encoding.as_ref().to_string());
        self
    }

    pub fn with_meta(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.metadata
            .insert(key.as_ref().to_string(), value.as_ref().to_string());
        self
    }

    pub fn to_envelope_bytes(&self) -> Result<Vec<u8>, DagError> {
        fn to_u16_len(len: usize, field: &str) -> Result<u16, DagError> {
            u16::try_from(len)
                .map_err(|_| DagError::InvalidPayloadEnvelope(format!("{field} too large")))
        }
        fn to_u32_len(len: usize, field: &str) -> Result<u32, DagError> {
            u32::try_from(len)
                .map_err(|_| DagError::InvalidPayloadEnvelope(format!("{field} too large")))
        }

        let ct_bytes = self.content_type.as_deref().unwrap_or("").as_bytes();
        let enc_bytes = self.encoding.as_deref().unwrap_or("").as_bytes();

        let ct_len = to_u16_len(ct_bytes.len(), "content_type")?;
        let enc_len = to_u16_len(enc_bytes.len(), "encoding")?;
        let meta_count = to_u16_len(self.metadata.len(), "metadata_count")?;
        let data_len = to_u32_len(self.bytes.len(), "bytes")?;

        let mut out = Vec::new();
        out.extend_from_slice(&Self::ENVELOPE_MAGIC);
        out.push(Self::ENVELOPE_VERSION);
        out.extend_from_slice(&ct_len.to_le_bytes());
        out.extend_from_slice(&enc_len.to_le_bytes());
        out.extend_from_slice(&meta_count.to_le_bytes());
        out.extend_from_slice(&data_len.to_le_bytes());
        out.extend_from_slice(ct_bytes);
        out.extend_from_slice(enc_bytes);

        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();

        for key in keys {
            let value = self
                .metadata
                .get(key)
                .ok_or_else(|| DagError::InvalidPayloadEnvelope("metadata key missing".into()))?;
            let k = key.as_bytes();
            let v = value.as_bytes();
            let k_len = to_u16_len(k.len(), "metadata_key")?;
            let v_len = to_u16_len(v.len(), "metadata_value")?;
            out.extend_from_slice(&k_len.to_le_bytes());
            out.extend_from_slice(&v_len.to_le_bytes());
            out.extend_from_slice(k);
            out.extend_from_slice(v);
        }

        out.extend_from_slice(&self.bytes);
        Ok(out)
    }

    pub fn from_envelope_bytes(input: &[u8]) -> Result<Self, DagError> {
        struct Cursor<'a> {
            buf: &'a [u8],
            pos: usize,
        }
        impl<'a> Cursor<'a> {
            fn new(buf: &'a [u8]) -> Self {
                Self { buf, pos: 0 }
            }
            fn read_exact(&mut self, n: usize) -> Result<&'a [u8], DagError> {
                if self.pos + n > self.buf.len() {
                    return Err(DagError::InvalidPayloadEnvelope("unexpected eof".into()));
                }
                let s = &self.buf[self.pos..self.pos + n];
                self.pos += n;
                Ok(s)
            }
            fn read_u8(&mut self) -> Result<u8, DagError> {
                Ok(self.read_exact(1)?[0])
            }
            fn read_u16(&mut self) -> Result<u16, DagError> {
                let b = self.read_exact(2)?;
                Ok(u16::from_le_bytes([b[0], b[1]]))
            }
            fn read_u32(&mut self) -> Result<u32, DagError> {
                let b = self.read_exact(4)?;
                Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            }
            fn read_str(&mut self, len: usize) -> Result<String, DagError> {
                let b = self.read_exact(len)?;
                std::str::from_utf8(b)
                    .map(|s| s.to_string())
                    .map_err(|_| DagError::InvalidPayloadEnvelope("invalid utf8".into()))
            }
        }

        let mut c = Cursor::new(input);
        let magic = c.read_exact(4)?;
        if magic != Self::ENVELOPE_MAGIC {
            return Err(DagError::InvalidPayloadEnvelope("bad magic".into()));
        }

        let version = c.read_u8()?;
        if version != Self::ENVELOPE_VERSION {
            return Err(DagError::UnsupportedPayloadVersion(version));
        }

        let ct_len = c.read_u16()? as usize;
        let enc_len = c.read_u16()? as usize;
        let meta_count = c.read_u16()? as usize;
        let data_len = c.read_u32()? as usize;

        let content_type = {
            let s = c.read_str(ct_len)?;
            if s.is_empty() { None } else { Some(s) }
        };
        let encoding = {
            let s = c.read_str(enc_len)?;
            if s.is_empty() { None } else { Some(s) }
        };

        let mut metadata = HashMap::new();
        for _ in 0..meta_count {
            let k_len = c.read_u16()? as usize;
            let v_len = c.read_u16()? as usize;
            let key = c.read_str(k_len)?;
            let value = c.read_str(v_len)?;
            metadata.insert(key, value);
        }

        let bytes = c.read_exact(data_len)?.to_vec();
        if c.pos != input.len() {
            return Err(DagError::InvalidPayloadEnvelope("trailing bytes".into()));
        }

        Ok(Self {
            bytes,
            content_type,
            encoding,
            metadata,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DagNodeStatus {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DagRunStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagStopSignal {
    pub requested_by: String,
    pub reason: String,
    pub requested_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagAdvanceGate {
    pub successor_node: String,
    pub expected_predecessors: Vec<String>,
    pub arrived_predecessors: Vec<String>,
    pub opened_at_ms: Option<u64>,
}

impl DagAdvanceGate {
    pub fn new(successor_node: impl Into<String>, expected_predecessors: Vec<String>) -> Self {
        let expected_predecessors = sorted_unique(expected_predecessors);
        let opened_at_ms = expected_predecessors.is_empty().then_some(0);
        Self {
            successor_node: successor_node.into(),
            expected_predecessors,
            arrived_predecessors: Vec::new(),
            opened_at_ms,
        }
    }

    pub fn is_open(&self) -> bool {
        self.opened_at_ms.is_some()
            || self.expected_predecessors.is_empty()
            || self.arrived_predecessors.len() == self.expected_predecessors.len()
    }

    pub fn record_arrival(
        &mut self,
        predecessor: impl AsRef<str>,
        timestamp_ms: u64,
    ) -> bool {
        let predecessor = predecessor.as_ref();
        if !self
            .expected_predecessors
            .iter()
            .any(|expected| expected == predecessor)
        {
            return self.is_open();
        }

        if !self
            .arrived_predecessors
            .iter()
            .any(|arrived| arrived == predecessor)
        {
            self.arrived_predecessors.push(predecessor.to_string());
            self.arrived_predecessors.sort();
        }

        if self.opened_at_ms.is_none()
            && self.arrived_predecessors.len() == self.expected_predecessors.len()
        {
            self.opened_at_ms = Some(timestamp_ms);
        }

        self.is_open()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagNodeRunState {
    pub node_id: String,
    pub status: DagNodeStatus,
    pub attempt: usize,
    pub placement: NodePlacement,
    pub worker_id: Option<String>,
    pub worker_group: Option<String>,
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

impl DagNodeRunState {
    pub fn new(
        node_id: impl Into<String>,
        status: DagNodeStatus,
        placement: NodePlacement,
        updated_at_ms: u64,
    ) -> Self {
        let worker_group = match &placement {
            NodePlacement::Local => None,
            NodePlacement::Remote { worker_group } => Some(worker_group.clone()),
        };

        Self {
            node_id: node_id.into(),
            status,
            attempt: 0,
            placement,
            worker_id: None,
            worker_group,
            last_error: None,
            updated_at_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NodeExecutionContext {
    pub run_id: String,
    pub run_fencing_token: Option<u64>,
    pub node_id: String,
    pub attempt: usize,
    pub upstream_nodes: Vec<String>,
    pub upstream_outputs: HashMap<String, TaskPayload>,
    /// Opaque runtime input reserved for future bridges from typed engine runtime
    /// into scheduler execution. Kept generic so `schedule::dag` stays decoupled
    /// from `common::model` transport types.
    pub runtime_input: Option<TaskPayload>,
    pub layer_index: usize,
}

#[async_trait]
pub trait DagNodeTrait: Send + Sync {
    async fn start(&self, context: NodeExecutionContext) -> Result<TaskPayload, DagError>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DagErrorCode {
    DuplicateNode,
    NodeNotFound,
    PrecedingNodeNotFound,
    CycleDetected,
    EmptyGraph,
    MissingNodeCompute,
    NodeExecutionFailed,
    RetryExhausted,
    TaskJoinFailed,
    RunAlreadyInProgress,
    RunGuardAcquireFailed,
    RunGuardRenewFailed,
    RunGuardReleaseFailed,
    MissingRunFencingToken,
    FencingTokenRejected,
    InvalidPayloadEnvelope,
    UnsupportedPayloadVersion,
    ReservedControlNode,
    InvalidPrecedingControlNode,
    RemoteDispatchNotConfigured,
    NodeTimeout,
    ExecutionTimeout,
    ExecutionIncomplete,
    RunStopped,
    InvalidStateTransition,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DagErrorClass {
    Retryable,
    NonRetryable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNodeSyncState {
    pub run_id: String,
    pub run_fencing_token: Option<u64>,
    pub node_id: String,
    pub stage: String,
    pub placement: String,
    pub worker_group: Option<String>,
    pub worker_id: Option<String>,
    pub attempt: usize,
    pub layer_index: usize,
    pub idempotency_key: Option<String>,
    pub deadline_ms: Option<u64>,
    pub dispatch_latency_ms: Option<u64>,
    pub error_code: Option<DagErrorCode>,
    pub error_class: Option<DagErrorClass>,
    pub error: Option<String>,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagRunResumeState {
    pub run_id: String,
    pub run_fencing_token: Option<u64>,
    pub runtime_identity: HashMap<String, String>,
    pub succeeded_outputs: HashMap<String, TaskPayload>,
}

impl DagRunResumeState {
    pub fn into_run_state(self) -> DagRunState {
        DagRunState::from_resume_state(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagRunState {
    pub run_id: String,
    pub run_fencing_token: Option<u64>,
    pub runtime_identity: HashMap<String, String>,
    pub status: DagRunStatus,
    pub version: u64,
    pub entry_nodes: Vec<String>,
    pub ready_nodes: Vec<String>,
    pub completed_nodes: Vec<String>,
    pub succeeded_outputs: HashMap<String, TaskPayload>,
    pub node_states: HashMap<String, DagNodeRunState>,
    pub advance_gates: HashMap<String, DagAdvanceGate>,
    pub stop_signal: Option<DagStopSignal>,
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

impl Default for DagRunState {
    fn default() -> Self {
        Self {
            run_id: String::new(),
            run_fencing_token: None,
            runtime_identity: HashMap::new(),
            status: DagRunStatus::Pending,
            version: 0,
            entry_nodes: Vec::new(),
            ready_nodes: Vec::new(),
            completed_nodes: Vec::new(),
            succeeded_outputs: HashMap::new(),
            node_states: HashMap::new(),
            advance_gates: HashMap::new(),
            stop_signal: None,
            last_error: None,
            updated_at_ms: 0,
        }
    }
}

impl DagRunState {
    pub fn new(
        run_id: impl Into<String>,
        run_fencing_token: Option<u64>,
        entry_nodes: Vec<String>,
        updated_at_ms: u64,
    ) -> Self {
        let entry_nodes = sorted_unique(entry_nodes);
        Self {
            run_id: run_id.into(),
            run_fencing_token,
            status: DagRunStatus::Running,
            version: 1,
            ready_nodes: entry_nodes.clone(),
            entry_nodes,
            updated_at_ms,
            ..Self::default()
        }
    }

    pub fn from_resume_state(resume: DagRunResumeState) -> Self {
        let mut completed_nodes: Vec<String> = resume.succeeded_outputs.keys().cloned().collect();
        completed_nodes.sort();
        let node_states = completed_nodes
            .iter()
            .map(|node_id| {
                (
                    node_id.clone(),
                    DagNodeRunState::new(
                        node_id.clone(),
                        DagNodeStatus::Succeeded,
                        NodePlacement::Local,
                        0,
                    ),
                )
            })
            .collect();

        Self {
            run_id: resume.run_id,
            run_fencing_token: resume.run_fencing_token,
            runtime_identity: resume.runtime_identity,
            status: DagRunStatus::Running,
            version: 1,
            entry_nodes: Vec::new(),
            ready_nodes: Vec::new(),
            completed_nodes,
            succeeded_outputs: resume.succeeded_outputs,
            node_states,
            advance_gates: HashMap::new(),
            stop_signal: None,
            last_error: None,
            updated_at_ms: 0,
        }
    }

    pub fn to_resume_state(&self) -> DagRunResumeState {
        DagRunResumeState {
            run_id: self.run_id.clone(),
            run_fencing_token: self.run_fencing_token,
            runtime_identity: self.runtime_identity.clone(),
            succeeded_outputs: self.succeeded_outputs.clone(),
        }
    }

    pub fn mark_ready(&mut self, node_id: impl AsRef<str>, updated_at_ms: u64) {
        let node_id = node_id.as_ref().to_string();
        if !self.ready_nodes.iter().any(|ready| ready == &node_id) {
            self.ready_nodes.push(node_id.clone());
            self.ready_nodes.sort();
        }
        self.node_states.insert(
            node_id.clone(),
            DagNodeRunState::new(
                node_id,
                DagNodeStatus::Ready,
                NodePlacement::Local,
                updated_at_ms,
            ),
        );
        self.updated_at_ms = updated_at_ms;
        self.version += 1;
    }

    pub fn mark_completed(
        &mut self,
        node_id: impl AsRef<str>,
        payload: TaskPayload,
        updated_at_ms: u64,
    ) {
        let node_id = node_id.as_ref().to_string();
        self.ready_nodes.retain(|ready| ready != &node_id);
        if !self.completed_nodes.iter().any(|completed| completed == &node_id) {
            self.completed_nodes.push(node_id.clone());
            self.completed_nodes.sort();
        }
        self.succeeded_outputs.insert(node_id.clone(), payload);
        self.node_states.insert(
            node_id.clone(),
            DagNodeRunState::new(
                node_id,
                DagNodeStatus::Succeeded,
                NodePlacement::Local,
                updated_at_ms,
            ),
        );
        self.updated_at_ms = updated_at_ms;
        self.version += 1;
    }

    pub fn mark_failed(
        &mut self,
        node_id: impl AsRef<str>,
        error: impl Into<String>,
        updated_at_ms: u64,
    ) {
        let node_id = node_id.as_ref().to_string();
        self.last_error = Some(error.into());
        self.status = DagRunStatus::Failed;
        self.node_states.insert(
            node_id.clone(),
            DagNodeRunState {
                last_error: self.last_error.clone(),
                ..DagNodeRunState::new(
                    node_id,
                    DagNodeStatus::Failed,
                    NodePlacement::Local,
                    updated_at_ms,
                )
            },
        );
        self.updated_at_ms = updated_at_ms;
        self.version += 1;
    }

    pub fn apply_stop_signal(&mut self, signal: DagStopSignal) {
        self.stop_signal = Some(signal);
        self.status = DagRunStatus::Stopped;
        self.version += 1;
    }
}

fn sorted_unique(values: Vec<String>) -> Vec<String> {
    BTreeSet::<String>::from_iter(values).into_iter().collect()
}

impl SyncAble for DagNodeSyncState {
    fn topic() -> String {
        "dag_node_sync_state".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodePlacement {
    Local,
    Remote { worker_group: String },
}

impl NodePlacement {
    pub fn local() -> Self {
        Self::Local
    }

    pub fn remote(worker_group: impl Into<String>) -> Self {
        Self::Remote {
            worker_group: worker_group.into(),
        }
    }

    pub fn placement_name(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote { .. } => "remote",
        }
    }

    pub fn worker_group(&self) -> Option<&str> {
        match self {
            Self::Local => None,
            Self::Remote { worker_group } => Some(worker_group.as_str()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DagNodeRetryMode {
    AllErrors,
    RetryableOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagNodeExecutionPolicy {
    pub max_retries: usize,
    pub timeout_ms: Option<u64>,
    pub retry_backoff_ms: u64,
    pub idempotency_key: Option<String>,
    pub retry_mode: DagNodeRetryMode,
    pub circuit_breaker_failure_threshold: Option<usize>,
    pub circuit_breaker_open_ms: u64,
}

impl Default for DagNodeExecutionPolicy {
    fn default() -> Self {
        Self {
            max_retries: 0,
            timeout_ms: None,
            retry_backoff_ms: 0,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::AllErrors,
            circuit_breaker_failure_threshold: None,
            circuit_breaker_open_ms: 0,
        }
    }
}

#[async_trait]
pub trait DagNodeDispatcher: Send + Sync {
    fn backend_name(&self) -> &'static str {
        "custom"
    }

    fn supports_placement(&self, _placement: &NodePlacement) -> bool {
        true
    }

    async fn dispatch(
        &self,
        node_id: &str,
        placement: &NodePlacement,
        executor: Arc<dyn DagNodeTrait>,
        context: NodeExecutionContext,
    ) -> Result<TaskPayload, DagError>;
}

#[async_trait]
pub trait DagFencingStore: Send + Sync {
    async fn commit(
        &self,
        resource: &str,
        token: u64,
        node_id: &str,
        payload: &TaskPayload,
    ) -> Result<(), DagError>;
}

#[async_trait]
pub trait DagRunStateStore: Send + Sync {
    async fn load(&self, run_key: &str) -> Result<Option<DagRunResumeState>, DagError>;
    async fn save(&self, run_key: &str, state: &DagRunResumeState) -> Result<(), DagError>;
    async fn clear(&self, run_key: &str) -> Result<(), DagError>;

    async fn load_state(&self, run_key: &str) -> Result<Option<DagRunState>, DagError> {
        Ok(self.load(run_key).await?.map(DagRunResumeState::into_run_state))
    }

    async fn save_state(&self, run_key: &str, state: &DagRunState) -> Result<(), DagError> {
        let resume = state.to_resume_state();
        self.save(run_key, &resume).await
    }

    async fn request_stop(&self, run_key: &str, signal: DagStopSignal) -> Result<(), DagError> {
        let mut state = self.load_state(run_key).await?.unwrap_or_else(|| DagRunState {
            run_id: run_key.to_string(),
            ..DagRunState::default()
        });
        state.apply_stop_signal(signal);
        self.save_state(run_key, &state).await
    }
}

#[derive(Debug, Clone)]
pub struct DagRunGuardAcquireOutcome {
    pub acquired: bool,
    pub fencing_token: Option<u64>,
}

#[async_trait]
pub trait DagRunGuard: Send + Sync {
    async fn try_acquire(
        &self,
        lock_key: &str,
        owner: &str,
        ttl_ms: u64,
    ) -> Result<DagRunGuardAcquireOutcome, DagError>;
    async fn renew(&self, lock_key: &str, owner: &str, ttl_ms: u64) -> Result<bool, DagError>;
    async fn release(&self, lock_key: &str, owner: &str) -> Result<(), DagError>;
}

#[derive(Default)]
pub struct LocalNodeDispatcher;

#[async_trait]
impl DagNodeDispatcher for LocalNodeDispatcher {
    fn backend_name(&self) -> &'static str {
        "local"
    }

    fn supports_placement(&self, placement: &NodePlacement) -> bool {
        matches!(placement, NodePlacement::Local)
    }

    async fn dispatch(
        &self,
        node_id: &str,
        placement: &NodePlacement,
        executor: Arc<dyn DagNodeTrait>,
        context: NodeExecutionContext,
    ) -> Result<TaskPayload, DagError> {
        match placement {
            NodePlacement::Local => executor.start(context).await,
            NodePlacement::Remote { .. } => {
                Err(DagError::RemoteDispatchNotConfigured(node_id.to_string()))
            }
        }
    }
}

#[derive(Clone)]
pub struct DagNodeRecord {
    pub id: String,
    pub predecessors: Vec<String>,
    pub status: DagNodeStatus,
    pub placement: NodePlacement,
    pub execution_policy: DagNodeExecutionPolicy,
    pub runtime_input: Option<TaskPayload>,
    pub executor: Option<Arc<dyn DagNodeTrait>>,
    pub result: Option<TaskPayload>,
    pub metadata: HashMap<String, String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DagNodeRuntimeOverride {
    pub node_id: String,
    #[serde(default)]
    pub placement: Option<NodePlacement>,
    #[serde(default)]
    pub execution_policy: Option<DagNodeExecutionPolicy>,
    #[serde(default)]
    pub runtime_input: Option<TaskPayload>,
}

impl DagNodeRuntimeOverride {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            placement: None,
            execution_policy: None,
            runtime_input: None,
        }
    }

    pub fn with_placement(mut self, placement: NodePlacement) -> Self {
        self.placement = Some(placement);
        self
    }

    pub fn with_execution_policy(mut self, execution_policy: DagNodeExecutionPolicy) -> Self {
        self.execution_policy = Some(execution_policy);
        self
    }

    pub fn with_runtime_input(mut self, runtime_input: TaskPayload) -> Self {
        self.runtime_input = Some(runtime_input);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.placement.is_none() && self.execution_policy.is_none() && self.runtime_input.is_none()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn dag_advance_gate_opens_after_all_predecessors_arrive() {
        let mut gate = DagAdvanceGate::new(
            "detail",
            vec!["page_b".to_string(), "page_a".to_string(), "page_a".to_string()],
        );

        assert!(!gate.is_open());
        assert!(!gate.record_arrival("page_a", 10));
        assert!(!gate.record_arrival("unknown", 11));
        assert!(gate.record_arrival("page_b", 12));
        assert_eq!(
            gate.expected_predecessors,
            vec!["page_a".to_string(), "page_b".to_string()]
        );
        assert_eq!(
            gate.arrived_predecessors,
            vec!["page_a".to_string(), "page_b".to_string()]
        );
        assert_eq!(gate.opened_at_ms, Some(12));
    }

    #[test]
    fn dag_run_state_tracks_ready_completed_and_stop_signal() {
        let mut state = DagRunState::new(
            "run-1",
            Some(7),
            vec!["entry_b".to_string(), "entry_a".to_string(), "entry_a".to_string()],
            100,
        );

        assert_eq!(state.status, DagRunStatus::Running);
        assert_eq!(
            state.ready_nodes,
            vec!["entry_a".to_string(), "entry_b".to_string()]
        );

        state.mark_completed("entry_a", TaskPayload::from_bytes(vec![1, 2, 3]), 200);
        state.apply_stop_signal(DagStopSignal {
            requested_by: "tester".to_string(),
            reason: "manual stop".to_string(),
            requested_at_ms: 300,
        });

        assert_eq!(state.status, DagRunStatus::Stopped);
        assert_eq!(state.completed_nodes, vec!["entry_a".to_string()]);
        assert_eq!(state.ready_nodes, vec!["entry_b".to_string()]);
        assert_eq!(
            state
                .node_states
                .get("entry_a")
                .map(|node| node.status.clone()),
            Some(DagNodeStatus::Succeeded)
        );
        assert_eq!(
            state.stop_signal.as_ref().map(|signal| signal.reason.as_str()),
            Some("manual stop")
        );
    }

    #[derive(Default)]
    struct ResumeOnlyStore {
        state: Mutex<HashMap<String, DagRunResumeState>>,
    }

    #[async_trait]
    impl DagRunStateStore for ResumeOnlyStore {
        async fn load(&self, run_key: &str) -> Result<Option<DagRunResumeState>, DagError> {
            Ok(self.state.lock().expect("lock poisoned").get(run_key).cloned())
        }

        async fn save(&self, run_key: &str, state: &DagRunResumeState) -> Result<(), DagError> {
            self.state
                .lock()
                .expect("lock poisoned")
                .insert(run_key.to_string(), state.clone());
            Ok(())
        }

        async fn clear(&self, run_key: &str) -> Result<(), DagError> {
            self.state.lock().expect("lock poisoned").remove(run_key);
            Ok(())
        }
    }

    #[tokio::test]
    async fn dag_run_state_store_bridges_resume_and_state_models() {
        let store = ResumeOnlyStore::default();
        let mut state = DagRunState::new("run-bridge", Some(3), vec!["entry".to_string()], 50);
        state.mark_completed("entry", TaskPayload::from_bytes(vec![9]), 60);

        store.save_state("bridge-key", &state).await.unwrap();

        let loaded = store
            .load_state("bridge-key")
            .await
            .unwrap()
            .expect("state should exist");
        assert_eq!(loaded.run_id, "run-bridge");
        assert_eq!(loaded.run_fencing_token, Some(3));
        assert_eq!(loaded.completed_nodes, vec!["entry".to_string()]);
        assert!(loaded.succeeded_outputs.contains_key("entry"));
    }

    #[test]
    fn node_placement_helpers_report_expected_labels() {
        let local = NodePlacement::local();
        let remote = NodePlacement::remote("wg-a");

        assert_eq!(local.placement_name(), "local");
        assert_eq!(local.worker_group(), None);
        assert_eq!(remote.placement_name(), "remote");
        assert_eq!(remote.worker_group(), Some("wg-a"));
    }
}
