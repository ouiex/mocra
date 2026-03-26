use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    NodeExecutionFailed { node_id: String, reason: String },
    RetryExhausted {
        node_id: String,
        attempts: usize,
        max_retries: usize,
        retryable: bool,
        last_error: String,
    },
    TaskJoinFailed { node_id: String, reason: String },
    RunAlreadyInProgress { lock_key: String },
    RunGuardAcquireFailed { lock_key: String, reason: String },
    RunGuardRenewFailed { lock_key: String, reason: String },
    RunGuardReleaseFailed { lock_key: String, reason: String },
    MissingRunFencingToken { resource: String },
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
    NodeTimeout { node_id: String, timeout_ms: u64 },
    ExecutionTimeout { run_id: String, timeout_ms: u64 },
    ExecutionIncomplete {
        run_id: String,
        unfinished_nodes: usize,
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
                write!(f, "dag run guard acquire failed: lock_key={lock_key} reason={reason}")
            }
            DagError::RunGuardRenewFailed { lock_key, reason } => {
                write!(f, "dag run guard renew failed: lock_key={lock_key} reason={reason}")
            }
            DagError::RunGuardReleaseFailed { lock_key, reason } => {
                write!(f, "dag run guard release failed: lock_key={lock_key} reason={reason}")
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
                write!(f, "node execution timeout: node={node_id} timeout_ms={timeout_ms}")
            }
            DagError::ExecutionTimeout { run_id, timeout_ms } => {
                write!(f, "dag execution timeout: run_id={run_id} timeout_ms={timeout_ms}")
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagNodeStatus {
    Pending,
    Ready,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone)]
pub struct NodeExecutionContext {
    pub run_id: String,
    pub run_fencing_token: Option<u64>,
    pub node_id: String,
    pub attempt: usize,
    pub upstream_nodes: Vec<String>,
    pub upstream_outputs: HashMap<String, TaskPayload>,
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

#[derive(Debug, Clone, Default)]
pub struct DagRunResumeState {
    pub run_id: String,
    pub run_fencing_token: Option<u64>,
    pub succeeded_outputs: HashMap<String, TaskPayload>,
}

impl SyncAble for DagNodeSyncState {
    fn topic() -> String {
        "dag_node_sync_state".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodePlacement {
    Local,
    Remote { worker_group: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagNodeRetryMode {
    AllErrors,
    RetryableOnly,
}

#[derive(Debug, Clone)]
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
    pub executor: Option<Arc<dyn DagNodeTrait>>,
    pub result: Option<TaskPayload>,
    pub metadata: HashMap<String, String>,
    pub error: Option<String>,
}
