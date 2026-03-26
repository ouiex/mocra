use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use futures::FutureExt;
use uuid::Uuid;

use crate::sync::SyncService;

use super::graph::Dag;
use super::types::{
    DagError, DagErrorClass, DagErrorCode, DagFencingStore, DagNodeDispatcher,
    DagNodeExecutionPolicy, DagNodeRetryMode, DagNodeStatus, DagNodeSyncState, DagRunGuard,
    DagRunResumeState, DagRunStateStore, LocalNodeDispatcher, NodeExecutionContext,
    NodePlacement, TaskPayload,
};

type JoinOutput = (String, usize, usize, Result<TaskPayload, DagError>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DagSyncStage {
    Running,
    Succeeded,
    Failed,
    RetryScheduled,
    CacheHit,
    SingleflightWait,
    SingleflightFulfilled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeFailureClass {
    Retryable,
    NonRetryable,
}

impl DagSyncStage {
    fn as_str(self) -> &'static str {
        match self {
            DagSyncStage::Running => "running",
            DagSyncStage::Succeeded => "succeeded",
            DagSyncStage::Failed => "failed",
            DagSyncStage::RetryScheduled => "retry_scheduled",
            DagSyncStage::CacheHit => "cache_hit",
            DagSyncStage::SingleflightWait => "singleflight_wait",
            DagSyncStage::SingleflightFulfilled => "singleflight_fulfilled",
        }
    }
}

struct RuntimeState {
    runtime: Dag,
    outputs: HashMap<String, TaskPayload>,
    node_results: Vec<NodeExecutionResult>,
    layers_by_depth: BTreeMap<usize, Vec<String>>,
    run_id: String,
    run_fencing_token: Option<u64>,
    attempts: HashMap<String, usize>,
    idempotent_results: HashMap<String, TaskPayload>,
    inflight_idempotency_owner: HashMap<String, String>,
    idempotency_waiters: HashMap<String, Vec<(String, usize)>>,
    remaining_preds: HashMap<String, usize>,
    node_depth: HashMap<String, usize>,
    ready_queue: VecDeque<String>,
    ready_set: HashSet<String>,
    consecutive_failures: HashMap<String, usize>,
    dispatch_started_ms: HashMap<String, u64>,
}

struct PreparedDispatch {
    node_id: String,
    layer_index: usize,
    attempt: usize,
    placement: NodePlacement,
    policy: super::types::DagNodeExecutionPolicy,
    context: NodeExecutionContext,
}

impl RuntimeState {
    fn new_with_resume(dag: &Dag, resume: Option<&DagRunResumeState>) -> Self {
        let mut runtime = dag.clone();
        runtime.reset_runtime();

        let resumed_outputs: HashMap<String, TaskPayload> = resume
            .map(|r| r.succeeded_outputs.clone())
            .unwrap_or_default();
        let resumed_nodes: std::collections::HashSet<String> =
            resumed_outputs.keys().cloned().collect();

        for node_id in &resumed_nodes {
            if let Some(node) = runtime.nodes.get_mut(node_id) {
                node.status = DagNodeStatus::Succeeded;
                if let Some(payload) = resumed_outputs.get(node_id) {
                    node.result = Some(payload.clone());
                }
            }
        }

        let remaining_preds: HashMap<String, usize> = runtime
            .nodes
            .iter()
            .map(|(id, node)| {
                if resumed_nodes.contains(id) {
                    return (id.clone(), 0usize);
                }
                let pending_preds = node
                    .predecessors
                    .iter()
                    .filter(|p| !resumed_nodes.contains(*p))
                    .count();
                (id.clone(), pending_preds)
            })
            .collect();
        let mut ready_nodes: Vec<String> = remaining_preds
            .iter()
            .filter_map(|(id, cnt)| {
                if *cnt == 0 && !resumed_nodes.contains(id) {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        ready_nodes.sort();
        let ready_set = ready_nodes.iter().cloned().collect();
        let ready_queue: VecDeque<String> = ready_nodes.into();

        Self {
            runtime,
            outputs: resumed_outputs,
            node_results: Vec::new(),
            layers_by_depth: BTreeMap::new(),
            run_id: resume
                .map(|r| r.run_id.clone())
                .unwrap_or_else(|| Uuid::now_v7().to_string()),
            run_fencing_token: resume.and_then(|r| r.run_fencing_token),
            attempts: HashMap::new(),
            idempotent_results: HashMap::new(),
            inflight_idempotency_owner: HashMap::new(),
            idempotency_waiters: HashMap::new(),
            remaining_preds,
            node_depth: HashMap::new(),
            ready_queue,
            ready_set,
            consecutive_failures: HashMap::new(),
            dispatch_started_ms: HashMap::new(),
        }
    }

    fn enqueue_ready(&mut self, node_id: String) {
        if self.ready_set.insert(node_id.clone()) {
            self.ready_queue.push_back(node_id);
        }
    }

    fn pop_ready(&mut self) -> Option<String> {
        let node_id = self.ready_queue.pop_front()?;
        self.ready_set.remove(&node_id);
        Some(node_id)
    }

    fn snapshot(&self) -> DagRunResumeState {
        let mut succeeded_outputs = HashMap::new();
        for (id, node) in &self.runtime.nodes {
            if node.status == DagNodeStatus::Succeeded {
                if let Some(payload) = self.outputs.get(id).cloned().or_else(|| node.result.clone()) {
                    succeeded_outputs.insert(id.clone(), payload);
                }
            }
        }

        DagRunResumeState {
            run_id: self.run_id.clone(),
            run_fencing_token: self.run_fencing_token,
            succeeded_outputs,
        }
    }

    fn into_report(self) -> Result<DagExecutionReport, DagError> {
        let end_status = self
            .runtime
            .nodes
            .get(Dag::CONTROL_END_NODE)
            .map(|n| n.status.clone())
            .unwrap_or(DagNodeStatus::Pending);

        if end_status != DagNodeStatus::Succeeded {
            let unfinished_nodes = self
                .runtime
                .nodes
                .values()
                .filter(|n| n.status != DagNodeStatus::Succeeded)
                .count();
            return Err(DagError::ExecutionIncomplete {
                run_id: self.run_id,
                unfinished_nodes,
            });
        }

        let node_states = self
            .runtime
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.status.clone()))
            .collect();

        Ok(DagExecutionReport {
            outputs: self.outputs,
            node_results: self.node_results,
            layers: self.layers_by_depth.into_values().collect(),
            node_states,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NodeExecutionResult {
    pub node_id: String,
    pub payload: TaskPayload,
    pub layer_index: usize,
}

#[derive(Debug, Clone, Default)]
pub struct DagExecutionReport {
    pub outputs: HashMap<String, TaskPayload>,
    pub node_results: Vec<NodeExecutionResult>,
    pub layers: Vec<Vec<String>>,
    pub node_states: HashMap<String, DagNodeStatus>,
}

#[derive(Clone)]
pub struct DagScheduler {
    dag: Dag,
    dispatcher: Arc<dyn DagNodeDispatcher>,
    options: DagSchedulerOptions,
    sync_service: Option<SyncService>,
    run_guard: Option<RunGuardConfig>,
    fencing_store: Option<FencingStoreConfig>,
    run_state_store: Option<RunStateStoreConfig>,
}

#[derive(Clone)]
struct RunGuardConfig {
    guard: Arc<dyn DagRunGuard>,
    lock_key: String,
    ttl_ms: u64,
    renew_interval_ms: Option<u64>,
    renew_jitter_pct: u8,
}

#[derive(Clone)]
struct FencingStoreConfig {
    store: Arc<dyn DagFencingStore>,
    resource: String,
}

#[derive(Clone)]
struct RunStateStoreConfig {
    store: Arc<dyn DagRunStateStore>,
    run_key: String,
}

#[derive(Debug, Clone)]
pub struct DagSchedulerOptions {
    pub max_in_flight: usize,
    pub cancel_inflight_on_failure: bool,
    pub run_timeout_ms: Option<u64>,
}

impl Default for DagSchedulerOptions {
    fn default() -> Self {
        Self {
            max_in_flight: usize::MAX,
            cancel_inflight_on_failure: true,
            run_timeout_ms: None,
        }
    }
}

impl DagScheduler {
    pub fn new(dag: Dag) -> Self {
        Self {
            dag,
            dispatcher: Arc::new(LocalNodeDispatcher),
            options: DagSchedulerOptions::default(),
            sync_service: None,
            run_guard: None,
            fencing_store: None,
            run_state_store: None,
        }
    }

    pub fn with_dispatcher(mut self, dispatcher: Arc<dyn DagNodeDispatcher>) -> Self {
        self.dispatcher = dispatcher;
        self
    }

    pub fn with_options(mut self, options: DagSchedulerOptions) -> Self {
        self.options = options;
        self
    }

    pub fn with_sync_service(mut self, sync_service: SyncService) -> Self {
        self.sync_service = Some(sync_service);
        self
    }

    pub fn with_run_guard(
        mut self,
        guard: Arc<dyn DagRunGuard>,
        lock_key: impl AsRef<str>,
        ttl_ms: u64,
    ) -> Self {
        self.run_guard = Some(RunGuardConfig {
            guard,
            lock_key: lock_key.as_ref().to_string(),
            ttl_ms,
            renew_interval_ms: if ttl_ms == 0 {
                None
            } else {
                Some((ttl_ms / 3).max(50))
            },
            renew_jitter_pct: 20,
        });
        self
    }

    pub fn with_run_guard_heartbeat_ms(mut self, renew_interval_ms: Option<u64>) -> Self {
        if let Some(cfg) = self.run_guard.as_mut() {
            cfg.renew_interval_ms = renew_interval_ms;
        }
        self
    }

    pub fn with_run_guard_heartbeat_jitter_pct(mut self, renew_jitter_pct: u8) -> Self {
        if let Some(cfg) = self.run_guard.as_mut() {
            cfg.renew_jitter_pct = renew_jitter_pct.min(90);
        }
        self
    }

    pub fn with_fencing_store(
        mut self,
        store: Arc<dyn DagFencingStore>,
        resource: impl AsRef<str>,
    ) -> Self {
        self.fencing_store = Some(FencingStoreConfig {
            store,
            resource: resource.as_ref().to_string(),
        });
        self
    }

    pub fn with_run_state_store(
        mut self,
        store: Arc<dyn DagRunStateStore>,
        run_key: impl AsRef<str>,
    ) -> Self {
        self.run_state_store = Some(RunStateStoreConfig {
            store,
            run_key: run_key.as_ref().to_string(),
        });
        self
    }

    fn run_guard_jittered_interval_ms(base_ms: u64, jitter_pct: u8) -> u64 {
        if base_ms == 0 || jitter_pct == 0 {
            return base_ms;
        }
        let jitter = ((base_ms as u128) * (jitter_pct as u128) / 100) as u64;
        if jitter == 0 {
            return base_ms;
        }
        let span = jitter.saturating_mul(2).saturating_add(1);
        let offset = Self::now_ms() % span;
        let signed_delta = offset as i64 - jitter as i64;
        let candidate = base_ms as i64 + signed_delta;
        candidate.max(10) as u64
    }

    fn record_run_guard_latency(stage: &str, ok: bool, elapsed: Duration) {
        let result = if ok { "ok" } else { "err" };
        crate::common::metrics::observe_latency("dag", "run_guard", stage, result, elapsed.as_secs_f64());
    }

    fn record_run_guard_counter(metric: &'static str, result: &str) {
        let operation = if metric.contains("acquire") {
            "acquire"
        } else if metric.contains("release") {
            "release"
        } else {
            "renew"
        };
        crate::common::metrics::inc_throughput("dag", "run_guard", operation, result, 1);
        if metric == "mocra_dag_run_guard_renew_total"
            && (result == "lost" || result == "error" || result == "failed_final")
        {
            crate::common::metrics::inc_error("dag", "run_guard", "critical", result, 1);
        }
    }

    fn top_level_error_class(error: &DagError) -> DagErrorClass {
        match error {
            DagError::NodeTimeout { .. } => DagErrorClass::Retryable,
            DagError::RetryExhausted { retryable, .. } => {
                if *retryable {
                    DagErrorClass::Retryable
                } else {
                    DagErrorClass::NonRetryable
                }
            }
            DagError::DuplicateNode(_)
            | DagError::NodeNotFound(_)
            | DagError::PrecedingNodeNotFound(_)
            | DagError::CycleDetected
            | DagError::EmptyGraph
            | DagError::MissingNodeCompute(_)
            | DagError::NodeExecutionFailed { .. }
            | DagError::TaskJoinFailed { .. }
            | DagError::RunAlreadyInProgress { .. }
            | DagError::RunGuardAcquireFailed { .. }
            | DagError::RunGuardRenewFailed { .. }
            | DagError::RunGuardReleaseFailed { .. }
            | DagError::MissingRunFencingToken { .. }
            | DagError::FencingTokenRejected { .. }
            | DagError::InvalidPayloadEnvelope(_)
            | DagError::UnsupportedPayloadVersion(_)
            | DagError::ReservedControlNode(_)
            | DagError::InvalidPrecedingControlNode(_)
            | DagError::RemoteDispatchNotConfigured(_)
            | DagError::ExecutionTimeout { .. }
            | DagError::ExecutionIncomplete { .. }
            | DagError::InvalidStateTransition { .. } => DagErrorClass::NonRetryable,
        }
    }

    fn record_execute_metrics(
        result: &Result<DagExecutionReport, DagError>,
        elapsed: Duration,
        run_guard_enabled: bool,
    ) {
        let _run_guard = if run_guard_enabled { "on" } else { "off" };
        match result {
            Ok(_) => {
                crate::common::metrics::inc_throughput("dag", "scheduler", "execute", "success", 1);
                crate::common::metrics::observe_latency(
                    "dag",
                    "scheduler",
                    "execute",
                    "success",
                    elapsed.as_secs_f64(),
                );
            }
            Err(err) => {
                let code = format!("{:?}", Self::error_code(err));
                let class = format!("{:?}", Self::top_level_error_class(err));
                crate::common::metrics::inc_throughput("dag", "scheduler", "execute", "error", 1);
                crate::common::metrics::observe_latency(
                    "dag",
                    "scheduler",
                    "execute",
                    "error",
                    elapsed.as_secs_f64(),
                );
                crate::common::metrics::inc_error("dag", "scheduler", &class, &code, 1);
            }
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn normalized_max_in_flight(&self) -> usize {
        if self.options.max_in_flight == 0 {
            1
        } else {
            self.options.max_in_flight
        }
    }

    fn panic_payload_to_reason(payload: Box<dyn std::any::Any + Send>) -> String {
        if let Some(s) = payload.downcast_ref::<&str>() {
            return (*s).to_string();
        }
        if let Some(s) = payload.downcast_ref::<String>() {
            return s.clone();
        }
        "unknown panic payload".to_string()
    }

    fn classify_node_failure(error: &DagError) -> NodeFailureClass {
        match error {
            DagError::NodeTimeout { .. } => NodeFailureClass::Retryable,
            DagError::NodeExecutionFailed { .. }
            | DagError::RetryExhausted { .. }
            | DagError::TaskJoinFailed { .. }
            | DagError::RunAlreadyInProgress { .. }
            | DagError::RunGuardAcquireFailed { .. }
            | DagError::RunGuardRenewFailed { .. }
            | DagError::RunGuardReleaseFailed { .. }
            | DagError::MissingRunFencingToken { .. }
            | DagError::FencingTokenRejected { .. }
            | DagError::InvalidPayloadEnvelope(_)
            | DagError::UnsupportedPayloadVersion(_)
            | DagError::ReservedControlNode(_)
            | DagError::InvalidPrecedingControlNode(_)
            | DagError::RemoteDispatchNotConfigured(_)
            | DagError::InvalidStateTransition { .. }
            | DagError::DuplicateNode(_)
            | DagError::NodeNotFound(_)
            | DagError::PrecedingNodeNotFound(_)
            | DagError::CycleDetected
            | DagError::EmptyGraph
            | DagError::MissingNodeCompute(_)
            | DagError::ExecutionTimeout { .. }
            | DagError::ExecutionIncomplete { .. } => NodeFailureClass::NonRetryable,
        }
    }

    fn error_code(error: &DagError) -> DagErrorCode {
        match error {
            DagError::DuplicateNode(_) => DagErrorCode::DuplicateNode,
            DagError::NodeNotFound(_) => DagErrorCode::NodeNotFound,
            DagError::PrecedingNodeNotFound(_) => DagErrorCode::PrecedingNodeNotFound,
            DagError::CycleDetected => DagErrorCode::CycleDetected,
            DagError::EmptyGraph => DagErrorCode::EmptyGraph,
            DagError::MissingNodeCompute(_) => DagErrorCode::MissingNodeCompute,
            DagError::NodeExecutionFailed { .. } => DagErrorCode::NodeExecutionFailed,
            DagError::RetryExhausted { .. } => DagErrorCode::RetryExhausted,
            DagError::TaskJoinFailed { .. } => DagErrorCode::TaskJoinFailed,
            DagError::RunAlreadyInProgress { .. } => DagErrorCode::RunAlreadyInProgress,
            DagError::RunGuardAcquireFailed { .. } => DagErrorCode::RunGuardAcquireFailed,
            DagError::RunGuardRenewFailed { .. } => DagErrorCode::RunGuardRenewFailed,
            DagError::RunGuardReleaseFailed { .. } => DagErrorCode::RunGuardReleaseFailed,
            DagError::MissingRunFencingToken { .. } => DagErrorCode::MissingRunFencingToken,
            DagError::FencingTokenRejected { .. } => DagErrorCode::FencingTokenRejected,
            DagError::InvalidPayloadEnvelope(_) => DagErrorCode::InvalidPayloadEnvelope,
            DagError::UnsupportedPayloadVersion(_) => DagErrorCode::UnsupportedPayloadVersion,
            DagError::ReservedControlNode(_) => DagErrorCode::ReservedControlNode,
            DagError::InvalidPrecedingControlNode(_) => DagErrorCode::InvalidPrecedingControlNode,
            DagError::RemoteDispatchNotConfigured(_) => DagErrorCode::RemoteDispatchNotConfigured,
            DagError::NodeTimeout { .. } => DagErrorCode::NodeTimeout,
            DagError::ExecutionTimeout { .. } => DagErrorCode::ExecutionTimeout,
            DagError::ExecutionIncomplete { .. } => DagErrorCode::ExecutionIncomplete,
            DagError::InvalidStateTransition { .. } => DagErrorCode::InvalidStateTransition,
        }
    }

    fn collect_upstream_outputs(
        runtime: &Dag,
        outputs: &HashMap<String, TaskPayload>,
        node_id: &str,
    ) -> (Vec<String>, HashMap<String, TaskPayload>) {
        let predecessors = runtime
            .nodes
            .get(node_id)
            .map(|n| n.predecessors.clone())
            .unwrap_or_default();
        let mut upstream_outputs = HashMap::with_capacity(predecessors.len());
        for p in &predecessors {
            if let Some(payload) = outputs.get(p) {
                upstream_outputs.insert(p.clone(), payload.clone());
            }
        }
        (predecessors, upstream_outputs)
    }

    fn control_node_payload(node_id: &str) -> TaskPayload {
        let mut payload = TaskPayload::from_bytes(Vec::new())
            .with_content_type("application/x-mocra-control")
            .with_meta("control", node_id);
        if node_id == Dag::CONTROL_END_NODE {
            payload = payload.with_meta("state", "completed");
        }
        payload
    }

    async fn emit_sync_state(
        &self,
        run_id: &str,
        run_fencing_token: Option<u64>,
        node_id: &str,
        stage: DagSyncStage,
        attempt: usize,
        layer_index: usize,
        placement: &NodePlacement,
        idempotency_key: Option<String>,
        worker_id: Option<String>,
        deadline_ms: Option<u64>,
        dispatch_latency_ms: Option<u64>,
        error_code: Option<DagErrorCode>,
        error_class: Option<DagErrorClass>,
        error: Option<String>,
    ) {
        let Some(sync_service) = &self.sync_service else {
            return;
        };

        let (placement_name, worker_group) = match placement {
            NodePlacement::Local => ("local".to_string(), None),
            NodePlacement::Remote { worker_group } => {
                ("remote".to_string(), Some(worker_group.clone()))
            }
        };

        let state = DagNodeSyncState {
            run_id: run_id.to_string(),
            run_fencing_token,
            node_id: node_id.to_string(),
            stage: stage.as_str().to_string(),
            placement: placement_name,
            worker_group,
            worker_id,
            attempt,
            layer_index,
            idempotency_key,
            deadline_ms,
            dispatch_latency_ms,
            error_code,
            error_class,
            error,
            timestamp_ms: Self::now_ms(),
        };

        if let Err(sync_err) = sync_service.send(&state).await {
            eprintln!(
                "DAG sync emit failed: run_id={} node_id={} stage={} error={}",
                run_id,
                node_id,
                stage.as_str(),
                sync_err
            );
        }
    }

    pub fn validate(&self) -> Result<(), DagError> {
        self.dag.topological_sort().map(|_| ())
    }

    fn unlock_successors(
        runtime: &Dag,
        finished_node_id: &str,
        finished_layer_index: usize,
        remaining_preds: &mut HashMap<String, usize>,
        node_depth: &mut HashMap<String, usize>,
        ready_queue: &mut VecDeque<String>,
        ready_set: &mut HashSet<String>,
    ) -> Result<(), DagError> {
        if let Some(next_nodes) = runtime.edges.get(finished_node_id) {
            for next in next_nodes {
                let entry = remaining_preds
                    .get_mut(next)
                    .ok_or_else(|| DagError::NodeNotFound(next.clone()))?;
                if *entry > 0 {
                    *entry -= 1;
                }

                let next_depth = finished_layer_index + 1;
                node_depth
                    .entry(next.clone())
                    .and_modify(|d| {
                        if *d < next_depth {
                            *d = next_depth;
                        }
                    })
                    .or_insert(next_depth);

                if *entry == 0 {
                    let next_is_succeeded = runtime
                        .nodes
                        .get(next)
                        .map(|n| n.status == DagNodeStatus::Succeeded)
                        .unwrap_or(false);
                    if !next_is_succeeded && ready_set.insert(next.clone()) {
                        ready_queue.push_back(next.clone());
                    }
                }
            }
        }
        Ok(())
    }

    async fn handle_node_success(
        &self,
        state: &mut RuntimeState,
        node_id: &str,
        layer_index: usize,
        attempt: usize,
        payload: TaskPayload,
    ) -> Result<(), DagError> {
        let policy = state
            .runtime
            .nodes
            .get(node_id)
            .map(|n| n.execution_policy.clone())
            .ok_or_else(|| DagError::NodeNotFound(node_id.to_string()))?;
        let placement = state
            .runtime
            .nodes
            .get(node_id)
            .map(|n| n.placement.clone())
            .ok_or_else(|| DagError::NodeNotFound(node_id.to_string()))?;

        if !Dag::is_control_node(node_id) {
            self.commit_with_fencing(state, node_id, &payload).await?;
        }

        state.runtime.set_status(node_id, DagNodeStatus::Succeeded)?;
        if let Some(node) = state.runtime.nodes.get_mut(node_id) {
            node.result = Some(payload.clone());
        }
        state.outputs.insert(node_id.to_string(), payload.clone());
        let dispatch_latency_ms = state
            .dispatch_started_ms
            .remove(node_id)
            .map(|started| Self::now_ms().saturating_sub(started));

        if let Some(key) = policy.idempotency_key.as_ref() {
            state.idempotent_results.insert(key.clone(), payload.clone());
            state.inflight_idempotency_owner.remove(key);

            if let Some(waiters) = state.idempotency_waiters.remove(key) {
                for (waiter_node_id, waiter_layer_index) in waiters {
                    state
                        .runtime
                        .set_status(&waiter_node_id, DagNodeStatus::Ready)?;
                    state
                        .runtime
                        .set_status(&waiter_node_id, DagNodeStatus::Running)?;
                    state
                        .runtime
                        .set_status(&waiter_node_id, DagNodeStatus::Succeeded)?;
                    if let Some(waiter_node) = state.runtime.nodes.get_mut(&waiter_node_id) {
                        waiter_node.result = Some(payload.clone());
                        waiter_node.error = None;
                    }
                    state.outputs.insert(waiter_node_id.clone(), payload.clone());
                    state.node_results.push(NodeExecutionResult {
                        node_id: waiter_node_id.clone(),
                        payload: payload.clone(),
                        layer_index: waiter_layer_index,
                    });
                    self
                        .emit_sync_state(
                            &state.run_id,
                            state.run_fencing_token,
                            &waiter_node_id,
                            DagSyncStage::SingleflightFulfilled,
                            state.attempts.get(&waiter_node_id).copied().unwrap_or(0),
                            waiter_layer_index,
                            &placement,
                            Some(key.clone()),
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                        .await;
                    Self::unlock_successors(
                        &state.runtime,
                        &waiter_node_id,
                        waiter_layer_index,
                        &mut state.remaining_preds,
                        &mut state.node_depth,
                        &mut state.ready_queue,
                        &mut state.ready_set,
                    )?;
                }
            }
        }

        state.consecutive_failures.remove(node_id);

        state.node_results.push(NodeExecutionResult {
            node_id: node_id.to_string(),
            payload,
            layer_index,
        });
        self
            .emit_sync_state(
                &state.run_id,
                state.run_fencing_token,
                node_id,
                DagSyncStage::Succeeded,
                attempt,
                layer_index,
                &placement,
                policy.idempotency_key.clone(),
                None,
                None,
                dispatch_latency_ms,
                None,
                None,
                None,
            )
            .await;
        Self::unlock_successors(
            &state.runtime,
            node_id,
            layer_index,
            &mut state.remaining_preds,
            &mut state.node_depth,
            &mut state.ready_queue,
            &mut state.ready_set,
        )?;

        if let Some(cfg) = &self.run_state_store {
            let snapshot = state.snapshot();
            cfg.store.save(&cfg.run_key, &snapshot).await?;
        }

        Ok(())
    }

    async fn commit_with_fencing(
        &self,
        state: &RuntimeState,
        node_id: &str,
        payload: &TaskPayload,
    ) -> Result<(), DagError> {
        let Some(cfg) = &self.fencing_store else {
            return Ok(());
        };

        let token = state
            .run_fencing_token
            .ok_or_else(|| DagError::MissingRunFencingToken {
                resource: cfg.resource.clone(),
            })?;

        cfg.store
            .commit(&cfg.resource, token, node_id, payload)
            .await
            .map_err(|e| match e {
                DagError::FencingTokenRejected { .. } => e,
                other => DagError::FencingTokenRejected {
                    resource: cfg.resource.clone(),
                    token,
                    reason: other.to_string(),
                },
            })
    }

    async fn handle_node_failure(
        &self,
        state: &mut RuntimeState,
        node_id: String,
        layer_index: usize,
        attempt: usize,
        error: DagError,
    ) -> Result<Option<DagError>, DagError> {
        let policy = state
            .runtime
            .nodes
            .get(&node_id)
            .map(|n| n.execution_policy.clone())
            .ok_or_else(|| DagError::NodeNotFound(node_id.clone()))?;
        let placement = state
            .runtime
            .nodes
            .get(&node_id)
            .map(|n| n.placement.clone())
            .ok_or_else(|| DagError::NodeNotFound(node_id.clone()))?;

        let failure_class = Self::classify_node_failure(&error);
        let failure_class_tag = match failure_class {
            NodeFailureClass::Retryable => DagErrorClass::Retryable,
            NodeFailureClass::NonRetryable => DagErrorClass::NonRetryable,
        };
        let failure_code = Self::error_code(&error);
        let failure_code_tag = format!("{:?}", failure_code);
        let failure_class_tag_str = format!("{:?}", failure_class_tag);
        let placement_tag = match &placement {
            NodePlacement::Local => "local".to_string(),
            NodePlacement::Remote { .. } => "remote".to_string(),
        };
        let allow_retry = match policy.retry_mode {
            DagNodeRetryMode::AllErrors => true,
            DagNodeRetryMode::RetryableOnly => failure_class == NodeFailureClass::Retryable,
        };
        let failure_streak = state
            .consecutive_failures
            .entry(node_id.clone())
            .and_modify(|v| *v += 1)
            .or_insert(1);

        let mut retry_delay_ms = policy.retry_backoff_ms;
        if let Some(threshold) = policy.circuit_breaker_failure_threshold {
            if *failure_streak >= threshold && policy.circuit_breaker_open_ms > retry_delay_ms {
                retry_delay_ms = policy.circuit_breaker_open_ms;
            }
        }

        if attempt <= policy.max_retries && allow_retry {
            crate::common::metrics::inc_error("dag", "node", &failure_class_tag_str, &failure_code_tag, 1);
            if let Some(node) = state.runtime.nodes.get_mut(&node_id) {
                node.error = Some(error.to_string());
            }
            state.runtime.set_status(&node_id, DagNodeStatus::Pending)?;
            self
                .emit_sync_state(
                    &state.run_id,
                    state.run_fencing_token,
                    &node_id,
                    DagSyncStage::RetryScheduled,
                    attempt,
                    layer_index,
                    &placement,
                    policy.idempotency_key.clone(),
                    None,
                    None,
                    state
                        .dispatch_started_ms
                        .get(&node_id)
                        .copied()
                        .map(|started| Self::now_ms().saturating_sub(started)),
                    Some(failure_code.clone()),
                    Some(failure_class_tag.clone()),
                    Some(error.to_string()),
                )
                .await;
            if retry_delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(retry_delay_ms)).await;
            }
            state.enqueue_ready(node_id);
            return Ok(None);
        }

        if let Some(node) = state.runtime.nodes.get_mut(&node_id) {
            node.error = Some(error.to_string());
        }
        crate::common::metrics::inc_error("dag", "node", &failure_class_tag_str, &failure_code_tag, 1);
        let _ = failure_code_tag;
        let _ = failure_class_tag_str;
        let _ = placement_tag;
        let dispatch_latency_ms = state
            .dispatch_started_ms
            .remove(&node_id)
            .map(|started| Self::now_ms().saturating_sub(started));
        state.runtime.set_status(&node_id, DagNodeStatus::Failed)?;
        if let Some(key) = policy.idempotency_key.as_ref() {
            state.inflight_idempotency_owner.remove(key);
            if let Some(waiters) = state.idempotency_waiters.remove(key) {
                for (waiter_node_id, _) in waiters {
                    if let Some(waiter_node) = state.runtime.nodes.get_mut(&waiter_node_id) {
                        waiter_node.error = Some(error.to_string());
                    }
                    state
                        .runtime
                        .set_status(&waiter_node_id, DagNodeStatus::Ready)?;
                    state
                        .runtime
                        .set_status(&waiter_node_id, DagNodeStatus::Running)?;
                    state
                        .runtime
                        .set_status(&waiter_node_id, DagNodeStatus::Failed)?;
                }
            }
        }
        self
            .emit_sync_state(
                &state.run_id,
                state.run_fencing_token,
                &node_id,
                DagSyncStage::Failed,
                attempt,
                layer_index,
                &placement,
                policy.idempotency_key.clone(),
                None,
                None,
                dispatch_latency_ms,
                Some(failure_code),
                Some(failure_class_tag),
                Some(error.to_string()),
            )
            .await;
        Ok(Some(DagError::RetryExhausted {
            node_id,
            attempts: attempt,
            max_retries: policy.max_retries,
            retryable: failure_class == NodeFailureClass::Retryable,
            last_error: error.to_string(),
        }))
    }

    async fn prepare_node_dispatch(
        &self,
        state: &mut RuntimeState,
        node_id: String,
    ) -> Result<Option<PreparedDispatch>, DagError> {
        state.runtime.set_status(&node_id, DagNodeStatus::Ready)?;
        state.runtime.set_status(&node_id, DagNodeStatus::Running)?;

        let layer_index = *state.node_depth.get(&node_id).unwrap_or(&0usize);
        state
            .layers_by_depth
            .entry(layer_index)
            .or_default()
            .push(node_id.clone());

        let (predecessors, upstream_outputs) = Self::collect_upstream_outputs(
            &state.runtime,
            &state.outputs,
            &node_id,
        );

        let (placement, policy) = state
            .runtime
            .nodes
            .get(&node_id)
            .map(|n| (n.placement.clone(), n.execution_policy.clone()))
            .ok_or_else(|| DagError::NodeNotFound(node_id.clone()))?;

        if self
            .pre_dispatch_idempotency_gate(state, &node_id, layer_index, &placement, &policy)
            .await?
        {
            return Ok(None);
        }

        let attempt = state.attempts.get(&node_id).copied().unwrap_or(0) + 1;
        state.attempts.insert(node_id.clone(), attempt);

        let ctx = NodeExecutionContext {
            run_id: state.run_id.clone(),
            run_fencing_token: state.run_fencing_token,
            node_id: node_id.clone(),
            attempt,
            upstream_nodes: predecessors,
            upstream_outputs,
            layer_index,
        };

        Ok(Some(PreparedDispatch {
            node_id,
            layer_index,
            attempt,
            placement,
            policy,
            context: ctx,
        }))
    }

    async fn spawn_node_task(
        &self,
        state: &mut RuntimeState,
        dispatch: PreparedDispatch,
        join_set: &mut tokio::task::JoinSet<JoinOutput>,
    ) -> Result<(), DagError> {
        let PreparedDispatch {
            node_id,
            layer_index,
            attempt,
            placement,
            policy,
            context,
        } = dispatch;

        let node_id_clone = node_id.clone();
        if Dag::is_control_node(&node_id) {
            join_set.spawn(async move {
                let payload = DagScheduler::control_node_payload(&node_id_clone);
                (node_id_clone, layer_index, attempt, Ok(payload))
            });
            return Ok(());
        }

        let executor = state
            .runtime
            .nodes
            .get(&node_id)
            .and_then(|n| n.executor.clone())
            .ok_or_else(|| DagError::MissingNodeCompute(node_id.clone()))?;

        if let Some(key) = policy.idempotency_key.as_ref() {
            state
                .inflight_idempotency_owner
                .insert(key.clone(), node_id.clone());
        }

        self.emit_sync_state(
            &state.run_id,
            state.run_fencing_token,
            &node_id,
            DagSyncStage::Running,
            attempt,
            layer_index,
            &placement,
            policy.idempotency_key.clone(),
            Some(match &placement {
                NodePlacement::Local => format!("local-{}", std::process::id()),
                NodePlacement::Remote { worker_group } => worker_group.clone(),
            }),
            policy.timeout_ms.map(|timeout_ms| Self::now_ms() + timeout_ms),
            None,
            None,
            None,
            None,
        )
        .await;
        state
            .dispatch_started_ms
            .insert(node_id.clone(), Self::now_ms());

        let dispatcher = self.dispatcher.clone();
        join_set.spawn(async move {
            let dispatch_future = async {
                if let Some(timeout_ms) = policy.timeout_ms {
                    match tokio::time::timeout(
                        Duration::from_millis(timeout_ms),
                        dispatcher.dispatch(&node_id_clone, &placement, executor, context),
                    )
                    .await
                    {
                        Ok(dispatch_result) => dispatch_result,
                        Err(_) => Err(DagError::NodeTimeout {
                            node_id: node_id_clone.clone(),
                            timeout_ms,
                        }),
                    }
                } else {
                    dispatcher
                        .dispatch(&node_id_clone, &placement, executor, context)
                        .await
                }
            };

            let result = match std::panic::AssertUnwindSafe(dispatch_future)
                .catch_unwind()
                .await
            {
                Ok(result) => result,
                Err(panic_payload) => Err(DagError::TaskJoinFailed {
                    node_id: node_id_clone.clone(),
                    reason: format!(
                        "node task panicked: {}",
                        Self::panic_payload_to_reason(panic_payload)
                    ),
                }),
            };

            (node_id_clone, layer_index, attempt, result)
        });

        Ok(())
    }

    async fn pre_dispatch_idempotency_gate(
        &self,
        state: &mut RuntimeState,
        node_id: &str,
        layer_index: usize,
        placement: &NodePlacement,
        policy: &DagNodeExecutionPolicy,
    ) -> Result<bool, DagError> {
        let Some(key) = policy.idempotency_key.as_ref() else {
            return Ok(false);
        };

        if let Some(payload) = state.idempotent_results.get(key).cloned() {
            state.runtime.set_status(node_id, DagNodeStatus::Succeeded)?;
            if let Some(node) = state.runtime.nodes.get_mut(node_id) {
                node.result = Some(payload.clone());
            }
            state.outputs.insert(node_id.to_string(), payload.clone());
            state.node_results.push(NodeExecutionResult {
                node_id: node_id.to_string(),
                payload,
                layer_index,
            });
            self.emit_sync_state(
                &state.run_id,
                state.run_fencing_token,
                node_id,
                DagSyncStage::CacheHit,
                state.attempts.get(node_id).copied().unwrap_or(0),
                layer_index,
                placement,
                policy.idempotency_key.clone(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await;
            Self::unlock_successors(
                &state.runtime,
                node_id,
                layer_index,
                &mut state.remaining_preds,
                &mut state.node_depth,
                &mut state.ready_queue,
                &mut state.ready_set,
            )?;
            return Ok(true);
        }

        if state.inflight_idempotency_owner.contains_key(key) {
            state.runtime.set_status(node_id, DagNodeStatus::Pending)?;
            state
                .idempotency_waiters
                .entry(key.clone())
                .or_default()
                .push((node_id.to_string(), layer_index));
            self.emit_sync_state(
                &state.run_id,
                state.run_fencing_token,
                node_id,
                DagSyncStage::SingleflightWait,
                state.attempts.get(node_id).copied().unwrap_or(0),
                layer_index,
                placement,
                policy.idempotency_key.clone(),
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await;
            return Ok(true);
        }

        Ok(false)
    }

    async fn persist_failure_snapshot_if_enabled(&self, state: &RuntimeState, stage: &str) {
        let Some(cfg) = &self.run_state_store else {
            return;
        };

        let snapshot = state.snapshot();
        match cfg.store.save(&cfg.run_key, &snapshot).await {
            Ok(()) => {
                crate::common::metrics::inc_throughput("dag", "run_state", stage, "saved", 1);
            }
            Err(e) => {
                crate::common::metrics::inc_error("dag", "run_state", "store", "save_error", 1);
                eprintln!(
                    "DAG run state snapshot save failed before error return: stage={} run_key={} error={}",
                    stage,
                    cfg.run_key,
                    e
                );
            }
        }
    }

    pub async fn execute_parallel(&self) -> Result<DagExecutionReport, DagError> {
        let run_started = Instant::now();
        crate::common::metrics::inc_throughput("dag", "scheduler", "execute", "started", 1);

        if let Err(e) = self.validate() {
            let result = Err(e);
            Self::record_execute_metrics(&result, run_started.elapsed(), self.run_guard.is_some());
            return result;
        }

        let run_guard_ctx = if let Some(cfg) = &self.run_guard {
            let owner = format!("pid-{}-{}", std::process::id(), Uuid::now_v7());
            let acquire_started = Instant::now();
            let acquire_outcome = match cfg
                .guard
                .try_acquire(&cfg.lock_key, &owner, cfg.ttl_ms)
                .await
            {
                Ok(outcome) => outcome,
                Err(e) => {
                    Self::record_run_guard_latency("acquire", false, acquire_started.elapsed());
                    Self::record_run_guard_counter("mocra_dag_run_guard_acquire_total", "error");
                    let result = Err(DagError::RunGuardAcquireFailed {
                        lock_key: cfg.lock_key.clone(),
                        reason: e.to_string(),
                    });
                    Self::record_execute_metrics(
                        &result,
                        run_started.elapsed(),
                        self.run_guard.is_some(),
                    );
                    return result;
                }
            };
            Self::record_run_guard_latency("acquire", true, acquire_started.elapsed());
            if !acquire_outcome.acquired {
                Self::record_run_guard_counter("mocra_dag_run_guard_acquire_total", "contention");
                let result = Err(DagError::RunAlreadyInProgress {
                    lock_key: cfg.lock_key.clone(),
                });
                Self::record_execute_metrics(&result, run_started.elapsed(), self.run_guard.is_some());
                return result;
            }
            Self::record_run_guard_counter("mocra_dag_run_guard_acquire_total", "success");
            Some((cfg.clone(), owner, acquire_outcome.fencing_token))
        } else {
            None
        };

        if let Some(fencing_cfg) = &self.fencing_store {
            let has_token = run_guard_ctx
                .as_ref()
                .and_then(|(_, _, token)| *token)
                .is_some();
            if !has_token {
                let result = Err(DagError::MissingRunFencingToken {
                    resource: fencing_cfg.resource.clone(),
                });
                Self::record_execute_metrics(&result, run_started.elapsed(), self.run_guard.is_some());
                return result;
            }
        }

        let renew_failed_reason: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let mut renew_stop_tx = None;
        let mut renew_task = None;

        if let Some((cfg, owner, _token)) = &run_guard_ctx {
            if let Some(interval_ms) = cfg.renew_interval_ms {
                let (tx, mut rx) = tokio::sync::watch::channel(false);
                let guard = cfg.guard.clone();
                let lock_key = cfg.lock_key.clone();
                let owner = owner.clone();
                let ttl_ms = cfg.ttl_ms;
                let renew_jitter_pct = cfg.renew_jitter_pct;
                let failed = renew_failed_reason.clone();
                renew_stop_tx = Some(tx);
                renew_task = Some(tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = rx.changed() => {
                                break;
                            }
                            _ = tokio::time::sleep(Duration::from_millis(Self::run_guard_jittered_interval_ms(interval_ms, renew_jitter_pct))) => {
                                let renew_started = Instant::now();
                                match guard.renew(&lock_key, &owner, ttl_ms).await {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        Self::record_run_guard_latency("renew", false, renew_started.elapsed());
                                        Self::record_run_guard_counter("mocra_dag_run_guard_renew_total", "lost");
                                        if let Ok(mut slot) = failed.lock() {
                                            *slot = Some("run guard lock ownership lost during renew".to_string());
                                        }
                                        break;
                                    }
                                    Err(e) => {
                                        Self::record_run_guard_latency("renew", false, renew_started.elapsed());
                                        Self::record_run_guard_counter("mocra_dag_run_guard_renew_total", "error");
                                        if let Ok(mut slot) = failed.lock() {
                                            *slot = Some(e.to_string());
                                        }
                                        break;
                                    }
                                }
                                Self::record_run_guard_latency("renew", true, renew_started.elapsed());
                                Self::record_run_guard_counter("mocra_dag_run_guard_renew_total", "success");
                            }
                        }
                    }
                }));
            }
        }

        let execute_result = async {
            let resume_state = if let Some(cfg) = &self.run_state_store {
                cfg.store.load(&cfg.run_key).await?
            } else {
                None
            };

            let mut state = RuntimeState::new_with_resume(&self.dag, resume_state.as_ref());
            state.run_fencing_token = run_guard_ctx
                .as_ref()
                .and_then(|(_, _, token)| *token);
            let cancel_inflight_on_failure = self.options.cancel_inflight_on_failure;
            let max_in_flight = self.normalized_max_in_flight();
            let run_timeout = self.options.run_timeout_ms.map(Duration::from_millis);
            let started_at = Instant::now();
            let run_guard_lock_key = run_guard_ctx
                .as_ref()
                .map(|(cfg, _, _)| cfg.lock_key.clone());
            let wait_poll_interval = Duration::from_millis(10);

            let mut join_set: tokio::task::JoinSet<JoinOutput> = tokio::task::JoinSet::new();

            while !state.ready_queue.is_empty() || !join_set.is_empty() {
                if let Some(lock_key) = run_guard_lock_key.as_ref() {
                    if let Some(reason) = renew_failed_reason.lock().ok().and_then(|g| g.clone()) {
                        join_set.abort_all();
                        self
                            .persist_failure_snapshot_if_enabled(&state, "renew_lost")
                            .await;
                        return Err(DagError::RunGuardRenewFailed {
                            lock_key: lock_key.clone(),
                            reason,
                        });
                    }
                }

                if let Some(timeout) = run_timeout {
                    if started_at.elapsed() > timeout {
                        join_set.abort_all();
                        self
                            .persist_failure_snapshot_if_enabled(&state, "run_timeout")
                            .await;
                        return Err(DagError::ExecutionTimeout {
                            run_id: state.run_id.clone(),
                            timeout_ms: timeout.as_millis() as u64,
                        });
                    }
                }

                while join_set.len() < max_in_flight {
                    let Some(node_id) = state.pop_ready() else {
                        break;
                    };
                    let Some(dispatch) = self.prepare_node_dispatch(&mut state, node_id).await? else {
                        continue;
                    };
                    self.spawn_node_task(&mut state, dispatch, &mut join_set)
                        .await?;
                }

                let Some(joined) = join_set.try_join_next() else {
                    if !join_set.is_empty() {
                        tokio::time::sleep(wait_poll_interval).await;
                        continue;
                    }
                    break;
                };

                let (node_id, layer_index, attempt, result) =
                    joined.map_err(|e| DagError::TaskJoinFailed {
                        node_id: "unknown".to_string(),
                        reason: e.to_string(),
                    })?;

                match result {
                    Ok(payload) => {
                        self
                            .handle_node_success(
                                &mut state,
                                &node_id,
                                layer_index,
                                attempt,
                                payload,
                            )
                            .await?;
                    }
                    Err(e) => {
                        if let Some(final_error) = self
                            .handle_node_failure(
                                &mut state,
                                node_id,
                                layer_index,
                                attempt,
                                e,
                            )
                            .await?
                        {
                            if cancel_inflight_on_failure {
                                join_set.abort_all();
                            }
                            self
                                .persist_failure_snapshot_if_enabled(&state, "node_failed")
                                .await;
                            return Err(final_error);
                        }
                    }
                }
            }

            let report = state.into_report();
            if report.is_ok() {
                if let Some(cfg) = &self.run_state_store {
                    cfg.store.clear(&cfg.run_key).await?;
                }
            }
            report
        }
        .await;

        if let Some(tx) = renew_stop_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(task) = renew_task {
            let _ = task.await;
        }

        let renew_failed = renew_failed_reason
            .lock()
            .ok()
            .and_then(|g| g.clone());

        if let Some((cfg, owner, _token)) = run_guard_ctx {
            let release_started = Instant::now();
            match cfg.guard.release(&cfg.lock_key, &owner).await {
                Err(e) => {
                    Self::record_run_guard_latency("release", false, release_started.elapsed());
                    Self::record_run_guard_counter("mocra_dag_run_guard_release_total", "error");
                    if execute_result.is_ok() {
                        let result = Err(DagError::RunGuardReleaseFailed {
                            lock_key: cfg.lock_key,
                            reason: e.to_string(),
                        });
                        Self::record_execute_metrics(
                            &result,
                            run_started.elapsed(),
                            self.run_guard.is_some(),
                        );
                        return result;
                    }
                }
                Ok(()) => {
                    Self::record_run_guard_latency("release", true, release_started.elapsed());
                    Self::record_run_guard_counter("mocra_dag_run_guard_release_total", "success");
                }
            }
            if execute_result.is_ok() {
                if let Some(reason) = renew_failed {
                    Self::record_run_guard_counter("mocra_dag_run_guard_renew_total", "failed_final");
                    let result = Err(DagError::RunGuardRenewFailed {
                        lock_key: cfg.lock_key,
                        reason,
                    });
                    Self::record_execute_metrics(
                        &result,
                        run_started.elapsed(),
                        self.run_guard.is_some(),
                    );
                    return result;
                }
            }
        }
        Self::record_execute_metrics(
            &execute_result,
            run_started.elapsed(),
            self.run_guard.is_some(),
        );
        execute_result
    }
}
