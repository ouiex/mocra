use super::*;
use crate::sync::SyncService;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

fn i64_to_payload(v: i64) -> TaskPayload {
    TaskPayload::from_bytes(v.to_le_bytes().to_vec())
        .with_content_type("application/x-i64")
        .with_encoding("little-endian")
}

fn payload_to_i64(payload: &TaskPayload) -> i64 {
    let mut buf = [0u8; 8];
    assert_eq!(payload.bytes.len(), 8, "invalid i64 payload length");
    buf.copy_from_slice(&payload.bytes);
    i64::from_le_bytes(buf)
}

struct TestNode {
    logic: Arc<
        dyn Fn(
                NodeExecutionContext,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<TaskPayload, DagError>> + Send>,
            > + Send
            + Sync,
    >,
}

#[derive(Default)]
struct DashMapWorkerDispatcher {
    dispatch_count_by_worker: DashMap<String, usize>,
    nodes_by_worker: DashMap<String, Vec<String>>,
}

#[async_trait]
impl DagNodeDispatcher for DashMapWorkerDispatcher {
    async fn dispatch(
        &self,
        node_id: &str,
        placement: &NodePlacement,
        executor: Arc<dyn DagNodeTrait>,
        context: NodeExecutionContext,
    ) -> Result<TaskPayload, DagError> {
        let worker = match placement {
            NodePlacement::Local => "local".to_string(),
            NodePlacement::Remote { worker_group } => worker_group.clone(),
        };
        self.dispatch_count_by_worker
            .entry(worker.clone())
            .and_modify(|v| *v += 1)
            .or_insert(1);
        self.nodes_by_worker
            .entry(worker)
            .and_modify(|v| v.push(node_id.to_string()))
            .or_insert_with(|| vec![node_id.to_string()]);

        executor.start(context).await
    }
}

struct FailAfterNRenewRunGuard {
    token: u64,
    renew_calls: AtomicUsize,
    fail_after_calls: usize,
}

impl FailAfterNRenewRunGuard {
    fn new(token: u64, fail_after_calls: usize) -> Self {
        Self {
            token,
            renew_calls: AtomicUsize::new(0),
            fail_after_calls,
        }
    }
}

#[async_trait]
impl DagRunGuard for FailAfterNRenewRunGuard {
    async fn try_acquire(
        &self,
        _lock_key: &str,
        _owner: &str,
        _ttl_ms: u64,
    ) -> Result<DagRunGuardAcquireOutcome, DagError> {
        Ok(DagRunGuardAcquireOutcome {
            acquired: true,
            fencing_token: Some(self.token),
        })
    }

    async fn renew(&self, _lock_key: &str, _owner: &str, _ttl_ms: u64) -> Result<bool, DagError> {
        let current = self.renew_calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(current <= self.fail_after_calls)
    }

    async fn release(&self, _lock_key: &str, _owner: &str) -> Result<(), DagError> {
        Ok(())
    }
}

struct FixedTokenRunGuard {
    token: u64,
}

#[async_trait]
impl DagRunGuard for FixedTokenRunGuard {
    async fn try_acquire(
        &self,
        _lock_key: &str,
        _owner: &str,
        _ttl_ms: u64,
    ) -> Result<DagRunGuardAcquireOutcome, DagError> {
        Ok(DagRunGuardAcquireOutcome {
            acquired: true,
            fencing_token: Some(self.token),
        })
    }

    async fn renew(&self, _lock_key: &str, _owner: &str, _ttl_ms: u64) -> Result<bool, DagError> {
        Ok(true)
    }

    async fn release(&self, _lock_key: &str, _owner: &str) -> Result<(), DagError> {
        Ok(())
    }
}

#[derive(Default)]
struct InMemoryRunStateStore {
    snapshots: DashMap<String, DagRunState>,
}

#[async_trait]
impl DagRunStateStore for InMemoryRunStateStore {
    async fn load(&self, run_key: &str) -> Result<Option<DagRunResumeState>, DagError> {
        Ok(self
            .snapshots
            .get(run_key)
            .map(|state| state.value().to_resume_state()))
    }

    async fn save(&self, run_key: &str, state: &DagRunResumeState) -> Result<(), DagError> {
        self.snapshots
            .insert(run_key.to_string(), state.clone().into_run_state());
        Ok(())
    }

    async fn clear(&self, run_key: &str) -> Result<(), DagError> {
        self.snapshots.remove(run_key);
        Ok(())
    }

    async fn load_state(&self, run_key: &str) -> Result<Option<DagRunState>, DagError> {
        Ok(self
            .snapshots
            .get(run_key)
            .map(|state| state.value().clone()))
    }

    async fn save_state(&self, run_key: &str, state: &DagRunState) -> Result<(), DagError> {
        self.snapshots.insert(run_key.to_string(), state.clone());
        Ok(())
    }
}

#[async_trait]
impl DagNodeTrait for TestNode {
    async fn start(&self, context: NodeExecutionContext) -> Result<TaskPayload, DagError> {
        (self.logic)(context).await
    }
}

fn node<F, Fut>(f: F) -> Arc<dyn DagNodeTrait>
where
    F: Fn(NodeExecutionContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<TaskPayload, DagError>> + Send + 'static,
{
    Arc::new(TestNode {
        logic: Arc::new(move |ctx| Box::pin(f(ctx))),
    })
}

#[test]
fn payload_envelope_roundtrip_works() {
    let payload = TaskPayload::from_bytes(vec![1, 2, 3, 4])
        .with_content_type("application/test")
        .with_encoding("raw")
        .with_meta("k1", "v1")
        .with_meta("k2", "v2");

    let encoded = payload.to_envelope_bytes().expect("encode should succeed");
    let decoded = TaskPayload::from_envelope_bytes(&encoded).expect("decode should succeed");
    assert_eq!(decoded, payload);
}

#[test]
fn payload_envelope_rejects_bad_magic() {
    let err = TaskPayload::from_envelope_bytes(b"BAD!").unwrap_err();
    assert!(matches!(err, DagError::InvalidPayloadEnvelope(_)));
}

#[test]
fn payload_envelope_rejects_unsupported_version() {
    let payload = TaskPayload::from_bytes(vec![7, 8, 9]);
    let mut encoded = payload.to_envelope_bytes().expect("encode should succeed");
    encoded[4] = 2;
    let err = TaskPayload::from_envelope_bytes(&encoded).unwrap_err();
    assert!(matches!(err, DagError::UnsupportedPayloadVersion(2)));
}

#[test]
fn add_node_with_none_connects_from_start() {
    let mut dag = Dag::new();
    let a = dag
        .add_node(None, node(|_ctx| async move { Ok(i64_to_payload(1)) }))
        .unwrap();

    let start = dag.control_start_ptr();
    let start_next = dag.successors(&start).unwrap();
    assert!(start_next.iter().any(|n| n.id == a.id));
}

#[test]
fn add_node_checks_preceding_exists() {
    let mut dag = Dag::new();
    let fake = Arc::new(DagNodeRecord {
        id: "does-not-exist".to_string(),
        predecessors: Vec::new(),
        status: DagNodeStatus::Pending,
        placement: NodePlacement::Local,
        execution_policy: DagNodeExecutionPolicy::default(),
        runtime_input: None,
        executor: None,
        result: None,
        metadata: std::collections::HashMap::new(),
        error: None,
    });
    let err = dag
        .add_node(
            Some(&[fake]),
            node(|_ctx| async move { Ok(i64_to_payload(1)) }),
        )
        .err()
        .expect("expected preceding node check to fail");
    assert!(matches!(err, DagError::PrecedingNodeNotFound(_)));
}

#[test]
fn add_node_rejects_reserved_control_node_id() {
    let mut dag = Dag::new();
    let err = dag
        .add_node_with_id(
            None,
            Dag::CONTROL_START_NODE,
            node(|_ctx| async move { Ok(i64_to_payload(1)) }),
        )
        .err()
        .expect("expected reserved control node rejection");
    assert!(matches!(err, DagError::ReservedControlNode(_)));
}

#[test]
fn add_node_rejects_end_node_as_predecessor() {
    let mut dag = Dag::new();
    let end = dag.control_end_ptr();
    let err = dag
        .add_node(
            Some(&[end]),
            node(|_ctx| async move { Ok(i64_to_payload(1)) }),
        )
        .err()
        .expect("expected end node predecessor rejection");
    assert!(matches!(err, DagError::InvalidPrecedingControlNode(_)));
}

#[test]
fn add_node_default_id_is_uuid_like() {
    let mut dag = Dag::new();
    let a = dag
        .add_node(None, node(|_ctx| async move { Ok(i64_to_payload(1)) }))
        .unwrap();
    assert!(Uuid::parse_str(&a.id).is_ok());
}

#[test]
fn add_node_rewires_terminal_edge_to_end() {
    let mut dag = Dag::new();
    let a = dag
        .add_node_with_id(None, "A", node(|_ctx| async move { Ok(i64_to_payload(1)) }))
        .unwrap();
    let b = dag
        .add_node_with_id(
            Some(std::slice::from_ref(&a)),
            "B",
            node(|_ctx| async move { Ok(i64_to_payload(2)) }),
        )
        .unwrap();

    let a_next = dag.successors(&a).unwrap();
    assert!(a_next.iter().any(|n| n.id == b.id));
    assert!(!a_next.iter().any(|n| n.id == Dag::CONTROL_END_NODE));

    let b_next = dag.successors(&b).unwrap();
    assert!(b_next.iter().any(|n| n.id == Dag::CONTROL_END_NODE));
}

#[test]
fn add_node_uses_pointer_flow_without_manual_id() {
    let mut dag = Dag::new();

    let a = dag
        .add_node(None, node(|_ctx| async move { Ok(i64_to_payload(10)) }))
        .unwrap();
    let b = dag
        .add_node(
            Some(std::slice::from_ref(&a)),
            node(|_ctx| async move { Ok(i64_to_payload(20)) }),
        )
        .unwrap();
    let c = dag
        .add_node(
            Some(std::slice::from_ref(&b)),
            node(|_ctx| async move { Ok(i64_to_payload(30)) }),
        )
        .unwrap();

    let start = dag.control_start_ptr();
    let start_next = dag.successors(&start).unwrap();
    assert!(start_next.iter().any(|n| n.id == a.id));

    let a_next = dag.successors(&a).unwrap();
    assert!(a_next.iter().any(|n| n.id == b.id));

    let b_next = dag.successors(&b).unwrap();
    assert!(b_next.iter().any(|n| n.id == c.id));

    let c_next = dag.successors(&c).unwrap();
    assert!(c_next.iter().any(|n| n.id == Dag::CONTROL_END_NODE));
}

#[test]
fn add_chain_node_builds_linear_chain() {
    let mut dag = Dag::new();
    let chain_a = dag
        .add_chain_node_with_id("A", node(|_ctx| async move { Ok(i64_to_payload(1)) }))
        .unwrap();
    let a = chain_a.last().clone();
    let chain_b = chain_a
        .add_chain_node_with_id("B", node(|_ctx| async move { Ok(i64_to_payload(2)) }))
        .unwrap();
    let b = chain_b.last().clone();
    let chain = chain_b
        .add_chain_node_with_id("C", node(|_ctx| async move { Ok(i64_to_payload(3)) }))
        .unwrap();
    let c = chain.last().clone();

    assert_eq!(chain.last().id, "C");

    let start = dag.control_start_ptr();

    let start_next = dag.successors(&start).unwrap();
    assert!(start_next.iter().any(|n| n.id == "A"));

    let a_next = dag.successors(&a).unwrap();
    assert!(a_next.iter().any(|n| n.id == "B"));

    let b_next = dag.successors(&b).unwrap();
    assert!(b_next.iter().any(|n| n.id == "C"));

    let c_next = dag.successors(&c).unwrap();
    assert!(c_next.iter().any(|n| n.id == Dag::CONTROL_END_NODE));
}

#[tokio::test]
async fn execute_parallel_file_lifecycle_validates_dag_node_trait_genericity() {
    fn payload_to_string(payload: &TaskPayload) -> String {
        String::from_utf8(payload.bytes.clone()).expect("payload should be utf8 string")
    }

    let expected_content = "dag-node-trait-file-io".to_string();
    let file_path = std::env::temp_dir().join(format!("mocra-dag-{}.txt", Uuid::now_v7()));
    let file_path_str = file_path.to_string_lossy().to_string();

    let mut dag = Dag::new();
    let create = dag
        .add_node(
            None,
            node({
                let file_path_str = file_path_str.clone();
                move |_ctx| {
                    let file_path_str = file_path_str.clone();
                    async move {
                        std::fs::File::create(&file_path_str).map_err(|e| {
                            DagError::NodeExecutionFailed {
                                node_id: "FILE_CREATE".to_string(),
                                reason: e.to_string(),
                            }
                        })?;
                        Ok(TaskPayload::from_bytes(file_path_str.into_bytes()))
                    }
                }
            }),
        )
        .unwrap();

    let write = dag
        .add_node(
            Some(std::slice::from_ref(&create)),
            node({
                let expected_content = expected_content.clone();
                move |ctx| {
                    let expected_content = expected_content.clone();
                    async move {
                        let path = ctx
                            .upstream_outputs
                            .values()
                            .next()
                            .map(payload_to_string)
                            .ok_or_else(|| DagError::NodeExecutionFailed {
                                node_id: "FILE_WRITE".to_string(),
                                reason: "missing path payload".to_string(),
                            })?;

                        std::fs::write(&path, expected_content.as_bytes()).map_err(|e| {
                            DagError::NodeExecutionFailed {
                                node_id: "FILE_WRITE".to_string(),
                                reason: e.to_string(),
                            }
                        })?;
                        Ok(TaskPayload::from_bytes(path.into_bytes()))
                    }
                }
            }),
        )
        .unwrap();

    let read = dag
        .add_node(
            Some(std::slice::from_ref(&write)),
            node(|ctx| async move {
                let path = ctx
                    .upstream_outputs
                    .values()
                    .next()
                    .map(payload_to_string)
                    .ok_or_else(|| DagError::NodeExecutionFailed {
                        node_id: "FILE_READ".to_string(),
                        reason: "missing path payload".to_string(),
                    })?;

                let content =
                    std::fs::read_to_string(&path).map_err(|e| DagError::NodeExecutionFailed {
                        node_id: "FILE_READ".to_string(),
                        reason: e.to_string(),
                    })?;
                Ok(TaskPayload::from_bytes(content.into_bytes()))
            }),
        )
        .unwrap();

    dag.add_node(
        Some(&[write]),
        node(|ctx| async move {
            let path = ctx
                .upstream_outputs
                .values()
                .next()
                .map(payload_to_string)
                .ok_or_else(|| DagError::NodeExecutionFailed {
                    node_id: "FILE_DELETE".to_string(),
                    reason: "missing path payload".to_string(),
                })?;

            std::fs::remove_file(&path).map_err(|e| DagError::NodeExecutionFailed {
                node_id: "FILE_DELETE".to_string(),
                reason: e.to_string(),
            })?;

            Ok(TaskPayload::from_bytes(path.into_bytes()))
        }),
    )
    .unwrap();

    let report = DagScheduler::new(dag).execute_parallel().await.unwrap();
    let actual_content = report
        .outputs
        .get(&read.id)
        .map(payload_to_string)
        .expect("read node output should exist");
    assert_eq!(actual_content, expected_content);
    assert!(
        !file_path.exists(),
        "file should be deleted by FILE_DELETE node"
    );
}

#[tokio::test]
async fn execute_parallel_fan_in_waits_for_all_upstream() {
    let mut dag = Dag::new();
    let a = dag
        .add_node_with_id(None, "A", node(|_ctx| async move { Ok(i64_to_payload(1)) }))
        .unwrap();

    let b_done = Arc::new(AtomicBool::new(false));
    let c_done = Arc::new(AtomicBool::new(false));

    let b = {
        let b_done = b_done.clone();
        dag.add_node_with_id(
            Some(std::slice::from_ref(&a)),
            "B",
            node(move |_ctx| {
                let b_done = b_done.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    b_done.store(true, Ordering::SeqCst);
                    Ok(i64_to_payload(2))
                }
            }),
        )
        .unwrap()
    };

    let c = {
        let c_done = c_done.clone();
        dag.add_node_with_id(
            Some(&[a]),
            "C",
            node(move |_ctx| {
                let c_done = c_done.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    c_done.store(true, Ordering::SeqCst);
                    Ok(i64_to_payload(3))
                }
            }),
        )
        .unwrap()
    };

    dag.add_node_with_id(
        Some(&[b, c]),
        "D",
        node(move |_ctx| {
            let b_done = b_done.clone();
            let c_done = c_done.clone();
            async move {
                assert!(b_done.load(Ordering::SeqCst));
                assert!(c_done.load(Ordering::SeqCst));
                Ok(i64_to_payload(5))
            }
        }),
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let report = scheduler.execute_parallel().await.unwrap();
    assert!(report.outputs.contains_key("D"));
}

#[tokio::test]
async fn execute_parallel_simulated_full_workflow() {
    let mut dag = Dag::new();
    let source = dag
        .add_node_with_id(
            None,
            "S",
            node(|_ctx| async move { Ok(i64_to_payload(10)) }),
        )
        .unwrap();

    let x = dag
        .add_node_with_id(
            Some(std::slice::from_ref(&source)),
            "X",
            node(|ctx| async move {
                let source = ctx
                    .upstream_outputs
                    .values()
                    .next()
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(source + 1))
            }),
        )
        .unwrap();

    let y = dag
        .add_node_with_id(
            Some(&[source]),
            "Y",
            node(|ctx| async move {
                let source = ctx
                    .upstream_outputs
                    .values()
                    .next()
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(source + 2))
            }),
        )
        .unwrap();

    let z = dag
        .add_node_with_id(
            Some(&[x, y]),
            "Z",
            node(|ctx| async move {
                let mut vals = ctx.upstream_outputs.values().map(payload_to_i64);
                let a = vals.next().unwrap_or(0);
                let b = vals.next().unwrap_or(0);
                Ok(i64_to_payload(a * b))
            }),
        )
        .unwrap();

    dag.add_node_with_id(
        Some(&[z]),
        "W",
        node(|ctx| async move {
            let z = ctx
                .upstream_outputs
                .values()
                .next()
                .map(payload_to_i64)
                .unwrap_or(0);
            Ok(i64_to_payload(z + 100))
        }),
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let report = scheduler.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(report.outputs.get("W").expect("W output missing")),
        232
    );
    assert_eq!(
        report.node_states.get(Dag::CONTROL_END_NODE).cloned(),
        Some(DagNodeStatus::Succeeded)
    );
}

#[tokio::test]
async fn execute_parallel_returns_node_failure() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "FAIL",
        node(|_ctx| async move { Err(DagError::InvalidPayloadEnvelope("boom".to_string())) }),
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let err = scheduler.execute_parallel().await.unwrap_err();
    match err {
        DagError::RetryExhausted {
            node_id,
            last_error: reason,
            ..
        } => {
            assert_eq!(node_id, "FAIL");
            assert!(reason.contains("boom"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn execute_parallel_remote_node_requires_dispatcher() {
    let mut dag = Dag::new();
    let n = dag
        .add_node_with_id(
            None,
            "REMOTE",
            node(|_ctx| async move { Ok(i64_to_payload(1)) }),
        )
        .unwrap();
    dag.set_node_placement(
        &n,
        NodePlacement::Remote {
            worker_group: "wg-a".to_string(),
        },
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let err = scheduler.execute_parallel().await.unwrap_err();
    match err {
        DagError::RemoteDispatchNotConfigured(node_id) => {
            assert_eq!(node_id, "REMOTE");
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn execute_parallel_node_panic_is_captured_with_node_context() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "PANIC_NODE",
        node(|_ctx| async move {
            panic!("panic-for-test");
            #[allow(unreachable_code)]
            Ok(i64_to_payload(0))
        }),
    )
    .unwrap();

    let err = DagScheduler::new(dag).execute_parallel().await.unwrap_err();
    match err {
        DagError::RetryExhausted {
            node_id,
            last_error,
            ..
        } => {
            assert_eq!(node_id, "PANIC_NODE");
            assert!(last_error.contains("node task panicked"));
            assert!(last_error.contains("panic-for-test"));
            assert!(last_error.contains("PANIC_NODE"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn execute_parallel_event_driven_allows_cross_layer_overlap() {
    let mut dag = Dag::new();
    let f_done = Arc::new(AtomicBool::new(false));
    let b_saw_f_done = Arc::new(AtomicBool::new(false));

    let a = dag
        .add_node_with_id(None, "A", node(|_ctx| async move { Ok(i64_to_payload(1)) }))
        .unwrap();

    dag.add_node_with_id(None, "F", {
        let f_done = f_done.clone();
        node(move |_ctx| {
            let f_done = f_done.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(250)).await;
                f_done.store(true, Ordering::SeqCst);
                Ok(i64_to_payload(6))
            }
        })
    })
    .unwrap();

    dag.add_node_with_id(Some(&[a]), "B", {
        let f_done = f_done.clone();
        let b_saw_f_done = b_saw_f_done.clone();
        node(move |_ctx| {
            let f_done = f_done.clone();
            let b_saw_f_done = b_saw_f_done.clone();
            async move {
                if f_done.load(Ordering::SeqCst) {
                    b_saw_f_done.store(true, Ordering::SeqCst);
                }
                Ok(i64_to_payload(2))
            }
        })
    })
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let _ = scheduler.execute_parallel().await.unwrap();

    assert!(f_done.load(Ordering::SeqCst));
    assert!(
        !b_saw_f_done.load(Ordering::SeqCst),
        "B should start before slow F completes under event-driven scheduling"
    );
}

#[tokio::test]
async fn execute_parallel_respects_max_in_flight_limit() {
    let mut dag = Dag::new();
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    for idx in 0..5 {
        let running_ref = running.clone();
        let peak_ref = peak.clone();
        dag.add_node_with_id(
            None,
            format!("N{idx}"),
            node(move |_ctx| {
                let running_ref = running_ref.clone();
                let peak_ref = peak_ref.clone();
                async move {
                    let now = running_ref.fetch_add(1, Ordering::SeqCst) + 1;
                    peak_ref.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    running_ref.fetch_sub(1, Ordering::SeqCst);
                    Ok(i64_to_payload(1))
                }
            }),
        )
        .unwrap();
    }

    let scheduler = DagScheduler::new(dag).with_options(DagSchedulerOptions {
        max_in_flight: 2,
        cancel_inflight_on_failure: true,
        run_timeout_ms: None,
    });

    let _ = scheduler.execute_parallel().await.unwrap();
    assert!(peak.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn execute_parallel_retries_then_succeeds() {
    let mut dag = Dag::new();
    let attempts = Arc::new(AtomicUsize::new(0));
    let n = {
        let attempts = attempts.clone();
        dag.add_node_with_id(
            None,
            "R",
            node(move |_ctx| {
                let attempts = attempts.clone();
                async move {
                    let now = attempts.fetch_add(1, Ordering::SeqCst);
                    if now == 0 {
                        Err(DagError::InvalidPayloadEnvelope("first fail".to_string()))
                    } else {
                        Ok(i64_to_payload(9))
                    }
                }
            }),
        )
        .unwrap()
    };

    dag.set_node_execution_policy(
        &n,
        DagNodeExecutionPolicy {
            max_retries: 1,
            timeout_ms: None,
            retry_backoff_ms: 0,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::AllErrors,
            circuit_breaker_failure_threshold: None,
            circuit_breaker_open_ms: 0,
        },
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let report = scheduler.execute_parallel().await.unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        payload_to_i64(report.outputs.get("R").expect("R output missing")),
        9
    );
}

#[tokio::test]
async fn execute_parallel_ready_queue_dedup_avoids_duplicate_dispatch() {
    let mut dag = Dag::new();
    let b_attempts = Arc::new(AtomicUsize::new(0));

    let a = dag
        .add_node_with_id(
            None,
            "RQ_A",
            node(|_ctx| async move { Ok(i64_to_payload(1)) }),
        )
        .unwrap();

    let b = {
        let b_attempts = b_attempts.clone();
        dag.add_node_with_id(
            Some(&[a]),
            "RQ_B",
            node(move |_ctx| {
                let b_attempts = b_attempts.clone();
                async move {
                    let now = b_attempts.fetch_add(1, Ordering::SeqCst);
                    if now == 0 {
                        return Err(DagError::InvalidPayloadEnvelope(
                            "rq_b_first_fail".to_string(),
                        ));
                    }
                    Ok(i64_to_payload(2))
                }
            }),
        )
        .unwrap()
    };

    dag.set_node_execution_policy(
        &b,
        DagNodeExecutionPolicy {
            max_retries: 1,
            timeout_ms: None,
            retry_backoff_ms: 0,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::AllErrors,
            circuit_breaker_failure_threshold: None,
            circuit_breaker_open_ms: 0,
        },
    )
    .unwrap();

    dag.add_node_with_id(
        Some(&[b]),
        "RQ_C",
        node(|ctx| async move {
            let v = ctx
                .upstream_outputs
                .get("RQ_B")
                .map(payload_to_i64)
                .unwrap_or(0);
            Ok(i64_to_payload(v + 10))
        }),
    )
    .unwrap();

    let report = DagScheduler::new(dag).execute_parallel().await.unwrap();
    assert_eq!(b_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        payload_to_i64(report.outputs.get("RQ_C").expect("RQ_C output missing")),
        12
    );
}

#[tokio::test]
async fn execute_parallel_timeout_fails() {
    let mut dag = Dag::new();
    let n = dag
        .add_node_with_id(
            None,
            "SLOW",
            node(|_ctx| async move {
                tokio::time::sleep(Duration::from_millis(120)).await;
                Ok(i64_to_payload(1))
            }),
        )
        .unwrap();

    dag.set_node_execution_policy(
        &n,
        DagNodeExecutionPolicy {
            max_retries: 0,
            timeout_ms: Some(20),
            retry_backoff_ms: 0,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::AllErrors,
            circuit_breaker_failure_threshold: None,
            circuit_breaker_open_ms: 0,
        },
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let err = scheduler.execute_parallel().await.unwrap_err();
    match err {
        DagError::RetryExhausted {
            node_id,
            last_error: reason,
            ..
        } => {
            assert_eq!(node_id, "SLOW");
            assert!(reason.contains("timeout"));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[tokio::test]
async fn execute_parallel_reuses_idempotency_result() {
    let mut dag = Dag::new();
    let run_count = Arc::new(AtomicUsize::new(0));

    let n1 = {
        let run_count = run_count.clone();
        dag.add_node_with_id(
            None,
            "I1",
            node(move |_ctx| {
                let run_count = run_count.clone();
                async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    Ok(i64_to_payload(42))
                }
            }),
        )
        .unwrap()
    };

    let n2 = {
        let run_count = run_count.clone();
        dag.add_node_with_id(
            None,
            "I2",
            node(move |_ctx| {
                let run_count = run_count.clone();
                async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    Ok(i64_to_payload(42))
                }
            }),
        )
        .unwrap()
    };

    let policy = DagNodeExecutionPolicy {
        max_retries: 0,
        timeout_ms: None,
        retry_backoff_ms: 0,
        idempotency_key: Some("idem-key-1".to_string()),
        retry_mode: DagNodeRetryMode::AllErrors,
        circuit_breaker_failure_threshold: None,
        circuit_breaker_open_ms: 0,
    };
    dag.set_node_execution_policy(&n1, policy.clone()).unwrap();
    dag.set_node_execution_policy(&n2, policy).unwrap();

    let scheduler = DagScheduler::new(dag).with_options(DagSchedulerOptions {
        max_in_flight: 1,
        cancel_inflight_on_failure: true,
        run_timeout_ms: None,
    });
    let report = scheduler.execute_parallel().await.unwrap();

    assert_eq!(run_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        payload_to_i64(report.outputs.get("I1").expect("I1 output missing")),
        42
    );
    assert_eq!(
        payload_to_i64(report.outputs.get("I2").expect("I2 output missing")),
        42
    );
}

#[tokio::test]
async fn execute_parallel_singleflight_for_inflight_same_idempotency_key() {
    let mut dag = Dag::new();
    let run_count = Arc::new(AtomicUsize::new(0));

    let n1 = {
        let run_count = run_count.clone();
        dag.add_node_with_id(
            None,
            "J1",
            node(move |_ctx| {
                let run_count = run_count.clone();
                async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    Ok(i64_to_payload(77))
                }
            }),
        )
        .unwrap()
    };

    let n2 = {
        let run_count = run_count.clone();
        dag.add_node_with_id(
            None,
            "J2",
            node(move |_ctx| {
                let run_count = run_count.clone();
                async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    Ok(i64_to_payload(77))
                }
            }),
        )
        .unwrap()
    };

    let policy = DagNodeExecutionPolicy {
        max_retries: 0,
        timeout_ms: None,
        retry_backoff_ms: 0,
        idempotency_key: Some("same-key".to_string()),
        retry_mode: DagNodeRetryMode::AllErrors,
        circuit_breaker_failure_threshold: None,
        circuit_breaker_open_ms: 0,
    };
    dag.set_node_execution_policy(&n1, policy.clone()).unwrap();
    dag.set_node_execution_policy(&n2, policy).unwrap();

    let scheduler = DagScheduler::new(dag).with_options(DagSchedulerOptions {
        max_in_flight: 4,
        cancel_inflight_on_failure: true,
        run_timeout_ms: None,
    });
    let report = scheduler.execute_parallel().await.unwrap();

    assert_eq!(run_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        payload_to_i64(report.outputs.get("J1").expect("J1 output missing")),
        77
    );
    assert_eq!(
        payload_to_i64(report.outputs.get("J2").expect("J2 output missing")),
        77
    );
}

#[tokio::test]
async fn execute_parallel_emits_syncable_state() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "S1",
        node(|_ctx| async move { Ok(i64_to_payload(11)) }),
    )
    .unwrap();

    let sync_service = SyncService::new(None, "dag_schedule_test".to_string());
    let scheduler = DagScheduler::new(dag).with_sync_service(sync_service.clone());

    let _ = scheduler.execute_parallel().await.unwrap();
    let latest = sync_service
        .fetch_latest::<DagNodeSyncState>()
        .await
        .expect("fetch latest sync state")
        .expect("sync state should exist");

    assert!(!latest.run_id.is_empty());
    assert!(!latest.node_id.is_empty());
    assert!(
        matches!(latest.stage.as_str(), "succeeded" | "running" | "cache_hit"),
        "unexpected stage: {}",
        latest.stage
    );
}

#[tokio::test]
async fn execute_parallel_respects_run_timeout() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "T1",
        node(|_ctx| async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            Ok(i64_to_payload(1))
        }),
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag).with_options(DagSchedulerOptions {
        max_in_flight: 1,
        cancel_inflight_on_failure: true,
        run_timeout_ms: Some(10),
    });

    let err = scheduler.execute_parallel().await.unwrap_err();
    assert!(matches!(err, DagError::ExecutionTimeout { .. }));
}

#[tokio::test]
async fn execute_parallel_retryable_only_does_not_retry_non_retryable_error() {
    let mut dag = Dag::new();
    let attempts = Arc::new(AtomicUsize::new(0));

    let n = {
        let attempts = attempts.clone();
        dag.add_node_with_id(
            None,
            "NR",
            node(move |_ctx| {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(DagError::InvalidPayloadEnvelope("bad payload".to_string()))
                }
            }),
        )
        .unwrap()
    };

    dag.set_node_execution_policy(
        &n,
        DagNodeExecutionPolicy {
            max_retries: 3,
            timeout_ms: None,
            retry_backoff_ms: 0,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::RetryableOnly,
            circuit_breaker_failure_threshold: None,
            circuit_breaker_open_ms: 0,
        },
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let err = scheduler.execute_parallel().await.unwrap_err();

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(matches!(err, DagError::RetryExhausted { .. }));
}

#[tokio::test]
async fn execute_parallel_retryable_only_retries_timeout_error() {
    let mut dag = Dag::new();

    let n = dag
        .add_node_with_id(
            None,
            "RT",
            node(|ctx| async move {
                if ctx.attempt == 1 {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                }
                Ok(i64_to_payload(99))
            }),
        )
        .unwrap();

    dag.set_node_execution_policy(
        &n,
        DagNodeExecutionPolicy {
            max_retries: 2,
            timeout_ms: Some(10),
            retry_backoff_ms: 0,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::RetryableOnly,
            circuit_breaker_failure_threshold: None,
            circuit_breaker_open_ms: 0,
        },
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let report = scheduler.execute_parallel().await.unwrap();

    assert_eq!(
        payload_to_i64(report.outputs.get("RT").expect("RT output missing")),
        99
    );
}

#[tokio::test]
async fn execute_parallel_applies_circuit_breaker_open_window_before_retry() {
    let mut dag = Dag::new();

    let n = dag
        .add_node_with_id(
            None,
            "CB",
            node(|ctx| async move {
                if ctx.attempt == 1 {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
                Ok(i64_to_payload(5))
            }),
        )
        .unwrap();

    dag.set_node_execution_policy(
        &n,
        DagNodeExecutionPolicy {
            max_retries: 1,
            timeout_ms: Some(10),
            retry_backoff_ms: 0,
            idempotency_key: None,
            retry_mode: DagNodeRetryMode::RetryableOnly,
            circuit_breaker_failure_threshold: Some(1),
            circuit_breaker_open_ms: 40,
        },
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag);
    let started = tokio::time::Instant::now();
    let report = scheduler.execute_parallel().await.unwrap();
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(40),
        "expected breaker open window to delay retry, elapsed: {:?}",
        elapsed
    );
    assert_eq!(
        payload_to_i64(report.outputs.get("CB").expect("CB output missing")),
        5
    );
}

#[tokio::test]
async fn execute_parallel_dashmap_dispatcher_simulates_multi_worker_execution() {
    let mut dag = Dag::new();

    let n1 = dag
        .add_node_with_id(
            None,
            "DW1",
            node(|_ctx| async move { Ok(i64_to_payload(2)) }),
        )
        .unwrap();
    let n2 = dag
        .add_node_with_id(
            None,
            "DW2",
            node(|_ctx| async move { Ok(i64_to_payload(3)) }),
        )
        .unwrap();
    dag.set_node_placement(
        &n1,
        NodePlacement::Remote {
            worker_group: "worker-a".to_string(),
        },
    )
    .unwrap();
    dag.set_node_placement(
        &n2,
        NodePlacement::Remote {
            worker_group: "worker-b".to_string(),
        },
    )
    .unwrap();

    dag.add_node_with_id(
        Some(&[n1, n2]),
        "SUM",
        node(|ctx| async move {
            let sum = ctx
                .upstream_outputs
                .values()
                .map(payload_to_i64)
                .sum::<i64>();
            Ok(i64_to_payload(sum))
        }),
    )
    .unwrap();

    let dispatcher = Arc::new(DashMapWorkerDispatcher::default());
    let scheduler = DagScheduler::new(dag)
        .with_dispatcher(dispatcher.clone())
        .with_options(DagSchedulerOptions {
            max_in_flight: 4,
            cancel_inflight_on_failure: true,
            run_timeout_ms: None,
        });

    let report = scheduler.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(report.outputs.get("SUM").expect("SUM output missing")),
        5
    );
    assert!(
        dispatcher
            .dispatch_count_by_worker
            .get("worker-a")
            .map(|v| *v > 0)
            .unwrap_or(false)
    );
    assert!(
        dispatcher
            .dispatch_count_by_worker
            .get("worker-b")
            .map(|v| *v > 0)
            .unwrap_or(false)
    );
}

#[tokio::test]
async fn execute_parallel_runtime_override_retargets_dispatch_worker() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "OVERRIDE_STEP",
        node(|_ctx| async move { Ok(i64_to_payload(11)) }),
    )
    .unwrap();

    let dispatcher = Arc::new(DashMapWorkerDispatcher::default());
    let scheduler = DagScheduler::new(dag)
        .with_dispatcher(dispatcher.clone())
        .with_runtime_overrides(vec![
            DagNodeRuntimeOverride::new("OVERRIDE_STEP")
                .with_placement(NodePlacement::remote("wg-override")),
        ]);

    let report = scheduler.execute_parallel().await.unwrap();

    assert_eq!(
        payload_to_i64(
            report
                .outputs
                .get("OVERRIDE_STEP")
                .expect("override output missing")
        ),
        11
    );
    assert_eq!(
        dispatcher
            .dispatch_count_by_worker
            .get("wg-override")
            .map(|value| *value)
            .unwrap_or_default(),
        1
    );
    assert!(dispatcher.dispatch_count_by_worker.get("local").is_none());
}

#[tokio::test]
async fn execute_parallel_runtime_override_updates_retry_policy() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_for_node = attempts.clone();

    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "RETRY_OVERRIDE",
        node(move |_ctx| {
            let attempts_for_node = attempts_for_node.clone();
            async move {
                let attempt = attempts_for_node.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(DagError::NodeExecutionFailed {
                        node_id: "RETRY_OVERRIDE".to_string(),
                        reason: "fail first".to_string(),
                    })
                } else {
                    Ok(i64_to_payload(19))
                }
            }
        }),
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag).with_runtime_overrides(vec![
        DagNodeRuntimeOverride::new("RETRY_OVERRIDE").with_execution_policy(
            DagNodeExecutionPolicy {
                max_retries: 1,
                ..DagNodeExecutionPolicy::default()
            },
        ),
    ]);

    let report = scheduler.execute_parallel().await.unwrap();

    assert_eq!(
        payload_to_i64(
            report
                .outputs
                .get("RETRY_OVERRIDE")
                .expect("retry override output missing")
        ),
        19
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn execute_parallel_runtime_override_injects_runtime_input() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "RUNTIME_INPUT_STEP",
        node(|ctx| async move { Ok(ctx.runtime_input.expect("runtime input should be present")) }),
    )
    .unwrap();

    let runtime_input = TaskPayload::from_bytes(br#"{\"cursor\":\"abc\"}"#.to_vec())
        .with_content_type("application/json")
        .with_meta("schema", "test.runtime_input");

    let report = DagScheduler::new(dag)
        .with_runtime_overrides(vec![
            DagNodeRuntimeOverride::new("RUNTIME_INPUT_STEP")
                .with_runtime_input(runtime_input.clone()),
        ])
        .execute_parallel()
        .await
        .expect("scheduler should pass runtime input through override");

    assert_eq!(
        report
            .outputs
            .get("RUNTIME_INPUT_STEP")
            .expect("runtime input output missing"),
        &runtime_input
    );
}

#[tokio::test]
async fn execute_parallel_runs_five_dags_with_two_nodes_each_concurrently() {
    let mut tasks = Vec::new();

    for dag_idx in 0..5_i64 {
        tasks.push(tokio::spawn(async move {
            let mut dag = Dag::new();

            let source_id = format!("D{dag_idx}_N1");
            let sink_id = format!("D{dag_idx}_N2");

            let source = dag
                .add_node_with_id(
                    None,
                    &source_id,
                    node(move |_ctx| async move { Ok(i64_to_payload(dag_idx)) }),
                )
                .unwrap();

            dag.add_node_with_id(
                Some(&[source]),
                &sink_id,
                node(move |ctx| async move {
                    let v = ctx
                        .upstream_outputs
                        .values()
                        .next()
                        .map(payload_to_i64)
                        .unwrap_or(0);
                    Ok(i64_to_payload(v + 100))
                }),
            )
            .unwrap();

            let scheduler = DagScheduler::new(dag).with_options(DagSchedulerOptions {
                max_in_flight: 2,
                cancel_inflight_on_failure: true,
                run_timeout_ms: Some(5_000),
            });

            let report = scheduler.execute_parallel().await.unwrap();
            let out = report
                .outputs
                .get(&sink_id)
                .map(payload_to_i64)
                .expect("sink output missing");
            (dag_idx, out)
        }));
    }

    let results = futures::future::join_all(tasks).await;
    assert_eq!(results.len(), 5);

    for joined in results {
        let (dag_idx, out) = joined.expect("dag task join should succeed");
        assert_eq!(out, dag_idx + 100);
    }
}

#[tokio::test]
async fn execute_parallel_run_guard_blocks_duplicate_run_in_memory() {
    fn build_two_node_dag() -> Dag {
        let mut dag = Dag::new();
        let a = dag
            .add_node_with_id(
                None,
                "RG_A",
                node(|_ctx| async move {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    Ok(i64_to_payload(1))
                }),
            )
            .unwrap();
        dag.add_node_with_id(
            Some(&[a]),
            "RG_B",
            node(|ctx| async move {
                let v = ctx
                    .upstream_outputs
                    .values()
                    .next()
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(v + 1))
            }),
        )
        .unwrap();
        dag
    }

    let guard = Arc::new(InMemoryDagRunGuard::default());
    let scheduler_1 = DagScheduler::new(build_two_node_dag()).with_run_guard(
        guard.clone(),
        "dag:run-lock:mem",
        3_000,
    );
    let scheduler_2 =
        DagScheduler::new(build_two_node_dag()).with_run_guard(guard, "dag:run-lock:mem", 3_000);

    let first = tokio::spawn(async move { scheduler_1.execute_parallel().await });
    tokio::time::sleep(Duration::from_millis(15)).await;
    let second = scheduler_2.execute_parallel().await;

    assert!(matches!(second, Err(DagError::RunAlreadyInProgress { .. })));

    let first_report = first
        .await
        .expect("first run join should succeed")
        .expect("first run should succeed");
    assert_eq!(
        payload_to_i64(first_report.outputs.get("RG_B").expect("RG_B missing")),
        2
    );
}

#[tokio::test]
async fn execute_parallel_run_guard_heartbeat_renews_in_memory_for_long_run() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "RGH_A",
        node(|_ctx| async move {
            tokio::time::sleep(Duration::from_millis(220)).await;
            Ok(i64_to_payload(7))
        }),
    )
    .unwrap();

    let guard = Arc::new(InMemoryDagRunGuard::default());
    let scheduler = DagScheduler::new(dag)
        .with_run_guard(guard.clone(), "dag:run-lock:mem:heartbeat", 180)
        .with_run_guard_heartbeat_ms(Some(40));

    let report = scheduler.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(report.outputs.get("RGH_A").expect("RGH_A missing")),
        7
    );
    assert!(
        guard.renew_count.load(Ordering::SeqCst) >= 2,
        "heartbeat renew should be called at least twice"
    );
}

#[tokio::test]
async fn execute_parallel_run_timeout_interrupts_wait_loop_quickly() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "RT_FAST_A",
        node(|_ctx| async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            Ok(i64_to_payload(1))
        }),
    )
    .unwrap();

    let scheduler = DagScheduler::new(dag).with_options(DagSchedulerOptions {
        max_in_flight: 1,
        cancel_inflight_on_failure: true,
        run_timeout_ms: Some(60),
    });

    let started = Instant::now();
    let err = scheduler.execute_parallel().await.unwrap_err();
    assert!(matches!(err, DagError::ExecutionTimeout { .. }));
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "run timeout should interrupt wait loop quickly"
    );
}

#[tokio::test]
async fn execute_parallel_run_guard_renew_lost_fails_fast() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "RGL_A",
        node(|_ctx| async move {
            tokio::time::sleep(Duration::from_millis(700)).await;
            Ok(i64_to_payload(1))
        }),
    )
    .unwrap();

    let guard = Arc::new(FailAfterNRenewRunGuard::new(1, 1));
    let scheduler = DagScheduler::new(dag)
        .with_run_guard(guard, "dag:run-lock:mem:renew-lost", 150)
        .with_run_guard_heartbeat_ms(Some(40));

    let started = Instant::now();
    let err = scheduler.execute_parallel().await.unwrap_err();
    assert!(matches!(err, DagError::RunGuardRenewFailed { .. }));
    assert!(
        started.elapsed() < Duration::from_millis(350),
        "renew loss should fail fast without waiting for task completion"
    );
}

#[tokio::test]
async fn execute_parallel_run_guard_propagates_fencing_token_into_context() {
    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "RGT_A",
        node(|ctx| async move {
            let token = ctx
                .run_fencing_token
                .expect("run fencing token should be present");
            Ok(i64_to_payload(token as i64))
        }),
    )
    .unwrap();

    let guard = Arc::new(InMemoryDagRunGuard::default());
    let scheduler = DagScheduler::new(dag).with_run_guard(guard, "dag:run-lock:mem:token", 3_000);

    let report = scheduler.execute_parallel().await.unwrap();
    let token = payload_to_i64(report.outputs.get("RGT_A").expect("RGT_A missing"));
    assert!(token > 0);
}

#[tokio::test]
async fn execute_parallel_fencing_store_requires_run_fencing_token() {
    let mut dag = Dag::new();
    let run_count = Arc::new(AtomicUsize::new(0));
    let run_count_clone = run_count.clone();
    dag.add_node_with_id(
        None,
        "FS_A",
        node(move |_ctx| {
            let run_count = run_count_clone.clone();
            async move {
                run_count.fetch_add(1, Ordering::SeqCst);
                Ok(i64_to_payload(1))
            }
        }),
    )
    .unwrap();

    let store = Arc::new(InMemoryDagFencingStore::default());
    let scheduler = DagScheduler::new(dag).with_fencing_store(store, "fencing:resource:1");

    let err = scheduler.execute_parallel().await.unwrap_err();
    assert!(matches!(err, DagError::MissingRunFencingToken { .. }));
    assert_eq!(run_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn execute_parallel_fencing_store_rejects_stale_token() {
    let build_dag = || {
        let mut dag = Dag::new();
        dag.add_node_with_id(
            None,
            "FS_B",
            node(|_ctx| async move { Ok(i64_to_payload(2)) }),
        )
        .unwrap();
        dag
    };

    let store = Arc::new(InMemoryDagFencingStore::default());
    let guard_high = Arc::new(FixedTokenRunGuard { token: 10 });
    let guard_low = Arc::new(FixedTokenRunGuard { token: 9 });

    let scheduler_1 = DagScheduler::new(build_dag())
        .with_run_guard(guard_high, "fencing:lock:stale", 3_000)
        .with_fencing_store(store.clone(), "fencing:resource:stale");
    let report = scheduler_1.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(report.outputs.get("FS_B").expect("FS_B missing")),
        2
    );

    let scheduler_2 = DagScheduler::new(build_dag())
        .with_run_guard(guard_low, "fencing:lock:stale", 3_000)
        .with_fencing_store(store, "fencing:resource:stale");
    let err = scheduler_2.execute_parallel().await.unwrap_err();
    assert!(matches!(err, DagError::FencingTokenRejected { .. }));
}

#[tokio::test]
async fn execute_parallel_resumes_from_run_state_store_snapshot() {
    let mut dag = Dag::new();
    dag.metadata
        .insert("profile_version".to_string(), "7".to_string());
    dag.metadata
        .insert("dag_version".to_string(), "dag-v1".to_string());
    let a_runs = Arc::new(AtomicUsize::new(0));
    let a_runs_clone = a_runs.clone();
    let a = dag
        .add_node_with_id(
            None,
            "RS_A",
            node(move |_ctx| {
                let a_runs = a_runs_clone.clone();
                async move {
                    a_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(i64_to_payload(10))
                }
            }),
        )
        .unwrap();
    dag.add_node_with_id(
        Some(&[a]),
        "RS_B",
        node(|ctx| async move {
            let v = ctx
                .upstream_outputs
                .get("RS_A")
                .map(payload_to_i64)
                .unwrap_or(0);
            Ok(i64_to_payload(v + 5))
        }),
    )
    .unwrap();

    let store = Arc::new(InMemoryRunStateStore::default());
    store
        .save(
            "resume-key-1",
            &DagRunResumeState {
                run_id: "resume-run-1".to_string(),
                run_fencing_token: Some(123),
                runtime_identity: std::iter::once(("profile_version".to_string(), "7".to_string()))
                    .chain(std::iter::once((
                        "dag_version".to_string(),
                        "dag-v1".to_string(),
                    )))
                    .collect(),
                succeeded_outputs: std::iter::once(("RS_A".to_string(), i64_to_payload(10)))
                    .collect(),
            },
        )
        .await
        .unwrap();

    let scheduler = DagScheduler::new(dag).with_run_state_store(store.clone(), "resume-key-1");
    let report = scheduler.execute_parallel().await.unwrap();

    assert_eq!(
        payload_to_i64(report.outputs.get("RS_A").expect("RS_A missing")),
        10
    );
    assert_eq!(
        payload_to_i64(report.outputs.get("RS_B").expect("RS_B missing")),
        15
    );
    assert_eq!(a_runs.load(Ordering::SeqCst), 0);
    assert!(store.load("resume-key-1").await.unwrap().is_none());
}

#[tokio::test]
async fn execute_parallel_discards_resume_snapshot_when_runtime_identity_changes() {
    let mut dag = Dag::new();
    dag.metadata
        .insert("profile_version".to_string(), "8".to_string());
    dag.metadata
        .insert("dag_version".to_string(), "dag-v2".to_string());
    let a_runs = Arc::new(AtomicUsize::new(0));
    let a_runs_clone = a_runs.clone();
    let a = dag
        .add_node_with_id(
            None,
            "RI_A",
            node(move |_ctx| {
                let a_runs = a_runs_clone.clone();
                async move {
                    a_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(i64_to_payload(20))
                }
            }),
        )
        .unwrap();
    dag.add_node_with_id(
        Some(&[a]),
        "RI_B",
        node(|ctx| async move {
            let v = ctx
                .upstream_outputs
                .get("RI_A")
                .map(payload_to_i64)
                .unwrap_or(0);
            Ok(i64_to_payload(v + 5))
        }),
    )
    .unwrap();

    let store = Arc::new(InMemoryRunStateStore::default());
    store
        .save(
            "resume-key-identity-1",
            &DagRunResumeState {
                run_id: "resume-run-identity-1".to_string(),
                run_fencing_token: Some(321),
                runtime_identity: std::iter::once(("profile_version".to_string(), "7".to_string()))
                    .chain(std::iter::once((
                        "dag_version".to_string(),
                        "dag-v1".to_string(),
                    )))
                    .collect(),
                succeeded_outputs: std::iter::once(("RI_A".to_string(), i64_to_payload(10)))
                    .collect(),
            },
        )
        .await
        .unwrap();

    let scheduler =
        DagScheduler::new(dag).with_run_state_store(store.clone(), "resume-key-identity-1");
    let report = scheduler.execute_parallel().await.unwrap();

    assert_eq!(
        payload_to_i64(report.outputs.get("RI_A").expect("RI_A missing")),
        20
    );
    assert_eq!(
        payload_to_i64(report.outputs.get("RI_B").expect("RI_B missing")),
        25
    );
    assert_eq!(a_runs.load(Ordering::SeqCst), 1);
    assert!(store.load("resume-key-identity-1").await.unwrap().is_none());
}

#[tokio::test]
async fn execute_parallel_failure_snapshot_allows_resume_without_rerunning_success_nodes() {
    let mut dag = Dag::new();
    let a_runs = Arc::new(AtomicUsize::new(0));
    let b_runs = Arc::new(AtomicUsize::new(0));

    let a_runs_clone = a_runs.clone();
    let a = dag
        .add_node_with_id(
            None,
            "FR_A",
            node(move |_ctx| {
                let a_runs = a_runs_clone.clone();
                async move {
                    a_runs.fetch_add(1, Ordering::SeqCst);
                    Ok(i64_to_payload(10))
                }
            }),
        )
        .unwrap();

    let b_runs_clone = b_runs.clone();
    dag.add_node_with_id(
        Some(&[a]),
        "FR_B",
        node(move |ctx| {
            let b_runs = b_runs_clone.clone();
            async move {
                let current = b_runs.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    return Err(DagError::NodeExecutionFailed {
                        node_id: "FR_B".to_string(),
                        reason: "first-run-fail".to_string(),
                    });
                }
                let base = ctx
                    .upstream_outputs
                    .get("FR_A")
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(base + 5))
            }
        }),
    )
    .unwrap();

    let store = Arc::new(InMemoryRunStateStore::default());

    let scheduler_first =
        DagScheduler::new(dag.clone()).with_run_state_store(store.clone(), "resume-key-fail-1");
    let first_err = scheduler_first.execute_parallel().await.unwrap_err();
    assert!(matches!(first_err, DagError::RetryExhausted { .. }));

    let snap = store
        .load("resume-key-fail-1")
        .await
        .unwrap()
        .expect("snapshot should be persisted on failure");
    assert!(snap.succeeded_outputs.contains_key("FR_A"));
    assert!(!snap.succeeded_outputs.contains_key("FR_B"));
    let rich_state = store
        .load_state("resume-key-fail-1")
        .await
        .unwrap()
        .expect("rich state should be persisted on failure");
    assert_eq!(rich_state.completed_nodes, vec!["FR_A".to_string()]);
    assert_eq!(
        rich_state
            .node_states
            .get("FR_A")
            .map(|node| node.status.clone()),
        Some(DagNodeStatus::Succeeded)
    );
    assert_eq!(
        rich_state
            .node_states
            .get("FR_B")
            .map(|node| node.status.clone()),
        Some(DagNodeStatus::Failed)
    );

    let scheduler_second =
        DagScheduler::new(dag).with_run_state_store(store.clone(), "resume-key-fail-1");
    let report = scheduler_second.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(report.outputs.get("FR_B").expect("FR_B missing")),
        15
    );
    assert_eq!(
        a_runs.load(Ordering::SeqCst),
        1,
        "FR_A should not rerun after resume"
    );
    assert_eq!(b_runs.load(Ordering::SeqCst), 2);

    assert!(store.load("resume-key-fail-1").await.unwrap().is_none());
}

#[tokio::test]
async fn execute_parallel_stop_signal_prevents_new_dispatch_and_persists_state() {
    let mut dag = Dag::new();
    let a_runs = Arc::new(AtomicUsize::new(0));
    let b_runs = Arc::new(AtomicUsize::new(0));

    let a_runs_clone = a_runs.clone();
    dag.add_node_with_id(
        None,
        "STOP_A",
        node(move |_ctx| {
            let a_runs = a_runs_clone.clone();
            async move {
                a_runs.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(120)).await;
                Ok(i64_to_payload(11))
            }
        }),
    )
    .unwrap();

    let b_runs_clone = b_runs.clone();
    dag.add_node_with_id(
        None,
        "STOP_B",
        node(move |_ctx| {
            let b_runs = b_runs_clone.clone();
            async move {
                b_runs.fetch_add(1, Ordering::SeqCst);
                Ok(i64_to_payload(22))
            }
        }),
    )
    .unwrap();

    let store = Arc::new(InMemoryRunStateStore::default());
    let scheduler = DagScheduler::new(dag)
        .with_options(DagSchedulerOptions {
            max_in_flight: 1,
            ..DagSchedulerOptions::default()
        })
        .with_run_state_store(store.clone(), "stop-key-1");

    let scheduler_task = tokio::spawn(async move { scheduler.execute_parallel().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    store
        .request_stop(
            "stop-key-1",
            DagStopSignal {
                requested_by: "tester".to_string(),
                reason: "manual stop".to_string(),
                requested_at_ms: 1,
            },
        )
        .await
        .unwrap();

    let err = scheduler_task
        .await
        .expect("scheduler join should succeed")
        .unwrap_err();
    assert!(matches!(err, DagError::RunStopped { .. }));
    assert_eq!(a_runs.load(Ordering::SeqCst), 1);
    assert_eq!(b_runs.load(Ordering::SeqCst), 0);

    let state = store
        .load_state("stop-key-1")
        .await
        .unwrap()
        .expect("stopped state should persist");
    assert_eq!(state.status, DagRunStatus::Stopped);
    assert_eq!(state.completed_nodes, vec!["STOP_A".to_string()]);
    assert_eq!(state.ready_nodes, vec!["STOP_B".to_string()]);
    assert_eq!(
        state
            .stop_signal
            .as_ref()
            .map(|signal| signal.reason.as_str()),
        Some("manual stop")
    );
}
