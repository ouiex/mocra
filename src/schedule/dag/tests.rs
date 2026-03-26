use super::*;
use async_trait::async_trait;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use redis::AsyncCommands;
use redis::Script;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;
use crate::cacheable::CacheService;
use crate::sync::SyncService;
use crate::utils::connector::create_redis_pool;

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

fn local_redis_cache_service(namespace: &str) -> Option<Arc<CacheService>> {
    let pool = create_redis_pool(
        "127.0.0.1",
        6379,
        0,
        &None,
        &None,
        Some(16),
        false,
    )?;
    Some(Arc::new(CacheService::new(
        Some(pool),
        namespace.to_string(),
        Some(Duration::from_secs(120)),
        Some(usize::MAX),
    )))
}

struct TestNode {
    logic: Arc<
        dyn Fn(NodeExecutionContext) -> std::pin::Pin<
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

struct RedisTracingDispatcher {
    client: redis::Client,
    key_prefix: String,
}

#[derive(Default)]
struct InMemoryRunGuard {
    locks: DashMap<String, (String, u64)>,
    renew_count: AtomicUsize,
    next_fencing_token: AtomicUsize,
}

#[async_trait]
impl DagRunGuard for InMemoryRunGuard {
    async fn try_acquire(
        &self,
        lock_key: &str,
        owner: &str,
        _ttl_ms: u64,
    ) -> Result<DagRunGuardAcquireOutcome, DagError> {
        match self.locks.entry(lock_key.to_string()) {
            Entry::Occupied(existing) => {
                let (current_owner, token) = existing.get();
                Ok(DagRunGuardAcquireOutcome {
                    acquired: current_owner == owner,
                    fencing_token: Some(*token),
                })
            }
            Entry::Vacant(slot) => {
                let token = self
                    .next_fencing_token
                    .fetch_add(1, Ordering::SeqCst)
                    .saturating_add(1) as u64;
                slot.insert((owner.to_string(), token));
                Ok(DagRunGuardAcquireOutcome {
                    acquired: true,
                    fencing_token: Some(token),
                })
            }
        }
    }

    async fn renew(&self, lock_key: &str, owner: &str, _ttl_ms: u64) -> Result<bool, DagError> {
        self.renew_count.fetch_add(1, Ordering::SeqCst);
        let ok = self
            .locks
            .get(lock_key)
            .map(|v| v.value().0 == owner)
            .unwrap_or(false);
        Ok(ok)
    }

    async fn release(&self, lock_key: &str, owner: &str) -> Result<(), DagError> {
        if let Some(current) = self.locks.get(lock_key) {
            if current.value().0 != owner {
                return Ok(());
            }
        }
        self.locks.remove(lock_key);
        Ok(())
    }
}

struct RedisRunGuard {
    client: redis::Client,
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
struct InMemoryFencingStore {
    latest_token_by_resource: DashMap<String, u64>,
}

#[derive(Default)]
struct InMemoryRunStateStore {
    snapshots: DashMap<String, DagRunResumeState>,
}

#[async_trait]
impl DagRunStateStore for InMemoryRunStateStore {
    async fn load(&self, run_key: &str) -> Result<Option<DagRunResumeState>, DagError> {
        Ok(self.snapshots.get(run_key).map(|v| v.clone()))
    }

    async fn save(&self, run_key: &str, state: &DagRunResumeState) -> Result<(), DagError> {
        self.snapshots.insert(run_key.to_string(), state.clone());
        Ok(())
    }

    async fn clear(&self, run_key: &str) -> Result<(), DagError> {
        self.snapshots.remove(run_key);
        Ok(())
    }
}

#[async_trait]
impl DagFencingStore for InMemoryFencingStore {
    async fn commit(
        &self,
        resource: &str,
        token: u64,
        node_id: &str,
        _payload: &TaskPayload,
    ) -> Result<(), DagError> {
        match self.latest_token_by_resource.entry(resource.to_string()) {
            Entry::Occupied(mut slot) => {
                if token <= *slot.get() {
                    return Err(DagError::NodeExecutionFailed {
                        node_id: node_id.to_string(),
                        reason: format!(
                            "stale fencing token: token={} latest={}",
                            token,
                            slot.get()
                        ),
                    });
                }
                slot.insert(token);
                Ok(())
            }
            Entry::Vacant(slot) => {
                slot.insert(token);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl DagRunGuard for RedisRunGuard {
    async fn try_acquire(
        &self,
        lock_key: &str,
        owner: &str,
        ttl_ms: u64,
    ) -> Result<DagRunGuardAcquireOutcome, DagError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::RunGuardAcquireFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        let token_key = format!("{lock_key}:token");
        let holder_token_key = format!("{lock_key}:holder_token");
        let script = Script::new(
            r#"
            if redis.call('GET', KEYS[1]) then
                return 0
            end
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            local t = redis.call('INCR', KEYS[2])
            redis.call('SET', KEYS[3], t, 'PX', ARGV[2])
            return t
            "#,
        );
        let token: i64 = script
            .key(lock_key)
            .key(&token_key)
            .key(&holder_token_key)
            .arg(owner)
            .arg(ttl_ms as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| DagError::RunGuardAcquireFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        if token <= 0 {
            return Ok(DagRunGuardAcquireOutcome {
                acquired: false,
                fencing_token: None,
            });
        }

        Ok(DagRunGuardAcquireOutcome {
            acquired: true,
            fencing_token: Some(token as u64),
        })
    }

    async fn renew(&self, lock_key: &str, owner: &str, ttl_ms: u64) -> Result<bool, DagError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::RunGuardRenewFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        let script = Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('PEXPIRE', KEYS[1], ARGV[2])
            end
            return 0
            "#,
        );
        let renewed: i64 = script
            .key(lock_key)
            .arg(owner)
            .arg(ttl_ms as i64)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| DagError::RunGuardRenewFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        Ok(renewed == 1)
    }

    async fn release(&self, lock_key: &str, owner: &str) -> Result<(), DagError> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::RunGuardReleaseFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        let script = Script::new(
            r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                redis.call('DEL', KEYS[2])
                return redis.call('DEL', KEYS[1])
            end
            return 0
            "#,
        );
        let holder_token_key = format!("{lock_key}:holder_token");
        let _: i64 = script
            .key(lock_key)
            .key(&holder_token_key)
            .arg(owner)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| DagError::RunGuardReleaseFailed {
                lock_key: lock_key.to_string(),
                reason: e.to_string(),
            })?;

        Ok(())
    }
}

#[async_trait]
impl DagNodeDispatcher for RedisTracingDispatcher {
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

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| DagError::NodeExecutionFailed {
                node_id: node_id.to_string(),
                reason: format!("redis connect failed: {e}"),
            })?;

        let list_key = format!("{}:dispatches", self.key_prefix);
        let worker_key = format!("{}:worker_count", self.key_prefix);

        let _: usize = conn
            .lpush(&list_key, format!("{}|{}", worker, node_id))
            .await
            .map_err(|e| DagError::NodeExecutionFailed {
                node_id: node_id.to_string(),
                reason: format!("redis lpush failed: {e}"),
            })?;
        let _: f64 = conn
            .hincr(&worker_key, worker, 1)
            .await
            .map_err(|e| DagError::NodeExecutionFailed {
                node_id: node_id.to_string(),
                reason: format!("redis hincr failed: {e}"),
            })?;

        executor.start(context).await
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
        executor: None,
        result: None,
        metadata: std::collections::HashMap::new(),
        error: None,
    });
    let err = dag
        .add_node(Some(&[fake]), node(|_ctx| async move { Ok(i64_to_payload(1)) }))
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
        .add_node(Some(&[end]), node(|_ctx| async move { Ok(i64_to_payload(1)) }))
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
            Some(&[a.clone()]),
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
            Some(&[a.clone()]),
            node(|_ctx| async move { Ok(i64_to_payload(20)) }),
        )
        .unwrap();
    let c = dag
        .add_node(
            Some(&[b.clone()]),
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
            Some(&[create.clone()]),
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
            Some(&[write.clone()]),
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

                let content = std::fs::read_to_string(&path).map_err(|e| DagError::NodeExecutionFailed {
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
    assert!(!file_path.exists(), "file should be deleted by FILE_DELETE node");
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
            Some(&[a.clone()]),
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
        .add_node_with_id(None, "S", node(|_ctx| async move { Ok(i64_to_payload(10)) }))
        .unwrap();

    let x = dag
        .add_node_with_id(
            Some(&[source.clone()]),
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
        .add_node_with_id(None, "REMOTE", node(|_ctx| async move { Ok(i64_to_payload(1)) }))
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
        DagError::RetryExhausted {
            node_id,
            last_error: reason,
            ..
        } => {
            assert_eq!(node_id, "REMOTE");
            assert!(reason.contains("remote dispatch not configured"));
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
        DagError::RetryExhausted { node_id, last_error, .. } => {
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

    dag.add_node_with_id(
        None,
        "F",
        {
            let f_done = f_done.clone();
            node(move |_ctx| {
                let f_done = f_done.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    f_done.store(true, Ordering::SeqCst);
                    Ok(i64_to_payload(6))
                }
            })
        },
    )
    .unwrap();

    dag.add_node_with_id(
        Some(&[a]),
        "B",
        {
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
        },
    )
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
        .add_node_with_id(None, "RQ_A", node(|_ctx| async move { Ok(i64_to_payload(1)) }))
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
                        return Err(DagError::InvalidPayloadEnvelope("rq_b_first_fail".to_string()));
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
    dag.add_node_with_id(None, "S1", node(|_ctx| async move { Ok(i64_to_payload(11)) }))
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
        .add_node_with_id(None, "DW1", node(|_ctx| async move { Ok(i64_to_payload(2)) }))
        .unwrap();
    let n2 = dag
        .add_node_with_id(None, "DW2", node(|_ctx| async move { Ok(i64_to_payload(3)) }))
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
async fn execute_parallel_redis_dispatcher_records_multi_worker_traffic() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut ping_conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut ping_conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:test:{}", Uuid::now_v7());
    let dispatch_key = format!("{}:dispatches", key_prefix);
    let worker_key = format!("{}:worker_count", key_prefix);

    let mut dag = Dag::new();
    let r1 = dag
        .add_node_with_id(None, "RR1", node(|_ctx| async move { Ok(i64_to_payload(4)) }))
        .unwrap();
    let r2 = dag
        .add_node_with_id(None, "RR2", node(|_ctx| async move { Ok(i64_to_payload(6)) }))
        .unwrap();
    dag.set_node_placement(
        &r1,
        NodePlacement::Remote {
            worker_group: "redis-worker-a".to_string(),
        },
    )
    .unwrap();
    dag.set_node_placement(
        &r2,
        NodePlacement::Remote {
            worker_group: "redis-worker-b".to_string(),
        },
    )
    .unwrap();
    dag.add_node_with_id(
        Some(&[r1, r2]),
        "RR_SUM",
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

    let dispatcher = Arc::new(RedisTracingDispatcher {
        client: client.clone(),
        key_prefix: key_prefix.clone(),
    });
    let scheduler = DagScheduler::new(dag)
        .with_dispatcher(dispatcher)
        .with_options(DagSchedulerOptions {
            max_in_flight: 4,
            cancel_inflight_on_failure: true,
            run_timeout_ms: None,
        });

    let report = scheduler.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(
            report
                .outputs
                .get("RR_SUM")
                .expect("RR_SUM output missing")
        ),
        10
    );

    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let dispatches: Vec<String> = conn.lrange(&dispatch_key, 0, -1).await.unwrap_or_default();
    assert!(dispatches.len() >= 2, "redis should record remote dispatches");
    let workers: Vec<String> = conn.hkeys(&worker_key).await.unwrap_or_default();
    assert!(workers.iter().any(|w| w == "redis-worker-a"));
    assert!(workers.iter().any(|w| w == "redis-worker-b"));

    let _: usize = conn.del(&dispatch_key).await.unwrap_or(0);
    let _: usize = conn.del(&worker_key).await.unwrap_or(0);
}

#[tokio::test]
async fn execute_parallel_redis_singleflight_cross_worker_runs_once() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut ping_conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut ping_conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:singleflight:{}", Uuid::now_v7());
    let dispatch_key = format!("{}:dispatches", key_prefix);
    let worker_key = format!("{}:worker_count", key_prefix);

    let mut dag = Dag::new();
    let run_count = Arc::new(AtomicUsize::new(0));

    let n1 = {
        let run_count = run_count.clone();
        dag.add_node_with_id(
            None,
            "SF_R1",
            node(move |_ctx| {
                let run_count = run_count.clone();
                async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(i64_to_payload(21))
                }
            }),
        )
        .unwrap()
    };

    let n2 = {
        let run_count = run_count.clone();
        dag.add_node_with_id(
            None,
            "SF_R2",
            node(move |_ctx| {
                let run_count = run_count.clone();
                async move {
                    run_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok(i64_to_payload(21))
                }
            }),
        )
        .unwrap()
    };

    dag.set_node_placement(
        &n1,
        NodePlacement::Remote {
            worker_group: "sf-worker-a".to_string(),
        },
    )
    .unwrap();
    dag.set_node_placement(
        &n2,
        NodePlacement::Remote {
            worker_group: "sf-worker-b".to_string(),
        },
    )
    .unwrap();

    let sf_policy = DagNodeExecutionPolicy {
        max_retries: 0,
        timeout_ms: None,
        retry_backoff_ms: 0,
        idempotency_key: Some("redis-sf-key".to_string()),
        retry_mode: DagNodeRetryMode::AllErrors,
        circuit_breaker_failure_threshold: None,
        circuit_breaker_open_ms: 0,
    };
    dag.set_node_execution_policy(&n1, sf_policy.clone()).unwrap();
    dag.set_node_execution_policy(&n2, sf_policy).unwrap();

    let dispatcher = Arc::new(RedisTracingDispatcher {
        client: client.clone(),
        key_prefix: key_prefix.clone(),
    });
    let scheduler = DagScheduler::new(dag)
        .with_dispatcher(dispatcher)
        .with_options(DagSchedulerOptions {
            max_in_flight: 8,
            cancel_inflight_on_failure: true,
            run_timeout_ms: None,
        });

    let report = scheduler.execute_parallel().await.unwrap();
    assert_eq!(run_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        payload_to_i64(report.outputs.get("SF_R1").expect("SF_R1 missing")),
        21
    );
    assert_eq!(
        payload_to_i64(report.outputs.get("SF_R2").expect("SF_R2 missing")),
        21
    );

    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let dispatches: Vec<String> = conn.lrange(&dispatch_key, 0, -1).await.unwrap_or_default();
    let remote_sf_dispatches = dispatches
        .iter()
        .filter(|entry| entry.contains("SF_R1") || entry.contains("SF_R2"))
        .count();
    assert_eq!(
        remote_sf_dispatches, 1,
        "singleflight should dispatch only one remote owner node"
    );

    let _: usize = conn.del(&dispatch_key).await.unwrap_or(0);
    let _: usize = conn.del(&worker_key).await.unwrap_or(0);
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

    let guard = Arc::new(InMemoryRunGuard::default());
    let scheduler_1 = DagScheduler::new(build_two_node_dag()).with_run_guard(
        guard.clone(),
        "dag:run-lock:mem",
        3_000,
    );
    let scheduler_2 = DagScheduler::new(build_two_node_dag()).with_run_guard(
        guard,
        "dag:run-lock:mem",
        3_000,
    );

    let first = tokio::spawn(async move { scheduler_1.execute_parallel().await });
    tokio::time::sleep(Duration::from_millis(15)).await;
    let second = scheduler_2.execute_parallel().await;

    assert!(matches!(
        second,
        Err(DagError::RunAlreadyInProgress { .. })
    ));

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

    let guard = Arc::new(InMemoryRunGuard::default());
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

    let guard = Arc::new(InMemoryRunGuard::default());
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

    let store = Arc::new(InMemoryFencingStore::default());
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

    let store = Arc::new(InMemoryFencingStore::default());
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
async fn execute_parallel_redis_fencing_store_rejects_stale_token() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut ping_conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut ping_conn).await;
    if ping_res.is_err() {
        return;
    }

    let build_dag = || {
        let mut dag = Dag::new();
        dag.add_node_with_id(
            None,
            "RFS_A",
            node(|_ctx| async move { Ok(i64_to_payload(3)) }),
        )
        .unwrap();
        dag
    };

    let key_prefix = format!("dag:fencing:e2e:{}", Uuid::now_v7());
    let resource = "resource-stale";
    let latest_key = format!("{}:fencing:{}:latest_token", key_prefix, resource);

    let store = Arc::new(RedisDagFencingStore::new(client.clone(), &key_prefix));
    let guard_high = Arc::new(FixedTokenRunGuard { token: 100 });
    let guard_low = Arc::new(FixedTokenRunGuard { token: 99 });

    let scheduler_1 = DagScheduler::new(build_dag())
        .with_run_guard(guard_high, "redis:fencing:lock", 3_000)
        .with_fencing_store(store.clone(), resource);
    let report = scheduler_1.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(report.outputs.get("RFS_A").expect("RFS_A missing")),
        3
    );

    let scheduler_2 = DagScheduler::new(build_dag())
        .with_run_guard(guard_low, "redis:fencing:lock", 3_000)
        .with_fencing_store(store, resource);
    let err = scheduler_2.execute_parallel().await.unwrap_err();
    assert!(matches!(err, DagError::FencingTokenRejected { .. }));

    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let latest: Option<u64> = redis::cmd("GET")
        .arg(&latest_key)
        .query_async(&mut conn)
        .await
        .ok();
    assert_eq!(latest, Some(100));
    let _: usize = conn.del(&latest_key).await.unwrap_or(0);
}

#[tokio::test]
async fn execute_parallel_redis_fencing_store_rejects_invalid_latest_token_format() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut ping_conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut ping_conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:fencing:invalid-latest:{}", Uuid::now_v7());
    let resource = "resource-invalid-latest";
    let latest_key = format!("{}:fencing:{}:latest_token", key_prefix, resource);

    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("SET")
        .arg(&latest_key)
        .arg("not-a-number")
        .query_async(&mut conn)
        .await
        .unwrap();

    let store = RedisDagFencingStore::new(client.clone(), &key_prefix);
    let err = store
        .commit(resource, 101, "NODE_INVALID", &i64_to_payload(1))
        .await
        .unwrap_err();

    match err {
        DagError::FencingTokenRejected { reason, .. } => {
            assert!(reason.contains("invalid latest fencing token format"));
        }
        other => panic!("unexpected error: {other}"),
    }

    let latest_raw: Option<String> = redis::cmd("GET")
        .arg(&latest_key)
        .query_async(&mut conn)
        .await
        .ok();
    assert_eq!(latest_raw.as_deref(), Some("not-a-number"));

    let _: usize = conn.del(&latest_key).await.unwrap_or(0);
}

#[tokio::test]
async fn execute_parallel_resumes_from_run_state_store_snapshot() {
    let mut dag = Dag::new();
    let a = dag
        .add_node_with_id(
            None,
            "RS_A",
            node(|_ctx| async move { Ok(i64_to_payload(10)) }),
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
    assert!(store.load("resume-key-1").await.unwrap().is_none());
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

    let scheduler_second = DagScheduler::new(dag).with_run_state_store(store.clone(), "resume-key-fail-1");
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
async fn execute_parallel_real_redis_remote_dispatch_worker_flow() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:e2e:{}", Uuid::now_v7());
    let worker_group = "prod-wg-a";
    let stop = Arc::new(AtomicBool::new(false));
    let Some(cache) = local_redis_cache_service("dag-remote-e2e") else {
        return;
    };

    let worker = RedisDagWorker::new(cache.clone(), &key_prefix, worker_group)
        .with_idle_sleep_ms(10)
        .register_handler(
            "RR_A",
            node(|ctx| async move {
                let v = ctx
                    .upstream_outputs
                    .get("RR_SRC")
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(v + 1))
            }),
        )
        .register_handler(
            "RR_B",
            node(|ctx| async move {
                let v = ctx
                    .upstream_outputs
                    .get("RR_SRC")
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(v + 2))
            }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    let mut dag = Dag::new();
    let src = dag
        .add_node_with_id(None, "RR_SRC", node(|_ctx| async move { Ok(i64_to_payload(10)) }))
        .unwrap();
    let a = dag
        .add_node_with_id(
            Some(&[src.clone()]),
            "RR_A",
            node(|_ctx| async move { Err(DagError::NodeExecutionFailed { node_id: "RR_A".into(), reason: "should not execute locally".into() }) }),
        )
        .unwrap();
    let b = dag
        .add_node_with_id(
            Some(&[src]),
            "RR_B",
            node(|_ctx| async move { Err(DagError::NodeExecutionFailed { node_id: "RR_B".into(), reason: "should not execute locally".into() }) }),
        )
        .unwrap();
    dag.set_node_placement(
        &a,
        NodePlacement::Remote {
            worker_group: worker_group.to_string(),
        },
    )
    .unwrap();
    dag.set_node_placement(
        &b,
        NodePlacement::Remote {
            worker_group: worker_group.to_string(),
        },
    )
    .unwrap();
    dag.add_node_with_id(
        Some(&[a, b]),
        "RR_SUM",
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

    let dispatcher = Arc::new(RedisRemoteDispatcher::new(
        cache.clone(),
        &key_prefix,
        RedisRemoteDispatcherOptions {
            response_timeout_ms: 8_000,
            poll_interval_ms: 10,
            result_ttl_secs: 120,
        },
    ));
    let scheduler = DagScheduler::new(dag)
        .with_dispatcher(dispatcher)
        .with_options(DagSchedulerOptions {
            max_in_flight: 8,
            cancel_inflight_on_failure: true,
            run_timeout_ms: Some(15_000),
        });
    let report = scheduler.execute_parallel().await.unwrap();
    assert_eq!(
        payload_to_i64(report.outputs.get("RR_SUM").expect("RR_SUM missing")),
        23
    );

    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();
}

#[tokio::test]
async fn execute_parallel_real_redis_remote_dispatch_dedupes_same_attempt_request() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:dedupe:{}", Uuid::now_v7());
    let worker_group = "prod-wg-dedupe";
    let stop = Arc::new(AtomicBool::new(false));
    let execute_count = Arc::new(AtomicUsize::new(0));
    let Some(cache) = local_redis_cache_service("dag-remote-dedupe") else {
        return;
    };

    let execute_count_clone = execute_count.clone();
    let worker = RedisDagWorker::new(cache.clone(), &key_prefix, worker_group)
        .with_idle_sleep_ms(5)
        .register_handler(
            "RD_NODE",
            node(move |_ctx| {
                let execute_count = execute_count_clone.clone();
                async move {
                    execute_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok(i64_to_payload(42))
                }
            }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    let dispatcher = Arc::new(RedisRemoteDispatcher::new(
        cache,
        &key_prefix,
        RedisRemoteDispatcherOptions {
            response_timeout_ms: 5_000,
            poll_interval_ms: 5,
            result_ttl_secs: 60,
        },
    ));

    let placement = NodePlacement::Remote {
        worker_group: worker_group.to_string(),
    };
    let context = NodeExecutionContext {
        run_id: "dedupe-run-1".to_string(),
        run_fencing_token: None,
        node_id: "RD_NODE".to_string(),
        attempt: 0,
        upstream_nodes: vec![],
        upstream_outputs: Default::default(),
        layer_index: 1,
    };
    let local_executor = node(|_ctx| async move {
        Err(DagError::NodeExecutionFailed {
            node_id: "RD_NODE".to_string(),
            reason: "should not execute locally".to_string(),
        })
    });

    let t1 = {
        let dispatcher = dispatcher.clone();
        let placement = placement.clone();
        let context = context.clone();
        let local_executor = local_executor.clone();
        tokio::spawn(async move {
            dispatcher
                .dispatch("RD_NODE", &placement, local_executor, context)
                .await
        })
    };

    let t2 = {
        let dispatcher = dispatcher.clone();
        let placement = placement.clone();
        let context = context.clone();
        let local_executor = local_executor.clone();
        tokio::spawn(async move {
            dispatcher
                .dispatch("RD_NODE", &placement, local_executor, context)
                .await
        })
    };

    let r1 = t1.await.unwrap().unwrap();
    let r2 = t2.await.unwrap().unwrap();
    assert_eq!(payload_to_i64(&r1), 42);
    assert_eq!(payload_to_i64(&r2), 42);
    assert_eq!(execute_count.load(Ordering::SeqCst), 1);

    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();
}

#[tokio::test]
async fn execute_parallel_real_redis_remote_dispatch_dedupes_long_running_with_short_result_ttl() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:dedupe-long:{}", Uuid::now_v7());
    let worker_group = "prod-wg-dedupe-long";
    let stop = Arc::new(AtomicBool::new(false));
    let execute_count = Arc::new(AtomicUsize::new(0));
    let Some(cache) = local_redis_cache_service("dag-remote-dedupe-long") else {
        return;
    };

    let execute_count_clone = execute_count.clone();
    let worker = RedisDagWorker::new(cache.clone(), &key_prefix, worker_group)
        .with_idle_sleep_ms(5)
        .register_handler(
            "RD_LONG",
            node(move |_ctx| {
                let execute_count = execute_count_clone.clone();
                async move {
                    execute_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1_500)).await;
                    Ok(i64_to_payload(77))
                }
            }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    let dispatcher = Arc::new(RedisRemoteDispatcher::new(
        cache,
        &key_prefix,
        RedisRemoteDispatcherOptions {
            response_timeout_ms: 4_000,
            poll_interval_ms: 5,
            result_ttl_secs: 1,
        },
    ));

    let placement = NodePlacement::Remote {
        worker_group: worker_group.to_string(),
    };
    let context = NodeExecutionContext {
        run_id: "dedupe-long-run-1".to_string(),
        run_fencing_token: None,
        node_id: "RD_LONG".to_string(),
        attempt: 0,
        upstream_nodes: vec![],
        upstream_outputs: Default::default(),
        layer_index: 1,
    };
    let local_executor = node(|_ctx| async move {
        Err(DagError::NodeExecutionFailed {
            node_id: "RD_LONG".to_string(),
            reason: "should not execute locally".to_string(),
        })
    });

    let t1 = {
        let dispatcher = dispatcher.clone();
        let placement = placement.clone();
        let context = context.clone();
        let local_executor = local_executor.clone();
        tokio::spawn(async move {
            dispatcher
                .dispatch("RD_LONG", &placement, local_executor, context)
                .await
        })
    };

    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let t2 = {
        let dispatcher = dispatcher.clone();
        let placement = placement.clone();
        let context = context.clone();
        let local_executor = local_executor.clone();
        tokio::spawn(async move {
            dispatcher
                .dispatch("RD_LONG", &placement, local_executor, context)
                .await
        })
    };

    let r1 = t1.await.unwrap().unwrap();
    let r2 = t2.await.unwrap().unwrap();
    assert_eq!(payload_to_i64(&r1), 77);
    assert_eq!(payload_to_i64(&r2), 77);
    assert_eq!(
        execute_count.load(Ordering::SeqCst),
        1,
        "long-running same attempt should still dedupe"
    );

    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();
}

#[tokio::test]
async fn execute_parallel_real_redis_remote_worker_retries_transient_failure() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:worker-retry:{}", Uuid::now_v7());
    let worker_group = "prod-wg-retry";
    let stop = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(AtomicUsize::new(0));
    let Some(cache) = local_redis_cache_service("dag-remote-worker-retry") else {
        return;
    };

    let attempts_clone = attempts.clone();
    let worker = RedisDagWorker::new(cache.clone(), &key_prefix, worker_group)
        .with_idle_sleep_ms(5)
        .with_handler_retry(2, 5)
        .register_handler(
            "RW_NODE",
            node(move |_ctx| {
                let attempts = attempts_clone.clone();
                async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        return Err(DagError::NodeExecutionFailed {
                            node_id: "RW_NODE".to_string(),
                            reason: format!("transient-{n}"),
                        });
                    }
                    Ok(i64_to_payload(9))
                }
            }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    let dispatcher = RedisRemoteDispatcher::new(
        cache,
        &key_prefix,
        RedisRemoteDispatcherOptions {
            response_timeout_ms: 5_000,
            poll_interval_ms: 5,
            result_ttl_secs: 60,
        },
    );

    let placement = NodePlacement::Remote {
        worker_group: worker_group.to_string(),
    };
    let context = NodeExecutionContext {
        run_id: "worker-retry-run-1".to_string(),
        run_fencing_token: None,
        node_id: "RW_NODE".to_string(),
        attempt: 0,
        upstream_nodes: vec![],
        upstream_outputs: Default::default(),
        layer_index: 1,
    };
    let local_executor = node(|_ctx| async move {
        Err(DagError::NodeExecutionFailed {
            node_id: "RW_NODE".to_string(),
            reason: "should not execute locally".to_string(),
        })
    });

    let out = dispatcher
        .dispatch("RW_NODE", &placement, local_executor, context)
        .await
        .unwrap();
    assert_eq!(payload_to_i64(&out), 9);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();
}

#[tokio::test]
async fn execute_parallel_real_redis_remote_worker_reclaims_stale_processing_jobs() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:reclaim:{}", Uuid::now_v7());
    let worker_group = "prod-wg-reclaim";
    let Some(cache) = local_redis_cache_service("dag-remote-reclaim") else {
        return;
    };

    let dispatcher = Arc::new(RedisRemoteDispatcher::new(
        cache.clone(),
        &key_prefix,
        RedisRemoteDispatcherOptions {
            response_timeout_ms: 8_000,
            poll_interval_ms: 5,
            result_ttl_secs: 60,
        },
    ));

    let placement = NodePlacement::Remote {
        worker_group: worker_group.to_string(),
    };
    let context = NodeExecutionContext {
        run_id: "reclaim-run-1".to_string(),
        run_fencing_token: None,
        node_id: "RC_NODE".to_string(),
        attempt: 0,
        upstream_nodes: vec![],
        upstream_outputs: Default::default(),
        layer_index: 1,
    };
    let local_executor = node(|_ctx| async move {
        Err(DagError::NodeExecutionFailed {
            node_id: "RC_NODE".to_string(),
            reason: "should not execute locally".to_string(),
        })
    });

    let dispatch_task = {
        let dispatcher = dispatcher.clone();
        let placement = placement.clone();
        let context = context.clone();
        let local_executor = local_executor.clone();
        tokio::spawn(async move {
            dispatcher
                .dispatch("RC_NODE", &placement, local_executor, context)
                .await
        })
    };

    let queue_key = format!("{}:remote:queue:{}", key_prefix, worker_group);
    let stale_worker_id = "stale-worker-1";
    let stale_processing_key = format!(
        "{}:remote:processing:{}:{}",
        key_prefix, worker_group, stale_worker_id
    );
    let workers_key = format!("{}:remote:workers:{}", key_prefix, worker_group);

    let mut moved = false;
    for _ in 0..40 {
        let maybe_job: Option<String> = redis::cmd("RPOPLPUSH")
            .arg(&queue_key)
            .arg(&stale_processing_key)
            .query_async(&mut conn)
            .await
            .ok()
            .flatten();
        if maybe_job.is_some() {
            moved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(moved, "expected to move one queued job into stale processing");

    let _ = redis::cmd("SADD")
        .arg(&workers_key)
        .arg(stale_worker_id)
        .query_async::<()>(&mut conn)
        .await;

    let stop = Arc::new(AtomicBool::new(false));
    let worker = RedisDagWorker::new(cache, &key_prefix, worker_group)
        .with_worker_id("live-worker-1")
        .with_reclaim_interval_ms(50)
        .with_heartbeat(500, 100)
        .with_idle_sleep_ms(5)
        .register_handler(
            "RC_NODE",
            node(|_ctx| async move { Ok(i64_to_payload(77)) }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    let out = dispatch_task.await.unwrap().unwrap();
    assert_eq!(payload_to_i64(&out), 77);

    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();
}

#[tokio::test]
async fn execute_parallel_real_redis_remote_worker_skips_canceled_request_before_execute() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:cancel-skip:{}", Uuid::now_v7());
    let worker_group = "prod-wg-cancel-skip";
    let Some(cache) = local_redis_cache_service("dag-remote-cancel-skip") else {
        return;
    };

    let dispatcher = RedisRemoteDispatcher::new(
        cache.clone(),
        &key_prefix,
        RedisRemoteDispatcherOptions {
            response_timeout_ms: 120,
            poll_interval_ms: 5,
            result_ttl_secs: 30,
        },
    );

    let execute_count = Arc::new(AtomicUsize::new(0));
    let placement = NodePlacement::Remote {
        worker_group: worker_group.to_string(),
    };
    let context = NodeExecutionContext {
        run_id: "cancel-skip-run-1".to_string(),
        run_fencing_token: None,
        node_id: "CANCEL_NODE".to_string(),
        attempt: 0,
        upstream_nodes: vec![],
        upstream_outputs: Default::default(),
        layer_index: 1,
    };
    let local_executor = node(|_ctx| async move {
        Err(DagError::NodeExecutionFailed {
            node_id: "CANCEL_NODE".to_string(),
            reason: "should not execute locally".to_string(),
        })
    });

    let dispatch_err = dispatcher
        .dispatch("CANCEL_NODE", &placement, local_executor, context)
        .await
        .unwrap_err();
    assert!(dispatch_err
        .to_string()
        .contains("remote dispatch response timeout"));

    let stop = Arc::new(AtomicBool::new(false));
    let execute_count_clone = execute_count.clone();
    let worker = RedisDagWorker::new(cache, &key_prefix, worker_group)
        .with_worker_id("live-worker-cancel-skip")
        .with_idle_sleep_ms(5)
        .register_handler(
            "CANCEL_NODE",
            node(move |_ctx| {
                let execute_count = execute_count_clone.clone();
                async move {
                    execute_count.fetch_add(1, Ordering::SeqCst);
                    Ok(i64_to_payload(99))
                }
            }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    tokio::time::sleep(Duration::from_millis(250)).await;
    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();

    assert_eq!(
        execute_count.load(Ordering::SeqCst),
        0,
        "canceled request should be skipped before handler execute"
    );
}

#[tokio::test]
async fn execute_parallel_real_redis_remote_worker_canceled_request_not_dlq_even_if_delivery_counter_high() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:cancel-no-dlq:{}", Uuid::now_v7());
    let worker_group = "prod-wg-cancel-no-dlq";
    let request_id = "cancel-no-dlq-req-1";
    let queue_key = format!("{}:remote:queue:{}", key_prefix, worker_group);
    let cancel_key = format!("{}:remote:cancel:{}", key_prefix, request_id);
    let delivery_count_key = format!("{}:remote:delivery:{}", key_prefix, request_id);
    let dlq_key = format!("{}:remote:dlq:{}", key_prefix, worker_group);

    let raw_job = serde_json::json!({
        "request_id": request_id,
        "node_id": "CANCEL_NO_DLQ_NODE",
        "worker_group": worker_group,
        "context": {
            "run_id": "cancel-no-dlq-run-1",
            "run_fencing_token": null,
            "node_id": "CANCEL_NO_DLQ_NODE",
            "attempt": 0,
            "upstream_nodes": [],
            "upstream_outputs_envelope": {},
            "layer_index": 1
        }
    })
    .to_string();

    let _: () = redis::cmd("LPUSH")
        .arg(&queue_key)
        .arg(&raw_job)
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&cancel_key)
        .arg("1")
        .arg("EX")
        .arg(30)
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&delivery_count_key)
        .arg(999)
        .arg("EX")
        .arg(30)
        .query_async(&mut conn)
        .await
        .unwrap();

    let Some(cache) = local_redis_cache_service("dag-remote-cancel-no-dlq") else {
        return;
    };

    let execute_count = Arc::new(AtomicUsize::new(0));
    let execute_count_clone = execute_count.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let worker = RedisDagWorker::new(cache, &key_prefix, worker_group)
        .with_worker_id("live-worker-cancel-no-dlq")
        .with_idle_sleep_ms(5)
        .with_max_delivery_attempts(1)
        .register_handler(
            "CANCEL_NO_DLQ_NODE",
            node(move |_ctx| {
                let execute_count = execute_count_clone.clone();
                async move {
                    execute_count.fetch_add(1, Ordering::SeqCst);
                    Ok(i64_to_payload(1))
                }
            }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    tokio::time::sleep(Duration::from_millis(250)).await;
    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();

    let dlq_len: i64 = redis::cmd("LLEN")
        .arg(&dlq_key)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);
    assert_eq!(dlq_len, 0, "canceled request must not go to DLQ");
    assert_eq!(
        execute_count.load(Ordering::SeqCst),
        0,
        "canceled request should not execute handler"
    );
}

#[tokio::test]
async fn execute_parallel_real_redis_remote_dispatch_multi_dag_concurrent() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    if ping_res.is_err() {
        return;
    }

    let key_prefix = format!("dag:remote:concurrent:{}", Uuid::now_v7());
    let worker_group = "prod-wg-b";
    let stop = Arc::new(AtomicBool::new(false));
    let Some(cache) = local_redis_cache_service("dag-remote-concurrent") else {
        return;
    };

    let worker = RedisDagWorker::new(cache.clone(), &key_prefix, worker_group)
        .with_worker_id("live-worker-1")
        .with_idle_sleep_ms(5)
        .register_handler(
            "MR_STEP",
            node(|ctx| async move {
                let v = ctx
                    .upstream_outputs
                    .get("MR_SRC")
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(v + 100))
            }),
        );
    let worker_task = tokio::spawn(worker.run_until_stopped(stop.clone()));

    let dispatcher = Arc::new(RedisRemoteDispatcher::new(
        cache,
        &key_prefix,
        RedisRemoteDispatcherOptions {
            response_timeout_ms: 8_000,
            poll_interval_ms: 5,
            result_ttl_secs: 120,
        },
    ));

    let mut tasks = Vec::new();
    for i in 0..5_i64 {
        let dispatcher = dispatcher.clone();
        let worker_group = worker_group.to_string();
        tasks.push(tokio::spawn(async move {
            let mut dag = Dag::new();
            let src = dag
                .add_node_with_id(
                    None,
                    "MR_SRC",
                    node(move |_ctx| async move { Ok(i64_to_payload(i)) }),
                )
                .unwrap();
            let step = dag
                .add_node_with_id(
                    Some(&[src]),
                    "MR_STEP",
                    node(|_ctx| async move { Err(DagError::NodeExecutionFailed { node_id: "MR_STEP".into(), reason: "should not execute locally".into() }) }),
                )
                .unwrap();
            dag.set_node_placement(
                &step,
                NodePlacement::Remote {
                    worker_group,
                },
            )
            .unwrap();

            let scheduler = DagScheduler::new(dag)
                .with_dispatcher(dispatcher)
                .with_options(DagSchedulerOptions {
                    max_in_flight: 4,
                    cancel_inflight_on_failure: true,
                    run_timeout_ms: Some(15_000),
                });
            let report = scheduler.execute_parallel().await.unwrap();
            payload_to_i64(report.outputs.get("MR_STEP").expect("MR_STEP missing"))
        }));
    }

    let vals = futures::future::join_all(tasks).await;
    for (i, v) in vals.into_iter().enumerate() {
        assert_eq!(v.unwrap(), i as i64 + 100);
    }

    stop.store(true, Ordering::SeqCst);
    worker_task.await.unwrap();
}

#[tokio::test]
async fn execute_parallel_redis_run_guard_blocks_duplicate_then_allows_reentry() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut ping_conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut ping_conn).await;
    if ping_res.is_err() {
        return;
    }

    fn build_two_node_dag() -> Dag {
        let mut dag = Dag::new();
        let a = dag
            .add_node_with_id(
                None,
                "RRG_A",
                node(|_ctx| async move {
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    Ok(i64_to_payload(3))
                }),
            )
            .unwrap();
        dag.add_node_with_id(
            Some(&[a]),
            "RRG_B",
            node(|ctx| async move {
                let v = ctx
                    .upstream_outputs
                    .values()
                    .next()
                    .map(payload_to_i64)
                    .unwrap_or(0);
                Ok(i64_to_payload(v + 2))
            }),
        )
        .unwrap();
        dag
    }

    let lock_key = format!("dag:run-lock:redis:{}", Uuid::now_v7());
    let guard = Arc::new(RedisRunGuard {
        client: client.clone(),
    });

    let scheduler_1 = DagScheduler::new(build_two_node_dag()).with_run_guard(
        guard.clone(),
        &lock_key,
        5_000,
    );
    let scheduler_2 = DagScheduler::new(build_two_node_dag()).with_run_guard(
        guard.clone(),
        &lock_key,
        5_000,
    );

    let first = tokio::spawn(async move { scheduler_1.execute_parallel().await });
    tokio::time::sleep(Duration::from_millis(15)).await;
    let second = scheduler_2.execute_parallel().await;
    assert!(matches!(
        second,
        Err(DagError::RunAlreadyInProgress { .. })
    ));

    let first_report = first
        .await
        .expect("first redis run join should succeed")
        .expect("first redis run should succeed");
    assert_eq!(
        payload_to_i64(first_report.outputs.get("RRG_B").expect("RRG_B missing")),
        5
    );

    let scheduler_3 = DagScheduler::new(build_two_node_dag()).with_run_guard(
        guard,
        &lock_key,
        5_000,
    );
    let third_report = scheduler_3
        .execute_parallel()
        .await
        .expect("third run should re-enter after release");
    assert_eq!(
        payload_to_i64(third_report.outputs.get("RRG_B").expect("RRG_B missing")),
        5
    );
}

#[tokio::test]
async fn execute_parallel_redis_run_guard_heartbeat_prevents_ttl_expiry() {
    let client = match redis::Client::open("redis://127.0.0.1:6379/") {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut ping_conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(_) => return,
    };
    let ping_res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut ping_conn).await;
    if ping_res.is_err() {
        return;
    }

    let mut dag = Dag::new();
    dag.add_node_with_id(
        None,
        "RRGH_A",
        node(|_ctx| async move {
            tokio::time::sleep(Duration::from_millis(350)).await;
            Ok(i64_to_payload(9))
        }),
    )
    .unwrap();

    let lock_key = format!("dag:run-lock:redis:heartbeat:{}", Uuid::now_v7());
    let guard = Arc::new(RedisRunGuard {
        client: client.clone(),
    });
    let scheduler_1 = DagScheduler::new(dag)
        .with_run_guard(guard.clone(), &lock_key, 120)
        .with_run_guard_heartbeat_ms(Some(40));

    let first = tokio::spawn(async move { scheduler_1.execute_parallel().await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut dag2 = Dag::new();
    dag2.add_node_with_id(
        None,
        "RRGH_B",
        node(|_ctx| async move { Ok(i64_to_payload(1)) }),
    )
    .unwrap();

    let scheduler_2 = DagScheduler::new(dag2)
        .with_run_guard(guard, &lock_key, 120)
        .with_run_guard_heartbeat_ms(Some(40));
    let second = scheduler_2.execute_parallel().await;
    assert!(matches!(
        second,
        Err(DagError::RunAlreadyInProgress { .. })
    ));

    let first_report = first
        .await
        .expect("heartbeat run join should succeed")
        .expect("heartbeat run should succeed");
    assert_eq!(
        payload_to_i64(first_report.outputs.get("RRGH_A").expect("RRGH_A missing")),
        9
    );
}
