use crate::engine::api::state::ApiState;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

/// Parameters for inspecting the Dead Letter Queue
#[derive(Deserialize)]
pub struct DlqParams {
    /// The topic to inspect (default: "task")
    pub topic: Option<String>,
    /// Number of messages to retrieve (default: 10)
    pub count: Option<usize>,
}

#[derive(Deserialize)]
pub struct DlqActionParams {
    pub topic: Option<String>,
}

/// Response model for DLQ messages
#[derive(Serialize)]
pub struct DlqMessageResponse {
    /// Backend record ID used for delete/requeue operations
    pub id: String,
    /// Logical DLQ envelope ID
    pub message_id: String,
    /// Original queue topic before entering DLQ
    pub source_topic: String,
    /// Original message ID before failure
    pub source_message_id: String,
    /// Reason for failure
    pub reason: String,
    /// Retry attempt observed when the message entered DLQ
    pub attempt: u32,
    /// Serialized payload schema information
    pub schema_id: String,
    pub schema_version: u16,
    pub codec: String,
    /// Structured payload bytes encoded as base64 for transport-safe inspection
    pub payload_base64: String,
    pub payload_len: usize,
}

/// Handler for `GET /dlq/messages`.
///
/// Retrieves messages from the Dead Letter Queue for inspection.
pub async fn get_dlq_messages(
    State(state): State<ApiState>,
    Query(params): Query<DlqParams>,
) -> Json<Vec<DlqMessageResponse>> {
    let topic = params.topic.unwrap_or_else(|| "task".to_string());
    let count = params.count.unwrap_or(10);

    match state.queue_manager.read_dlq_records(&topic, count).await {
        Ok(messages) => {
            let response: Vec<DlqMessageResponse> = messages
                .into_iter()
                .map(|message| {
                    let payload = message.envelope.payload;
                    DlqMessageResponse {
                        id: message.record_id,
                        message_id: message.envelope.id.to_string(),
                        source_topic: message.envelope.source_topic,
                        source_message_id: message.envelope.source_message_id,
                        reason: message.envelope.reason,
                        attempt: message.envelope.attempt,
                        schema_id: payload.schema_id,
                        schema_version: payload.schema_version,
                        codec: format!("{:?}", payload.codec).to_lowercase(),
                        payload_base64: STANDARD.encode(&payload.payload),
                        payload_len: payload.payload.len(),
                    }
                })
                .collect();
            Json(response)
        }
        Err(e) => {
            log::error!("Failed to read DLQ: {}", e);
            Json(vec![])
        }
    }
}

pub async fn requeue_dlq_message(
    State(state): State<ApiState>,
    Path(record_id): Path<String>,
    Query(params): Query<DlqActionParams>,
) -> StatusCode {
    let topic = params.topic.unwrap_or_else(|| "task".to_string());

    match state
        .queue_manager
        .requeue_dlq_message(&topic, &record_id)
        .await
    {
        Ok(true) => StatusCode::ACCEPTED,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            log::error!("Failed to requeue DLQ message {}: {}", record_id, e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

pub async fn delete_dlq_message(
    State(state): State<ApiState>,
    Path(record_id): Path<String>,
    Query(params): Query<DlqActionParams>,
) -> StatusCode {
    let topic = params.topic.unwrap_or_else(|| "task".to_string());

    match state
        .queue_manager
        .delete_dlq_message(&topic, &record_id)
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            log::error!("Failed to delete DLQ message {}: {}", record_id, e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DlqActionParams, DlqParams, delete_dlq_message, get_dlq_messages, requeue_dlq_message,
    };
    use crate::cacheable::CacheService;
    use crate::common::model::message::TaskEvent;
    use crate::common::model::config::{
        BlobStorageConfig, CacheConfig, ChannelConfig, Config, CrawlerConfig, DatabaseConfig,
        DownloadConfig,
    };
    use crate::common::model::{
        DeadLetterEnvelope, ExecutionMark, PayloadCodec, Priority, Request, Response,
    };
    use crate::common::state::State as AppState;
    use crate::engine::api::profile_store::ProfileControlPlaneStore;
    use crate::engine::api::state::ApiState;
    use crate::engine::task::parser_error_adapter::{
        build_error_envelope_from_seed, build_parser_dispatch_from_seed, ErrorEnvelopeSeed,
        ParserDispatchSeed,
    };
    use crate::engine::task::request_response_adapter::{
        build_request_dispatch, build_response_dispatch,
    };
    use crate::engine::task::task_dispatch_adapter::build_task_dispatch;
    use crate::engine::task::task_manager::TaskManager;
    use crate::queue::codec::queue_codec;
    use crate::queue::{DlqRecord, Identifiable, Message, MqBackend, QueueManager};
    use async_trait::async_trait;
    use axum::Json;
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct DlqApiBackend {
        records: Arc<Mutex<Vec<DlqRecord>>>,
        requeue_calls: Arc<Mutex<Vec<(String, Option<String>, Vec<u8>)>>>,
        deleted: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[async_trait]
    impl MqBackend for DlqApiBackend {
        async fn publish(
            &self,
            _topic: &str,
            _key: Option<&str>,
            _payload: &[u8],
        ) -> crate::errors::Result<()> {
            Ok(())
        }

        async fn publish_with_headers(
            &self,
            topic: &str,
            key: Option<&str>,
            payload: &[u8],
            _headers: &HashMap<String, String>,
        ) -> crate::errors::Result<()> {
            self.requeue_calls.lock().unwrap().push((
                topic.to_string(),
                key.map(ToString::to_string),
                payload.to_vec(),
            ));
            Ok(())
        }

        async fn publish_batch_with_headers(
            &self,
            _topic: &str,
            _items: &[(Option<String>, Vec<u8>, HashMap<String, String>)],
        ) -> crate::errors::Result<()> {
            Ok(())
        }

        async fn subscribe(
            &self,
            _topic: &str,
            _sender: mpsc::Sender<Message>,
        ) -> crate::errors::Result<()> {
            Ok(())
        }

        async fn clean_storage(&self) -> crate::errors::Result<()> {
            Ok(())
        }

        async fn send_to_dlq(
            &self,
            _topic: &str,
            id: &str,
            payload: &[u8],
            reason: &str,
        ) -> crate::errors::Result<()> {
            let mut records = self.records.lock().unwrap();
            let record_id = format!("dlq-{}", records.len() + 1);
            records.push(DlqRecord {
                id: record_id,
                payload: payload.to_vec(),
                reason: reason.to_string(),
                original_id: id.to_string(),
                attempt: Some(0),
            });
            Ok(())
        }

        async fn read_dlq(
            &self,
            _topic: &str,
            _count: usize,
        ) -> crate::errors::Result<Vec<DlqRecord>> {
            Ok(self.records.lock().unwrap().clone())
        }

        async fn read_dlq_record(
            &self,
            _topic: &str,
            record_id: &str,
        ) -> crate::errors::Result<Option<DlqRecord>> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .iter()
                .find(|record| record.id == record_id)
                .cloned())
        }

        async fn delete_dlq(
            &self,
            topic: &str,
            record_id: &str,
        ) -> crate::errors::Result<bool> {
            self.deleted
                .lock()
                .unwrap()
                .push((topic.to_string(), record_id.to_string()));
            let mut records = self.records.lock().unwrap();
            let initial_len = records.len();
            records.retain(|record| record.id != record_id);
            Ok(records.len() != initial_len)
        }
    }

    fn sample_config() -> Config {
        Config {
            name: "demo".to_string(),
            db: DatabaseConfig {
                url: None,
                database_schema: None,
                pool_size: None,
                tls: None,
            },
            download_config: DownloadConfig {
                downloader_expire: 60,
                timeout: 30,
                rate_limit: 5.0,
                enable_session: true,
                enable_locker: false,
                enable_rate_limit: true,
                cache_ttl: 60,
                wss_timeout: 30,
                pool_size: None,
                max_response_size: None,
            },
            cache: CacheConfig {
                backend: None,
                ttl: 60,
                redis: None,
                compression_threshold: None,
                enable_l1: Some(false),
                l1_ttl_secs: None,
                l1_max_entries: None,
            },
            crawler: CrawlerConfig {
                request_max_retries: 3,
                task_max_errors: 5,
                module_max_errors: 5,
                module_locker_ttl: 60,
                node_id: None,
                task_concurrency: None,
                publish_concurrency: None,
                parser_concurrency: None,
                error_task_concurrency: None,
                backpressure_retry_delay_ms: None,
                dedup_ttl_secs: None,
                idle_stop_secs: None,
                scheduler_ingress_cutover_gate: None,
            },
            scheduler: None,
            sync: None,
            cookie: None,
            channel_config: ChannelConfig {
                blob_storage: Some(BlobStorageConfig { path: None }),
                redis: None,
                kafka: None,
                compensator: None,
                minid_time: 0,
                capacity: 16,
                queue_codec: None,
                batch_concurrency: None,
                compression_threshold: None,
                nack_max_retries: None,
                nack_backoff_ms: None,
                federation_request_namespaces: Vec::new(),
                federation_response_cache_api_endpoints: Default::default(),
            },
            proxy: None,
            api: None,
            event_bus: None,
            logger: None,
            policy: None,
            raft: None,
        }
    }

    async fn build_api_state_with_queue_manager(queue_manager: Arc<QueueManager>) -> ApiState {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite should connect");
        let profile_store = Arc::new(ProfileControlPlaneStore::open_temp("dlq-api").unwrap());
        let state = Arc::new(AppState {
            db: Arc::new(db),
            config: Arc::new(RwLock::new(sample_config())),
            cache_service: Arc::new(CacheService::new(
                None,
                "demo:cache".to_string(),
                Some(Duration::from_secs(60)),
                None,
            )),
            cookie_service: None,
            locker: Arc::new(crate::utils::redis_lock::DistributedLockManager::new(
                None,
                "demo",
            )),
            limiter: Arc::new(
                crate::utils::distributed_rate_limit::DistributedSlidingWindowRateLimiter::new(
                    None,
                    Arc::new(crate::utils::redis_lock::DistributedLockManager::new(
                        None,
                        "demo",
                    )),
                    "demo",
                    crate::utils::distributed_rate_limit::RateLimitConfig {
                        max_requests_per_second: 5.0,
                        window_size_millis: 1000,
                        base_max_requests_per_second: Some(5.0),
                    },
                ),
            ),
            api_limiter: None,
            status_tracker: Arc::new(crate::common::status_tracker::StatusTracker::new(
                crate::common::status_tracker::ErrorTrackerConfig {
                    task_max_errors: 5,
                    module_max_errors: 5,
                    request_max_retries: 3,
                    parse_max_retries: 3,
                    enable_success_decay: true,
                    success_decay_amount: 1,
                    enable_time_window: false,
                    time_window_seconds: 3600,
                    consecutive_error_threshold: 3,
                    error_ttl: 60,
                },
                profile_store.clone(),
            )),
            profile_store,
            raft_runtime_config: None,
            raft_runtime: None,
            redis: None,
        });
        let task_manager = Arc::new(TaskManager::new(state.clone()));
        ApiState::new(queue_manager, None, state, task_manager)
    }

    async fn build_api_state(backend: Arc<DlqApiBackend>) -> ApiState {
        build_api_state_with_queue_manager(Arc::new(QueueManager::new(Some(backend), 16))).await
    }

    fn make_response() -> Response {
        Response {
            id: Uuid::new_v4(),
            platform: "platform-a".to_string(),
            account: "account-a".to_string(),
            module: "catalog".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: b"ok".to_vec(),
            storage_path: None,
            headers: Vec::new(),
            task_retry_times: 0,
            metadata: Default::default(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: ExecutionMark::default(),
            run_id: Uuid::new_v4(),
            prefix_request: Uuid::nil(),
            request_hash: None,
            priority: Priority::Normal,
        }
    }

    fn make_task_event(module: &str, run_id: Uuid) -> TaskEvent {
        TaskEvent {
            account: "account-a".to_string(),
            platform: "platform-a".to_string(),
            module: Some(vec![module.to_string()]),
            priority: Priority::Normal,
            run_id,
        }
    }

    fn make_parser_dispatch(module: &str) -> crate::common::model::NodeDispatchEnvelope {
        let run_id = Uuid::new_v4();
        build_parser_dispatch_from_seed(
            &ParserDispatchSeed {
                request_id: Uuid::new_v4(),
                task_model: make_task_event(module, run_id),
                timestamp: 1,
                metadata: Default::default(),
                context: ExecutionMark::default().with_node_id("step_0"),
                run_id,
                prefix_request: Uuid::nil(),
                dispatch: None,
            },
            "demo",
        )
        .expect("parser dispatch should build")
    }

    fn make_error_envelope(module: &str) -> crate::common::model::NodeErrorEnvelope {
        let run_id = Uuid::new_v4();
        build_error_envelope_from_seed(
            &ErrorEnvelopeSeed {
                request_id: Uuid::new_v4(),
                task_model: make_task_event(module, run_id),
                error_message: "retry me".to_string(),
                timestamp: 1,
                metadata: Default::default(),
                context: ExecutionMark::default().with_node_id("step_0"),
                run_id,
                prefix_request: Uuid::nil(),
            },
            "demo",
        )
        .expect("error envelope should build")
    }

    #[tokio::test]
    async fn get_dlq_messages_exposes_request_dispatch_contract_metadata() {
        let backend = Arc::new(DlqApiBackend::default());
        let api_state = build_api_state(backend.clone()).await;

        let request = Request::new("http://example.com", "GET");
        let dispatch = build_request_dispatch(&request, "origin").expect("request dispatch should build");
        let payload = queue_codec()
            .encode(&dispatch)
            .expect("dispatch payload should encode");
        let envelope = DeadLetterEnvelope::new(
            "demo".to_string(),
            crate::common::model::QueueTopicKind::Request,
            "request-normal".to_string(),
            dispatch.get_id(),
            "poison".to_string(),
            0,
            1,
            crate::common::model::QueueEnvelope::new(
                "queue.request",
                1,
                PayloadCodec::MsgPack,
                payload,
            )
            .expect("queue envelope should build"),
        );
        backend.records.lock().unwrap().push(DlqRecord {
            id: "dlq-req-1".to_string(),
            payload: queue_codec()
                .encode(&envelope)
                .expect("dead letter envelope should encode"),
            reason: "poison".to_string(),
            original_id: dispatch.get_id(),
            attempt: Some(0),
        });

        let Json(messages) = get_dlq_messages(
            State(api_state),
            Query(DlqParams {
                topic: Some("request-normal".to_string()),
                count: Some(10),
            }),
        )
        .await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "dlq-req-1");
        assert_eq!(messages[0].schema_id, "queue.request");
        assert_eq!(messages[0].schema_version, 1);
        assert_eq!(messages[0].codec, "msgpack");
        assert!(messages[0].payload_len > 0);
    }

    #[tokio::test]
    async fn requeue_dlq_message_replays_response_dispatch_payload_by_record_id() {
        let backend = Arc::new(DlqApiBackend::default());
        let api_state = build_api_state(backend.clone()).await;

        let response = make_response();
        let dispatch = build_response_dispatch(&response, "download-pool")
            .expect("response dispatch should build");
        let payload = queue_codec()
            .encode(&dispatch)
            .expect("dispatch payload should encode");
        let envelope = DeadLetterEnvelope::new(
            "demo".to_string(),
            crate::common::model::QueueTopicKind::Response,
            "origin::response-normal".to_string(),
            dispatch.get_id(),
            "retry me".to_string(),
            0,
            1,
            crate::common::model::QueueEnvelope::new(
                "queue.response",
                1,
                PayloadCodec::MsgPack,
                payload,
            )
            .expect("queue envelope should build"),
        );
        backend.records.lock().unwrap().push(DlqRecord {
            id: "dlq-res-1".to_string(),
            payload: queue_codec()
                .encode(&envelope)
                .expect("dead letter envelope should encode"),
            reason: "retry me".to_string(),
            original_id: dispatch.get_id(),
            attempt: Some(0),
        });

        let status = requeue_dlq_message(
            State(api_state),
            Path("dlq-res-1".to_string()),
            Query(DlqActionParams {
                topic: Some("response-normal".to_string()),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::ACCEPTED);

        let replays = backend.requeue_calls.lock().unwrap();
        assert_eq!(replays.len(), 1);
        assert_eq!(replays[0].0, "origin::response-normal");
        assert_eq!(replays[0].1.as_deref(), Some(dispatch.get_id().as_str()));

        let replayed_dispatch = queue_codec()
            .decode::<crate::common::model::ResponseDispatchEnvelope>(&replays[0].2)
            .expect("replayed payload should remain a response dispatch envelope");
        assert_eq!(replayed_dispatch.routing.request_id, response.id);
    }

    #[tokio::test]
    async fn queue_manager_stored_request_dlq_record_can_be_replayed_via_api() {
        let backend = Arc::new(DlqApiBackend::default());
        let mut queue_manager = QueueManager::new(Some(backend.clone()), 16);
        queue_manager.namespace = "demo".to_string();
        let queue_manager = Arc::new(queue_manager);
        let api_state = build_api_state_with_queue_manager(queue_manager.clone()).await;

        let request = Request::new("http://example.com", "GET");
        let dispatch =
            build_request_dispatch(&request, "origin").expect("request dispatch should build");

        queue_manager
            .send_to_dlq("request-normal", &dispatch, "poison")
            .await
            .expect("queue manager should store request dlq record");

        let Json(messages) = get_dlq_messages(
            State(api_state.clone()),
            Query(DlqParams {
                topic: Some("request-normal".to_string()),
                count: Some(10),
            }),
        )
        .await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "dlq-1");
        assert_eq!(messages[0].schema_id, "queue.request");

        let status = requeue_dlq_message(
            State(api_state),
            Path("dlq-1".to_string()),
            Query(DlqActionParams {
                topic: Some("request-normal".to_string()),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::ACCEPTED);

        let replays = backend.requeue_calls.lock().unwrap();
        assert_eq!(replays.len(), 1);
        assert_eq!(replays[0].0, "request-normal");
        assert_eq!(replays[0].1.as_deref(), Some(dispatch.get_id().as_str()));

        let replayed_dispatch = queue_codec()
            .decode::<crate::common::model::RequestDispatchEnvelope>(&replays[0].2)
            .expect("replayed payload should remain a request dispatch envelope");
        assert_eq!(replayed_dispatch.routing.request_id, request.id);
    }

    #[tokio::test]
    async fn queue_manager_stored_response_dlq_record_can_be_replayed_via_api() {
        let backend = Arc::new(DlqApiBackend::default());
        let mut queue_manager = QueueManager::new(Some(backend.clone()), 16);
        queue_manager.namespace = "demo".to_string();
        let queue_manager = Arc::new(queue_manager);
        let api_state = build_api_state_with_queue_manager(queue_manager.clone()).await;

        let response = make_response();
        let dispatch = build_response_dispatch(&response, "download-pool")
            .expect("response dispatch should build");

        queue_manager
            .send_to_dlq("origin::response-normal", &dispatch, "poison")
            .await
            .expect("queue manager should store response dlq record");

        let Json(messages) = get_dlq_messages(
            State(api_state.clone()),
            Query(DlqParams {
                topic: Some("origin::response-normal".to_string()),
                count: Some(10),
            }),
        )
        .await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "dlq-1");
        assert_eq!(messages[0].schema_id, "queue.response");

        let status = requeue_dlq_message(
            State(api_state),
            Path("dlq-1".to_string()),
            Query(DlqActionParams {
                topic: Some("response-normal".to_string()),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::ACCEPTED);

        let replays = backend.requeue_calls.lock().unwrap();
        assert_eq!(replays.len(), 1);
        assert_eq!(replays[0].0, "origin::response-normal");
        assert_eq!(replays[0].1.as_deref(), Some(dispatch.get_id().as_str()));

        let replayed_dispatch = queue_codec()
            .decode::<crate::common::model::ResponseDispatchEnvelope>(&replays[0].2)
            .expect("replayed payload should remain a response dispatch envelope");
        assert_eq!(replayed_dispatch.routing.request_id, response.id);
    }

    #[tokio::test]
    async fn queue_manager_stored_parser_dispatch_dlq_record_can_be_replayed_via_api() {
        let backend = Arc::new(DlqApiBackend::default());
        let mut queue_manager = QueueManager::new(Some(backend.clone()), 16);
        queue_manager.namespace = "demo".to_string();
        let queue_manager = Arc::new(queue_manager);
        let api_state = build_api_state_with_queue_manager(queue_manager.clone()).await;

        let dispatch = make_parser_dispatch("catalog");

        queue_manager
            .send_to_dlq("parser_task-normal", &dispatch, "poison")
            .await
            .expect("queue manager should store parser dispatch dlq record");

        let Json(messages) = get_dlq_messages(
            State(api_state.clone()),
            Query(DlqParams {
                topic: Some("parser_task-normal".to_string()),
                count: Some(10),
            }),
        )
        .await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "dlq-1");
        assert_eq!(messages[0].schema_id, "queue.parser_task");

        let status = requeue_dlq_message(
            State(api_state),
            Path("dlq-1".to_string()),
            Query(DlqActionParams {
                topic: Some("parser_task-normal".to_string()),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::ACCEPTED);

        let replays = backend.requeue_calls.lock().unwrap();
        assert_eq!(replays.len(), 1);
        assert_eq!(replays[0].0, "parser_task-normal");
        assert_eq!(replays[0].1.as_deref(), Some(dispatch.get_id().as_str()));

        let replayed_dispatch = queue_codec()
            .decode::<crate::common::model::NodeDispatchEnvelope>(&replays[0].2)
            .expect("replayed payload should remain a parser dispatch envelope");
        assert_eq!(replayed_dispatch.routing.request_id, dispatch.routing.request_id);
        assert_eq!(replayed_dispatch.routing.module, "catalog");
    }

    #[tokio::test]
    async fn queue_manager_stored_error_envelope_dlq_record_can_be_replayed_via_api() {
        let backend = Arc::new(DlqApiBackend::default());
        let mut queue_manager = QueueManager::new(Some(backend.clone()), 16);
        queue_manager.namespace = "demo".to_string();
        let queue_manager = Arc::new(queue_manager);
        let api_state = build_api_state_with_queue_manager(queue_manager.clone()).await;

        let envelope = make_error_envelope("catalog");

        queue_manager
            .send_to_dlq("error_task-normal", &envelope, "poison")
            .await
            .expect("queue manager should store error envelope dlq record");

        let Json(messages) = get_dlq_messages(
            State(api_state.clone()),
            Query(DlqParams {
                topic: Some("error_task-normal".to_string()),
                count: Some(10),
            }),
        )
        .await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "dlq-1");
        assert_eq!(messages[0].schema_id, "queue.error_task");

        let status = requeue_dlq_message(
            State(api_state),
            Path("dlq-1".to_string()),
            Query(DlqActionParams {
                topic: Some("error_task-normal".to_string()),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::ACCEPTED);

        let replays = backend.requeue_calls.lock().unwrap();
        assert_eq!(replays.len(), 1);
        assert_eq!(replays[0].0, "error_task-normal");
        assert_eq!(replays[0].1.as_deref(), Some(envelope.get_id().as_str()));

        let replayed_envelope = queue_codec()
            .decode::<crate::common::model::NodeErrorEnvelope>(&replays[0].2)
            .expect("replayed payload should remain an error dispatch envelope");
        assert_eq!(replayed_envelope.routing.request_id, envelope.routing.request_id);
        assert_eq!(replayed_envelope.routing.module, "catalog");
        assert_eq!(replayed_envelope.error_message, "retry me");
    }

    #[tokio::test]
    async fn queue_manager_stored_task_dispatch_dlq_record_can_be_replayed_via_api() {
        let backend = Arc::new(DlqApiBackend::default());
        let mut queue_manager = QueueManager::new(Some(backend.clone()), 16);
        queue_manager.namespace = "demo".to_string();
        let queue_manager = Arc::new(queue_manager);
        let api_state = build_api_state_with_queue_manager(queue_manager.clone()).await;

        let run_id = Uuid::new_v4();
        let task = make_task_event("catalog", run_id);
        let envelope = build_task_dispatch(&task, "demo").expect("task dispatch should build");

        queue_manager
            .send_to_dlq("task-normal", &envelope, "transient")
            .await
            .expect("queue manager should store task dispatch dlq record");

        let Json(messages) = get_dlq_messages(
            State(api_state.clone()),
            Query(DlqParams {
                topic: Some("task-normal".to_string()),
                count: Some(10),
            }),
        )
        .await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, "dlq-1");
        assert_eq!(messages[0].schema_id, "queue.task");

        let status = requeue_dlq_message(
            State(api_state),
            Path("dlq-1".to_string()),
            Query(DlqActionParams {
                topic: Some("task-normal".to_string()),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::ACCEPTED);

        let replays = backend.requeue_calls.lock().unwrap();
        assert_eq!(replays.len(), 1);
        assert_eq!(replays[0].0, "task-normal");
        assert_eq!(replays[0].1.as_deref(), Some(envelope.get_id().as_str()));

        let replayed_envelope = queue_codec()
            .decode::<crate::common::model::TaskDispatchEnvelope>(&replays[0].2)
            .expect("replayed payload should remain a task dispatch envelope");
        assert_eq!(replayed_envelope.routing.request_id, envelope.routing.request_id);
        assert_eq!(replayed_envelope.routing.module, "catalog");
    }

    #[tokio::test]
    async fn queue_manager_stored_parser_dispatch_dlq_record_can_be_deleted_via_api() {
        let backend = Arc::new(DlqApiBackend::default());
        let mut queue_manager = QueueManager::new(Some(backend.clone()), 16);
        queue_manager.namespace = "demo".to_string();
        let queue_manager = Arc::new(queue_manager);
        let api_state = build_api_state_with_queue_manager(queue_manager.clone()).await;

        let dispatch = make_parser_dispatch("catalog");

        queue_manager
            .send_to_dlq("parser_task-normal", &dispatch, "poison")
            .await
            .expect("queue manager should store parser dispatch dlq record");

        let status = delete_dlq_message(
            State(api_state.clone()),
            Path("dlq-1".to_string()),
            Query(DlqActionParams {
                topic: Some("parser_task-normal".to_string()),
            }),
        )
        .await;

        assert_eq!(status, StatusCode::NO_CONTENT);

        let deleted = backend.deleted.lock().unwrap();
        assert_eq!(deleted.len(), 1);
        assert_eq!(deleted[0].0, "parser_task-normal");
        assert_eq!(deleted[0].1, "dlq-1");

        let Json(messages) = get_dlq_messages(
            State(api_state),
            Query(DlqParams {
                topic: Some("parser_task-normal".to_string()),
                count: Some(10),
            }),
        )
        .await;

        assert!(messages.is_empty());
    }
}
