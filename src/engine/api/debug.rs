use std::collections::BTreeMap;
use std::time::Instant;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::cacheable::CacheAble;
use crate::common::model::control_plane_profile::DefaultConfigScope;
use crate::common::model::{MiddlewareType, PipelineStage, Response, TaskProfileSnapshot, TaskStatus, TypedEnvelope};
use crate::engine::api::profile_store::{ProfileControlPlaneStore, StoredStatusEntryRecord};
use crate::engine::api::state::ApiState;

#[derive(Debug, Serialize)]
pub struct DebugConfigResponse {
    pub profile_key: String,
    pub merged: Value,
    pub layers: Value,
    pub version: u64,
}

#[derive(Debug, Serialize)]
pub struct DebugProfileResponse {
    pub profile_key: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub download_middleware: Vec<String>,
    pub data_middleware: Vec<String>,
    pub config: Value,
    pub enabled: bool,
    pub version: u64,
    pub raft_index: Option<u64>,
    pub updated_by: String,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct DebugStatusStageParams {
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DebugStatusResponse {
    pub task_id: String,
    pub stage: PipelineStage,
    pub status: TaskStatus,
    pub retry_count: u32,
    pub node_id: String,
    pub updated_at: i64,
    pub error_msg: Option<String>,
    pub version: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DebugStatusCountsResponse {
    pub counts: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DebugCachedResponse {
    pub cache_key: String,
    pub response: Response,
}

pub async fn get_debug_config(
    State(state): State<ApiState>,
    Path((account, platform, module)): Path<(String, String, String)>,
) -> Result<Json<DebugConfigResponse>, StatusCode> {
    let started = Instant::now();
    let snapshot = load_profile(&state, &account, &platform, &module).await?;
    let namespace = state.state.config.read().await.name.clone();
    let response = Json(DebugConfigResponse {
        profile_key: snapshot.profile_key(),
        merged: merged_debug_config(&snapshot, &state.state.profile_store, &namespace),
        layers: build_debug_layers(&snapshot, &state.state.profile_store, &namespace),
        version: snapshot.version,
    });
    record_debug_metrics("get_debug_config", "success", started);
    Ok(response)
}

pub async fn get_debug_profile(
    State(state): State<ApiState>,
    Path((account, platform, module)): Path<(String, String, String)>,
) -> Result<Json<DebugProfileResponse>, StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let record = state
        .profile_store
        .get_profile_record(&namespace, &account, &platform, &module)
        .ok_or(StatusCode::NOT_FOUND)?;
    let snapshot = record.snapshot;
    let response = Json(DebugProfileResponse {
        profile_key: snapshot.profile_key(),
        account: snapshot.account,
        platform: snapshot.platform,
        module: snapshot.module,
        download_middleware: snapshot
            .download_middleware
            .iter()
            .map(|binding| binding.name.clone())
            .collect(),
        data_middleware: snapshot
            .data_middleware
            .iter()
            .map(|binding| binding.name.clone())
            .collect(),
        config: serde_json::to_value(&snapshot.common).unwrap_or(Value::Null),
        enabled: snapshot.enabled,
        version: snapshot.version,
        raft_index: Some(record.raft_index),
        updated_by: snapshot.updated_by,
        updated_at: snapshot.updated_at,
    });
    record_debug_metrics("get_debug_profile", "success", started);
    Ok(response)
}

pub async fn get_debug_status(
    State(state): State<ApiState>,
    Path(task_id): Path<String>,
) -> Result<Json<DebugStatusResponse>, StatusCode> {
    let started = Instant::now();
    let result = state
        .state
        .profile_store
        .get_status_entry(&task_id)
        .map(status_record_response)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND);
    record_debug_status_metrics(
        "get_debug_status",
        result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or(StatusCode::NOT_FOUND),
        started,
    );
    result
}

pub async fn list_debug_status_by_stage(
    State(state): State<ApiState>,
    Path(stage): Path<String>,
    Query(params): Query<DebugStatusStageParams>,
) -> Result<Json<Vec<DebugStatusResponse>>, StatusCode> {
    let started = Instant::now();
    let Some(stage) = parse_pipeline_stage(&stage) else {
        record_debug_status_metrics("list_debug_status_by_stage", StatusCode::BAD_REQUEST, started);
        return Err(StatusCode::BAD_REQUEST);
    };

    let limit = params.limit.unwrap_or(50).clamp(1, 500);
    let response = Json(
        state
            .state
            .profile_store
            .list_status_entries_by_stage(stage, limit)
            .into_iter()
            .map(status_record_response)
            .collect(),
    );
    record_debug_metrics("list_debug_status_by_stage", "success", started);
    Ok(response)
}

pub async fn get_debug_status_counts(
    State(state): State<ApiState>,
) -> Json<DebugStatusCountsResponse> {
    let started = Instant::now();
    let response = Json(DebugStatusCountsResponse {
        counts: status_counts_response(state.state.profile_store.count_status_entries_by_status()),
    });
    record_debug_metrics("get_debug_status_counts", "success", started);
    response
}

pub async fn get_debug_cached_response(
    State(state): State<ApiState>,
    Path(cache_key): Path<String>,
) -> Result<Json<DebugCachedResponse>, StatusCode> {
    let started = Instant::now();
    let cached = match load_cached_response(&cache_key, &state.state.cache_service).await {
        Ok(Some(response)) => response,
        Ok(None) => {
            record_debug_status_metrics("get_debug_cached_response", StatusCode::NOT_FOUND, started);
            return Err(StatusCode::NOT_FOUND);
        }
        Err(err) => {
            log::error!("Failed to read cached response {cache_key}: {err}");
            record_debug_status_metrics(
                "get_debug_cached_response",
                StatusCode::INTERNAL_SERVER_ERROR,
                started,
            );
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    record_debug_metrics("get_debug_cached_response", "success", started);
    Ok(Json(DebugCachedResponse {
        cache_key,
        response: cached,
    }))
}

async fn load_cached_response(
    cache_key: &str,
    cache_service: &crate::cacheable::CacheService,
) -> Result<Option<Response>, crate::errors::CacheError> {
    Response::sync(cache_key, cache_service).await
}

async fn load_profile(
    state: &ApiState,
    account: &str,
    platform: &str,
    module: &str,
) -> Result<TaskProfileSnapshot, StatusCode> {
    let namespace = state.state.config.read().await.name.clone();
    state
        .profile_store
        .get_profile(&namespace, account, platform, module)
        .ok_or(StatusCode::NOT_FOUND)
}

fn build_debug_layers(
    snapshot: &TaskProfileSnapshot,
    store: &ProfileControlPlaneStore,
    namespace: &str,
) -> Value {
    if let Some(layers) = snapshot.debug_layers_json.clone() {
        return layers;
    }

    let mut middleware_configs = Map::new();
    for (name, config) in &snapshot.middleware_configs {
        middleware_configs.insert(name.clone(), typed_envelope_value(config));
    }

    let account_config = store
        .get_default(DefaultConfigScope::Account, namespace, &snapshot.account)
        .map(|record| typed_envelope_value(&record.default.config))
        .unwrap_or(Value::Null);
    let platform_config = store
        .get_default(DefaultConfigScope::Platform, namespace, &snapshot.platform)
        .map(|record| typed_envelope_value(&record.default.config))
        .unwrap_or(Value::Null);
    let module_config = store
        .get_default(DefaultConfigScope::Module, namespace, &snapshot.module)
        .map(|record| typed_envelope_value(&record.default.config))
        .unwrap_or(Value::Null);

    json!({
        "account_config": account_config,
        "platform_config": platform_config,
        "module_config": module_config,
        "task_profile_config": serde_json::to_value(&snapshot.common).unwrap_or(Value::Null),
        "download_middleware_config": middleware_subset(snapshot, store, namespace, MiddlewareType::Download),
        "data_middleware_config": middleware_subset(snapshot, store, namespace, MiddlewareType::Data),
        "node_configs": snapshot.node_configs.iter().map(|(name, config)| {
            (name.clone(), typed_envelope_value(config))
        }).collect::<Map<String, Value>>(),
        "middleware_configs": middleware_configs,
    })
}

fn middleware_subset(
    snapshot: &TaskProfileSnapshot,
    store: &ProfileControlPlaneStore,
    namespace: &str,
    middleware_type: MiddlewareType,
) -> Value {
    let names: Vec<String> = match middleware_type {
        MiddlewareType::Download => snapshot
            .download_middleware
            .iter()
            .map(|binding| binding.name.clone())
            .collect(),
        MiddlewareType::Data => snapshot
            .data_middleware
            .iter()
            .map(|binding| binding.name.clone())
            .collect(),
    };

    let mut resolved: Map<String, Value> = store
        .middleware_layers(namespace, middleware_type, &names)
        .into_iter()
        .map(|(name, config)| (name, typed_envelope_value(&config)))
        .collect();

    for name in names {
        if resolved.contains_key(&name) {
            continue;
        }
        if let Some(config) = snapshot.middleware_configs.get(&name) {
            resolved.insert(name, typed_envelope_value(config));
        }
    }

    Value::Object(resolved)
}

fn merged_debug_config(
    snapshot: &TaskProfileSnapshot,
    store: &ProfileControlPlaneStore,
    namespace: &str,
) -> Value {
    let layers = build_debug_layers(snapshot, store, namespace);
    let mut merged = Map::new();
    for key in [
        "account_config",
        "platform_config",
        "module_config",
        "task_profile_config",
    ] {
        merge_object_layer(&mut merged, layers.get(key));
    }
    Value::Object(merged)
}

fn merge_object_layer(target: &mut Map<String, Value>, layer: Option<&Value>) {
    let Some(Value::Object(map)) = layer else {
        return;
    };
    for (key, value) in map {
        target.insert(key.clone(), value.clone());
    }
}

fn typed_envelope_value(envelope: &TypedEnvelope) -> Value {
    if envelope.codec == crate::common::model::PayloadCodec::Json {
        serde_json::from_slice(&envelope.bytes).unwrap_or_else(|_| {
            json!({
                "schema_id": envelope.schema_id,
                "schema_version": envelope.schema_version,
            })
        })
    } else {
        json!({
            "schema_id": envelope.schema_id,
            "schema_version": envelope.schema_version,
            "codec": format!("{:?}", envelope.codec).to_lowercase(),
            "bytes_len": envelope.bytes.len(),
        })
    }
}

fn parse_pipeline_stage(value: &str) -> Option<PipelineStage> {
    match value.trim().to_ascii_lowercase().as_str() {
        "task" => Some(PipelineStage::Task),
        "request" => Some(PipelineStage::Request),
        "response" => Some(PipelineStage::Response),
        "parser_task" | "parser-task" | "parsertask" => Some(PipelineStage::ParserTask),
        "error" | "error_task" | "error-task" | "errortask" => Some(PipelineStage::Error),
        _ => None,
    }
}

fn status_record_response(record: StoredStatusEntryRecord) -> DebugStatusResponse {
    DebugStatusResponse {
        task_id: record.entry.task_id,
        stage: record.entry.stage,
        status: record.entry.status,
        retry_count: record.entry.retry_count,
        node_id: record.entry.node_id,
        updated_at: record.entry.updated_at,
        error_msg: record.entry.error_msg,
        version: record.version,
    }
}

fn status_counts_response(
    counts: std::collections::HashMap<TaskStatus, u64>,
) -> BTreeMap<String, u64> {
    [
        TaskStatus::Pending,
        TaskStatus::Running,
        TaskStatus::Done,
        TaskStatus::Failed,
        TaskStatus::Retrying,
    ]
    .into_iter()
    .map(|status| {
        (
            status.as_str().to_string(),
            counts.get(&status).copied().unwrap_or(0),
        )
    })
    .collect()
}

fn record_debug_metrics(action: &str, result: &str, started: Instant) {
    crate::common::metrics::inc_throughput("control_plane", "debug", action, result, 1);
    crate::common::metrics::observe_latency(
        "control_plane",
        "debug",
        action,
        result,
        started.elapsed().as_secs_f64(),
    );
}

fn record_debug_status_metrics(action: &str, status: StatusCode, started: Instant) {
    let result = match status {
        StatusCode::OK => "success",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::BAD_REQUEST => "bad_request",
        _ => "error",
    };
    record_debug_metrics(action, result, started);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        build_debug_layers, load_cached_response, merge_object_layer, parse_pipeline_stage,
        status_counts_response, status_record_response, DebugStatusResponse,
    };
    use crate::cacheable::{CacheAble, CacheService};
    use crate::common::model::{
        ExecutionMark, MiddlewareBinding, MiddlewareType, PayloadCodec, PipelineStage, Priority,
        ResolvedCommonConfig, Response, StatusEntry, TaskProfileSnapshot, TaskStatus,
        TypedEnvelope,
    };
    use crate::engine::api::profile_store::{ProfileControlPlaneStore, StoredStatusEntryRecord};
    use serde_json::{Map, json};
    use uuid::Uuid;

    #[test]
    fn debug_layers_fall_back_to_snapshot_content() {
        let store = ProfileControlPlaneStore::default();
        let snapshot = TaskProfileSnapshot {
            namespace: "demo".to_string(),
            account: "account-a".to_string(),
            platform: "shopee".to_string(),
            module: "search".to_string(),
            version: 3,
            enabled: true,
            common: ResolvedCommonConfig::default(),
            node_configs: BTreeMap::from([(
                "entry".to_string(),
                TypedEnvelope::new("node.entry", 1, PayloadCodec::Json, br#"{"page_size":50}"#),
            )]),
            download_middleware: vec![MiddlewareBinding {
                name: "proxy".to_string(),
                middleware_type: MiddlewareType::Download,
                weight: 10,
            }],
            data_middleware: Vec::new(),
            middleware_configs: BTreeMap::from([(
                "proxy".to_string(),
                TypedEnvelope::new("mw.proxy", 1, PayloadCodec::Json, br#"{"pool":"sg"}"#),
            )]),
            debug_layers_json: None,
            updated_at: 1,
            updated_by: "tester".to_string(),
        };

        let layers = build_debug_layers(&snapshot, &store, "demo");
        assert_eq!(layers["download_middleware_config"]["proxy"]["pool"], "sg");
        assert_eq!(layers["node_configs"]["entry"]["page_size"], 50);
    }

    #[test]
    fn merge_object_layer_overrides_existing_keys() {
        let mut merged = Map::new();
        merge_object_layer(&mut merged, Some(&json!({"timeout": 30, "pool": "a"})));
        merge_object_layer(&mut merged, Some(&json!({"pool": "b"})));
        assert_eq!(merged.get("pool"), Some(&json!("b")));
    }

    #[test]
    fn parse_pipeline_stage_accepts_runtime_aliases() {
        assert_eq!(parse_pipeline_stage("task"), Some(PipelineStage::Task));
        assert_eq!(parse_pipeline_stage("parser_task"), Some(PipelineStage::ParserTask));
        assert_eq!(parse_pipeline_stage("parser-task"), Some(PipelineStage::ParserTask));
        assert_eq!(parse_pipeline_stage("error_task"), Some(PipelineStage::Error));
        assert_eq!(parse_pipeline_stage("unknown"), None);
    }

    #[test]
    fn status_record_response_flattens_stored_entry() {
        let response = status_record_response(StoredStatusEntryRecord {
            entry: StatusEntry {
                task_id: "task-1".to_string(),
                stage: PipelineStage::Response,
                status: TaskStatus::Done,
                retry_count: 1,
                node_id: "node-a".to_string(),
                updated_at: 42,
                error_msg: None,
            },
            version: 7,
        });

        assert_eq!(
            response,
            DebugStatusResponse {
                task_id: "task-1".to_string(),
                stage: PipelineStage::Response,
                status: TaskStatus::Done,
                retry_count: 1,
                node_id: "node-a".to_string(),
                updated_at: 42,
                error_msg: None,
                version: 7,
            }
        );
    }

    #[tokio::test]
    async fn load_cached_response_returns_cached_payload() {
        let cache_service = CacheService::new(None, "demo:cache".to_string(), None, None);
        let cache_key = "response-cache-key".to_string();
        let response = Response {
            id: Uuid::now_v7(),
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
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            request_hash: Some(cache_key.clone()),
            priority: Priority::Normal,
        };
        response
            .send(&cache_key, &cache_service)
            .await
            .expect("response should be cached");

        let payload = load_cached_response(&cache_key, &cache_service)
            .await
            .expect("cache lookup should succeed")
            .expect("cached response should be returned");

        assert_eq!(payload.request_hash.as_deref(), Some(cache_key.as_str()));
        assert_eq!(payload.content, b"ok".to_vec());
    }

    #[test]
    fn status_counts_response_includes_zeroes_for_missing_states() {
        let counts = status_counts_response(std::collections::HashMap::from([
            (TaskStatus::Done, 2),
            (TaskStatus::Failed, 1),
        ]));

        assert_eq!(counts.get("pending"), Some(&0));
        assert_eq!(counts.get("running"), Some(&0));
        assert_eq!(counts.get("done"), Some(&2));
        assert_eq!(counts.get("failed"), Some(&1));
        assert_eq!(counts.get("retrying"), Some(&0));
    }
}