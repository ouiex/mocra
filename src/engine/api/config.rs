use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;

use crate::common::model::control_plane_profile::DefaultConfigScope;
use crate::common::model::{
    DefaultConfigUpsert, MiddlewareType, MiddlewareUpsert, TaskProfileIdentity,
    TaskProfileSnapshot, TaskProfileUpsert,
};
use crate::engine::api::profile_store::{
    ControlPlaneStoreError, DefaultConfigPatch, MiddlewarePatch, TaskProfilePatch,
};
use crate::engine::api::state::ApiState;

#[derive(Debug, Deserialize)]
pub struct ProfileListParams {
    pub account: Option<String>,
    pub platform: Option<String>,
    pub module: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DefaultConfigResponse {
    pub name: String,
    pub enabled: bool,
    pub config: Value,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct DefaultConfigPayload {
    pub namespace: String,
    pub name: String,
    pub enabled: bool,
    pub config: Value,
}

#[derive(Debug, Deserialize)]
pub struct DefaultConfigPatchPayload {
    pub enabled: Option<bool>,
    pub config: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct MiddlewareResponse {
    pub name: String,
    #[serde(rename = "type")]
    pub middleware_type: MiddlewareType,
    pub weight: i32,
    pub enabled: bool,
    pub config: Value,
    pub version: u64,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize)]
pub struct MiddlewarePayload {
    pub namespace: String,
    pub name: String,
    #[serde(rename = "type")]
    pub middleware_type: MiddlewareType,
    pub weight: i32,
    pub enabled: bool,
    pub config: Value,
}

#[derive(Debug, Deserialize)]
pub struct MiddlewarePatchPayload {
    pub enabled: Option<bool>,
    pub weight: Option<i32>,
    pub config: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct MiddlewareQuery {
    #[serde(rename = "type")]
    pub middleware_type: Option<MiddlewareType>,
}

#[derive(Debug, Deserialize)]
pub struct TaskProfilePatchPayload {
    pub enabled: Option<bool>,
    pub common: Option<crate::common::model::ResolvedCommonConfig>,
    pub node_configs:
        Option<std::collections::BTreeMap<String, crate::common::model::TypedEnvelope>>,
    pub download_middleware: Option<Vec<crate::common::model::MiddlewareBinding>>,
    pub data_middleware: Option<Vec<crate::common::model::MiddlewareBinding>>,
    pub middleware_configs:
        Option<std::collections::BTreeMap<String, crate::common::model::TypedEnvelope>>,
    pub debug_layers_json: Option<serde_json::Value>,
    pub updated_by: Option<String>,
}

pub async fn list_profiles(
    State(state): State<ApiState>,
    Query(params): Query<ProfileListParams>,
) -> Json<Vec<TaskProfileSnapshot>> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let response = Json(state.state.profile_store.list_profiles(
        &namespace,
        params.account.as_deref(),
        params.platform.as_deref(),
        params.module.as_deref(),
    ));
    record_api_metrics("list_profiles", "success", started);
    response
}

pub async fn get_profile(
    State(state): State<ApiState>,
    Path((account, platform, module)): Path<(String, String, String)>,
) -> Result<Json<TaskProfileSnapshot>, StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let result = state
        .profile_store
        .get_profile(&namespace, &account, &platform, &module)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND);
    record_status_metrics(
        "get_profile",
        result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or(StatusCode::NOT_FOUND),
        started,
    );
    result
}

pub async fn create_profile(
    State(state): State<ApiState>,
    Json(upsert): Json<TaskProfileUpsert>,
) -> Result<(StatusCode, Json<TaskProfileSnapshot>), StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    if upsert.identity.namespace != namespace {
        record_status_metrics("create_profile", StatusCode::BAD_REQUEST, started);
        return Err(StatusCode::BAD_REQUEST);
    }

    let result = state
        .profile_store
        .upsert_profile(upsert)
        .await
        .map(Json)
        .map(|snapshot| (StatusCode::CREATED, snapshot))
        .map_err(map_profile_store_error);
    record_status_metrics(
        "create_profile",
        result
            .as_ref()
            .map(|_| StatusCode::CREATED)
            .unwrap_or(StatusCode::BAD_REQUEST),
        started,
    );
    result
}

pub async fn patch_profile(
    State(state): State<ApiState>,
    Path((account, platform, module)): Path<(String, String, String)>,
    Json(payload): Json<TaskProfilePatchPayload>,
) -> Result<Json<TaskProfileSnapshot>, StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let identity = TaskProfileIdentity {
        namespace,
        account,
        platform,
        module,
    };

    let patch = TaskProfilePatch {
        enabled: payload.enabled,
        common: payload.common,
        node_configs: payload.node_configs,
        download_middleware: payload.download_middleware,
        data_middleware: payload.data_middleware,
        middleware_configs: payload.middleware_configs,
        debug_layers_json: payload.debug_layers_json,
        updated_by: payload.updated_by,
    };

    let result = state
        .profile_store
        .patch_profile(identity, patch)
        .await
        .map_err(map_profile_store_error)?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND);
    record_status_metrics(
        "patch_profile",
        result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or(StatusCode::NOT_FOUND),
        started,
    );
    result
}

fn map_profile_store_error(error: ControlPlaneStoreError) -> StatusCode {
    match error {
        ControlPlaneStoreError::Validation(error) => {
            log::warn!("Invalid control-plane payload: {}", error);
            StatusCode::BAD_REQUEST
        }
        other => {
            log::error!("Control-plane storage failure: {}", other);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

pub async fn list_accounts(State(state): State<ApiState>) -> Json<Vec<DefaultConfigResponse>> {
    list_defaults(state, DefaultConfigScope::Account).await
}

pub async fn get_account(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    get_default(state, DefaultConfigScope::Account, name).await
}

pub async fn create_account(
    State(state): State<ApiState>,
    Json(payload): Json<DefaultConfigPayload>,
) -> Result<(StatusCode, Json<DefaultConfigResponse>), StatusCode> {
    create_default(state, DefaultConfigScope::Account, payload).await
}

pub async fn patch_account(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(payload): Json<DefaultConfigPatchPayload>,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    patch_default(state, DefaultConfigScope::Account, name, payload).await
}

pub async fn delete_account(State(state): State<ApiState>, Path(name): Path<String>) -> StatusCode {
    delete_default(state, DefaultConfigScope::Account, name).await
}

pub async fn list_platforms(State(state): State<ApiState>) -> Json<Vec<DefaultConfigResponse>> {
    list_defaults(state, DefaultConfigScope::Platform).await
}

pub async fn get_platform(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    get_default(state, DefaultConfigScope::Platform, name).await
}

pub async fn create_platform(
    State(state): State<ApiState>,
    Json(payload): Json<DefaultConfigPayload>,
) -> Result<(StatusCode, Json<DefaultConfigResponse>), StatusCode> {
    create_default(state, DefaultConfigScope::Platform, payload).await
}

pub async fn patch_platform(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(payload): Json<DefaultConfigPatchPayload>,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    patch_default(state, DefaultConfigScope::Platform, name, payload).await
}

pub async fn delete_platform(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> StatusCode {
    delete_default(state, DefaultConfigScope::Platform, name).await
}

pub async fn list_modules(State(state): State<ApiState>) -> Json<Vec<DefaultConfigResponse>> {
    list_defaults(state, DefaultConfigScope::Module).await
}

pub async fn get_module(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    get_default(state, DefaultConfigScope::Module, name).await
}

pub async fn create_module(
    State(state): State<ApiState>,
    Json(payload): Json<DefaultConfigPayload>,
) -> Result<(StatusCode, Json<DefaultConfigResponse>), StatusCode> {
    create_default(state, DefaultConfigScope::Module, payload).await
}

pub async fn patch_module(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Json(payload): Json<DefaultConfigPatchPayload>,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    patch_default(state, DefaultConfigScope::Module, name, payload).await
}

pub async fn delete_module(State(state): State<ApiState>, Path(name): Path<String>) -> StatusCode {
    delete_default(state, DefaultConfigScope::Module, name).await
}

pub async fn list_middlewares(
    State(state): State<ApiState>,
    Query(query): Query<MiddlewareQuery>,
) -> Json<Vec<MiddlewareResponse>> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let response = Json(
        state
            .profile_store
            .list_middlewares(&namespace, query.middleware_type)
            .into_iter()
            .map(middleware_response)
            .collect(),
    );
    record_api_metrics("list_middlewares", "success", started);
    response
}

pub async fn get_middleware(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Query(query): Query<MiddlewareQuery>,
) -> Result<Json<MiddlewareResponse>, StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let Some(middleware_type) = query.middleware_type else {
        record_status_metrics("get_middleware", StatusCode::BAD_REQUEST, started);
        return Err(StatusCode::BAD_REQUEST);
    };
    let result = state
        .profile_store
        .get_middleware(&namespace, &name, middleware_type)
        .map(middleware_response)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND);
    record_status_metrics(
        "get_middleware",
        result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or(StatusCode::NOT_FOUND),
        started,
    );
    result
}

pub async fn create_middleware(
    State(state): State<ApiState>,
    Json(payload): Json<MiddlewarePayload>,
) -> Result<(StatusCode, Json<MiddlewareResponse>), StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    if payload.namespace != namespace {
        record_status_metrics("create_middleware", StatusCode::BAD_REQUEST, started);
        return Err(StatusCode::BAD_REQUEST);
    }
    let result = state
        .profile_store
        .upsert_middleware(
            MiddlewareUpsert {
                namespace: payload.namespace,
                name: payload.name,
                middleware_type: payload.middleware_type,
                enabled: payload.enabled,
                config: json_envelope("control.middleware", &payload.config),
                weight: payload.weight,
            },
            payload.enabled,
        )
        .await
        .map(middleware_response)
        .map(Json)
        .map(|response| (StatusCode::CREATED, response))
        .map_err(map_profile_store_error);
    record_status_metrics(
        "create_middleware",
        result
            .as_ref()
            .map(|_| StatusCode::CREATED)
            .unwrap_or(StatusCode::BAD_REQUEST),
        started,
    );
    result
}

pub async fn patch_middleware(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Query(query): Query<MiddlewareQuery>,
    Json(payload): Json<MiddlewarePatchPayload>,
) -> Result<Json<MiddlewareResponse>, StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let Some(middleware_type) = query.middleware_type else {
        record_status_metrics("patch_middleware", StatusCode::BAD_REQUEST, started);
        return Err(StatusCode::BAD_REQUEST);
    };
    let result = state
        .profile_store
        .patch_middleware(
            &namespace,
            middleware_type,
            &name,
            MiddlewarePatch {
                enabled: payload.enabled,
                config: payload
                    .config
                    .as_ref()
                    .map(|value| json_envelope("control.middleware", value)),
                weight: payload.weight,
            },
        )
        .await
        .map_err(map_profile_store_error)?
        .map(middleware_response)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND);
    record_status_metrics(
        "patch_middleware",
        result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or(StatusCode::NOT_FOUND),
        started,
    );
    result
}

pub async fn delete_middleware(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Query(query): Query<MiddlewareQuery>,
) -> StatusCode {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let Some(middleware_type) = query.middleware_type else {
        record_status_metrics("delete_middleware", StatusCode::BAD_REQUEST, started);
        return StatusCode::BAD_REQUEST;
    };
    let status = match state
        .profile_store
        .disable_middleware(&namespace, middleware_type, &name)
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            log::error!("Failed to disable middleware {}: {}", name, e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    record_status_metrics("delete_middleware", status, started);
    status
}

async fn list_defaults(
    state: ApiState,
    scope: DefaultConfigScope,
) -> Json<Vec<DefaultConfigResponse>> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let response = Json(
        state
            .profile_store
            .list_defaults(scope, &namespace)
            .into_iter()
            .map(default_config_response)
            .collect(),
    );
    record_api_metrics(list_action(scope), "success", started);
    response
}

async fn get_default(
    state: ApiState,
    scope: DefaultConfigScope,
    name: String,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let result = state
        .profile_store
        .get_default(scope, &namespace, &name)
        .map(default_config_response)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND);
    record_status_metrics(
        get_action(scope),
        result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or(StatusCode::NOT_FOUND),
        started,
    );
    result
}

async fn create_default(
    state: ApiState,
    scope: DefaultConfigScope,
    payload: DefaultConfigPayload,
) -> Result<(StatusCode, Json<DefaultConfigResponse>), StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    if payload.namespace != namespace {
        record_status_metrics(create_action(scope), StatusCode::BAD_REQUEST, started);
        return Err(StatusCode::BAD_REQUEST);
    }

    let result = state
        .profile_store
        .upsert_default(
            scope,
            DefaultConfigUpsert {
                namespace: payload.namespace,
                name: payload.name,
                enabled: payload.enabled,
                config: json_envelope("control.default", &payload.config),
            },
        )
        .await
        .map(default_config_response)
        .map(Json)
        .map(|response| (StatusCode::CREATED, response))
        .map_err(map_profile_store_error);

    record_status_metrics(
        create_action(scope),
        result
            .as_ref()
            .map(|_| StatusCode::CREATED)
            .unwrap_or(StatusCode::BAD_REQUEST),
        started,
    );
    result
}

async fn patch_default(
    state: ApiState,
    scope: DefaultConfigScope,
    name: String,
    payload: DefaultConfigPatchPayload,
) -> Result<Json<DefaultConfigResponse>, StatusCode> {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let result = state
        .profile_store
        .patch_default(
            scope,
            &namespace,
            &name,
            DefaultConfigPatch {
                enabled: payload.enabled,
                config: payload
                    .config
                    .as_ref()
                    .map(|value| json_envelope("control.default", value)),
            },
        )
        .await
        .map_err(map_profile_store_error)?
        .map(default_config_response)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND);
    record_status_metrics(
        patch_action(scope),
        result
            .as_ref()
            .map(|_| StatusCode::OK)
            .unwrap_or(StatusCode::NOT_FOUND),
        started,
    );
    result
}

async fn delete_default(state: ApiState, scope: DefaultConfigScope, name: String) -> StatusCode {
    let started = Instant::now();
    let namespace = state.state.config.read().await.name.clone();
    let status = match state
        .state
        .profile_store
        .disable_default(scope, &namespace, &name)
        .await
    {
        Ok(true) => StatusCode::NO_CONTENT,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(e) => {
            log::error!("Failed to disable default config {}: {}", name, e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    record_status_metrics(delete_action(scope), status, started);
    status
}

fn default_config_response(
    record: crate::engine::api::profile_store::StoredDefaultConfig,
) -> DefaultConfigResponse {
    DefaultConfigResponse {
        name: record.default.name,
        enabled: record.default.enabled,
        config: typed_envelope_to_json(&record.default.config),
        version: record.version,
        updated_at: record.updated_at,
    }
}

fn middleware_response(
    record: crate::engine::api::profile_store::StoredMiddleware,
) -> MiddlewareResponse {
    MiddlewareResponse {
        name: record.middleware.name,
        middleware_type: record.middleware.middleware_type,
        weight: record.middleware.weight,
        enabled: record.enabled,
        config: typed_envelope_to_json(&record.middleware.config),
        version: record.version,
        updated_at: record.updated_at,
    }
}

fn json_envelope(schema_id: &str, value: &Value) -> crate::common::model::TypedEnvelope {
    crate::common::model::TypedEnvelope::new(
        schema_id,
        1,
        crate::common::model::PayloadCodec::Json,
        serde_json::to_vec(value).unwrap_or_default(),
    )
}

fn typed_envelope_to_json(envelope: &crate::common::model::TypedEnvelope) -> Value {
    if envelope.codec == crate::common::model::PayloadCodec::Json {
        serde_json::from_slice(&envelope.bytes).unwrap_or(Value::Null)
    } else {
        Value::Null
    }
}

fn record_api_metrics(action: &str, result: &str, started: Instant) {
    crate::common::metrics::inc_throughput("control_plane", "config", action, result, 1);
    crate::common::metrics::observe_latency(
        "control_plane",
        "config",
        action,
        result,
        started.elapsed().as_secs_f64(),
    );
}

fn record_status_metrics(action: &str, status: StatusCode, started: Instant) {
    let result = match status {
        StatusCode::OK | StatusCode::CREATED | StatusCode::NO_CONTENT => "success",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::BAD_REQUEST => "bad_request",
        _ => "error",
    };
    record_api_metrics(action, result, started);
    if result == "error" {
        crate::common::metrics::inc_error("control_plane", "config", "http", status.as_str(), 1);
    }
}

fn list_action(scope: DefaultConfigScope) -> &'static str {
    match scope {
        DefaultConfigScope::Account => "list_accounts",
        DefaultConfigScope::Platform => "list_platforms",
        DefaultConfigScope::Module => "list_modules",
    }
}

fn get_action(scope: DefaultConfigScope) -> &'static str {
    match scope {
        DefaultConfigScope::Account => "get_account",
        DefaultConfigScope::Platform => "get_platform",
        DefaultConfigScope::Module => "get_module",
    }
}

fn create_action(scope: DefaultConfigScope) -> &'static str {
    match scope {
        DefaultConfigScope::Account => "create_account",
        DefaultConfigScope::Platform => "create_platform",
        DefaultConfigScope::Module => "create_module",
    }
}

fn patch_action(scope: DefaultConfigScope) -> &'static str {
    match scope {
        DefaultConfigScope::Account => "patch_account",
        DefaultConfigScope::Platform => "patch_platform",
        DefaultConfigScope::Module => "patch_module",
    }
}

fn delete_action(scope: DefaultConfigScope) -> &'static str {
    match scope {
        DefaultConfigScope::Account => "delete_account",
        DefaultConfigScope::Platform => "delete_platform",
        DefaultConfigScope::Module => "delete_module",
    }
}
