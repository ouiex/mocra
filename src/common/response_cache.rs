use crate::cacheable::{CacheAble, CacheService};
use crate::common::model::meta::MetaData;
use crate::common::model::{Request, ResolvedCommonConfig, Response};
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use log::warn;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const RESPONSE_CACHE_OWNER_NAMESPACE_KEY: &str = "response_cache_owner_namespace";
pub const RESPONSE_CACHE_OWNER_NODE_ID_KEY: &str = "response_cache_owner_node_id";
pub const RESPONSE_CACHE_OWNER_API_BASE_URL_KEY: &str = "response_cache_owner_api_base_url";
pub const RESPONSE_CACHE_LOOKUP_KEY: &str = "response_cache_lookup_key";
pub const RESPONSE_CACHE_EXPIRES_AT_KEY: &str = "response_cache_expires_at";

pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn remaining_ttl_from_expires_at(expires_at: i64) -> Option<Duration> {
    let remaining_ms = expires_at.saturating_sub(current_time_ms());
    if remaining_ms <= 0 {
        return None;
    }

    Some(Duration::from_millis(remaining_ms as u64))
}

pub fn response_cache_expires_at(metadata: &MetaData) -> Option<i64> {
    metadata.get_trait_config::<i64>(RESPONSE_CACHE_EXPIRES_AT_KEY)
}

pub fn resolve_response_cache_expires_at(
    metadata: &MetaData,
    fallback_ttl: Option<Duration>,
) -> Option<i64> {
    response_cache_expires_at(metadata).or_else(|| {
        fallback_ttl.map(|ttl| current_time_ms().saturating_add(ttl.as_millis() as i64))
    })
}

pub fn current_owner_api_base_url(
    profile_store: &ProfileControlPlaneStore,
    owner_node_id: Option<&str>,
    active_node_heartbeat_ttl: Duration,
) -> Option<String> {
    let owner_node_id = owner_node_id?;
    let node = profile_store
        .list_active_nodes(active_node_heartbeat_ttl)
        .into_iter()
        .find(|candidate| candidate.id == owner_node_id)?;
    let api_port = node.api_port?;
    Some(format!("http://{}:{}", node.ip, api_port))
}

pub fn annotate_response_cache_metadata(
    mut metadata: MetaData,
    owner_namespace: &str,
    owner_node_id: Option<&str>,
    owner_api_base_url: Option<&str>,
    request_hash: Option<&str>,
    expires_at: Option<i64>,
) -> MetaData {
    metadata = metadata.add_trait_config(
        RESPONSE_CACHE_OWNER_NAMESPACE_KEY,
        owner_namespace.to_string(),
    );
    if let Some(node_id) = owner_node_id {
        metadata = metadata.add_trait_config(RESPONSE_CACHE_OWNER_NODE_ID_KEY, node_id.to_string());
    } else if let Some(map) = metadata.trait_meta.as_object_mut() {
        map.remove(RESPONSE_CACHE_OWNER_NODE_ID_KEY);
    }

    if let Some(owner_api_base_url) = owner_api_base_url {
        metadata = metadata.add_trait_config(
            RESPONSE_CACHE_OWNER_API_BASE_URL_KEY,
            owner_api_base_url.to_string(),
        );
    } else if let Some(map) = metadata.trait_meta.as_object_mut() {
        map.remove(RESPONSE_CACHE_OWNER_API_BASE_URL_KEY);
    }

    if let Some(hash) = request_hash {
        metadata = metadata.add_trait_config(RESPONSE_CACHE_LOOKUP_KEY, hash.to_string());
    }

    if let Some(expires_at) = expires_at {
        metadata = metadata.add_trait_config(RESPONSE_CACHE_EXPIRES_AT_KEY, expires_at);
    } else if let Some(map) = metadata.trait_meta.as_object_mut() {
        map.remove(RESPONSE_CACHE_EXPIRES_AT_KEY);
    }

    metadata
}

pub fn apply_request_response_cache_policy(
    mut request: Request,
    common: &ResolvedCommonConfig,
) -> Request {
    request.enable_response_cache = request.enable_response_cache || common.response_cache_enabled;

    if !request.enable_response_cache {
        if let Some(map) = request.meta.trait_meta.as_object_mut() {
            map.remove(RESPONSE_CACHE_EXPIRES_AT_KEY);
        }
        return request;
    }

    if let Some(expires_at) = resolve_response_cache_expires_at(
        &request.meta,
        common.response_cache_ttl_secs.map(Duration::from_secs),
    ) {
        request = request.add_meta(RESPONSE_CACHE_EXPIRES_AT_KEY, expires_at);
    }

    request
}

pub fn localize_response_cache_entry(
    response: &Response,
    owner_namespace: &str,
    owner_node_id: Option<&str>,
    owner_api_base_url: Option<&str>,
    fallback_ttl: Option<Duration>,
) -> Option<(Response, Option<i64>)> {
    let request_hash = response.request_hash.as_deref()?;
    let expires_at = resolve_response_cache_expires_at(&response.metadata, fallback_ttl);

    let mut localized = response.clone();
    localized.metadata = annotate_response_cache_metadata(
        localized.metadata,
        owner_namespace,
        owner_node_id,
        owner_api_base_url,
        Some(request_hash),
        expires_at,
    );

    Some((localized, expires_at))
}

pub struct ResponseCachePersistRequest<'a> {
    pub response: &'a Response,
    pub owner_namespace: &'a str,
    pub owner_node_id: Option<&'a str>,
    pub owner_api_base_url: Option<&'a str>,
    pub fallback_ttl: Option<Duration>,
    pub cache_service: &'a CacheService,
    pub profile_store: &'a ProfileControlPlaneStore,
    pub context: &'a str,
}

pub async fn persist_response_cache_entry(
    request: ResponseCachePersistRequest<'_>,
) -> Option<Response> {
    let (localized_response, expires_at) = localize_response_cache_entry(
        request.response,
        request.owner_namespace,
        request.owner_node_id,
        request.owner_api_base_url,
        request.fallback_ttl,
    )?;
    let request_hash = localized_response.request_hash.as_deref()?;

    let cache_write = if let Some(expires_at) = expires_at {
        let Some(ttl) = remaining_ttl_from_expires_at(expires_at) else {
            warn!(
                "Skipping expired response cache persist: context={} request_id={} module_id={} cache_key={} expires_at={}",
                request.context,
                localized_response.id,
                localized_response.module_id(),
                request_hash,
                expires_at
            );
            return None;
        };
        localized_response
            .send_with_ttl(request_hash, request.cache_service, ttl)
            .await
    } else {
        localized_response
            .send_persistent(request_hash, request.cache_service)
            .await
    };

    if let Err(err) = cache_write {
        warn!(
            "Failed to persist response cache entry: context={} request_id={} module_id={} cache_key={} error={:?}",
            request.context,
            localized_response.id,
            localized_response.module_id(),
            request_hash,
            err
        );
        return None;
    }

    if let Err(err) = request
        .profile_store
        .upsert_response_cache_owner_with_details(
            request_hash,
            request.owner_namespace,
            request.owner_node_id,
            request.owner_api_base_url,
            expires_at,
        )
        .await
    {
        warn!(
            "Failed to record response cache owner: context={} request_id={} module_id={} cache_key={} error={:?}",
            request.context,
            localized_response.id,
            localized_response.module_id(),
            request_hash,
            err
        );
    }

    Some(localized_response)
}

// ── ResponseCacheRemoteClient trait ──────────────────────────────────────

use crate::common::model::Response as ResponseModel;
use async_trait::async_trait;

/// Abstraction for fetching cached responses from remote owner nodes.
/// Allows swapping HTTP/gRPC/mock implementations without changing
/// the downloader's cache lookup logic.
#[async_trait]
pub trait ResponseCacheRemoteClient: Send + Sync {
    /// Fetch a cached response from a remote owner node.
    /// Returns `None` on cache miss, timeout, connection failure,
    /// or any non-success HTTP status.
    async fn fetch_remote_cached_response(&self, cache_key: &str) -> Option<ResponseModel>;
}
