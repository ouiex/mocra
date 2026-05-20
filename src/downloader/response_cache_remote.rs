use crate::common::model::Response;
use crate::common::response_cache::ResponseCacheRemoteClient;
use crate::engine::api::profile_store::ProfileControlPlaneStore;
use async_trait::async_trait;
use log::warn;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const ACTIVE_NODE_HEARTBEAT_TTL_SECS: u64 = 30;
const REMOTE_CACHE_LOOKUP_TIMEOUT_SECS: u64 = 3;

fn response_cache_current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, serde::Deserialize)]
struct RemoteCachedResponsePayload {
    response: Response,
}

/// HTTP-based implementation of `ResponseCacheRemoteClient`.
/// Handles owner lookup, endpoint resolution, HTTP fetch, and stale-record cleanup.
#[derive(Clone)]
pub struct HttpResponseCacheRemoteClient {
    owner_namespace: String,
    owner_node_id: Option<String>,
    profile_store: Arc<ProfileControlPlaneStore>,
    api_key: Option<String>,
    /// Federation response-cache API endpoints, keyed by namespace.
    pub federation_endpoints: HashMap<String, String>,
    http_client: Client,
}

impl HttpResponseCacheRemoteClient {
    pub fn new(
        owner_namespace: String,
        owner_node_id: Option<String>,
        profile_store: Arc<ProfileControlPlaneStore>,
        api_key: Option<String>,
        federation_endpoints: HashMap<String, String>,
        http_client: Client,
    ) -> Self {
        Self {
            owner_namespace,
            owner_node_id,
            profile_store,
            api_key,
            federation_endpoints,
            http_client,
        }
    }
}

#[async_trait]
impl ResponseCacheRemoteClient for HttpResponseCacheRemoteClient {
    async fn fetch_remote_cached_response(&self, cache_key: &str) -> Option<Response> {
        let owner = self.profile_store.get_response_cache_owner(cache_key)?;
        if owner
            .expires_at
            .is_some_and(|expires_at| expires_at <= response_cache_current_time_ms())
        {
            self.clear_stale_owner(
                cache_key,
                &owner.owner_namespace,
                owner.owner_node_id.as_deref(),
                "owner_record_expired",
            )
            .await;
            return None;
        }
        let Some(api_key) = self.api_key.as_deref() else {
            warn!(
                "Remote cached response lookup skipped: cache_key={} owner_namespace={} reason=missing_api_key",
                cache_key, owner.owner_namespace
            );
            return None;
        };

        if owner.owner_namespace == self.owner_namespace {
            let Some(owner_node_id) = owner.owner_node_id.as_deref() else {
                warn!(
                    "Remote cached response lookup skipped: cache_key={} owner_namespace={} reason=missing_owner_node_id",
                    cache_key, owner.owner_namespace
                );
                return None;
            };
            if self.owner_node_id.as_deref() == Some(owner_node_id) {
                self.clear_stale_owner(
                    cache_key,
                    &owner.owner_namespace,
                    Some(owner_node_id),
                    "local_owner_cache_miss",
                )
                .await;
                return None;
            }
            let node_active = self
                .profile_store
                .list_active_nodes(Duration::from_secs(ACTIVE_NODE_HEARTBEAT_TTL_SECS))
                .into_iter()
                .any(|candidate| candidate.id == owner_node_id);
            if !node_active {
                self.clear_stale_owner(
                    cache_key,
                    &owner.owner_namespace,
                    Some(owner_node_id),
                    "same_namespace_owner_not_active",
                )
                .await;
                return None;
            }
        }

        let encoded_cache_key: String =
            url::form_urlencoded::byte_serialize(cache_key.as_bytes()).collect();
        let remote_endpoints = self.resolve_endpoints(&owner, &encoded_cache_key).await;

        if remote_endpoints.is_empty() {
            let reason = if owner.owner_namespace == self.owner_namespace {
                "missing_same_namespace_endpoint"
            } else {
                "missing_foreign_namespace_endpoint_mapping"
            };
            warn!(
                "Remote cached response lookup skipped: cache_key={} owner_namespace={} owner_node_id={:?} reason={}",
                cache_key, owner.owner_namespace, owner.owner_node_id, reason
            );
            return None;
        }

        for (endpoint, authoritative_not_found) in remote_endpoints {
            let response = match self
                .http_client
                .get(&endpoint)
                .header("x-api-key", api_key)
                .timeout(Duration::from_secs(REMOTE_CACHE_LOOKUP_TIMEOUT_SECS))
                .send()
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    warn!(
                        "Remote cached response lookup failed: cache_key={} owner_namespace={} endpoint={} reason=request_failed error={:?}",
                        cache_key, owner.owner_namespace, endpoint, err
                    );
                    continue;
                }
            };

            if response.status() == reqwest::StatusCode::NOT_FOUND {
                if authoritative_not_found {
                    self.clear_stale_owner(
                        cache_key,
                        &owner.owner_namespace,
                        owner.owner_node_id.as_deref(),
                        "remote_cache_not_found",
                    )
                    .await;
                }
                continue;
            }

            if !response.status().is_success() {
                warn!(
                    "Remote cached response lookup returned non-success: cache_key={} owner_namespace={} endpoint={} reason=http_status status={}",
                    cache_key, owner.owner_namespace, endpoint, response.status()
                );
                continue;
            }

            match response.json::<RemoteCachedResponsePayload>().await {
                Ok(payload) => {
                    if self.response_matches_key(cache_key, &payload.response) {
                        return Some(payload.response);
                    }
                }
                Err(err) => {
                    warn!(
                        "Remote cached response lookup failed: cache_key={} owner_namespace={} endpoint={} reason=decode_failed error={:?}",
                        cache_key, owner.owner_namespace, endpoint, err
                    );
                }
            }
        }

        None
    }
}

impl HttpResponseCacheRemoteClient {
    async fn resolve_endpoints(
        &self,
        owner: &crate::engine::api::profile_store::StoredResponseCacheOwnerRecord,
        encoded_cache_key: &str,
    ) -> Vec<(String, bool)> {
        let mut endpoints: Vec<(String, bool)> = Vec::new();
        let mut push = |endpoint: String, authoritative_not_found: bool| {
            if let Some((_, existing)) = endpoints
                .iter_mut()
                .find(|(e, _)| *e == endpoint)
            {
                *existing |= authoritative_not_found;
            } else {
                endpoints.push((endpoint, authoritative_not_found));
            }
        };

        if owner.owner_namespace == self.owner_namespace {
            let Some(owner_node_id) = owner.owner_node_id.as_deref() else {
                return endpoints;
            };
            let Some(node) = self
                .profile_store
                .list_active_nodes(Duration::from_secs(ACTIVE_NODE_HEARTBEAT_TTL_SECS))
                .into_iter()
                .find(|candidate| candidate.id == owner_node_id)
            else {
                return endpoints;
            };
            let Some(api_port) = node.api_port else {
                return endpoints;
            };
            push(
                format!(
                    "http://{}:{}/debug/cache/response/{}",
                    node.ip, api_port, encoded_cache_key
                ),
                true,
            );
        } else {
            if let Some(base_url) = owner.owner_api_base_url.as_deref() {
                push(
                    format!(
                        "{}/debug/cache/response/{}",
                        base_url.trim_end_matches('/'),
                        encoded_cache_key
                    ),
                    false,
                );
            }
            if let Some(base_url) = self.federation_endpoints.get(&owner.owner_namespace) {
                push(
                    format!(
                        "{}/debug/cache/response/{}",
                        base_url.trim_end_matches('/'),
                        encoded_cache_key
                    ),
                    true,
                );
            }
        }

        endpoints
    }

    fn response_matches_key(&self, cache_key: &str, response: &Response) -> bool {
        use crate::common::response_cache::RESPONSE_CACHE_LOOKUP_KEY;

        if let Some(response_hash) = response.request_hash.as_deref()
            && response_hash != cache_key
        {
            warn!(
                "Cached response returned mismatched request_hash: cache_key={} response_request_hash={}",
                cache_key, response_hash
            );
            return false;
        }

        if let Some(lookup_key) = response
            .metadata
            .get_trait_config::<String>(RESPONSE_CACHE_LOOKUP_KEY)
            .as_deref()
            && lookup_key != cache_key
        {
            warn!(
                "Cached response returned mismatched lookup_key: cache_key={} response_lookup_key={}",
                cache_key, lookup_key
            );
            return false;
        }

        true
    }

    async fn clear_stale_owner(
        &self,
        cache_key: &str,
        owner_namespace: &str,
        owner_node_id: Option<&str>,
        reason: &str,
    ) {
        if let Err(err) = self
            .profile_store
            .remove_response_cache_owner_if_matches(cache_key, owner_namespace, owner_node_id)
            .await
        {
            warn!(
                "Failed to clear stale response cache owner: cache_key={} owner_namespace={} owner_node_id={:?} reason={} error={:?}",
                cache_key, owner_namespace, owner_node_id, reason, err
            );
        }
    }
}
