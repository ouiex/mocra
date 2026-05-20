use async_trait::async_trait;
use rmp_serde as rmps;
use std::sync::Arc;
use uuid::Uuid;

use crate::common::model::meta::MetaSource;
use crate::common::interface::storage::{BlobStorage, Offloadable};
use crate::common::model::{
    ExecutionMark, ExecutionMeta, PayloadCodec, Prioritizable, Priority, Request,
    RequestDispatchEnvelope, Response, ResponseDispatchEnvelope, RoutingMeta, TypedEnvelope,
};
use crate::errors::{Error, ErrorKind, Result};
use crate::queue::Identifiable;

const TRANSPORT_REQUEST_SCHEMA_ID: &str = "transport.request";
const TRANSPORT_RESPONSE_SCHEMA_ID: &str = "transport.response";
const TRANSPORT_REQUEST_NODE: &str = "transport_request";
const TRANSPORT_RESPONSE_NODE: &str = "transport_response";
const TRANSPORT_NAMESPACE_KEY: &str = "transport_namespace";

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn transport_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Queue, Some(error))
}

fn transport_node_key(context: &ExecutionMark, fallback: &str) -> String {
    context
        .node_id
        .clone()
        .or_else(|| context.step_idx.map(|step_idx| format!("step_{step_idx}")))
        .unwrap_or_else(|| fallback.to_string())
}

fn transport_parent_request(prefix_request: Uuid) -> Option<Uuid> {
    (!prefix_request.is_nil()).then_some(prefix_request)
}

fn extract_profile_version_from_request(request: &Request) -> u64 {
    request.meta.get_task_config::<u64>("profile_version").unwrap_or(0)
}

fn extract_profile_version_from_response(response: &Response) -> u64 {
    response
        .metadata
        .get_task_config::<u64>("profile_version")
        .unwrap_or(0)
}

fn resolve_request_namespace(request: &Request, fallback: impl Into<String>) -> String {
    request
        .meta
        .get_task_config::<String>(TRANSPORT_NAMESPACE_KEY)
        .unwrap_or_else(|| fallback.into())
}

fn resolve_response_namespace(response: &Response, fallback: impl Into<String>) -> String {
    response
        .metadata
        .get_task_config::<String>(TRANSPORT_NAMESPACE_KEY)
        .unwrap_or_else(|| fallback.into())
}

pub fn build_request_dispatch(
    request: &Request,
    namespace: impl Into<String>,
) -> Result<RequestDispatchEnvelope> {
    let payload = TypedEnvelope::new(
        TRANSPORT_REQUEST_SCHEMA_ID,
        1,
        PayloadCodec::MsgPack,
        rmps::to_vec(request).map_err(transport_error)?,
    );
    let timestamp = now_ms();

    Ok(RequestDispatchEnvelope::new(
        RoutingMeta {
            namespace: resolve_request_namespace(request, namespace),
            account: request.account.clone(),
            platform: request.platform.clone(),
            module: request.module.clone(),
            node_key: transport_node_key(&request.context, TRANSPORT_REQUEST_NODE),
            run_id: request.run_id,
            request_id: request.id,
            parent_request_id: transport_parent_request(request.prefix_request),
            priority: request.priority,
        },
        ExecutionMeta {
            retry_count: request.retry_times as u32,
            task_retry_count: request.task_retry_times as u32,
            profile_version: extract_profile_version_from_request(request),
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
            ..ExecutionMeta::default()
        },
        payload,
    ))
}

pub fn decode_request_dispatch(dispatch: RequestDispatchEnvelope) -> Result<Request> {
    let namespace = dispatch.routing.namespace.clone();
    let mut request: Request = match dispatch.request.codec {
        PayloadCodec::MsgPack => {
            rmps::from_slice(&dispatch.request.bytes).map_err(transport_error)?
        }
        codec => {
            return Err(transport_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported request dispatch codec: {codec:?}"),
            )));
        }
    };
    request.meta = request.meta.add(
        TRANSPORT_NAMESPACE_KEY,
        serde_json::Value::String(namespace),
        MetaSource::Task,
    );
    Ok(request)
}

pub fn build_response_dispatch(
    response: &Response,
    namespace: impl Into<String>,
) -> Result<ResponseDispatchEnvelope> {
    let payload = TypedEnvelope::new(
        TRANSPORT_RESPONSE_SCHEMA_ID,
        1,
        PayloadCodec::MsgPack,
        rmps::to_vec(response).map_err(transport_error)?,
    );
    let timestamp = now_ms();

    Ok(ResponseDispatchEnvelope::new(
        RoutingMeta {
            namespace: resolve_response_namespace(response, namespace),
            account: response.account.clone(),
            platform: response.platform.clone(),
            module: response.module.clone(),
            node_key: transport_node_key(&response.context, TRANSPORT_RESPONSE_NODE),
            run_id: response.run_id,
            request_id: response.id,
            parent_request_id: transport_parent_request(response.prefix_request),
            priority: response.priority,
        },
        ExecutionMeta {
            task_retry_count: response.task_retry_times as u32,
            profile_version: extract_profile_version_from_response(response),
            created_at_ms: timestamp,
            updated_at_ms: timestamp,
            ..ExecutionMeta::default()
        },
        payload,
    ))
}

pub fn decode_response_dispatch(dispatch: ResponseDispatchEnvelope) -> Result<Response> {
    let namespace = dispatch.routing.namespace.clone();
    let mut response: Response = match dispatch.response.codec {
        PayloadCodec::MsgPack => {
            rmps::from_slice(&dispatch.response.bytes).map_err(transport_error)?
        }
        codec => {
            return Err(transport_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported response dispatch codec: {codec:?}"),
            )));
        }
    };
    response.metadata = response.metadata.add(
        TRANSPORT_NAMESPACE_KEY,
        serde_json::Value::String(namespace),
        MetaSource::Task,
    );
    Ok(response)
}

impl Identifiable for RequestDispatchEnvelope {
    fn get_id(&self) -> String {
        self.routing.request_id.to_string()
    }
}

impl Identifiable for ResponseDispatchEnvelope {
    fn get_id(&self) -> String {
        self.routing.request_id.to_string()
    }
}

impl Prioritizable for RequestDispatchEnvelope {
    fn get_priority(&self) -> Priority {
        self.routing.priority
    }
}

impl Prioritizable for ResponseDispatchEnvelope {
    fn get_priority(&self) -> Priority {
        self.routing.priority
    }
}

#[async_trait]
impl Offloadable for RequestDispatchEnvelope {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }

    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> Result<()> {
        Ok(())
    }

    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Offloadable for ResponseDispatchEnvelope {
    fn should_offload(&self, threshold: usize) -> bool {
        self.response.bytes.len() > threshold
    }

    async fn offload(&mut self, storage: &Arc<dyn BlobStorage>) -> Result<()> {
        let mut response = decode_response_dispatch(self.clone())?;
        response.offload(storage).await?;
        *self = build_response_dispatch(&response, self.routing.namespace.clone())?;
        Ok(())
    }

    async fn reload(&mut self, storage: &Arc<dyn BlobStorage>) -> Result<()> {
        let mut response = decode_response_dispatch(self.clone())?;
        response.reload(storage).await?;
        *self = build_response_dispatch(&response, self.routing.namespace.clone())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::Priority;

    #[test]
    fn request_dispatch_round_trips_transport_request() {
        let request = Request::new("http://example.com", "GET").with_priority(Priority::High);

        let dispatch = build_request_dispatch(&request, "demo").expect("dispatch should build");
        assert_eq!(dispatch.routing.namespace, "demo");
        assert_eq!(dispatch.routing.request_id, request.id);
        assert_eq!(dispatch.exec.retry_count, 0);
        assert_eq!(dispatch.request.schema_id, TRANSPORT_REQUEST_SCHEMA_ID);

        let decoded = decode_request_dispatch(dispatch).expect("dispatch should decode");
        assert_eq!(decoded.id, request.id);
        assert_eq!(decoded.url, request.url);
        assert_eq!(decoded.priority, Priority::High);
    }

    #[test]
    fn request_dispatch_rejects_non_msgpack_codec() {
        let request = Request::new("http://example.com", "GET");
        let mut dispatch = build_request_dispatch(&request, "demo").expect("dispatch should build");
        dispatch.request.codec = PayloadCodec::Json;
        dispatch.request.bytes = serde_json::to_vec(&request).expect("json bytes");

        let err = decode_request_dispatch(dispatch).expect_err("json transport codec should be rejected");
        assert!(err.to_string().contains("unsupported request dispatch codec"));
    }

    #[test]
    fn response_dispatch_round_trips_transport_response() {
        let response = Response {
            id: Uuid::now_v7(),
            platform: "platform-a".to_string(),
            account: "account-a".to_string(),
            module: "catalog".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: b"ok".to_vec(),
            storage_path: None,
            headers: vec![("content-type".to_string(), "text/plain".to_string())],
            task_retry_times: 1,
            metadata: Default::default(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: ExecutionMark::default(),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            request_hash: None,
            priority: Priority::Normal,
        };

        let dispatch = build_response_dispatch(&response, "demo").expect("dispatch should build");
        assert_eq!(dispatch.routing.request_id, response.id);
        assert_eq!(dispatch.exec.task_retry_count, 1);
        assert_eq!(dispatch.response.schema_id, TRANSPORT_RESPONSE_SCHEMA_ID);

        let decoded = decode_response_dispatch(dispatch).expect("dispatch should decode");
        assert_eq!(decoded.id, response.id);
        assert_eq!(decoded.content, response.content);
        assert_eq!(decoded.status_code, 200);
        assert_eq!(
            decoded
                .metadata
                .get_task_config::<String>(TRANSPORT_NAMESPACE_KEY)
                .as_deref(),
            Some("demo")
        );
    }

    #[test]
    fn response_dispatch_rejects_non_msgpack_codec() {
        let response = Response {
            id: Uuid::now_v7(),
            platform: "platform-a".to_string(),
            account: "account-a".to_string(),
            module: "catalog".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: b"ok".to_vec(),
            storage_path: None,
            headers: vec![("content-type".to_string(), "text/plain".to_string())],
            task_retry_times: 1,
            metadata: Default::default(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: ExecutionMark::default(),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            request_hash: None,
            priority: Priority::Normal,
        };

        let mut dispatch = build_response_dispatch(&response, "demo").expect("dispatch should build");
        dispatch.response.codec = PayloadCodec::Json;
        dispatch.response.bytes = serde_json::to_vec(&response).expect("json bytes");

        let err = decode_response_dispatch(dispatch).expect_err("json transport codec should be rejected");
        assert!(err.to_string().contains("unsupported response dispatch codec"));
    }

    #[test]
    fn response_dispatch_prefers_origin_namespace_from_request_transport_metadata() {
        let mut request = Request::new("http://example.com", "GET").with_priority(Priority::Normal);
        request.account = "account-a".to_string();
        request.platform = "platform-a".to_string();
        request.module = "catalog".to_string();
        let request_dispatch =
            build_request_dispatch(&request, "origin-ns").expect("request dispatch should build");
        let request = decode_request_dispatch(request_dispatch).expect("request dispatch should decode");

        let response = Response {
            id: request.id,
            platform: request.platform.clone(),
            account: request.account.clone(),
            module: request.module.clone(),
            status_code: 200,
            cookies: Default::default(),
            content: b"ok".to_vec(),
            storage_path: None,
            headers: vec![("content-type".to_string(), "text/plain".to_string())],
            task_retry_times: request.task_retry_times,
            metadata: request.meta.clone(),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: request.context.clone(),
            run_id: request.run_id,
            prefix_request: request.prefix_request,
            request_hash: None,
            priority: request.priority,
        };

        let dispatch = build_response_dispatch(&response, "download-pool")
            .expect("response dispatch should build");

        assert_eq!(dispatch.routing.namespace, "origin-ns");
    }

    #[test]
    fn response_dispatch_round_trip_preserves_response_cache_metadata() {
        let response = Response {
            id: Uuid::now_v7(),
            platform: "platform-a".to_string(),
            account: "account-a".to_string(),
            module: "catalog".to_string(),
            status_code: 200,
            cookies: Default::default(),
            content: b"ok".to_vec(),
            storage_path: None,
            headers: vec![("content-type".to_string(), "text/plain".to_string())],
            task_retry_times: 1,
            metadata: crate::common::model::meta::MetaData::default()
                .add_trait_config("response_cache_owner_namespace", "demo")
                .add_trait_config("response_cache_owner_node_id", "node-a")
                .add_trait_config("response_cache_lookup_key", "cache-key-1"),
            download_middleware: Vec::new(),
            data_middleware: Vec::new(),
            task_finished: false,
            context: ExecutionMark::default(),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            request_hash: Some("cache-key-1".to_string()),
            priority: Priority::Normal,
        };

        let dispatch = build_response_dispatch(&response, "demo").expect("dispatch should build");
        let decoded = decode_response_dispatch(dispatch).expect("dispatch should decode");

        assert_eq!(
            decoded
                .metadata
                .get_trait_config::<String>("response_cache_owner_namespace")
                .as_deref(),
            Some("demo")
        );
        assert_eq!(
            decoded
                .metadata
                .get_trait_config::<String>("response_cache_owner_node_id")
                .as_deref(),
            Some("node-a")
        );
        assert_eq!(
            decoded
                .metadata
                .get_trait_config::<String>("response_cache_lookup_key")
                .as_deref(),
            Some("cache-key-1")
        );
    }
}