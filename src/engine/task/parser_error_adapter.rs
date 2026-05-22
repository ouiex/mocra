use async_trait::async_trait;
use serde_json::{Map, Value};
use std::sync::Arc;
use uuid::Uuid;

use crate::common::interface::storage::{BlobStorage, Offloadable};
use crate::common::model::data::DataEvent;
use crate::common::model::{
    ErrorDispatchContext, ExecutionMeta, NodeDispatch, NodeDispatchEnvelope, NodeErrorEnvelope,
    NodeInput, ParserDispatchContext, PayloadCodec, PipelineStage, Priority, RoutingMeta,
    TypedEnvelope, extract_runtime_node_hint, insert_runtime_node_hint,
};
use crate::errors::{Error, ErrorKind, Result};
use crate::queue::Identifiable;

const TRANSPORT_PARSER_DISPATCH_SCHEMA_ID: &str = "transport.parser_dispatch";
const TRANSPORT_PARSER_NODE: &str = "transport_parser";
const TRANSPORT_ERROR_NODE: &str = "transport_error";

#[derive(Debug, Clone)]
pub struct ParserDispatchSeed {
    pub request_id: Uuid,
    pub task_model: crate::common::model::message::TaskEvent,
    pub timestamp: u64,
    pub metadata: Map<String, Value>,
    pub context: crate::common::model::ExecutionMark,
    pub run_id: Uuid,
    pub prefix_request: Uuid,
    pub dispatch: Option<NodeDispatch>,
}

impl ParserDispatchSeed {
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.task_model.account, self.task_model.platform)
    }
}

#[derive(Debug, Clone)]
pub struct ErrorEnvelopeSeed {
    pub request_id: Uuid,
    pub task_model: crate::common::model::message::TaskEvent,
    pub error_message: String,
    pub timestamp: u64,
    pub metadata: Map<String, Value>,
    pub context: crate::common::model::ExecutionMark,
    pub run_id: Uuid,
    pub prefix_request: Uuid,
}

impl ErrorEnvelopeSeed {
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.task_model.account, self.task_model.platform)
    }
}

#[derive(Debug, Clone, Default)]
pub struct TypedParserOutput {
    pub data: Vec<DataEvent>,
    pub next_dispatches: Vec<ParserDispatchSeed>,
    pub error: Option<ErrorEnvelopeSeed>,
    pub stop: bool,
}

impl TypedParserOutput {
    pub fn with_next_dispatch(mut self, dispatch: ParserDispatchSeed) -> Self {
        self.next_dispatches.push(dispatch);
        self
    }

    pub fn with_error(mut self, error: ErrorEnvelopeSeed) -> Self {
        self.error = Some(error);
        self
    }

    pub fn with_stop(mut self) -> Self {
        self.stop = true;
        self
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn transport_node_key(context: &crate::common::model::ExecutionMark, fallback: &str) -> String {
    context
        .node_id
        .clone()
        .or_else(|| context.step_idx.map(|step_idx| format!("step_{step_idx}")))
        .unwrap_or_else(|| fallback.to_string())
}

fn transport_parent_request(prefix_request: uuid::Uuid) -> Option<uuid::Uuid> {
    (!prefix_request.is_nil()).then_some(prefix_request)
}

fn task_module_name(task: &crate::common::model::message::TaskEvent) -> String {
    task.module
        .as_ref()
        .and_then(|modules| modules.first())
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

fn task_timestamp_ms(timestamp: u64) -> i64 {
    if timestamp > 1_000_000_000_000 {
        timestamp as i64
    } else {
        (timestamp as i64) * 1000
    }
}

fn task_timestamp_secs(timestamp_ms: i64) -> u64 {
    if timestamp_ms > 1_000_000_000_000 {
        (timestamp_ms / 1000).max(0) as u64
    } else {
        timestamp_ms.max(0) as u64
    }
}

pub fn build_parser_dispatch_from_seed(
    seed: &ParserDispatchSeed,
    namespace: impl Into<String>,
) -> Result<NodeDispatchEnvelope> {
    let target_node = transport_node_key(&seed.context, TRANSPORT_PARSER_NODE);
    let dispatch = seed.dispatch.clone().unwrap_or_else(|| {
        let payload = TypedEnvelope::new(
            TRANSPORT_PARSER_DISPATCH_SCHEMA_ID,
            1,
            PayloadCodec::Json,
            b"null".to_vec(),
        );
        NodeDispatch::new(
            target_node.clone(),
            NodeInput::new(target_node.clone(), payload),
        )
    });

    Ok(NodeDispatchEnvelope::new(
        RoutingMeta {
            namespace: namespace.into(),
            account: seed.task_model.account.clone(),
            platform: seed.task_model.platform.clone(),
            module: task_module_name(&seed.task_model),
            node_key: target_node.clone(),
            run_id: seed.run_id,
            request_id: seed.request_id,
            parent_request_id: transport_parent_request(seed.prefix_request),
            priority: seed.task_model.priority,
        },
        ExecutionMeta {
            created_at_ms: task_timestamp_ms(seed.timestamp),
            updated_at_ms: now_ms(),
            ..ExecutionMeta::default()
        },
        dispatch,
    )
    .with_parser_context(ParserDispatchContext {
        modules: seed.task_model.module.clone(),
        metadata: seed.metadata.clone(),
        context: seed.context.clone(),
        prefix_request_id: transport_parent_request(seed.prefix_request),
        runtime_node: extract_runtime_node_hint(&seed.metadata),
    }))
}

pub fn extract_parser_dispatch_seed(dispatch: &NodeDispatchEnvelope) -> Result<ParserDispatchSeed> {
    let Some(parser_context) = dispatch.parser_context.as_ref() else {
        return Err(Error::new(
            ErrorKind::Queue,
            Some(std::io::Error::other(
                "missing parser dispatch context for typed parser seed extraction",
            )),
        ));
    };

    let mut metadata = parser_context.metadata.clone();
    if let Some(runtime_node) = parser_context.runtime_node.as_ref() {
        insert_runtime_node_hint(&mut metadata, runtime_node);
    }
    Ok(ParserDispatchSeed {
        request_id: dispatch.routing.request_id,
        task_model: crate::common::model::message::TaskEvent {
            account: dispatch.routing.account.clone(),
            platform: dispatch.routing.platform.clone(),
            module: parser_context
                .modules
                .clone()
                .or_else(|| Some(vec![dispatch.routing.module.clone()])),
            priority: dispatch.routing.priority,
            run_id: dispatch.routing.run_id,
        },
        timestamp: task_timestamp_secs(dispatch.exec.created_at_ms),
        metadata,
        context: parser_context.context.clone(),
        run_id: dispatch.routing.run_id,
        prefix_request: parser_context
            .prefix_request_id
            .or(dispatch.routing.parent_request_id)
            .unwrap_or_default(),
        dispatch: Some(dispatch.dispatch.clone()),
    })
}

pub fn build_error_envelope_from_seed(
    seed: &ErrorEnvelopeSeed,
    namespace: impl Into<String>,
) -> Result<NodeErrorEnvelope> {
    Ok(NodeErrorEnvelope::new(
        RoutingMeta {
            namespace: namespace.into(),
            account: seed.task_model.account.clone(),
            platform: seed.task_model.platform.clone(),
            module: task_module_name(&seed.task_model),
            node_key: transport_node_key(&seed.context, TRANSPORT_ERROR_NODE),
            run_id: seed.run_id,
            request_id: seed.request_id,
            parent_request_id: transport_parent_request(seed.prefix_request),
            priority: seed.task_model.priority,
        },
        ExecutionMeta {
            created_at_ms: task_timestamp_ms(seed.timestamp),
            updated_at_ms: now_ms(),
            ..ExecutionMeta::default()
        },
        PipelineStage::Error,
        seed.error_message.clone(),
    )
    .with_error_context(ErrorDispatchContext {
        modules: seed.task_model.module.clone(),
        metadata: seed.metadata.clone(),
        context: seed.context.clone(),
        prefix_request_id: transport_parent_request(seed.prefix_request),
        runtime_node: extract_runtime_node_hint(&seed.metadata),
    }))
}

pub fn extract_error_envelope_seed(envelope: &NodeErrorEnvelope) -> Result<ErrorEnvelopeSeed> {
    let Some(error_context) = envelope.error_context.as_ref() else {
        return Err(Error::new(
            ErrorKind::Queue,
            Some(std::io::Error::other(
                "missing error dispatch context for typed error seed extraction",
            )),
        ));
    };

    let mut metadata = error_context.metadata.clone();
    if let Some(runtime_node) = error_context.runtime_node.as_ref() {
        insert_runtime_node_hint(&mut metadata, runtime_node);
    }
    Ok(ErrorEnvelopeSeed {
        request_id: envelope.routing.request_id,
        task_model: crate::common::model::message::TaskEvent {
            account: envelope.routing.account.clone(),
            platform: envelope.routing.platform.clone(),
            module: error_context
                .modules
                .clone()
                .or_else(|| Some(vec![envelope.routing.module.clone()])),
            priority: envelope.routing.priority,
            run_id: envelope.routing.run_id,
        },
        error_message: envelope.error_message.clone(),
        timestamp: task_timestamp_secs(envelope.exec.created_at_ms),
        metadata,
        context: error_context.context.clone(),
        run_id: envelope.routing.run_id,
        prefix_request: error_context
            .prefix_request_id
            .or(envelope.routing.parent_request_id)
            .unwrap_or_default(),
    })
}

impl Identifiable for NodeDispatchEnvelope {
    fn get_id(&self) -> String {
        self.routing.request_id.to_string()
    }
}

impl Identifiable for NodeErrorEnvelope {
    fn get_id(&self) -> String {
        self.routing.request_id.to_string()
    }
}

impl crate::common::model::Prioritizable for NodeDispatchEnvelope {
    fn get_priority(&self) -> Priority {
        self.routing.priority
    }
}

impl crate::common::model::Prioritizable for NodeErrorEnvelope {
    fn get_priority(&self) -> Priority {
        self.routing.priority
    }
}

#[async_trait]
impl Offloadable for NodeDispatchEnvelope {
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
impl Offloadable for NodeErrorEnvelope {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::message::TaskEvent;
    use crate::common::model::{
        ExecutionMark, RuntimeNodeRoutingHint, extract_runtime_node_hint, insert_runtime_node_hint,
    };

    #[test]
    fn parser_task_dispatch_round_trips_transport_task() {
        let mut metadata = serde_json::Map::new();
        insert_runtime_node_hint(
            &mut metadata,
            &RuntimeNodeRoutingHint::new("detail")
                .with_placement(crate::schedule::dag::NodePlacement::remote("wg-parser")),
        );
        let seed = ParserDispatchSeed {
            request_id: uuid::Uuid::now_v7(),
            task_model: TaskEvent {
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: Some(vec!["catalog".to_string()]),
                priority: Priority::High,
                run_id: uuid::Uuid::now_v7(),
            },
            timestamp: 1_700_000_000,
            metadata,
            context: ExecutionMark::default().with_node_id("detail"),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
            dispatch: None,
        };

        let envelope =
            build_parser_dispatch_from_seed(&seed, "demo").expect("dispatch should build");
        assert_eq!(envelope.routing.namespace, "demo");
        assert_eq!(envelope.routing.node_key, "detail");
        assert!(envelope.parser_context.is_some());
        assert_eq!(
            envelope.dispatch.input.payload.schema_id,
            TRANSPORT_PARSER_DISPATCH_SCHEMA_ID
        );

        let seed = extract_parser_dispatch_seed(&envelope).expect("seed should extract");
        assert_eq!(seed.task_model.account, "account-a");
        assert_eq!(seed.context.node_id.as_deref(), Some("detail"));
        assert_eq!(
            envelope
                .parser_context
                .as_ref()
                .and_then(|context| context.runtime_node.as_ref())
                .map(|hint| hint.node_key.as_str()),
            Some("detail")
        );
        assert_eq!(
            extract_runtime_node_hint(&seed.metadata)
                .as_ref()
                .and_then(|hint| hint.placement.as_ref())
                .map(|placement| placement.placement_name()),
            Some("remote")
        );

        assert_eq!(seed.task_model.module, Some(vec!["catalog".to_string()]));
    }

    #[test]
    fn error_task_envelope_round_trips_transport_task() {
        let mut metadata = serde_json::Map::new();
        insert_runtime_node_hint(
            &mut metadata,
            &RuntimeNodeRoutingHint::new("transport_error")
                .with_placement(crate::schedule::dag::NodePlacement::local()),
        );
        let seed = ErrorEnvelopeSeed {
            request_id: uuid::Uuid::now_v7(),
            task_model: TaskEvent {
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: Some(vec!["catalog".to_string()]),
                priority: Priority::Normal,
                run_id: uuid::Uuid::now_v7(),
            },
            error_message: "boom".to_string(),
            timestamp: 1_700_000_000,
            metadata,
            context: ExecutionMark::default(),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
        };

        let envelope =
            build_error_envelope_from_seed(&seed, "demo").expect("envelope should build");
        assert_eq!(envelope.routing.namespace, "demo");
        assert_eq!(envelope.error_message, "boom");
        assert!(envelope.detail.is_none());
        assert!(envelope.error_context.is_some());

        let seed = extract_error_envelope_seed(&envelope).expect("seed should extract");
        assert_eq!(seed.task_model.account, "account-a");
        assert_eq!(seed.error_message, "boom");
        assert_eq!(
            envelope
                .error_context
                .as_ref()
                .and_then(|context| context.runtime_node.as_ref())
                .map(|hint| hint.node_key.as_str()),
            Some("transport_error")
        );
        assert_eq!(
            extract_runtime_node_hint(&seed.metadata)
                .as_ref()
                .and_then(|hint| hint.placement.as_ref())
                .map(|placement| placement.placement_name()),
            Some("local")
        );

        assert_eq!(seed.task_model.module, Some(vec!["catalog".to_string()]));
    }

    #[test]
    fn extract_parser_seed_prefers_typed_context_when_present() {
        let seed = ParserDispatchSeed {
            request_id: uuid::Uuid::now_v7(),
            task_model: TaskEvent {
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: Some(vec!["catalog".to_string()]),
                priority: Priority::Normal,
                run_id: uuid::Uuid::now_v7(),
            },
            timestamp: 1_700_000_000,
            metadata: serde_json::Map::new(),
            context: ExecutionMark::default().with_node_id("typed_node"),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
            dispatch: None,
        };

        let mut envelope =
            build_parser_dispatch_from_seed(&seed, "demo").expect("dispatch should build");
        envelope.dispatch.input.payload.bytes = b"not-msgpack".to_vec();

        let seed = extract_parser_dispatch_seed(&envelope).expect("typed context should decode");
        assert_eq!(seed.context.node_id.as_deref(), Some("typed_node"));
    }

    #[test]
    fn extract_parser_seed_requires_typed_context() {
        let seed = ParserDispatchSeed {
            request_id: uuid::Uuid::now_v7(),
            task_model: TaskEvent {
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: Some(vec!["catalog".to_string()]),
                priority: Priority::Normal,
                run_id: uuid::Uuid::now_v7(),
            },
            timestamp: 1_700_000_000,
            metadata: serde_json::Map::new(),
            context: ExecutionMark::default().with_node_id("typed_node"),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
            dispatch: None,
        };

        let mut envelope =
            build_parser_dispatch_from_seed(&seed, "demo").expect("dispatch should build");
        envelope.parser_context = None;

        let err =
            extract_parser_dispatch_seed(&envelope).expect_err("missing typed context should fail");
        assert!(err.to_string().contains("missing parser dispatch context"));
    }

    #[test]
    fn extract_error_seed_prefers_typed_context_when_present() {
        let seed = ErrorEnvelopeSeed {
            request_id: uuid::Uuid::now_v7(),
            task_model: TaskEvent {
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: Some(vec!["catalog".to_string()]),
                priority: Priority::Normal,
                run_id: uuid::Uuid::now_v7(),
            },
            error_message: "boom".to_string(),
            timestamp: 1_700_000_000,
            metadata: serde_json::Map::new(),
            context: ExecutionMark::default().with_node_id("typed_error_node"),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
        };

        let mut envelope =
            build_error_envelope_from_seed(&seed, "demo").expect("envelope should build");
        envelope.detail = Some(TypedEnvelope::new(
            "transport.error_detail",
            1,
            PayloadCodec::Json,
            b"not-json".to_vec(),
        ));

        let seed = extract_error_envelope_seed(&envelope).expect("typed context should decode");
        assert_eq!(seed.context.node_id.as_deref(), Some("typed_error_node"));
    }

    #[test]
    fn extract_error_seed_requires_typed_context() {
        let seed = ErrorEnvelopeSeed {
            request_id: uuid::Uuid::now_v7(),
            task_model: TaskEvent {
                account: "account-a".to_string(),
                platform: "platform-x".to_string(),
                module: Some(vec!["catalog".to_string()]),
                priority: Priority::Normal,
                run_id: uuid::Uuid::now_v7(),
            },
            error_message: "boom".to_string(),
            timestamp: 1_700_000_000,
            metadata: serde_json::Map::new(),
            context: ExecutionMark::default().with_node_id("typed_error_node"),
            run_id: uuid::Uuid::now_v7(),
            prefix_request: uuid::Uuid::nil(),
        };

        let mut envelope =
            build_error_envelope_from_seed(&seed, "demo").expect("envelope should build");
        envelope.error_context = None;

        let err =
            extract_error_envelope_seed(&envelope).expect_err("missing typed context should fail");
        assert!(err.to_string().contains("missing error dispatch context"));
    }
}
