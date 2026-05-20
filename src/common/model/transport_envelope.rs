use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

use crate::common::model::{
    ExecutionMark, ExecutionMeta, NodeDispatch, NodeInput, PayloadCodec, RequestContextRef,
    RoutingMeta, RuntimeNodeRoutingHint, TypedEnvelope,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueueTopicKind {
    Task,
    Request,
    Response,
    ParserTask,
    Error,
    Dlq,
}

impl QueueTopicKind {
    pub fn from_topic_name(topic: &str) -> Option<Self> {
        let normalized = topic
            .replace('{', "")
            .replace('}', "")
            .trim_end_matches(":dlq")
            .trim_end_matches("-dlq")
            .to_string();

        let tail = normalized.rsplit(':').next().unwrap_or(normalized.as_str());
        let base = tail
            .strip_suffix("-high")
            .or_else(|| tail.strip_suffix("-normal"))
            .or_else(|| tail.strip_suffix("-low"))
            .unwrap_or(tail);

        if base.ends_with("parser_task") {
            Some(Self::ParserTask)
        } else if base.ends_with("error_task") {
            Some(Self::Error)
        } else if base.ends_with("response") {
            Some(Self::Response)
        } else if base.ends_with("request") {
            Some(Self::Request)
        } else if base.ends_with("task") {
            Some(Self::Task)
        } else if topic.contains("dlq") {
            Some(Self::Dlq)
        } else {
            None
        }
    }

    pub fn schema_id(self) -> &'static str {
        match self {
            Self::Task => "queue.task",
            Self::Request => "queue.request",
            Self::Response => "queue.response",
            Self::ParserTask => "queue.parser_task",
            Self::Error => "queue.error_task",
            Self::Dlq => "queue.dlq",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineStage {
    Task,
    Request,
    Response,
    ParserTask,
    Error,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransportEnvelopeError {
    #[error("queue envelope schema id must not be empty")]
    EmptySchemaId,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueEnvelope {
    pub schema_id: String,
    pub schema_version: u16,
    pub codec: PayloadCodec,
    #[serde(default)]
    pub payload: Vec<u8>,
}

impl QueueEnvelope {
    pub fn new(
        schema_id: impl Into<String>,
        schema_version: u16,
        codec: PayloadCodec,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Self, TransportEnvelopeError> {
        let schema_id = schema_id.into();
        if schema_id.is_empty() {
            return Err(TransportEnvelopeError::EmptySchemaId);
        }

        Ok(Self {
            schema_id,
            schema_version,
            codec,
            payload: payload.into(),
        })
    }

    pub fn from_typed(envelope: TypedEnvelope) -> Result<Self, TransportEnvelopeError> {
        Self::new(
            envelope.schema_id,
            envelope.schema_version,
            envelope.codec,
            envelope.bytes,
        )
    }

    pub fn into_typed(self) -> TypedEnvelope {
        TypedEnvelope::new(
            self.schema_id,
            self.schema_version,
            self.codec,
            self.payload,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskDispatchEnvelope {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    pub input: NodeInput,
}

impl TaskDispatchEnvelope {
    pub fn new(routing: RoutingMeta, exec: ExecutionMeta, input: NodeInput) -> Self {
        Self {
            routing,
            exec,
            input,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestDispatchEnvelope {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    #[serde(default)]
    pub context: Option<RequestContextRef>,
    pub request: TypedEnvelope,
}

impl RequestDispatchEnvelope {
    pub fn new(
        routing: RoutingMeta,
        exec: ExecutionMeta,
        request: TypedEnvelope,
    ) -> Self {
        Self {
            routing,
            exec,
            context: None,
            request,
        }
    }

    pub fn with_context(mut self, context: RequestContextRef) -> Self {
        self.context = Some(context);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseDispatchEnvelope {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    pub response: TypedEnvelope,
}

impl ResponseDispatchEnvelope {
    pub fn new(routing: RoutingMeta, exec: ExecutionMeta, response: TypedEnvelope) -> Self {
        Self {
            routing,
            exec,
            response,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParserDispatchContext {
    #[serde(default)]
    pub modules: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub context: ExecutionMark,
    #[serde(default)]
    pub prefix_request_id: Option<Uuid>,
    #[serde(default)]
    pub runtime_node: Option<RuntimeNodeRoutingHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeDispatchEnvelope {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    pub dispatch: NodeDispatch,
    #[serde(default)]
    pub parser_context: Option<ParserDispatchContext>,
}

impl NodeDispatchEnvelope {
    pub fn new(routing: RoutingMeta, exec: ExecutionMeta, dispatch: NodeDispatch) -> Self {
        Self {
            routing,
            exec,
            dispatch,
            parser_context: None,
        }
    }

    pub fn with_parser_context(mut self, parser_context: ParserDispatchContext) -> Self {
        self.parser_context = Some(parser_context);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorDispatchContext {
    #[serde(default)]
    pub modules: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub context: ExecutionMark,
    #[serde(default)]
    pub prefix_request_id: Option<Uuid>,
    #[serde(default)]
    pub runtime_node: Option<RuntimeNodeRoutingHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeErrorEnvelope {
    pub routing: RoutingMeta,
    pub exec: ExecutionMeta,
    pub stage: PipelineStage,
    pub error_message: String,
    pub error_code: Option<String>,
    #[serde(default)]
    pub detail: Option<TypedEnvelope>,
    #[serde(default)]
    pub error_context: Option<ErrorDispatchContext>,
}

impl NodeErrorEnvelope {
    pub fn new(
        routing: RoutingMeta,
        exec: ExecutionMeta,
        stage: PipelineStage,
        error_message: impl Into<String>,
    ) -> Self {
        Self {
            routing,
            exec,
            stage,
            error_message: error_message.into(),
            error_code: None,
            detail: None,
            error_context: None,
        }
    }

    pub fn with_error_code(mut self, error_code: impl Into<String>) -> Self {
        self.error_code = Some(error_code.into());
        self
    }

    pub fn with_detail(mut self, detail: TypedEnvelope) -> Self {
        self.detail = Some(detail);
        self
    }

    pub fn with_error_context(mut self, error_context: ErrorDispatchContext) -> Self {
        self.error_context = Some(error_context);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeadLetterEnvelope {
    pub id: Uuid,
    pub namespace: String,
    pub topic: QueueTopicKind,
    pub source_topic: String,
    pub source_message_id: String,
    pub reason: String,
    pub attempt: u32,
    pub failed_at_ms: i64,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    pub payload: QueueEnvelope,
}

impl DeadLetterEnvelope {
    pub fn new(
        namespace: impl Into<String>,
        topic: QueueTopicKind,
        source_topic: impl Into<String>,
        source_message_id: impl Into<String>,
        reason: impl Into<String>,
        attempt: u32,
        failed_at_ms: i64,
        payload: QueueEnvelope,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            namespace: namespace.into(),
            topic,
            source_topic: source_topic.into(),
            source_message_id: source_message_id.into(),
            reason: reason.into(),
            attempt,
            failed_at_ms,
            headers: HashMap::new(),
            payload,
        }
    }

    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::{ExecutionMeta, NodeInput, PayloadCodec, Priority, RoutingMeta};

    fn sample_routing() -> RoutingMeta {
        RoutingMeta {
            namespace: "demo".to_string(),
            account: "account-a".to_string(),
            platform: "platform-x".to_string(),
            module: "catalog".to_string(),
            node_key: "page".to_string(),
            run_id: Uuid::now_v7(),
            request_id: Uuid::now_v7(),
            parent_request_id: None,
            priority: Priority::Normal,
        }
    }

    fn sample_input() -> NodeInput {
        NodeInput::new(
            "detail",
            TypedEnvelope::new("node.payload", 1, PayloadCodec::MsgPack, vec![1_u8, 2_u8]),
        )
        .from_source("page")
    }

    #[test]
    fn queue_envelope_round_trips_with_typed_envelope() {
        let typed = TypedEnvelope::new(
            "task.dispatch",
            2,
            PayloadCodec::MsgPack,
            vec![7_u8, 8_u8, 9_u8],
        );

        let queue = QueueEnvelope::from_typed(typed.clone()).expect("queue envelope should build");
        assert_eq!(queue.schema_id, "task.dispatch");
        assert_eq!(queue.schema_version, 2);
        assert_eq!(queue.codec, PayloadCodec::MsgPack);
        assert_eq!(queue.payload, vec![7_u8, 8_u8, 9_u8]);
        assert_eq!(queue.clone().into_typed(), typed);
    }

    #[test]
    fn queue_envelope_rejects_empty_schema_id() {
        let result = QueueEnvelope::new("", 1, PayloadCodec::Json, Vec::<u8>::new());
        assert_eq!(result, Err(TransportEnvelopeError::EmptySchemaId));
    }

    #[test]
    fn task_and_node_dispatch_envelopes_preserve_context() {
        let routing = sample_routing();
        let exec = ExecutionMeta {
            profile_version: 5,
            ..ExecutionMeta::default()
        };
        let input = sample_input();
        let task_envelope = TaskDispatchEnvelope::new(routing.clone(), exec.clone(), input.clone());
        let dispatch = NodeDispatch::new("detail", input.clone());
        let parser_envelope =
            NodeDispatchEnvelope::new(routing.clone(), exec.clone(), dispatch.clone());

        assert_eq!(task_envelope.routing, routing);
        assert_eq!(task_envelope.exec.profile_version, 5);
        assert_eq!(task_envelope.input, input);
        assert_eq!(parser_envelope.dispatch, dispatch);
        assert_eq!(parser_envelope.routing.module, "catalog");
        assert!(parser_envelope.parser_context.is_none());
    }

    #[test]
    fn request_and_response_dispatch_envelopes_preserve_context() {
        let routing = sample_routing();
        let exec = ExecutionMeta {
            retry_count: 2,
            profile_version: 7,
            ..ExecutionMeta::default()
        };
        let request = TypedEnvelope::new(
            "transport.request",
            1,
            PayloadCodec::MsgPack,
            vec![1_u8, 2_u8],
        );
        let response = TypedEnvelope::new(
            "transport.response",
            1,
            PayloadCodec::MsgPack,
            vec![3_u8, 4_u8],
        );

        let request_envelope = RequestDispatchEnvelope::new(routing.clone(), exec.clone(), request.clone());
        let response_envelope =
            ResponseDispatchEnvelope::new(routing.clone(), exec.clone(), response.clone());

        assert_eq!(request_envelope.routing, routing);
        assert_eq!(request_envelope.exec.retry_count, 2);
        assert_eq!(request_envelope.request, request);
        assert_eq!(response_envelope.exec.profile_version, 7);
        assert_eq!(response_envelope.response, response);
    }

    #[test]
    fn dead_letter_envelope_wraps_failed_queue_payload() {
        let payload = QueueEnvelope::new(
            "node.error",
            1,
            PayloadCodec::MsgPack,
            vec![4_u8, 5_u8, 6_u8],
        )
        .expect("queue envelope should build");
        let dead_letter = DeadLetterEnvelope::new(
            "demo",
            QueueTopicKind::Error,
            "error_task-normal",
            "msg-1",
            "deserialization failed",
            3,
            1_700_000_000_000,
            payload.clone(),
        );

        assert_eq!(dead_letter.namespace, "demo");
        assert_eq!(dead_letter.topic, QueueTopicKind::Error);
        assert_eq!(dead_letter.source_topic, "error_task-normal");
        assert_eq!(dead_letter.source_message_id, "msg-1");
        assert_eq!(dead_letter.reason, "deserialization failed");
        assert_eq!(dead_letter.attempt, 3);
        assert_eq!(dead_letter.payload, payload);
    }

    #[test]
    fn queue_topic_kind_parses_topic_names() {
        assert_eq!(
            QueueTopicKind::from_topic_name("request-high"),
            Some(QueueTopicKind::Request)
        );
        assert_eq!(
            QueueTopicKind::from_topic_name("{demo:parser_task-low}:dlq"),
            Some(QueueTopicKind::ParserTask)
        );
        assert_eq!(
            QueueTopicKind::from_topic_name("demo-response-normal"),
            Some(QueueTopicKind::Response)
        );
    }

    #[test]
    fn node_error_envelope_tracks_stage_and_code() {
        let error = NodeErrorEnvelope::new(
            sample_routing(),
            ExecutionMeta::default(),
            PipelineStage::ParserTask,
            "parser failed",
        )
        .with_error_code("parse_failed");

        assert_eq!(error.stage, PipelineStage::ParserTask);
        assert_eq!(error.error_message, "parser failed");
        assert_eq!(error.error_code.as_deref(), Some("parse_failed"));
        assert!(error.detail.is_none());
        assert!(error.error_context.is_none());
    }

    #[test]
    fn node_dispatch_and_error_context_store_typed_parser_fields() {
        let parser_context = ParserDispatchContext {
            modules: Some(vec!["catalog".to_string(), "detail".to_string()]),
            metadata: serde_json::Map::from_iter([(
                "page".to_string(),
                serde_json::Value::from(3),
            )]),
            context: ExecutionMark::default().with_node_id("detail"),
            prefix_request_id: Some(Uuid::now_v7()),
            runtime_node: Some(
                RuntimeNodeRoutingHint::new("detail")
                    .with_placement(crate::schedule::dag::NodePlacement::remote("wg-parser"))
                    .with_policy(crate::schedule::dag::DagNodeExecutionPolicy {
                        max_retries: 2,
                        timeout_ms: Some(1500),
                        retry_backoff_ms: 250,
                        idempotency_key: Some("detail-route".to_string()),
                        retry_mode: crate::schedule::dag::DagNodeRetryMode::RetryableOnly,
                        circuit_breaker_failure_threshold: None,
                        circuit_breaker_open_ms: 0,
                    }),
            ),
        };
        let parser_envelope = NodeDispatchEnvelope::new(
            sample_routing(),
            ExecutionMeta::default(),
            NodeDispatch::new("detail", sample_input()),
        )
        .with_parser_context(parser_context.clone());

        assert_eq!(parser_envelope.parser_context.as_ref(), Some(&parser_context));

        let error_context = ErrorDispatchContext {
            modules: parser_context.modules.clone(),
            metadata: parser_context.metadata.clone(),
            context: parser_context.context.clone(),
            prefix_request_id: parser_context.prefix_request_id,
            runtime_node: parser_context.runtime_node.clone(),
        };
        let error_envelope = NodeErrorEnvelope::new(
            sample_routing(),
            ExecutionMeta::default(),
            PipelineStage::Error,
            "boom",
        )
        .with_error_context(error_context.clone());

        assert_eq!(error_envelope.error_context.as_ref(), Some(&error_context));
    }
}
