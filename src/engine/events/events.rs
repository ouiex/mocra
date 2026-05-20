use crate::common::model::message::TaskEvent;
use crate::common::model::{NodeDispatchEnvelope, NodeErrorEnvelope};
use crate::common::model::{Request, Response};
use crate::engine::task::module::Module;
use crate::errors::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Top-level domain that namespaces event families.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventDomain {
    Engine,
    System,
}

/// Canonical event type identifiers emitted by the engine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventType {
    TaskModel,
    ParserDispatch,
    ParserTaskProduced,
    ErrorTaskProduced,
    ModuleStepAdvanced,
    ModuleStepFallback,
    TaskTerminatedByThreshold,
    RequestPublish,
    RequestMiddleware,
    Download,
    ResponseMiddleware,
    ResponsePublish,
    ModuleGenerate,
    Parser,
    MiddlewareBefore,
    DataStore,
    SystemError,
    SystemHealth,
}

/// Lifecycle phase attached to each event emission.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventPhase {
    Started,
    Completed,
    Failed,
    Retry,
}

/// Error payload carried by failed events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventError {
    pub kind: ErrorKind,
    pub message: String,
}

/// Transport envelope used by the event bus and storage backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub domain: EventDomain,
    pub event_type: EventType,
    pub phase: EventPhase,
    pub payload: serde_json::Value,
    pub error: Option<EventError>,
    pub timestamp_ms: u128,
    pub trace_id: Option<String>,
}

impl EventDomain {
    /// Returns the canonical lowercase domain key.
    ///
    /// ```
    /// use mocra::engine::events::EventDomain;
    /// assert_eq!(EventDomain::Engine.as_str(), "engine");
    /// assert_eq!(EventDomain::System.as_str(), "system");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            EventDomain::Engine => "engine",
            EventDomain::System => "system",
        }
    }
}

impl EventType {
    /// Returns the canonical lowercase event type key.
    ///
    /// ```
    /// use mocra::engine::events::EventType;
    /// assert_eq!(EventType::RequestPublish.as_str(), "request_publish");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::TaskModel => "task_model",
            EventType::ParserDispatch => "parser_dispatch",
            EventType::ParserTaskProduced => "parser_task_produced",
            EventType::ErrorTaskProduced => "error_task_produced",
            EventType::ModuleStepAdvanced => "module_step_advanced",
            EventType::ModuleStepFallback => "module_step_fallback",
            EventType::TaskTerminatedByThreshold => "task_terminated_by_threshold",
            EventType::RequestPublish => "request_publish",
            EventType::RequestMiddleware => "request_middleware",
            EventType::Download => "download",
            EventType::ResponseMiddleware => "response_middleware",
            EventType::ResponsePublish => "response_publish",
            EventType::ModuleGenerate => "module_generate",
            EventType::Parser => "parser",
            EventType::MiddlewareBefore => "middleware_before",
            EventType::DataStore => "data_store",
            EventType::SystemError => "system_error",
            EventType::SystemHealth => "system_health",
        }
    }
}

impl EventPhase {
    /// Returns the canonical lowercase phase key.
    ///
    /// ```
    /// use mocra::engine::events::EventPhase;
    /// assert_eq!(EventPhase::Retry.as_str(), "retry");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            EventPhase::Started => "started",
            EventPhase::Completed => "completed",
            EventPhase::Failed => "failed",
            EventPhase::Retry => "retry",
        }
    }
}

impl EventEnvelope {
    pub fn engine<T: Serialize>(event_type: EventType, phase: EventPhase, payload: T) -> Self {
        Self {
            domain: EventDomain::Engine,
            event_type,
            phase,
            payload: serde_json::to_value(payload).unwrap_or_else(|_| serde_json::json!({})),
            error: None,
            timestamp_ms: Self::now_ms(),
            trace_id: None,
        }
    }

    pub fn engine_error<T: Serialize>(
        event_type: EventType,
        phase: EventPhase,
        payload: T,
        err: &Error,
    ) -> Self {
        Self {
            domain: EventDomain::Engine,
            event_type,
            phase,
            payload: serde_json::to_value(payload).unwrap_or_else(|_| serde_json::json!({})),
            error: Some(EventError {
                kind: err.kind().clone(),
                message: err.to_string(),
            }),
            timestamp_ms: Self::now_ms(),
            trace_id: None,
        }
    }

    pub fn system_error(message: impl Into<String>, phase: EventPhase) -> Self {
        Self {
            domain: EventDomain::System,
            event_type: EventType::SystemError,
            phase,
            payload: serde_json::json!({ "message": message.into() }),
            error: None,
            timestamp_ms: Self::now_ms(),
            trace_id: None,
        }
    }

    pub fn system_health(payload: HealthCheckEvent, phase: EventPhase) -> Self {
        Self {
            domain: EventDomain::System,
            event_type: EventType::SystemHealth,
            phase,
            payload: serde_json::to_value(payload).unwrap_or_else(|_| serde_json::json!({})),
            error: None,
            timestamp_ms: Self::now_ms(),
            trace_id: None,
        }
    }

    /// Builds the stable composite event key `domain.type.phase`.
    ///
    /// ```
    /// use mocra::engine::events::{EventEnvelope, EventPhase, EventType};
    /// let evt = EventEnvelope::engine(EventType::ParserDispatch, EventPhase::Completed, serde_json::json!({}));
    /// assert_eq!(evt.event_key(), "engine.parser_dispatch.completed");
    /// ```
    pub fn event_key(&self) -> String {
        format!(
            "{}.{}.{}",
            self.domain.as_str(),
            self.event_type.as_str(),
            self.phase.as_str()
        )
    }

    pub fn now_ms() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::{EventEnvelope, EventPhase, EventType, ParserDispatchEvent};
    use crate::common::model::{
        ErrorDispatchContext, ExecutionMark, ExecutionMeta, NodeDispatch, NodeDispatchEnvelope,
        NodeErrorEnvelope, NodeInput, ParserDispatchContext, PayloadCodec, PipelineStage,
        RoutingMeta, TypedEnvelope,
    };
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn threshold_termination_event_type_has_stable_name() {
        assert_eq!(
            EventType::TaskTerminatedByThreshold.as_str(),
            "task_terminated_by_threshold"
        );
    }

    #[test]
    fn task_model_chain_semantic_event_types_have_stable_names() {
        assert_eq!(
            EventType::ParserTaskProduced.as_str(),
            "parser_task_produced"
        );
        assert_eq!(EventType::ErrorTaskProduced.as_str(), "error_task_produced");
        assert_eq!(
            EventType::ModuleStepAdvanced.as_str(),
            "module_step_advanced"
        );
        assert_eq!(
            EventType::ModuleStepFallback.as_str(),
            "module_step_fallback"
        );
    }

    #[test]
    fn parser_dispatch_event_mapping_is_consistent_for_parser_and_error_inputs() {
        let run_id = Uuid::now_v7();
        let request_id = Uuid::now_v7();
        let parent_request_id = Uuid::now_v7();
        let routing = RoutingMeta {
            namespace: "ns".to_string(),
            account: "acc".to_string(),
            platform: "pf".to_string(),
            module: "m1".to_string(),
            node_key: "node_a".to_string(),
            run_id,
            request_id,
            parent_request_id: Some(parent_request_id),
            priority: Default::default(),
        };
        let exec = ExecutionMeta::default();
        let dispatch = NodeDispatch::new(
            "node_a",
            NodeInput::new(
                "node_a",
                TypedEnvelope::new("test.parser", 1, PayloadCodec::Json, b"null".to_vec()),
            ),
        );

        let parser_dispatch = NodeDispatchEnvelope::new(routing.clone(), exec.clone(), dispatch)
            .with_parser_context(ParserDispatchContext {
                modules: Some(vec!["m1".to_string()]),
                metadata: json!({"k":"v"}).as_object().cloned().unwrap_or_default(),
                context: ExecutionMark::default(),
                prefix_request_id: Some(parent_request_id),
                runtime_node: None,
            });
        let parser_evt = ParserDispatchEvent::from(&parser_dispatch);
        assert_eq!(parser_evt.account, "acc");
        assert_eq!(parser_evt.platform, "pf");
        assert_eq!(parser_evt.modules, Some(vec!["m1".to_string()]));

        let error_envelope = NodeErrorEnvelope::new(
            routing,
            exec,
            PipelineStage::ParserTask,
            "err",
        )
        .with_error_context(ErrorDispatchContext {
            modules: Some(vec!["m1".to_string()]),
            metadata: json!({"e":"x"}).as_object().cloned().unwrap_or_default(),
            context: ExecutionMark::default(),
            prefix_request_id: Some(parent_request_id),
            runtime_node: None,
        });
        let error_evt = ParserDispatchEvent::from(&error_envelope);
        assert_eq!(error_evt.account, "acc");
        assert_eq!(error_evt.platform, "pf");
        assert_eq!(error_evt.modules, Some(vec!["m1".to_string()]));
    }

    #[test]
    fn parser_dispatch_event_envelope_key_is_stable() {
        let payload = json!({"account":"acc","platform":"pf"});
        let evt = EventEnvelope::engine(EventType::ParserDispatch, EventPhase::Completed, payload);
        assert_eq!(evt.event_key(), "engine.parser_dispatch.completed");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskModelEvent {
    pub account: String,
    pub platform: String,
    pub modules: Option<Vec<String>>,
}
impl From<&TaskEvent> for TaskModelEvent {
    fn from(value: &TaskEvent) -> Self {
        TaskModelEvent {
            account: value.account.clone(),
            platform: value.platform.clone(),
            modules: None,
        }
    }
}

/// Compact event payload representing parser/error task-model inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserDispatchEvent {
    pub account: String,
    pub platform: String,
    pub modules: Option<Vec<String>>,
    #[serde(skip)]
    pub metadata: Option<serde_json::Value>,
}
impl From<&NodeDispatchEnvelope> for ParserDispatchEvent {
    fn from(value: &NodeDispatchEnvelope) -> Self {
        Self {
            account: value.routing.account.clone(),
            platform: value.routing.platform.clone(),
            modules: value
                .parser_context
                .as_ref()
                .and_then(|ctx| ctx.modules.clone())
                .or_else(|| Some(vec![value.routing.module.clone()])),
            metadata: value
                .parser_context
                .as_ref()
                .map(|ctx| serde_json::to_value(ctx.metadata.clone()).unwrap_or_default()),
        }
    }
}
impl From<&NodeErrorEnvelope> for ParserDispatchEvent {
    fn from(value: &NodeErrorEnvelope) -> Self {
        Self {
            account: value.routing.account.clone(),
            platform: value.routing.platform.clone(),
            modules: value
                .error_context
                .as_ref()
                .and_then(|ctx| ctx.modules.clone())
                .or_else(|| Some(vec![value.routing.module.clone()])),
            metadata: value
                .error_context
                .as_ref()
                .map(|ctx| serde_json::to_value(ctx.metadata.clone()).unwrap_or_default()),
        }
    }
}

/// Event payload emitted when a concrete module instance is generated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleGenerateEvent {
    pub account: String,
    pub platform: String,
    pub module: String,
}

impl From<&Module> for ModuleGenerateEvent {
    fn from(value: &Module) -> Self {
        Self {
            account: value.account.name.clone(),
            platform: value.platform.name.clone(),
            module: value.module.name().to_string(),
        }
    }
}

/// Event payload for outbound request publication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestEvent {
    pub request_id: Uuid,
    pub url: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub method: String,
    #[serde(skip)]
    pub metadata: serde_json::Value,
}

impl From<&Request> for RequestEvent {
    fn from(value: &Request) -> Self {
        RequestEvent {
            request_id: value.id,
            url: value.url.clone(),
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            method: value.method.clone(),
            metadata: serde_json::to_value(value.meta.clone()).unwrap_or_default(),
        }
    }
}

/// Event payload for download execution telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadEvent {
    pub request_id: Uuid,
    pub url: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub method: String,
    pub duration_ms: Option<u64>,
    pub response_size: Option<u32>,
    pub status_code: Option<u16>,
}
impl From<&Request> for DownloadEvent {
    fn from(value: &Request) -> Self {
        DownloadEvent {
            request_id: value.id,
            url: value.url.clone(),
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            method: value.method.clone(),
            duration_ms: None,
            response_size: None,
            status_code: None,
        }
    }
}

/// Event payload for parser execution telemetry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserEvent {
    pub account: String,
    pub platform: String,
    pub module: String,
    pub request_id: String,
}

impl From<&Response> for ParserEvent {
    fn from(value: &Response) -> Self {
        Self {
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            request_id: value.id.to_string(),
        }
    }
}

/// Event payload for data-middleware transformation stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMiddlewareEvent {
    pub account: String,
    pub platform: String,
    pub module: String,
    pub request_id: String,
    pub schema_size: usize,
    pub after_size: Option<usize>,
}
impl From<&crate::common::model::data::DataEvent> for DataMiddlewareEvent {
    fn from(value: &crate::common::model::data::DataEvent) -> Self {
        Self {
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            request_id: value.request_id.to_string(),
            schema_size: 0,
            after_size: None,
        }
    }
}

/// Event payload for final data-store persistence stages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataStoreEvent {
    pub account: String,
    pub platform: String,
    pub module: String,
    pub request_id: String,
    pub schema_size: (usize, usize),
    pub store_middleware: Option<String>,
}
impl From<&crate::common::model::data::DataEvent> for DataStoreEvent {
    fn from(value: &crate::common::model::data::DataEvent) -> Self {
        Self {
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            request_id: value.request_id.to_string(),
            schema_size: (0, 0),
            store_middleware: None,
        }
    }
}

/// Event payload for request middleware application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMiddlewareEvent {
    pub request_id: Uuid,
    pub url: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub method: String,
    pub middleware: Vec<String>,
}
impl From<&Request> for RequestMiddlewareEvent {
    fn from(value: &Request) -> Self {
        RequestMiddlewareEvent {
            request_id: value.id,
            url: value.url.clone(),
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            method: value.method.clone(),
            middleware: value.download_middleware.clone(),
        }
    }
}

/// Event payload for response middleware processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseEvent {
    pub response_id: Uuid,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub status_code: Option<u16>,
    pub middleware: Vec<String>,
}
impl From<&Response> for ResponseEvent {
    fn from(value: &Response) -> Self {
        ResponseEvent {
            response_id: value.id,
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            status_code: Some(value.status_code),
            middleware: value.data_middleware.clone(),
        }
    }
}

/// Event payload for periodic component health reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckEvent {
    pub component: String,
    pub status: String,
    pub metrics: serde_json::Value,
    pub timestamp: u64,
}
impl From<&Response> for ModuleGenerateEvent {
    fn from(value: &Response) -> Self {
        Self {
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
        }
    }
}
