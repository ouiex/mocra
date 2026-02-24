
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use common::model::{Request, Response};
use crate::task::module::Module;
use errors::{Error, ErrorKind};

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
    ParserTaskModel,
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
    /// use engine::events::EventDomain;
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
    /// use engine::events::EventType;
    /// assert_eq!(EventType::RequestPublish.as_str(), "request_publish");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::TaskModel => "task_model",
            EventType::ParserTaskModel => "parser_task_model",
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
    /// use engine::events::EventPhase;
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
    /// use engine::events::{EventEnvelope, EventPhase, EventType};
    /// let evt = EventEnvelope::engine(EventType::ParserTaskModel, EventPhase::Completed, serde_json::json!({}));
    /// assert_eq!(evt.event_key(), "engine.parser_task_model.completed");
    /// ```
    pub fn event_key(&self) -> String {
        format!("{}.{}.{}", self.domain.as_str(), self.event_type.as_str(), self.phase.as_str())
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
    use super::{EventEnvelope, EventPhase, EventType, ParserTaskModelEvent};
    use common::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
    use common::model::ExecutionMark;
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
        assert_eq!(EventType::ParserTaskProduced.as_str(), "parser_task_produced");
        assert_eq!(EventType::ErrorTaskProduced.as_str(), "error_task_produced");
        assert_eq!(EventType::ModuleStepAdvanced.as_str(), "module_step_advanced");
        assert_eq!(EventType::ModuleStepFallback.as_str(), "module_step_fallback");
    }

    #[test]
    fn parser_task_model_event_mapping_is_consistent_for_parser_and_error_inputs() {
        let run_id = Uuid::now_v7();
        let task_model = TaskModel {
            account: "acc".to_string(),
            platform: "pf".to_string(),
            module: Some(vec!["m1".to_string()]),
            priority: Default::default(),
            run_id,
        };

        let parser_task = ParserTaskModel {
            id: Uuid::now_v7(),
            account_task: task_model.clone(),
            timestamp: 0,
            metadata: json!({"k":"v"}),
            context: ExecutionMark::default(),
            run_id,
            prefix_request: Uuid::now_v7(),
        };
        let parser_evt = ParserTaskModelEvent::from(&parser_task);
        assert_eq!(parser_evt.account, "acc");
        assert_eq!(parser_evt.platform, "pf");
        assert_eq!(parser_evt.modules, Some(vec!["m1".to_string()]));

        let error_task = ErrorTaskModel {
            id: Uuid::now_v7(),
            account_task: task_model,
            error_msg: "err".to_string(),
            timestamp: 0,
            metadata: json!({"e":"x"}),
            context: ExecutionMark::default(),
            run_id,
            prefix_request: Uuid::now_v7(),
        };
        let error_evt = ParserTaskModelEvent::from(&error_task);
        assert_eq!(error_evt.account, "acc");
        assert_eq!(error_evt.platform, "pf");
        assert_eq!(error_evt.modules, Some(vec!["m1".to_string()]));
    }

    #[test]
    fn parser_task_model_event_envelope_key_is_stable() {
        let payload = json!({"account":"acc","platform":"pf"});
        let evt = EventEnvelope::engine(EventType::ParserTaskModel, EventPhase::Completed, payload);
        assert_eq!(evt.event_key(), "engine.parser_task_model.completed");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskModelEvent {
    pub account: String,
    pub platform: String,
    pub modules: Option<Vec<String>>,
    
}
impl From<&TaskModel> for TaskModelEvent {
    fn from(value: &TaskModel) -> Self {
        TaskModelEvent {
            account: value.account.clone(),
            platform: value.platform.clone(),
            modules: None,
        }
    }
}

/// Compact event payload representing parser/error task-model inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserTaskModelEvent {
    pub account: String,
    pub platform: String,
    pub modules: Option<Vec<String>>,
    #[serde(skip)]
    pub metadata: Option<serde_json::Value>,
}
impl From<&ParserTaskModel> for ParserTaskModelEvent {
    fn from(value: &ParserTaskModel) -> Self {
        ParserTaskModelEvent {
            account: value.account_task.account.clone(),
            platform: value.account_task.platform.clone(),
            modules: value.account_task.module.clone(),
            metadata: Some(serde_json::to_value(value.metadata.clone()).unwrap_or_default()),
        }
    }
}
impl From<&ErrorTaskModel> for ParserTaskModelEvent {
    fn from(value: &ErrorTaskModel) -> Self {
        Self {
            account: value.account_task.account.clone(),
            platform: value.account_task.platform.clone(),
            modules: value.account_task.module.clone(),
            metadata: Some(serde_json::to_value(value.metadata.clone()).unwrap_or_default()),
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
            module: value.module.name().clone(),
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
impl From<&common::model::data::Data> for DataMiddlewareEvent {
    fn from(value: &common::model::data::Data) -> Self {
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
impl From<&common::model::data::Data> for DataStoreEvent {
    fn from(value: &common::model::data::Data) -> Self {
        Self {
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            request_id: value.request_id.to_string(),
            schema_size: (0,0),
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
