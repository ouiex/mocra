use crate::common::interface::StoreTrait;
use crate::common::interface::storage::{Offloadable, BlobStorage};
use std::sync::Arc;
use async_trait::async_trait;
use crate::common::model::data::Data;
use crate::common::model::{ExecutionMark, Response};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum TopicType {
    /// Task queue.
    Task,
    /// Request queue.
    Request,
    /// Response queue (raw task output).
    Response,
    /// Parser task queue (processed results).
    ParserTask,
    /// Error queue (error details).
    Error,
}

impl TopicType {
    /// Returns topic suffix.
    pub(crate) fn suffix(&self) -> &'static str {
        match self {
            TopicType::Response => "response",
            TopicType::ParserTask => "parser_task",
            TopicType::Error => "error",
            TopicType::Task => "task",
            TopicType::Request => "request",
        }
    }
    pub fn get_name(&self, name: &str) -> String {
        format!("crawler-{}-{}", name, self.suffix())
    }
}

/// Parser task message model.
///
/// Used to create downstream tasks after parsing, or move to the next processing stage.
/// Contains task identity, metadata, execution context, and predecessor request reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserTaskModel {
    /// Unique identifier.
    pub id: Uuid,
    /// Associated account task information.
    pub account_task: TaskModel,
    /// Timestamp.
    pub timestamp: u64,
    /// Metadata (`ParserTaskModel.meta => Task.metadata => Context.meta.task_meta => Module.generate`).
    #[serde(with = "crate::common::model::serde_value::value")]
    pub metadata: serde_json::Value,
    /// Execution context (`ExecutionMark`).
    pub context: ExecutionMark,
    /// Run identifier (Run ID).
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
    /// Predecessor request identifier.
    pub prefix_request: Uuid,
}

#[async_trait]
impl Offloadable for ParserTaskModel {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
}

impl ParserTaskModel {
    /// Sets explicit execution context (`ExecutionMark`) for parser-chain execution.
    ///
    /// Typical uses:
    /// - Precisely control which step index target module starts from.
    /// - Combine with `stay_current_step` to prevent auto-advancing.
    pub fn with_context(mut self, ctx: ExecutionMark) -> Self {
        self.context = ctx;
        self
    }
    /// Marks this task to stay on the current parser node and avoid auto-advance.
    ///
    /// Behavior:
    /// - When parser returns `ParserTaskModel` and this is set, the chain loops on this step.
    /// - Otherwise, the loop is considered complete and processing continues.
    pub fn stay_current_step(mut self) -> Self {
        // Set `ExecutionMark.stay_current_step`; executor locks progress to current response step.
        self.context.stay_current_step = true;
        self
    }
    pub fn get_context(&self) -> &ExecutionMark {
        &self.context
    }
    pub fn with_meta<T>(mut self, meta: T) -> Self
    where
        T: serde::Serialize,
    {
        if let Ok(data) = serde_json::to_value(meta) {
            self.metadata = data;
        }
        self
    }
    pub fn add_meta<T>(mut self, key: impl AsRef<str>, value: T) -> Self
    where
        T: serde::Serialize,
    {
        if let Ok(val) = serde_json::to_value(value) {
            let mut map = match std::mem::take(&mut self.metadata) {
                serde_json::Value::Object(map) => map,
                _ => serde_json::Map::new(),
            };
            map.insert(key.as_ref().to_string(), val);
            self.metadata = serde_json::Value::Object(map);
        }
        self
    }
    /// Overrides chain backtracking pointer (points to predecessor `Request.id`).
    ///
    /// By default this pointer is inherited from `Response.prefix_request`.
    /// For cross-module jumps, you can set it explicitly to change first-failure fallback target.
    pub fn with_prefix_request(mut self, prefix: Uuid) -> Self {
        self.prefix_request = prefix;
        self
    }
    // Creates a new `ParserTaskModel` for target module within same account + platform.
    pub fn start_other_module(response: &Response, module_name: impl AsRef<str>) -> Self {
        debug_assert!(
            !module_name.as_ref().is_empty(),
            "module_name must not be empty"
        );
        ParserTaskModel {
            id: Uuid::now_v7(),
            account_task: TaskModel {
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: Some(vec![module_name.as_ref().to_string()]),
                priority: response.priority,
                run_id: Uuid::now_v7(),
            },
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: json!({}),
            // Reset context to ensure target module starts from the beginning.
            context: ExecutionMark::default(),
            run_id: Uuid::now_v7(),
            prefix_request: response.prefix_request,
        }
    }
    /// Creates `ParserTaskModel` for target module with explicit context.
    ///
    /// Typical usage:
    /// - Cross-jump to step 0: pass `ExecutionMark::default().with_step_idx(0)`.
    /// - Stay on current step during retry: pass `ExecutionMark` with `stay_current_step=true`.
    pub fn start_other_module_with_ctx(
        response: &Response,
        module_name: impl AsRef<str>,
        ctx: ExecutionMark,
    ) -> Self {
        debug_assert!(
            !module_name.as_ref().is_empty(),
            "module_name must not be empty"
        );
        ParserTaskModel {
            id: Uuid::now_v7(),
            account_task: TaskModel {
                account: response.account.clone(),
                platform: response.platform.clone(),
                module: Some(vec![module_name.as_ref().to_string()]),
                priority: response.priority,
                run_id: Uuid::now_v7(),
            },
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: json!({}),
            context: ctx,
            run_id: Uuid::now_v7(),
            prefix_request: response.prefix_request,
        }
    }
}

/// Error task message model.
///
/// Records processing-time errors, including error details and execution context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorTaskModel {
    /// Unique identifier.
    pub id: Uuid,
    /// Associated account task information.
    pub account_task: TaskModel,
    /// Error message.
    pub error_msg: String,
    /// Timestamp.
    pub timestamp: u64,
    /// Metadata.
    #[serde(with = "crate::common::model::serde_value::value")]
    pub metadata: serde_json::Value,
    /// Execution context.
    pub context: ExecutionMark,
    /// Run identifier.
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
    /// Predecessor request identifier (points to predecessor `Request.id`).
    pub prefix_request: Uuid,
}

#[async_trait]
impl Offloadable for ErrorTaskModel {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
}

impl ErrorTaskModel {
    pub fn get_context(&self) -> &ExecutionMark {
        &self.context
    }
    pub fn with_context(mut self, ctx: ExecutionMark) -> Self {
        self.context = ctx;
        self
    }
}
/// Base task model.
///
/// Defines minimal task identity with account, platform, and module information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskModel {
    /// Account identifier.
    pub account: String,
    /// Platform identifier.
    pub platform: String,
    /// Module list (optional; empty means all modules).
    pub module: Option<Vec<String>>,
    /// Priority.
    #[serde(default)]
    pub priority: crate::common::model::Priority,
    /// Run identifier.
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum UnifiedTaskInput {
    Task(TaskModel),
    ParserTask(ParserTaskModel),
    ErrorTask(ErrorTaskModel),
}

impl UnifiedTaskInput {
    pub fn run_id(&self) -> Uuid {
        match self {
            UnifiedTaskInput::Task(value) => value.run_id,
            UnifiedTaskInput::ParserTask(value) => value.run_id,
            UnifiedTaskInput::ErrorTask(value) => value.run_id,
        }
    }
}

impl From<TaskModel> for UnifiedTaskInput {
    fn from(value: TaskModel) -> Self {
        UnifiedTaskInput::Task(value)
    }
}

impl From<ParserTaskModel> for UnifiedTaskInput {
    fn from(value: ParserTaskModel) -> Self {
        UnifiedTaskInput::ParserTask(value)
    }
}

impl From<ErrorTaskModel> for UnifiedTaskInput {
    fn from(value: ErrorTaskModel) -> Self {
        UnifiedTaskInput::ErrorTask(value)
    }
}

#[async_trait]
impl Offloadable for TaskModel {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> crate::errors::Result<()> {
        Ok(())
    }
}

impl crate::common::model::priority::Prioritizable for TaskModel {
    fn get_priority(&self) -> crate::common::model::Priority {
        self.priority
    }
}

impl crate::common::model::priority::Prioritizable for ParserTaskModel {
    fn get_priority(&self) -> crate::common::model::Priority {
        self.account_task.priority
    }
}

impl crate::common::model::priority::Prioritizable for ErrorTaskModel {
    fn get_priority(&self) -> crate::common::model::Priority {
        self.account_task.priority
    }
}

impl From<&Response> for ParserTaskModel {
    fn from(value: &Response) -> Self {
        ParserTaskModel {
            id: Uuid::now_v7(),
            account_task: TaskModel {
                account: value.account.clone(),
                platform: value.platform.clone(),
                module: Some(vec![value.module.clone()]),
                priority: value.priority,
                run_id: value.run_id,
            },
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: json!({}),
            context: value.context.clone(),
            run_id: value.run_id,
            prefix_request: value.prefix_request,
        }
    }
}

impl From<&Response> for ErrorTaskModel {
    fn from(value: &Response) -> Self {
        ErrorTaskModel {
            id: Uuid::now_v7(),
            account_task: TaskModel {
                account: value.account.clone(),
                platform: value.platform.clone(),
                module: Some(vec![value.module.clone()]),
                priority: value.priority,
                run_id: value.run_id,
            },
            error_msg: "".to_string(),
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: value.metadata.clone().into(),
            context: value.context.clone(),
            run_id: value.run_id,
            prefix_request: value.prefix_request,
        }
    }
}

/// Serde default function to auto-generate a run_id using UUID v7
fn default_run_id() -> Uuid {
    Uuid::now_v7()
}

/// Parser output envelope.
///
/// Contains parsed data, next task, error task, and control flags.
#[derive(Debug, Default)]
pub struct ParserData {
    /// Parsed data list.
    pub data: Vec<Data>,
    /// Generated next parser tasks.
    pub parser_task: Vec<ParserTaskModel>,
    /// Generated error task (optional).
    pub error_task: Option<ErrorTaskModel>,
    /// Stop flag (optional).
    pub stop: Option<bool>,
}
impl ParserData {
    pub fn with_data(mut self, data: Vec<impl StoreTrait>) -> Self {
        self.data = data.into_iter().map(|d| d.build()).collect();
        self
    }
    pub fn with_task(mut self, task: ParserTaskModel) -> Self {
        self.parser_task.push(task);
        self
    }
    pub fn with_tasks(mut self, tasks: Vec<ParserTaskModel>) -> Self {
        self.parser_task.extend(tasks);
        self
    }
    pub fn with_error(mut self, error: ErrorTaskModel) -> Self {
        self.error_task = Some(error);
        self
    }

    /// Module-level stop flag; when `true`, subsequent requests are no longer processed.
    /// For WSS flows this can be used to signal upper layers to close connection.
    pub fn with_stop(mut self, stop: bool) -> Self {
        self.stop = Some(stop);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::ExecutionMark;

    #[test]
    fn test_topic_type() {
        assert_eq!(TopicType::Task.suffix(), "task");
        assert_eq!(TopicType::Request.suffix(), "request");
        assert_eq!(TopicType::Response.suffix(), "response");
        
        let name = TopicType::Task.get_name("test_mod");
        assert_eq!(name, "crawler-test_mod-task");
    }

    #[test]
    fn test_parser_task_model_builder() {
        let id = Uuid::now_v7();
        let run_id = Uuid::now_v7();
        let prefix_req = Uuid::now_v7();
        
        let task = ParserTaskModel {
            id,
            account_task: TaskModel {
                account: "acc".into(),
                platform: "plat".into(),
                module: None,
                priority: Default::default(),
                run_id,
            },
            timestamp: 123456,
            metadata: json!({}),
            context: ExecutionMark::default(),
            run_id,
            prefix_request: prefix_req,
        };

        let ctx = ExecutionMark::default().with_step_idx(5);
        let task = task.with_context(ctx.clone());
        assert_eq!(task.context.step_idx, Some(5));
        
        let task = task.stay_current_step();
        assert!(task.context.stay_current_step);

        let new_prefix = Uuid::now_v7();
        let task = task.with_prefix_request(new_prefix);
        assert_eq!(task.prefix_request, new_prefix);

        let task = task.add_meta("foo", "bar");
        assert_eq!(task.metadata["foo"], "bar");
    }
    
    #[test]
    fn test_response_conversion() {
        let run_id = Uuid::now_v7();
        let prefix_request = Uuid::now_v7();
        let response = Response {
            id: Uuid::now_v7(),
            platform: "plat".into(),
            account: "acc".into(),
            module: "mod".into(),
            status_code: 200,
            cookies: Default::default(),
            content: vec![],
            storage_path: None,
            headers: vec![],
            task_retry_times: 0,
            metadata: crate::common::model::meta::MetaData::default(),
            download_middleware: vec![],
            data_middleware: vec![],
            task_finished: false,
            context: ExecutionMark::default(),
            run_id,
            prefix_request,
            request_hash: None,
            priority: Default::default(),
        };

        let parser_task: ParserTaskModel = (&response).into();
        assert_eq!(parser_task.account_task.account, "acc");
        assert_eq!(parser_task.account_task.platform, "plat");
        assert_eq!(parser_task.run_id, run_id);
        assert_eq!(parser_task.prefix_request, prefix_request);
        
        let error_task: ErrorTaskModel = (&response).into();
        assert_eq!(error_task.account_task.account, "acc");
        assert_eq!(error_task.run_id, run_id);
    }
}
