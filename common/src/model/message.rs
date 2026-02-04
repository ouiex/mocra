use crate::interface::StoreTrait;
use crate::interface::storage::{Offloadable, BlobStorage};
use std::sync::Arc;
use async_trait::async_trait;
use crate::model::data::Data;
use crate::model::{ExecutionMark, Response};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum TopicType {
    /// 任务队列
    Task,
    /// 请求队列
    Request,
    /// 响应队列 (原始任务数据)
    Response,
    /// 解析任务队列 (处理结果)
    ParserTask,
    /// 错误队列 (错误信息)
    Error,
}

impl TopicType {
    /// 获取topic后缀
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

/// 任务消息模型
///
/// 用于在解析完成后生成新的任务，或者流转到下一个处理阶段。
/// 包含了任务的基本信息、元数据、上下文以及前置请求的引用。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserTaskModel {
    /// 唯一标识
    pub id: Uuid,
    /// 关联的账号任务信息
    pub account_task: TaskModel,
    /// 时间戳
    pub timestamp: u64,
    /// 元数据 (ParserTaskModel.meta => Task.metadata => Context.meta.task_meta => Module.generate)
    #[serde(with = "crate::model::serde_value::value")]
    pub metadata: serde_json::Value,
    /// 执行上下文 (ExecutionMark)
    pub context: ExecutionMark,
    /// 运行标识 (Run ID)
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
    /// 前置请求标识
    pub prefix_request: Uuid,
}

#[async_trait]
impl Offloadable for ParserTaskModel {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
}

impl ParserTaskModel {
    /// 为解析链设置明确的执行上下文（ExecutionMark）。
    ///
    /// 用途：
    /// - 精确控制目标模块从哪一个 step 开始执行（例如从 0 开始，或在错误重试时停留在当前步）。
    /// - 可结合 `stay_current_step` 防止自动推进到下一步。
    pub fn with_context(mut self, ctx: ExecutionMark) -> Self {
        self.context = ctx;
        self
    }
    /// 标记：本次任务应当停留在当前解析节点，避免自动推进到下一步。
    ///
    /// 说明：
    /// - 当解析器返回 ParserTaskModel 且调用了该方法时，链式处理器会在本步循环（不前进）。
    /// - 若未调用该方法（或解析器未返回 ParserTaskModel），则视为本步循环结束，继续后续流程。
    pub fn stay_current_step(mut self) -> Self {
        // 仅需设置 ExecutionMark 的 stay_current_step 标志；
        // 实际执行时，处理器会将步进锁定在当前 Response 的 step 索引。
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
    /// 覆盖链式回溯指针（指向前置 Request 的 request.id）。
    ///
    /// 默认情况下，`ParserTaskModel` 会从 `Response.prefix_request` 继承该指针，
    /// 在跨模块时可按需显式指定，以改变首次失败回溯的落点。
    pub fn with_prefix_request(mut self, prefix: Uuid) -> Self {
        self.prefix_request = prefix;
        self
    }
    // 为指定的模块创建一个新的ParserTaskModel，必须来自同一个account+platform
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
            // 重置context  确保指定的模块从头开始执行
            context: ExecutionMark::default(),
            run_id: Uuid::now_v7(),
            prefix_request: response.prefix_request,
        }
    }
    /// 为指定的模块创建 ParserTaskModel（可显式指定上下文）。
    ///
    /// 典型用法：
    /// - 从其它模块跨跳到目标模块的第 0 步：传入 `ExecutionMark::default().with_step_idx(0)`。
    /// - 在错误重试场景中，停留在当前步：传入带 `stay_current_step=true` 的 `ExecutionMark`。
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

/// 错误消息模型
///
/// 记录任务处理过程中发生的错误，包含错误信息和上下文。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorTaskModel {
    /// 唯一标识
    pub id: Uuid,
    /// 关联的账号任务信息
    pub account_task: TaskModel,
    /// 错误信息
    pub error_msg: String,
    /// 时间戳
    pub timestamp: u64,
    /// 元数据
    #[serde(with = "crate::model::serde_value::value")]
    pub metadata: serde_json::Value,
    /// 执行上下文
    pub context: ExecutionMark,
    /// 运行标识
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
    /// 前置请求标识 (指向前置 Request.id)
    pub prefix_request: Uuid,
}

#[async_trait]
impl Offloadable for ErrorTaskModel {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
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
/// 基础任务模型
///
/// 定义了任务的最小单元，包含账号、平台和模块信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskModel {
    /// 账号标识
    pub account: String,
    /// 平台标识
    pub platform: String,
    /// 模块列表 (可选，若为空则包含所有模块)
    pub module: Option<Vec<String>>,
    /// 优先级
    #[serde(default)]
    pub priority: crate::model::Priority,
    /// 运行标识
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
}

#[async_trait]
impl Offloadable for TaskModel {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
}

impl crate::model::priority::Prioritizable for TaskModel {
    fn get_priority(&self) -> crate::model::Priority {
        self.priority
    }
}

impl crate::model::priority::Prioritizable for ParserTaskModel {
    fn get_priority(&self) -> crate::model::Priority {
        self.account_task.priority
    }
}

impl crate::model::priority::Prioritizable for ErrorTaskModel {
    fn get_priority(&self) -> crate::model::Priority {
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

/// 解析器数据封装
///
/// 包含解析出的数据、新的任务、错误信息以及控制指令。
#[derive(Debug, Default)]
pub struct ParserData {
    /// 解析出的数据列表
    pub data: Vec<Data>,
    /// 生成的新任务 (可选)
    pub parser_task: Option<ParserTaskModel>,
    /// 生成的错误任务 (可选)
    pub error_task: Option<ErrorTaskModel>,
    /// 停止标志 (可选)
    pub stop: Option<bool>,
}
impl ParserData {
    pub fn with_data(mut self, data: Vec<impl StoreTrait>) -> Self {
        self.data = data.into_iter().map(|d| d.build()).collect();
        self
    }
    pub fn with_task(mut self, task: ParserTaskModel) -> Self {
        self.parser_task = Some(task);
        self
    }
    pub fn with_error(mut self, error: ErrorTaskModel) -> Self {
        self.error_task = Some(error);
        self
    }

    /// wss 连接关闭标志，用于通知上层模块关闭连接
    /// 模块级别的停止标志，true=>后续的请求将不再被处理，
    pub fn with_stop(mut self, stop: bool) -> Self {
        self.stop = Some(stop);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ExecutionMark;

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
            metadata: crate::model::meta::MetaData::default(),
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
