use crate::interface::StoreTrait;
use crate::model::data::Data;
use crate::model::{ExecutionMark, Response};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub enum TopicType {
    Task,
    Request,
    Response,   // 原始任务数据
    ParserTask, // 处理结果
    Error,      // 错误信息
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

/// 任务消息类型
/// 解析器返回的内容
/// 根据内容生成调用对应的task生成新的request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserTaskModel {
    pub id: Uuid,
    pub account_task: TaskModel,
    pub timestamp: u64,
    /// ParserTaskModel.meta=>Task.metadata=>Context.meta.task_meta=>Module.generate=>ModuleTrait.generate
    pub metadata: serde_json::Value,
    pub context: ExecutionMark,
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
    pub prefix_request: Uuid,
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

/// 错误消息类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorTaskModel {
    pub id: Uuid,
    pub account_task: TaskModel,
    pub error_msg: String,
    pub timestamp: u64,
    pub metadata: serde_json::Value,
    pub context: ExecutionMark,
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,

    /// 所有的prefix_id都指向前置的Request.id
    pub prefix_request: Uuid,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskModel {
    pub account: String,
    pub platform: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<Vec<String>>,
    #[serde(default = "default_run_id")]
    pub run_id: Uuid,
}

impl From<&Response> for ParserTaskModel {
    fn from(value: &Response) -> Self {
        ParserTaskModel {
            id: Uuid::now_v7(),
            account_task: TaskModel {
                account: value.account.clone(),
                platform: value.platform.clone(),
                module: Some(vec![value.module.clone()]),
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
                run_id: value.run_id,
            },
            error_msg: "".to_string(),
            timestamp: chrono::Utc::now().timestamp() as u64,
            metadata: serde_json::to_value(&value.metadata).unwrap(),
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

#[derive(Debug, Default)]
pub struct ParserData {
    pub data: Vec<Data>,
    pub parser_task: Option<ParserTaskModel>,
    pub error_task: Option<ErrorTaskModel>,
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
