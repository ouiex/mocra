
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;
use kernel::model::message::{ErrorTaskModel, ParserTaskModel, TaskModel};
use kernel::model::{ModuleConfig, Request, Response};
use proxy::ProxyEnum;
use kernel::Module;
use kernel::Task;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventTaskModel {
    TaskModelReceived(TaskModelEvent),
    TaskModelStarted(TaskModelEvent),
    TaskModelCompleted(TaskModelEvent),
    TaskModelFailed(DynFailureEvent<TaskModelEvent>),
    TaskModelRetry(DynRetryEvent<TaskModelEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventParserTaskModel {
    ParserTaskModelReceived(ParserTaskModelEvent),
    ParserTaskModelStarted(ParserTaskModelEvent),
    ParserTaskModelCompleted(ParserTaskModelEvent),
    ParserTaskModelFailed(DynFailureEvent<ParserTaskModelEvent>),
    ParserTaskModelRetry(DynRetryEvent<ParserTaskModelEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventParserError {
    ParserErrorReceived(ParserErrorEvent),
    ParserErrorStarted(ParserErrorEvent),
    ParserErrorCompleted(ParserErrorEvent),
    ParserErrorFailed(DynFailureEvent<ParserErrorEvent>),
    ParserErrorRetry(DynRetryEvent<ParserErrorEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventTask {
    /// 任务相关事件
    TaskReceived(TaskEvent),
    TaskStarted(TaskEvent),
    TaskCompleted(TaskEvent),
    TaskFailed(DynFailureEvent<TaskEvent>),
    TaskRetry(DynRetryEvent<TaskEvent>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventModule {
    /// 任务相关事件
    ModuleGenerate(ModuleEvent),
    ModuleStarted(ModuleEvent),
    ModuleCompleted(ModuleEvent),
    ModuleFailed(DynFailureEvent<ModuleEvent>),
    ModuleRetry(DynRetryEvent<ModuleEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventRequest {
    /// 请求相关事件
    RequestReceived(RequestEvent),
    RequestStarted(RequestEvent),
    RequestCompleted(RequestEvent),
    RequestFailed(DynFailureEvent<RequestEvent>),
    RequestRetry(DynRetryEvent<RequestEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventRequestPublish {
    RequestPublishPrepared(RequestEvent),
    RequestPublishSend(RequestEvent),
    RequestPublishCompleted(RequestEvent),
    RequestPublishFailed(DynFailureEvent<RequestEvent>),
    RequestPublishRetry(DynRetryEvent<RequestEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventConfigLoad {
    ConfigLoadPrepared(ConfigLoadEvent),
    ConfigLoadStarted(ConfigLoadEvent),
    ConfigLoadCompleted(ConfigLoadEvent),
    ConfigLoadFailed(DynFailureEvent<ConfigLoadEvent>),
    ConfigLoadRetry(DynRetryEvent<ConfigLoadEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventProxy {
    /// Proxy相关事件
    ProxyPrepared(ProxyEvent),
    ProxyStarted(ProxyEvent),
    ProxyCompleted(ProxyEvent),
    ProxyFailed(DynFailureEvent<ProxyEvent>),
    ProxyRetry(DynRetryEvent<ProxyEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventRequestMiddleware {
    /// 请求中间件
    RequestMiddlewarePrepared(RequestMiddlewareEvent),
    RequestMiddlewareStarted(RequestMiddlewareEvent),
    RequestMiddlewareCompleted(RequestMiddlewareEvent),
    RequestMiddlewareFailed(DynFailureEvent<RequestMiddlewareEvent>),
    RequestMiddlewareRetry(DynRetryEvent<RequestMiddlewareEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventDownload {
    /// 下载相关事件
    DownloaderCreate(DownloadEvent),
    DownloadStarted(DownloadEvent),
    DownloadCompleted(DownloadEvent),
    DownloadFailed(DynFailureEvent<DownloadEvent>),
    DownloadRetry(DynRetryEvent<DownloadEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventResponseMiddleware {
    /// 响应中间件
    ResponseMiddlewarePrepared(ResponseEvent),
    ResponseMiddlewareStarted(ResponseEvent),
    ResponseMiddlewareCompleted(ResponseEvent),
    ResponseMiddlewareFailed(DynFailureEvent<ResponseEvent>),
    ResponseMiddlewareRetry(DynRetryEvent<ResponseEvent>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventResponsePublish {
    /// 响应发送事件
    ResponsePublishPrepared(ResponseEvent),
    ResponsePublishSend(ResponseEvent),
    ResponsePublishCompleted(ResponseEvent),
    ResponsePublishFailed(DynFailureEvent<ResponseEvent>),
    ResponsePublishRetry(DynRetryEvent<ResponseEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventParser {
    /// 解析相关事件
    ParserReceived(ParserEvent),
    ParserStarted(ParserEvent),
    ParserCompleted(ParserEvent),
    ParserFailed(DynFailureEvent<ParserEvent>),
    ParserRetry(DynRetryEvent<ParserEvent>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventDataStore {
    /// 数据保存事件
    StoreBefore(DataStoreEvent),
    StoreStarted(DataStoreEvent),
    StoreCompleted(DataStoreEvent),
    StoreFailed(DynFailureEvent<DataStoreEvent>),
    StoreRetry(DynRetryEvent<DataStoreEvent>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventDataMiddleware {
    /// 数据中间件事件
    MiddlewareBefore(DataMiddlewareEvent),
    MiddlewareStarted(DataMiddlewareEvent),
    MiddlewareCompleted(DataMiddlewareEvent),
    MiddlewareFailed(DynFailureEvent<DataMiddlewareEvent>),
    MiddlewareRetry(DynRetryEvent<DataMiddlewareEvent>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventResponseModuleLoad{
    ModuleGenerate(ResponseModuleLoad),
    ModuleGenerateStarted(ResponseModuleLoad),
    ModuleGenerateCompleted(ResponseModuleLoad),
    ModuleGenerateFailed(DynFailureEvent<ResponseModuleLoad>),
    ModuleGenerateRetry(DynRetryEvent<ResponseModuleLoad>),
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventSystem {
    ErrorOccurred(String),
    ErrorHandled(String),
    SystemStarted,
    SystemShutdown,
    ComponentHealthCheck(HealthCheckEvent),
}
/// 系统事件枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SystemEvent {
    TaskModel(EventTaskModel),
    ParserTaskModel(EventParserTaskModel),
    ParserError(EventParserError),
    Task(EventTask),
    Module(EventModule),
    Request(EventRequest),
    RequestPublish(EventRequestPublish),
    ConfigLoad(EventConfigLoad),
    Proxy(EventProxy),
    RequestMiddleware(EventRequestMiddleware),
    Download(EventDownload),
    ResponseMiddleware(EventResponseMiddleware),
    ResponsePublish(EventResponsePublish),
    ResponseModuleLoad(EventResponseModuleLoad),
    Parser(EventParser),
    DataMiddleware(EventDataMiddleware),
    DataStore(EventDataStore),
    System(EventSystem),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynRetryEvent<T>
where
    T: Serialize + Debug + Clone,
{
    pub data: T,
    pub retry_count: u32,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynFailureEvent<T> {
    pub data: T,
    pub error: String,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserErrorEvent {
    pub account: String,
    pub platform: String,
    pub modules: Option<Vec<String>>,
    pub error_msg: String,
    #[serde(skip)]
    pub metadata: serde_json::Value,
}
impl From<&ErrorTaskModel> for ParserErrorEvent {
    fn from(value: &ErrorTaskModel) -> Self {
        Self {
            account: value.account_task.account.clone(),
            platform: value.account_task.platform.clone(),
            modules: value.account_task.module.clone(),
            error_msg: value.error_msg.clone(),
            metadata: serde_json::to_value(value.metadata.clone()).unwrap_or_default(),
        }
    }
}


/// 任务事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    pub account: String,
    pub platform: String,
    pub modules: Vec<String>,
    // pub error_times: usize,
}
impl From<&Task> for TaskEvent {
    fn from(value: &Task) -> Self {
        TaskEvent {
            account: value.account.name.clone(),
            platform: value.platform.name.clone(),
            modules: value.modules.iter().map(|m| m.module.name()).collect(),
            // error_times: value.error_times,
        }
    }
}


/// Module事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleEvent {
    pub account: String,
    pub platform: String,
    pub modules: String,
    pub error_times: u32,
}
impl From<&Module> for ModuleEvent {
    fn from(value: &Module) -> Self {
        ModuleEvent {
            account: value.account.name.clone(),
            platform: value.platform.name.clone(),
            modules: value.module.name().clone(),
            error_times: value.error_times,
        }
    }
}


/// 请求事件
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigLoadEvent {
    pub request_id: Uuid,
    pub url: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub method: String,
    #[serde(skip)]
    pub metadata: serde_json::Value,
    pub config: Option<ModuleConfig>,
}
impl From<&(Request, Option<ModuleConfig>)> for ConfigLoadEvent {
    fn from(value: &(Request, Option<ModuleConfig>)) -> Self {
        ConfigLoadEvent {
            request_id: value.0.id,
            url: value.0.url.clone(),
            account: value.0.account.clone(),
            platform: value.0.platform.clone(),
            module: value.0.module.clone(),
            method: value.0.method.clone(),
            metadata: serde_json::to_value(value.0.meta.clone()).unwrap_or_default(),
            config: value.1.clone(),
        }
    }
}
/// 下载事件
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

/// 解析事件
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMiddlewareEvent {
    pub account: String,
    pub platform: String,
    pub module: String,
    pub request_id: String,
    pub schema_size: usize,
    pub after_size: Option<usize>,
}
impl From<&kernel::model::data::Data> for DataMiddlewareEvent {
    fn from(value: &kernel::model::data::Data) -> Self {
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


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataStoreEvent {
    pub account: String,
    pub platform: String,
    pub module: String,
    pub request_id: String,
    pub schema_size: (usize, usize),
    pub store_middleware: Option<String>,
}
impl From<&kernel::model::data::Data> for DataStoreEvent {
    fn from(value: &kernel::model::data::Data) -> Self {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyEvent {
    pub request_id: Uuid,
    pub url: String,
    pub account: String,
    pub platform: String,
    pub module: String,
    pub method: String,
    pub proxy: Option<ProxyEnum>,
}
impl From<&Request> for ProxyEvent {
    fn from(value: &Request) -> Self {
        ProxyEvent {
            request_id: value.id,
            url: value.url.clone(),
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
            method: value.method.clone(),
            proxy: value.proxy.clone(),
        }
    }
}
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseModuleLoad{
    pub account: String,
    pub platform: String,
    pub module: String,
}
impl From<&Response> for ResponseModuleLoad {
    fn from(value: &Response) -> Self {
        ResponseModuleLoad {
            account: value.account.clone(),
            platform: value.platform.clone(),
            module: value.module.clone(),
        }
    }
}



/// 健康检查事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckEvent {
    pub component: String,
    pub status: String,
    pub metrics: serde_json::Value,
    pub timestamp: u64,
}

impl SystemEvent {
    /// 获取事件的时间戳
    pub fn timestamp(&self) -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
        // match self {
        //     _ => {
        //         std::time::SystemTime::now()
        //             .duration_since(std::time::UNIX_EPOCH)
        //             .unwrap()
        //             .as_secs()
        //     }
        // }
    }

    /// 获取事件类型
    pub fn event_type(&self) -> &'static str {
        match self {
            SystemEvent::TaskModel(_) => "task_model",
            SystemEvent::ParserTaskModel(_) => "parser_task_model",
            SystemEvent::ParserError(_) => "parser_error",
            SystemEvent::Task(_) => "task",
            SystemEvent::Request(_) => "request",
            SystemEvent::RequestPublish(_) => "request_publish",
            SystemEvent::ConfigLoad(_) => "config_load",
            SystemEvent::Proxy(_) => "proxy",
            SystemEvent::RequestMiddleware(_) => "request_middleware",
            SystemEvent::Download(_) => "download",
            SystemEvent::ResponseMiddleware(_) => "response_middleware",
            SystemEvent::ResponsePublish(_) => "response_publish",
            SystemEvent::Parser(_) => "parser",
            SystemEvent::System(_) => "system",
            SystemEvent::DataStore(_) => "data_store",
            SystemEvent::ResponseModuleLoad(_) => "response_module_load",
            SystemEvent::DataMiddleware(_) => "data_middleware",
            SystemEvent::Module(_) => "module"
        }
    }
}
