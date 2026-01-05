#![allow(unused)]
use std::error::Error as StdError;
use std::fmt;
use std::num::ParseIntError;
use std::str::ParseBoolError;
use thiserror::Error;

/// 通用错误详情类型
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    Request,
    Response,
    Command,
    Service,
    Proxy,
    Cookie,
    Download,
    Queue,
    Orm,
    Task,
    Module,
    Header,
    RateLimit,
    Sync,
    ProcessorChain,
    Parser,
    DataMiddleware,
    DataStore,
    DynamicLibrary,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Request => write!(f, "request"),
            ErrorKind::Response => write!(f, "response"),
            ErrorKind::Command => write!(f, "command"),
            ErrorKind::Service => write!(f, "service"),
            ErrorKind::Proxy => write!(f, "proxy"),
            ErrorKind::Cookie => write!(f, "cookie"),
            ErrorKind::Download => write!(f, "download"),
            ErrorKind::Queue => write!(f, "queue"),
            ErrorKind::Orm => write!(f, "orm"),
            ErrorKind::Module => write!(f, "task"),
            ErrorKind::Header => write!(f, "header"),
            ErrorKind::RateLimit => write!(f, "rate limit"),
            ErrorKind::Sync => write!(f, "sync"),
            ErrorKind::ProcessorChain => write!(f, "processor chain"),
            ErrorKind::Parser => write!(f, "parser"),
            ErrorKind::DataMiddleware => write!(f, "data middleware"),
            ErrorKind::DataStore => write!(f, "data store"),
            ErrorKind::Task => write!(f, "task"),
            ErrorKind::DynamicLibrary => write!(f, "dynamic library"),
        }
    }
}

pub struct ErrorInner {
    pub kind: ErrorKind,
    pub source: Option<BoxError>,
    pub message: Option<String>,
}

pub struct Error {
    pub inner: Box<ErrorInner>,
}

impl Error {
    pub fn new<E>(kind: ErrorKind, source: Option<E>) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Box::new(ErrorInner {
                kind,
                source: source.map(Into::into),
                message: None,
            }),
        }
    }

    pub(crate) fn with_message<E>(kind: ErrorKind, message: String, source: Option<E>) -> Error
    where
        E: Into<BoxError>,
    {
        Error {
            inner: Box::new(ErrorInner {
                kind,
                source: source.map(Into::into),
                message: Some(message),
            }),
        }
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.inner.kind
    }

    pub fn is_request(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Request)
    }

    pub fn is_response(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Response)
    }

    pub fn is_command(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Command)
    }

    pub fn is_service(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Service)
    }

    pub fn is_proxy(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Proxy)
    }

    pub fn is_cookie(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Cookie)
    }

    pub fn is_download(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Download)
    }
    pub fn is_queue(&self) -> bool {
        matches!(self.inner.kind, ErrorKind::Queue)
    }
    pub fn is_timeout(&self) -> bool {
        if let Some(source) = &self.inner.source {
            // 检查是否为超时错误
            source.to_string().to_lowercase().contains("timeout")
        } else {
            false
        }
    }

    pub fn is_connect(&self) -> bool {
        if let Some(source) = &self.inner.source {
            let msg = source.to_string().to_lowercase();
            msg.contains("connect") || msg.contains("connection")
        } else {
            false
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("crawler_engine::Error");
        f.field("kind", &self.inner.kind);
        if let Some(ref message) = self.inner.message {
            f.field("message", message);
        }
        if let Some(ref source) = self.inner.source {
            f.field("source", source);
        }
        f.finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref message) = self.inner.message {
            write!(f, "{} error: {}", self.inner.kind, message)?;
        } else {
            write!(f, "{} error", self.inner.kind)?;
        }

        if let Some(ref source) = self.inner.source {
            write!(f, ": {source}")?;
        }

        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_ref()
            .map(|e| &**e as &(dyn StdError + 'static))
    }
}

/// 便利宏，用于创建不同类型的错误
macro_rules! error {
    ($kind:ident) => {
        Error::new(ErrorKind::$kind, None::<BoxError>)
    };
    ($kind:ident, $src:expr) => {
        Error::new(ErrorKind::$kind, Some($src))
    };
    ($kind:ident, $msg:expr, $src:expr) => {
        Error::with_message(ErrorKind::$kind, $msg.to_string(), Some($src))
    };
}

pub(crate) use error;

// 从特定错误类型转换为通用错误
impl From<RequestError> for Error {
    fn from(err: RequestError) -> Self {
        Error::new(ErrorKind::Request, Some(err))
    }
}

impl From<ResponseError> for Error {
    fn from(err: ResponseError) -> Self {
        Error::new(ErrorKind::Response, Some(err))
    }
}

impl From<CommandError> for Error {
    fn from(err: CommandError) -> Self {
        Error::new(ErrorKind::Command, Some(err))
    }
}

impl From<ServiceError> for Error {
    fn from(err: ServiceError) -> Self {
        Error::new(ErrorKind::Service, Some(err))
    }
}

impl From<ProxyError> for Error {
    fn from(err: ProxyError) -> Self {
        Error::new(ErrorKind::Proxy, Some(err))
    }
}

impl From<CookieError> for Error {
    fn from(err: CookieError) -> Self {
        Error::new(ErrorKind::Cookie, Some(err))
    }
}

impl From<DownloadError> for Error {
    fn from(err: DownloadError) -> Self {
        Error::new(ErrorKind::Download, Some(err))
    }
}
impl From<QueueError> for Error {
    fn from(err: QueueError) -> Self {
        Error::new(ErrorKind::Queue, Some(err))
    }
}
impl From<OrmError> for Error {
    fn from(err: OrmError) -> Self {
        Error::new(ErrorKind::Orm, Some(err))
    }
}

impl From<ModuleError> for Error {
    fn from(err: ModuleError) -> Self {
        Error::new(ErrorKind::Module, Some(err))
    }
}

impl From<HeaderError> for Error {
    fn from(err: HeaderError) -> Self {
        Error::new(ErrorKind::Header, Some(err))
    }
}
impl From<RateLimitError> for Error {
    fn from(err: RateLimitError) -> Self {
        Error::new(ErrorKind::RateLimit, Some(err))
    }
}
impl From<SyncError> for Error {
    fn from(err: SyncError) -> Self {
        Error::new(ErrorKind::Sync, Some(err))
    }
}
impl From<ProcessorChainError> for Error {
    fn from(err: ProcessorChainError) -> Self {
        Error::new(ErrorKind::ProcessorChain, Some(err))
    }
}
impl From<ParserError> for Error {
    fn from(value: ParserError) -> Self {
        Error::new(ErrorKind::Parser, Some(value))
    }
}
impl From<DataMiddlewareError> for Error {
    fn from(value: DataMiddlewareError) -> Self {
        Error::new(ErrorKind::DataMiddleware, Some(value))
    }
}

impl From<DataStoreError> for Error {
    fn from(value: DataStoreError) -> Self {
        Error::new(ErrorKind::DataStore, Some(value))
    }
}
impl From<TaskError> for Error {
    fn from(value: TaskError) -> Self {
        Error::new(ErrorKind::Task, Some(value))
    }
}

impl From<DynLibError> for Error {
    fn from(value: DynLibError) -> Self {
        Error::new(ErrorKind::DynamicLibrary, Some(value))
    }
}

// 具体错误类型定义
#[derive(Debug, Error)]
pub enum RequestError {
    #[error("invalid method")]
    InvalidMethod(#[source] BoxError),
    #[error("build failed")]
    BuildFailed(#[source] BoxError),
    #[error("bad request")]
    BadRequest,
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("not found")]
    NotFound,
    #[error("timeout")]
    Timeout,
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("invalid header: {0}")]
    InvalidHeader(String),
    #[error("invalid body: {0}")]
    InvalidBody(String),
    #[error("{0}")]
    InvalidParams(#[source] BoxError),
    #[error("{0}")]
    NotLogin(#[source] BoxError),
    #[error("{0}")]
    NotImplemented(#[source] BoxError),
    #[error("{0}")]
    InvalidMetaForRemote(#[source] BoxError)
}

#[derive(Debug, Error)]
pub enum ResponseError {
    #[error("invalid format")]
    InvalidFormat,
    #[error("{0}")]
    ParseError(BoxError),
    #[error("empty response")]
    EmptyResponse,
    #[error("invalid status: {0}")]
    InvalidStatus(u16),
    #[error("body too large: {0} bytes")]
    BodyTooLarge(usize),
    #[error("decode error: {0}")]
    DecodeError(String),
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("invalid command")]
    InvalidCommand,
    #[error("execution failed")]
    ExecutionFailed,
    #[error("permission denied")]
    PermissionDenied,
    #[error("command not found: {0}")]
    CommandNotFound(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("service unavailable")]
    ServiceUnavailable,
    #[error("connection failed")]
    ConnectionFailed,
    #[error("internal error")]
    InternalError,
    #[error("service timeout")]
    ServiceTimeout,
    #[error("rate limit exceeded")]
    RateLimitExceeded,
    #[error("authentication failed")]
    AuthenticationFailed,
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("invalid config: {0}")]
    InvalidConfig(#[source] BoxError),
    #[error("missing field: {0}")]
    MissingField(#[source] BoxError),
    #[error("parse error: {0}")]
    ParseError(#[source] BoxError),
    #[error("get proxy error: {0}")]
    GetProxy(#[source] BoxError),
    #[error("proxy not found")]
    ProxyNotFound,
    #[error("proxy not available: {0}")]
    ProxyNotAvailable(#[source] BoxError),
    #[error("proxy already expired")]
    ProxyExpired,
    #[error("proxy provider already expired")]
    ProxyProviderExpired,
    #[error("proxy connection failed")]
    ProxyConnectionFailed,
    #[error("proxy authentication failed")]
    ProxyAuthenticationFailed,
}

#[derive(Debug, Error)]
pub enum CookieError {
    #[error("{0}")]
    ParseError(#[source] BoxError),
    #[error("cookie not found: {0}")]
    NotFound(#[source] BoxError),
    #[error("cookie already exists: {0}")]
    AlreadyExists(#[source] BoxError),
    #[error("cookie load failed: {0}")]
    LoadFailed(#[source] BoxError),
    #[error("cookie save failed: {0}")]
    SaveFailed(#[source] BoxError),
    #[error("cookie domain mismatch: {0}")]
    DomainMismatch(String),
    #[error("cookie expired")]
    Expired,
    #[error("{0}")]
    LoadError(#[source] BoxError),
}
#[derive(Debug, Error)]
pub enum HeaderError {
    #[error("header parse error: {0}")]
    ParseError(#[source] BoxError),
    #[error("header not found: {0}")]
    NotFound(#[source] BoxError),
    #[error("header already exists: {0}")]
    AlreadyExists(#[source] BoxError),
    #[error("header load failed: {0}")]
    LoadFailed(#[source] BoxError),
    #[error("header save failed: {0}")]
    SaveFailed(#[source] BoxError),
    #[error("{0}")]
    LoadError(#[source] BoxError),
}
#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("download failed: {0}")]
    DownloadFailed(#[source] BoxError),
    #[error("params error: {0}")]
    InvalidParams(#[source] BoxError),
    #[error("invalid url: {0}")]
    InvalidUrl(#[source] BoxError),
    #[error("invalid proxy: {0}")]
    InvalidProxy(#[source] BoxError),
    #[error("network error: {0}")]
    NetworkError(#[source] BoxError),
    #[error("timeout error: {0}")]
    TimeoutError(#[source] BoxError),
    #[error("client error: {0}")]
    ClientError(#[source] BoxError),
    #[error("invalid response with error: {0}")]
    InvalidResponse(#[source] BoxError),
    #[error("invalid method: {0}")]
    InvalidMethod(#[source] BoxError),
    #[error("file write error: {0}")]
    FileWriteError(#[source] BoxError),
    #[error("insufficient storage space")]
    InsufficientSpace,
    #[error("download interrupted")]
    Interrupted,
    #[error("max retry exceeded")]
    MaxRetryExceeded,
}

#[derive(Debug, Error)]
pub enum RateLimitError {
    #[error("{0}")]
    RedisError(#[source] BoxError),
    #[error("wait time: {0} seconds")]
    WaitTime(u64),
}
#[derive(Debug, Error)]
pub enum SyncError {
    #[error("{0}")]
    ConnectFailed(#[source] BoxError),
    #[error("{0}")]
    SyncLoadFailed(#[source] BoxError),
    #[error("{0}")]
    SyncSaveFailed(#[source] BoxError),
    #[error("{0}")]
    SerdeFailed(#[source] BoxError),
    #[error("{0}")]
    DeserializeFailed(#[source] BoxError),
    #[error("{0}")]
    Timeout(#[source] BoxError),
    #[error("{0}")]
    ExecuteFailed(#[source] BoxError),
}

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("data not serialization")]
    SerializationFailed(BoxError),
    #[error("data not deserialization")]
    DeserializationFailed(BoxError),
    #[error("connection failed")]
    ConnectionFailed,
    #[error("channel create failed: {0}")]
    ChannelCreateFailed(BoxError),
    #[error("channel not found: {0}")]
    ChannelNotFound(BoxError),
    #[error("push data to queue failed")]
    PushFailed(BoxError),
    #[error("receive from queue failed")]
    PopFailed(BoxError),
    #[error("queue operation failed: {0}")]
    OperationFailed(#[source] BoxError),
}

#[derive(Debug, Error)]
pub enum OrmError {
    #[error("database connection error: {0}")]
    ConnectionError(#[source] BoxError),
    #[error("query execution error: {0}")]
    QueryExecutionError(#[source] BoxError),
    #[error("transaction error: {0}")]
    TransactionError(#[source] BoxError),
    #[error("data not found")]
    NotFound,
    #[error("data already exists")]
    AlreadyExists,
    #[error("invalid data: {0}")]
    InvalidData(String),
}
#[derive(Debug, Error)]
pub enum ModuleError {
    #[error("json type cannot use error: {0}")]
    Json(#[source] BoxError),
    #[error("form type cannot use error: {0}")]
    Form(#[source] BoxError),
    #[error("params type cannot use error: {0}")]
    Params(#[source] BoxError),
    #[error("body must type of String: {0}")]
    Body(#[source] BoxError),
    #[error("{0}")]
    Model(#[source] BoxError),
    #[error("{0}")]
    ConfigSync(#[source] BoxError),
    #[error("{0}")]
    ModuleNotFound(#[source] BoxError),
    #[error("{0}")]
    TaskMaxError(#[source] BoxError),
    #[error("{0}")]
    ModuleMaxError(#[source] BoxError),
}
#[derive(Debug, Error)]
pub enum ProcessorChainError {
    #[error("{0}")]
    FatalFailure(#[source] BoxError),
    #[error("{0}")]
    DowncastFailure(#[source] BoxError),
    #[error("{0}")]
    MaxRetriesExceeded(#[source] BoxError),
    #[error("{0}")]
    EmptyChain(#[source] BoxError),
    #[error("{0}")]
    Unexpected(#[source] BoxError),
    #[error("{0}")]
    Timeout(#[source] BoxError),
    #[error("{0}")]
    Cancelled(#[source] BoxError),
}

#[derive(Debug, Error)]
pub enum ParserError {
    #[error("{0}")]
    InvalidFormat(#[source] BoxError),
    #[error("{0}")]
    ParseError(#[source] BoxError),
    #[error("{0}")]
    EmptyResponse(#[source] BoxError),
    #[error("{0}")]
    InvalidStatus(#[source] BoxError),
    #[error("{0}")]
    InvalidMetaData(#[source] BoxError),
    #[error("{0}")]
    BodyTooLarge(#[source] BoxError),
    #[error("{0}")]
    DecodeError(#[source] BoxError),
    #[error("{0}")]
    PolarsError(#[source] BoxError),
    #[error("{0}")]
    JsonParseError(#[source] BoxError),
    #[error("{0}")]
    DataError(#[source] BoxError),
}
#[derive(Debug, Error)]
pub enum DataMiddlewareError {
    #[error("{0}")]
    HandleDataError(#[source] BoxError),
    #[error("{0}")]
    InvalidData(#[source] BoxError),
    #[error("{0}")]
    EmptyData(#[source] BoxError),
    #[error("{0}")]
    MiddlewareNotFound(#[source] BoxError),
}
#[derive(Debug, Error)]
pub enum DataStoreError {
    #[error("{0}")]
    SaveFailed(#[source] BoxError),
    #[error("{0}")]
    InvalidData(#[source] BoxError),
    #[error("{0}")]
    ConnectionFailed(#[source] BoxError),
    #[error("{0}")]
    TransactionFailed(#[source] BoxError),
}

#[derive(Debug, Error)]
pub enum TaskError {
    #[error("{0}")]
    TaskNotFound(#[source] BoxError),
    #[error("{0}")]
    TaskAlreadyExists(#[source] BoxError),
    #[error("{0}")]
    InvalidTaskConfig(#[source] BoxError),
    #[error("{0}")]
    TaskExecutionFailed(#[source] BoxError),
    #[error("{0}")]
    TaskTimeout(#[source] BoxError),
    #[error("{0}")]
    TaskMaxError(#[source] BoxError),
}

#[derive(Debug, Error)]
pub enum DynLibError {
    #[error("{0}")]
    LoadError(#[source] BoxError),
    #[error("{0}")]
    SymbolNotFound(#[source] BoxError),
    #[error("{0}")]
    InitializationFailed(#[source] BoxError),
    #[error("{0}")]
    ExecutionFailed(#[source] BoxError),
    #[error("{0}")]
    IncompatibleVersion(#[source] BoxError),
    #[error("{0}")]
    InvalidPath(#[source] BoxError),
    #[error("{0}")]
    DependencyError(#[source] BoxError),
    #[error("{0}")]
    Other(#[source] BoxError),
}

// 便利函数，用于创建常见的错误类型
impl Error {
    pub fn request_timeout() -> Self {
        Error::from(RequestError::Timeout)
    }

    pub fn request_not_found() -> Self {
        Error::from(RequestError::NotFound)
    }

    pub fn proxy_not_found() -> Self {
        Error::from(ProxyError::ProxyNotFound)
    }

    pub fn proxy_expired() -> Self {
        Error::from(ProxyError::ProxyExpired)
    }

    pub fn download_failed<E: Into<BoxError>>(source: E) -> Self {
        Error::from(DownloadError::DownloadFailed(source.into()))
    }

    pub fn cookie_parse_error<E: Into<BoxError>>(source: E) -> Self {
        Error::from(CookieError::ParseError(source.into()))
    }

    pub fn service_unavailable() -> Self {
        Error::from(ServiceError::ServiceUnavailable)
    }
}

// 针对常见的外部错误类型的转换
impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::TimedOut => Error::request_timeout(),
            std::io::ErrorKind::ConnectionRefused => Error::from(ServiceError::ConnectionFailed),
            std::io::ErrorKind::PermissionDenied => Error::from(CommandError::PermissionDenied),
            std::io::ErrorKind::NotFound => Error::request_not_found(),
            _ => Error::new(ErrorKind::Service, Some(err)),
        }
    }
}


impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::new(ErrorKind::Parser,Some(value))
    }
}
impl From<ParseBoolError> for Error{
    fn from(value: ParseBoolError) -> Self {
        todo!()
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::from(ResponseError::ParseError(err.to_string().into()))
    }
}

impl From<polars::prelude::PolarsError> for Error {
    fn from(err: polars::prelude::PolarsError) -> Self {
        Error::from(ParserError::PolarsError(err.to_string().into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = Error::request_timeout();
        assert!(err.is_request());
        assert!(err.is_timeout());
    }

    #[test]
    fn test_error_display() {
        let err = Error::request_not_found();
        assert_eq!(err.to_string(), "request error: not found");
    }

    #[test]
    fn test_error_source() {
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timed out");
        let err = Error::from(io_err);
        assert!(err.source().is_some());
    }

    #[test]
    fn test_error_kinds() {
        let err = Error::proxy_not_found();
        assert!(err.is_proxy());
        assert!(!err.is_request());

        let err = Error::service_unavailable();
        assert!(err.is_service());
        assert!(!err.is_proxy());
    }
}
