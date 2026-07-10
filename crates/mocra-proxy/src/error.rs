//! mocra-proxy 的错误类型。
//!
//! 独立于宿主 crate —— 作为可单独使用的库,proxy 有自己的 `ProxyError` / `Result`;
//! 宿主 `mocra` 通过 `impl From<ProxyError> for mocra::Error` 在边界纳入自身 Error 体系。

use thiserror::Error;

/// 装箱的底层错误源(与宿主 `mocra::BoxError` 同形)。
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// mocra-proxy 的 `Result` 别名。
pub type Result<T> = std::result::Result<T, ProxyError>;

/// 代理子系统错误。
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
