//! Error types for mocra-proxy.
//!
//! Independent of the host crate — as a standalone-usable library, proxy has its own
//! `ProxyError` / `Result`; the host `mocra` folds them into its own error hierarchy at the
//! boundary via `impl From<ProxyError> for mocra::Error`.

use thiserror::Error;

/// A boxed underlying error source (same shape as the host's `mocra::BoxError`).
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// `Result` alias for mocra-proxy.
pub type Result<T> = std::result::Result<T, ProxyError>;

/// Proxy subsystem errors.
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
