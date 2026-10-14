//! A configuration-driven proxy pool / manager, usable independently of the crawling engine.
//!
//! - [`ProxyManager`] / [`ProxyPool`]: a multi-provider proxy pool with tunnels, health checks,
//!   rotation, and expiry reclamation.
//! - [`ProxyConfig`]: TOML configuration loading (`ProxyConfig::load_from_toml`).
//! - [`ProxyError`] converts into the framework's error hierarchy via `From`.

pub mod error;
pub mod proxy_impl;
pub mod proxy_manager;
pub mod proxy_pool;

pub use error::{BoxError, ProxyError, Result};
pub use proxy_manager::ProxyManager;
pub use proxy_pool::*;
