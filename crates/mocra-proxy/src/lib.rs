//! mocra-proxy:配置驱动的代理池 / 管理器(从 mocra 主 crate 抽出,零 State、可独立使用)。
//!
//! - [`ProxyManager`] / [`ProxyPool`]:多 provider 代理池,支持隧道、健康检查、轮换、过期回收。
//! - [`ProxyConfig`]:TOML 配置加载(`ProxyConfig::load_from_toml`)。
//! - 独立错误 [`ProxyError`];宿主 crate 经 `From` 纳入自身 Error 体系。

pub mod error;
pub mod proxy_impl;
pub mod proxy_manager;
pub mod proxy_pool;

pub use error::{BoxError, ProxyError, Result};
pub use proxy_manager::ProxyManager;
pub use proxy_pool::*;
