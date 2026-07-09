//! 代理子系统已抽为独立 crate [`mocra_proxy`]。
//!
//! 此模块保留为 re-export shim,使既有 `crate::proxy::*` 引用继续有效(零改动迁移);
//! 新代码可直接 `use mocra_proxy::...`。`ProxyError` 也在 [`mocra_proxy`],主 crate 的
//! `Error` 经 `From<mocra_proxy::ProxyError>` 在边界纳入。

pub use mocra_proxy::*;
