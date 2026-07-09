//! 错误类型已抽为独立 crate [`mocra_core`] 的 `errors` 模块。
//!
//! 此模块保留为 re-export shim,使既有 `crate::errors::*` 引用继续有效(零改动迁移);
//! 新代码可直接 `use mocra_core::errors::...`。

pub use mocra_core::errors::*;
