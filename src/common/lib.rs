//! 共享域模型 / 运行时上下文已抽为独立 crate [`mocra_core`] 的 `common` 模块。
//!
//! 此模块保留为 re-export shim,使既有 `crate::common::*` 引用继续有效(零改动迁移);
//! 新代码可直接 `use mocra_core::common::...`。

pub use mocra_core::common::*;
