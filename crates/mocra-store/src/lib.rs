//! mocra-store:mocra 的多租户元数据实体(sea-orm,从主 crate 抽出)。
//!
//! `account × platform × module` 模型 + 数据/下载中间件 + 关系表 + 任务结果 / 日志。
//! sea-orm-codegen 生成的实体,纯数据层(零爬虫 / 引擎耦合),供 `store` 特性使用。

pub mod entity;

pub use entity::*;
