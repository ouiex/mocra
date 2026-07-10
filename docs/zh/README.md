# mocra 文档

mocra 是一个面向 Rust 的分布式、事件驱动的爬虫与数据采集框架。大多数项目只用**门面**：
实现一个 `Spider`，用 `Mocra::builder().run()` 跑起来 —— 单机、内存引擎，无需数据库、无需
Redis。同一份代码在需要时可扩展为多阶段 DAG 与分布式集群。请从**快速上手**开始；下面的指南
会深入运行时、进阶模块 API 与运维。

> **English version:** [docs/README.md](../README.md)

## 文档索引

| 文档 | 说明 |
|---|---|
| [快速上手](getting-started.md) | 安装 mocra、编写第一个 `Spider` 并运行 —— 无 DB、无 Redis |
| [系统架构](architecture.md) | 队列驱动的流水线、DAG 执行引擎，以及单机 vs 分布式 |
| [模块开发](module-development.md) | **进阶路径** —— `ModuleTrait` / `ModuleNodeTrait`、多节点流水线、节点间数据传递 |
| [DAG 执行](dag-guide.md) | DAG 定义、扇出 / 汇合图与推进门（advance gate） |
| [中间件](middleware-guide.md) | 下载、数据转换与存储中间件 |
| [配置参考](configuration.md) | 完整 TOML 参考（数据库、Redis、队列、控制 API） |
| [API 参考](api-reference.md) | 内置 HTTP 控制面与 Prometheus 指标端点 |
| [部署指南](deployment.md) | 单机 vs 分布式、监控与运维 |

## 可运行示例

更喜欢读代码？[`examples/`](../../examples/) 目录里是完整、可运行的程序：

- [`spider_quickstart.rs`](../../examples/spider_quickstart.rs) —— 最小 `Spider`（无 DB / 无 Redis）。
- [`custom_downloader.rs`](../../examples/custom_downloader.rs) —— 实现 `Downloader` trait 并用 `.default_downloader()` 注入（离线、确定性）。
- [`dashboard.rs`](../../examples/dashboard.rs) —— 内置可观测 dashboard（`--features dashboard`）。
- [`cluster_quickstart.rs`](../../examples/cluster_quickstart.rs) —— 自组织内嵌集群（`--features cluster-embedded`）。

## 设计与重构

| 文档 | 说明 |
|---|---|
| [重构方案（中文）](../refactor/README.md) | 演进为可嵌入分布式爬虫库的设计记录：目标 API、集群架构（redb + Raft 控制面 / 可插拔 MQ 数据面）与路线图 |

## 快捷链接

- **仓库:** <https://github.com/ouiex/mocra>
- **API 文档 (docs.rs):** <https://docs.rs/mocra>
- **Crate (crates.io):** <https://crates.io/crates/mocra>
