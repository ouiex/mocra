# mocra 文档

mocra 是一个面向 Rust 的分布式、事件驱动的爬虫与数据采集框架。大多数项目只用**门面**：
实现一个 `Spider`，用 `Mocra::builder().run()` 跑起来 —— 单机、内存引擎，无需数据库。
底层模块 API 支持多阶段 DAG，运行时也支持分布式部署。请从**快速上手**开始；下面的指南
会深入运行时、进阶模块 API 与运维。

> **English version:** [docs/README.md](../README.md)

## 文档索引

| 文档 | 说明 |
|---|---|
| [快速上手](getting-started.md) | 安装 mocra、编写第一个 `Spider` 并运行 —— 无 DB |
| [系统架构](architecture.md) | 队列驱动的流水线、DAG 执行引擎，以及单机 vs 分布式 |
| [模块开发](module-development.md) | **进阶路径** —— `ModuleTrait` / `ModuleNodeTrait`、多节点流水线、节点间数据传递 |
| [DAG 执行](dag-guide.md) | DAG 定义、扇出 / 汇合图与推进门（advance gate） |
| [中间件](middleware-guide.md) | 下载、数据转换与存储中间件 |
| [配置参考](configuration.md) | 完整 TOML 参考（数据库、队列、控制 API） |
| [API 参考](api-reference.md) | 内置 HTTP 控制面与 Prometheus 指标端点 |
| [部署指南](deployment.md) | 单机 vs 分布式、监控与运维 |
| [后续请求](follow-up-requests.md) | `Ctx::follow` 保留 POST、请求头、Cookie、元数据与代理 |
| [代理与下载器](proxies-and-downloaders.md) | 固定/托管代理、反馈、重试轮换与自定义下载器 |
| [运行时调优](runtime-tuning.md) | 队列上限、代理选择与缓存、DAG 检查点、验证边界 |

使用开发代理实现项目功能时，可参阅仓库中的 [mocra 默认英文版 Skill](../../.agents/skills/mocra/SKILL.md)或[中文版](../../.agents/skills/mocra-zh/SKILL.md)。

## 可运行示例

更喜欢读代码？[`examples/`](../../examples/) 目录里是完整、可运行的程序：
[示例索引](../../examples/README.md)列出了命令与前置条件。

- [`spider_quickstart.rs`](../../examples/spider_quickstart.rs) —— 最小 `Spider`（无 DB）。
- [`quotes_scraper.rs`](../../examples/quotes_scraper.rs) —— 对 [quotes.toscrape.com](https://quotes.toscrape.com) 的真实端到端抓取：翻页跟进、详情页扇出、去重、类型化产出，以及写 JSONL 的自定义 `DataSink`。
- [`custom_downloader.rs`](../../examples/custom_downloader.rs) —— 实现 `Downloader` trait 并用 `.default_downloader()` 注入（离线、确定性）。
- [`follow_request.rs`](../../examples/follow_request.rs) —— 验证后续 POST 字段的保留（离线）。
- [`proxy_pool.rs`](../../examples/proxy_pool.rs) —— 代理选择与模拟反馈（离线）。
- [`explicit_proxy.rs`](../../examples/explicit_proxy.rs) —— 通过可用代理发送请求。
- [`dashboard.rs`](../../examples/dashboard.rs) —— 内置可观测 dashboard（`--features dashboard`）。
- [`cluster_quickstart.rs`](../../examples/cluster_quickstart.rs) —— 自组织内嵌集群（`--features cluster-embedded`）。

## 快捷链接

- **仓库:** <https://github.com/ouiex/mocra>
- **API 文档 (docs.rs):** <https://docs.rs/mocra>
- **Crate (crates.io):** <https://crates.io/crates/mocra>
- **更新日志：** [已发布及尚未发布的改动](../../CHANGELOG.md)
