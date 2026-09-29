---
name: mocra-zh
description: 中文版 mocra 开发指南。用于中文请求中的 Spider、后续请求、代理池、自定义下载器、模块、DAG 与项目维护；默认英文版使用 mocra。
---

# mocra 项目开发

默认英文版见 [mocra](../mocra/SKILL.md)。

此 Skill 面向 mocra 当前仓库及使用其 API 的项目；先查看 `Cargo.toml` 确认版本。确定用户要做的是编写爬虫、接入下载能力，还是修改运行时，再读取对应参考文件。用户给出的目标、约束和现有代码优先于这里的示例。

## 选择入口

| 任务 | 先读 | 关键源码或示例 |
| --- | --- | --- |
| 新建或修复 `Spider`、解析、翻页、数据输出 | [Spider 与请求流](references/spider.md) | `src/facade.rs`、`examples/spider_quickstart.rs`、`examples/follow_request.rs` |
| 代理选择、失败轮换、自定义下载器、浏览器下载 | [代理与下载器](references/network.md) | `crates/mocra-proxy/`、`crates/mocra-core/src/engine/chain/proxy_attempt.rs`、`examples/custom_downloader.rs` |
| 模块、DAG、队列、集群、配置或内部性能问题 | [运行时与验证](references/runtime.md) | `crates/mocra-core/`、`docs/architecture.md`、`docs/configuration.md` |

若任务跨越多个入口，按实际触及的部分读取。`docs/zh/README.md` 汇总了[后续请求](../../../docs/zh/follow-up-requests.md)、[代理与下载器](../../../docs/zh/proxies-and-downloaders.md)及[运行时调优](../../../docs/zh/runtime-tuning.md)等指南；`examples/README.md` 列出运行命令与前置条件。代码与文档不一致时，以当前类型定义、调用路径及测试为准，并修正受影响的文档。

## 实施要点

1. 面向普通使用者先尝试 `mocra::prelude::*`、`Spider` 和 `Mocra::builder()`。默认运行是单机、内存队列、无数据库；需要账号×平台×模块任务模型或自定义 DAG 时才转到低层模块 API。
2. 后续请求要保留调用者设置的字段：`Ctx::follow(Request)` 会重新入队完整请求；`follow_get(url)` 只新建 GET。改动这条路径时检查方法、请求头、body、Cookie、超时、元数据、显式代理和优先级，参考 `examples/follow_request.rs`。
3. 区分显式代理与托管代理池。`Request::use_proxy` 固定本次请求的代理；托管选择还需要引擎代理配置和模块 JSON 的 `enable_proxy`。不要将单独的 `ProxyManager` 演示误写成 `Spider` 自动轮换。
4. 自定义 `Downloader` 返回的 `Response` 必须携带对应请求的关联字段，否则解析阶段可能找不到原任务。按 `examples/custom_downloader.rs` 的完整构造方式实现。
5. 修改用户可见 API 时同步检查 `README.md`、`README.zh.md`、`docs/` 和 `examples/` 中受影响的说法；旧版本片段应与当前源码核对后再使用。

## 验证结果

依据修改范围运行最小而有意义的检查：`cargo fmt --all -- --check`、相关 crate 的测试、`cargo check --examples`，以及对应特性的示例检查。新爬虫至少用少量真实或本地响应确认输出；网络不可用时明确说明只完成了编译或离线验证。不要用要求真实代理、外站或分布式服务的示例充当离线测试。
