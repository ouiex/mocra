<div align="center">

# mocra

**面向 Rust 的分布式、事件驱动爬取与数据采集框架**

[![Crates.io](https://img.shields.io/crates/v/mocra.svg)](https://crates.io/crates/mocra)
[![docs.rs](https://docs.rs/mocra/badge.svg)](https://docs.rs/mocra)
[![License](https://img.shields.io/crates/l/mocra.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

[English](README.md) | 中文

</div>

---

mocra 是一个 Rust 框架，用于构建可扩展的数据采集流水线。它将爬取建模为**队列驱动的处理阶段**，由 **DAG 执行引擎**编排，能自动从单进程扩展到分布式集群。

## 特性

- **基于 DAG 的模块系统** — 定义线性链或自定义扇出/汇合图
- **队列驱动的流水线** — 任务 → 请求 → 下载 → 解析 → 存储，完全解耦
- **自动扩展运行时** — 单节点（内存）或分布式（Redis/Kafka），零代码改动
- **有界并发** — 信号量控制的工作池，支持暂停/恢复/关闭
- **中间件管道** — 下载、数据转换和存储中间件，按权重排序
- **内置控制面板** — HTTP API，支持健康检查、指标、暂停/恢复、任务注入和 DLQ 查看
- **Prometheus 指标** — 统一的 `mocra_*` 指标族，覆盖吞吐量、延迟、错误和积压
- **Cron 调度** — 使用 cron 表达式周期性执行任务
- **错误恢复** — 策略驱动的重试、回退门控、熔断器和死信队列

## 快速开始

在 `Cargo.toml` 中添加：

```toml
[dependencies]
mocra = "0.2"
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
futures = "0.3"
```

编写模块：

```rust
use std::sync::Arc;
use async_trait::async_trait;
use futures::stream;
use serde_json::{Map, Value};
use mocra::prelude::*;
use mocra::common::model::login_info::LoginInfo;
use mocra::common::state::State;
use mocra::engine::engine::Engine;

struct FetchNode;

#[async_trait]
impl ModuleNodeTrait for FetchNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        _params: Map<String, Value>,
        _login_info: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let req = Request::new("https://httpbin.org/get", RequestMethod::Get.as_ref());
        Ok(Box::pin(stream::iter(vec![req])))
    }

    async fn parser(
        &self, response: Response, _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        println!("状态码: {}, 响应体: {} 字节", response.status, response.body.len());
        Ok(TaskOutputEvent::default())
    }
}

struct MyModule;

#[async_trait]
impl ModuleTrait for MyModule {
    fn name(&self) -> String { "my_module".into() }
    fn version(&self) -> i32 { 1 }
    fn should_login(&self) -> bool { false }
    fn default_arc() -> Arc<dyn ModuleTrait> { Arc::new(Self) }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(FetchNode)]
    }
}

#[tokio::main]
async fn main() {
    let state = Arc::new(State::new("config.toml").await);
    let engine = Engine::new(state, None).await;
    engine.register_module(MyModule::default_arc()).await;
    engine.run().await;
}
```

创建 `config.toml`：

```toml
name = "my_crawler"

[db]
url = "sqlite://data/crawler.db?mode=rwc"

[cache]
ttl = 60

[download_config]
timeout = 30
rate_limit = 10

[crawler]
request_max_retries = 3

[channel_config]
capacity = 5000
```

运行：

```bash
cargo run
```

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                          引擎                           │
│                                                         │
│  TaskEvent ──▶ generate() ──▶ download() ──▶ parser()  │
│      │              │              │              │      │
│  [任务队列]    [请求队列]    [响应队列]    [解析队列]    │
│                                                  │      │
│                              ┌────────────┬──────┘      │
│                              ▼            ▼             │
│                         [数据存储]    [下一节点]         │
│                                      [错误队列 → DLQ]   │
└─────────────────────────────────────────────────────────┘
```

每个阶段通过消息队列解耦。单节点模式使用本地 Tokio 通道，分布式模式使用 Redis Streams 或 Kafka — **同一代码，零改动**。

## DAG 执行

定义复杂的流水线，支持扇出和汇合：

```rust
async fn dag_definition(&self) -> Option<ModuleDagDefinition> {
    Some(ModuleDagDefinition {
        nodes: vec![
            ModuleDagNodeDef { node_id: "start".into(), node: Arc::new(StartNode), ..Default::default() },
            ModuleDagNodeDef { node_id: "branch_a".into(), node: Arc::new(BranchA), ..Default::default() },
            ModuleDagNodeDef { node_id: "branch_b".into(), node: Arc::new(BranchB), ..Default::default() },
            ModuleDagNodeDef { node_id: "merge".into(), node: Arc::new(MergeNode), ..Default::default() },
        ],
        edges: vec![
            ModuleDagEdgeDef { from: "start".into(), to: "branch_a".into() },
            ModuleDagEdgeDef { from: "start".into(), to: "branch_b".into() },
            ModuleDagEdgeDef { from: "branch_a".into(), to: "merge".into() },
            ModuleDagEdgeDef { from: "branch_b".into(), to: "merge".into() },
        ],
        entry_nodes: vec!["start".into()],
        ..Default::default()
    })
}
```

```
       ┌── branch_a ──┐
start ─┤               ├── merge
       └── branch_b ──┘
```

## 单节点 vs 分布式

| | 单节点 | 分布式 |
|---|---|---|
| **配置** | 不配置 `cache.redis` | 添加 `cache.redis` |
| **队列** | Tokio mpsc（内存） | Redis Streams 或 Kafka |
| **缓存** | 内存 | Redis |
| **锁** | 本地 mutex | Redis 分布式锁 |
| **Worker** | 1 个进程 | N 个进程，相同二进制 |
| **代码改动** | 无 | 无 |

只需添加 Redis 即可切换到分布式：

```toml
[cache.redis]
url = "redis://localhost:6379"
```

## 文档

| 文档 | 说明 |
|---|---|
| [快速上手](docs/zh/getting-started.md) | 安装与第一个模块 |
| [系统架构](docs/zh/architecture.md) | 系统设计与流水线内部原理 |
| [模块开发](docs/zh/module-development.md) | ModuleTrait、ModuleNodeTrait、数据传递 |
| [DAG 指南](docs/zh/dag-guide.md) | DAG 定义、扇出/汇合、推进门控 |
| [中间件](docs/zh/middleware-guide.md) | 下载、数据、存储中间件 |
| [配置参考](docs/zh/configuration.md) | TOML 完整配置参考 |
| [API 参考](docs/zh/api-reference.md) | HTTP 控制面板端点 |
| [部署指南](docs/zh/deployment.md) | 单节点、分布式、监控 |

## 示例

参见 [`simple/`](simple/) 目录：

- [`simple/module_node_trait_dag.rs`](simple/module_node_trait_dag.rs) — 扇出/汇合 DAG 模块

## 监控

```bash
# 启动 Prometheus + Grafana
docker compose -f docker-compose.monitoring.yml up -d

# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000
# 指标:      http://localhost:8080/metrics
```

## 许可证

双许可，任选其一：

- MIT 许可证
- Apache 许可证 2.0
