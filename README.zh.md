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
- **自动扩展运行时** — 单节点（内存）或分布式（内嵌集群），零代码改动
- **有界并发** — 信号量控制的工作池，支持暂停/恢复/关闭
- **中间件管道** — 下载、数据转换和存储中间件，按权重排序
- **可插拔下载器** — 默认用 reqwest；可整体替换（`.default_downloader()`）或按模块注册具名下载器（`.downloader()`），用于浏览器渲染、代理轮换、自定义重试
- **后台管理 dashboard** — 开启 `dashboard` 特性即得一套只读可观测 HTTP API **以及**内置单文件网页面板（指标 / 日志 / 任务 / 性能），无需前端构建
- **内置控制面板** — HTTP API，支持健康检查、指标、暂停/恢复、任务注入和 DLQ 查看
- **Prometheus 指标** — 统一的 `mocra_*` 指标族，覆盖吞吐量、延迟、错误和积压
- **Cron 调度** — 使用 cron 表达式周期性执行任务
- **错误恢复** — 策略驱动的重试、回退门控、熔断器和死信队列

## 快速开始

在 `Cargo.toml` 中添加：

```toml
[dependencies]
mocra = "0.4"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

写一个 `Spider` 并运行 —— **无需数据库,三步搞定**（单节点内存模式）：

```rust
use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Page { url: String, status: u16 }

struct Httpbin;

#[async_trait]
impl Spider for Httpbin {
    type Item = Page;
    fn name(&self) -> &str { "httpbin" }

    async fn start(&self, s: &mut Seeds) {
        s.get("https://httpbin.org/get");
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        // 用 res.text()? / res.json()? 解析响应；cx.emit 产出数据；cx.follow 跟进请求。
        cx.emit(Page { url: res.module_id(), status: res.status_code });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(Httpbin, on_item(|p: Page| async move {
            println!("[item] {} -> {}", p.url, p.status);
        }))
        .run()
        .await
}
```

运行：

```bash
cargo run
```

### 分布式集群

开启 `cluster-embedded` 特性，启动一个**自组网的 Raft 集群** —— 把任意节点注册到任意已知节点即可组网：

```toml
mocra = { version = "0.4", features = ["cluster-embedded"] }
```

```rust
// 首个核心节点 —— 自举一个新集群
Mocra::builder()
    .spider(Httpbin, on_item(|p: Page| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;

// 其余节点通过种子地址入网（容器环境可用 ClusterConfig::from_env()）
Mocra::builder()
    .spider(Httpbin, on_item(|p: Page| async move { /* ... */ }))
    .cluster(ClusterConfig::join(2, "127.0.0.1:7002", "./data/n2", "127.0.0.1:7001"))
    .run().await?;
```

**控制面**（选举、分布式锁、成员、分区归属）跑在内嵌的 **redb + Raft** 上，无需外部协调器。**数据面**保留可插拔消息队列（Kafka / **NATS JetStream** / 内存），任务按 `hash(account)` 路由实现消费亲和。

> **进阶（多阶段 DAG）**：需要多节点、登录、翻页、自定义中间件的流水线，可直接实现 `ModuleTrait` / `ModuleNodeTrait`（开启 `store` 特性以启用 account × platform × module 模型）。见[模块开发](docs/zh/module-development.md)。

### 后台管理 dashboard

开启 `dashboard` 特性并调用 `.dashboard(port)` —— 引擎会托管一套只读可观测 API **以及**一个内置单文件网页面板。浏览器打开该端口即见 **指标 / 日志 / 任务 / 性能**；无需前端构建，也无需手填 endpoint（页面同源自动指向本引擎）：

```toml
mocra = { version = "0.4", features = ["dashboard"] }
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .dashboard(12800)   // GET / → 网页面板；/metrics、/observability/{engine,cluster,system,logs}
    .run().await?;
```

```bash
cargo run --example dashboard --features dashboard   # 然后浏览器打开 http://127.0.0.1:12800
```

只读端点（`/`、`/metrics`、`/health`、`/observability/*`）开启了 CORS、免 API key，独立前端也可跨域消费；写操作端点（`/control/*`、`/start_work`）仍需鉴权。

### 自定义下载器

默认下载器是 reqwest。实现 `Downloader` trait 即可把它换成浏览器渲染、代理轮换或自定义重试策略 —— 全局替换或按模块选择：

```rust
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .default_downloader(BrowserDownloader::new())  // 全局替换 reqwest
    // .downloader(MyDownloader::new())            // 或按名注册；当模块的 config.downloader
    //                                             // 等于其 name() 时路由过去
    .run().await?;
```

可运行示例见 [`examples/custom_downloader.rs`](examples/custom_downloader.rs)（离线、确定性）。

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

每个阶段通过消息队列解耦。单节点模式使用本地 Tokio 通道，分布式模式使用 Kafka / NATS(JetStream) —— **同一代码，零改动**。

### Workspace crate

mocra 是一个 Cargo workspace。整个运行时都在 `mocra-core` 里；你依赖的 `mocra` crate 是它之上的**薄门面**（默认仅 12 个直接依赖）。可复用的子系统以独立 crate 发布，依赖单向无环（`mocra → mocra-core → {mocra-cluster, mocra-dag, mocra-proxy, mocra-store}`，内层 crate 从不反依赖）：

| Crate | 说明 |
|---|---|
| [`mocra`](.) | **薄门面** —— `Spider` trait、`Mocra` 构建器、prelude、默认 sink。多数用户唯一需要 import 的 crate。 |
| [`mocra-core`](crates/mocra-core) | 完整运行时：领域模型、下载器、队列、协调/同步、调度器、引擎 + 可观测/管理 API。 |
| [`mocra-cluster`](crates/mocra-cluster) | 内嵌控制面：Raft + redb（选举、带 fencing 的分布式锁、成员、分区归属）—— 无需外部协调器。 |
| [`mocra-dag`](crates/mocra-dag) | 通用分布式 DAG 执行引擎（零爬虫耦合）。 |
| [`mocra-proxy`](crates/mocra-proxy) | 配置驱动的代理池 / 管理器（独立可用）。 |
| [`mocra-store`](crates/mocra-store) | 多租户 sea-orm 实体模型（`store` 特性）。 |

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

| | 单节点 | 内嵌集群（`cluster-embedded`） |
|---|---|---|
| **控制面** | 进程内 | 内嵌 **redb + Raft**（选举 / 锁 / 成员 / 分区归属）—— **无需外部协调器** |
| **队列（数据面）** | Tokio mpsc（内存） | 可插拔 MQ：Kafka / NATS JetStream / 内存 |
| **锁 / 选举** | 本地 | Raft 共识（fencing token） |
| **Worker** | 1 个进程 | N 个节点，同一二进制；任意节点注册到任意已知节点 |
| **任务分发** | — | Cron 按 `hash(account)` 归属 + MQ 消费亲和 |
| **代码改动** | 无 | 加一行 `.cluster(ClusterConfig::…)` |

开启内嵌集群（无需外部协调器）：

```rust
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;
```

数据面的消息队列独立于控制面选择 —— Kafka（`queue-kafka`）、NATS JetStream（`queue-nats`）或内存。

## 特性开关

均为可选;默认构建是**单机、无 DB**。用 `mocra = { version = "0.4", features = ["…"] }` 开启。

| 特性 | 解锁什么 |
|---|---|
| `dashboard` | 只读可观测 HTTP API + 内置网页面板(`.dashboard(port)`) |
| `cluster-embedded` | 内嵌 Raft + redb 控制面(`.cluster(…)`)—— 无需外部协调器 |
| `store` | DB 驱动的 account × platform × module 模型(sea-orm) |
| `queue-kafka` | Kafka 数据面队列后端 |
| `queue-nats` | NATS JetStream 数据面队列后端 |
| `polars` | DataFrame 支持(`DataFrameStore`、`polars_utils`) |
| `excel` | Excel 解析(calamine → DataFrame),隐含开启 `polars` |
| `js-v8` | 内嵌 V8 JS 运行时,用于解析脚本 |
| `mimalloc` | mimalloc 全局分配器(**默认开启**) |

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

可运行示例见 [`examples/`](examples/) 目录：

- [`examples/spider_quickstart.rs`](examples/spider_quickstart.rs) — 最小 `Spider`（无 DB）
- [`examples/custom_downloader.rs`](examples/custom_downloader.rs) — 实现 `Downloader` trait 并用 `.default_downloader()` 注入（离线、确定性）
- [`examples/dashboard.rs`](examples/dashboard.rs) — 内置可观测 dashboard（`--features dashboard`）
- [`examples/cluster_quickstart.rs`](examples/cluster_quickstart.rs) — 自组网内嵌集群（`--features cluster-embedded`）

进阶 `ModuleTrait` / DAG 用法：见[模块开发](docs/zh/module-development.md)与 [DAG 指南](docs/zh/dag-guide.md)。

## 监控

```bash
# 启动 Prometheus + Grafana
docker compose -f docker-compose.monitoring.yml up -d

# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000
# 指标:      http://localhost:12800/metrics
```

## 许可证

双许可，任选其一：

- MIT 许可证
- Apache 许可证 2.0

任由你选。
