# 系统架构

本文档介绍 mocra 的组织方式：工作区（workspace）布局、队列驱动的流水线、高层门面（facade）
相对于底层引擎的位置，以及同一份代码如何从单进程扩展到分布式集群。

> **English version：** [docs/architecture.md](../architecture.md)

## 概述

mocra 将数据采集建模为一组由 **DAG 执行引擎**编排的**队列驱动处理阶段**。每个阶段与下一个阶段
之间通过消息队列解耦，因此无论队列是进程内的 Tokio 通道（单节点），还是分布式消息代理
（Kafka / NATS JetStream），同一条流水线都原样运行。横向扩展是配置 / 构建器层面
的事，而非重写。

## 工作区布局

mocra 是一个 Cargo 工作区。整个运行时都在 `mocra-core` 中；你所依赖的 `mocra` crate 只是它之上的
一层**薄门面**。可复用的子系统以独立 crate 发布，依赖方向单一且**无环**——门面依赖核心，核心依赖
各子系统 crate，而子系统 crate 从不反向依赖：

```
mocra  ──▶  mocra-core  ──▶  { mocra-cluster, mocra-dag, mocra-proxy, mocra-store }
（门面）      （运行时）              （可复用子系统，无反向依赖）
```

| Crate | 是什么 |
|---|---|
| [`mocra`](../..) | **薄门面**——`Spider` trait、`Mocra` 构建器、prelude、默认数据出口（sink）。多数用户唯一需要引入的 crate。 |
| [`mocra-core`](../../crates/mocra-core) | 完整运行时：领域模型、下载器、队列、协调（`sync`）、调度器、引擎，以及可观测 / 后台管理 HTTP API。 |
| [`mocra-cluster`](../../crates/mocra-cluster) | 内嵌控制面：Raft + redb——选主、带栅栏（fencing）的分布式锁、成员管理、分区归属，**无需外部协调器**。 |
| [`mocra-dag`](../../crates/mocra-dag) | 通用分布式 DAG 执行引擎，与爬虫零耦合。 |
| [`mocra-proxy`](../../crates/mocra-proxy) | 配置驱动的代理池 / 管理器（独立可用）。 |
| [`mocra-store`](../../crates/mocra-store) | 多租户 sea-orm 实体模型（账号 × 平台 × 模块），位于 `store` 特性之后。 |

`mocra-core` 无条件引入 `mocra-proxy` 与 `mocra-dag`，仅在启用 `store` / `cluster-embedded` 特性时
才引入 `mocra-store` / `mocra-cluster`。由于各子系统 crate 不反向依赖核心，每一个都可以独立理解、
测试与复用。

## 门面与引擎

进入运行时有两条路径，它们最终都编译到同一个引擎：

- **`Spider` 门面**——覆盖 80% 场景的路径。实现一个 [`Spider`](../../src/facade.rs)（一个 `name`、
  一个播种 URL 的 `start`、一个产出类型化数据的 `parse`），用
  `Mocra::builder().spider(spider, sink)` 注册，再调用 `.run()`。类型化数据通过
  [`DataSink`](../../src/facade.rs) 交付到你的代码——用 `on_item(...)` 传闭包、用
  `ChannelSink::new(tx)` 传 channel，或自行 `impl DataSink`。所需一切都从 `mocra::prelude` 重导出。

- **底层引擎**——`mocra-core` 中的 `ModuleTrait` / `ModuleNodeTrait`。这是进阶路径，用于带登录、
  分页、自定义中间件与手写 DAG 的多节点流水线。启用 `store` 特性即可使用账号 × 平台 × 模块模型。

在内部，一个 `Spider` 会被适配成**单节点模块**，并编译进进阶路径所用的同一个引擎：`Spider::start`
成为该节点的 `generate()`（播种请求），`Spider::parse` 成为其 `parser()`（产出数据 + 追加后继请求）。
选择门面在运行时没有任何额外开销——它只是同一套机制之上更薄的表层。

底层 trait 详见[模块开发](module-development.md)，多节点图详见 [DAG 执行指南](dag-guide.md)。

## 队列驱动的流水线

流水线完全由队列驱动。一个任务扇出为多个请求，请求被下载为响应，响应被解析为数据项与后继任务
——每个阶段都通过队列交给下一个阶段：

```
┌─────────────────────────────────────────────────────────┐
│                         Engine                          │
│                                                         │
│  TaskEvent ──▶ generate() ──▶ download() ──▶ parser()  │
│      │              │              │              │      │
│  [Task Q]     [Request Q]   [Response Q]   [Parser Q]  │
│                                                  │      │
│                              ┌────────────┬──────┘      │
│                              ▼            ▼             │
│                         [Data Store]  [Next Node]       │
│                                       [Error Q → DLQ]  │
└─────────────────────────────────────────────────────────┘
```

| 阶段 | 输入 | 输出 | 发生了什么 |
|---|---|---|---|
| **任务** | `TaskEvent` | `Request` 流 | 解析模块 / DAG 节点，调用 `generate()` 产出种子（或后继）请求 |
| **下载** | `Request` | `Response` | 通过当前下载器抓取（默认 reqwest，或自定义下载器），支持代理、限速、会话 |
| **解析** | `Response` | `TaskOutputEvent` | 调用 `parser()`；将产出数据路由到 sink / 数据存储，后继任务路由到下一节点，失败路由到错误队列 |
| **错误** | 错误事件 | 重试或终止 | 基于阈值的重试与退避；耗尽后进入死信队列（DLQ） |

关键特性在于：**每个阶段都由消息队列解耦。** 单节点模式下队列是本地 Tokio 通道；分布式模式下则是
Kafka / NATS JetStream——**同一份代码，零改动**。变化的只是边界背后的队列后端。

## 单节点 vs 分布式

有两样东西各自独立扩展：**控制面**（谁做主、谁持锁、谁拥有哪个分区）与**数据面**（在各阶段之间
传递任务的消息队列）。

| | 单节点 | 内嵌集群（`cluster-embedded`） |
|---|---|---|
| **控制面** | 进程内 | 内嵌 **redb + Raft**——选主 / 带栅栏的锁 / 成员管理 / 分区归属，**无需外部协调器** |
| **数据面（队列）** | Tokio mpsc（内存） | 可插拔 MQ：Kafka / NATS JetStream / 内存 |
| **锁 / 选主** | 本地 | Raft 共识（栅栏令牌） |
| **Worker** | 1 个进程 | N 个节点，同一二进制；把任意节点注册到任意已知节点 |
| **工作分发** | — | 按 `hash(account)` 归属的 cron + MQ 消费者亲和 |
| **代码改动** | 无（门面默认） | 增加 `.cluster(ClusterConfig::…)` |

两种具体拓扑：

- **单节点（门面默认）。** 不用 `.cluster(...)`、不用 `.from_toml(...)`：构建器使用内存、单进程、
  零基础设施的配置。引擎自动为每个 spider 播种，并在空闲时停止——非常适合一次性抓取与开发。
  无数据库、无外部服务。

- **内嵌分布式控制面。** 启用 `cluster-embedded`，在首节点加 `.cluster(ClusterConfig::bootstrap(...))`，
  其余节点加 `.cluster(ClusterConfig::join(...))`。协调（选主、锁、成员管理、分区归属）运行在内嵌的
  **redb + Raft** 上——**无需外部协调器**。把任意节点注册到任意已知节点即可组网。

在上述所有分布式场景中，**数据面队列都独立于控制面选择**——内存、Kafka
（`queue-kafka`）或 NATS JetStream（`queue-nats`）。配了分布式 MQ 时，任务按账号亲和
（`hash(account)`）扇出到各节点；用内存队列时，工作留在播种节点。

各拓扑的具体步骤与命令见[部署指南](deployment.md)。

## DAG 编排

每个模块都会被编译为一个 DAG。一个 `Spider` 编译为单节点；底层路径可声明线性链（`add_step()`）
或自定义的扇出 / 汇合图（`dag_definition()`）：

```
       ┌── branch_a ──┐
start ─┤               ├── merge
       └── branch_b ──┘
```

DAG 的**拓扑是静态的**（初始化时确定）；每个节点的**执行次数是动态的**，由流经队列的解析事件数量
驱动。定义、路由与推进 / 回退门控详见 [DAG 执行指南](dag-guide.md)。

## 可观测与后台管理

启用 `dashboard` 特性并调用 `.dashboard(port)`，引擎便会托管一套**只读可观测 HTTP API**，外加一个
**内置的单文件 Web UI**（无需任何前端构建）：

- `GET /`——内置 dashboard 页面（指标 / 日志 / 任务 / 性能）
- `GET /metrics`——Prometheus 指标（统一的 `mocra_*` 指标族）
- `GET /health`——健康检查
- `GET /observability/{engine,cluster,system,logs}`——队列 / 引擎快照、Raft 集群状态
  （单机时为 `null`）、主机 CPU/内存/swap，以及近期结构化日志

只读端点（`/`、`/metrics`、`/health`、`/observability/*`）已开启 CORS 且免 API key，独立前端可跨域
消费；写操作端点（`/control/*`、`/start_work`）仍需鉴权。完整路由表见 [API 参考](api-reference.md)，
Prometheus/Grafana 栈见[部署指南](deployment.md#监控)。

## 相关指南

- [快速上手](getting-started.md)——安装与第一个 spider
- [模块开发](module-development.md)——`ModuleTrait` / `ModuleNodeTrait`，进阶路径
- [DAG 执行指南](dag-guide.md)——扇出 / 汇合、路由、推进门控
- [中间件](middleware-guide.md)——下载、数据、存储中间件
- [配置参考](configuration.md)——完整 TOML 参考
- [部署指南](deployment.md)——单节点、内嵌集群、监控
