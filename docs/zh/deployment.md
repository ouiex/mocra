# 部署指南

本指南介绍如何在生产中运行 mocra：零基础设施的单节点、内嵌 Raft 集群（无需 Redis）、Redis 支撑的
分布式部署，以及监控。

> **English version：** [docs/deployment.md](../deployment.md)

mocra 沿两条相互独立的轴扩展——**控制面**（协调）与**数据面**（消息队列）。请从下面选择一种拓扑；
各部分如何组合见[系统架构](architecture.md)。

## 单节点（零基础设施）

默认方式。无数据库、无 Redis、无消息代理——门面构建一个内存、单进程的引擎。

### 适用场景

- 开发与测试
- 一次性抓取与独立脚本
- 单进程即可容纳的低流量采集

### 配置

依赖 mocra 并运行一个 spider——这就是全部部署：

```toml
[dependencies]
mocra = "0.4"
```

```rust
use mocra::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    Mocra::builder()
        .spider(MySpider, on_item(|item: Item| async move {
            // 落库 / 打印 / 转发
        }))
        .run()          // 不用 .from_toml、不用 .cluster → 内存单节点
        .await
}
```

```bash
cargo run --release
```

### 运行效果

- 队列是进程内的 Tokio mpsc 通道。
- 协调在进程内（无分布式锁）。
- 未提供 `.from_toml` 时，构建器会自动为每个 spider 播种，并在**空闲时停止**——非常适合一次性运行。
  持续产生任务的 spider 会持续运行。

无需 Postgres，也无需 Redis。账号 × 平台 × 模块的数据库模型通过 `store` 特性与底层 `ModuleTrait`
路径按需启用——`Spider` 门面并不需要它。

## 内嵌集群（`cluster-embedded`，无需 Redis）

运行一个**自组网的 Raft 集群**，其控制面是内嵌的 **redb + Raft**——选主、带栅栏（fencing）的分布式锁、
成员管理与分区归属，**无需外部协调器**。

```toml
mocra = { version = "0.4", features = ["cluster-embedded"] }
```

### 注册节点

首个节点**自举**（bootstrap）一个新集群；其余每个节点通过任意已知节点的地址（一个「种子」）**加入**
（join）。把任意节点注册到任意已在网中的节点：

```rust
// 首个核心节点——自举一个新集群（无种子）
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;

// 其余节点通过种子地址加入
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::join(2, "127.0.0.1:7002", "./data/n2", "127.0.0.1:7001"))
    .run().await?;
```

`ClusterConfig::bootstrap(node_id, http_addr, data_dir)` 启动一个新集群；`ClusterConfig::join` 额外
带上任意已有节点的种子地址。`data_dir` 是 redb 状态机 + Raft 日志目录，因此要给每个节点各自的路径。

运行内置示例即可拉起三个节点（三个终端——首个自举，其余通过它加入）：

```bash
# 首个核心节点（无种子 → 自举）
cargo run --example cluster_quickstart --features cluster-embedded -- 1 127.0.0.1:7001
# 其余节点（第三个参数 = 种子 = 首节点地址 → join）
cargo run --example cluster_quickstart --features cluster-embedded -- 2 127.0.0.1:7002 127.0.0.1:7001
cargo run --example cluster_quickstart --features cluster-embedded -- 3 127.0.0.1:7003 127.0.0.1:7001
```

### 容器化部署

若要跨节点复用同一镜像，用 `ClusterConfig::from_env()`，仅通过环境变量区分：

| 变量 | 含义 |
|---|---|
| `MOCRA_NODE_ID` | 节点唯一 id（u64，必填） |
| `MOCRA_HTTP_ADDR` | 本节点对外地址，如 `10.0.0.4:7001`（必填） |
| `MOCRA_DATA_DIR` | redb + Raft 目录（默认 `./mocra-data/node-{id}`） |
| `MOCRA_SEEDS` | 逗号分隔的种子地址；为空 = 自举，非空 = 加入 |

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::from_env()?)
    .run().await?;
```

### 数据面

内嵌集群只是控制面。**队列后端独立选择**：

- **内存**（默认）——进程内队列不跨节点，播种的工作留在产生它的节点上。
- **Kafka / NATS JetStream / Redis Streams**——任务扇出到各节点，按 `hash(account)` 路由以实现消费者
  亲和（同一账号 → 同一消费者）。启用 `queue-kafka` 或 `queue-nats`，并在 TOML 中配置该后端
  （见[配置参考](configuration.md)）。

## Redis 支撑的分布式控制面

如果你已经在运行 Redis（或相比内嵌集群更偏好它），可通过 `.from_toml(cfg)` 加载带 `[cache.redis]` 段的
TOML 配置，让协调改走 Redis 而非 Raft：

```toml
# config.toml
[cache]
ttl = 60

[cache.redis]
url = "redis://redis-host:6379"
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .from_toml("config.toml")   // 存在 [cache.redis] → Redis 支撑的协调
    .run().await?;
```

用**相同配置**运行多个实例；锁 / 选主经 Redis 完成。与内嵌集群一样，**数据面队列独立选择**——
Redis Streams、Kafka（`queue-kafka`）、NATS JetStream（`queue-nats`）或内存：

```toml
# 用 Redis Streams 作为数据面队列
[channel_config.redis]
url = "redis://redis-host:6379"

# 或使用 Kafka
[channel_config.kafka]
brokers = "kafka-host:9092"
```

```bash
# 每个节点用同一二进制、同一 config.toml
cargo run --release
```

完整的 `[cache]` 与 `[channel_config]` 参考见[配置参考](configuration.md)。

## 拓扑选择

| | 单节点 | 内嵌集群 | Redis 支撑 |
|---|---|---|---|
| 特性开关 | 无 | `cluster-embedded` | 无（配置驱动） |
| 外部基础设施 | **无** | **无** | Redis |
| 协调 | 进程内 | redb + Raft | Redis |
| 启用方式 | 门面默认 | `.cluster(ClusterConfig::…)` | `.from_toml(cfg)` + `[cache.redis]` |
| 数据面队列 | 内存 | 内存 / Kafka / NATS / Redis | 内存 / Redis / Kafka / NATS |
| 最适合 | 开发、一次性、低流量 | 自包含集群 | 已在运行 Redis 的团队 |

## 监控

### Dashboard、指标与 Web UI

启用 `dashboard` 特性并调用 `.dashboard(port)`。引擎会托管一套只读可观测 HTTP API **以及**一个内置的
单文件 Web UI——浏览器打开该端口即见 **指标 / 日志 / 任务 / 性能**，无需任何前端构建：

```toml
mocra = { version = "0.4", features = ["dashboard"] }
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .dashboard(8080)   // GET / → Web UI；  /metrics、  /health、  /observability/*
    .run().await?;
```

```bash
cargo run --example dashboard --features dashboard   # 然后打开 http://127.0.0.1:8080
```

端点：

| 路由 | 鉴权 | 说明 |
|---|---|---|
| `GET /` | 无 | 内置 dashboard Web UI |
| `GET /metrics` | 无（限速） | Prometheus 指标（`mocra_*` 指标族） |
| `GET /health` | 无 | 健康 / 存活探针 |
| `GET /observability/engine` | 无 | 引擎 + 各队列快照 |
| `GET /observability/cluster` | 无 | Raft 集群状态（单机时为 `null`） |
| `GET /observability/system` | 无 | 主机 CPU / 内存 / swap |
| `GET /observability/logs?limit=N` | 无 | 近期结构化日志 |
| `POST /control/pause` \| `resume` | API key | 暂停 / 恢复引擎 |
| `POST /start_work` | API key | 手动注入任务 |

只读端点已开启 CORS 且免 API key（独立前端可跨域消费）；写操作端点仍需鉴权。详见
[API 参考](api-reference.md)。

### Prometheus + Grafana

仓库自带一套监控栈。`docker-compose.monitoring.yml` 运行 Prometheus 与 Grafana；Prometheus 采集你引擎的
`/metrics`（采集目标配置在 `monitoring/prometheus/prometheus.yml`，容器内通过 `host.docker.internal`
访问宿主机）：

```bash
docker compose -f docker-compose.monitoring.yml up -d

# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000   （admin / admin）
# 指标:       http://localhost:8080/metrics   （即你的 .dashboard(port)）
```

预置的 Grafana 数据源与仪表盘位于 `monitoring/grafana/provisioning`。

### 日志

mocra 使用 `log` / `tracing` 生态。通过 `RUST_LOG` 配置：

```bash
RUST_LOG=mocra=info cargo run          # 或 mocra=debug 获取更多细节
```

启用 `.dashboard(...)` 时，近期日志还会被采集进内存环形缓冲，并在 `GET /observability/logs` 提供。

## 生产环境检查清单

- [ ] 选择一种拓扑：单节点、内嵌集群（`cluster-embedded`），或 Redis 支撑（`.from_toml` +
      `[cache.redis]`）。
- [ ] 若为集群，给每个节点分配唯一的 `node_id` 和各自的 `data_dir`；其余节点通过已知地址加入
      （容器中用 `ClusterConfig::from_env()`）。
- [ ] 选择数据面队列：内存、Redis Streams、Kafka（`queue-kafka`）或 NATS JetStream（`queue-nats`）。
- [ ] 启用 `dashboard` 并暴露 `/metrics`；用 Prometheus 采集、用 Grafana 绘图。
- [ ] 将 `RUST_LOG` 设为合适的级别。
- [ ] 若在生产中注入任务或暂停 / 恢复，用 API key 保护写操作端点（`[api] api_key`）。
- [ ] 在 TOML 中调优重试 / 错误阈值与限速（`[crawler]`、`[download_config]`）——见
      [配置参考](configuration.md)。
- [ ] 生产构建保持开启 `mimalloc` 特性（它是默认特性）。

## 相关指南

- [系统架构](architecture.md)——控制面 vs 数据面、工作区布局
- [配置参考](configuration.md)——完整 TOML 参考（`[cache]`、`[channel_config]`、`[api]`）
- [API 参考](api-reference.md)——HTTP 控制面与指标
- [快速上手](getting-started.md)——安装与第一个 spider
