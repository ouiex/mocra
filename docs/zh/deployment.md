# 部署指南

本指南介绍如何在生产中运行 mocra：零基础设施的单节点、内嵌 Raft 集群、跨节点数据面队列，以及监控。

> **English version：** [docs/deployment.md](../deployment.md)

mocra 沿两条相互独立的轴扩展——**控制面**（协调）与**数据面**（消息队列）。请从下面选择一种拓扑；
各部分如何组合见[系统架构](architecture.md)。

## 单节点（零基础设施）

默认方式。无数据库、无外部服务、无消息代理——门面构建一个内存、单进程的引擎。

### 适用场景

- 开发与测试
- 一次性抓取与独立脚本
- 单进程即可容纳的低流量采集

### 配置

依赖 mocra 并运行一个 spider——这就是全部部署：

```toml
[dependencies]
mocra = "0.6"
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

无需 Postgres，也无需外部服务。账号 × 平台 × 模块的数据库模型通过 `store` 特性与底层 `ModuleTrait`
路径按需启用——`Spider` 门面并不需要它。

## 内嵌集群（`cluster-embedded`）

运行一个**自组网的 Raft 集群**，其控制面是内嵌的 **redb + Raft**——选主、带栅栏（fencing）的分布式锁、
成员管理与分区归属，**无需外部协调器**。

```toml
mocra = { version = "0.6", features = ["cluster-embedded"] }
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

从 0.5.2 起，使用已初始化的数据目录重启时会直接恢复 Raft 状态，不再重新初始化或加入集群。
保留原来的节点 id 和对外地址；种子仅用于尚未初始化的节点。启动失败（包括 State 或 Engine
初始化错误）时，会在返回错误前关闭 HTTP 监听和 Raft 运行时，允许立即使用同一地址和目录重试。

正常停止时，引擎也会在返回前等待管理 API 和自身启动的后台任务退出。API 先留出 1 秒等待请求
完成，随后关闭已接受的连接，再最多等待 1 秒，避免未完成的请求头或 body 占用 API 端口和
Raft 数据目录；下载器清理任务与 zombie cleaner 会随引擎一起停止。

显式停止（包括 Ctrl+C）会取消未完成的处理任务，并等待这些任务销毁后才结束引擎运行；不会确认
被取消的消息，消息代理按现有队列规则重新投递。输入通道自然关闭且未收到停止信号时，则等待
已接收的任务完成。使用底层 Engine API 时，请在调用 `shutdown()` 后继续等待 `start()` 返回，
再复用集群数据目录。
Raft 停止还会等待状态机、快照和日志复制任务释放存储句柄。

组合使用 `.from_toml(...)` 与 `.dashboard(port)` 时，仅覆盖 API 端口，保留配置中的
`api_key` 和 `rate_limit`。

恢复时最多等待 10 秒确认当前领导权，再决定由哪个节点注入 Spider 初始任务。仅记住 leader id
不足以判定就绪：恢复的 leader 要确认 quorum，follower/learner 要收到 leader 的新一轮已提交投票。
HTTP 关闭先留出 1 秒等待请求完成，随后关闭已接受的连接，再最多等待 1 秒；未完成的 HTTP
请求头或 body 不会阻塞恢复清理。

新节点会按顺序尝试配置的种子。种子是 follower 时，会把加入的 POST 请求重定向到其已知的
leader。每次 HTTP 加入请求的连接超时为 2 秒，总超时为 10 秒，最多重定向 5 次。
注册成功后，启动最多等待 10 秒，直到本节点收到成员配置并得知 leader；失败信息包含种子地址和原因。

**加入的节点是 learner，不是 voter。** 下方三进程示例只有一个 voter 和两个 learner；唯一的
voter 故障后无法选出新 leader。若需要选主故障转移，应通过底层
`RaftControlPlane::change_membership` API，在加入 learner 后显式配置三个 voter。
builder 不会自动晋升节点。

`mocra-cluster` 0.5.0 要求持久化的 Raft 状态机元数据。0.4.1 写入的数据目录没有这些元数据，
尤其在日志清理后无法安全地直接重启。升级前应备份每个节点的目录并迁移状态；新版本会拒绝
原地启动旧目录，而不会把它当作空集群。0.4.1 不支持滚动升级到此版本。

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
- **Kafka / NATS JetStream**——消息代理的消费者在节点之间分担任务。启用 `queue-kafka` 或
  `queue-nats` 并在 TOML 中配置后端（见[配置参考](configuration.md)）。当前不保证跨阶段的账号亲和；
  请求回退和会话仍使用节点本地缓存，依赖这些能力的工作负载需要另行设计亲和路由或共享状态。

### 失败处理与当前限制

引擎策略将消息转入 DLQ 时，只有 DLQ 写入成功后才确认原消息。写入失败会记录错误，并保留
原消息未确认状态：NATS 可在确认超时后重新投递；Kafka 保留未提交的 offset，供消费者重启后
重新读取。这属于至少一次处理，数据输出端需要容忍重复。

本补丁尚未解决阶段交接的持久化：消息成功进入本地通道后，上游确认可能早于下游处理或 broker
持久化，进程崩溃仍存在丢失窗口。分区故障接管租约、运行时 fencing token 传递、控制状态的
线性一致读取，以及 Raft pub/sub 的原子发布和历史保留，也需要分别处理。

## 跨节点数据面（Kafka / NATS）

内嵌集群是控制面，进程内队列永远不会离开产生该工作的进程。要把任务扇出到各节点，请通过
`.from_toml(cfg)` 加载 TOML 配置，把**数据面队列**指向共享消息代理——Kafka（`queue-kafka`）或
NATS JetStream（`queue-nats`）。消息代理采用竞争消费，不保证同一账号的不同阶段落在同一节点：

```toml
# config.toml —— 用 Kafka 作为数据面队列
[channel_config.kafka]
brokers = "kafka-host:9092"

# 或使用 NATS JetStream
# [channel_config.nats]
# url = "nats://nats-host:4222"
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .from_toml("config.toml")            // 数据面队列（Kafka / NATS）
    .cluster(ClusterConfig::from_env()?) // 控制面（选主、锁）
    .run().await?;
```

```bash
# 每个节点用同一二进制、同一 config.toml
cargo run --release --features "cluster-embedded queue-kafka"
```

完整的 `[channel_config]` 参考见[配置参考](configuration.md)。

## 拓扑选择

| | 单节点 | 内嵌集群 |
|---|---|---|
| 特性开关 | 无 | `cluster-embedded` |
| 外部基础设施 | **无** | **无** |
| 协调 | 进程内 | redb + Raft |
| 启用方式 | 门面默认 | `.cluster(ClusterConfig::…)` |
| 数据面队列 | 内存 | 内存 / Kafka / NATS |
| 最适合 | 开发、一次性、低流量 | 自包含集群、跨节点扩展 |

## 监控

### Dashboard、指标与 Web UI

启用 `dashboard` 特性并调用 `.dashboard(port)`。引擎会托管一套只读可观测 HTTP API **以及**一个内置的
单文件 Web UI——浏览器打开该端口即见 **指标 / 日志 / 任务 / 性能**，无需任何前端构建：

```toml
mocra = { version = "0.6", features = ["dashboard"] }
```

```rust
Mocra::builder()
    .spider(MySpider, on_item(|_: Item| async move { /* ... */ }))
    .dashboard(12800)   // GET / → Web UI；  /metrics、  /health、  /observability/*
    .run().await?;
```

```bash
cargo run --example dashboard --features dashboard   # 然后打开 http://127.0.0.1:12800
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
# 指标:       http://localhost:12800/metrics   （即你的 .dashboard(port)）
```

预置的 Grafana 数据源与仪表盘位于 `monitoring/grafana/provisioning`。

### 日志

mocra 使用 `log` / `tracing` 生态。通过 `RUST_LOG` 配置：

```bash
RUST_LOG=mocra=info cargo run          # 或 mocra=debug 获取更多细节
```

启用 `.dashboard(...)` 时，近期日志还会被采集进内存环形缓冲，并在 `GET /observability/logs` 提供。

## 生产环境检查清单

- [ ] 选择一种拓扑：单节点或内嵌集群（`cluster-embedded`）。
- [ ] 若为集群，给每个节点分配唯一的 `node_id` 和各自的 `data_dir`；其余节点通过已知地址加入
      （容器中用 `ClusterConfig::from_env()`）。
- [ ] 选择数据面队列：内存、Kafka（`queue-kafka`）或 NATS JetStream（`queue-nats`）。
- [ ] 启用 `dashboard` 并暴露 `/metrics`；用 Prometheus 采集、用 Grafana 绘图。
- [ ] 将 `RUST_LOG` 设为合适的级别。
- [ ] 若在生产中注入任务或暂停 / 恢复，用 API key 保护写操作端点（`[api] api_key`）。
- [ ] 在 TOML 中调优重试 / 错误阈值与限速（`[crawler]`、`[download_config]`）——见
      [配置参考](configuration.md)。
- [ ] 生产构建保持开启 `mimalloc` 特性（它是默认特性）。

## 相关指南

- [系统架构](architecture.md)——控制面 vs 数据面、单包布局
- [配置参考](configuration.md)——完整 TOML 参考（`[cache]`、`[channel_config]`、`[api]`）
- [API 参考](api-reference.md)——HTTP 控制面与指标
- [快速上手](getting-started.md)——安装与第一个 spider
