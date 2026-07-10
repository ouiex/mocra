# 03 · 集群架构

> 决策:**控制面用内嵌 redb + Raft(强一致、自组网、零外部依赖);数据面保留多消息队列可插拔(用户自选 Kafka / NATS / Redis 搬运与消费数据)。**
>
> 拓扑采用**方案 A:小 Raft 投票核心(3~5)+ 多 worker**(见 [重构文档索引 · 关键决策](README.md))。

## 为什么分两个平面

Redis 今天同时承担了两类**性质完全不同**的职责:

- **控制面(低频、要强一致)** —— leader 选举、分布式锁/fencing、成员注册、少量共享配置、cron 归属。这些天然适合 Raft。
- **数据面(高频、吞吐导向、只需最终一致)** —— 任务/请求/响应队列、限流、缓存、去重。这些是 Raft 的死穴:Raft 所有写入串行经单一 leader、复制到多数派才提交,把 2 万容量的队列灌进去会直接把日志撑爆、被单 leader 吞吐卡死。

> **旁证:Kafka 自己**从 ZooKeeper 迁到 KRaft(内嵌 Raft),但 Raft **只管集群元数据**,消息日志走分区 + 每分区 leader-follower 复制,**不进全局共识**。本设计同款思路。

```
┌─────────────────────────────────────────────────────────────┐
│  控制面 CONTROL PLANE   redb + openraft · 投票核心 3~5 · 强一致 │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐              │
│  │ node-1     │  │ node-2     │  │ node-3     │              │
│  │ LEADER     │◀▶│ follower   │◀▶│ follower   │              │
│  └────────────┘  └────────────┘  └────────────┘              │
│  成员/心跳 · leader 选举 · 锁&fencing · 分区归属表 · cron 归属  │
│  · 模块/配置注册表                                            │
└───────────────────────────┬─────────────────────────────────┘
                            │  分区归属表 partition → owner
              ▲ 领分区/报心跳/拿锁 │ 下发归属/再平衡 ▼
┌───────────────────────────┴─────────────────────────────────┐
│  数据面 DATA PLANE   可插拔 MQ · 多 worker · 高吞吐最终一致    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                    │
│  │ worker A │  │ worker B │  │ worker C │  … learner/client  │
│  └──────────┘  └──────────┘  └──────────┘                    │
│  队列后端(择一): Kafka / NATS / Redis Streams / … / in-mem  │
└─────────────────────────────────────────────────────────────┘
```

## 职责划分:Redis 那一堆活分别落到哪

关键在于:队列搬到用户的 MQ 后,Redis 还兼着的**去重 / 限流 / 会话 / 缓存**既不在 MQ、也不该进 Raft 日志。它们归**第三处——由分区归属表授权的 owner 节点本地 redb**。

| 职责 | 归属 | 机制 |
|---|---|---|
| 集群成员 / 心跳注册 | **控制面** | Raft 成员变更(openraft)。心跳 = 状态机里的租约,过期即判失联 |
| Leader 选举 | **控制面** | Raft 自带 leader,**免费**;cron 归属直接复用 |
| 分布式锁 / fencing | **控制面** | 线性一致租约 + fencing token(取 Raft 日志 index,天然单调) |
| 分区归属表 | **控制面** | `partition → owner`,失败即再平衡;两平面衔接核心 |
| 模块 / 配置注册表 | **控制面** | 小体量强一致:模块启用/版本、共享配置传播 |
| 任务/请求/响应队列 | **数据面** | 用户选定 MQ 搬运 + 消费组 + ack/claim + DLQ。**框架不重建恢复语义** |
| 数据出口(消费数据) | **数据面** | 解析出的 Item 经 `DataSink` 落到你选的 MQ / 库 / channel |
| 去重 | **本地(redb)** | owner 节点本地布隆/集合,按分区权威去重。爬虫容忍偶发重复,**绝不逐 URL 进 Raft** |
| 会话 / Cookie | **本地(redb)** | 粘在 account 分区的 owner 节点,他人路由过去 |
| 限流 | **本地** | 按存活成员数分摊(全局额 / N),成员变更时刷新。失去原子全局计数,换热路径零协调 |
| 缓存 | **本地** | 每节点本地 redb / 内存;仅极少权威位进 Raft KV |

> **纪律(红线):只有小体量、强一致的控制数据进 Raft 复制日志。** 守住这条,共识核心永远轻,快照/压缩压力可控。

## 分区归属:两个平面如何协同

分区归属是整个设计的枢纽。

1. **划分区** —— 按 `account` / 域名 / URL 哈希把工作切成 N 片(N 远大于节点数,如 256,便于再平衡)。
2. **路由** —— 数据面按分区键投递;消费组让每片被其 owner 消费。
3. **本地授权** —— owner 对自己那片**权威**地去重、持会话(本地 redb),无需跨节点协调。
4. **故障接管** —— 节点掉线 → Raft 心跳租约过期 → leader 把其分区**再平衡**给存活节点 → MQ 消费组随之 rebalance → 新 owner 从 redb/MQ 恢复。fencing token 防老 owner 复活重复处理。

### 实现要点:分区归属可委托给 MQ 消费组

一个务实的细节:**带消费组语义的 MQ(Kafka/NATS/Redis Streams)本身就会做分区分配与再平衡**。因此:

- **首选**:分区键 = `hash(account 或 域名)`,让相关工作 + 其会话落到同一分区。MQ 把分区键映射到自己的分区、消费组把分区分配到节点——**既得 MQ 原生路由,又得 account→节点亲和**(会话粘性),只要 MQ 分区数稳定。此时 Raft **不需要**为消息路由维护归属表,只需管成员/锁/cron/配置,本地状态(去重/会话)的归属直接**从 MQ 分配派生**。
- **需要 Raft 归属表的场景**:后端**无消费组语义**(in-memory / 纯 pub-sub),或要**独立于 MQ 内部**的确定性亲和。此时 leader 计算归属、写 `AssignPartition` 日志,worker watch `partitions/` 前缀,增删自己消费的分区。

> 换言之:归属表是**概念上的枢纽**;**实现上**优先复用 MQ 消费组,Raft 归属表作为无消费组后端的兜底 + 本地状态故障接管的权威。

## 集群拓扑(方案 A)与组网

- **投票核心(3~5 节点)** 跑 Raft,组成控制面。共识要小而稳的投票集——**不要把上百个爬虫节点全塞进一个 Raft 组**,否则共识延迟和成员抖动会拖垮它。
- **众多 worker** 作为 learner / 纯客户端注册进来:领分区、报心跳、干采集,不参与投票。这是 etcd/TiKV 的成熟模型。

### 「注册任意节点即入网」流程

```
新节点(带种子地址)
   │ POST /cluster/join {node_id, addr, role}
   ▼
任意现有节点 ──转发──▶ Leader (openraft 返回 leader 信息,API 层转发)
                          │ 1. add_learner(node) → 复制日志追平状态
                          │ 2. 若 role=voter 且投票集未满 → change_membership 提升
                          │    否则保持 learner/worker
                          ▼
                     归属表分配若干分区 ──▶ 数据面 MQ 消费组 rebalance
                                              ▼
                                        新节点开始消费
```

可选:叠一层 gossip(`chitchat` / `foca`)做成员发现与快速故障探测,Raft 仍是**投票成员的唯一真相源**。

## 控制面接口:RaftBackend → CoordinationBackend

现有 `sync/backend.rs` 的 `CoordinationBackend` 就是**现成的缝**——实现一个 `RaftBackend` 即整体替掉 Redis 协调。方法级映射:

| CoordinationBackend 方法 | Raft/redb 实现 | 一致性 |
|---|---|---|
| `set(key, value)` | `raft.client_write(Cmd::Set{key, value})` | 线性一致(过共识) |
| `get(key)` | 读本地状态机(可选 read-index 做线性读;默认 stale-ok 本地读) | 可调 |
| `cas(key, old, new)` | `client_write(Cmd::Cas{key, old, new})`,状态机条件应用返回 bool | 线性一致 |
| `acquire_lock(key, val, ttl)` | `client_write(Cmd::AcquireLock{key, holder, expire_at})`,状态机判空闲/过期后授予 + 返回 fencing token | 线性一致 |
| `renew_lock(key, val, ttl)` | `client_write(Cmd::RenewLock)`,仍为持有者则续期 | 线性一致 |
| `publish(topic, payload)` | 控制事件写状态机 topic + 变更源通知(高频事件不走此处) | —— |
| `subscribe(topic)` | 注册对状态机变更源(change feed)在该 topic/前缀的 watch | —— |

> `LeaderElector`(`sync/leader.rs`)本就走 `CoordinationBackend`——Raft 自带 leader,选举变成原生/免费。

### 状态机命令(Raft 日志条目)

```rust
enum Cmd {
    // KV & 锁
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Cas { key: Vec<u8>, old: Option<Vec<u8>>, new: Vec<u8> },
    AcquireLock { key: String, holder: NodeId, expire_at: u64 },
    RenewLock   { key: String, holder: NodeId, expire_at: u64 },
    ReleaseLock { key: String, holder: NodeId },
    // 成员 & 心跳
    Heartbeat { node: NodeId, at: u64 },
    // 分区(无消费组后端时)
    AssignPartition { partition: u32, owner: NodeId, epoch: u64 },
    // 配置注册表
    RegisterModule { name: String, version: i32, enabled: bool },
}
```

### redb 状态机 schema

redb 作为 openraft 的存储层,存两类数据:**Raft 日志/快照** 与 **状态机表**。状态机按 keyspace 组织:

| 表(keyspace) | key → value | 说明 |
|---|---|---|
| `members` | `node_id` → `NodeInfo{addr, role, last_hb}` | 成员与心跳租约 |
| `locks` | `lock_key` → `Lock{holder, fencing_token, expire_at}` | 分布式锁 |
| `kv` | `key` → `bytes` | 通用小配置 KV |
| `partitions` | `partition_id` → `Assignment{owner, epoch}` | 归属表(无消费组后端) |
| `cron` | `module` → `owner_node` | cron 调度归属 |
| `modules` | `name` → `{version, enabled}` | 模块注册表 |

> fencing token 直接取授予时的 **Raft 日志 index**——天然单调、全局唯一。worker 行动时携带 token,owner 变更后旧 token 被拒。

## 数据面接口:MqBackend(保留多实现)

`queue/lib.rs` 的 `MqBackend` 保留并扩展。`Message` 自带 ack 回调,映射到各 MQ 的原生语义:

| 后端 | 传输/分区 | ack / 恢复 | 备注 |
|---|---|---|---|
| **Kafka** | 分区日志 + 消费组 | offset commit;rebalance | 高吞吐首选;已有实现 |
| **NATS JetStream** | stream + durable consumer | ack/nak;无 ack 重投 | 轻量 |
| **Redis Streams** | 消费组 | XACK / XCLAIM 恢复卡住消息 | mocra 现用;控制面去 Redis 后此处仍可选用 |
| **Pulsar** | topic + subscription | ack / negative-ack | 可选 |
| **RabbitMQ** | queue + DLX | consumer ack/nack;requeue | 可选 |
| **in-memory** | Tokio mpsc | 无持久/恢复 | 单机开发默认 |
| **embedded(未来)** | 本地 redb 分区队列 + P2P | 框架自实现 ack/claim | 「零外部依赖数据面」选项,后续可做 |

> 框架**不重建**队列恢复语义——交给所选 MQ 的原生能力。`send_to_dlq` / `read_dlq` 已在 trait 中。

## 数据出口:DataSink

见 [02 · 目标 API › DataSink](02-target-api.md)。这是「用户用不同 MQ 消费数据」的第二条缝:`emit` 出的类型化 Item → 可选数据中间件 → sink(`KafkaSink` / `NatsSink` / `on_item` / 自定义)。

## Rust 技术栈

| 层 | 选型 | 理由 |
|---|---|---|
| 共识 | `openraft` | Rust 事实标准,异步、存储/网络可插拔,生产验证 |
| 状态机存储 | `redb` | 纯 Rust、ACID、无 C++ 工具链——契合「轻、可嵌入」,避开 RocksDB 的重型 native 构建。需自实现 openraft 存储适配(官方示例为 rocksdb/sled) |
| 控制面接口 | `CoordinationBackend`(现成缝) | 实现 `RaftBackend` 即整体替掉 Redis 协调 |
| 数据面接口 | `MqBackend`(保留多实现) | Kafka / Redis / **NATS(JetStream)** 已实现 |
| 集群 RPC | `tonic`(gRPC)或 `axum` | Raft 节点间通信 + join API;axum 已是依赖,可复用 |
| 成员发现(可选) | `chitchat` / `foca` | gossip 故障探测,Raft 仍为投票真相源 |

## 已解耦(已实现)

`DistributedLockManager`(`utils/redis_lock.rs`)与限流器(`utils/distributed_rate_limit.rs`)原先**直吃 `deadpool_redis::Pool`**,现已接入 `CoordinationBackend`:

- **锁** → `new_with_coordination` 注入协调后端;有则**优先走 Raft**(带续租 + CAS-del 释放),取代无 Redis 时退化的进程内锁。
- **限流** → `share_rps` 在无 Redis、有协调后端时按 `cluster_size()` 分摊全局额;有 Redis 仍走原子全局。

## 实现状态与用法

**控制面已全部 Raft 化并测试**(选举 / 锁+fencing / KV / CAS / 成员 / 分区归属 / 跨节点 pub-sub / 崩溃恢复 / leader 失效重选 / 任意节点受理写)。**数据面**保持 `MqBackend` 可插拔(Kafka / Redis / 内存 / **NATS JetStream** —— 后者已对真实服务器集成测试测通:持久化 + ack + nack 重投/DLQ),任务消息按 `hash(account)` 设分区键实现消费亲和(NATS 下暂为竞争消费,粘账号后续用分区 subject)。

三步起一个自组网集群(无需 Redis):

```rust
use mocra::prelude::*;

// 首个核心节点(自举)
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::bootstrap(1, "127.0.0.1:7001", "./data/n1"))
    .run().await?;

// 其余节点(注册到任意已知节点即入网)
Mocra::builder()
    .spider(MySpider, on_item(|x: Item| async move { /* ... */ }))
    .cluster(ClusterConfig::join(2, "127.0.0.1:7002", "./data/n2", "127.0.0.1:7001"))
    .run().await?;
```

可运行示例见 `examples/cluster_quickstart.rs`。

## 权衡

- **失去原子全局限流计数**(Redis Lua 滑窗)→ 换本地近似分摊(全局额 / 存活节点数);真全局限流需热路径协调往返,通常不划算。
- **运维复杂度**从「跑一个 Redis」变成「运维/排障一套共识」:快照、日志压缩、成员变更安全、慢 follower。对「可嵌入、自组网」目标是净赚,但要睁眼进。

---

上一篇:[02 · 目标 API](02-target-api.md) · 下一篇:[04 · 实施路线](04-roadmap.md)
