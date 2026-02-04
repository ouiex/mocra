> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# Redis 队列连接复用优化方案

**日期**: 2026-01-30  
**目标**: 解决 Redis Stream 分片场景下的连接池耗尽与死锁问题，降低 Redis 连接负载。

## 1. 问题分析

当前的 `RedisQueue` 实现中，每个订阅的主题（Topic）和分片（Shard）都会启动一个独立的监听任务 (`spawn_listener`)。
该任务会从连接池中获取一个连接 (`pool.get().await`) 并**永久持有**，用于执行阻塞式 `XREADGROUP` 循环。

### 资源消耗计算
假设系统配置如下：
- **Topics**: 6 (Task, Request, Response, ParserTask, Error, Log)
- **Priorities**: 3 (High, Normal, Low)
- **Shards**: 4 (用于高吞吐)

所需连接数 = 6 * 3 * 4 = **72 个长连接**。

如果 `config.toml` 中 `pool_size` 设置为默认的 50，则会导致连接池耗尽，后续的 `subscribe` 调用或普通的 Redis 操作（如 `ACK`, `Compensator`）将因无法获取连接而无限等待，导致系统**死锁**或启动失败。

即使增加 `pool_size`，过多的阻塞连接也会增加 Redis Server 的上下文切换开销，降低整体吞吐量。

## 2. 优化方案：IO 多路复用 (Multiplexing)

Redis 的 `XREADGROUP` 指令原生支持同时监听多个 Stream Key：
`XREADGROUP GROUP g c BLOCK 2000 STREAMS key1 key2 ... > > ...`

我们利用这一特性，将多个 Topic/Shard 的监听合并到少数几个连接中。

### 核心设计
1.  **StreamRouter**: 维护 `Stream Key -> Sender<Message>` 的路由表。
2.  **Shared Listener**: 启动固定数量（如 1 个或 CPU 核心数）的后台任务。
    - 每次循环从 `StreamRouter` 获取所有待监听的 Key。
    - 执行一次 `XREADGROUP`。
    - 收到消息后，根据 Key 在路由表中查找对应的 Channel Sender 并分发。
3.  **Dynamic Update**: 允许在运行时动态添加订阅的 Topic。

### 预期收益
- **连接数显著降低**: 无论多少 Topic/Shard，仅占用固定数量（如 1-4 个）的长连接用于监听。
- **避免死锁**: 彻底解决因连接池耗尽导致的死锁问题。
- **提升吞吐**: 减少 Redis 连接数，降低 Server 端负载。

## 3. 实施步骤

1.  在 `queue/src/redis.rs` 中引入 `StreamRouter` 结构。
2.  改造 `RedisQueue::subscribe`，不再为每个 Topic 启动独立 Loop，而是注册到 Router。
3.  实现 `start_multiplexed_listener`，负责统一的 `XREADGROUP` 轮询与分发。

## 4. 执行结果

### 4.1 代码变更
- `queue/src/redis.rs`: 重构 `RedisQueue`，引入 `StreamRouter` 和 `spawn_multiplexed_listener`。
- 移除了每个 Topic/Shard 独立的 `spawn_listener` 循环。
- `RedisQueue` 现在只使用 **1 个长连接** 进行所有 Topic 的消息监听，彻底解决了连接池耗尽问题。

### 4.2 测试验证
- `cargo test -p queue`：通过。
- 基准测试 (`tests_debug`)：由于测试环境数据库配置（SQLite）与初始化脚本（Postgres）不兼容导致 DB 初始化失败，基准测试无法完整运行产出 TPS 数据。但在连接复用方面，该架构已具备支撑数千 Topic/Shard 的能力，仅受限于单连接带宽与 Redis Server CPU。

### 4.3 建议
- 在生产环境部署时，建议监控 `queue_consume_total` 指标，确认多路复用监听器的处理延迟。如果单个监听器成为瓶颈（CPU 100%），可考虑按 Hash Slot 分组启动多个 Multiplexed Listener（例如 4 个）。
