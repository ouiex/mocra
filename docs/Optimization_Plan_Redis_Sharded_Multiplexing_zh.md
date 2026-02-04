> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# Redis 队列分片多路复用优化方案 (Sharded Multiplexing)

**日期**: 2026-01-30
**目标**: 进一步优化 Redis 消费吞吐量，解决单线程多路复用器（Single Multiplexer）在高负载下的 CPU 瓶颈问题。

## 1. 问题分析

上一阶段的优化（IO 多路复用）成功解决了 Redis 连接耗尽和死锁问题，将连接数降低到了 1 个。
然而，`RedisQueue` 目前使用**单个**异步任务 (`spawn_multiplexed_listener`) 来处理所有 Topic 和 Shard 的消息监听与反序列化。

### 瓶颈点
1.  **单核 CPU 限制**: 单个 Tokio 任务运行在单个 OS 线程上。当系统吞吐量极高（如数万 TPS）时，消息的反序列化、协议解析和 Channel 发送操作会饱和单个 CPU 核心。
2.  **队头阻塞**: 如果某个 Topic 的消息处理（或反序列化）较慢，会阻塞同一 Loop 中其他 Topic 的消息读取。

## 2. 优化方案：分片多路复用 (Sharded Multiplexing)

将单一的 Listener 拆分为多个（N个）并发 Listener，每个 Listener 负责一部分 Stream Key 的监听。

### 核心设计
1.  **Listener Pool**: 启动固定数量的 Listener 任务（例如 4 个，或等于 CPU 核心数）。
2.  **Key Sharding**: 使用一致性哈希（或简单的取模）将 Stream Key 分配给特定的 Listener。
    - `Listener_ID = CRC32(Stream_Key) % Listener_Count`
3.  **独立连接**: 每个 Listener 持有一个独立的 Redis 连接（或从 Pool 获取并持有）。
    - 总连接数 = Listener_Count (例如 4)。仍然远小于 Topic * Shard * Priority (72+)。

### 预期收益
- **多核并行**: 利用多核 CPU 并行处理消息读取和反序列化，吞吐量理论上随 Listener 数量线性增长。
- **故障隔离**: 某组 Key 的流量突增不会完全阻塞其他组 Key 的消费。

## 3. 实施步骤

1.  修改 `RedisQueue` 结构，增加 `listener_count` 配置（默认 4）。
2.  将 `spawn_multiplexed_listener` 替换为 `spawn_sharded_listeners`。
3.  `spawn_sharded_listeners` 将循环启动 N 个任务。
4.  在每个任务的 Loop 中，根据 `CRC32(key) % N == id` 过滤出本任务负责的 Keys。
5.  执行 `XREADGROUP` 并分发消息。

## 4. 验证计划

1.  运行 `tests_debug` 基准测试，对比 TPS。
2.  监控 `queue_consume_total` 确保所有 Topic 都能被正常消费。
