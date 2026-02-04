> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# Redis Lua 批处理与集群适配优化方案 (Redis Lua Batching & Cluster Readiness)

**日期**: 2026-01-31
**状态**: 已完成 (Completed)

## 1. 背景与问题 (Background & Problem)

当前 `publish_batch` 使用 Redis Pipeline 发送多个 `XADD` 命令。虽然 Pipeline 减少了 RTT，但在高并发场景下：
1.  **协议开销**: 每个 `XADD` 都有独立的请求和响应头，占用带宽。
2.  **原子性**: Pipeline 不保证原子性（除非包裹在 `MULTI/EXEC` 中，但这在 Cluster 模式下有许多限制）。
3.  **Cluster 兼容性**: 目前生成的 Key 未使用 Hash Tag，导致相关联的 Key（如补偿机制中的 ZSet 和 Hash）在 Redis Cluster 中可能分布在不同 Slot，无法使用 Lua 脚本进行原子操作。

## 2. 优化目标 (Objectives)

1.  **Lua 脚本批处理**: 使用 Lua 脚本替代 Pipeline 执行 `publish_batch`，大幅减少网络包数量和协议解析开销。
2.  **Hash Tag 适配**: 统一 Key 生成规则，引入 `{tag}`，确保相关 Key 落入同一 Slot，为未来迁移到 Redis Cluster 扫清障碍，并允许使用 Lua 脚本操作多 Key。

## 3. 技术方案 (Technical Solution)

### 3.1 引入 Hash Tag
修改 Key 生成逻辑，确保同一分片/主题的 Key 具有相同的 Hash Tag。
- Stream Key: `{namespace:topic:shard}`
- Compensator Key: `{namespace:compensation:topic}:zset` / `{namespace:compensation:topic}:data`

### 3.2 Lua `publish_batch`
编写 Lua 脚本，接收一批 `(key, payload, headers)`，在服务端循环执行 `XADD`。
由于 `XADD` 是针对特定 Key 的，且我们有分片逻辑，我们需要按 Key 分组调用 Lua（或者在 Lua 中处理多 Key，前提是它们在同一 Slot - 但分片 Key 肯定不在同一 Slot）。
因此，优化策略是：**按 Shard 分组**，对每个 Shard 执行一次 Lua 脚本（该 Shard 内的 Key 是相同的）。

由于 `publish_batch` 中的 items 可能属于不同的 Shard（取决于 Key 参数），我们需要先在客户端按 Shard 分组，然后对每个组并发（或 Pipeline）执行 Lua 脚本。

### 3.3 预期收益
- **带宽降低**: 减少 30% 以上的协议头部开销。
- **CPU 降低**: 减少 Redis 服务端 Syscall 次数。
- **Cluster 兼容**: 支持未来无缝迁移到 Redis Cluster。

## 4. 执行计划
- [x] 修改 `queue/src/compensation.rs` 使用 Hash Tag。
- [x] 修改 `queue/src/redis.rs` 使用 Hash Tag。
- [x] 修改 `queue/src/redis.rs` 的 `publish_batch`，实现分片分组 + Lua 脚本提交。

## 5. 执行结果 (Execution Results)

- [x] **Hash Tag 适配**: 
    - 修改 `queue/src/compensation.rs`，将补偿队列的 ZSet 和 Hash Key 统一使用 `{namespace:compensation:topic}` 作为 Hash Tag。
    - 修改 `queue/src/redis.rs` 中的 `get_topic_key`，使用 `{namespace:topic:shard}` 格式。
    - 更新 `extract_shard_id` 以支持解析带 `{}` 的 Key。

- [x] **Lua Batching 实现**:
    - 重构 `RedisQueue::publish_batch`，使用 Lua 脚本批量提交 `XADD`。
    - 重构 `RedisQueue::publish_batch_with_headers`，支持带 Headers 的 Lua 批量提交。

- [x] **验证**:
    - `cargo test -p queue` 通过。
    - 编译检查通过。
    - Benchmark 准备就绪 (等待 Redis 环境执行)。
