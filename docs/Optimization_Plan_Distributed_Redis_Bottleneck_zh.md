> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# 分布式 Redis 瓶颈优化方案 (Distributed Redis Bottleneck Optimization)

**日期**: 2026-01-30
**状态**: 已完成 (Completed)

## 1. 现状分析 (Current Status)

经过代码审查和基准测试分析，当前系统的 Redis 队列实现已具备以下优化特性：
1.  **分片多路复用 (Sharded Multiplexing)**: 支持多 Listener 并发监听。
2.  **智能压缩 (Smart Compression)**: 对 >1KB 数据自动进行 Zstd 压缩。
3.  **批处理 (Batching)**: 支持 `XADD` 和 `XREAD` 的批量操作。
4.  **动态配置**: `listener_count` 已支持通过配置动态调整。

## 2. 剩余瓶颈 (Remaining Bottlenecks)

尽管已有上述优化，但在高并发分布式场景下仍存在以下瓶颈：

1.  **默认分片数过低 (Low Default Shards)**:
    - 目前 `shards` 默认值为 `1`。这意味着即使配置了多个 Listener，它们也会争抢同一个 Redis Stream Key 的锁（虽然 Redis 是单线程，但在处理 Group Metadata 时仍有开销）。
    - 在 Redis Cluster 模式下，`shards=1` 导致所有流量打到单个 Slot/Node，无法利用集群优势。

2.  **内存与带宽开销 (Memory & Bandwidth Overhead)**:
    - `RedisCompensator` 接口接收 `&[u8]`，导致必须执行 `payload.to_vec()` 进行内存克隆才能发送到异步通道。
    - 而 `Message` 结构体中实际上已经持有 `Arc<Vec<u8>>`。如果修改接口支持 `Arc` 传递，可以实现**零拷贝 (Zero-Copy)** 补偿记录。

## 3. 优化执行计划 (Execution Plan)

### 3.1 提升并发默认值 (Tune Defaults)
- **目标**: 提高开箱即用的性能，更好适配多核 CPU 和 Redis Cluster。
- **操作**:
    - [x] 修改 `queue/src/redis.rs`: 将默认 `shards` 从 `1` 提升至 `8`。
    - [x] 修改 `queue/src/redis.rs`: 将默认 `listener_count` 从 `4` 提升至 `8`。

### 3.2 优化补偿器内存使用 (Optimize Compensator Memory)
- **目标**: 减少内存分配和拷贝。
- **操作**:
    - [x] 修改 `queue/src/compensation.rs`: 将 `Compensator` trait 的 `add_task` 方法签名从 `&[u8]` 改为 `std::sync::Arc<Vec<u8>>`。
    - [x] 修改 `RedisCompensator`: 适配新签名，并在 `CompensationMessage` 中持有 `Arc<Vec<u8>>` 而非 `Vec<u8>`。
    - [x] 修改 `queue/src/manager.rs`: 在调用 `add_task` 时直接传递 `msg.payload`。

### 3.4 确定性分片路由 (Deterministic Sharding Routing)
- **问题**: 使用 `CRC32(key) % listener_count` 进行分片分配可能导致哈希碰撞，使得部分 Listener 负载过重，而部分 Listener 空闲。例如，`topic:0` 和 `topic:1` 可能都被哈希到 Listener 0。
- **解决方案**: 
    - 修改 `queue/src/redis.rs` 中的分片分配逻辑。
    - 优先尝试从 Redis Key 中解析分片 ID (后缀 `:{shard_id}`)。
    - 如果解析成功，直接使用 `shard_id % listener_count` 进行分配，确保均匀分布。
    - 如果解析失败（旧 Key 或非标准 Key），回退到 CRC32 哈希。
- **状态**: [x] 已完成

### 3.5 验证 (Verification)
- [x] 添加单元测试 `test_extract_shard_id` 验证解析逻辑。
- [x] 运行 `cargo test -p queue` 通过。
- [x] 运行 `tests_debug` (使用内存队列，验证逻辑正确性)。
- [x] 运行 `benchmark_redis` (验证混合存储性能提升 ~300x)。

## 4. 长期建议 (Long-term Recommendations)
- **混合存储 (Hybrid Storage)**: 对于 `Response` 类型的消息，建议仅在 Redis 中传输元数据和存储路径，实际 Body 存入 S3/MinIO。
