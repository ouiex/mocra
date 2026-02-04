> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# 核心优化文档 (Core Optimization Document)

**日期**: 2026-01-31
**状态**: 执行中 (In Progress)

本文档汇总了针对 Mocra 分布式爬虫框架的 Redis 瓶颈优化方案及执行情况。

## 1. 分布式 Redis 瓶颈优化 (Distributed Redis Bottleneck Optimization)

### 1.1 问题分析
*   **分片争抢**: 默认单分片 (`shards=1`) 导致在高并发下所有 Listener 争抢同一个 Redis Stream Key 锁。
*   **路由不均**: 简单的 CRC32 哈希导致某些分片负载过高。
*   **内存拷贝**: 补偿器 (`Compensator`) 接口导致不必要的内存克隆。

### 1.2 优化方案 (已执行)
1.  **提升并发默认值**:
    *   将 `RedisQueue` 默认 `shards` 提升至 **8**。
    *   将默认 `listener_count` 提升至 **8**。
    *   适配 Redis Cluster 和多核环境。
2.  **确定性分片路由 (Deterministic Routing)**:
    *   生产者生成的 Topic Key 包含分片后缀 (e.g., `topic:0`).
    *   消费者通过解析 Key 后缀 (`extract_shard_id`) 绑定特定分片，确保负载绝对均匀，消除哈希碰撞。
3.  **零拷贝补偿 (Zero-Copy Compensation)**:
    *   重构 `Compensator` 接口，使用 `Arc<Vec<u8>>` 替代 `&[u8]`，避免内存复制。
4.  **共享 Claimer 与 Lag Monitor**:
    *   将每个 Topic/优先级的独立 `XAUTOCLAIM` 任务合并为**单个**后台任务。
    *   通过 `StreamRouter` 动态路由，避免 15+ 个定时任务对 Redis 造成额外压力。
    *   统一收集队列长度指标，按 Topic 聚合统计。

## 2. 混合存储优化 (Hybrid Storage Optimization)

### 2.1 问题分析
*   **大对象阻塞**: 完整的 HTML/JSON 响应体 (Payload) 直接存入 Redis，导致内存耗尽和网络带宽瓶颈。
*   **Redis 延迟**: 大 Value 读写阻塞 Redis 主线程。

### 2.2 优化方案 (已执行)
1.  **自动卸载 (Auto Offloading)**:
    *   引入 `BlobStorage` 接口。
    *   在 `QueueManager` 生产者端，对超过阈值 (默认 64KB) 的 `Response`，自动将 Payload 写入文件系统/S3，仅在 Redis 中保留元数据和路径。
2.  **透明加载 (Transparent Reloading)**:
    *   消费者端收到消息后，自动从 Storage 加载 Payload，对上层业务透明。

## 3. 测试与验证 (Verification)

*   **单元测试**: `cargo test -p queue` 通过，覆盖了分片解析、压缩和集成逻辑。
*   **基准测试**: 设计了 `benchmark_redis` 工具用于压测 (需 Redis 环境)。
*   **集成测试**: `tests_debug` 用于验证系统完整流程 (In-Memory 模式通过)。

## 4. 后续计划 (Future Plan)
*   **Redis Cluster 适配**: 进一步验证在真实 Redis Cluster 环境下的槽位分配。
*   **Pipeline 批处理**: 进一步优化 Lua 脚本，支持更高密度的批量写入。
