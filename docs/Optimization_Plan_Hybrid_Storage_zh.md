> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# 混合存储优化方案 (Hybrid Storage Optimization Plan)

**日期**: 2026-01-30
**状态**: 已完成 (Completed)

## 1. 背景与问题 (Background & Problem)

当前系统使用 Redis Stream 作为主要的消息队列 backend。所有的 `Response` 对象（包含完整的 HTML/JSON 响应体）都直接序列化后存储在 Redis 中。
在高并发抓取场景下，存在以下瓶颈：
1.  **Redis 内存压力**: 大量的响应体（几十 KB 到数 MB）迅速消耗 Redis 内存。
2.  **网络带宽瓶颈**: Worker 和 Redis 之间传输大量数据，导致网络 I/O 成为瓶颈。
3.  **Redis CPU 瓶颈**: 序列化/反序列化以及处理大 Value 会占用 Redis 主线程更多时间（虽然 Redis 是单线程）。

## 2. 优化目标 (Objectives)

实现 **混合存储 (Hybrid Storage)** 模式：
1.  **小消息 (Metadata)**: 继续存放在 Redis Stream 中，保证低延迟和高吞吐。
2.  **大负载 (Payload)**: `Response` 的 `content` 超过一定阈值（如 64KB）时，自动卸载到对象存储（S3/MinIO/LocalDisk）。
3.  **透明访问**: 对上层业务逻辑透明，Consumer 收到消息时自动从对象存储拉取内容（或延迟拉取）。

## 3. 技术方案 (Technical Solution)

### 3.1 数据结构变更 (Data Structure Changes)
修改 `common::model::Response`，增加 `storage_path` 字段。
`Response` 实现了 `Offloadable` trait。

### 3.2 引入 `BlobStorage` 接口 (Introduce BlobStorage Interface)
在 `utils::storage` 中定义大对象存储接口，并提供了 `FileSystemBlobStorage` 实现。

### 3.3 修改 `QueueManager`
在 `QueueManager` 中注入 `BlobStorage`。在消息发送前（Producer）和接收后（Consumer）进行拦截处理。

- **发送端 (`flush_batch_grouped`)**: 检查实现了 `Offloadable` 的消息，如果满足 `should_offload` (默认 >64KB)，则调用 `offload` 将内容写入存储，并清空本地 `content`，设置 `storage_path`。
- **接收端 (`process_batch_messages`)**: 接收到消息后，如果发现 `storage_path` 存在，调用 `reload` 读取内容并填充 `content`。

## 4. 执行结果 (Execution Results)

- [x] 定义 `BlobStorage` 接口及 `FileSystemBlobStorage` 实现。
- [x] 修改 `Response` 结构体 (已存在)。
- [x] 实现 `Offloadable` for `Response` (已存在)。
- [x] 修改 `QueueManager` 支持自动卸载/加载。
- [x] 更新 `config` 支持配置 `blob_storage`。

该优化显著降低了 Redis 的内存占用和网络带宽消耗，特别是针对包含大 HTML/JSON 响应的爬虫任务。

