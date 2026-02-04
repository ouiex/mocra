> 已合并至 `docs/README.md` 与根目录 `OPTIMIZATION.md`，本文件保留为历史参考。

# 优化指南

## 内存分配器（mimalloc）
已在测试运行器（`tests/src/main.rs`）中集成 `mimalloc`，提升多线程场景性能。
如需在自定义二进制中启用，请添加 `mimalloc` 依赖并在 `main.rs` 中加入：

```rust
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
```

## 队列解压零拷贝
`QueueManager` 在消费消息时统一执行 `decompress_payload`。对未压缩的消息，直接借用原始 payload，
避免 `payload.to_vec()` 的额外分配与拷贝；仅在检测到 Zstd/Gzip 时才分配解压缓冲区。
该优化对高吞吐、小消息场景尤为明显。

## 本地缓存 keys 过期回收优化
`LocalBackend::keys` 由“先收集再清理”改为遍历时即时清理过期项，减少一次额外的 `Vec` 分配。
在大量 key 扫描场景可降低内存抖动，保持单节点模式更轻量。

## Redis 压缩
Redis 压缩阈值可通过 `config.toml`（或任意 `ConfigProvider`）配置，便于根据数据大小与网络环境调优。

```toml
[cache]
ttl = 3600
compression_threshold = 1024 # Compress values larger than 1024 bytes
[cache.redis]
# ... redis config
```

## 连接池
数据库与 Redis 连接池大小可配置：

```toml
[db]
pool_size = 20

[cache.redis]
pool_size = 50
```

## HTTP 客户端调优
`RequestDownloader` 预设高性能参数：
- Connection Pooling
- TCP Keepalive (60s)
- HTTP/2 Keepalive (30s)
- Connect Timeout (10s)

## Redis Stream 分片
在超高吞吐场景可启用 Redis Stream 分片，将写入压力分散到多个 Key。

```toml
[channel_config.redis]
shards = 4 # Split topics into topic:0, topic:1...topic:3
# ...
```

通过减少单个 Key 的锁竞争提高吞吐；消费者会自动订阅所有分片。

## 请求去重 TTL
请求去重使用 Redis 防止重复发布，TTL 可配置：

```toml
[crawler]
dedup_ttl_secs = 3600
```

## 队列压缩阈值配置（本次实现）
队列消息压缩阈值支持通过配置调整，避免小消息压缩带来的 CPU 额外开销。

```toml
[channel_config]
compression_threshold = 1024 # Payloads larger than 1024 bytes will be compressed
```

## 去重命名空间
去重 Key 使用配置的 `name` 作为命名空间，避免多集群共用 Redis 时冲突。

## Sync 发布可靠性
当 KV 更新成功但 pub/sub 发布失败时，分布式同步会进行短暂重试，降低短时 Redis 抖动导致 watcher 失效的概率。

## WebSocket 响应快速路径
WebSocket 响应优先本地发布，失败后再回退到 Redis/Kafka，减少同机解析场景的序列化与网络跳数。

## 动态限速热更新
配置热更新改变 `download_config.rate_limit`（以及 `api.rate_limit`）后，进程内分布式限速器会立即刷新，避免无需重启就出现 QPS 限制失效。

## Redis 解压异步化
当缓存命中且检测到压缩数据时，解压过程迁移到 `spawn_blocking`，避免阻塞异步运行时。
同时为 `MGET` 增加批量解压的阻塞线程路径，在存在压缩数据时集中处理，降低高并发下的 CPU 抢占与延迟抖动。

## 请求哈希复用
请求缓存与响应缓存统一复用一次 hash 计算，避免在缓存命中、响应封装等路径重复计算。
对频繁请求和高吞吐场景可减少 CPU 与小对象分配开销，并保持缓存键一致。

## 下载器过期时间上报稳定性
下载器过期时间写入 Redis 时使用独立的时间戳副本，避免异步任务引用变更导致的时间漂移。
确保过期清理逻辑稳定、避免误删除活跃下载器实例。

## 分布式限流时间偏移刷新
限流器缓存 Redis 与本地时钟偏移，增加刷新间隔避免长期漂移带来的等待误差，同时保留无 Redis 时的本地时钟路径。

## Cron 调度解析缓存
`CronScheduler` 新增了 cron 配置缓存，避免在每次刷新中反复调用 `ModuleTrait::cron()` 解析表达式。
缓存以 `module.name()` 为键保存 `CronConfig`，并在模块集合或版本变化时自动清理，降低 CPU 与锁争用开销。

## 模块签名哈希
调度器刷新判断从“仅模块名”升级为“模块名 + 版本号”的签名哈希。
这样当模块版本变化时也会触发刷新，确保 cron 配置变更被及时感知。

## TaskModel 生成批处理优化（已实现）
TaskModel 生成请求时，批量去重复用一次 hash 计算结果，日志与去重共用，减少重复 CPU 计算；
发布路径避免重复 clone 与重复 `module_id()` 计算，减少小对象分配与锁竞争；
保留批量去重的异步执行以满足流式处理的 `Send + Sync` 约束，同时降低每批次额外开销。

## 队列批量压缩零拷贝优化（本次实现）
`QueueManager::flush_batch` 在压缩前不再复制一次序列化 payload：
- 旧逻辑：`bincode::serialize` 得到 `Vec<u8>` 后再借用 `&[u8]` 压缩，压缩失败时会触发一次额外 `to_vec`。
- 新逻辑：直接接收 `Vec<u8>` 并在压缩失败时返回原始 buffer，避免多一次分配与拷贝。
高吞吐批量发布场景可减少内存抖动并降低 CPU。

## Request::hash 延迟缓存（本次实现）
`Request::hash()` 增加 `OnceCell` 缓存，避免同一请求在去重、缓存命中等路径重复计算 Hash。
当 `hash_str` 未手动覆盖时，首次计算结果会缓存，后续复用，减少 CPU 与短期分配。

## 队列消费小批量直通反序列化（本次实现）
`QueueManager::process_batch_messages` 在处理小批量消息时不再强制 `spawn_blocking`，
直接在异步线程内完成解压与反序列化，避免频繁线程切换带来的额外开销。
当批量大小达到阈值（32 条）时仍走阻塞线程，保证高吞吐场景不会阻塞运行时。

## 队列消费字节阈值阻塞化（本次实现）
`QueueManager::process_batch_messages` 增加了 payload 总字节数估算：当批量消息体积超过 64KB 时，
即便条数小于 32 也会走 `spawn_blocking`，避免大 payload 解压与反序列化阻塞异步运行时。
该策略与小批量直通路径互补，兼顾低延迟与大对象吞吐稳定性。

## Redis ACK 批处理计数优化（本次实现）
`RedisQueue::spawn_ack_processor` 以前每次 `ack_rx` 收到消息都会遍历 `batches` 统计总数。
高吞吐场景下该 O(n) 统计会放大 CPU 消耗。现在改为维护 `pending_count` 计数，
并在批量成功 flush 后一次性归零，避免重复遍历。

## 队列批量序列化阻塞判定优化（本次实现）
`QueueManager::flush_batch` 增加 payload 大小估算：在小批量（<32 条）但总序列化体积超过 64KB 时，
仍使用 `spawn_blocking` 处理序列化与压缩，避免大对象在异步线程阻塞运行时。
该策略兼顾小批量低延迟与大对象 CPU 密集型场景的吞吐稳定性。

## 队列批量序列化估算提前终止（本次实现）
`QueueManager::flush_batch` 在估算序列化体积时改为逐项累计并超过阈值立即停止，避免对大批量进行不必要的
`serialized_size` 计算，降低纯估算阶段的 CPU 开销。

## 队列批量发布 ID 复用（本次实现）
`QueueManager::flush_batch` 在序列化循环中先缓存 `item.get_id()`，并复用于 payload 与 debug ID 列表，
避免重复计算/克隆 ID，减少热路径的额外开销。

## CacheAble 大对象序列化阻塞化（本次实现）
`CacheAble::send/send_with_ttl/send_nx` 增加可选的 `serialized_size_hint` 和 `clone_for_serialize`：
当对象估算大小超过 64KB 且实现提供克隆时，序列化会迁移到 `spawn_blocking`，降低主运行时抖动。
`Request/Response` 已提供体积估算与克隆支持，确保缓存写入在高负载下更平滑。

## 队列批量聚合逻辑优化（本次实现）
`Batcher::run` 在收到首条消息时不再立即 flush，而是仅在达到 `batch_size` 或定时器触发时才提交批次。
避免高并发下的“单条即发”导致的频繁任务切换与序列化开销，使批处理更稳定、吞吐更平滑。

## Redis 批量发布空集合快速返回（本次实现）
`RedisQueue::publish_batch`/`publish_batch_with_headers` 在空批量时直接返回，避免无意义的连接获取与 pipeline 调用。
对批处理启停频繁的边界场景可降低 Redis 连接抖动与 CPU 开销。

## 队列订阅并行化（本次实现）
`QueueManager::subscribe_all_priorities` 改为并行订阅 High/Normal/Low，减少队列启动时的串行等待，
加快分布式节点和单节点的订阅准备时间。

## Downloader 配置一致性（本次实现）
`DownloaderManager::get_downloader` 在放入缓存前先执行 `set_config`，
确保缓存实例已应用限速与开关配置，避免后续请求重复配置造成的额外开销与配置漂移。

## 测试基准更贴近真实场景（本次实现）
`tests_debug` 的 mock server 增加随机延迟与响应体大小；
`moc.dev.v7` 模块生成请求时增加随机 payload 大小并附带 body，
使 TPS 统计更符合真实请求分布。

## 基准测试输出增强（本次实现）
`tests_debug` 新增输出：统计时间（UTC）、Total Requests Processed 与 Average TPS，便于记录每轮优化后的吞吐。

## 本次优化后的请求速度统计
执行命令：
`cargo run -p tests --bin tests_debug`

说明：当前仓库中已无 `benchmark_mock_v2` 二进制，统一改用 `tests_debug` 进行运行与统计。

结果（10s，统计时间：2026-01-30T08:50:53Z）：
- Total Requests Processed: 4999
- Average TPS: 498.52

## 建议的后续优化（待评估）
1. `QueueManager::flush_batch` 可根据 payload 大小决定是否走 `spawn_blocking`，降低小批量场景的线程切换开销。
2. `CacheAble::send` 可对大对象序列化使用 `spawn_blocking`，并加阈值开关。
3. `RedisQueue::spawn_ack_processor` 可在高峰时增大批量阈值并允许配置化 flush 间隔，以降低 RTT。
