# 运行时调优与验证

> **English version:** [Runtime tuning and validation](../runtime-tuning.md)

本指南说明 0.5.0 中的队列、代理与独立 DAG 调度器改动。先使用默认值，每次只调整一个上限，再对比同一负载的前后结果。[基准测试索引](../../benchmarks/README.md)记录了本地方法与局限。

## 有界队列与批处理

`channel_config.capacity` 是**每条**本地通道（包括日志通道）的正整数容量。批处理也遵守配置的批大小；分发器等待并发许可时会停止继续接收。因此处理器变慢时，有界输入队列产生背压，而不会积累无限量的待运行批任务。

```toml
[channel_config]
minid_time = 0
capacity = 1000
batch_concurrency = 10
```

这只是[完整引擎配置](configuration.md)的片段，其他必需段仍要填写。`batch_concurrency` 限制同时执行的批量刷新，`capacity` 分别限制各条本地通道。过载时观察队列深度、背压重试、接收/完成数量、进程 RSS 和 p95/p99 延迟。调大容量可以承接短时突发，也会提高排队消息占用内存的上限。

## 代理选择与 HTTP Client

托管代理从两个候选中选取，而非每次都选择最高评分代理。这样会把请求分散到合格代理，同时优先选两者中更好的一个。相比原先最高分策略，它可能更多地选中低评分代理；取舍见[本地选择对比](../../benchmarks/proxy-selection.md)。提供商 `max_size` 限制**每个提供商**动态拉取的 IP 代理；健康探测和代理专用 HTTP Client 缓存另有独立上限。

代理 Client 缓存容量默认 1000。可根据测量设置 `download_config.proxy_client_cache_capacity`，或设为 `0` 关闭缓存。应一起对比命中/未命中、新建计数、空闲连接和 RSS；[本地缓存测量](../../benchmarks/proxy-client-cache.md)并不能给出通用最优值。请求级选择与反馈见[代理与下载器](proxies-and-downloaders.md)。

## 独立 DAG 调度器

`mocra-dag` 调度器等待节点完成事件，而不再轮询。使用 `DagRunStateStore` 时，`.with_run_state_store(store, run_key)` 默认每成功完成 16 个节点保存一次完整检查点。要恢复逐节点保存，在 `.with_run_state_store` **之后**调用 `.with_run_state_checkpoint_interval(1)`。默认间隔下，检查点之间发生进程丢失，最多可能重跑 15 个已成功节点；节点副作用应可安全重复，或改用更短间隔。已处理的错误会保存失败快照。

这是 `mocra-dag` 调度器设置，**不是** `Spider` 或引擎 TOML 选项。复制开销与恢复取舍见[检查点测量](../../benchmarks/dag-checkpoints.md)；这些微基准不能代表完整爬虫吞吐。

## 验收边界

[本地全链路验收负载](../../benchmarks/runtime-acceptance.md)用本地 HTTP 代理覆盖了有界队列、批处理、托管代理路径和重试，并记录吞吐、延迟、内存及 ACK/NACK 核对。它**不**包含生产或预发布环境的灰度验证，也不包含真实 Kafka/NATS 压测、解析工作或存储服务。扩大生产流量前，应按报告中的灰度与回滚检查，对真实代理池和重试预算进行验证。
