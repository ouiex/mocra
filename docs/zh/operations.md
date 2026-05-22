# 运维文档

本文说明嵌入 `mocra` 的应用如何运行和运维。

## 本地开发

最简单的开发配置是使用本地缓存，并省略 Kafka/Raft 配置。

```toml
[cache]
ttl = 3600
backend = "local"

[channel_config]
minid_time = 0
capacity = 1024
queue_codec = "msgpack"
```

运行嵌入 `Engine` 的应用二进制。在 `engine.start().await` 前注册模块。

## Kafka 队列部署

需要使用 Kafka 作为队列传输时，添加 `channel_config.kafka`。

运维要求：

- 每个运行时节点都必须能访问 Kafka brokers；
- topic 命名和 envelope 路由在节点间保持一致；
- 同一部署内 `queue_codec` 应保持一致；
- 生产队列应监控 DLQ。

## Raft/RocksDB 协调

需要由 Raft/RocksDB 承载分布式协调和状态时，添加 `[raft]` 并使用 `cache.backend = "raft_rocksdb"`。

运维要求：

- `name` 标识 namespace；
- `raft.addr` 必须能被 peer 节点访问；
- 加入已有 group 时，`raft.peers` 应至少包含一个可达 peer；
- `raft.data_dir` 应持久化；
- heartbeat 和 election timeout 应根据网络延迟设置。

## HTTP API

配置 `[api]` 后启用运维端点。`api_key` 应保密，并通过应用部署流程轮换。

常用端点：

- `/health` 用于健康检查；
- `/metrics` 用于指标采集；
- `/dlq/messages` 用于失败消息检查；
- `/control/pause` 和 `/control/resume` 用于运行时控制。

## 监控

至少监控：

- 进程健康；
- 请求和 parser 吞吐；
- 队列积压；
- DLQ 大小；
- 重试次数；
- 模块错误数；
- 启用 Raft 时的 leader 和 peer 健康。

## 故障处理

队列消息失败后，可能根据 `nack_max_retries` 和 `nack_backoff_ms` 进入 DLQ。重新入队前应先检查 DLQ 消息。

parser 或模块失败时，优先定位 module name、node key、run ID 和相关 profile/config version。

## 升级检查

升级应用时建议验证：

- `cargo check`；
- 模块 DAG 编译；
- 请求生成测试；
- parser 输出测试；
- 生产 TOML 配置加载；
- 使用目标队列/缓存/控制面配置启动。

不要把 Redis 配置带入新部署。

