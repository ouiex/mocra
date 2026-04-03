# 部署指南

本指南介绍 mocra 的单节点和分布式模式部署。

## 单节点模式

最简单的部署方式 — 除数据库外无其他外部依赖。

### 适用场景

- 开发和测试
- 低流量数据采集
- 独立脚本

### 配置

1. 使用本地数据库（SQLite 或 PostgreSQL）：
   ```toml
   [db]
   url = "sqlite://data/crawler.db?mode=rwc"
   ```

2. **不要**配置 Redis：
   ```toml
   [cache]
   ttl = 60
   # 没有 redis = {} 段
   ```

3. 运行单个实例：
   ```bash
   cargo run --release
   ```

### 运行效果

- 队列使用进程内 Tokio mpsc 通道
- 缓存在内存中（重启后不持久化）
- 无分布式锁 — 仅单进程并发
- 所有模块在一个进程中运行

## 分布式模式

通过配置 Redis 启用分布式模式。引擎从 `cache.redis` 自动检测。

### 适用场景

- 高流量数据采集
- 多工作进程
- 跨节点去重和限速
- 队列持久化的容错性

### 配置

1. 配置 Redis：
   ```toml
   [cache]
   ttl = 60

   [cache.redis]
   url = "redis://redis-host:6379"
   ```

2. 可选配置 Redis Streams 作为队列后端：
   ```toml
   [channel_config]
   capacity = 5000
   minid_time = 12

   [channel_config.redis]
   url = "redis://redis-host:6379"
   ```

3. 或使用 Kafka：
   ```toml
   [channel_config.kafka]
   brokers = "kafka-host:9092"
   ```

4. 运行多个实例（相同配置）：
   ```bash
   # 节点 1
   cargo run --release

   # 节点 2（相同二进制，相同 config.toml）
   cargo run --release
   ```

### 运行效果

- 队列使用 Redis Streams 或 Kafka 的消费者组
- 缓存基于 Redis（跨节点共享）
- 通过 Redis 实现分布式锁，用于限速和去重
- 节点心跳和集群感知
- 工作自动在节点间分配

## 队列后端对比

| 特性 | 本地（Tokio） | Redis Streams | Kafka |
|---|---|---|---|
| 部署复杂度 | 无 | 低 | 中等 |
| 持久化 | 否 | 是 | 是 |
| 多进程 | 否 | 是 | 是 |
| 顺序保证 | FIFO | 每个流 FIFO | 每个分区 |
| DLQ 支持 | 有限 | 完整 | 完整 |
| 背压 | 通道容量 | 流裁剪 | 消费者滞后 |
| 编码 | 内存 | MsgPack / JSON | MsgPack / JSON |

## 队列编码

远程队列默认使用 **MsgPack** 以提升效率。切换为 JSON 便于调试：

```toml
[channel_config]
queue_codec = "json"  # "msgpack"（默认）或 "json"
```

## 压缩

大载荷在超过配置阈值时自动使用 **zstd** 压缩。通过 `channel_config` 设置配置。

## 监控

### Prometheus 指标

启用 API 服务器并采集 `/metrics`：

```toml
[api]
port = 8080
api_key = "secret"
```

指标包括：
- 每模块请求计数
- 错误率
- 队列深度
- 下载延迟
- 活跃 worker 数

### 健康检查

使用 `/health` 作为负载均衡器健康探针：

```bash
curl http://localhost:8080/health
```

### 日志

mocra 使用 `log` crate。通过 `RUST_LOG` 配置：

```bash
RUST_LOG=mocra=info,warn cargo run
```

或获取更多详情：
```bash
RUST_LOG=mocra=debug cargo run
```

## Docker Compose

仓库包含用于支撑服务的 Docker Compose 文件：

- **`docker-compose.kafka.yml`** — Kafka + Zookeeper，用于 Kafka 队列后端
- **`docker-compose.monitoring.yml`** — Prometheus + Grafana，用于监控

```bash
# 启动 Kafka
docker compose -f docker-compose.kafka.yml up -d

# 启动监控栈
docker compose -f docker-compose.monitoring.yml up -d
```

## 生产环境检查清单

- [ ] 使用 PostgreSQL（而非 SQLite）存储元数据
- [ ] 配置 Redis 启用分布式模式
- [ ] 为 `api.api_key` 设置强密钥
- [ ] 配置 `RUST_LOG` 使用适当的日志级别
- [ ] 在 `[crawler]` 中设置 `request_max_retries` 和 `module_max_errors`
- [ ] 使用 Prometheus/Grafana 监控 `/metrics`
- [ ] 使用 `/health` 作为负载均衡器健康检查
- [ ] 在 `[download_config]` 中配置适当的 `rate_limit`
- [ ] 生产构建启用 `mimalloc` 特性（默认）
