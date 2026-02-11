> 已合并至 `docs/README.md`，本文件保留为历史参考。

# Engine 设计文档

## 概览
Engine 是 Mocra 的核心编排器，负责驱动任务、请求与响应在各处理链中流转，保证高效、分布式与可恢复的采集流程。

## 架构
系统基于事件驱动的流水线模型，通过 `QueueManager` 进行消息传递，并用 `EventBus` 统一发布可观测事件。

### Pipeline 阶段
1. **Task Processor**：消费 `TaskModel` -> 生成 `Request`。
   - 使用 `TaskModelChain` 执行模块级逻辑。
2. **Download Processor**：消费 `Request` -> 下载内容 -> 生成 `Response`。
   - 使用 `DownloadChain` 或 `WebSocketDownloadChain`。
   - 支持下载中间件（`before_request`、`after_response`）。
3. **Response Processor**：消费 `Response` -> 解析内容 -> 生成 `ParserTask`。
   - 使用 `ParserChain` 执行解析逻辑。
4. **Parser Processor**：消费 `ParserTask` -> 产出数据/新任务。
   - 使用 `ParserTaskChain`。
5. **Error Processor**：处理失败与重试，必要时写入 DLQ。

## 组件

### Event Bus
`EventBus` 通过统一的 `EventEnvelope` 事件模型传播状态变化，包含 `domain`/`event_type`/`phase`/`payload`/`error`，并以 `event_key` 路由。
可插拔处理器示例：
- `ConsoleLogHandler`：控制台结构化日志。
- `QueueLogHandler`：写入 Redis/Kafka 等队列。
- `RedisEventHandler`/`DbEventHandler`：事件持久化。

### Middleware Manager
管理三类中间件：
- `DownloadMiddleware`：请求/响应拦截（鉴权、签名、代理等）。
- `DataMiddleware`：数据清洗/校验/去重。
- `DataStoreMiddleware`：数据持久化（PG/ES/OSS 等）。

### Task Manager
注册并管理 `Module`，负责定义 URL 生成与解析规则。

### Scheduler
- **CronScheduler**：基于 Redis 进行分布式调度与选主，防止重复触发。

### Node Registry
Engine 实例以 UUID 注册到 Redis，周期性上报心跳，便于控制面追踪节点状态。

### Control Plane
对外暴露管理接口：
- `GET /metrics`：Prometheus 指标。
- `GET /api/nodes`：节点列表。
- `POST /api/control/pause`：全局暂停。
- `POST /api/control/resume`：全局恢复。
- `GET /api/dlq`：DLQ 查询。

## 并发与安全
- **Processors**：在 Tokio 任务中并发运行，支持 panic 保护。
- **Channels**：通过 `QueueManager` 与 Redis/Kafka 传输消息。
- **Graceful Shutdown**：支持 SIGINT/SIGTERM，尽量完成在途任务。
- **Idempotency**：Parser 任务通过 Redis 去重，尽可能实现一次性副作用。
