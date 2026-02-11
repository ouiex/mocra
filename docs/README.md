# Mocra 文档总览

本目录已做合并与精简，**架构、数据结构、流程、分布式部署与配置**集中在本页。其余历史文档保留为索引或已被合并。

---

## 1. 系统概览

Mocra 是一个 **分布式、事件驱动的采集/爬虫框架**，核心思想是：
- **无状态 Worker** + **共享外部存储**（Redis/Postgres）
- **统一消息契约**（Task/Request/Response/ParserTask/Data）
- **可插拔模块/中间件**（下载、解析、存储）
- **分布式调度/选主**（Cron + Leader Election）

核心组件：
- **Engine**：调度与管线执行
- **Queue**：消息总线（Redis Streams / Kafka）
- **Downloader**：网络请求与缓存/限流
- **Common**：配置、状态、模型、接口契约
- **Sync**：分布式一致性/选主
- **Cacheable**：二级缓存（本地 + Redis）

---

## 2. 核心数据结构（DTO）

> 详见 `common/src/model/*`

### 2.1 TaskModel
**用途**：任务顶层描述（账号/平台/模块/优先级/运行实例）。
- 主要字段：
	- `account` / `platform` / `module`
	- `priority`（高/中/低）
	- `run_id`（运行实例）
	- `metadata`（任务上下文）
- 来源：API/Cron

### 2.2 Request
**用途**：具体请求（HTTP/WebSocket）。
- 主要字段：
	- `url`, `method`, `headers`, `cookies`
	- `context`（ExecutionMark）
	- `run_id`, `priority`, `request_hash`
- 由 `Module.generate()` 或 `Parser` 生成

### 2.3 Response
**用途**：下载结果。
- 主要字段：
	- `status_code`, `content`, `headers`
	- `context`, `run_id`, `request_hash`
	- `storage_path`（大包体可落盘）
- 由 `Downloader` 生成

### 2.4 ParserTask
**用途**：解析过程中生成的“后续任务/步骤状态”。
- 主要字段：`metadata`, `context`, `prefix_request`

### 2.5 Data
**用途**：解析出的业务数据。
- 主要字段：`data`（Json/DataFrame/File 等）, `meta`, `data_middleware`

---

## 3. 处理流程（Pipeline）

```text
TaskModel -> TaskProcessor -> Request
Request   -> DownloadProcessor -> Response
Response  -> ParserProcessor -> (Data / Request / ParserTask)
ParserTask -> ParserTaskProcessor -> Request / Data
```

流程要点：
- **DownloadMiddleware**：请求签名、代理、加密、鉴权
- **DataMiddleware**：数据清洗、校验、去重、格式转换
- **DataStoreMiddleware**：数据持久化（PG/OSS/ES）

---

## 4. 分布式部署

### 4.1 角色划分
- **Worker**：运行 Engine（核心处理器 + 队列消费）。
- **Scheduler Leader**：分布式 Cron 调度，仅 Leader 触发任务。
- **Control Plane（可选）**：管理 API/监控。

### 4.2 基础设施
- **Redis**：队列（Stream）、缓存、锁、限流、节点注册、去重。
- **Postgres**：任务配置、日志、业务数据。
- **Kafka（可选）**：高吞吐消息替代。

### 4.3 扩展方式
- **水平扩展**：增加 Worker 数量。
- **队列分片**：Redis Streams 分片或多队列拓扑。
- **读写分离**：DB/Redis 做主从或集群。

---

## 5. 配置说明

配置详情见：`docs/configuration.md`

**数据库仅支持**：
- 使用 `db.url` 简化数据库配置。
- Redis 统一采用 `redis_host/redis_port/redis_db`。

**核心配置块**：
- `[db]`：数据库连接（`url`/`pool_size`）
- `[cache.redis]`：缓存 Redis
- `[channel_config.redis]`：队列 Redis（Stream）
- `[download_config]`：下载并发、超时、限流
- `[crawler]`：全局任务容错/并发参数

示例（节选）：
```toml
[db]
url = "postgres://user:password@localhost:5432/crawler"
pool_size = 10

[channel_config.redis]
redis_host = "127.0.0.1"
redis_port = 6379
redis_db = 0
pool_size = 100
shards = 8
listener_count = 8
```

测试配置样例：
- `tests/config.test.toml`
- `tests/config.mock.toml`
- `tests/config.mock.pure.toml`
- `tests/config.mock.pure.engine.toml`
- `tests/config.prod_like.toml`

---

## 6. 运行与部署建议

- Redis 建议启用 **持久化（AOF/RDB）**。
- Postgres 建议设置合理连接池与索引。
- 日志默认写入 `logs/mocra.{name}`，环境变量可覆盖。

---

## 7. 开发文档

### 7.1 依赖与环境

- Rust 工具链（建议 stable）
- Redis（本地或容器）
- Postgres（本地或容器）
- 可选：Kafka（高吞吐队列场景）

### 7.2 本地开发流程

1. 准备配置：复制并修改测试配置
	- [tests/config.test.toml](tests/config.test.toml)
2. 初始化数据库（PG）
	- [init.sql](init.sql) 与 [seed_data.sql](seed_data.sql)
3. 启动本地 Redis / Postgres
4. 运行测试或基准程序（见 7.3）

### 7.3 常用运行入口

- 测试运行器：`tests_debug`
- Mock 基准：`mock_benchmark`

> 入口定义见 [tests/Cargo.toml](tests/Cargo.toml)

### 7.4 配置与热更新

配置系统支持文件与 Redis 两种 Provider，核心结构在：
- [common/src/model/config.rs](common/src/model/config.rs)
- [common/src/state.rs](common/src/state.rs)

### 7.5 日志系统

日志初始化：`init_app_logger()`，环境变量：
`DISABLE_LOGS` / `MOCRA_DISABLE_LOGS`。

更多说明见：
- [docs/design/utils.md](docs/design/utils.md)

---

## 8. 进阶与优化

性能与优化建议请参考：
- `OPTIMIZATION.md`
- `docs/optimization.md`

---

## 9. 文档索引（保留）

以下文档已被合并或保留为历史参考：
- `docs/System_Architecture_zh.md`
- `docs/Design_Distributed_Cron.md`
- `docs/design/*`
- `docs/Optimization_*.md`
