# Python Mocra 开发文档

> 适用范围：python_mocra 目录

## 1. 目标与架构概览

Python Mocra 与 Rust 版本保持对齐，核心链路：

```
Task -> Request -> Download -> Response -> Parse -> Data/Task
```

关键组件：
- **MQ/Queue**：Redis Stream（支持分片、优先级 topic）
- **限流**：分布式平滑限流（Redis 可选）
- **缓存**：Redis/本地双模式
- **事件总线**：EventBus
- **下载器**：HTTPX + 代理池
- **任务系统**：TaskManager + ModuleManager
- **解析链路**：Parser Pipeline
- **离线存储**：BlobStorage（可选）

## 2. 环境准备

### 2.1 基础依赖
- Python 3.13+
- Redis
- PostgreSQL（可选；SQLite 可用于本地测试）
- UV（推荐）

### 2.2 安装
```bash
cd python_mocra
uv sync
```

## 3. 启动方式

### 3.1 本地模式（无 Redis）
```bash
uv run python main.py run-standalone
uv run python main.py start-worker
```

### 3.2 分布式模式（Redis）
```bash
redis-server
uv run python main.py start-worker
```

## 4. 配置

### 4.1 TOML 配置对齐 Rust
默认读取当前目录或 python_mocra 目录下的 config.toml。

支持字段（部分）：
- `name`：Redis 命名空间
- `channel_config.queue_codec`：bincode/json/msgpack
- `channel_config.redis`：队列 Redis
- `channel_config.blob_storage.path`：响应内容落盘路径

### 4.2 配置项（TOML）
所有配置均通过 TOML 设置，不使用环境变量。

## 5. 运行测试

```bash
uv run pytest tests/
```

性能基准：
```bash
uv run python tests/benchmark_pure_engine.py
uv run python tests/benchmark_prod_like.py
uv run python tests/benchmark_compare_scrapy_redis.py
```

## 6. 队列编码对齐（Rust + Python）

若与 Rust 混合消费/发布，请保证两端一致：
```toml
[channel_config]
queue_codec = "msgpack"
```
只需在 TOML 里设置即可。

## 7. 代码结构
```
python_mocra/
  cacheable/      缓存服务
  common/         公共模型、状态、配置
  downloader/     下载器
  engine/         引擎与管线
  mq/             MQ（Redis/Kafka/Memory）
  proxy/          代理池
  sync/           分布式同步
  utils/          工具集
  tests/          测试与基准
```

## 8. 开发规范

### 8.1 格式化与检查
```bash
uv run ruff format .
uv run ruff check .
uv run mypy .
```

### 8.2 新模块建议
- 模块实现放在 `modules/`
- 在 `engine/components/module_manager.py` 中注册/发现
- 输出 `ParserData` 或 `Data`

## 9. 常见问题

### 9.1 Redis 连接失败
检查 Redis 是否运行或使用本地模式。

### 9.2 Rust/Python 无法互通
确认：
- `name` 一致（命名空间）
- `queue_codec` 一致
- 使用同一 Redis DB

## 10. 维护建议

- 优先保证队列兼容（codec + topic）
- 关键性能路径尽量批处理
- 可在 Redis 上做分片/多 listener 以提升吞吐
