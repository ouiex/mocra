# Python mocra 快速开始指南

## 环境要求

- Python 3.13+
- UV 包管理器
- Redis (可选，用于分布式模式)
- PostgreSQL/SQLite (可选，用于持久化)

## 安装

### 1. 安装 UV (如果尚未安装)

```bash
# Windows (PowerShell)
irm https://astral.sh/uv/install.ps1 | iex

# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 安装项目依赖

```bash
cd python_mocra
uv sync
```

## 运行模式

### 本地模式（无需 Redis）

适合开发和测试，所有功能使用内存实现。

```bash
# 运行单次任务
uv run python main.py run_standalone

# 启动持续 worker
uv run python main.py start_worker
```

### 分布式模式（需要 Redis）

适合生产环境，支持多节点协同。

```bash
# 1. 启动 Redis
redis-server

# 2. 配置 Redis URL（可选，默认 localhost:6379）
export REDIS_URL="redis://localhost:6379/0"

# 3. 启动 worker
uv run python main.py start_worker
```

## 运行测试

### 核心功能测试

```bash
uv run python tests/test_core_functionality.py
```

### 集成测试

```bash
uv run python tests/test_integration.py
```

### 使用 pytest

```bash
uv run pytest tests/
```

## 项目结构

```
python_mocra/
├── cacheable/          # 缓存服务
│   ├── __init__.py
│   └── service.py     # CacheService, RedisBackend, LocalBackend
├── common/            # 共享组件
│   ├── config.py      # 配置管理
│   ├── state.py       # 全局状态
│   ├── models/        # 数据模型
│   └── middlewares/   # 中间件
├── downloader/        # 下载器
│   ├── manager.py     # DownloaderManager
│   └── client.py      # HTTP 客户端
├── engine/            # 任务引擎
│   ├── worker.py      # 统一 Worker
│   ├── task.py        # TaskManager
│   ├── core/          # 核心组件 (EventBus, Pipeline)
│   ├── components/    # 组件 (Scheduler, Monitor, etc.)
│   └── processors/    # 处理链
├── mq/                # 消息队列
│   ├── interface.py   # MqBackend 接口
│   ├── redis_backend.py
│   ├── kafka_backend.py
│   └── memory_backend.py
├── sync/              # 分布式同步 ⭐ 新增
│   ├── __init__.py
│   ├── backend.py     # CoordinationBackend 接口
│   ├── redis_backend.py  # Redis 协调后端
│   ├── distributed.py    # SyncService, SyncAble
│   ├── leader.py      # LeaderElector
│   └── lock.py        # 分布式锁
├── tests/             # 测试
│   ├── test_core_functionality.py
│   └── test_integration.py
├── pyproject.toml     # 项目配置 (UV)
└── main.py            # 入口文件
```

## 核心功能使用示例

### 1. 缓存服务

```python
from cacheable.service import CacheService, LocalBackend

# 创建缓存服务
backend = LocalBackend()
cache = CacheService(backend, namespace="myapp")

# 存储和获取
await cache.set("key", b"value", ttl=3600)
value = await cache.get("key")

# JSON 对象
from pydantic import BaseModel

class Config(BaseModel):
    rate: int

config = Config(rate=100)
await cache.set_json("config", config)
loaded = await cache.get_json("config", Config)
```

### 2. 分布式同步

```python
from sync import SyncService, SyncAble, RedisCoordinationBackend

# 定义可同步的配置
class AppConfig(SyncAble):
    def __init__(self, max_workers: int):
        self.max_workers = max_workers
    
    @classmethod
    def topic(cls) -> str:
        return "app_config"

# 创建同步服务
backend = RedisCoordinationBackend("redis://localhost:6379")
sync_service = SyncService(backend, namespace="myapp")

# 订阅配置变化
config_sync = await sync_service.sync(AppConfig)

# 发布新配置
new_config = AppConfig(max_workers=10)
await sync_service.publish(new_config)

# 监听变化
await config_sync.changed()
current = config_sync.get()
print(f"Workers: {current.max_workers}")
```

### 3. Leader 选举

```python
from sync import LeaderElector
import redis.asyncio as redis

redis_client = redis.from_url("redis://localhost:6379")
elector = LeaderElector(redis_client, "my_service_leader", ttl_ms=10000)

await elector.start()

if elector.is_leader:
    print("I am the leader!")
    # 执行只应由 leader 执行的任务
else:
    print("I am a follower")
```

### 4. 任务队列

```python
from common.state import get_state
from common.models.message import TaskModel
from uuid6 import uuid7

state = get_state()
await state.init()

# 发布任务
task = TaskModel(
    account="user1",
    platform="twitter",
    module=["tweet_spider"],
    run_id=uuid7()
)

await state.mq.publish_task(task)

# 消费任务
queued_task = await state.mq.consume_task("default", "worker1")
if queued_task:
    task = queued_task.item
    # 处理任务...
    await queued_task.ack()
```

## 配置

### 环境变量

```bash
# Redis
REDIS_URL=redis://localhost:6379/0

# Database
DATABASE_URL=postgresql://user:pass@localhost/mocra

# Kafka (可选)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# 日志级别
LOG_LEVEL=INFO
```

### 配置文件

编辑 `common/config.py` 或创建 `.env` 文件。

## 开发

### 添加新依赖

```bash
uv add <package-name>
```

### 代码格式化

```bash
uv run ruff check .
uv run ruff format .
```

### 类型检查

```bash
uv run mypy .
```

## 故障排除

### 问题：无法连接 Redis

**解决方案：** 确保 Redis 正在运行，或者使用本地模式：

```python
# 程序会自动降级到本地模式
# 无需手动配置
```

### 问题：导入错误

**解决方案：** 确保已运行 `uv sync`

```bash
uv sync
```

### 问题：测试失败

**解决方案：** 检查 Python 版本

```bash
python --version  # 应该是 3.13+
uv run python --version
```

## 更多资源

- [完整实现报告](IMPLEMENTATION_REPORT.md)
- [原始 Rust 项目文档](../docs/)
- [TODO 列表](TODO.md)

## 支持

如有问题，请查看：
1. 测试文件 `tests/` 中的示例代码
2. `IMPLEMENTATION_REPORT.md` 中的详细说明
3. 原始 Rust 项目的设计文档

祝使用愉快！🚀
