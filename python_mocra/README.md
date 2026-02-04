# Mocra Python Implementation

Python 版本的 Mocra 分布式爬虫引擎，完全复刻自 Rust mocra 项目。

## ✨ 特性

- 🚀 **完整的分布式架构** - 支持多节点协同工作
- 🔄 **分布式同步服务** - 实时状态同步和配置更新
- 🗄️ **灵活的缓存系统** - Redis 或本地内存缓存
- 📊 **多种队列后端** - Redis Stream / Kafka / 内存队列
- 👑 **Leader 选举机制** - 确保任务唯一执行
- ⏰ **Cron 任务调度** - 支持定时任务
- 🔌 **插件式中间件** - 灵活的请求/响应处理
- 📡 **事件驱动架构** - 解耦的组件通信

## 🏗️ 架构

基于 Rust mocra 的架构设计，采用事件驱动的管道模式：

```
Task → Download → Parse → Data/NewTasks
  ↓        ↓         ↓          ↓
 Queue → Queue → Queue → Storage
```

### 核心模块

- **sync/** - 分布式同步服务（SyncService, LeaderElector, 分布式锁）
- **cacheable/** - 缓存服务（支持 Redis 和本地后端）
- **engine/** - 任务引擎（Worker, Scheduler, Monitor）
- **mq/** - 消息队列（Redis/Kafka/Memory）
- **downloader/** - HTTP/WebSocket 下载器
- **common/** - 共享组件和数据模型
- **proxy/** - 代理管理
- **js_v8/** - JavaScript 运行时

## 🚀 快速开始

### 环境要求

- Python 3.13+
- UV 包管理器
- Redis (可选，用于分布式模式)

### 安装

```bash
# 安装 UV（如果尚未安装）
# Windows
irm https://astral.sh/uv/install.ps1 | iex

# Linux/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# 安装依赖
cd python_mocra
uv sync
```

### 运行

#### 本地模式（无需 Redis）

```bash
# 运行单次测试
uv run python main.py run-standalone

# 启动 worker
uv run python main.py start-worker
```

#### 分布式模式（需要 Redis）

```bash
# 1. 启动 Redis
redis-server

# 2. 启动 worker
uv run python main.py start-worker
```

## 🧪 测试

### 运行所有测试

```bash
# 核心功能测试
uv run python tests/test_core_functionality.py

# 集成测试
uv run python tests/test_integration.py

# 使用 pytest
uv run pytest tests/
```

### 性能基准测试

```bash
# 真实场景模拟（500 任务，50 并发）
uv run python tests/benchmark_real_world.py 500 50
```

## 📚 文档

- [快速开始指南](QUICKSTART.md) - 详细的使用说明
- [实现报告](IMPLEMENTATION_REPORT.md) - 完整的功能对比和实现说明
- [TODO 列表](TODO.md) - 未来计划

## 🔧 开发

### 添加依赖

```bash
uv add <package-name>
```

### 代码质量

```bash
# 格式化
uv run ruff format .

# 检查
uv run ruff check .

# 类型检查
uv run mypy .
```

## 📦 项目结构

```
python_mocra/
├── cacheable/          # 缓存服务
├── common/             # 共享组件
├── downloader/         # 下载器
├── engine/             # 任务引擎
├── mq/                 # 消息队列
├── sync/               # 分布式同步 ⭐ 新增
├── proxy/              # 代理管理
├── js_v8/              # JS 运行时
├── tests/              # 测试
├── modules/            # 爬虫模块
├── pyproject.toml      # 项目配置
└── main.py             # 入口
```

## 🌟 核心功能

### 分布式同步

```python
from sync import SyncService, SyncAble

class AppConfig(SyncAble):
    def __init__(self, rate: int):
        self.rate = rate
    
    @classmethod
    def topic(cls) -> str:
        return "app_config"

sync_service = SyncService(backend, namespace="myapp")
config_sync = await sync_service.sync(AppConfig)

# 发布配置
await sync_service.publish(AppConfig(rate=100))

# 监听变化
await config_sync.changed()
```

### Leader 选举

```python
from sync import LeaderElector

elector = LeaderElector(redis_client, "my_leader", ttl_ms=10000)
await elector.start()

if elector.is_leader:
    # 执行 leader 任务
    pass
```

### 缓存服务

```python
from cacheable import CacheService, LocalBackend

cache = CacheService(LocalBackend(), namespace="myapp")
await cache.set("key", b"value", ttl=3600)
value = await cache.get("key")
```

## 🔄 与 Rust 版本的对比

| 特性 | Rust | Python | 状态 |
|-----|------|--------|-----|
| 分布式同步 | ✅ | ✅ | 完成 |
| 缓存系统 | ✅ | ✅ | 完成 |
| Redis 队列 | ✅ | ✅ | 完成 |
| Kafka 队列 | ✅ | ✅ | 完成 |
| Leader 选举 | ✅ | ✅ | 完成 |
| Cron 调度 | ✅ | ✅ | 完成 |
| 事件总线 | ✅ | ✅ | 完成 |
| 中间件 | ✅ | ✅ | 完成 |

详见 [IMPLEMENTATION_REPORT.md](IMPLEMENTATION_REPORT.md)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

与主项目相同的许可证

## 🙏 致谢

本项目是 [Rust mocra](../) 的 Python 实现版本，完全复刻了其架构和功能。


Typical performance on local machine with `fakeredis` (50ms latency, 50 workers):
- **Concurrency**: Masks network delay effectively.
- **Throughput**: ~800+ items/sec (Mocked 50ms Network I/O)
- **Efficiency**: ~80% of theoretical maximum (Concurrency * 1/Latency)
