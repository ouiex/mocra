# 配置指南 (Configuration)

> 架构、流程、分布式部署等统一入口见 `docs/README.md`。

本项目使用 TOML 作为统一配置格式。建议使用 **URL + 少量必要字段** 的方式，减少冗余与错误配置。

## 1. 推荐配置结构

```toml
name = "crawler"

[db]
# 推荐使用 url，减少冗余字段
url = "postgres://user:password@localhost:5432/crawler"
# 可选：schema
database_schema = "base"
# 可选：连接池
pool_size = 10

[cache]
ttl = 60

[cache.redis]
redis_host = "127.0.0.1"
redis_port = 6379
redis_db = 0
pool_size = 50

[channel_config]
minid_time = 12
capacity = 5000
compression_threshold = 1024

[channel_config.redis]
redis_host = "127.0.0.1"
redis_port = 6379
redis_db = 0
pool_size = 100
shards = 8
listener_count = 8

[download_config]
# 建议根据压测调整
pool_size = 200
# 秒
timeout = 30
rate_limit = 0
enable_cache = false
enable_locker = false
enable_rate_limit = false
cache_ttl = 60
wss_timeout = 60

[crawler]
request_max_retries = 3
task_max_errors = 100
module_max_errors = 10
module_locker_ttl = 5
task_concurrency = 200
```

## 2. 数据库配置

仅支持 `db.url`：
- `db.url = "postgres://user:password@host:5432/db"`

## 3. Redis 配置

目前使用 `redis_host/redis_port/redis_db` 结构。用于：
- 缓存 (`cache.redis`)
- 队列 (`channel_config.redis`)
- Cookie/锁/限流 (`cookie`/`cache` 共用池)

## 4. 日志配置（简化版）

默认写入 `logs/mocra.{name}`。可用环境变量覆盖：
- `MOCRA_LOG_LEVEL`
- `MOCRA_LOG_FILE`（`none/off/false` 可关闭文件输出）
- `MOCRA_LOG_CONSOLE`
- `MOCRA_LOG_JSON`
- `DISABLE_LOGS` / `MOCRA_DISABLE_LOGS`

## 5. 测试配置参考

- [tests/config.test.toml](tests/config.test.toml)
- [tests/config.mock.toml](tests/config.mock.toml)
