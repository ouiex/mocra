# API 参考

> **从门面开始。** 跑一个爬虫用不到这些 —— [快速开始](getting-started.md) 的门面用法（`Mocra::builder().spider(...).run()`)才是主路径。本页介绍**可选的**可观测 / 后台管理 HTTP API,这是 `dashboard` cargo 特性背后的进阶功能。

启用 `dashboard` 特性后,引擎在同一个端口上提供两样东西:

1. 一套**只读可观测 HTTP API**（指标、健康、引擎/集群/主机快照、近期日志) —— 开启 CORS 且无需认证,浏览器或外部监控可直接消费。
2. 一个位于 `GET /` 的**内置单文件 Web UI** —— 指标 / 日志 / 任务 / 性能 面板,无需任何前端构建、无需手填 endpoint（页面同源自动指向本引擎)。

此外还提供一小组**写 / 控制**端点（任务注入、暂停/恢复、节点与 DLQ 查看),但这些需要 API key。

## 启用

添加特性:

```toml
mocra = { version = "0.4", features = ["dashboard"] }
```

然后用以下两种方式之一启用。

**门面（主路径）:** 在构建器上调用 `.dashboard(port)`。这会以无 API key 的方式暴露只读可观测 API 与 Web UI。单机模式下还会让引擎常驻（关闭空闲自停),并开启日志采集,让日志面板有数据可看。

```rust
Mocra::builder()
    .spider(MySpider, on_item(|item: Item| async move { /* ... */ }))
    .dashboard(12800)   // GET / → Web UI; /metrics、/observability/*
    .run()
    .await?;
```

```bash
cargo run --example dashboard --features dashboard   # 然后浏览器打开 http://127.0.0.1:12800
```

**TOML 配置（`from_toml`):** 添加 `[api]` 段。这是设置 **API key** 的方式,而写 / 控制端点需要它。

```toml
[api]
port = 12800
api_key = "your-secret-key"   # 使用写 / 控制端点所必需
rate_limit = 50               # 可选:每秒请求数,按调用方计
```

`[api]` 表恰好有三个键: `port`（u16）、`api_key`（可选字符串）、`rate_limit`（可选浮点,每秒请求数)。没有单独的限速时间窗口设置。

> 仅使用 `.dashboard(port)` 时不会配置 API key,因此下方的鉴权端点会返回 `403 Forbidden`。需配置带 `api_key` 的 `[api]` 才能使用它们。

## 访问层级

| 端点 | 方法 | 认证 | 限速 |
|---|---|---|---|
| `/` | GET | 无 | 否 |
| `/health` | GET | 无 | 否 |
| `/metrics` | GET | 无 | 是 |
| `/observability/engine` | GET | 无 | 是 |
| `/observability/cluster` | GET | 无 | 是 |
| `/observability/system` | GET | 无 | 是 |
| `/observability/logs` | GET | 无 | 是 |
| `/start_work` | POST | API key | 是 |
| `/nodes` | GET | API key | 是 |
| `/dlq` | GET | API key | 是 |
| `/control/pause` | POST | API key | 是 |
| `/control/resume` | POST | API key | 是 |

## 认证

鉴权路由接受以下任一请求头形式的 API key:

- `Authorization: Bearer <api_key>`
- `x-api-key: <api_key>`

key 错误返回 `401 Unauthorized`。若根本未配置 `api_key`,所有鉴权路由返回 `403 Forbidden`。

只读端点（`/`、`/health`、`/metrics`、`/observability/*`)无需 key,并附带宽松的 CORS 层,独立前端可跨域消费。

## 限速

设置了 `rate_limit` 时,除 `/` 与 `/health` 外的所有路由都受限速。限速器按调用方身份计数 —— `x-api-key` / `Bearer` 值,缺失时为 `"anonymous"` —— 速率为每秒 `rate_limit` 个请求。超限返回 `429 Too Many Requests`。

---

## 只读端点

### GET /

内置单文件 Web dashboard（HTML)。随 `dashboard` 特性编译进二进制;浏览器打开该端口即可查看面板。

**认证:** 无 · **限速:** 否

---

### GET /health

引擎后端服务的存活 / 就绪状态。

**认证:** 无 · **限速:** 否

**响应:**
```json
{
  "status": "up",
  "components": {
    "cache": { "status": "up" },
    "db":    { "status": "up" }
  }
}
```

所有组件为 up 时 `status` 为 `"up"`,否则为 `"degraded"`。每个组件为 `"up"` 或 `"down"`;down 的组件会附带 `error` 字符串。未启用 `store` 特性（或无 DB 模式)时,`db` 组件报告 `"up"`。

---

### GET /metrics

Prometheus 文本格式的指标。

**认证:** 无 · **限速:** 是

**响应:**
```
# TYPE mocra_latency_seconds histogram
mocra_latency_seconds_bucket{le="0.05"} 1200
# TYPE mocra_errors_total counter
mocra_errors_total 4
# TYPE mocra_inflight gauge
mocra_inflight 12
# TYPE mocra_backlog_depth gauge
mocra_backlog_depth 0
```

指标名以 `mocra_` 为前缀（延迟、错误、在途、积压深度、缓存命中/未命中、去重等)。

---

### GET /observability/engine

本节点的引擎 / 队列运行时快照。

**认证:** 无 · **限速:** 是

**响应:**
```json
{
  "namespace": "my_crawler",
  "single_node": true,
  "clustered": false,
  "pending": {
    "task": 0,
    "download": 3,
    "response": 1,
    "parser": 0,
    "error": 0,
    "remote_task": 0,
    "total": 4
  }
}
```

`namespace` 为配置的 `name`;`pending` 按阶段拆分本地队列深度。

---

### GET /observability/cluster

内嵌 Raft 集群状态。独立运行（无集群协调)时返回 `null`。

**认证:** 无 · **限速:** 是

**响应（集群模式):**
```json
{
  "node_id": 1,
  "is_leader": true,
  "current_leader": 1,
  "term": 4,
  "last_applied_index": 128,
  "member_count": 3,
  "voter_count": 3
}
```

---

### GET /observability/system

最近一次主机资源快照（引擎启动后不久开始采集)。首个采样前返回 `null`。

**认证:** 无 · **限速:** 是

**响应:**
```json
{
  "cpu_usage_percent": 12.4,
  "memory_used_bytes": 5368709120,
  "memory_total_bytes": 17179869184,
  "memory_usage_percent": 31.2,
  "swap_used_bytes": 0,
  "swap_total_bytes": 2147483648,
  "updated_at_ms": 1720598400000
}
```

---

### GET /observability/logs

近期结构化日志记录,最新在前。

**认证:** 无 · **限速:** 是

**查询参数:**

| 参数 | 默认 | 说明 |
|---|---|---|
| `limit` | `200` | 返回的最大记录数（上限 `1000`） |

**响应:**
```json
[
  {
    "time": "2026-07-10T10:30:00.123Z",
    "level": "INFO",
    "module": "engine",
    "message": "task completed"
  }
]
```

记录还可能携带可选的 `status` 与 `event_type` 字段。

---

## 鉴权端点

### POST /start_work

向引擎的处理队列注入任务。

**认证:** 需要 · **限速:** 是

**请求体** —— 一个 `TaskEvent`:
```json
{
  "account": "demo",
  "platform": "example",
  "module": ["my_module"]
}
```

`account` 与 `platform` 必填。`module` 是可选的模块名列表（省略或留空表示面向全部模块);`priority` 与 `run_id` 可选,默认自动填充。

**响应:** `200 OK`（无响应体)。

---

### GET /nodes

列出集群中的活跃节点。

**认证:** 需要 · **限速:** 是

**响应:**
```json
[
  {
    "id": "node-abc123",
    "ip": "10.0.0.4",
    "hostname": "worker-1",
    "last_heartbeat": 1720598400,
    "version": "0.4.0"
  }
]
```

---

### GET /dlq

查看死信队列（Dead Letter Queue)。

**认证:** 需要 · **限速:** 是

**查询参数:**

| 参数 | 默认 | 说明 |
|---|---|---|
| `topic` | `task` | 要读取的 DLQ 主题 |
| `count` | `10` | 获取的消息数 |

**响应:**
```json
[
  {
    "id": "1720598400000-0",
    "payload": "{...}",
    "reason": "max retries exceeded",
    "original_id": "1720598399000-0"
  }
]
```

---

### POST /control/pause

暂停全局引擎。处理器运行器停止从队列消费（暂停标志存于引擎命名空间下)。

**认证:** 需要 · **限速:** 是

**响应:** `200 OK`。

---

### POST /control/resume

清除暂停标志并恢复消费。

**认证:** 需要 · **限速:** 是

**响应:** `200 OK`。

## 参见

- [快速开始](getting-started.md) —— 门面快速上手（主路径）
- [部署](deployment.md) —— 单机 vs 分布式运行、监控
- [配置](configuration.md) —— 完整 TOML 参考,含 `[api]`
- [中间件](middleware-guide.md) —— 进阶的引擎级流水线钩子
- [`examples/dashboard.rs`](../../examples/dashboard.rs) —— 可运行的可观测示例
