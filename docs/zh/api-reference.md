# API 参考

mocra 内置了基于 [Axum](https://github.com/tokio-rs/axum) 的 HTTP 控制面板。通过在 `config.toml` 中添加 `[api]` 段来启用。

## 配置

```toml
[api]
port = 8080
api_key = "your-secret-key"
rate_limit = 100        # 每个时间窗口的请求数
rate_limit_window = 60  # 时间窗口（秒）
```

## 认证

受保护的路由需要 API Key，通过以下方式发送：

- **请求头:** `Authorization: Bearer <api_key>` 或 `x-api-key: <api_key>`

健康检查和指标端点不需要认证（指标端点受限速保护）。

## 端点

### GET /health

健康检查端点。

**认证:** 无  
**限速:** 否

**响应:**
```json
{
  "status": "ok"
}
```

---

### GET /metrics

Prometheus 文本格式的指标数据。

**认证:** 无  
**限速:** 是

**响应:**
```
# HELP mocra_requests_total Total requests processed
# TYPE mocra_requests_total counter
mocra_requests_total{module="my_module"} 1234
...
```

---

### POST /start_work

手动向引擎的处理队列注入任务。

**认证:** 需要  
**限速:** 是

**请求体:**
```json
{
  "module": "my_module",
  "platform": "example",
  "account": "demo",
  "meta": {
    "custom_key": "custom_value"
  }
}
```

请求体为 `TaskEvent` JSON 对象，至少需指定 `module` 名称。

**响应:** `200 OK`（无响应体）

---

### GET /nodes

列出集群中的活跃节点（分布式模式）。

**认证:** 需要  
**限速:** 是

**响应:**
```json
[
  {
    "node_id": "node-abc123",
    "last_heartbeat": "2024-01-15T10:30:00Z",
    "modules": ["module_a", "module_b"]
  }
]
```

---

### GET /dlq

查看死信队列（Dead Letter Queue）。

**认证:** 需要  
**限速:** 是

**响应:** 返回 DLQ 条目，包含失败任务详情。

---

### POST /control/pause

暂停全局引擎。所有处理器运行器将停止从队列消费。

**认证:** 需要  
**限速:** 是

**响应:** `200 OK`

---

### POST /control/resume

恢复已暂停的引擎。

**认证:** 需要  
**限速:** 是

**响应:** `200 OK`

## 限速

除 `/health` 外的所有路由均受限速保护。限速器使用基于客户端 IP 的滑动窗口。

配置方式：
```toml
[api]
rate_limit = 100        # 每个窗口的最大请求数
rate_limit_window = 60  # 窗口持续时间（秒）
```
