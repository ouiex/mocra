# HTTP API

HTTP API 通过 `[api]` 配置启用。它暴露运行时健康检查、指标、任务分发、配置、DLQ、调试和控制端点。

## 认证

公开端点：

- `GET /metrics`
- `GET /health`

其他端点受配置的 API key 保护。

## 健康检查和指标

```text
GET /health
GET /metrics
```

这些端点适合负载均衡器、readiness probe 和指标采集使用。

## 任务分发

```text
POST /tasks/dispatch
```

通过配置的 task/queue 路径向运行时分发任务。

## 集群

```text
GET /cluster/nodes
GET /cluster/leader
```

部署启用控制面时返回 Raft/控制面集群信息。

## 配置

```text
/config/accounts
/config/platforms
/config/modules
/config/middlewares
/config/profiles
```

这些端点管理任务/profile 解析使用的运行时配置模型。

## DLQ

```text
GET /dlq/messages
POST /dlq/messages/{id}/requeue
DELETE /dlq/messages/{id}
```

使用 DLQ 端点查看失败消息、重新入队可恢复消息，或删除不应重试的消息。

## Debug

```text
/debug/status/*
/debug/cache/response/{cache_key}
/debug/config/{account}/{platform}/{module}
/debug/profile/{account}/{platform}/{module}
```

Debug 端点用于检查运行时状态、缓存响应、解析后的配置和解析后的 profile。

## Control

```text
/control/pause
/control/resume
/control/fallback-gates/{module}
```

Control 端点用于运行时 pause/resume，以及模块 fallback gate 的检查或更新。

