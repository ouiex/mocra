# mocra API 架构文档

> 本文档描述 mocra 新架构下的完整 HTTP API 设计，涵盖现有接口和规划中的配置管理、集群管理接口。  
> 所有配置写入操作均经由 Raft 共识同步至集群所有节点（详见主架构文档 §9、§19）。

---

## 目录

1. [认证与安全](#1-认证与安全)
2. [路由层次结构](#2-路由层次结构)
3. [配置管理 API](#3-配置管理-api)
   - 3.1 [Account 管理](#31-account-管理)
   - 3.2 [Platform 管理](#32-platform-管理)
   - 3.3 [Module 管理](#33-module-管理)
   - 3.4 [Middleware 管理](#34-middleware-管理)
   - 3.5 [Task Profile 管理](#35-task-profile-管理)
4. [任务控制 API](#4-任务控制-api)
5. [集群管理 API](#5-集群管理-api)
6. [DLQ 管理 API](#6-dlq-管理-api)
7. [观测性 API](#7-观测性-api)
8. [调试 API](#8-调试-api)
9. [完整路由汇总表](#9-完整路由汇总表)

---

## 1. 认证与安全

### 1.1 认证方式

所有受保护接口使用 API Key 认证，支持两种 Header 格式：

```
Authorization: Bearer <api_key>
x-api-key: <api_key>
```

API Key 在 `config.toml` 中配置：

```toml
[api]
api_key = "your-secret-key"
bind    = "0.0.0.0:8080"
```

未配置 `api_key` 时，所有受保护接口返回 `403 Forbidden`。

### 1.2 限流

所有接口（包括公开接口，除 `/health` 外）受全局速率限制保护。  
限流基于 `x-api-key` / `Authorization` Header 识别请求者，未认证请求归入 `anonymous` 桶。  
超出限额返回 `429 Too Many Requests`。

### 1.3 错误格式

```json
{
  "error": "描述信息",
  "code": "ERROR_CODE",
  "request_id": "uuid"
}
```

| HTTP 状态码 | 含义 |
|------------|------|
| `200 OK` | 成功 |
| `201 Created` | 资源创建成功 |
| `204 No Content` | 删除/禁用成功 |
| `400 Bad Request` | 请求体格式错误 |
| `401 Unauthorized` | 缺少或无效 API Key |
| `403 Forbidden` | 未配置 API Key |
| `404 Not Found` | 资源不存在 |
| `409 Conflict` | 资源已存在（唯一约束冲突）|
| `429 Too Many Requests` | 超出速率限制 |
| `503 Service Unavailable` | Raft 不可用（无 Leader）|

### 1.4 Raft 写入转发

任意节点均可接收写请求。若当前节点不是 Leader，API 层自动将请求转发至当前 Leader，对调用方透明。  
若集群无 Leader（选举中），写接口返回 `503`，读接口仍可通过租约读提供服务。

---

## 2. 路由层次结构

```
/
├── health                          [公开，无限流]
├── metrics                         [公开，有限流]
│
├── /cluster                        [需认证 + 限流] ← 集群管理
│   ├── GET  /cluster/nodes
│   ├── GET  /cluster/leader
│   └── POST /cluster/transfer-leader
│
├── /config                         [需认证 + 限流] ← 配置管理（写 Raft）
│   ├── /config/accounts/...
│   ├── /config/platforms/...
│   ├── /config/modules/...
│   ├── /config/middlewares/...
│   └── /config/profiles/...
│
├── /tasks                          [需认证 + 限流] ← 任务控制
│   ├── POST /tasks/dispatch
│   └── GET  /tasks/running
│
├── /dlq                            [需认证 + 限流] ← 死信队列
│   ├── GET  /dlq
│   ├── POST /dlq/{id}/requeue
│   └── DELETE /dlq/{id}
│
├── /control                        [需认证 + 限流] ← 引擎控制
│   ├── POST /control/pause
│   └── POST /control/resume
│
└── /debug                          [需认证 + 限流] ← 调试（生产可禁用）
  ├── GET /debug/status/{task_id}
  ├── GET /debug/status/stage/{stage}
  ├── GET /debug/status/counts
    ├── GET /debug/config/{account}/{platform}/{module}
    └── GET /debug/profile/{account}/{platform}/{module}
```

---

## 3. 配置管理 API

> 所有写操作（POST / PUT / PATCH / DELETE）均提交 Raft 命令，经多数派共识后持久化，并自动同步到所有节点的本地状态机。
> 读操作使用租约读，从本地 RocksDB 返回，0 网络 RTT。

### 3.1 Account 管理

#### `GET /config/accounts`

列出当前命名空间下所有 Account。

**响应：**
```json
[
  {
    "name": "account_a",
    "enabled": true,
    "priority": 10,
    "config": { "timeout": 30 },
    "version": 5,
    "updated_at": "2024-04-10T06:00:00Z"
  }
]
```

---

#### `GET /config/accounts/{name}`

获取单个 Account 详情。

**响应：** 同上单条记录，`404` 若不存在。

---

#### `POST /config/accounts`

创建 Account。写入 Raft，同步至所有节点。

**请求体：**
```json
{
  "name": "account_a",
  "enabled": true,
  "priority": 10,
  "config": { "timeout": 30 }
}
```

**响应：** `201 Created` + 创建后的完整对象。  
**Raft Command：** `UpsertAccount`

---

#### `PUT /config/accounts/{name}`

全量更新 Account。

**请求体：** 同 POST（不含 `name`）  
**Raft Command：** `UpsertAccount`

---

#### `PATCH /config/accounts/{name}`

部分更新 Account（仅更新提供的字段）。

**请求体：**
```json
{
  "enabled": false
}
```

**注意：** 若 Account 被禁用，其所有关联 `task_profile` 的 `enabled` 状态由 Profile 自身管理，不自动级联禁用。需额外调用 `POST /config/profiles/disable-by-account/{name}`。

---

#### `DELETE /config/accounts/{name}`

删除 Account（软删除，标记 disabled）。  
不会自动删除关联的 `task_profile`，需先确认无运行中任务。  
**响应：** `204 No Content`

---

### 3.2 Platform 管理

与 Account 管理结构相同，路径前缀为 `/config/platforms`。

#### `GET /config/platforms`
#### `GET /config/platforms/{name}`
#### `POST /config/platforms`

**请求体：**
```json
{
  "name": "shopee",
  "description": "Shopee 电商平台",
  "base_url": "https://shopee.sg",
  "enabled": true,
  "config": { "rate_limit_rps": 5 }
}
```

**Raft Command：** `UpsertPlatform`

#### `PUT /config/platforms/{name}`
#### `PATCH /config/platforms/{name}`
#### `DELETE /config/platforms/{name}`

---

### 3.3 Module 管理

路径前缀为 `/config/modules`。

#### `GET /config/modules`
#### `GET /config/modules/{name}`
#### `POST /config/modules`

**请求体：**
```json
{
  "name": "search",
  "enabled": true,
  "priority": 5,
  "config": {
    "page_size": 20,
    "max_pages": 100
  }
}
```

**Raft Command：** `UpsertModule`

#### `PUT /config/modules/{name}`
#### `PATCH /config/modules/{name}`
#### `DELETE /config/modules/{name}`

---

### 3.4 Middleware 管理

路径前缀为 `/config/middlewares`，分下载中间件和数据中间件两类。

#### `GET /config/middlewares`

**Query：** `?type=download|data`（不传则返回全部）

**响应：**
```json
[
  {
    "name": "proxy_mw",
    "type": "download",
    "weight": 10,
    "enabled": true,
    "config": { "pool_size": 100 }
  }
]
```

---

#### `GET /config/middlewares/{name}`

**Query：** `?type=download|data`（名称不唯一时区分类型）

---

#### `POST /config/middlewares`

**请求体：**
```json
{
  "name": "proxy_mw",
  "type": "download",
  "weight": 10,
  "enabled": true,
  "config": { "pool_size": 100 }
}
```

**`weight` 说明：** 数值越小越先执行（与 `MiddlewareTrait::weight()` 对应）。  
**Raft Command：** `UpsertMiddleware`

---

#### `PUT /config/middlewares/{name}`
#### `PATCH /config/middlewares/{name}`
#### `DELETE /config/middlewares/{name}`

**注意：** 更新中间件 config 后，服务端自动重算所有引用此中间件的 `task_profile.config`，并批量提交 Raft 更新。

---

### 3.5 Task Profile 管理

Task Profile 是 `(namespace, account, platform, module)` 四元组的运行时配置实体，替代原有 5 张关系表（见主文档 §19.4）。

#### `GET /config/profiles`

列出当前命名空间下所有 Task Profile。

**Query：**
- `account=<name>` — 按 account 过滤
- `platform=<name>` — 按 platform 过滤
- `module=<name>` — 按 module 过滤
- `enabled=true|false`

**响应：**
```json
[
  {
    "account": "account_a",
    "platform": "shopee",
    "module": "search",
    "download_middleware": ["proxy_mw", "ua_rotate"],
    "data_middleware": ["json_parser", "pg_store"],
    "config": { "page_size": 20, "proxy_pool": "sg" },
    "enabled": true,
    "priority": 5,
    "version": 12,
    "updated_at": "2024-04-10T06:00:00Z",
    "updated_by": "admin"
  }
]
```

---

#### `GET /config/profiles/{account}/{platform}/{module}`

获取单个 Task Profile。

---

#### `POST /config/profiles`

创建 Task Profile。服务端自动完成 config 预合并。

**请求体：**
```json
{
  "account": "account_a",
  "platform": "shopee",
  "module": "search",
  "download_middleware": ["proxy_mw", "ua_rotate"],
  "data_middleware": ["json_parser", "pg_store"],
  "config": {
    "proxy_pool": "sg"
  },
  "enabled": true,
  "priority": 5
}
```

**服务端行为：**
1. 从 Raft 状态机读取 account/platform/module/middleware 各实体 config
2. 执行 `ConfigAssembler::merge()`，合并 config 层级
3. 提交 `RaftCommand::UpsertTaskProfile`（含预合并结果）
4. Raft commit 后返回 `201 Created`

**响应：** 含 `config`（预合并后完整配置）的 Task Profile 对象。

---

#### `PUT /config/profiles/{account}/{platform}/{module}`

全量替换 Task Profile。自动重新触发 config 预合并。

---

#### `PATCH /config/profiles/{account}/{platform}/{module}`

部分更新。仅支持以下字段：
- `download_middleware`
- `data_middleware`
- `config`（会与现有 config 深度合并）
- `enabled`
- `priority`

更新后自动重新触发 config 预合并，提交 Raft。

**典型用途：**
```json
// 热更新：为特定组合切换下载代理
{ "download_middleware": ["premium_proxy", "ua_rotate"] }

// 热更新：修改某参数
{ "config": { "page_size": 50 } }

// 临时禁用
{ "enabled": false }
```

---

#### `DELETE /config/profiles/{account}/{platform}/{module}`

删除 Task Profile。**Raft Command：** `DisableTaskProfile`（软删除）。  
**响应：** `204 No Content`

---

#### `POST /config/profiles/disable-by-account/{account}`

批量禁用某 account 下的所有 Task Profile。  
**Raft Command：** `BatchUpsertTaskProfiles`（批量 enabled=false）  
**响应：** `200 OK` + `{ "affected": 15 }`

---

#### `POST /config/profiles/batch`

批量创建或更新 Task Profile，单次 Raft 写入，原子提交。

**请求体：**
```json
{
  "profiles": [
    {
      "account": "account_a",
      "platform": "shopee",
      "module": "search",
      "download_middleware": ["proxy_mw"],
      "data_middleware": ["json_parser"],
      "config": {}
    }
  ]
}
```

**Raft Command：** `BatchUpsertTaskProfiles`

---

## 4. 任务控制 API

#### `POST /tasks/dispatch`

手动下发 TaskEvent 到处理队列（等效于原 `POST /start_work`，路径重命名）。

**请求体：**
```json
{
  "account":  "account_a",
  "platform": "shopee",
  "module":   ["search", "detail"],
  "priority": "high",
  "run_id":   "01900000-0000-7000-0000-000000000000"
}
```

**字段说明：**
- `module`：可选。不传则触发此 account+platform 下所有 enabled 模块
- `priority`：可选，默认 `normal`。枚举：`low | normal | high`
- `run_id`：可选，不传时服务端自动生成 UUID v7

**响应：** `200 OK` + `{ "run_id": "...", "queued_modules": ["search", "detail"] }`

---

#### `GET /tasks/running`

查询当前节点正在运行的任务。

**响应：**
```json
[
  {
    "run_id":   "01900000-...",
    "account":  "account_a",
    "platform": "shopee",
    "modules":  ["search"],
    "started_at": "2024-04-10T06:00:00Z",
    "status":   "running"
  }
]
```

---

## 5. 集群管理 API

> 对应主文档 §9 Raft 集群管理。

#### `GET /cluster/nodes`

列出集群中所有已知节点（等效于原 `GET /nodes`）。

**响应：**
```json
[
  {
    "node_id":   "node-001",
    "namespace": "crawler_ns",
    "address":   "10.0.0.1:8080",
    "raft_port": 9090,
    "role":      "leader",
    "last_seen": "2024-04-10T06:00:00Z",
    "version":   "0.4.2"
  }
]
```

---

#### `GET /cluster/leader`

返回当前 Leader 节点信息。若集群无 Leader，返回 `503`。

**响应：**
```json
{
  "mode": "raft",
  "local_node_id": 8615780017974386108,
  "local_node_addr": "127.0.0.1:3201",
  "leader_id": 8615780017974386108,
  "leader_addr": "127.0.0.1:3201",
  "is_local_leader": true
}
```

当节点未启用 Raft 控制面时，该接口返回：

```json
{
  "mode": "local",
  "local_node_id": null,
  "local_node_addr": null,
  "leader_id": null,
  "leader_addr": null,
  "is_local_leader": true
}
```

---

#### `POST /cluster/transfer-leader`

主动转移 Leader 到指定节点（用于滚动重启、维护）。

**请求体：**
```json
{ "target_node_id": "node-002" }
```

**响应：** `200 OK` 或 `503`（转移失败）。

---

## 6. DLQ 管理 API

死信队列（Dead Letter Queue）存放处理失败超过重试次数的消息。

#### `GET /dlq`

查看 DLQ 中的消息。

**Query：**
- `topic=task|request|response|parser`（默认 `task`）
- `count=10`（默认 10，最大 100）

**响应：**
```json
[
  {
    "id":          "1712700000000-0",
    "topic":       "task",
    "payload":     "{\"account\":\"account_a\",...}",
    "reason":      "max_retries_exceeded",
    "original_id": "1712699000000-0",
    "enqueued_at": "2024-04-10T05:00:00Z"
  }
]
```

---

#### `POST /dlq/{id}/requeue`

将指定消息重新投入原队列（手动重试）。

**Path：** `id` 为 DLQ 消息 ID  
**Query：** `topic=task`（指定消息所在的 topic）  
**响应：** `200 OK` + `{ "requeued": true, "new_id": "..." }`

---

#### `DELETE /dlq/{id}`

从 DLQ 中永久删除消息（确认丢弃）。  
**Response：** `204 No Content`

---

#### `POST /dlq/requeue-all`

批量重新投递某 topic 下所有 DLQ 消息。  
**Query：** `topic=task`  
**响应：** `200 OK` + `{ "requeued": 42 }`

---

## 7. 观测性 API

#### `GET /health`

系统健康检查（公开，无认证，无限流）。

**响应：**
```json
{
  "status": "up",
  "components": {
    "db":    { "status": "up" },
    "raft":  { "status": "up", "role": "follower", "leader": "node-001" },
    "cache": { "status": "up", "engine": "rocksdb" }
  }
}
```

`status` 为 `"up"` 或 `"degraded"`（任一组件异常）。

---

#### `GET /metrics`

Prometheus 格式指标（公开，有限流）。

**响应：** `text/plain; version=0.0.4` 格式的 Prometheus metrics 文本。

**主要指标：**

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `mocra_tasks_total` | Counter | 任务总处理次数，按 `account`/`platform`/`status` 分组 |
| `mocra_requests_total` | Counter | HTTP 请求总次数（下载器维度）|
| `mocra_request_duration_seconds` | Histogram | 下载请求耗时分布 |
| `mocra_queue_depth` | Gauge | 各 topic 队列深度 |
| `mocra_dlq_size` | Gauge | DLQ 消息积压量 |
| `mocra_raft_term` | Gauge | 当前 Raft term |
| `mocra_raft_commit_index` | Gauge | Raft commit index |
| `mocra_active_tasks` | Gauge | 当前运行中任务数 |
| `mocra_middleware_duration_seconds` | Histogram | 中间件执行耗时 |

---

## 8. 调试 API

> 仅用于开发和问题排查，生产环境建议通过 `config.toml` 禁用或限制访问。

#### `GET /debug/config/{account}/{platform}/{module}`

查看某 Task Profile 的**各层原始配置**（未合并）。

**响应：**
```json
{
  "profile_key": "ns:profile:account_a:shopee:search",
  "merged": { "page_size": 50, "proxy_pool": "sg" },
  "layers": {
    "account_config":             { "timeout": 30 },
    "platform_config":            { "rate_limit_rps": 5 },
    "module_config":              { "page_size": 20 },
    "task_profile_config":        { "page_size": 50, "proxy_pool": "sg" },
    "download_middleware_config": { "proxy_mw": { "pool_size": 100 } },
    "data_middleware_config":     { "json_parser": {} }
  },
  "version": 12
}
```

用于排查"为什么最终配置值是这个"。

---

#### `GET /debug/profile/{account}/{platform}/{module}`

查看 Task Profile 完整快照（含 Raft 元数据）。

**响应：**
```json
{
  "account":             "account_a",
  "platform":            "shopee",
  "module":              "search",
  "download_middleware": ["proxy_mw", "ua_rotate"],
  "data_middleware":     ["json_parser", "pg_store"],
  "config":              { "page_size": 50 },
  "enabled":             true,
  "version":             12,
  "raft_index":          1042,
  "updated_by":          "admin",
  "updated_at":          "2024-04-10T06:00:00Z"
}
```

---

#### `GET /debug/status/{task_id}`

查看某个任务当前持久化的状态记录。

**响应：**
```json
{
  "task_id": "01900000-0000-7000-0000-000000000000",
  "stage": "response",
  "status": "done",
  "retry_count": 1,
  "node_id": "node-001",
  "updated_at": 1712728800000,
  "error_msg": null,
  "version": 1049
}
```

---

#### `GET /debug/status/stage/{stage}`

按 pipeline stage 查看最近更新的状态记录。

**Path 参数：**
- `stage`：`task | request | response | parser_task | error`

**Query：**
- `limit=<n>`：返回条数，默认 `50`，最大 `500`

**响应：**
```json
[
  {
    "task_id": "01900000-0000-7000-0000-000000000000",
    "stage": "response",
    "status": "done",
    "retry_count": 1,
    "node_id": "node-001",
    "updated_at": 1712728800000,
    "error_msg": null,
    "version": 1049
  }
]
```

---

#### `GET /debug/status/counts`

查看当前命名空间中各任务状态的聚合计数。

**响应：**
```json
{
  "counts": {
    "pending": 0,
    "running": 3,
    "done": 128,
    "failed": 2,
    "retrying": 5
  }
}
```

---

## 9. 完整路由汇总表

| 方法 | 路径 | 认证 | 限流 | Raft写 | 说明 |
|------|------|:----:|:----:|:------:|------|
| GET | `/health` | ❌ | ❌ | ❌ | 健康检查 |
| GET | `/metrics` | ❌ | ✅ | ❌ | Prometheus 指标 |
| **集群管理** | | | | | |
| GET | `/cluster/nodes` | ✅ | ✅ | ❌ | 列出集群节点 |
| GET | `/cluster/leader` | ✅ | ✅ | ❌ | 查看当前 Leader |
| POST | `/cluster/transfer-leader` | ✅ | ✅ | ❌ | 转移 Leader |
| **Account** | | | | | |
| GET | `/config/accounts` | ✅ | ✅ | ❌ | 列出 Account |
| GET | `/config/accounts/{name}` | ✅ | ✅ | ❌ | 查看 Account |
| POST | `/config/accounts` | ✅ | ✅ | ✅ | 创建 Account |
| PUT | `/config/accounts/{name}` | ✅ | ✅ | ✅ | 全量更新 Account |
| PATCH | `/config/accounts/{name}` | ✅ | ✅ | ✅ | 部分更新 Account |
| DELETE | `/config/accounts/{name}` | ✅ | ✅ | ✅ | 删除 Account |
| **Platform** | | | | | |
| GET | `/config/platforms` | ✅ | ✅ | ❌ | 列出 Platform |
| GET | `/config/platforms/{name}` | ✅ | ✅ | ❌ | 查看 Platform |
| POST | `/config/platforms` | ✅ | ✅ | ✅ | 创建 Platform |
| PUT | `/config/platforms/{name}` | ✅ | ✅ | ✅ | 全量更新 Platform |
| PATCH | `/config/platforms/{name}` | ✅ | ✅ | ✅ | 部分更新 Platform |
| DELETE | `/config/platforms/{name}` | ✅ | ✅ | ✅ | 删除 Platform |
| **Module** | | | | | |
| GET | `/config/modules` | ✅ | ✅ | ❌ | 列出 Module |
| GET | `/config/modules/{name}` | ✅ | ✅ | ❌ | 查看 Module |
| POST | `/config/modules` | ✅ | ✅ | ✅ | 创建 Module |
| PUT | `/config/modules/{name}` | ✅ | ✅ | ✅ | 全量更新 Module |
| PATCH | `/config/modules/{name}` | ✅ | ✅ | ✅ | 部分更新 Module |
| DELETE | `/config/modules/{name}` | ✅ | ✅ | ✅ | 删除 Module |
| **Middleware** | | | | | |
| GET | `/config/middlewares` | ✅ | ✅ | ❌ | 列出中间件 |
| GET | `/config/middlewares/{name}` | ✅ | ✅ | ❌ | 查看中间件 |
| POST | `/config/middlewares` | ✅ | ✅ | ✅ | 创建中间件 |
| PUT | `/config/middlewares/{name}` | ✅ | ✅ | ✅ | 全量更新中间件 |
| PATCH | `/config/middlewares/{name}` | ✅ | ✅ | ✅ | 部分更新中间件 |
| DELETE | `/config/middlewares/{name}` | ✅ | ✅ | ✅ | 删除中间件 |
| **Task Profile** | | | | | |
| GET | `/config/profiles` | ✅ | ✅ | ❌ | 列出 Task Profile |
| GET | `/config/profiles/{account}/{platform}/{module}` | ✅ | ✅ | ❌ | 查看 Profile |
| POST | `/config/profiles` | ✅ | ✅ | ✅ | 创建 Profile（含预合并）|
| PUT | `/config/profiles/{account}/{platform}/{module}` | ✅ | ✅ | ✅ | 全量更新 Profile |
| PATCH | `/config/profiles/{account}/{platform}/{module}` | ✅ | ✅ | ✅ | 热更新中间件/参数 |
| DELETE | `/config/profiles/{account}/{platform}/{module}` | ✅ | ✅ | ✅ | 删除 Profile |
| POST | `/config/profiles/disable-by-account/{account}` | ✅ | ✅ | ✅ | 批量禁用 Account 下 Profile |
| POST | `/config/profiles/batch` | ✅ | ✅ | ✅ | 批量创建/更新 Profile |
| **任务控制** | | | | | |
| POST | `/tasks/dispatch` | ✅ | ✅ | ❌ | 下发任务 |
| GET | `/tasks/running` | ✅ | ✅ | ❌ | 查看运行中任务 |
| POST | `/control/pause` | ✅ | ✅ | ❌ | 暂停引擎 |
| POST | `/control/resume` | ✅ | ✅ | ❌ | 恢复引擎 |
| **DLQ** | | | | | |
| GET | `/dlq` | ✅ | ✅ | ❌ | 查看 DLQ |
| POST | `/dlq/{id}/requeue` | ✅ | ✅ | ❌ | 重新投递 |
| DELETE | `/dlq/{id}` | ✅ | ✅ | ❌ | 丢弃消息 |
| POST | `/dlq/requeue-all` | ✅ | ✅ | ❌ | 批量重新投递 |
| **调试** | | | | | |
| GET | `/debug/status/{task_id}` | ✅ | ✅ | ❌ | 查看任务状态记录 |
| GET | `/debug/status/stage/{stage}` | ✅ | ✅ | ❌ | 按 stage 查看任务状态 |
| GET | `/debug/status/counts` | ✅ | ✅ | ❌ | 查看任务状态聚合计数 |
| GET | `/debug/config/{account}/{platform}/{module}` | ✅ | ✅ | ❌ | 查看配置分层 |
| GET | `/debug/profile/{account}/{platform}/{module}` | ✅ | ✅ | ❌ | 查看 Profile 快照 |

---

*文档版本：与主架构文档 `decentralized-crawler-architecture-v2.md` §9、§13、§18、§19 对应*
