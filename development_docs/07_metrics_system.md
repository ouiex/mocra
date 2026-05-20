# 指标体系重构方案

## 1. 文档目的

本文档用于把“指标体系重构”从架构层描述拆成可执行的开发口径。  
主目标不是简单补几个 Prometheus 指标，而是把项目的观测能力收敛成一套能够稳定回答以下问题的体系：

1. 当前链路有没有堵塞。
2. 堵在 task / queue / download / parse / DAG / control plane 的哪一层。
3. 是单节点问题、后端问题、配置漂移问题，还是调度恢复问题。
4. 指标切换后，旧 dashboard 和告警不会立刻失效。

## 2. 当前代码基线

### 2.1 已有能力

当前代码已经具备以下基础：

- `Engine::new()` 安装 `PrometheusBuilder`
- `GET /metrics` 输出 `PrometheusHandle::render()`
- `/metrics` 当前虽然免认证，但仍经过 `rate_limit_middleware`
- `start_health_monitor()` 每 30 秒清理限流器过期键并发布 `SystemHealth` 事件，但它不是 HTTP `/health`
- `src\common\metrics.rs` 已有基础 helper：
  - `mocra_node_up`
  - `mocra_component_health`
  - `mocra_resource_usage`
  - `mocra_backlog_depth`
  - `mocra_inflight`
  - `mocra_throughput_total`
  - `mocra_latency_seconds`
  - `mocra_errors_total`
  - `mocra_policy_decisions_total`

### 2.2 当前问题

当前代码的主要问题不是“完全没指标”，而是“指标太分散、语义不统一”：

1. `common::metrics` 与大量 `counter! / histogram! / gauge!` 直接打点并存。
2. queue、DAG、scheduler、dedup、cache、logger 各自维护一套命名和标签。
3. 单位混用 `_us / _ms / _seconds`。
4. API、control plane、config hot update、profile version skew 观测明显不足。
5. Redis 兼容路径指标较多，Kafka / in-memory / 未来 `Raft + RocksDB` 路径不对称。
6. 当前 `init_metrics()` 调用点把 `config.name` 当成 node 维度，分布式下会混淆 `namespace` 与 `node_id`。
7. `/metrics` 当前与未认证请求共享 `"anonymous"` 限流桶，Prometheus 抓取容易被匿名流量扰动。
8. `HealthMonitor`、`/health` 路由和 Prometheus 指标输出目前还是三套近似独立信号源，`mocra_component_health` 的覆盖范围也不足以表达完整组件健康。

## 3. 目标设计

### 3.1 三层模型

| 层级 | 目标 | 典型用途 |
|------|------|----------|
| L1 核心流水线指标 | 统一看吞吐、延迟、错误、积压、并发 | 默认 dashboard、SLO、回归对比 |
| L2 子系统指标 | 统一看 queue / dag / scheduler / downloader / api / config / coordination 健康 | 故障定位 |
| L3 调试指标 | 保留 PTM、remote dispatcher、Lua action、细粒度 backpressure | 深度排障 |

### 3.2 标签规范

#### 必带标签

- `namespace`
- `node_id`
- `component`
- `deployment_mode`

#### 按需标签

- `pipeline`
- `stage`
- `backend`
- `result`
- `error_class`
- `error_code`
- `module`
- `workflow`
- `node_name`

#### 明确禁止

- `account`
- `request_id`
- `task_id`
- `run_id`
- 原始 URL
- 原始异常消息

### 3.3 命名规则

1. Counter 统一以 `_total` 结尾。
2. Histogram 统一以 `_duration_seconds` 或 `_bytes` 结尾。
3. Gauge 只表达当前值。
4. 新指标不再增加 `_ms / _us` 后缀。
5. `error_code` 必须来自受控枚举，不允许把错误字符串直接做标签。

## 4. 指标族清单

### 4.1 核心流水线指标（L1）

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `mocra_stage_events_total` | Counter | `namespace,node_id,component,pipeline,stage,action,result,module?` | 统一吞吐 |
| `mocra_stage_duration_seconds` | Histogram | 同上 | 统一阶段耗时 |
| `mocra_stage_errors_total` | Counter | `namespace,node_id,component,pipeline,stage,error_class,error_code,module?` | 统一错误 |
| `mocra_stage_inflight` | Gauge | `namespace,node_id,component,pipeline,stage` | 在途任务 |
| `mocra_stage_backlog` | Gauge | `namespace,node_id,component,pipeline,queue,priority,backend` | 积压深度 |

必须覆盖的主链路：

- task ingress
- request publish
- download
- response parse
- parser_task dispatch
- error handling
- 目标态 DAG dispatch / recover

### 4.2 Queue / MQ

| 指标名 | 类型 | 关键标签 |
|--------|------|----------|
| `mocra_queue_messages_total` | Counter | `backend,topic,priority,operation,result` |
| `mocra_queue_message_bytes_total` | Counter | `backend,topic,operation` |
| `mocra_queue_operation_duration_seconds` | Histogram | `backend,topic,operation,result` |
| `mocra_queue_backlog` | Gauge | `backend,topic,priority` |
| `mocra_queue_consumer_lag` | Gauge | `backend,topic,consumer_group` |
| `mocra_queue_redelivery_total` | Counter | `backend,topic,reason` |
| `mocra_queue_dlq_messages_total` | Counter | `backend,topic,reason` |
| `mocra_queue_codec_total` | Counter | `backend,codec,operation,result` |

### 4.3 Scheduler / DAG

| 域 | 指标 |
|----|------|
| Scheduler | `mocra_scheduler_ticks_total`、`mocra_scheduler_tick_duration_seconds`、`mocra_scheduler_lock_total`、`mocra_scheduler_lock_duration_seconds`、`mocra_scheduler_triggers_total`、`mocra_scheduler_misfire_total` |
| DAG run | `mocra_dag_runs_total`、`mocra_dag_run_duration_seconds`、`mocra_dag_oldest_incomplete_run_age_seconds` |
| DAG node | `mocra_dag_nodes_total`、`mocra_dag_node_duration_seconds`、`mocra_dag_dispatch_total`、`mocra_dag_ready_nodes` |
| DAG guard/fencing | `mocra_dag_run_guard_total`、`mocra_dag_run_guard_duration_seconds`、`mocra_dag_fencing_total`、`mocra_dag_recovery_total`、`mocra_dag_state_store_total` |

### 4.4 Downloader / Proxy / 限流

| 指标名 | 类型 | 关键标签 |
|--------|------|----------|
| `mocra_http_requests_total` | Counter | `module,host,method,status_class,result,proxy_group?` |
| `mocra_http_request_duration_seconds` | Histogram | `module,host,method,status_class,result` |
| `mocra_http_response_bytes_total` | Counter | `module,host,status_class` |
| `mocra_http_phase_duration_seconds` | Histogram | `phase` |
| `mocra_proxy_acquire_total` | Counter | `proxy_group,result` |
| `mocra_proxy_pool_size` | Gauge | `proxy_group,state` |
| `mocra_proxy_health` | Gauge | `proxy_group` |
| `mocra_rate_limit_actions_total` | Counter | `action,result` |

### 4.5 Parser / Data / Store

| 指标名 | 类型 | 关键标签 |
|--------|------|----------|
| `mocra_generate_tasks_total` | Counter | `module,result` |
| `mocra_parse_outputs_total` | Counter | `module,node_name,output_type,result` |
| `mocra_parse_duration_seconds` | Histogram | `module,node_name,result` |
| `mocra_parsed_records_total` | Counter | `module,entity` |
| `mocra_parse_empty_total` | Counter | `module,reason` |
| `mocra_data_store_writes_total` | Counter | `store,entity,result` |
| `mocra_data_store_duration_seconds` | Histogram | `store,entity,result` |
| `mocra_data_quality_total` | Counter | `module,rule,result` |

### 4.6 API / Control / Config

| 指标名 | 类型 | 关键标签 |
|--------|------|----------|
| `mocra_api_requests_total` | Counter | `route,method,status_class` |
| `mocra_api_request_duration_seconds` | Histogram | `route,method,status_class` |
| `mocra_api_auth_total` | Counter | `route,result` |
| `mocra_api_rate_limit_total` | Counter | `route,result` |
| `mocra_control_actions_total` | Counter | `action,result` |
| `mocra_config_updates_total` | Counter | `scope,result` |
| `mocra_config_apply_duration_seconds` | Histogram | `scope,result` |
| `mocra_config_applied_version` | Gauge | `node_id,module` |
| `mocra_config_version_skew_nodes` | Gauge | `module` |
| `mocra_pause_state` | Gauge | `namespace` |

### 4.7 Coordination / Cache / Dedup / Logger

| 域 | 指标 |
|----|------|
| Coordination | `mocra_cluster_nodes_active`、`mocra_cluster_heartbeat_age_seconds`、`mocra_leader_changes_total`、`mocra_coordination_ops_total`、`mocra_lock_acquire_total`、`mocra_lock_wait_duration_seconds`、`mocra_sync_messages_total`、`mocra_sync_message_duration_seconds` |
| Cache | `mocra_cache_ops_total`、`mocra_cache_duration_seconds`、`mocra_cache_evictions_total` |
| Dedup | `mocra_dedup_checks_total`、`mocra_dedup_duration_seconds` |
| Logger | `mocra_log_events_total`、`mocra_log_dropped_total`、`mocra_log_queue_lag` |

## 5. 开发顺序

建议顺序固定为：

1. 先修 `MetricsScope` 和标签模型。
2. 再重构 `common::metrics` facade。
3. 再为主链路补齐 L1 五件套。
4. 再对齐 queue backend。
5. 再补 DAG / scheduler / API / config / coordination。
6. 最后收口 downloader / cache / dedup / logger 和旧散点指标。

## 6. 切换映射

| 当前指标族 | 目标指标族 | 处理策略 |
|------------|------------|----------|
| `mocra_throughput_total` | `mocra_stage_events_total` | 在 cutover 阶段同步更新 dashboard/query，不要求 runtime 双写 |
| `mocra_latency_seconds` | `mocra_stage_duration_seconds` | 统一切换到 seconds 语义，不保留旧命名作为主路径 |
| `mocra_errors_total` | `mocra_stage_errors_total` | 由 stage/error_class/error_code 重新建模 |
| `mocra_backlog_depth` | `mocra_queue_backlog` / `mocra_stage_backlog` | 按 queue/stage 拆分并同步修改看板 |
| `mocra_policy_decisions_total` | 各域指标 + L3 policy 调试指标 | 收口为调试指标，不再作为默认总览指标 |
| `mocra_dag_remote_*` | `mocra_dag_dispatch_total` 等 backend 化指标 | 统一到 DAG dispatch 指标族，保留必要的 L3 细项 |

## 7. Dashboard 与告警

### 7.1 推荐 dashboard

1. Cluster Overview
2. Pipeline SLA
3. Queue & Backpressure
4. DAG Execution
5. Downloader & Proxy
6. Control Plane & Config

### 7.2 推荐告警

1. 节点不可达或 `/metrics` 抓取中断
2. queue backlog / consumer lag 持续升高
3. stage error rate / P99 latency 异常
4. oldest incomplete run age 持续升高
5. config version skew 不收敛
6. timeout / 429 / proxy acquire failure 激增

## 8. 验收要点

指标体系重构完成后，至少要满足：

1. `/metrics` 中能看到新的核心指标族。
2. `namespace` 与 `node_id` 标签语义正确。
3. 不出现 request_id / task_id / account / 原始异常文本等高基数标签。
4. 运维可以仅靠 dashboard 回答“系统是否堵塞、堵在哪层、是否配置漂移”。
