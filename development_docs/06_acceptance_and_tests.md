# 验收标准与测试

## 1. 总体原则

重构验收分为五层：

1. **编译层**：代码能通过构建与静态检查。
2. **目标能力层**：目标模型、目标接口、目标调度能力真实可用。
3. **切换就绪层**：cutover、drain、启动顺序和脚本切换可执行。
4. **分布式一致性层**：恢复、重试、热更新、fencing、run guard 正确。
5. **可观测层**：指标、健康、控制面信号可以支持上线与排障。

本轮不以“新旧路径共存”作为主验收目标。

## 2. 默认命令入口

> 以下命令来自当前项目实际可用命令，应作为主验收入口。

### 2.1 必跑命令

```powershell
cargo build
cargo test --lib
cargo clippy --all-targets --all-features
pwsh -File scripts\ci_contract_tests.ps1
```

### 2.2 定向回归命令

```powershell
cargo test test_poison_message_triggers_nack --lib
```

### 2.3 说明

- `tests\Cargo.toml` 里的 `tests_debug` / e2e harness 当前不是默认绿路径，不能作为本轮主验收门槛。
- 若本地提供 Redis/Kafka 环境，可增加相关契约验证，但不应让文档依赖不存在的外部环境。

## 3. 验收标准

### 3.1 基础验收

| 编号 | 验收项 | 标准 |
|------|--------|------|
| B-01 | 编译通过 | `cargo build` 成功 |
| B-02 | 单测通过 | `cargo test --lib` 成功 |
| B-03 | Clippy 通过 | `cargo clippy --all-targets --all-features` 成功 |
| B-04 | 契约脚本通过 | `scripts\ci_contract_tests.ps1` 成功 |

### 3.2 模型验收

| 编号 | 验收项 | 标准 |
|------|--------|------|
| M-01 | `QueueEnvelope` 可编码/解码 | runtime queue schema 正确 |
| M-02 | `TaskProfileSnapshot` 可装配 | profile 写入后能生成稳定 snapshot |
| M-03 | `WorkflowDefinition` 可编译 | module/profile 可生成确定性 workflow |
| M-04 | `ResolvedNodeConfig` 可切片 | 按 node_key 获取正确 config |
| M-05 | `NodeGenerateContext / NodeParseContext` 完整 | 节点不再需要 `ModuleConfig` 和 `Map<String, Value>` |
| M-06 | `NodeParseOutput / NodeDispatchEnvelope` 完整 | parser 能直接表达 next/data/stop/error |

### 3.3 DAG 验收

| 编号 | 验收项 | 标准 |
|------|--------|------|
| D-01 | entry node 选择正确 | 显式 entry 与自动推导都正确 |
| D-02 | successor 路由正确 | 单后继与多后继 fan-out 正确 |
| D-03 | stay_current_step 正确 | 同节点重试不误推进 |
| D-04 | 空输出推进正确 | advance gate 只赢一次 |
| D-05 | stop signal 正确 | run 停止后晚到消息不会重启 DAG |
| D-06 | run resume 正确 | 已成功节点不会重复执行 |
| D-07 | fencing 正确 | 旧执行结果不能覆盖新执行结果 |

### 3.4 队列与 transport 验收

| 编号 | 验收项 | 标准 |
|------|--------|------|
| Q-01 | task/parser/error topic 可消费 | 新 envelope 消息成功收发 |
| Q-02 | blob offload 可回放 | 被卸载的大 payload 可 reload、消费与 requeue |
| Q-03 | DLQ 结构化读写可用 | `/dlq/messages*` 可列出、回放、删除 |
| Q-04 | poison message 处理正确 | 反序列化失败不会伪装成成功消费 |
| Q-05 | codec 切换边界清晰 | JSON/MsgPack 不在同一批 worker 中混跑 |
| Q-06 | cutover 顺序可执行 | drain、停机、启动新 consumer 的顺序已演练通过 |

### 3.5 API 验收

| 编号 | 验收项 | 标准 |
|------|--------|------|
| A-01 | `POST /tasks/dispatch` 可入队任务 | 统一任务入口可用 |
| A-02 | `/config/*` 可读写 profile | 生成 snapshot/version |
| A-03 | `/cluster/*` 可观测 | nodes/control/health 信息可读 |
| A-04 | `/debug/*` 与 `/dlq/messages*` 可用 | explain、死信巡检、requeue/delete 可用 |
| A-05 | `/health` 契约明确 | HTTP 状态码、body 与组件字段语义一致 |
| A-06 | pause/resume 传播可观测 | 控制面可确认集群进入 pause/resume |
| A-07 | 历史 route 已下线 | `/start_work`、`/nodes` 等旧路由不再对外暴露 |

### 3.6 指标与可观测验收

| 编号 | 验收项 | 标准 |
|------|--------|------|
| O-01 | `/metrics` 暴露目标态核心指标族 | 至少能看到 `mocra_stage_*`、queue、DAG、API/config 核心指标 |
| O-02 | `namespace` 与 `node_id` 语义正确 | 多节点抓取后不会把 namespace 聚合成 node |
| O-03 | 主链路阶段指标齐全 | task/request/download/response/parse/error 都有吞吐、延迟、错误、inflight |
| O-04 | Queue / DAG / control plane 可观测 | 能回答“堵在哪层、为什么堵、是否有版本漂移” |
| O-05 | 标签基数受控 | 指标中不出现 request_id、task_id、account、原始异常文本 |
| O-06 | `/metrics` 抓取语义稳定 | scrape 不会被匿名流量或错误限流策略误伤 |
| O-07 | dashboard/alert 已切到新查询 | 运维面不再依赖旧指标名 |

## 4. 建议测试矩阵

### 4.1 单元测试

优先新增：

1. `QueueEnvelope` encode/decode
2. `TaskProfileSnapshot` 装配
3. `WorkflowDefinition` compile
4. `ResolvedNodeConfig` 切片
5. `NodeGenerateContext / NodeParseContext` 构造
6. `NodeParseOutput / NodeDispatchEnvelope` 适配
7. `DagRunStateStore` save/load/clear
8. `DagRunGuard` acquire/renew/release
9. `DagFencingStore` token 提交规则

### 4.2 DAG 语义测试

建议覆盖：

1. entry node 自动推导
2. 非法 node id 直接拒绝而不是隐式 fallback
3. multi-branch fan-out
4. empty output -> advance gate
5. parser failure -> same-node retry
6. stop signal propagation
7. resume after partial success
8. placement local/remote 决策正确

### 4.3 Cutover 测试

建议覆盖：

1. 旧 worker 已完全停止后才启动新 consumer
2. queue topic 在切换前已 drain 或重建
3. 历史 route 已从 router 中删除
4. 调用方脚本与 SDK 已切到新路由
5. DLQ / blob 的离线迁移或清理流程已执行

### 4.4 分布式一致性测试

建议覆盖：

1. 同一 run 双 worker 竞争 guard
2. 旧 token 提交失败
3. 新 profile version 只影响新 run
4. 同一 successor advance gate 只成功一次
5. local fast path 与 remote dispatch 路径行为一致
6. pause/resume 在多节点下按文档时序传播

### 4.5 指标回归测试

建议覆盖：

1. `MetricsScope` 生成的 `namespace / node_id / deployment_mode` 标签正确
2. `mocra_stage_*` 在各 stage helper 上输出完整标签
3. `/metrics` 渲染文本中包含新的核心指标族
4. 新标签白名单不允许 request_id / task_id / account / 原始异常文本进入输出
5. queue / DAG / API / config 指标都能被 dashboard 查询命中
6. `/health`、`HealthMonitor` 与 Prometheus 指标语义不混淆

## 5. 上线前检查单

1. 所有必跑命令通过。
2. 新增单测和关键集成测试通过。
3. 至少完成一次“drain 旧 topic -> 启动新 consumer”演练。
4. 至少完成一次“run 恢复 + fencing + fan-out”组合场景验证。
5. 至少完成一次“profile 热更新但不影响进行中 run”演练。
6. 至少完成一次 `/metrics` 抓取和 dashboard 样本检查。
7. 至少完成一次 `/health`、`/cluster/*`、`/dlq/messages*` 契约核对。
8. 至少完成一次调用方脚本与 SDK 的新路由联调。
9. 文档与实现同步更新。

## 6. 当前不作为主验收门槛的内容

以下内容可以作为补充验证，但不应阻塞主线重构推进：

- `tests\Cargo.toml` 下的旧 harness 全量恢复
- 所有 Redis 历史测试全部转绿
- 所有联邦部署能力一次性全开

这些内容应在主链路稳定后再逐项收口。
