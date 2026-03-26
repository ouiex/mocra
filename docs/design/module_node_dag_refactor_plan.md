# DAG 两阶段分布式改造方案

## 1. 设计结论

本方案采用明确的两阶段演进路线，避免一次性切换导致高风险：

1. 阶段1：Module 级别分布式。
2. 阶段2：ModuleNode 级别分布式（分布式 DAG）。

阶段1先把调度粒度收敛到 Module，保证稳定性和可回滚；阶段2再引入节点级分布式能力，并参考 Spark 在聚合和重分发方面的成熟机制。

---

## 2. 背景与目标

当前系统已经具备模块执行链、错误阈值、任务队列与上下文追踪能力。问题在于，若直接进入 ModuleNode 级分布式 DAG，工程复杂度会在短期内显著上升：

1. 节点级路由与汇聚一致性复杂。
2. 跨节点状态传递与幂等边界复杂。
3. 故障恢复路径更长，排障成本更高。

因此本方案目标是先完成可控升级，再进入增强版分布式 DAG：

1. 阶段1稳定落地 Module 级分布式。
2. 阶段2在阶段1基础上平滑升级到 ModuleNode 级分布式。

---

## 3. 范围与非目标

### 3.1 范围

1. 定义两阶段架构边界与迁移路径。
2. 给出阶段1的生产落地方案。
3. 给出阶段2的分布式 DAG 设计框架。
4. 在阶段2中引入 Spark 参考机制。

### 3.2 非目标

1. 不在当前迭代一次性完成阶段2全部能力。
2. 不替换现有队列中间件。
3. 不改数据库 schema。

---

## 4. 总体架构

### 4.1 统一分层

1. Task Ingress Layer
   - 负责输入归一化、任务装载、分发与重试入口。
2. Execution Layer
   - 阶段1：Module Runtime（节点内 DAG）。
   - 阶段2：ModuleNode Runtime（跨节点 DAG）。
3. Observability and Control Layer
   - 指标、日志、告警、开关、灰度、回滚。

### 4.2 核心原则

1. 先稳定后增强。
2. 兼容优先。
3. 所有新增能力都必须有开关化回滚路径。

---

## 5. 阶段1：Module 级别分布式（DAG 仅在一个节点内运行）

### 5.1 设计定义

1. 分布式调度单元是 Module。
2. 一个 Module 在一个执行节点内完成其内部 DAG 流转。
3. ModuleNode 不跨节点迁移。

### 5.2 运行语义

1. Task Ingress 选择待执行 Module。
2. Module 被分发到某执行节点。
3. 节点内执行 Module DAG（或线性 step 兼容逻辑）。
4. 产出数据、后续 parser task、或 error task。
5. 错误链按现有阈值决策继续重试或终止。

### 5.3 优势

1. 工程改动小，可直接复用现有链路。
2. 上下文与状态闭环在单节点内，故障恢复更简单。
3. 排障和观测粒度清晰。

### 5.4 阶段1需要改造的点

1. 明确并固化 Module 粒度调度语义。
2. 将跨节点假设从 ModuleNode 层移除。
3. 保留 step 与 node 兼容字段，但主路由先不跨节点。

---

## 6. 阶段2：ModuleNode 级别分布式（分布式 DAG）

### 6.1 设计定义

1. 分布式调度单元升级为 ModuleNode。
2. DAG 的节点可在不同执行节点运行。
3. 系统需要支持跨节点分叉、汇聚、重试、补偿。

### 6.2 必备能力

1. 节点级路由（node_id 作为主路由键）。
2. 分叉与汇聚（fan-out 和 fan-in）一致性控制。
3. 节点级幂等与去重。
4. 节点级失败恢复与回退门闸。
5. 跨节点上下文传播与版本控制。

### 6.3 上下文字段建议

1. run_id
2. module_id
3. node_id
4. branch_token
5. prefix_request
6. attempt
7. epoch（可选）

---

## 7. 阶段2对 Spark 的参考策略

本节用于指导 ModuleNode 级分布式 DAG 的调度和聚合，参考 Spark 的机制，不照搬其运行时实现。

### 7.1 参考点一：两阶段聚合

1. 局部聚合（map-side combine）
   - 先在节点本地做 partial 聚合，减少网络传输。
2. 全局聚合（reduce-side aggregate）
   - 按 key 重分区后，在目标节点做 final 聚合。

### 7.2 参考点二：按 key 重分区

1. 采用确定性分区函数。
2. 同 key 必须稳定落在同分区。
3. 支持分区扩缩容与迁移策略。

建议表达：partition = hash(key) mod N。

### 7.3 参考点三：处理数据倾斜

1. 热 key salting 拆分。
2. 分区内先聚合，再二级聚合归并。
3. 对热点分区启用限流与优先级。

### 7.4 参考点四：失败重分发

1. 失败任务按同分区规则重派发。
2. attempt 增量并附带幂等键。
3. 输出端必须可幂等写入。

### 7.5 参考点五：慢任务推测执行（可选）

1. 对慢分区触发副本任务。
2. 谁先成功谁生效。
3. 仅在结果可幂等合并时启用。

---

## 8. 两阶段迁移路线

### 8.1 里程碑 A：完成阶段1

1. 固化 Module 级调度。
2. 完成节点内 DAG 运行稳定化。
3. 完成阶段1指标与告警。

验收标准：

1. 线上稳定运行，错误率不劣化。
2. 具备灰度开关与回滚能力。

### 8.2 里程碑 B：阶段2基础能力

1. node_id 主路由上线（灰度）。
2. 节点级幂等与 dedup 生效。
3. fan-out 和 fan-in 基础能力上线。

验收标准：

1. 分布式 DAG 可跑通典型流程。
2. 无重复推进与明显丢任务。

### 8.3 里程碑 C：阶段2增强能力

1. 引入 Spark 参考策略下的聚合重分发。
2. 引入 skew 治理。
3. 引入分区失败重派发与可选推测执行。

验收标准：

1. 聚合吞吐显著提升。
2. 延迟与错误率维持在目标阈值内。

---

## 9. 数据契约与键空间建议

### 9.1 阶段1关键键

1. module:run:{run_id}:{module_id}
2. module:error:{run_id}:{module_id}
3. module:retry:{run_id}:{module_id}:{attempt}

### 9.2 阶段2关键键

1. dag:node:state:{run}:{module}:{node}:{branch}
2. dag:edge:gate:{run}:{module}:{from}:{to}:{branch}
3. dag:barrier:arrive:{run}:{module}:{node}:{branch}:{from_node}
4. dag:barrier:release:{run}:{module}:{node}:{branch}
5. dag:dedup:{run}:{module}:{node}:{branch}:{event_id}

### 9.3 聚合键（Spark 参考）

1. agg:partial:{run}:{module}:{partition}:{window}:{key}
2. agg:final:{run}:{module}:{partition}:{window}:{key}
3. agg:dedup:{run}:{module}:{partition}:{window}:{event_id}
4. agg:watermark:{run}:{module}:{window}

---

## 10. 测试策略

### 10.1 阶段1测试

1. Module 调度与重试回归。
2. 节点内 DAG 正确性。
3. 阈值终止与回滚验证。

### 10.2 阶段2测试

1. 跨节点路由正确性。
2. fan-out 和 fan-in 一致性。
3. 节点级 dedup 与幂等。
4. 分区失败重分发。
5. skew 场景压测。

### 10.3 指标门槛

1. 丢任务率接近 0。
2. 重复处理率低于阈值。
3. p95 和 p99 延迟受控。

---

## 11. 可观测性与告警

### 11.1 阶段1指标

1. module_dispatch_total
2. module_dispatch_failed_total
3. module_retry_total
4. module_terminated_total

### 11.2 阶段2指标

1. dag_node_dispatch_total
2. dag_node_dispatch_failed_total
3. dag_barrier_wait_total
4. dag_barrier_release_total
5. dag_dedup_hit_total
6. dag_redispatch_total
7. dag_skew_detected_total

### 11.3 告警建议

1. barrier 长时间不释放。
2. dedup 命中率异常升高。
3. 重分发持续失败。
4. 分区积压持续增长。

---

## 12. 发布与回滚策略

### 12.1 开关设计

1. dag_phase1_enabled
2. dag_phase2_enabled
3. dag_phase2_modules（模块白名单）
4. dag_aggregation_redispatch_enabled

### 12.2 发布顺序

1. 先全量阶段1。
2. 阶段2按模块白名单灰度。
3. 聚合重分发在阶段2内再单独灰度。

### 12.3 回滚顺序

1. 先关闭聚合重分发。
2. 再关闭阶段2回退到阶段1。
3. 保留状态数据用于审计与复盘。

---

## 13. 风险与缓解

1. 阶段并行导致语义分裂
   - 缓解：严格定义阶段边界与开关策略。
2. 跨节点 DAG 导致重复执行
   - 缓解：node 级 dedup 和幂等写入。
3. 聚合分区热点
   - 缓解：salting、限流、动态分区。
4. 回滚复杂
   - 缓解：分层回滚，禁止跨层同时切换。

---

## 14. 任务拆解

1. P1-M1：阶段1调度语义收敛。
2. P1-M2：阶段1观测与回滚开关。
3. P2-M1：node_id 路由与节点级上下文。
4. P2-M2：fan-out/fan-in 与 barrier。
5. P2-M3：dedup 与幂等框架。
6. P2-M4：Spark 参考聚合重分发实现。
7. P2-M5：skew 治理与压测。
8. P2-M6：灰度、演练与收口。

---

## 15. 成功标准

1. 阶段1稳定上线并成为默认运行模式。
2. 阶段2可在白名单模块稳定运行。
3. Spark 参考的聚合重分发在目标场景带来可量化收益。
4. 两阶段都具备可观测、可灰度、可回滚能力。
