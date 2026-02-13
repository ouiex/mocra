# Mocra 向 ETL 平台演进路线（2026）

## 1. 目标与判断

### 1.1 当前基础（已具备）
- 事件驱动主链路：Task -> Request -> Response -> Data。
- 可插拔中间件：DownloadMiddleware、DataMiddleware、DataStoreMiddleware。
- 分布式基础：Redis/Kafka、调度（Cron）、选主、重试与 DLQ、可观测性。
- 数据类型基础：结构化数据（DataFrame）与文件数据（File）。

### 1.2 目标形态（ETL 平台）
- **Extract**：支持多源连接器（HTTP、DB、对象存储、消息流）。
- **Transform**：标准化变换算子、质量规则、Schema 管理。
- **Load**：多目标 Sink（Postgres、湖仓、对象存储、Kafka 回写）。
- **治理**：血缘、质量、审计、回放、版本化发布。

---

## 2. 演进原则

1. **兼容优先**：不破坏现有爬虫与任务契约（Task/Request/Response/Data）。
2. **插件优先**：新能力尽量通过连接器和中间件扩展，而非修改核心链路。
3. **批流统一**：同一套模型支持 Batch 与 Near-Real-Time。
4. **可观测先行**：所有新增链路必须带 Metrics、日志、错误码与回放能力。
5. **小步快跑**：按 30/60/90 天里程碑推进，阶段可回滚。

---

## 3. 30/60/90 天路线图

## 3.1 第一阶段（T+30 天）：ETL MVP 内核

### 阶段目标
在不破坏现有采集能力的前提下，把 Mocra 变成“可声明式配置的 ETL 管线引擎”。

### 关键交付
1. **ETL Pipeline 配置模型（MVP）**
   - 在配置中引入 Pipeline 概念：source、transform、sink、schedule、retry_policy。
   - 兼容现有 module 配置；旧配置默认映射到新模型。

2. **Source 抽象（先做 2 个）**
   - HttpSource（复用现有 Downloader）
   - DbSource（Postgres 增量拉取，按 watermark）

3. **Transform 抽象（先做 3 个）**
   - 字段映射（rename/select）
   - 过滤（where）
   - 轻量聚合（group/agg，优先基于 Polars lazy）

4. **Sink 抽象（先做 2 个）**
   - PostgresSink（幂等 upsert）
   - ObjectStorageSink（按分区路径落盘）

5. **统一失败语义**
   - 明确重试边界：source/transform/sink 各自可配置重试次数与回退策略。
   - DLQ 记录标准化字段（pipeline_id、stage、error_code、payload_ref）。

### 验收标准
- 至少 2 条可运行 ETL 作业（API 数据拉取入库、DB 增量同步入对象存储）。
- 每条作业具备：成功率、延迟、吞吐、重试、DLQ 指标。
- 现有 crawler 作业零回归。

---

## 3.2 第二阶段（T+60 天）：生产化与治理能力

### 阶段目标
把 MVP 提升为可稳定上线的生产 ETL 子系统。

### 关键交付
1. **Schema 与版本管理**
   - 引入 Schema Registry（轻量版即可，先落库）。
   - 增加兼容性策略：backward/forward/full（先支持 backward）。

2. **数据质量规则（DQ）**
   - 非空、唯一、范围、枚举校验。
   - 质量失败策略：阻断/告警/旁路落错表。

3. **作业编排增强**
   - 支持依赖 DAG（A 完成触发 B）。
   - 支持重跑窗口（按时间区间重放）。

4. **观测与运维增强**
   - 按 pipeline/stage 输出指标。
   - 增加运行看板和告警模板（SLA 延迟、错误率、堆积）。

5. **性能优化（有量化目标）**
   - 增加批写与向量化转换路径。
   - 控制内存峰值（大包体 offload 与分段处理）。

### 验收标准
- 关键作业 7x24 运行稳定，SLA 达标（成功率 > 99%，按业务定义延迟阈值）。
- 能定位并回放失败批次，平均故障恢复时间显著下降。

---

## 3.3 第三阶段（T+90 天）：平台化与生态扩展

### 阶段目标
形成“可复用、可交付”的 ETL 平台能力，而非单项目能力。

### 关键交付
1. **连接器生态扩展**
   - 新增 KafkaSource/KafkaSink。
   - 新增对象存储读取 Source（CSV/JSON/Parquet）。

2. **平台接口**
   - Pipeline 管理 API：创建、发布、暂停、回滚、重跑。
   - 运行审计 API：按 run_id/pipeline_id 查询全链路状态。

3. **多租户与隔离**
   - namespace + 资源配额（并发、队列、存储）。
   - 秘钥与凭据隔离。

4. **发布与灰度机制**
   - Pipeline 版本化发布（v1/v2 并行、灰度流量切换）。
   - 自动回滚触发条件（错误率/延迟阈值）。

### 验收标准
- 新业务可在 1 天内接入一条 ETL 管线。
- 同时稳定运行多条异构 pipeline（批 + 准实时）。
- 平台具备标准化上线流程和回滚机制。

---

## 4. 建议的模块改造顺序（最小风险）

1. 在 common 增加 ETL Pipeline 配置结构与校验。
2. 在 engine 增加 stage 抽象（source/transform/sink）并复用现有链路。
3. 在 queue 保持消息契约不变，新增 ETL 事件类型与 header。
4. 在 downloader 与 storage 侧逐步沉淀可复用 connector。
5. 在 docs 与 tests 同步补齐契约测试与回放测试。

---

## 5. 风险与应对

1. **风险：旧爬虫与新 ETL 模型冲突**
   - 应对：双轨配置；保留 legacy 模式，分阶段迁移。

2. **风险：Sink 幂等与一致性不足**
   - 应对：统一幂等键（run_id + partition + record_key），默认 upsert。

3. **风险：高吞吐下内存抖动**
   - 应对：批大小自适应、背压、分段序列化、offload。

4. **风险：可观测性不足导致排障慢**
   - 应对：统一事件字段与错误码，强制 run_id 全链路透传。

---

## 6. 里程碑清单（可直接跟踪）

- M1（第 2 周）：ETL 配置模型完成，兼容旧配置。
- M2（第 4 周）：2 Source + 3 Transform + 2 Sink 可跑通。
- M3（第 6 周）：Schema/DQ/告警接入。
- M4（第 8 周）：DAG 依赖 + 回放能力上线。
- M5（第 12 周）：连接器扩展、版本发布、灰度回滚闭环。

---

## 7. 结论

Mocra 已具备 ETL 平台所需的核心引擎要素（分布式、可插拔、可调度、可观测）。
后续重点不在“重写引擎”，而在“补齐连接器、治理与平台接口”。
按照本路线推进，90 天内可以从“采集框架”升级为“可生产交付的 ETL 平台雏形”。
