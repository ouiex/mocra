# mocra 重构开发文档索引

## 1. 文档目的

`docs\decentralized-crawler-architecture-v2.md` 是**架构蓝图**，用于描述项目的目标架构、约束和演进方向。  
`development_docs\` 是**开发执行文档集**，用于把“目标架构”拆成可以直接落地的模块、任务、切换步骤和验收标准。

本目录的定位是：

- 面向**开发执行**，不是只做架构说明；
- 同时覆盖**当前实现**与**目标实现**；
- 明确指出哪些内容是**当前代码现状**，哪些是**目标态要求**；
- 将重构拆到**模块与单任务级别**，便于实施和验收。

## 2. 当前重构基线

基于当前代码和主架构文档，当前项目有以下重构前提：

| 领域 | 当前基线 | 重构目标 |
|------|----------|----------|
| 控制面 | `State` 仍大量依赖 Redis 池、分布式锁、滑动窗口限流、软 leader | 收敛到 `Raft + RocksDB` |
| 配置模型 | `ModuleConfig` 为 Value-heavy 多层 merge | `TaskProfileSnapshot + ResolvedNodeConfig` |
| 元数据 | `MetaData` 与 `Map<String, Value>` 在多阶段传播 | `RoutingMeta + ExecutionMeta + NodeInput + TypedEnvelope` |
| DAG | 主执行链依赖 `ModuleDagProcessor`，`schedule/dag` 为更强的目标态抽象 | 引入 run guard、resume state、fencing、placement |
| 队列 | `QueueManager` 已具备 MsgPack/JSON、压缩、补偿、blob 卸载 | 收敛为目标 envelope 与结构化 DLQ/blob contract |
| API | 当前只有 `/health`、`/metrics`、`/start_work`、`/nodes`、`/dlq`、`/control/*` | 收敛到 `/config/*`、`/tasks/*`、`/cluster/*`、`/debug/*`、`/dlq/messages*` |
| 指标与可观测 | Prometheus 已接入，但统一封装与散点直打点混用，标签/单位/覆盖不完整 | 收敛到三层指标体系，显式区分 `namespace + node_id`，补齐 queue / DAG / API / config / coordination 观测 |

## 3. 文档清单

| 文件 | 作用 | 适用阶段 |
|------|------|----------|
| `01_current_logic.md` | 当前代码逻辑、现有链路、主要问题 | 开工前、排查阶段 |
| `02_target_logic.md` | 修改后目标逻辑、最终结构、关键设计约束 | 设计、评审阶段 |
| `03_interface_change_list.md` | 接口替换清单、切换策略、受影响文件 | 开发、联调阶段 |
| `04_task_breakdown.md` | 拆分到单任务粒度的开发清单 | 排期、执行阶段 |
| `05_migration_plan.md` | 目标态重构推进方案、阶段门槛、cutover 约束 | 实施、切换阶段 |
| `06_acceptance_and_tests.md` | 验收标准、测试矩阵、命令入口 | 联调、上线前阶段 |
| `07_metrics_system.md` | 指标体系设计、标签规范、看板告警、切换口径 | 指标重构、运维对齐阶段 |
| `development_path.md` | 模块级重构顺序与依赖路径 | 项目推进全过程 |

## 4. 使用方式

推荐使用顺序：

1. 先读 `01_current_logic.md`，确认当前代码的真实基线。
2. 再读 `02_target_logic.md`，确认本轮重构要收敛到什么形态。
3. 若本轮涉及指标重构，同时读 `07_metrics_system.md`，统一命名、标签、看板与告警口径。
4. 开始编码前对照 `03_interface_change_list.md` 和 `04_task_breakdown.md`。
5. 实施过程中按 `development_path.md` 控制模块顺序。
6. 每一阶段结束时按 `06_acceptance_and_tests.md` 做验收。

## 5. 执行原则

1. 本轮按**目标态直切**推进，不以保留历史 API 或历史消息格式为约束。
2. 控制面、调度、transport、processor 按阶段重组，但每阶段输出都直接服务最终架构。
3. 切换采用 **drain -> 停机/重启 -> 启动新 runtime**，不允许 mixed-version worker 混跑。
4. 运行中的 DAG run 不允许跨切换窗口续跑到另一套 profile/schema。
5. 以根 crate 的构建、单测、契约测试为主要验收入口；`tests\Cargo.toml` 下的旧 harness 不作为默认绿路径。
