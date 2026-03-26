# ModuleTrait 返回 DAG 改造落地开发文档

## 0. 实时进度

更新时间：2026-03-26

已完成：

1. `engine/task` DAG 核心组件已落地
   - `module_dag_compiler.rs`
   - `module_dag_orchestrator.rs`
   - `module_node_dag_adapter.rs`
2. `TaskManager` 已提供线性兼容 DAG 编译与执行入口
   - `compile_module_dag_linear_compat`
   - `execute_module_dag_linear_compat`
3. `Module` 已接入 DAG 预检能力
   - `prepare_execution_graph`
   - `run_dag_preflight_dry_run`
4. `task_model_chain` 与 `parser_chain` 已接入 dry-run 预检埋点
   - 指标：`module_dag_preflight_total`
5. `TaskProcessor` 已接入 shadow 执行分支（不影响原请求输出）
   - 指标：`module_dag_shadow_execute_total`
6. 白名单门控已接入 `Module`
   - `module_dag_whitelist`
7. 单测已补齐并命中通过（编译器、orchestrator、module dry-run、whitelist）
8. `TaskProcessor` 已接入 shadow compare 一致性埋点
   - 指标：`module_dag_shadow_compare_total{result=match|mismatch|error,module=...}`
9. `parser_chain` 本地生成分支已接入 shadow execute + compare
   - 与 `TaskProcessor` 使用同一组 shadow 指标，观测口径统一
10. `TaskProcessor` 与 `parser_chain` 已接入 primary execute 预览分支
   - 开关：`module_dag_execute_primary`
   - 说明：当前为预览执行，仍保留 legacy generate 输出
   - 指标：`module_dag_primary_execute_total`
11. DAG compare 逻辑已统一下沉到 `TaskManager`
   - 新增：`execute_module_dag_linear_compat_with_compare`
   - `task_model_chain` / `parser_chain` 复用同一实现，减少重复逻辑
12. DAG 运行开关已收敛为统一运行时标志
   - 新增：`ModuleDagRuntimeFlags` + `dag_runtime_flags()`
   - `task_model_chain` / `parser_chain` 改为“读取一次标志，多分支复用”
13. cutover 骨架已接入并启用安全挡板
   - 开关：`module_dag_execute_primary_cutover`
   - 指标：`module_dag_cutover_total`
   - 当前行为：在适配器未完整业务化前，cutover 请求会被阻断并回退 legacy
14. cutover 已接入“连续失败自动降级”治理
   - `TaskManager` 新增模块级失败连续计数与阈值判断
   - `task_model_chain` / `parser_chain` 会在达到阈值后自动阻断 cutover 并回退 preview/legacy
   - 指标：`module_dag_cutover_total{result=auto_downgraded|execution_error|preview_ok|preview_error,...}`
15. remote dispatcher 已接入轮询退避与临时错误容忍
   - 文件：`src/schedule/dag/remote_redis.rs`
   - 行为：远端结果轮询采用渐进退避（上限 1s），Redis 轮询瞬时错误不立即失败，改为退避重试直到超时
   - 指标：新增 `dag_remote_dispatch_total{result=poll_error,...}` 观测瞬时链路抖动
16. Redis fencing store 已强化“非法 latest_token 格式严格拒写”
   - 文件：`src/schedule/dag/fencing.rs`
   - 行为：当 Redis 中 latest_token 非数字时，直接拒绝提交并返回 `FencingTokenRejected`
   - 指标：新增 `dag_fencing_commit_total{result=accepted|rejected_stale|rejected_invalid_latest,resource=...}`
17. run state checkpoint 已补齐失败快照持久化
   - 文件：`src/schedule/dag/scheduler.rs`
   - 行为：在 `renew_lost`、`run_timeout`、`node_failed` 返回前持久化已完成节点快照
   - 指标：新增 `dag_run_state_store_total{stage=renew_lost|run_timeout|node_failed,result=saved|save_error}`
18. run state resume 已补齐失败恢复回归用例
   - 文件：`src/schedule/dag/tests.rs`
   - 用例：`execute_parallel_failure_snapshot_allows_resume_without_rerunning_success_nodes`
   - 断言：失败后第二次执行从快照恢复，已成功节点不重复执行
19. remote worker 已补齐“取消快速生效”治理
   - 文件：`src/schedule/dag/remote_redis.rs`
   - 行为：worker 在 handler 执行前先检查 cancel_key；已取消请求直接跳过，不再消耗业务执行资源
   - 指标：新增 `dag_remote_worker_cancel_total{stage=before_execute|after_execute,...}`
20. 已新增取消前置跳过回归用例
   - 文件：`src/schedule/dag/tests.rs`
   - 用例：`execute_parallel_real_redis_remote_worker_skips_canceled_request_before_execute`
   - 断言：dispatcher 超时取消后，请求即使被 worker 消费也不会触发 handler 执行
21. remote worker 已修正 cancel 与 delivery_exhausted 判定顺序
   - 文件：`src/schedule/dag/remote_redis.rs`
   - 行为：已取消请求优先被跳过，不再进入 delivery_exhausted 分支
   - 目标：避免 canceled 请求误入 DLQ，降低噪音告警与无效重试
22. 已新增 cancel 与 delivery_exhausted 交叉回归用例
   - 文件：`src/schedule/dag/tests.rs`
   - 用例：`execute_parallel_real_redis_remote_worker_canceled_request_not_dlq_even_if_delivery_counter_high`
   - 断言：高 delivery counter 下 canceled 请求不会进入 DLQ 且不执行 handler
23. DAG 调度已补齐告警友好聚合指标
   - 文件：`src/schedule/dag/scheduler.rs`
   - 新增：`dag_execute_error_total{run_guard,error_code,error_class}`
   - 新增：`dag_alert_event_total{source=dag_scheduler|dag_run_guard,severity,event,error_class?}`
   - 目标：灰度阶段可直接按错误类别与严重级别配置阈值告警
24. remote dispatcher 已强化 request_map TTL 策略
   - 文件：`src/schedule/dag/remote_redis.rs`
   - 行为：request_map TTL 对齐 response timeout（含安全余量），不再仅依赖 result_ttl
   - 目标：长执行场景下保持同 attempt 去重稳定，避免 request_map 提前过期导致重复派发
25. 已新增“长执行 + 短 result_ttl”去重回归用例
   - 文件：`src/schedule/dag/tests.rs`
   - 用例：`execute_parallel_real_redis_remote_dispatch_dedupes_long_running_with_short_result_ttl`
   - 断言：第二次同 attempt 调度不会触发重复 handler 执行
26. remote dispatcher/worker 已补齐统一告警事件打点
   - 文件：`src/schedule/dag/remote_redis.rs`
   - 新增：`dag_alert_event_total{source=dag_remote_dispatcher|dag_remote_worker,severity,event,...}`
   - 覆盖：dispatch timeout、remote execute failed、delivery_exhausted、worker handler_failed
   - 目标：与 scheduler 侧告警口径一致，支持跨组件统一阈值与告警路由
27. DAG 告警阈值模板已落地
   - 文件：`docs/alerts/logging_alerts.yml`
   - 规则组：`dag_runtime_alerts`
   - 覆盖：critical/warning 事件聚合、可重试/不可重试错误率、run-state snapshot save_error
   - 目标：支撑 M4 灰度期间统一告警阈值与快速分诊
28. DAG 告警实盘校准 Runbook 已落地
   - 文件：`docs/alerts/dag_alerts_calibration_runbook.md`
   - 覆盖：dev/staging/prod 阈值建议、source/event 分诊映射、PromQL 快查、灰度放量 Gate
   - 目标：将 `dag_runtime_alerts` 从“模板”推进到“可执行值班手册”
29. cutover 失败治理已补齐冷却恢复窗口
   - 文件：`src/engine/task/task_manager.rs`、`src/engine/chain/task_model_chain.rs`、`src/engine/chain/parser_chain.rs`
   - 新增配置：`module_dag_cutover_recovery_window_secs`（默认 300s）
   - 行为：模块 cutover 连续失败达阈值后会临时降级；冷却窗口到期后自动允许一次探测恢复，避免永久阻断
30. DAG 基线采集脚本已落地
   - 文件：`scripts/dag_alerts_baseline.py`、`scripts/dag_alerts_baseline.ps1`、`scripts/dag_alerts_baseline.sh`
   - 能力：按环境 profile 从 Prometheus 拉取 `dag_alert_event_total`/`dag_execute_error_total`/`dag_run_state_store_total` 并生成 Markdown 基线报告
   - 默认输出：`docs/dashboards/dag_alerts_baseline_latest.md`
31. 告警校准 Runbook 已接入脚本化流程
   - 文件：`docs/alerts/dag_alerts_calibration_runbook.md`
   - 覆盖：PowerShell/Bash/Python 三种执行方式 + 报告归档要求
32. cutover 已接入“显式模块支持白名单”安全门控
   - 文件：`src/engine/task/module.rs`、`src/engine/chain/task_model_chain.rs`、`src/engine/chain/parser_chain.rs`
   - 新增配置：`module_dag_cutover_whitelist`
   - 行为：`module_dag_execute_primary_cutover=true` 仅表示请求切流；只有模块命中 cutover 白名单才允许进入 cutover 决策，否则记 `blocked_not_supported` 并回退 preview/legacy
   - 目标：将真实切流从“全局开关”收敛为“显式验证模块”模式，满足生产安全前置条件
33. cutover 已接入“shadow compare 一致性门控”
   - 文件：`src/engine/chain/task_model_chain.rs`、`src/engine/chain/parser_chain.rs`
   - 新增配置：`module_dag_cutover_require_shadow_match`（默认 true）
   - 行为：当开启 shadow execute 且 compare 结果非 `match`（或 shadow 执行 error）时，cutover 被阻断并记 `module_dag_cutover_total{result=blocked_shadow_compare}`
   - 目标：在真实切流前强制语义一致性，降低行为回归风险
34. cutover 门控链路已完成三层组合保护
   - 层 1：`module_dag_cutover_whitelist`（模块显式支持）
   - 层 2：`module_dag_cutover_require_shadow_match`（影子一致性）
   - 层 3：失败阈值 + 冷却恢复窗口（稳定性）
   - 说明：真实 cutover 仅在三层门控都满足时放行
35. cutover 已接入“shadow 预热窗口”门控
   - 文件：`src/engine/task/task_manager.rs`、`src/engine/chain/task_model_chain.rs`、`src/engine/chain/parser_chain.rs`
   - 新增配置：`module_dag_cutover_require_warmup`（默认 true）、`module_dag_cutover_min_shadow_matches`（默认 3）、`module_dag_cutover_min_observation_window_secs`（默认 600）
   - 行为：仅当 shadow compare 连续累计达到最小 match 次数且观察窗口达标时，cutover 才可放行；否则记 `module_dag_cutover_total{result=blocked_warmup}`
   - 目标：防止“短时偶发一致”触发过早切流，提升生产稳定性
36. cutover 状态跟踪已切换为“运行时作用域隔离”
   - 文件：`src/engine/chain/task_model_chain.rs`、`src/engine/chain/parser_chain.rs`
   - 行为：cutover 失败连续计数与 warmup 累积不再按“模块名”共享，改为按运行时作用域键（`account-platform-module`）跟踪
   - 目标：避免多账号/多平台并发下的跨租户状态污染，提高真实切流决策准确性
37. cutover 状态管理已抽象为可回归验证的独立 tracker
   - 文件：`src/engine/task/task_manager.rs`
   - 行为：`TaskManager` 的 cutover failure/warmup 逻辑下沉到 `DagCutoverStateTracker`，并新增“多作用域隔离”单测
   - 目标：把生产关键门控逻辑从“实现存在”提升到“可持续回归验证”
38. cutover 时间门控已支持“精确时间窗口回归”
   - 文件：`src/engine/task/task_manager.rs`
   - 行为：`DagCutoverStateTracker` 引入可注入时钟能力，补齐 `recovery_window` 与 `warmup_observation_window` 的精确单测
   - 目标：防止时间门控逻辑在后续重构中退化，保障切流门控的可验证性
39. parser 链路 cutover scope 已完成一致性收敛
   - 文件：`src/engine/chain/parser_chain.rs`
   - 行为：shadow/non-shadow 两条 primary preview 分支统一复用同一 `cutover_scope`，避免作用域分散导致的维护漂移
   - 目标：确保 parser 链路在所有 cutover 路径上保持一致的作用域隔离语义
40. cutover scope 生成入口已统一到 `TaskManager`
   - 文件：`src/engine/task/task_manager.rs`、`src/engine/chain/task_model_chain.rs`、`src/engine/chain/parser_chain.rs`
   - 行为：新增 `TaskManager::module_cutover_scope`，task/parser 两条链路都改为统一调用，消除手写拼接差异
   - 目标：进一步降低后续改造时出现作用域键不一致的风险
41. DAG 基线采集脚本已补齐“归档 + gate 门禁”能力
   - 文件：`scripts/dag_alerts_baseline.py`、`scripts/dag_alerts_baseline.ps1`、`scripts/dag_alerts_baseline.sh`、`docs/alerts/dag_alerts_calibration_runbook.md`
   - 行为：脚本新增 `--archive-dir` / `--archive-prefix` 自动归档、`--summary-json` 结构化输出、`--fail-on-gate` 门禁退出码；PowerShell/Bash 包装脚本已透传新参数
   - 目标：将“基线采集”从一次性报表升级为可直接接入灰度发布 gate 的执行流程
42. 灰度发布窗口 gate 模板已补齐“可直接执行”入口
   - 文件：`scripts/dag_alerts_gate.ps1`、`scripts/dag_alerts_gate.sh`、`scripts/dag_alerts_baseline.py`、`docs/alerts/dag_alerts_calibration_runbook.md`
   - 行为：新增发布窗口包装脚本，自动拼接 release tag 归档前缀并强制 `fail-on-gate`；baseline Python 失败默认输出精简错误信息（可选 `--debug` 打印堆栈）
   - 目标：将 gate 执行从“参数拼接”收敛为“值班一键命令”，降低发布窗口误用风险
43. tests 子工程已补齐 DAG + ModuleTrait 全链路集成测试
   - 文件：`tests/src/dag_module_trait_e2e.rs`、`tests/Cargo.toml`
   - 覆盖：axum 本地 HTTP 服务、ModuleTrait 多 step 执行、线性兼容 DAG 多节点编译执行、session 开关行为、多 downloader 节点共享 session 的分布式场景
   - 目标：在 tests 目录形成“可直接运行”的 DAG + ModuleTrait 回归入口，支撑 M4 实盘前联调
44. tests 已补齐“无 Redis 配置 = 单节点模式”全链路验证
   - 文件：`tests/src/dag_module_trait_e2e.rs`
   - 覆盖：`config.mock.pure.engine.toml` 下 `is_single_node_mode=true` 判定、ModuleTrait 多 step 执行、线性兼容 DAG 多节点执行、session 同实例可复用/跨实例不共享
   - 目标：确保在纯单节点部署形态下 DAG + ModuleTrait 依然可运行且会话行为符合预期
45. tests 已新增“Engine 驱动”生产贴近型全链路回归
   - 文件：`tests/src/dag_module_trait_e2e.rs`
   - 覆盖：按 `main.rs` 方式启动 Engine、注册 ModuleTrait 模块、通过 Engine request 队列投递请求、由 Engine 下载/解析链路执行并通过 EventBus 验证执行完成
   - 目标：将 DAG + ModuleTrait 回归从“函数级验证”提升到“Engine 运行态验证”，贴近生产执行路径
46. tests 已新增“双 Engine 节点并发”运行态回归
   - 文件：`tests/src/dag_module_trait_e2e.rs`
   - 覆盖：并发启动两套 Engine（单节点模式配置）、分别注册同一 ModuleTrait、各自通过 Engine request 队列执行完整链路并用 EventBus 验证下载步骤完成
   - 目标：验证多节点并发运行下 DAG + ModuleTrait 链路稳定性，补齐“多节点运行态”测试证据
47. tests 已补齐“Task 队列驱动”Engine 全链路回归
   - 文件：`tests/src/dag_module_trait_e2e.rs`、`src/engine/task/repository.rs`
   - 覆盖：通过 Engine task 队列投递 `TaskEvent`，由 `TaskModelProcessor -> TaskFactory -> Download/Parser` 完整链路执行，并验证至少 3 次模块下载完成事件
   - 兼容修正：补齐 SQLite 任务仓储读取兼容路径，避免 task 模型加载阶段字符串类型不兼容导致的重试失败
   - 目标：满足“完整 task 队列驱动 + Engine 运行态”生产贴近型回归要求
48. 运行时抗崩溃加固已补齐第一批（避免进程级硬退出）
   - 文件：`src/engine/engine/runtime.rs`、`src/queue/manager.rs`、`src/common/model/request.rs`
   - 覆盖：API 启动失败不再 `process::exit`；Redis/Kafka 队列初始化失败降级到 in-memory；`Request::with_json/with_form` 序列化失败不再 `unwrap` 崩溃
   - 目标：降低基础设施瞬时异常引发的全进程退出风险，提升可恢复性
49. `State` 初始化失败路径已显式返回详细错误并由入口统一退出
   - 文件：`src/common/state.rs`
   - 覆盖：新增 `StateInitError`、`try_new/try_new_with_provider`；原 `new/new_with_provider` 改为记录详细失败原因后 `exit(1)`
   - 目标：将初始化失败从隐式 `expect` panic 升级为可定位、可观测、可治理的失败路径
50. M4 灰度收敛阶段已完成
   - 范围：白名单灰度治理、可观测与 gate 脚本、DAG + ModuleTrait 运行态全链路回归
   - 结果：进入“发布后稳态运营/增量优化”阶段，不再作为当前阶段阻塞项
51. tests 已补齐“primary cutover=true”Engine task 队列主路径回归
   - 文件：`tests/src/dag_module_trait_e2e.rs`
   - 覆盖：通过 task 队列投递任务，启用 `module_dag_execute_primary_cutover=true` + cutover whitelist，并验证 cutover 成功后会清理预置失败状态（证明主切换成功路径被执行）
   - 目标：补齐 M4 最后一块“真实 primary cutover 运行态验证”证据

进行中：

1. 发布后稳态运营
   - 持续监控核心指标与告警阈值，按版本节奏做增量优化
2. 后续阶段规划
   - 准备 add_step 废弃期与后续治理项拆解（不影响 M4 完成态）

待完成：

1. 无（M4 阶段收尾完成）

最近验证结果：

1. `cargo check`：通过
2. `cargo test compile_linear_compat_succeeds -- --nocapture`：通过
3. `cargo test dag_dry_run_enabled_with_steps_returns_ok -- --nocapture`：通过
4. `cargo test dag_dry_run_enabled_but_not_in_whitelist_returns_ok_without_running -- --nocapture`：通过
5. `cargo check`（shadow compare 接入后）：通过
6. `cargo check`（parser shadow 接入后）：通过
7. `cargo test compile_linear_compat_succeeds -- --nocapture`（parser shadow 接入后）：通过
8. `cargo check`（primary preview 接入后）：通过
9. `cargo test compile_linear_compat_succeeds -- --nocapture`（primary preview 接入后）：通过
10. `cargo check`（compare 下沉重构后）：通过
11. `cargo test compile_linear_compat_succeeds -- --nocapture`（compare 下沉重构后）：通过
12. `cargo check`（runtime flags 收敛后）：通过
13. `cargo test compile_linear_compat_succeeds -- --nocapture`（runtime flags 收敛后）：通过
14. `cargo check`（cutover 骨架接入后）：通过
15. `cargo test compile_linear_compat_succeeds -- --nocapture`（cutover 骨架接入后）：通过
16. `cargo check`（cutover 自动降级接入后）：通过
17. `cargo test module_dag -- --nocapture`（cutover 自动降级接入后）：通过（8 passed）
18. `cargo check`（remote dispatcher 轮询退避接入后）：通过
19. `cargo test schedule::dag::remote_redis::tests::next_poll_delay_ms_grows_and_caps -- --exact --nocapture`：通过
20. `cargo test schedule::dag -- --nocapture`：通过（50 passed）
21. `cargo check`（Redis fencing 严格拒写接入后）：通过
22. `cargo test schedule::dag::tests::execute_parallel_redis_fencing_store_rejects_stale_token -- --exact --nocapture`：通过
23. `cargo test schedule::dag::tests::execute_parallel_redis_fencing_store_rejects_invalid_latest_token_format -- --exact --nocapture`：通过
24. `cargo test schedule::dag -- --nocapture`：通过（51 passed）
25. `cargo check`（run state 失败快照接入后）：通过
26. `cargo test schedule::dag::tests::execute_parallel_failure_snapshot_allows_resume_without_rerunning_success_nodes -- --exact --nocapture`：通过
27. `cargo test schedule::dag -- --nocapture`：通过（52 passed）
28. `cargo check`（remote worker 取消快速生效接入后）：通过
29. `cargo test schedule::dag::tests::execute_parallel_real_redis_remote_worker_skips_canceled_request_before_execute -- --exact --nocapture`：通过
30. `cargo test schedule::dag -- --nocapture`：通过（53 passed）
31. `cargo check`（cancel/delivery_exhausted 顺序修正后）：通过
32. `cargo test schedule::dag::tests::execute_parallel_real_redis_remote_worker_canceled_request_not_dlq_even_if_delivery_counter_high -- --exact --nocapture`：通过
33. `cargo test schedule::dag -- --nocapture`：通过（54 passed）
34. `cargo check`（DAG 告警聚合指标接入后）：通过
35. `cargo test schedule::dag -- --nocapture`（告警聚合指标接入后）：通过（54 passed）
36. `cargo test module_dag -- --nocapture`（告警聚合指标接入后）：通过（8 passed）
37. `cargo check`（request_map TTL 强化后）：通过
38. `cargo test schedule::dag::tests::execute_parallel_real_redis_remote_dispatch_dedupes_long_running_with_short_result_ttl -- --exact --nocapture`：通过
39. `cargo test schedule::dag -- --nocapture`：通过（55 passed）
40. `cargo test module_dag -- --nocapture`：通过（8 passed）
41. `cargo check`（remote 告警事件打点接入后）：通过
42. `cargo test schedule::dag -- --nocapture`（remote 告警事件打点接入后）：通过（55 passed）
43. `cargo test module_dag -- --nocapture`（remote 告警事件打点接入后）：通过（8 passed）
44. `docs/alerts/logging_alerts.yml` 已新增 `dag_runtime_alerts` 告警模板（待实盘校准）
45. `docs/alerts/dag_alerts_calibration_runbook.md` 已新增（待按真实流量完成阈值校准与基线归档）
46. `cargo check`（cutover 冷却恢复窗口接入后）：通过
47. `cargo test module_dag -- --nocapture`（cutover 冷却恢复窗口接入后）：通过（8 passed）
48. `cargo test schedule::dag -- --nocapture`（cutover 冷却恢复窗口接入后）：通过（55 passed）
49. `scripts/dag_alerts_baseline.py` / `.ps1` / `.sh` 已新增（待接入实盘 Prometheus 执行）
50. `cargo check`（cutover 显式白名单门控接入后）：通过
51. `cargo test dag_cutover -- --nocapture`（cutover 白名单门控新单测）：通过（2 passed）
52. `cargo test module_dag -- --nocapture`（cutover 白名单门控接入后）：通过（8 passed）
53. `cargo test schedule::dag -- --nocapture`（cross-layer 回归）：通过（55 passed）
54. `cargo check`（shadow compare 门控接入后）：通过
55. `cargo test dag_cutover -- --nocapture`（cutover 门控单测）：通过（2 passed）
56. `cargo test module_dag -- --nocapture`（shadow compare 门控接入后）：通过（8 passed）
57. `cargo test schedule::dag -- --nocapture`（cross-layer 回归）：通过（55 passed）
58. `cargo check`（cutover 三层门控收敛后）：通过
59. `cargo test dag_cutover -- --nocapture`（cutover 门控回归）：通过（2 passed）
60. `cargo check`（cutover 预热门控接入后）：通过
61. `cargo test dag_cutover -- --nocapture`（cutover 门控回归）：通过（2 passed）
62. `cargo test module_dag -- --nocapture`（cutover 预热门控接入后）：通过（8 passed）
63. `cargo test schedule::dag -- --nocapture`（cross-layer 回归）：通过（55 passed）
64. `cargo test dag_cutover -- --nocapture`（cutover 运行时作用域隔离接入后）：通过（2 passed）
65. `cargo test module_dag -- --nocapture`（cutover 运行时作用域隔离接入后）：通过（8 passed）
66. `cargo check`（cutover 运行时作用域隔离接入后）：通过
67. `cargo test cutover_failure_isolation_by_scope_key -- --nocapture`（cutover tracker 隔离单测）：通过（1 passed）
68. `cargo test warmup_isolation_and_reset_by_scope_key -- --nocapture`（warmup tracker 隔离/重置单测）：通过（1 passed）
69. `cargo test module_dag -- --nocapture`（cutover tracker 重构后回归）：通过（8 passed）
70. `cargo check`（cutover tracker 重构后）：通过
71. `cargo test cutover_recovery_window_allows_probe_after_cooldown -- --nocapture`（cooldown 恢复窗口单测）：通过（1 passed）
72. `cargo test warmup_observation_window_requires_elapsed_time -- --nocapture`（warmup 观察窗口单测）：通过（1 passed）
73. `cargo test module_dag -- --nocapture`（cutover 时间门控可测试化后回归）：通过（8 passed）
74. `cargo check`（cutover 时间门控可测试化后）：通过
75. `cargo test dag_cutover -- --nocapture`（parser scope 一致性收敛后）：通过（2 passed）
76. `cargo test module_dag -- --nocapture`（parser scope 一致性收敛后）：通过（8 passed）
77. `cargo check`（parser scope 一致性收敛后）：通过
78. `cargo test dag_cutover -- --nocapture`（scope 统一入口收敛后）：通过（2 passed）
79. `cargo test module_dag -- --nocapture`（scope 统一入口收敛后）：通过（8 passed）
80. `cargo check`（scope 统一入口收敛后）：通过
81. `python -m py_compile scripts/dag_alerts_baseline.py`（baseline 脚本增强后语法检查）：通过
82. `python scripts/dag_alerts_baseline.py --help`（新增参数暴露校验）：通过
83. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e engine_task_queue_driven_single_node_full_chain_runs -- --nocapture`：通过
84. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`：通过（8 passed）
83. `python -m py_compile scripts/dag_alerts_baseline.py`（失败输出治理后语法检查）：通过
84. `pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/dag_alerts_gate.ps1 -?`（发布 gate 包装脚本参数检查）：通过
85. `bash -n scripts/dag_alerts_gate.sh`（发布 gate 包装脚本语法检查）：通过
86. `python scripts/dag_alerts_baseline.py --prom-url http://127.0.0.1:1 --profile dev --lookback-hours 1 --step-seconds 60 --output docs/dashboards/dag_alerts_baseline_latest.md --archive-dir docs/dashboards/archive --archive-prefix dag_alerts_baseline --summary-json docs/dashboards/dag_alerts_baseline_latest.json`（不可达 Prometheus 失败路径校验）：按预期失败（exit code 1，错误信息精简）
87. `pwsh -File scripts/dag_alerts_baseline.ps1 ...`（不可达 Prometheus 失败路径验证）：通过（参数透传正确，按预期失败退出）
88. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`（DAG + ModuleTrait 全链路测试）：通过（4 passed；Redis 不可用时 session 用例按预期 skip）
89. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e single_node_mode_without_redis_runs_full_chain -- --nocapture`（无 Redis 配置单节点全链路）：通过（1 passed）
90. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`（补充单节点用例后全量回归）：通过（5 passed；Redis 不可用时分布式 session 用例按预期 skip）
91. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e engine_driven_single_node_full_chain_runs -- --nocapture`（Engine 驱动全链路）：通过（1 passed）
92. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`（加入 Engine 驱动用例后全量回归）：通过（6 passed）
93. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e engine_dual_node_concurrent_full_chain_runs -- --nocapture`（双 Engine 并发全链路）：通过（1 passed）
94. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`（加入双 Engine 用例后全量回归）：通过（7 passed）
95. `cargo check --workspace`（运行时抗崩溃加固后）：通过
96. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`（运行时抗崩溃加固后）：通过（8 passed）
97. `cargo check --workspace`（State 初始化显式错误返回改造后）：通过
98. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`（State 初始化改造后全链路回归）：通过（8 passed）
99. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e engine_task_queue_primary_cutover_emits_preview_ok_metric -- --nocapture`：通过
100. `cargo test --manifest-path tests/Cargo.toml --bin dag_module_trait_e2e -- --nocapture`（补齐 primary cutover 运行态用例后全量回归）：通过（9 passed）

## 1. 文档目的

本文件用于将方案文档转化为可执行研发计划，目标是让团队可以按阶段并行开发、联调、灰度与回滚。

适用范围：

1. 模块接口层改造
2. DAG 编排接入
3. 分布式一致性与恢复能力接入
4. 测试、灰度、上线、回滚

输入文档：

1. docs/design/module_trait_dag_execution_refactor_plan.md
2. docs/design/module_node_dag_refactor_plan.md
3. docs/design/module_node_dag_development_guide.md


## 2. 交付总览

最终交付件：

1. 接口层
   - ModuleTrait 新增 build_dag
   - ModuleDagBuildContext 与 ModuleDagDefinition
   - 线性兼容转换 from_linear_steps
2. 编排层
   - ModuleDagOrchestrator
   - ModuleDagCompiler
   - ModuleNodeDagAdapter
3. 调度与分布式增强
   - remote dispatcher 幂等去重
   - fencing 严格拒写与错误码
   - run guard 续租与分区快速失败
   - run state checkpoint 恢复
4. 治理能力
   - 开关矩阵
   - 指标日志告警
   - 灰度白名单
5. 质量保障
   - 单元测试
   - 集成测试
   - 混沌演练
   - 回滚演练


## 3. 目标状态定义

功能目标：

1. 新模块默认通过 build_dag 定义执行图。
2. 老模块无改动可继续运行，自动映射为线性 DAG。
3. TaskManager 统一走 DAG 编排入口。
4. 分布式场景下具备恢复、去重、fencing、租约治理能力。

非功能目标：

1. 零数据丢失
2. 无双主并发写
3. 可观测可回滚
4. 关键路径性能回归在阈值内


## 4. 阶段计划与里程碑

### M1 接口就绪

时间建议：1 周

交付内容：

1. 在 src/common/interface/module.rs 增加 build_dag 默认实现
2. 定义 ModuleDagBuildContext
3. 定义 ModuleDagDefinition、ModuleDagNodeDef、ModuleDagEdgeDef
4. 实现 from_linear_steps 兼容转换
5. 完成接口层单测

退出条件：

1. 编译通过
2. 兼容模块行为与旧链路一致
3. 接口文档完成并评审通过


### M2 编排接入

时间建议：1 到 2 周

交付内容：

1. 新增 src/engine/task/module_dag_orchestrator.rs
2. 新增 src/engine/task/module_dag_compiler.rs
3. 新增 src/engine/task/module_node_dag_adapter.rs
4. TaskManager 接入统一 DAG 执行入口
5. module.rs 调整为图驱动执行准备

退出条件：

1. 显式 DAG 模块端到端可运行
2. 线性兼容路径可运行
3. 分叉与汇聚测试通过


### M3 分布式增强

时间建议：1 到 2 周

当前状态：已完成（代码与回归已落地，详见“已完成”清单 15-42 及相关验证记录）

交付内容：

1. src/schedule/dag/remote_redis.rs 增加幂等去重与退避
2. src/schedule/dag/fencing.rs 增加严格 token 拒写路径
3. src/schedule/dag/scheduler.rs 增加续租抖动和分区快速失败日志
4. run state checkpoint 与恢复流程对齐
5. 指标、日志、告警接入

退出条件：

1. 分区容错与恢复用例通过
2. 无双写回归
3. 可观测指标完整上报


### M4 灰度收敛

时间建议：1 周

当前状态：已完成（灰度治理、回归验证与收尾验收已完成）

交付内容：

1. 模块白名单逐批切流
2. 兼容路径稳定观察
3. 旧链路收敛计划确认
4. 变更公告与值班手册更新

退出条件：

1. Top N 核心模块迁移完成
2. 连续 2 个发布周期无回退
3. 进入 add_step 废弃周期

退出条件达成情况：

1. 已达成
2. 已达成
3. 已进入下一阶段规划（不阻塞 M4 完成）


## 5. 代码改造清单

### 5.1 接口层

主改造文件：

1. src/common/interface/module.rs

新增能力：

1. ModuleTrait.build_dag
2. ModuleDagBuildContext
3. ModuleDagDefinition 系列结构
4. from_linear_steps

实现要求：

1. build_dag 默认不做 I/O
2. 节点 ID 唯一且稳定
3. 禁止保留节点名冲突


### 5.2 编排层

主改造文件：

1. src/engine/task/task_manager.rs
2. src/engine/task/module.rs
3. src/engine/task/module_processor_with_chain.rs

新增文件：

1. src/engine/task/module_dag_orchestrator.rs
2. src/engine/task/module_dag_compiler.rs
3. src/engine/task/module_node_dag_adapter.rs

实现要求：

1. 入口统一到 orchestrator
2. compiler 提供构建期校验
3. adapter 负责 payload 编解码与错误归类
4. 兼容桥仅保留，不继续叠加新逻辑


### 5.3 调度与分布式能力

主改造文件：

1. src/schedule/dag/scheduler.rs
2. src/schedule/dag/remote_redis.rs
3. src/schedule/dag/fencing.rs

实现要求：

1. run guard 续租失败快速失败
2. remote dispatch 失败受控重试与退避
3. fencing token 失败统一错误码与指标
4. 恢复仅重放未成功节点


## 6. 配置与开关执行策略

开关清单：

1. module_dag_enabled
2. module_dag_force_linear_compat
3. module_dag_whitelist
4. module_dag_remote_dispatch_enabled
5. module_dag_cross_region_enabled
6. module_dag_strict_fencing

执行顺序：

1. 若 module_dag_enabled 为 false，走旧链路
2. 若 force_linear_compat 为 true，走线性兼容 DAG
3. 若模块在白名单，走显式 build_dag
4. 若不在白名单，走线性兼容 DAG
5. 若 cross_region 关闭，placement 强制本地域
6. 若 strict_fencing 开启，token 异常立即失败

默认建议：

1. 第一阶段只开启 module_dag_enabled 与白名单
2. strict_fencing 先在灰度环境开启
3. cross_region 默认关闭，稳定后再开启


## 7. 详细任务拆分

### 任务包 A 接口层

A1 新增 trait 与定义结构

1. 修改 module.rs
2. 增加结构体与默认实现
3. 保持向后兼容

A2 线性兼容转换

1. add_step 输出映射为线性 DAG
2. 节点 ID 生成策略固定
3. 增加边界校验

A3 单元测试

1. 空 step
2. 单 step
3. 多 step
4. 重复 ID 防护


### 任务包 B 编排层

B1 编译器

1. 定义输入输出
2. 执行拓扑校验
3. 构建 schedule::dag::Dag

B2 适配器

1. payload decode
2. 调用 module node generate 与 parser
3. payload encode
4. 错误归类

B3 编排器

1. 注入 dispatcher run_guard fencing run_state
2. 调用 execute_parallel
3. 结果映射 TaskOutputEvent

B4 接入 TaskManager

1. 增加 DAG 路径入口
2. 保留旧路径开关
3. 打通模块运行上下文


### 任务包 C 分布式治理

C1 幂等

1. 设计 idempotency_key 规则
2. 实现去重存储与 TTL
3. 实现重复执行短路

C2 租约与 fencing

1. 心跳参数对齐
2. token 校验拒写
3. 失败分类与错误码

C3 恢复

1. checkpoint 存储
2. 重启恢复流程
3. side effect 节点重放保护

C4 观测

1. 指标埋点
2. 结构化日志
3. 告警规则


### 任务包 D 灰度上线

D1 样板模块迁移

1. 选择 1 到 2 个核心模块
2. 显式 build_dag 实现
3. 对比旧链路结果

D2 分批灰度

1. 白名单按批次扩容
2. 观察指标与报警
3. 满足门槛后再扩批

D3 回滚演练

1. 一键回退到兼容路径
2. 一键关闭 DAG 总开关
3. 校验恢复耗时与正确性


## 8. 质量门禁与 DoD

每个任务完成定义：

1. 功能实现完成
2. 单测通过
3. 集成测试通过
4. 指标日志补齐
5. 文档更新
6. Code Review 通过

阶段完成定义：

1. M1 所有接口项可编译可测试
2. M2 显式 DAG 与兼容 DAG 均通过 E2E
3. M3 分布式故障用例通过
4. M4 灰度稳定达到阈值


## 9. 测试计划

### 9.1 单元测试

范围：

1. DAG 定义校验
2. from_linear_steps
3. adapter 编解码
4. 错误归类


### 9.2 集成测试

范围：

1. 分叉汇聚执行
2. fail-fast 与 fail-continue
3. timeout 与 cancel
4. run guard 与 fencing
5. resume 与恢复


### 9.3 分布式与混沌测试

范围：

1. leader kill
2. 网络分区
3. 远程分发抖动
4. 时钟漂移
5. 重复投递

通过判据：

1. 无双写
2. 无重复副作用
3. 恢复成功率满足阈值


### 9.4 建议命令

1. cargo test schedule::dag
2. cargo test schedule::dag::tests::execute_parallel_run_guard_renew_lost_fails_fast -- --exact
3. cargo test schedule::dag::tests::execute_parallel_run_timeout_interrupts_wait_loop_quickly -- --exact
4. cargo test schedule::dag::tests::execute_parallel_ready_queue_dedup_avoids_duplicate_dispatch -- --exact
5. cargo test module_processor_with_chain -- --nocapture


## 10. 灰度发布方案

灰度批次建议：

1. 批次 1：低风险模块，流量 5%
2. 批次 2：中风险模块，流量 20%
3. 批次 3：核心模块，流量 50%
4. 批次 4：全量

每批观测窗口：

1. 至少 24 小时
2. 覆盖高峰期
3. 无 P1 与持续告警

扩批条件：

1. 成功率达标
2. 延迟达标
3. duplicate_effect_rate 达标
4. fencing_reject_rate 可解释且稳定


## 11. 回滚 Runbook

触发条件：

1. 成功率显著下降
2. 出现双写或重复副作用
3. run guard 续租失败突增
4. 恢复失败率升高

回滚步骤：

1. 将 module_dag_whitelist 清空
2. 将 module_dag_force_linear_compat 置为 true
3. 必要时将 module_dag_enabled 置为 false
4. 保留现场日志与状态快照
5. 触发故障复盘流程

回滚后验证：

1. 旧链路恢复正常
2. 错误率回落
3. 核心任务积压回落


## 12. 可观测性实施细则

必备指标：

1. dag_run_total
2. dag_node_total
3. dag_node_latency_ms
4. dag_retry_total
5. dag_guard_renew_fail_total
6. dag_resume_reused_nodes
7. duplicate_effect_rate
8. fencing_reject_rate

必备日志字段：

1. run_id
2. module_id
3. node_id
4. attempt
5. placement
6. fencing_token
7. idempotency_key
8. error_code


## 13. 角色与职责

研发负责人：

1. 把控接口与架构一致性
2. 审核关键 PR
3. 组织联调与上线

调度与平台负责人：

1. run guard fencing resume 实现
2. 分布式故障演练
3. 观测体系完善

模块负责人：

1. build_dag 实现
2. 节点幂等策略落实
3. 模块回归测试

测试负责人：

1. 测试矩阵执行
2. 混沌演练执行
3. 验收报告输出


## 14. 研发排期模板

建议节奏：

1. 第 1 周：M1
2. 第 2 到 3 周：M2
3. 第 4 到 5 周：M3
4. 第 6 周：M4

周会检查项：

1. 里程碑状态
2. 阻塞与风险
3. 指标趋势
4. 回滚预案可用性


## 15. 验收标准

功能验收：

1. 显式 DAG 与兼容 DAG 均可运行
2. 关键模块迁移后行为零回归
3. 默认入口切换为 DAG

稳定性验收：

1. 成功率不低于旧链路
2. P95 回归不超过 5%
3. run guard 与 fencing 错误占比低于阈值
4. duplicate_effect_rate 低于阈值
5. 恢复成功率高于阈值

运维验收：

1. 指标与告警完整
2. 回滚可一键执行
3. 演练记录完整


## 16. 附录 A 实施检查清单

上线前：

1. 开关矩阵已校验
2. 白名单已配置
3. 告警规则已生效
4. 回滚脚本可用
5. 值班人员已知会

上线中：

1. 实时看板正常
2. 关键指标稳定
3. 无新增 P1

上线后：

1. 输出阶段验收报告
2. 更新迁移台账
3. 进入下一批次灰度
