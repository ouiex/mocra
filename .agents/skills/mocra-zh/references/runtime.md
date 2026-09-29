# 运行时、扩展点与验证

适用于修改队列、DAG、模块、集群、配置、性能或文档。先从 `docs/architecture.md` 找到阶段，再看相应源码与测试。主包 `mocra` 是 facade；主要运行时在 `crates/mocra-core`；`mocra-proxy`、`mocra-dag`、`mocra-cluster`、`mocra-store` 是可复用子 crate。

## 路径选择

| 需求 | 入口 | 深入阅读 |
| --- | --- | --- |
| 单个抓取和数据输出 | `Spider` + `DataSink` | `src/facade.rs`、`docs/getting-started.md` |
| 多节点处理、登录或自定义 DAG | `ModuleTrait` / `ModuleNodeTrait` | `docs/module-development.md`、`docs/dag-guide.md` |
| 下载、数据、存储拦截 | middleware traits | `docs/middleware-guide.md` |
| 配置或分布式部署 | `MocraBuilder::from_toml` 与 feature flags | `docs/configuration.md`、`docs/deployment.md` |
| 指标与控制接口 | `dashboard` feature | `docs/api-reference.md`、`examples/dashboard.rs` |

一次任务依次经过 task/generate、request/download、response/parse，再到输出、后续任务或错误处理。改跨阶段字段时沿事件类型、序列化、队列编码、重试与落盘路径追踪，尤其检查旧任务格式能否继续读取。`Spider` 会适配为单节点模块；不要在 facade 中再造一套与核心运行时不同的下载或代理路径。

## feature 与部署边界

- 默认构建无数据库与外部消息队列。`store` 开启数据库任务模型；`dashboard` 开启管理与观测 API；`cluster-embedded` 开启 Raft + redb 协调面；`queue-kafka` / `queue-nats` 分别开启跨进程数据队列。
- `.cluster(…)` 负责领导选举、锁与成员关系。若任务队列仍为内存实现，多个进程之间不会共享爬取任务；跨节点数据面需另配 Kafka 或 NATS。见 `examples/cluster_quickstart.rs` 和 `docs/deployment.md`。
- `.from_toml(path)` 加载引擎配置。配置字段受 Cargo feature 约束；不要仅凭 TOML 中存在某段配置就推断功能已编译或开启。

## 验证矩阵

| 变更 | 有意义的检查 |
| --- | --- |
| `Spider` 或 facade | `cargo test -p mocra --lib`；`cargo check --examples`；运行相关离线示例 |
| `mocra-core` 事件、下载或解析链 | `cargo test -p mocra-core --lib`；覆盖受影响的请求字段与失败路径 |
| 代理池 | `cargo test -p mocra-proxy --lib`；`cargo run --example proxy_pool` |
| dashboard / 集群 | `cargo check --examples --features dashboard,cluster-embedded`；必要时运行对应示例 |
| 文档或示例 | `cargo fmt --all -- --check`、`git diff --check`；复制执行文档中的关键命令 |

使用最小可验证范围，遇到编译 feature 的系统依赖或外部服务缺失时报告实际边界。`docs/zh/` 与英文指南有对应章节；改动行为或示例入口时同步更新相关语言版本。
