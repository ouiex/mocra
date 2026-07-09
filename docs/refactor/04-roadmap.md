# 04 · 实施路线

> 原则:**每阶段独立可发布、可回退。** Phase 0+1 就能兑现「简单 API」这个最大卖点,且几乎不动核心管线——风险最低、回报最高。集群化(Phase 3)在内核解耦之后进行。

## 目标 workspace / crate 布局

从单 crate 拆成 workspace。许多子系统本就干净、与爬虫无关,天然适合独立成 crate。

```
mocra/                       (workspace 根)
├── crates/
│   ├── mocra/               门面:Spider trait、Mocra 构建器、prelude、默认 sink
│   │                        —— 用户唯一需要 import 的东西
│   ├── mocra-core/          管线、processor 循环、中间件、下载器、模块运行时(解耦 State)
│   ├── mocra-cluster/       控制面:openraft + redb、CoordinationBackend 实现、分区归属、join API
│   ├── mocra-queue/         数据面:MqBackend trait + Kafka/NATS/Redis/in-mem 实现
│   ├── mocra-dag/           通用分布式 DAG 引擎(现 schedule/dag,零爬虫耦合)
│   ├── mocra-proxy/         ProxyManager / ProxyPool(配置驱动、零 State)
│   └── mocra-store/         (选配)account×platform×module 多租户模型、SeaORM 实体、Cron 批量调度
├── examples/                纳入 CI 的可编译示例
└── docs/
```

依赖方向(单向,无环):

```
mocra ──▶ mocra-core ──▶ mocra-queue
  │           │      └──▶ mocra-dag
  │           ├────────▶ mocra-cluster ──▶ (openraft, redb)
  │           ├────────▶ mocra-proxy
  │           └────────▶ mocra-store   (仅 store 特性)
```

### 特性门控(Cargo features)

```toml
[features]
default = ["sqlite", "cluster-embedded", "queue-memory"]

# 数据面 MQ(择需开启)
queue-memory = []
queue-kafka  = ["dep:rdkafka"]
queue-nats   = ["dep:async-nats"]
queue-redis  = ["dep:deadpool-redis"]

# 控制面
cluster-embedded = ["dep:openraft", "dep:redb"]   # redb+raft
cluster-redis    = ["dep:deadpool-redis"]         # 过渡期:仍用 Redis 协调

# 可选能力
store    = ["dep:sea-orm"]        # L2 多租户
polars   = ["dep:polars"]         # 数据帧(修掉现在的空壳 no-op)
excel    = ["dep:calamine"]
js-v8    = ["dep:v8"]
```

> 现状:polars / calamine / rdkafka(cmake)/ sea-orm 全部强制编译,且 `polars` 特性是空壳。门控后**默认精简**为 sqlite + 内嵌集群 + 内存队列,拉进依赖不再是一次沉重编译。

## 分阶段任务拆解

### Phase 0 · 地基止血 · 🟢 低风险 · ⏳ 进行中

为后续一切改动立安全网。

- [x] 建 `[workspace]`(root 单包成员,`resolver = "2"`);`tests/` 暂缓纳入 —— 其引用已删类型,待 Phase 1 示例改写时一并修。
- [x] 加 **CI**(`.github/workflows/ci.yml`):`build` / `test` / `clippy`。**主 crate 全特性 clippy 已清零并 CI 收紧 `-D warnings`** —— 默认(41 → 0)+ **特性矩阵**(`store` / `dashboard` / `cluster-embedded` / `queue-nats` / `queue-kafka` / `polars,excel`,修掉 store 的 5 处无效 SQL 转义空操作 + polars 的 2 处冗余 `as` + 1 处冗余闭包);4 个子 crate `-D warnings` + `cargo doc -D warnings`;`store` / `cluster-embedded` / `queue-nats`(真实 NATS 集成)/ `polars,excel` / examples(含 dashboard)全覆盖。`fmt` 待收紧。
- [x] 移除库代码里的 `process::exit`(`common/state.rs`:删除 `new` / `new_with_provider` 退出包装,仅保留 `try_new*`)。v8 worker 内一处仍在 `js-v8` 特性内,后续处理。
- [x] **特性门控 rdkafka** → `queue-kafka`(默认关闭,移除 cmake/C++ 构建负担;`queue`/`sync` 的 kafka 后端与相关测试随之门控)。已验证默认 `cargo check --lib --tests` 不再编译 rdkafka。
- [x] **特性门控 polars / calamine** —— 三个 polars crate + calamine 改 `optional`;`polars = ["dep:polars", ...]`(修掉空壳)/ `excel = ["dep:calamine", "polars"]`,默认**全关**。`DataType` 新增始终存在的 `Empty` 默认变体,`DataFrame` 变体 + `DataFrameStore` + `polars_utils` / `excel_dataframe` / `type_convert`(零外部引用的死代码)全部门控。补 `serde/rc`(原先靠 polars 隐式带入 `Arc: Serialize`)。**默认依赖树不再含 polars**;`--features polars` / `excel` 恢复能力,测试全绿。(postgres 随 `store` 特性,已在 Phase 2 门控。)
- [ ] 修正漂移的文档与示例:`run()`→`start()`、已删的 `Data`/`ParserData` 类型、`pre_process`/`cron` 签名(随 Phase 1 示例改写)。

**产出**:能过 CI、默认精简依赖(已去 rdkafka/cmake)、示例可编译的基线。

### Phase 1 · 门面 API · 🟢 低风险 · ⏳ 进行中

**头号交付。** 在现有引擎**之上**加薄层,不改核心。落在新模块 `src/facade.rs`。

- [x] `Spider` trait + `Seeds` / `Ctx`;`Response::text` / `text_lossy` / `json` 便捷解析(`css` 待 `html` 特性)。
- [x] `DataSink` trait + `on_item`(闭包 sink);`Spider::Item` 类型化出口(`ChannelSink` / `KafkaSink` 后补)。
- [x] `Spider` → `ModuleTrait` / `ModuleNodeTrait` 适配器(单节点;`follow` 经元数据回灌 `generate`)。
- [x] `Mocra::builder()` + `.spider(spider, sink)` + `.from_toml(path)` + `.run()`(已编译通过)。
- [x] **内存 metadata provider**:合成 account/platform、自动注入种子任务,**绕开 DB 强制要求**(L0 无 DB 运行)—— 已随 Phase 2 的 DB 可选化落地(`default_standalone_config` + `StaticConfigProvider`,`Mocra::builder().spider(..).run()` 免 TOML / 免 DB 直接跑)。
- [x] 程序化默认配置(免 TOML)—— 同上,`Mocra::builder()` 无 `from_toml` 时用内置单机默认配置。
- [~] 能编译的 doctest + 快速上手文档:crate 根 `//!` 加**编译校验的 quick-start doctest**(镜像 `spider_quickstart`,`cargo test --doc` 纳入 CI,防 API 漂移);端到端运行验证(mock server + sqlite)仍待补。

**产出**:`Mocra::builder().spider(s, on_item(..)).run()` —— **无 DB、无 Redis 三行跑通**,已编译通过(依赖 Phase 2 的 DB 可选化)。运行时端到端验证进行中。

### Phase 2 · 解耦内核 · 🟡 中风险 · ⏳ 进行中

把门面的「薄封装」变成真正的薄。

- [x] 拆 `State` 上帝对象为聚焦上下文。**核心采集管线已全部脱离 `State` 结构体**:
  - 新增 [`PipelineContext`](../../src/common/context.rs)(`config` / `cache_service` / `status_tracker` / `locker` 四字段)+ `State::pipeline_ctx()`;**四个 chain(download / parser / task-model / stream)由 `Arc<State>` 窄化为 `Arc<PipelineContext>`** —— 管线不再触达 `db` / `cookie_service` / `limiter` / `api_limiter` / `redis` / `coordination` 等可选或集群子系统(10 字段 → 4)。
  - `SystemMonitor::run` 去掉从不使用的 `Arc<State>` 参数(彻底脱钩);`TaskManager::new` 由 `Arc<State>` 窄化为 `&DbHandle` + `cache_service` / `cookie_service` / `config` 显式依赖。
  - 早前已卸下:`MiddlewareManager`(删死耦合)、`DownloaderManager`(窄化为 4 依赖)、`TaskFactory`(窄化为 `app_config` + 复用 `cache_service`)。
  - 剩余 `Arc<State>` 持有者为**组合根 `Engine`** 与**天然耦合协调 / DB 的后台任务**(`CronScheduler` 需 `coordination`+`db`、`zombie` 需 `db`)—— 属正当依赖,是未来抽 `mocra-core` 时的宿主侧装配点。默认 / `store` / `dashboard` / `cluster-embedded` 全绿。
- [x] **DB 变可选**:`State.db` 改 `Option<Arc<DatabaseConnection>>`;`db.url` 缺省即 standalone;无 DB 时 `TaskFactory::create_synthetic_task` 从内存模块注册表合成任务;zombie / scheduler / health 均加无 DB 守卫;`Mocra` 程序化默认配置 + 自动种子任务注入。已验证热路径无 DB 写入。
- [x] **`Task`/`Module` 从 sea-orm 实体解耦**(sea-orm gating 的关键前置):新增轻量 `AccountInfo`/`PlatformInfo`(`common/model/scope.rs`)取代内嵌的 `AccountModel`/`PlatformModel`;DB 路径转换填充,synthetic 路径直接构造;`factory` 不再直接依赖实体类型。
- [x] **把 sea-orm gate 出默认构建**:新增 `store` 特性(默认关闭);`entity` / `repository` / `txn` / `ConfigAssembler` / `connector` 的 db 部分全部门控;`State.db` 用 `DbHandle` 类型别名(无 `store` 时为占位 `()`,恒 `None`);`factory` / `task_manager` / `scheduler` / `zombie` / `health` 的 DB 路径按特性分支(`MaybeRepository` 别名统一 `TaskFactory::new` 签名)。**默认构建不再编译 sea-orm**;`--features store` 恢复完整 DB / 多租户能力。
- [x] 把 DB 可选化收敛为正式 `MetadataStore` trait;`mocra-store` 收纳多租户逻辑。
  - **`mocra-store` crate**([`crates/mocra-store`](../../crates/mocra-store)):sea-orm 多租户实体(account×platform×module + 中间件/关系表)抽出;纯 sea-orm+serde、零主 crate 耦合、独立编译、精简 features(无 runtime/sqlx)。主 crate `model::entity` 转 `pub use mocra_store::entity`(保留所有路径)。
  - **`MetadataStore` trait**(`repository.rs`):13 个 `load_*` 方法收敛为正式 trait,`TaskRepository` impl 它;引擎的 `MaybeRepository` 从 `Option<TaskRepository>` 改为 **`Option<Arc<dyn MetadataStore>>`** —— DB 访问经 trait 对象派发,便于替换 / mock。216 个 store 测试全绿。
- [x] `MiddlewareManager` 去除未使用的 `Arc<State>` 死耦合(此前存了却从不读);`DownloaderManager` 从 `Arc<State>` 窄化为 `config`/`limiter`/`locker`/`cache_service` 四个具体依赖 —— 二者均可脱离 `State` 构造与测试。
- [ ] 收敛 `pub` 边界(2214 → 精选):内部转 `pub(crate)`,精修 `prelude`。
- [x] 抽 `mocra-dag` / `mocra-proxy`(现成干净)为独立 crate。**两者均已抽出、独立编译(不反依赖主 crate)、clippy `-D warnings` 净、CI 纳入**:
  - [`crates/mocra-proxy`](../../crates/mocra-proxy):自带 `ProxyError` / `Result`,`src/proxy` 转 shim `pub use mocra_proxy::*` + `impl From<mocra_proxy::ProxyError> for Error` 边界转换;9 个精简依赖。
  - [`crates/mocra-dag`](../../crates/mocra-dag):通用分布式 DAG 引擎,**运行时依赖 trait 化** —— `DagStore`(get/set/del/incr/eval_lua)抽象 `CacheService`、`DagEventSink` 抽象 `SyncService`,`crate::common::metrics` 内联;宿主 `src/schedule` 转 shim + adapter(`impl DagStore for CacheService`、`SyncAble for DagNodeSyncState`);**54 个测试留主 crate 用真实 `CacheService` 驱动、全绿**。

**产出**:核心不再强制依赖 DB(已达成「无 DB 可跑」);公共 API 表面可控,可谈语义版本(待续)。

### Phase 3 · 集群化(redb+Raft)· 🔴 较高投入

还清最大技术债 + 落地新控制面。详见 [03 · 集群架构](03-cluster-architecture.md)。

- [x] **新增 `mocra-cluster` crate**(workspace 成员):
  - [x] redb 复制状态机(`kv` / `locks` + 单调 fencing token,确定性 `apply(Cmd)`)+ 单元测试(cas、锁 fencing/过期/续租/释放全绿)。
  - [x] `ControlPlane` trait + `LocalControlPlane`(单节点:set/get/cas/acquire_lock/renew_lock/release_lock)。
  - [x] **openraft 共识节点**(0.9.24 · `storage-v2`):`RaftLogStorage`+`RaftLogReader` + `RaftStateMachine`+`RaftSnapshotBuilder`(over redb,快照走 dump/restore)+ `RaftControlPlane`(装配 `Raft::new`+`initialize`,经 `client_write` 实现 `ControlPlane`)。
  - [x] **多节点**:HTTP + msgpack 网络 RPC(append/vote/snapshot)+ `/cluster/join`(方案 A:小投票核心 3~5 + 多 worker learner)+ **持久化 redb 日志**(`RedbLogStore`)+ **client-write 转发**(`/cluster/write`:任意节点受理写,follower 自动转发 leader)。**3 节点 HTTP 集群端到端测试通过**(复制 / 成员 / 锁 fencing / 分区)。
  - [x] 主 crate 侧 `RaftCoordinationBackend` 适配 `CoordinationBackend`(set/get/cas/lock/renew/release/pub-sub),替换 Redis 协调;facade `.cluster(ClusterConfig)` 自举 / join。
- [x] `DistributedLockManager` 改走 `CoordinationBackend`(→ Raft,`new_with_coordination`,带续租 + CAS-del 释放,端到端测试绿);限流改本地按成员数分摊(`share_rps`,仅无 Redis 时分摊,Redis 仍走原子全局)。
- [x] 分区归属**原语**:`hash(account)` → 分区 + **rendezvous(HRW)分配**(确定性、成员增减仅 ~1/N 迁移)+ Raft **fencing 归属租约**(`acquire_partition`);经 `CoordinationBackend::owns_partition_key` 暴露给引擎。**跨节点归属互斥 + 全覆盖 + fencing 单调**均测试绿。
- [x] **接入任务分发(控制面)**:`CronScheduler` 在多节点 Raft 集群下**各节点只调度归属自己的分区**(账号 rendezvous 互斥,无 leader 瓶颈,cron 负载水平扩展);单机 / Redis 协调回退到既有 leader 模式。内存队列下**触发与处理同在归属节点**,端到端闭环;决策逻辑单测。种子注入亦按 leader 去重(避免 N× 重复抓取)。
- [x] **接入任务分发(数据面)**:`Identifiable::partition_key`(`TaskEvent` 覆写为账号)让任务消息按 `hash(account)` 落同一 Kafka 分区 / Redis 流分片,复用 MQ 消费组分配实现**跨节点消费亲和**;与去重 / 补偿用的 `get_id`(run_id)相互独立。单测 + 端到端(mock 后端捕获分区键 = 账号)绿。
- [x] **保留 `MqBackend` 多实现**,新增 **NATS(JetStream)后端**(`queue-nats` 特性,[`src/queue/nats.rs`](../../src/queue/nats.rs)):持久化 stream + durable pull consumer + explicit ack + nack 按 `NackPolicy` 重投原 subject / 投 DLQ,语义对齐 Kafka;in-memory(`backend=None`)作单机默认。**对真实 NATS 集成测试测通**(publish→subscribe→ack 往返;nack attempt 0→1 重投→超限投 DLQ),CI 用 Docker 起 JetStream 容器跑 `--ignored` 集成测试。账号亲和在 NATS 下暂为竞争消费(负载均衡),粘账号后续用分区 subject 实现。
- [x] 合并 DAG 执行:**统一为单一路径**。先前已删死执行器 `ModuleProcessorWithChain`(1104 行);本轮再剥掉**从未真正执行、仅在注册时预编译入缓存的「影子」DAG 系统** —— 占位 `ModuleNodeDagAdapter`(其 `DagNodeTrait::start` 是 no-op)、`ModuleDagCompiler`(编译成 `mocra-dag` 的 `Dag`)、orchestrator 的 `compile_*` / `execute_dag`、`TaskManager` 的 per-module `compiled_dags` 缓存 + `DagCutoverStateTracker` + 全部 cutover/compare 包装、以及公开 API `Engine::get_module_dag`(破坏性)。模块 DAG 现只有一条路:`ModuleDagOrchestrator::build_definition` → 队列驱动的 `ModuleDagProcessor`;顺带去掉每次模块注册时的无用预编译。删净后核心引擎不再残留并行 DAG 表示,`task_manager.rs` 由 514 → ~140 行。默认 / `store` / `dashboard` / `cluster-embedded` / `queue-*` / `polars,excel` 全绿。

**产出**:「不装 Redis 也能起一个自组网的分布式采集集群。」—— 控制面(选举 / 锁 / KV / 成员 / 分区归属)已全部 Raft 化并测试;数据面 MQ 可插拔待补 NATS。

**本轮收口验证**:
- facade `Mocra::builder().cluster(ClusterConfig{node_id,http_addr,data_dir,seeds}).run()` —— 自举 / join 一步到位;协调后端在 State 构造前注入,锁 / 选举 / 限流从起点即走 Raft。
- **端到端选主测试**(`test_raft_leader_election`):两节点各跑 `LeaderElector`,经 `RaftCoordinationBackend → RaftControlPlane → Raft → redb` 全链路,**恰好一个** leader,原 leader 停续租后另一个在 TTL 内接管。
- **崩溃恢复**(`single_node_recovers_state_after_restart`):KV / CAS / 锁状态经 redb 持久化,重开同一目录全部恢复;新增 `RaftControlPlane::shutdown()` 优雅释放存储句柄。
- **leader 失效重选**(`cluster_survives_leader_failure`):干掉 3 节点集群的 leader,剩余多数派重新选主并继续提交(写经转发到新 leader),崩溃前数据无损。
- **动态重平衡**(`cluster_rebalances_partitions_as_nodes_join`):节点逐个入网,活集群里分区归属按 HRW 重平衡(既有节点归属集合只缩小,`set3 ⊆ set2`,绝不重获)。
- **日志压缩 + 快照安装**(`new_node_catches_up_via_snapshot_install`):触发快照 + purge 日志后新节点加入,只能经 `install_snapshot` 追平(RaftSnapshotBuilder → RPC → redb restore 全链路)—— 长时运行集群的关键。
- **缩容 / 下线**(`cluster_handles_voter_removal`):3 投票节点移除一个,新多数派继续提交并复制;quorum 重算正确。
- **写路径鲁棒性**:`write()` 在**选举中(暂无 leader)**退避重试本地 `client_write`(该情形命令确定未提交,对非幂等 CAS/AcquireLock **无双应用风险**;已知 leader 的转发仅一次,HTTP 失败不重试因结果不确定)。写转发复用**共享 `reqwest::Client`**(连接池复用,follower 写热路径不再每次新建客户端)。
- **可调 Raft 时序**:`RaftTuning`(心跳 / 选举超时)+ `start_cluster_node_with`;facade 侧 `ClusterConfig::with_raft_tuning(..)` 透出 —— 广域网 / 高延迟集群可放大超时。默认局域网参数不变。
- **DAG 死执行器清理**:删除已被 `ModuleDagProcessor` 取代的 `ModuleProcessorWithChain`(1104 行,零外部引用)。
- **跨节点 pub/sub**(`raft_backend_pubsub_across_nodes`):节点 1 发布、节点 2 收到(经复制 KV);**并修掉一个真实竞态**——follower 上 seq(前一条日志)可能先于消息(后一条)被 apply,原 `subscribe` 贸然自增会**永久丢该消息**;改为「读到才推进」。
- **优雅关闭**:`CoordinationBackend::shutdown`(Raft 覆写调 `RaftControlPlane::shutdown`),facade 在引擎停机后调用,干净释放 redb 句柄。
- **易用 API**:`ClusterConfig::bootstrap(id, addr, dir)` / `join(id, addr, dir, seed)` 便捷构造 + `from_env()`(容器化部署,读 `MOCRA_*` 环境变量);`.spider().cluster(ClusterConfig::bootstrap(..)).run()` 三步起集群。
- 可编译 `examples/cluster_quickstart.rs`(`required-features = cluster-embedded`)。
- `mocra-cluster` clippy 零告警(12 项测试);主 crate `cluster-embedded` 231 项、默认 / `store` / `queue-kafka` 全绿;清理主 crate 5 处 unused-import 告警。

### Phase 4 · 发布打磨 · 🟢 低风险

- [x] `examples/` 纳入 CI(`cargo build --examples`,含 dashboard 示例);docs.rs:4 个子 crate `cargo doc` 净(0 告警)+ **主 crate 全部 intra-doc 链接修净** + `[package.metadata.docs.rs] all-features`(门控条目在文档站可解析)+ crate 根 quick-start doctest;`cargo test --doc` 纳入 CI。
- [x] 确立 **MSRV**(`rust-version = "1.85"`,edition 2024 基线,root + 4 crate)、`CHANGELOG.md`(记录本次重构)。语义版本纪律待正式发布确立。
- [ ] 基准与对标(上手曲线 vs scrapy / colly;吞吐 vs 现 Redis 版)。
- [~] 发布各 crate 到 crates.io:4 个子 crate 已备 README + 元数据(repository / keywords / categories / readme)、`cargo package` 通过(可发布);正式发布待定。

## 推进顺序与「什么时候发什么」

```
Phase 0 ──▶ 发 0.3(精简依赖 + CI + 修文档)
Phase 1 ──▶ 发 0.4(Spider + builder,「简单 API」上线)      ← 最大卖点,尽早
Phase 2 ──▶ 发 0.5(DB 可选、API 边界收敛)
Phase 3 ──▶ 发 0.6(mocra-cluster 内嵌集群;数据面保持 MQ 可插拔)  ← 分步:先控制面 Raft 化,再补 NATS,后合并 DAG
Phase 4 ──▶ 发 1.0(打磨、语义版本承诺)
```

集群化内部再细分,每步可独立发布/回退:

1. **先做控制面 Raft 化**(接入点 `CoordinationBackend` 干净、可独立验证);
2. **数据面先保持 Kafka/Redis 可插拔**不动;
3. **再补 NATS 等后端 + `DataSink` 出口**;
4. **最后啃 DAG 合并**。

## 兼容与迁移策略

- **L2 语义不变**:`ModuleTrait` / `TaskEvent` / account×platform×module 全部保留,迁进 `store` 特性。老用户开 `store` 即维持现状。
- **过渡期双控制面**:`cluster-redis` 特性保留 Redis 协调,`cluster-embedded` 为新 redb+Raft。两者都实现 `CoordinationBackend`,可灰度切换、按需回退。
- **配置向后兼容**:现有 `config.toml` 仍可 `Mocra::from_toml()` 加载;新增字段带默认值。
- **数据面无强制迁移**:仍用 Redis Streams 队列的用户,去掉的只是**控制面**的 Redis(锁/协调/选举),队列可继续用 `queue-redis`。

## 风险登记

| 风险 | 阶段 | 缓解 |
|---|---|---|
| Raft 运维复杂度(快照/压缩/成员变更) | P3 | 沿用 openraft 成熟实现;小投票集(3~5);充分测试成员变更路径 |
| 分区亲和与 MQ 消费组分配错位(会话粘性) | P3 | 分区键 = `hash(account)`;优先复用 MQ 分配,Raft 归属表作兜底 |
| 失去原子全局限流 | P3 | 本地按成员分摊 + 文档明确「近似」语义 |
| DAG 合并引入回归 | P3 | 影子对比(现有 `DagCutover` 机制)灰度切换 |
| API 大改冲击老用户 | P1–P2 | `Spider` 为新增主路径,`ModuleTrait` 并存;语义版本 + CHANGELOG |

---

上一篇:[03 · 集群架构](03-cluster-architecture.md) · 返回 [重构文档索引](README.md)
