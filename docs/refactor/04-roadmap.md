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
- [x] 加 **CI**(`.github/workflows/ci.yml`):`build` / `test` / `clippy`(`-D warnings` / `fmt` / `doc` 待警告清零后收紧)。
- [x] 移除库代码里的 `process::exit`(`common/state.rs`:删除 `new` / `new_with_provider` 退出包装,仅保留 `try_new*`)。v8 worker 内一处仍在 `js-v8` 特性内,后续处理。
- [x] **特性门控 rdkafka** → `queue-kafka`(默认关闭,移除 cmake/C++ 构建负担;`queue`/`sync` 的 kafka 后端与相关测试随之门控)。已验证默认 `cargo check --lib --tests` 不再编译 rdkafka。
- [ ] 特性门控 polars / calamine / postgres —— 与核心数据模型(`DataEvent` 内嵌 `DataFrame`)/DB 深耦合,随 Phase 1/2 的 `DataSink` / `MetadataStore` 重设计一并处理;`polars = []` 空壳同期修正。
- [ ] 修正漂移的文档与示例:`run()`→`start()`、已删的 `Data`/`ParserData` 类型、`pre_process`/`cron` 签名(随 Phase 1 示例改写)。

**产出**:能过 CI、默认精简依赖(已去 rdkafka/cmake)、示例可编译的基线。

### Phase 1 · 门面 API · 🟢 低风险 · ⏳ 进行中

**头号交付。** 在现有引擎**之上**加薄层,不改核心。落在新模块 `src/facade.rs`。

- [x] `Spider` trait + `Seeds` / `Ctx`;`Response::text` / `text_lossy` / `json` 便捷解析(`css` 待 `html` 特性)。
- [x] `DataSink` trait + `on_item`(闭包 sink);`Spider::Item` 类型化出口(`ChannelSink` / `KafkaSink` 后补)。
- [x] `Spider` → `ModuleTrait` / `ModuleNodeTrait` 适配器(单节点;`follow` 经元数据回灌 `generate`)。
- [x] `Mocra::builder()` + `.spider(spider, sink)` + `.from_toml(path)` + `.run()`(已编译通过)。
- [ ] **内存 metadata provider**:合成 account/platform、自动注入种子任务,**绕开 DB 强制要求**(L0 无 DB 运行)—— 归入 Phase 2 的 DB 可选化,当前 `run()` 仍需 `from_toml` 提供含 `db` 的 config。
- [ ] 程序化默认配置(免 TOML)—— 同上,随 Phase 2。
- [ ] 能编译的 doctest + 快速上手文档 + 端到端运行验证(需 mock server + sqlite)。

**产出**:`Mocra::builder().spider(s, on_item(..)).run()` —— **无 DB、无 Redis 三行跑通**,已编译通过(依赖 Phase 2 的 DB 可选化)。运行时端到端验证进行中。

### Phase 2 · 解耦内核 · 🟡 中风险 · ⏳ 进行中

把门面的「薄封装」变成真正的薄。

- [ ] 拆 `State` 上帝对象为聚焦上下文(Cache / Lock / Queue / Limit …)或精简 `Runtime`。已从两个 manager 卸下 `Arc<State>`(见下),其余持有点待续。
- [x] **DB 变可选**:`State.db` 改 `Option<Arc<DatabaseConnection>>`;`db.url` 缺省即 standalone;无 DB 时 `TaskFactory::create_synthetic_task` 从内存模块注册表合成任务;zombie / scheduler / health 均加无 DB 守卫;`Mocra` 程序化默认配置 + 自动种子任务注入。已验证热路径无 DB 写入。
- [x] **`Task`/`Module` 从 sea-orm 实体解耦**(sea-orm gating 的关键前置):新增轻量 `AccountInfo`/`PlatformInfo`(`common/model/scope.rs`)取代内嵌的 `AccountModel`/`PlatformModel`;DB 路径转换填充,synthetic 路径直接构造;`factory` 不再直接依赖实体类型。
- [x] **把 sea-orm gate 出默认构建**:新增 `store` 特性(默认关闭);`entity` / `repository` / `txn` / `ConfigAssembler` / `connector` 的 db 部分全部门控;`State.db` 用 `DbHandle` 类型别名(无 `store` 时为占位 `()`,恒 `None`);`factory` / `task_manager` / `scheduler` / `zombie` / `health` 的 DB 路径按特性分支(`MaybeRepository` 别名统一 `TaskFactory::new` 签名)。**默认构建不再编译 sea-orm**;`--features store` 恢复完整 DB / 多租户能力。
- [ ] (后续)把 DB 可选化收敛为正式 `MetadataStore` trait;`mocra-store` 收纳多租户逻辑。
- [x] `MiddlewareManager` 去除未使用的 `Arc<State>` 死耦合(此前存了却从不读);`DownloaderManager` 从 `Arc<State>` 窄化为 `config`/`limiter`/`locker`/`cache_service` 四个具体依赖 —— 二者均可脱离 `State` 构造与测试。
- [ ] 收敛 `pub` 边界(2214 → 精选):内部转 `pub(crate)`,精修 `prelude`。
- [ ] 抽 `mocra-dag` / `mocra-proxy`(现成干净)为独立 crate。

**产出**:核心不再强制依赖 DB(已达成「无 DB 可跑」);公共 API 表面可控,可谈语义版本(待续)。

### Phase 3 · 集群化(redb+Raft)· 🔴 较高投入

还清最大技术债 + 落地新控制面。详见 [03 · 集群架构](03-cluster-architecture.md)。

- [ ] **新增 `mocra-cluster`**:`openraft` + `redb` 状态机、`RaftBackend: CoordinationBackend`、join API、成员/心跳。
- [ ] `DistributedLockManager` 改走 `CoordinationBackend`(→ Raft);限流改本地按成员数分摊。
- [ ] 分区归属:优先复用 MQ 消费组分配 + `hash(account)` 亲和;为无消费组后端做 Raft 归属表 + 再平衡 + fencing。
- [ ] **保留 `MqBackend` 多实现**,新增 `mocra-queue` 的 NATS 后端;in-memory 作单机默认。
- [ ] 合并 DAG 执行:`module_dag_processor` + `module_processor_with_chain` → 单一队列驱动执行器,删占位 adapter 与重复门控。

**产出**:「不装 Redis 也能起一个自组网的分布式采集集群。」

### Phase 4 · 发布打磨 · 🟢 低风险

- [ ] `examples/` 纳入 CI;docs.rs 全绿(feature 组合验证)。
- [ ] 确立 **MSRV**、语义版本纪律、`CHANGELOG.md`。
- [ ] 基准与对标(上手曲线 vs scrapy / colly;吞吐 vs 现 Redis 版)。
- [ ] 发布各 crate 到 crates.io。

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
