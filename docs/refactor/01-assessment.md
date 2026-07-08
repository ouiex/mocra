# 01 · 现状评估

> 结论先行:**引擎内核质量良好,症结在使用模型。** 工作单元被建模成 `account × platform × module` 三元组,且全部由数据库表驱动,与「快速起一个采集集群」的目标直接冲突。

## mocra 今天是什么

mocra 把采集建模为一条**队列解耦的处理管线**,每个阶段是一个有界并发的 worker 循环,阶段之间用消息队列衔接:

```
TaskEvent ──▶ generate() ──▶ [Request Q] ──▶ download() ──▶ [Response Q] ──▶ parse()
                                                                                │
                                              ┌──────────────┬──────────────────┤
                                              ▼              ▼                  ▼
                                         [数据落地]    [下一节点/Parser Q]   [错误 Q → DLQ]
```

队列后端在**本地 Tokio channel / Redis Streams / Kafka** 之间按配置自动切换——同一份代码,从单进程平滑扩展到 N 节点集群。这是它最有价值的设计。

用户侧抽象是**模块**(`ModuleTrait`)与**节点**(`ModuleNodeTrait`):

- `ModuleNodeTrait::generate()` → 产出 `Stream<Request>`
- `ModuleNodeTrait::parser()` → 消费 `Response`,产出 `TaskOutputEvent`(数据 + 后继任务)
- 多节点可编成线性链(`add_step`)或自定义扇出/汇合 DAG(`dag_definition`)

此外内置:Cron 调度、Prometheus 指标、控制面 HTTP API(`/start_work`、`/health`、`/metrics`、暂停/恢复、DLQ)、熔断/重试/死信队列。

### 规模

| 指标 | 值 |
|---|---|
| 版本 / edition | v0.2.16 · Rust 2024 · rust 1.85 |
| 结构 | 单 crate(原 workspace,现用 `#[path]` 内嵌模块) |
| 代码量 | ~49k LOC · ~180 文件 |
| 公共项 | 约 2214 个 `pub` |

## 核心问题:为什么现在跑不起来一个 Hello World

文档里的「最小示例」实际无法运行。真正的启动路径与文档承诺之间隔着一整套数据库耦合。

**文档承诺(不成立):**

```rust
let state  = State::new("config.toml").await;
let engine = Engine::new(state, None).await;
engine.register_module(MyModule::default_arc()).await;
engine.run().await;   // ← run() 根本不存在,实际是 start()
```

**实际要跑起来还需要(文档只字未提):**

1. 一个 SQLite / Postgres 库 + `base` schema:`module` / `account` / `platform` / `rel_module_account` / `rel_module_platform` / `rel_account_platform` … 7+ 张表;
2. 手动写入 `account` / `platform` 行;
3. 代码模块 `name()` + `version()` 必须与库中 `module` 行对齐;
4. 注入 `TaskEvent { account, platform, module }`——否则 `register` 之后引擎只是空转。

工作单元 `TaskEvent { account, platform, module, run_id }` 的每一维都要在库里存在。`register_module()` 只登记内存中的代码,任务工厂(`TaskFactory`)却从数据库按 `account × platform` 关系加载模块——两边对不上就报 `ModuleNotFound`。即便「纯引擎」单机模式,`db.url` 为空也会在 `State` 初始化时直接失败退出(`db_connection` 对 `None` 返回 `None` → `StateInitError::DatabaseConnect`)。

> 这套模型服务于「多租户 RPA 平台」(为账号 A 在平台 B 上跑模块 X),很强大——但它是**每个用户的第一道门槛**,而非选配。

## 阻碍清单

按对「简单易用 + 快速起集群」目标的阻碍程度排序。均已在源码中逐条核实。

| 问题 | 严重度 | 影响 | 关键位置 |
|---|---|---|---|
| **数据库强制耦合** —— account/platform/module 三元组由 7+ 张表驱动,单机模式也必须连库 | 🔴 阻断 | 无法「写个解析器就跑」 | `common/state.rs`、`engine/task/factory.rs`、`repository.rs` |
| **数据出口只能走中间件 + DB 路由** —— 无 `on_item`/sink 回调;要拿数据必须实现 `DataStoreMiddleware` 并经库中 relation 挂载 | 🔴 阻断 | 数据无法简单送进用户的 DB/Kafka/channel | `common/interface/middleware.rs`、`engine/chain/parser_chain.rs` |
| **`State` 上帝对象 + 库内 `process::exit`** —— `Arc<State>` 遍布 30 处;初始化失败直接 `exit(1)` | 🟠 高 | 难测试、难裁剪、难嵌入 | `common/state.rs:76,96` |
| **构造仪式繁琐 + 必须 TOML 文件** —— State→Engine→register→start 四步;14 段配置多为必填、无默认、无程序化构建器 | 🟠 高 | 无「三行起步」路径 | `common/model/config.rs`(`Config` 14 段) |
| **重型依赖全部强制编译** —— polars / calamine / rdkafka(cmake) / sea-orm(pg+sqlite) / reqwest(13 特性) 均非可选;`polars` 特性还是空壳 no-op | 🟠 高 | 拉进依赖 = 一次沉重编译 | `Cargo.toml` |
| **公共 API 边界失控** —— 2214 个 `pub`,几乎无 pub/internal 分界;prelude 不完整 | 🟡 中 | 无法演进内部而不破坏用户;语义版本无从谈起 | `src/**`、`prelude.rs` |
| **DAG 执行有两三套并存** —— 通用 `schedule/dag` + `module_dag_processor` + `module_processor_with_chain`,门控逻辑重复,含占位空壳 adapter | 🟡 中 | 维护困惑、行为不一,内核最大技术债 | `engine/task/module_dag_*`、`module_processor_with_chain.rs` |
| **文档/示例与代码漂移,无 CI/workspace** —— tests 是独立 crate;启用示例引用已删除的 `Data`/`ParserData` 类型 | 🟡 中 | 示例编译不过也没人拦 | `tests/`、无 `.github/workflows` |

## 子系统质量地图

好消息:多个子系统本就干净、与爬虫无关,天然适合抽取。重构以「抽取 / 加薄层」为主。

| 子系统 | 复用度 | 说明 |
|---|---|---|
| `schedule/dag/`(通用分布式 DAG 引擎) | ⭐⭐⭐⭐⭐ | 零爬虫耦合,现成可独立成 crate。公共 API:`Dag` / `DagScheduler` / `TaskPayload` / `DagError` |
| `sync/`(协调层) | ⭐⭐⭐⭐⭐ | `CoordinationBackend` trait 干净,`SyncService` / `LeaderElector` 已与引擎解耦 |
| `proxy/`(代理池) | ⭐⭐⭐⭐ | `ProxyManager` / `ProxyPool` 配置驱动、零 `State` 依赖(85% 现成) |
| `queue/`(队列) | ⭐⭐⭐⭐ | `MqBackend` trait 干净;但 `Channel` 硬编码 22 个爬虫专用通道 → 应转内部私有 |
| `downloader/`(下载器) | ⭐⭐⭐ | `Downloader` trait 干净;`DownloaderManager` 需要 `State`(可解耦) |
| `engine/chain/`(处理链) | ⭐⭐⭐ | `processor_chain.rs` 2300 行,复杂但结构清晰;具体链与 `State` 紧耦合 |
| `engine/task/module_dag_*` | ⚠️ | 占位/冗余,与 `module_processor_with_chain` 重复,待合并 |

## 关键抽象缝(后续重构的接入点)

这三个 trait 是「加实现而非重写」的支点:

```rust
// sync/backend.rs —— 控制面协调(将由 redb+Raft 实现)
trait CoordinationBackend {
    async fn publish/subscribe(...);   // pub-sub
    async fn set/get(...);             // KV
    async fn cas(...);                 // 乐观锁(线性一致)
    async fn acquire_lock/renew_lock(...);  // 分布式锁
}

// queue/lib.rs —— 数据面传输(保留多后端)
trait MqBackend {
    async fn publish_with_headers(...);
    async fn subscribe(topic, sender);  // Message 自带 ack 回调
    async fn send_to_dlq / read_dlq(...);
}

// cacheable —— 缓存后端(已抽象:local / redis / two-level)
trait CacheBackend { ... }
```

**目前仅两处直吃 `deadpool_redis::Pool`、未走 trait**:`DistributedLockManager`(`utils/redis_lock.rs`)与限流器(`utils/distributed_rate_limit.rs`)。这两处需要 trait 化——是明确、有界的重构。

---

下一篇:[02 · 目标 API](02-target-api.md)
