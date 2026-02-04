> 已合并至 `docs/README.md`，本文件保留为历史参考。

# 分布式定时任务启动设计方案

## 1. 需求背景 (Background)

当前 Mocra 框架支持基于队列 (Queue) 的任务驱动模式，但缺乏内置的定时触发机制。为了支持周期性任务（如：每日定时抓取、心跳检测、定时清理等），需要在 `ModuleTrait` 中引入定时配置 (`CronConfig`)。

由于 Mocra 是分布式部署的，多个 Engine 节点可能同时运行相同的模块。因此，必须保证定时任务在 **整个集群中仅触发一次**，避免重复执行。

**注意：当前阶段的定时任务仅需支持分钟级精度（秒字段固定为 0）。**

## 2. 核心设计目标 (Objectives)

1.  **配置化**: 在 `ModuleTrait` 中定义 Cron 表达式，无需额外编写调度代码，并提供便捷的构建方法。
2.  **分布式互斥**: 无论部署多少个 Engine 节点，同一任务在同一时间点只能产生一条消息。
3.  **高可用**: 任何节点的宕机不应影响定时任务的触发（只要有至少一个节点存活）。
4.  **无状态**: 调度器不依赖本地持久化状态，仅依赖 Redis 和系统时钟。

## 3. 总体架构 (Architecture)

将在 `engine` crate 中引入一个新的组件 `CronScheduler`。该组件作为后台任务运行，周期性扫描所有活跃的 Module 实例，检查是否满足 Cron 触发条件。如果满足，则通过 Redis 分布式锁竞争执行权，胜者负责构造 `TaskModel` 并推送到 Queue。

### 3.1 模块配置扩展 (`ModuleTrait`)

在 `common` crate 中引入 `CronConfig` 并更新 `ModuleTrait`。

**Struct Definition:**

```rust
// common/src/model/cron_config.rs (新建)
use cron::Schedule;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct CronConfig {
    pub schedule: Schedule,
    /// 保留原始表达式用于日志记录和调试
    pub expression: String,
    pub enable: bool,
}

pub enum CronInterval {
    Minute(u32),    // 每 n 分钟
    Hour(u32),      // 每 n 小时
    Day(u32),       // 每 n 天
    Week(Vec<u32>), // 每周的指定几天 (0=Sun, 1=Mon...6=Sat)
    Month(u32),     // 每 n 月
    Custom(String), // 自定义 Cron 表达式
}

impl CronConfig {
    /// 基础构造函数，支持直接传入 Cron 表达式
    /// 示例: CronConfig::new("0 0 12 * * *")
    /// 注意：如果表达式无效，此处会 Panic。建议在模块加载阶段尽早调用以暴露错误。
    pub fn new(expression: impl Into<String>) -> Self {
        let expr = expression.into();
        let schedule = Schedule::from_str(&expr).expect("Invalid Cron expression");
        Self {
            schedule,
            expression: expr,
            enable: true,
        }
    }

    /// Fluent Builder 入口
    /// 示例: CronConfig::every(CronInterval::Day(1)).at(10, 30) // 每天 10:30
    pub fn every(interval: CronInterval) -> CronBuilder {
        CronBuilder::new(interval)
    }
}

pub struct CronBuilder {
    interval: CronInterval,
    hour: u32,
    minute: u32,
    day_of_month: Option<u32>,
}

impl CronBuilder {
    pub fn new(interval: CronInterval) -> Self {
        Self {
            interval,
            hour: 0,
            minute: 0,
            day_of_month: None,
        }
    }

    /// 设置小时 (0-23)
    pub fn at_hour(mut self, hour: u32) -> Self {
        self.hour = hour;
        self
    }
    
    /// 设置分钟 (0-59)
    pub fn at_minute(mut self, minute: u32) -> Self {
        self.minute = minute;
        self
    }
    
    /// 复合设置时间: .at(10, 30) -> 10:30
    pub fn at(mut self, hour: u32, minute: u32) -> Self {
        self.hour = hour;
        self.minute = minute;
        self
    }
    
    /// 针对 Monthly 任务，设置几号 (1-31)
    pub fn on_day(mut self, day: u32) -> Self {
        self.day_of_month = Some(day);
        self
    }

    /// 构建 CronConfig
    /// 自动生成秒级为 0 的 Cron 表达式
    pub fn build(self) -> CronConfig {
        let expr = match self.interval {
            CronInterval::Minute(n) => {
                format!("0 */{} * * * *", n)
            }
            CronInterval::Hour(n) => {
                format!("0 {} */{} * * *", self.minute, n)
            }
            CronInterval::Day(n) => {
                format!("0 {} {} */{} * *", self.minute, self.hour, n)
            }
            CronInterval::Week(days) => {
                let days_str = if days.is_empty() {
                    "*".to_string()
                } else {
                    days.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(",")
                };
                format!("0 {} {} * * {}", self.minute, self.hour, days_str)
            }
            CronInterval::Month(n) => {
                let day = self.day_of_month.unwrap_or(1);
                format!("0 {} {} {} */{} *", self.minute, self.hour, day, n)
            }
            CronInterval::Custom(expr) => expr,
        };
        CronConfig::new(expr)
    }
}
```

**Trait Update:**

```rust
// common/src/interface/module.rs

pub trait ModuleTrait: Send + Sync {
    // ... 原有方法 ...

    /// 返回该模块的定时调度配置
    /// 默认为 None (不启用定时启动)
    fn cron(&self) -> Option<CronConfig> {
        None
    }
}
```

### 3.2 调度器设计 (`CronScheduler`)

`CronScheduler` 将作为 `Engine` 的子模块运行。它可以利用 `engine` 中已有的 `ModuleProcessor` 或 `ModuleManager` 来获取当前所有已加载的 Module 实例。

#### 核心逻辑

1.  **轮询**: 调度器每秒（或每 5 秒）唤醒一次。
2.  **遍历**: 遍历内存中所有已初始化的 `Module` 对象。
    *   注意：`Module` 包含了 `ModuleTrait` 的逻辑代码以及 `Account` 和 `Platform` 的具体配置。
    *   这意味着如果同一个模块配置了 10 个账号，会有 10 个 Module 实例。Cron 对它们分别生效（如果这是预期行为）。
    *   *设计决策*: Cron 通常是"业务逻辑"层面的。如果希望它是全局单例（不区分账号），需要在配置中区分。但鉴于 `TaskModel` 需要明确的 `account`，我们默认 **Cron 是针对 Module 实例（即特定账号/平台）生效的**。
3.  **时间检查**:
    *   直接使用 `config.schedule` (`cron::Schedule`) 进行计算。
    *   计算 `CreateTime`：即该 Cron 表达式相对于当前时间最近的一次**过去**触发点。
    *   **取整处理**: 鉴于精度限制，将 `CreateTime` 的秒数归零（truncate to minute），确保不同节点计算出的时间戳一致。
    *   过滤：如果 `CreateTime` 过于久远（例如超过 1 分钟），则忽略（作为错过的任务处理，或者直接跳过）。
4.  **分布式竞争 (Distributed Lock)**:
    *   构造唯一的锁 Key：
        `cron_lock:{module_name}:{account}:{platform}:{trigger_timestamp}`
    *   `trigger_timestamp` 是 `CreateTime` (分钟级) 的时间戳。这确保了集群中所有节点对于"这一次任务" (例如 "2023-10-27 10:00:00") 达成共识。
    *   使用 Redis `SET NX EX` 尝试加锁。TTL 设置为 60s（防止死锁，同时覆盖该分钟的执行窗口）。
5.  **任务分发**:
    *   如果获取锁成功：表示本节点是该时间点的主节点。
    *   构造 `TaskModel`:
        *   `module`: `self.module.name()`
        *   `account`: `self.account.name`
        *   `platform`: `self.platform.name`
        *   `run_id`: `Uuid::new_v7()`
    *   调用 `Queue::push` 发送任务。
    *   记录日志: "Triggered cron task for ..."

### 3.3 时钟同步与边缘情况

*   **时钟偏移**: 节点的系统时间必须大致同步（NTP）。如果偏移过大，不同节点计算出的 `CreateTime` 可能不一致（例如一个算的是这一分钟，另一个算的是下一分钟），会导致锁 Key 不同，从而重复执行。
    *   *缓解*: 调度检查窗口应覆盖过去 60s 的触发点。锁定 Key 使用精确的触发时间点（cron 计算出的时间），而不是当前时间。只要 Cron 解析稳定，时间点就是确定的。
*   **重复检查**: 轮询频率必须高于 Cron 最小精度。如果 Cron 是每秒执行，轮询必须每秒进行（或更高频）。建议默认支持分钟级 Cron，轮询间隔 10s-30s。若需秒级，需提高轮询频率。

## 4. 详细实施步骤

### Step 1: 引入依赖
在 `common` 和 `engine` 的 `Cargo.toml` 中添加 `cron` crate。

```toml
[dependencies]
cron = "0.15"
```

### Step 2: 定义数据结构
在 `common/src/model/` 下创建 `cron.rs` (或在 `config.rs` 中添加)，定义 `CronConfig`。

并在 `ModuleTrait` 中添加钩子方法。

### Step 3: 实现调度器 (`engine`)
在 `engine/src/scheduler` (新建目录) 中实现 `CronScheduler`.

**注意**: 所有时间计算统一使用 **UTC**，避免跨时区部署导致的不一致。

```rust
// 伪代码示例
pub struct CronScheduler {
    modules: Vec<Arc<Module>>, // 引用 Engine 中的模块列表
    redis_pool: Pool, // 用于分布式锁
    queue: Arc<dyn Queue>, // 用于发送任务
}

impl CronScheduler {
    pub async fn run(&self) {
        loop {
            let now = Utc::now();
            for module in &self.modules {
                if let Some(cfg) = module.module.cron() {
                    self.check_and_trigger(module, cfg, now).await;
                }
            }
            sleep(Duration::from_secs(10)).await;
        }
    }
    
    async fn check_and_trigger(&self, module: &Module, cfg: CronConfig, now: DateTime<Utc>) {
        // 直接使用预解析好的 Schedule 对象
        let schedule = &cfg.schedule;
        
        // 获取最近一次应该执行的时间
        // 这里查找过去 1 分钟内的触发点
        let check_window_start = now.checked_sub_signed(Duration::minutes(1)).unwrap();
        
        if let Some(prev) = schedule.after(&check_window_start).next() {
            // 如果计算出的下一次时间在当前时间之前（或刚好是现在），说明是应该执行的过去时间点
            if prev <= now {
                // 构造锁 Key (带命名空间，避免多实例冲突)
                let key = format!("{}:cron:{}:{}:{}:{}", config.name, module.id(), prev.timestamp());
                
                // 尝试加锁
                if self.try_lock(key).await {
                    self.send_task(module).await;
                }
            }
        }
    }
}
```

### Step 4: 集成到 Engine 启动流程
在 `engine/src/engine.rs` 的启动逻辑中，初始化 `CronScheduler` 并通过 `tokio::spawn` 启动其主循环。我们需要确保它能访问到所有加载的 `Module` 实例以及 `redis` 和 `queue` 句柄。

> **性能优化建议**: 由于 `ModuleTrait::cron()` 可能会重新构建 `CronConfig`（涉及字符串解析），建议在 `Module` 结构体（`engine/src/task/module.rs`）初始化时，将 `CronConfig` 解析并缓存到 `Module` 实例中，或者在 `CronScheduler` 内部维护一个 `HashMap<ModuleId, CronConfig>` 缓存，避免每秒重复解析造成的 CPU 浪费。

## 5. 依赖检查

*   **Redis**: 现有 `utils::redis_lock` 或直接使用 `deadpool-redis` 均可支持。
*   **Queue**: `queue` crate 已经提供了推送接口。
*   **ModuleRegistry**: Engine 需要暴露当前持有的 `Module` 列表给 Scheduler。目前的架构中 Engine 应该持有 `Vec<Arc<Module>>` 或类似的 Map，需要确保 Scheduler 能读取它。

## 6. 总结

该方案利用 Redis 的原子性操作解决了分布式环境下的任务去重问题，通过 `trait` 扩展实现了低侵入性的配置。

**优点**:
*   简单可靠，无中心主节点 (Leaderless)，避免了选主逻辑的复杂性。
*   利用 Redis NX 特性，天然实现互斥。
*   与现有 `TaskModel` 和 `Queue` 机制完全解耦。

**缺点**:
*   依赖系统时钟同步。
*   如果 Cron 频率极高（如每秒），Redis 压力会增大（频繁加锁）。但对于爬虫场景（通常分钟/小时级），开销可忽略不计。
