use cron::Schedule;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct CronConfig {
    pub schedule: Schedule,
    /// 保留原始表达式用于日志记录和调试
    pub expression: String,
    pub enable: bool,
    pub right_now: bool,
    pub run_now_and_schedule: bool,
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
            right_now: false,
            run_now_and_schedule: false,
        }
    }

    /// 立即执行一次，不经过 cron 调度
    pub fn right_now() -> Self {
        let config = Self {
            schedule: Schedule::from_str("0 0 0 1 1 ? *").unwrap(), // 永远不会触发的表达式
            expression: "right_now".to_string(),
            enable: true,
            right_now: true,
            run_now_and_schedule: false,
        };
        config
    }

    /// 立即执行一次，后续继续按 cron 调度
    pub fn run_now_and_schedule(mut self) -> Self {
        self.run_now_and_schedule = true;
        self.right_now = false;
        self
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
    run_now_and_schedule: bool,
}

impl CronBuilder {
    pub fn new(interval: CronInterval) -> Self {
        Self {
            interval,
            hour: 0,
            minute: 0,
            day_of_month: None,
            run_now_and_schedule: false,
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

    /// 立即执行一次，后续继续按 cron 调度
    pub fn run_now_and_schedule(mut self) -> Self {
        self.run_now_and_schedule = true;
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
        let mut config = CronConfig::new(expr);
        config.run_now_and_schedule = self.run_now_and_schedule;
        config
    }
}
