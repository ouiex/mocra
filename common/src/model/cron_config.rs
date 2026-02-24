use cron::Schedule;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct CronConfig {
    pub schedule: Schedule,
    /// Keeps original expression for logging and debugging.
    pub expression: String,
    pub enable: bool,
    pub right_now: bool,
    pub run_now_and_schedule: bool,
}

pub enum CronInterval {
    Minute(u32),    // Every n minutes.
    Hour(u32),      // Every n hours.
    Day(u32),       // Every n days.
    Week(Vec<u32>), // Specific weekdays (0=Sun, 1=Mon...6=Sat).
    Month(u32),     // Every n months.
    Custom(String), // Custom cron expression.
}

impl CronConfig {
    /// Base constructor that accepts a raw cron expression.
    /// Example: `CronConfig::new("0 0 12 * * *")`.
    /// Note: invalid expressions panic here; call early during module startup.
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

    /// Executes once immediately, without cron scheduling.
    pub fn right_now() -> Self {
        let config = Self {
            schedule: Schedule::from_str("0 0 0 1 1 ? *").unwrap(), // Expression that never triggers.
            expression: "right_now".to_string(),
            enable: true,
            right_now: true,
            run_now_and_schedule: false,
        };
        config
    }

    /// Executes once immediately, then continues cron scheduling.
    pub fn run_now_and_schedule(mut self) -> Self {
        self.run_now_and_schedule = true;
        self.right_now = false;
        self
    }

    /// Fluent builder entrypoint.
    /// Example: `CronConfig::every(CronInterval::Day(1)).at(10, 30)` (daily at 10:30).
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

    /// Sets hour (`0-23`).
    pub fn at_hour(mut self, hour: u32) -> Self {
        self.hour = hour;
        self
    }
    
    /// Sets minute (`0-59`).
    pub fn at_minute(mut self, minute: u32) -> Self {
        self.minute = minute;
        self
    }
    
    /// Combined time setter: `.at(10, 30)` -> `10:30`.
    pub fn at(mut self, hour: u32, minute: u32) -> Self {
        self.hour = hour;
        self.minute = minute;
        self
    }
    
    /// Sets day of month (`1-31`) for monthly schedules.
    pub fn on_day(mut self, day: u32) -> Self {
        self.day_of_month = Some(day);
        self
    }

    /// Executes once immediately, then continues cron scheduling.
    pub fn run_now_and_schedule(mut self) -> Self {
        self.run_now_and_schedule = true;
        self
    }

    /// Builds `CronConfig`.
    /// Automatically generates cron expression with second field fixed at `0`.
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
