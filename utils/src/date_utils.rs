#![allow(unused)]
use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime, TimeZone};
use regex::Regex;
/// Date utilities mirroring the provided Python DateUtils behavior, using chrono.
/// All functions accept `Option<NaiveDate>` where `None` means "today" in local time.
pub struct DateUtils;

impl DateUtils {
    #[inline]
    pub fn ensure_date(day: Option<NaiveDate>) -> NaiveDate {
        day.unwrap_or_else(|| Local::now().date_naive())
    }

    /// First day of the previous month.
    pub fn last_month(day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        let first_this_month = NaiveDate::from_ymd_opt(d.year(), d.month(), 1).unwrap();
        let prev_day = first_this_month - Duration::days(1);
        NaiveDate::from_ymd_opt(prev_day.year(), prev_day.month(), 1).unwrap()
    }

    /// First day of current month.
    pub fn first_day_of_month(day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        NaiveDate::from_ymd_opt(d.year(), d.month(), 1).unwrap()
    }

    /// First day of the next month.
    pub fn next_month(day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        let last = Self::last_day_of_month(Some(d));
        last + Duration::days(1)
    }

    /// Last day (date) of current month.
    pub fn last_day_of_month(day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        // First day of next month then minus one day
        let (ny, nm) = if d.month() == 12 {
            (d.year() + 1, 1)
        } else {
            (d.year(), d.month() + 1)
        };
        let first_next_month = NaiveDate::from_ymd_opt(ny, nm, 1).unwrap();
        first_next_month - Duration::days(1)
    }

    /// Monday of the previous week.
    pub fn last_week(day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        let monday = Self::first_day_of_week(Some(d));
        monday - Duration::days(7)
    }

    /// Monday of the next week.
    pub fn next_week(day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        let monday = Self::first_day_of_week(Some(d));
        monday + Duration::days(7)
    }

    /// Monday of the current week.
    pub fn first_day_of_week(day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        let offset = d.weekday().num_days_from_monday() as i64; // 0..=6
        d - Duration::days(offset)
    }

    /// Sunday of the current week.
    pub fn last_day_of_week(day: Option<NaiveDate>) -> NaiveDate {
        let monday = Self::first_day_of_week(day);
        monday + Duration::days(6)
    }

    /// First day of the month after adding `n` months (can be negative).
    /// Matches the Python logic that always aligns to the first day of the month.
    pub fn add_month(n: i32, day: Option<NaiveDate>) -> NaiveDate {
        let mut d = Self::first_day_of_month(day);
        if n == 0 {
            return d;
        }

        if n > 0 {
            for _ in 0..n {
                // move to first of next month
                d = Self::next_month(Some(d));
            }
        } else {
            for _ in 0..(-n) {
                // go to previous month's first day
                let prev_end = d - Duration::days(1);
                d = NaiveDate::from_ymd_opt(prev_end.year(), prev_end.month(), 1).unwrap();
            }
        }
        d
    }

    /// Monday of the week after adding `n` weeks (can be negative).
    pub fn add_week(n: i32, day: Option<NaiveDate>) -> NaiveDate {
        let monday = Self::first_day_of_week(day);
        monday + Duration::days(7 * n as i64)
    }

    /// First day of the year after adding `n` years (can be negative).
    pub fn add_year(n: i32, day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        let y = d.year() + n;
        NaiveDate::from_ymd_opt(y, 1, 1).unwrap()
    }

    /// Whether the provided day is the last day of its month.
    pub fn is_last_day_month(day: Option<NaiveDate>) -> bool {
        let d = Self::ensure_date(day);
        d == Self::last_day_of_month(Some(d))
    }
    pub fn add_days(n: i64, day: Option<NaiveDate>) -> NaiveDate {
        let d = Self::ensure_date(day);
        d + Duration::days(n)
    }
    // 返回的是不带时区的时间戳  即utc+0，如果需要带时区的时间戳，需要在此基础上加上时区偏移量
    pub fn local_ts(day: Option<NaiveDate>) -> i64 {
        // 返回“无时区”的时间戳：直接以 1970-01-01 00:00:00（Naive）为基准，
        // 计算该日期 00:00:00（Naive）到 epoch 的毫秒差，不做任何时区偏移。
        let d = Self::ensure_date(day);
        let ndt: NaiveDateTime = d.and_time(chrono::NaiveTime::MIN);
        let epoch: NaiveDateTime = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        ndt.signed_duration_since(epoch).num_milliseconds()
    }
    pub fn range(start_date:NaiveDate, end_date:NaiveDate) -> Vec<NaiveDate> {
        let mut dates = Vec::new();
        let mut current_date = start_date;
        while current_date <= end_date {
            dates.push(current_date);
            current_date = current_date + Duration::days(1);
        }
        dates
    }
}



pub fn convert_to_seconds(s:&str)->Option<u64>{
    if s.trim().is_empty() {
        return Some(0);
    }
    let trimmed = s.trim();
    if trimmed == "--" {
        return Some(0);
    }

    // Handle colon formats like HH:MM:SS or MM:SS
    if trimmed.contains(':') {
        let parts: Vec<&str> = trimmed.split(':').collect();
        let all_digits = parts
            .iter()
            .all(|p| p.trim().chars().all(|c| c.is_ascii_digit()));
        if all_digits {
            let nums: Vec<u64> = parts
                .iter()
                .map(|p| p.trim().parse::<u64>().unwrap_or(0))
                .collect();
            let (h, m, sec) = match nums.len() {
                3 => (nums[0], nums[1], nums[2]),
                2 => (0, nums[0], nums[1]),
                1 => (0, 0, nums[0]),
                _ => return None,
            };
            return Some(
                h.saturating_mul(3600)
                    .saturating_add(m.saturating_mul(60))
                    .saturating_add(sec),
            );
        }
    }

    // Normalize Chinese units to h/m/s
    let mut normalized = trimmed
        .replace("小时", "h")
        .replace("时", "h")
        .replace("分钟", "m")
        .replace("分", "m")
        .replace("秒钟", "s")
        .replace("秒", "s");

    // Remove whitespace
    let ws = Regex::new(r"\s+").unwrap();
    normalized = ws.replace_all(&normalized, "").to_string();

    // Sum numbers by unit
    let re = Regex::new(r"(\d+)\s*([hms])").unwrap();
    let mut total: u64 = 0;
    let mut matched = false;
    for cap in re.captures_iter(&normalized) {
        matched = true;
        let n: u64 = cap.get(1).and_then(|m| m.as_str().parse().ok()).unwrap_or(0);
        match cap.get(2).map(|m| m.as_str()).unwrap_or("") {
            "h" => total = total.saturating_add(n.saturating_mul(3600)),
            "m" => total = total.saturating_add(n.saturating_mul(60)),
            "s" => total = total.saturating_add(n),
            _ => {}
        }
    }
    if matched { Some(total) } else { None }
}



#[cfg(test)]
mod tests {
    use super::*;
    use chrono::format::Fixed::TimezoneOffset;
    use chrono::{FixedOffset, TimeZone};
    use std::ops::Sub;

    #[test]
    fn test_convert_to_seconds() {
        let s = "17分23秒";
        println!("{:?}",convert_to_seconds(s));
    }


    #[test]
    fn test_first_and_last_day_of_month() {
        let d = NaiveDate::from_ymd_opt(2025, 8, 28).unwrap();
        assert_eq!(
            DateUtils::first_day_of_month(Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 1).unwrap()
        );
        assert_eq!(
            DateUtils::last_day_of_month(Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 31).unwrap()
        );
    }

    #[test]
    fn test_next_and_last_month() {
        let d = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
        assert_eq!(
            DateUtils::next_month(Some(d)),
            NaiveDate::from_ymd_opt(2025, 2, 1).unwrap()
        );
        assert_eq!(
            DateUtils::last_month(Some(d)),
            NaiveDate::from_ymd_opt(2024, 12, 1).unwrap()
        );
    }

    #[test]
    fn test_week_helpers() {
        // 2025-08-28 is Thursday; week Mon=2025-08-25, Sun=2025-08-31
        let d = NaiveDate::from_ymd_opt(2025, 8, 28).unwrap();
        assert_eq!(
            DateUtils::first_day_of_week(Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 25).unwrap()
        );
        assert_eq!(
            DateUtils::last_day_of_week(Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 31).unwrap()
        );
        assert_eq!(
            DateUtils::last_week(Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 18).unwrap()
        );
        assert_eq!(
            DateUtils::next_week(Some(d)),
            NaiveDate::from_ymd_opt(2025, 9, 1).unwrap()
        );
    }

    #[test]
    fn test_add_month_week_year() {
        let d = NaiveDate::from_ymd_opt(2025, 8, 28).unwrap();
        assert_eq!(
            DateUtils::add_month(0, Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 1).unwrap()
        );
        assert_eq!(
            DateUtils::add_month(1, Some(d)),
            NaiveDate::from_ymd_opt(2025, 9, 1).unwrap()
        );
        assert_eq!(
            DateUtils::add_month(-1, Some(d)),
            NaiveDate::from_ymd_opt(2025, 7, 1).unwrap()
        );

        assert_eq!(
            DateUtils::add_week(0, Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 25).unwrap()
        );
        assert_eq!(
            DateUtils::add_week(1, Some(d)),
            NaiveDate::from_ymd_opt(2025, 9, 1).unwrap()
        );
        assert_eq!(
            DateUtils::add_week(-1, Some(d)),
            NaiveDate::from_ymd_opt(2025, 8, 18).unwrap()
        );

        assert_eq!(
            DateUtils::add_year(0, Some(d)),
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()
        );
        assert_eq!(
            DateUtils::add_year(1, Some(d)),
            NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
        );
        assert_eq!(
            DateUtils::add_year(-1, Some(d)),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
    }

    #[test]
    fn test_is_last_day_month() {
        let d1 = NaiveDate::from_ymd_opt(2025, 2, 28).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        assert!(DateUtils::is_last_day_month(Some(d1)));
        assert!(DateUtils::is_last_day_month(Some(d2)));
        let d3 = NaiveDate::from_ymd_opt(2025, 8, 30).unwrap();
        assert!(!DateUtils::is_last_day_month(Some(d3)));
    }
    #[test]
    fn test_add_days() {
        let date = DateUtils::add_days(-1, None);
        let start_date = DateUtils::first_day_of_week(Some(DateUtils::first_day_of_month(None)))
            .sub(chrono::Duration::days(1));
        let end_date = DateUtils::last_day_of_week(Some(DateUtils::last_day_of_month(None)))
            .sub(chrono::Duration::days(1));
        let tz = FixedOffset::east_opt(8 * 3600).unwrap();
        let start_ts = tz
            .from_local_datetime(&start_date.and_time(chrono::NaiveTime::MIN))
            .single()
            .unwrap()
            .timestamp_millis();
        let end_ts = tz
            .from_local_datetime(
                &((end_date + Duration::days(1)).and_time(chrono::NaiveTime::MIN)),
            )
            .single()
            .unwrap()
            .timestamp_millis()
            - 1;
        println!("{:?}", date);
        println!("{:?}", start_ts);
        println!("{:?}", end_ts);
        assert_eq!(start_ts, 1758988800000);
        assert_eq!(end_ts, 1762012799999);
    }
    #[test]
    fn test_local_ts() {
        let t =
        chrono::DateTime::from_timestamp(1761062400,0).unwrap().with_timezone(&Local).naive_local();
        println!("{:?}", t);
    }

    #[test]
    fn range(){
        let start_date = DateUtils::add_days(-31,None);
        let end_date = DateUtils::add_days(-1,None);
        let range = DateUtils::range(start_date,end_date);
        println!("{:#?}", range);
    }
}
