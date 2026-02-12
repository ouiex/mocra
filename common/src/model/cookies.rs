use async_trait::async_trait;
use cacheable::CacheAble;
use chrono::{TimeZone, Utc};
use errors::CookieError;
use reqwest::cookie::Jar;
use reqwest::header::HeaderValue;
use reqwest_cookie_store::CookieStore;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;
use std::fmt;

/// cookie由外部来保证是否有效，这里只做简单的序列化和反序列化，不验证有效性
#[derive(Serialize, Clone)]
pub struct CookieItem {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    // Unix timestamp seconds; accept i64/f64 on input, store as u64 seconds (ms inputs will be converted to seconds)
    pub expires: Option<u64>,
    // Seconds; accept numeric forms, store as u64 seconds
    pub max_age: Option<u64>,
    pub secure: bool,
    #[serde(rename = "httpOnly")]
    pub http_only: Option<bool>,
}

impl fmt::Debug for CookieItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
         f.debug_struct("CookieItem")
            .field("name", &self.name)
            .field("value", &"***REDACTED***")
            .field("domain", &self.domain)
            .field("path", &self.path)
            .field("expires", &self.expires)
            .field("max_age", &self.max_age)
            .field("secure", &self.secure)
            .field("http_only", &self.http_only)
            .finish()
    }
}

impl<'de> Deserialize<'de> for CookieItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawItem {
            name: Option<String>,
            value: Option<String>,
            domain: Option<String>,
            path: Option<String>,
            // 我们自己支持两种来源：expires(秒) 或 expirationDate(字符串/数字)
            // 允许 number/string/null；统一解析为秒
            expires: Option<serde_json::Value>,
            #[serde(rename = "expirationDate")]
            expiration_date: Option<serde_json::Value>,
            max_age: Option<serde_json::Value>,
            secure: Option<bool>,
            #[serde(rename = "httpOnly")]
            http_only: Option<bool>,
            // 兼容 max-age / maxAge 两种写法
            #[serde(rename = "max-age")]
            max_age_dash: Option<serde_json::Value>,
            #[serde(rename = "maxAge")]
            max_age_camel: Option<serde_json::Value>,
        }

        fn parse_expiration_value(v: &serde_json::Value) -> Option<f64> {
            match v {
                serde_json::Value::Null => None,
                serde_json::Value::Number(n) => n.as_f64().map(|u| {
                    // 处理毫秒时间戳（大于等于 1e12 视为毫秒）
                    if u.abs() >= 1_000_000_000_000.0 {
                        u / 1000.0
                    } else {
                        u
                    }
                }),
                serde_json::Value::String(s) => parse_expiration_date_str(s),
                _ => None,
            }
        }

        // 简单解析 "YYYY/M/D HH:MM:SS" 或 "YYYY-MM-DD HH:MM:SS" 到秒
        fn parse_expiration_date_str(s: &str) -> Option<f64> {
            let (date_part, time_part) = s.split_once(' ')?;

            let mut hms = time_part.split(':');
            let hh = hms.next()?.parse::<u32>().ok()?;
            let mm = hms.next()?.parse::<u32>().ok()?;
            let ss = hms.next()?.parse::<u32>().ok()?;

            let (y, mo, d) = if date_part.contains('/') {
                let mut it = date_part.split('/');
                (
                    it.next()?.parse::<i32>().ok()?,
                    it.next()?.parse::<u32>().ok()?,
                    it.next()?.parse::<u32>().ok()?,
                )
            } else if date_part.contains('-') {
                let mut it = date_part.split('-');
                (
                    it.next()?.parse::<i32>().ok()?,
                    it.next()?.parse::<u32>().ok()?,
                    it.next()?.parse::<u32>().ok()?,
                )
            } else {
                return None;
            };

            Utc.with_ymd_and_hms(y, mo, d, hh, mm, ss)
                .single()
                .map(|dt| dt.timestamp().max(0) as f64)
        }

        let raw = RawItem::deserialize(deserializer)?;
        let name = raw
            .name
            .ok_or_else(|| serde::de::Error::missing_field("name"))?;
        let value = raw
            .value
            .ok_or_else(|| serde::de::Error::missing_field("value"))?;
        let domain = raw.domain.unwrap_or_default();
        let path = raw.path.unwrap_or_else(|| "/".to_string());
        let secure = raw.secure.unwrap_or(false);
        let http_only = raw.http_only; // 保持 Option<bool>

        // 计算 expires（秒，u64）
        let expires = match (raw.expires.as_ref(), raw.expiration_date.as_ref()) {
            (Some(v), _) => parse_expiration_value(v),
            (None, Some(v)) => parse_expiration_value(v),
            _ => None,
        }
        .map(|secs| secs.floor().max(0.0) as u64);

        // 计算 max_age：优先取输入中的 max-age / maxAge / max_age；若无且有 expires，则基于 expires 与当前时间差计算
        fn parse_seconds_value(v: &serde_json::Value) -> Option<f64> {
            match v {
                serde_json::Value::Null => None,
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::String(s) => s.parse::<f64>().ok(),
                _ => None,
            }
        }

        let max_age = raw
            .max_age
            .as_ref()
            .and_then(parse_seconds_value)
            .or_else(|| raw.max_age_dash.as_ref().and_then(parse_seconds_value))
            .or_else(|| raw.max_age_camel.as_ref().and_then(parse_seconds_value))
            .or_else(|| {
                expires.map(|e| {
                    let now = Utc::now().timestamp();
                    let diff = (e as i64) - now;
                    diff.max(0) as f64
                })
            })
            .map(|secs| secs.floor().max(0.0) as u64);

        Ok(CookieItem {
            name,
            value,
            domain,
            path,
            expires,
            max_age,
            secure,
            http_only,
        })
    }
}

#[derive(
    Default,
    Serialize,
    Deserialize,
    Debug,
    Clone,
)]
pub struct Cookies {
    pub cookies: Vec<CookieItem>,
}

impl Cookies {
    pub fn add(&mut self, name: impl AsRef<str>, value: impl AsRef<str>, domain: impl AsRef<str>) {
        self.cookies.push(CookieItem {
            name: name.as_ref().into(),
            value: value.as_ref().into(),
            domain: domain.as_ref().into(),
            path: "".to_string(),
            expires: None,
            max_age: None,
            secure: false,
            http_only: None,
        })
    }
    pub fn merge(&mut self, other: &Cookies) {
        for cookie in &other.cookies {
            if !self
                .cookies
                .iter()
                .any(|c| c.name == cookie.name && c.domain == cookie.domain)
            {
                self.cookies.push(cookie.clone());
            }
        }
    }
    pub fn merge_cookie_store(&mut self, other: &CookieStore) {
        let other_cookies: Cookies = other.clone().into();
        self.merge(&other_cookies)
    }
    pub fn is_empty(&self) -> bool {
        self.cookies.is_empty()
    }
    pub fn contains(&self, name: impl AsRef<str>, domain: impl AsRef<str>) -> bool {
        self.cookies
            .iter()
            .any(|c| c.name == name.as_ref() && c.domain == domain.as_ref())
    }
    pub fn string(&self) -> String {
        self.cookies
            .iter()
            .map(|c| format!("{}={}", c.name, c.value))
            .collect::<Vec<String>>()
            .join(";")
    }
    pub fn str_by_domain(&self, domain: &[impl AsRef<str>]) -> String {
        self.cookies
            .iter()
            .filter(|c| domain.iter().any(|d| d.as_ref() == c.domain))
            .map(|c| format!("{}={}", c.name, c.value))
            .collect::<Vec<String>>()
            .join(";")
    }
}
impl From<Cookies> for Jar {
    fn from(value: Cookies) -> Self {
        let jar = Jar::default();

        for cookie_item in value.cookies {
            // 构建 cookie 字符串
            let mut cookie_str = format!("{}={}", cookie_item.name, cookie_item.value);

            // 添加域名
            if !cookie_item.domain.is_empty() {
                cookie_str.push_str(&format!("; Domain={}", cookie_item.domain));
            }

            // 添加路径
            if !cookie_item.path.is_empty() {
                cookie_str.push_str(&format!("; Path={}", cookie_item.path));
            }

            // 添加过期时间（按 Cookie 标准使用 GMT 时间）
            if let Some(expires) = cookie_item.expires
                && let Some(dt) = Utc.timestamp_opt(expires as i64, 0).single() {
                    cookie_str.push_str(&format!(
                        "; Expires={}",
                        dt.format("%a, %d %b %Y %H:%M:%S GMT")
                    ));
                }

            // 添加 Max-Age（秒）
            if let Some(max_age) = cookie_item.max_age {
                cookie_str.push_str(&format!("; Max-Age={max_age}"));
            }

            // 添加 Secure 标志
            if cookie_item.secure {
                cookie_str.push_str("; Secure");
            }

            // 添加 HttpOnly 标志
            if cookie_item.http_only == Some(true) {
                cookie_str.push_str("; HttpOnly");
            }

            // 构建 URL（使用域名）
            let url_str = if cookie_item.secure {
                format!("https://{}", cookie_item.domain)
            } else {
                format!("http://{}", cookie_item.domain)
            };

            if let Ok(url) = url_str.parse::<Url>() {
                jar.add_cookie_str(&cookie_str, &url);
            }
        }

        jar
    }
}
impl From<CookieItem> for HeaderValue {
    fn from(value: CookieItem) -> Self {
        let mut cookie_str = format!("{}={}", value.name, value.value);
        if !value.domain.is_empty() {
            cookie_str.push_str(&format!("; Domain={}", value.domain));
        }
        if !value.path.is_empty() {
            cookie_str.push_str(&format!("; Path={}", value.path));
        }
        if let Some(expires) = value.expires
            && let Some(dt) = Utc.timestamp_opt(expires as i64, 0).single() {
                cookie_str.push_str(&format!(
                    "; Expires={}",
                    dt.format("%a, %d %b %Y %H:%M:%S GMT")
                ));
            }
        if let Some(max_age) = value.max_age {
            cookie_str.push_str(&format!("; Max-Age={max_age}"));
        }
        if value.secure {
            cookie_str.push_str("; Secure");
        }
        if value.http_only == Some(true) {
            cookie_str.push_str("; HttpOnly");
        }
        HeaderValue::from_str(&cookie_str).unwrap_or_else(|_| HeaderValue::from_static(""))
    }
}
impl From<CookieStore> for Cookies {
    fn from(value: CookieStore) -> Self {
        let mut cookies = Vec::new();

        // 遍历 CookieStore 中的所有 cookies
        for cookie in value.iter_any() {
            let cookie_item = CookieItem {
                name: cookie.name().to_string(),
                value: cookie.value().to_string(),
                domain: cookie.domain().map(|d| d.to_string()).unwrap_or_default(),
                path: cookie.path().map(|p| p.to_string()).unwrap_or_default(),
                // 将过期时间统一转换为秒（Unix 时间戳，u64）
                expires: cookie.expires().and_then(|exp| {
                    exp.datetime().map(|x| {
                        let ts = x.unix_timestamp();
                        if ts < 0 { 0 } else { ts as u64 }
                    })
                }),
                // Max-Age（秒），若不可用则为 None
                max_age: cookie.max_age().map(|duration| {
                    // cookie_store 的 Duration 通常来自 time crate
                    {
                        (duration.whole_seconds()).max(0) as u64
                    }
                }),
                secure: cookie.secure().unwrap_or(false),
                http_only: cookie.http_only(),
            };
            cookies.push(cookie_item);
        }

        Cookies { cookies }
    }
}
impl TryFrom<&Cookies> for CookieStore {
    type Error = CookieError;

    fn try_from(value: &Cookies) -> Result<Self, Self::Error> {
        let mut store = CookieStore::default();

        for cookie_item in &value.cookies {
            // 构建 cookie 字符串
            let mut cookie_str = format!("{}={}", cookie_item.name, cookie_item.value);

            // 若提供了域名，则设置为 Domain Cookie；规范上不需要前导点，统一去掉
            let normalized_domain = cookie_item.domain.trim().trim_start_matches('.');
            if !normalized_domain.is_empty() {
                cookie_str.push_str(&format!("; Domain={}", normalized_domain));
            }

            // Path（默认为 "/" 覆盖更广）
            if !cookie_item.path.is_empty() {
                cookie_str.push_str(&format!("; Path={}", cookie_item.path));
            } else {
                cookie_str.push_str("; Path=/");
            }

            // Expires（GMT 格式）
            // if let Some(expires) = &cookie_item.expires {
            //     if let Some(dt) = Utc.timestamp_opt(*expires as i64, 0).single() {
            //         cookie_str.push_str(&format!(
            //             "; Expires={}",
            //             dt.format("%a, %d %b %Y %H:%M:%S GMT")
            //         ));
            //     }
            // }
            // Max-Age（秒）
            // if let Some(max_age) = &cookie_item.max_age {
            //     cookie_str.push_str(&format!("; Max-Age={}", max_age));
            // }

            // 标志位
            if cookie_item.secure {
                cookie_str.push_str("; Secure");
            }
            if cookie_item.http_only == Some(true) {
                cookie_str.push_str("; HttpOnly");
            }

            // 构建 URL（修剪前导点，避免诸如 .example.com 导致的非法主机）
            let host = normalized_domain;
            if host.is_empty() {
                // 缺少域名时无法确定 Host-Only 的宿主，这里跳过（若需绑定到特定域，请使用 into_store_for_base_url）
                continue;
            }
            let url_str = if cookie_item.secure {
                format!("https://{}", host)
            } else {
                format!("http://{}", host)
            };

            if let Ok(url) = url_str.parse::<Url>() {
                store
                    .parse(&cookie_str, &url)
                    .map_err(|e| CookieError::ParseError(e.into()))?;
            }
        }

        Ok(store)
    }
}

impl From<Vec<CookieItem>> for Cookies {
    fn from(value: Vec<CookieItem>) -> Self {
        Cookies { cookies: value }
    }
}
#[async_trait]
impl CacheAble for Cookies {
    fn field() -> impl AsRef<str> {
        "cookies".to_string()
    }
}
