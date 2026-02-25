use async_trait::async_trait;
use crate::cacheable::CacheAble;
use chrono::{TimeZone, Utc};
use crate::errors::CookieError;
use reqwest::cookie::Jar;
use reqwest::header::HeaderValue;
use reqwest_cookie_store::CookieStore;
use serde::{Deserialize, Deserializer, Serialize};
use url::Url;
use std::fmt;

/// Cookie validity is managed externally.
/// This type only performs serialization/deserialization and does not validate cookie validity.
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
        fn parse_expiration_value(v: &serde_json::Value) -> Option<f64> {
            match v {
                serde_json::Value::Null => None,
                serde_json::Value::Number(n) => n.as_f64().map(|u| {
                    // Handle millisecond timestamps (>= 1e12 is treated as milliseconds).
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

        // Parse "YYYY/M/D HH:MM:SS" or "YYYY-MM-DD HH:MM:SS" into seconds.
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

        fn parse_seconds_value(v: &serde_json::Value) -> Option<f64> {
            match v {
                serde_json::Value::Null => None,
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::String(s) => s.parse::<f64>().ok(),
                _ => None,
            }
        }

        struct CookieItemVisitor;

        impl<'de> serde::de::Visitor<'de> for CookieItemVisitor {
            type Value = CookieItem;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("CookieItem as map or seq")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let name: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let value: String = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                let domain: String = seq.next_element()?.unwrap_or_default();
                let path: String = seq.next_element()?.unwrap_or_else(|| "/".to_string());
                let expires: Option<u64> = seq.next_element()?.unwrap_or(None);
                let max_age: Option<u64> = seq.next_element()?.unwrap_or(None);
                let secure: bool = seq.next_element()?.unwrap_or(false);
                let http_only: Option<bool> = seq.next_element()?.unwrap_or(None);

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

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut name: Option<String> = None;
                let mut value: Option<String> = None;
                let mut domain: Option<String> = None;
                let mut path: Option<String> = None;
                let mut secure: Option<bool> = None;
                let mut http_only: Option<bool> = None;
                let mut expires: Option<serde_json::Value> = None;
                let mut expiration_date: Option<serde_json::Value> = None;
                let mut max_age: Option<serde_json::Value> = None;
                let mut max_age_dash: Option<serde_json::Value> = None;
                let mut max_age_camel: Option<serde_json::Value> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "name" => name = Some(map.next_value()?),
                        "value" => value = Some(map.next_value()?),
                        "domain" => domain = Some(map.next_value()?),
                        "path" => path = Some(map.next_value()?),
                        "secure" => secure = Some(map.next_value()?),
                        "httpOnly" => http_only = Some(map.next_value()?),
                        "expires" => expires = Some(map.next_value()?),
                        "expirationDate" => expiration_date = Some(map.next_value()?),
                        "max_age" => max_age = Some(map.next_value()?),
                        "max-age" => max_age_dash = Some(map.next_value()?),
                        "maxAge" => max_age_camel = Some(map.next_value()?),
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let name = name.ok_or_else(|| serde::de::Error::missing_field("name"))?;
                let value = value.ok_or_else(|| serde::de::Error::missing_field("value"))?;
                let domain = domain.unwrap_or_default();
                let path = path.unwrap_or_else(|| "/".to_string());
                let secure = secure.unwrap_or(false);
                let http_only = http_only;

                let expires = match (expires.as_ref(), expiration_date.as_ref()) {
                    (Some(v), _) => parse_expiration_value(v),
                    (None, Some(v)) => parse_expiration_value(v),
                    _ => None,
                }
                .map(|secs| secs.floor().max(0.0) as u64);

                let max_age = max_age
                    .as_ref()
                    .and_then(parse_seconds_value)
                    .or_else(|| max_age_dash.as_ref().and_then(parse_seconds_value))
                    .or_else(|| max_age_camel.as_ref().and_then(parse_seconds_value))
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

        deserializer.deserialize_any(CookieItemVisitor)
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

    pub fn cookie_header_for_url(&self, url: &Url) -> Option<String> {
        if self.cookies.is_empty() {
            return None;
        }

        if let Ok(store) = CookieStore::try_from(self) {
            let pairs = store
                .get_request_values(url)
                .map(|(name, value)| format!("{}={}", name, value))
                .collect::<Vec<_>>();
            if !pairs.is_empty() {
                return Some(pairs.join("; "));
            }
        }

        let host = url.host_str()?.to_ascii_lowercase();
        let pairs = self
            .cookies
            .iter()
            .filter_map(|cookie| {
                let domain = cookie
                    .domain
                    .trim()
                    .trim_start_matches('.')
                    .to_ascii_lowercase();
                if domain.is_empty() {
                    return None;
                }
                if host == domain || host.ends_with(&format!(".{domain}")) {
                    Some(format!("{}={}", cookie.name, cookie.value))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if pairs.is_empty() {
            None
        } else {
            Some(pairs.join("; "))
        }
    }
}
impl From<Cookies> for Jar {
    fn from(value: Cookies) -> Self {
        let jar = Jar::default();

        for cookie_item in value.cookies {
            // Build cookie string.
            let mut cookie_str = format!("{}={}", cookie_item.name, cookie_item.value);

            // Add domain.
            if !cookie_item.domain.is_empty() {
                cookie_str.push_str(&format!("; Domain={}", cookie_item.domain));
            }

            // Add path.
            if !cookie_item.path.is_empty() {
                cookie_str.push_str(&format!("; Path={}", cookie_item.path));
            }

            // Add expiration (GMT, per cookie spec).
            if let Some(expires) = cookie_item.expires
                && let Some(dt) = Utc.timestamp_opt(expires as i64, 0).single() {
                    cookie_str.push_str(&format!(
                        "; Expires={}",
                        dt.format("%a, %d %b %Y %H:%M:%S GMT")
                    ));
                }

            // Add Max-Age (seconds).
            if let Some(max_age) = cookie_item.max_age {
                cookie_str.push_str(&format!("; Max-Age={max_age}"));
            }

            // Add Secure flag.
            if cookie_item.secure {
                cookie_str.push_str("; Secure");
            }

            // Add HttpOnly flag.
            if cookie_item.http_only == Some(true) {
                cookie_str.push_str("; HttpOnly");
            }

            // Build URL using domain.
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

        // Iterate through all cookies in `CookieStore`.
        for cookie in value.iter_any() {
            let cookie_item = CookieItem {
                name: cookie.name().to_string(),
                value: cookie.value().to_string(),
                domain: cookie.domain().map(|d| d.to_string()).unwrap_or_default(),
                path: cookie.path().map(|p| p.to_string()).unwrap_or_default(),
                // Normalize expiration to seconds (Unix timestamp, `u64`).
                expires: cookie.expires().and_then(|exp| {
                    exp.datetime().map(|x| {
                        let ts = x.unix_timestamp();
                        if ts < 0 { 0 } else { ts as u64 }
                    })
                }),
                // Max-Age (seconds), or `None` when unavailable.
                max_age: cookie.max_age().map(|duration| {
                    // `cookie_store` duration usually comes from `time` crate.
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
            // Build cookie string.
            let mut cookie_str = format!("{}={}", cookie_item.name, cookie_item.value);

            // If domain is provided, create Domain Cookie; leading dot is stripped per normalization.
            let normalized_domain = cookie_item.domain.trim().trim_start_matches('.');
            if !normalized_domain.is_empty() {
                cookie_str.push_str(&format!("; Domain={}", normalized_domain));
            }

            // Path (default `/` for broader matching).
            if !cookie_item.path.is_empty() {
                cookie_str.push_str(&format!("; Path={}", cookie_item.path));
            } else {
                cookie_str.push_str("; Path=/");
            }

            // Expires (GMT format).
            // if let Some(expires) = &cookie_item.expires {
            //     if let Some(dt) = Utc.timestamp_opt(*expires as i64, 0).single() {
            //         cookie_str.push_str(&format!(
            //             "; Expires={}",
            //             dt.format("%a, %d %b %Y %H:%M:%S GMT")
            //         ));
            //     }
            // }
            // Max-Age (seconds).
            // if let Some(max_age) = &cookie_item.max_age {
            //     cookie_str.push_str(&format!("; Max-Age={}", max_age));
            // }

            // Flags.
            if cookie_item.secure {
                cookie_str.push_str("; Secure");
            }
            if cookie_item.http_only == Some(true) {
                cookie_str.push_str("; HttpOnly");
            }

            // Build URL (trim leading dot to avoid invalid hosts like `.example.com`).
            let host = normalized_domain;
            if host.is_empty() {
                // Without domain we cannot infer host-only owner; skip here.
                // Use `into_store_for_base_url` when binding to a specific host is required.
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
