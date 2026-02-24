use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use cacheable::CacheAble;
use std::fmt;

#[derive(Clone, Serialize, Deserialize)]
pub struct HeaderItem {
    pub key: String,
    pub value: String,
}

impl fmt::Debug for HeaderItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key_lower = self.key.to_lowercase();
        let value = if key_lower.contains("auth") || key_lower.contains("cookie") || key_lower.contains("secret") || key_lower.contains("token") {
            "***REDACTED***"
        } else {
            &self.value
        };
        
        f.debug_struct("HeaderItem")
            .field("key", &self.key)
            .field("value", &value)
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Headers {
    pub headers: Vec<HeaderItem>,
}

impl fmt::Debug for Headers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Headers")
            .field("headers", &self.headers)
            .finish()
    }
}

impl From<Headers> for Vec<(String, String)> {
    fn from(value: Headers) -> Self {
        value
            .headers
            .into_iter()
            .map(|h| (h.key, h.value))
            .collect()
    }
}

impl Default for Headers {
    fn default() -> Self {
        Self::new()
    }
}

impl Headers {
    pub fn new() -> Self {
        Headers {
            headers: Vec::new(),
        }
    }

    pub fn add(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        if let Some(v) = self
            .headers
            .iter_mut()
            .find(|h| h.key.eq_ignore_ascii_case(key.as_ref()))
        {
            v.value = value.as_ref().into();
        } else {
            self.headers.push(HeaderItem {
                key: key.as_ref().into(),
                value: value.as_ref().into(),
            })
        }
        self
    }
    pub fn merge(&mut self, other: &Headers) {
        for header_item in &other.headers {
            self.headers.push(header_item.clone());
        }
    }
    pub fn merge_map(&mut self, other: &HeaderMap) {
        for (key, value) in other.iter() {
            if let Ok(value_str) = value.to_str() {
                self.headers.push(HeaderItem {
                    key: key.as_str().to_string(),
                    value: value_str.to_string(),
                });
            }
        }
    }
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }
    pub fn contains(&self, key: impl AsRef<str>) -> bool {
        self.headers
            .iter()
            .any(|header_item| header_item.key.eq_ignore_ascii_case(key.as_ref()))
    }
    pub fn get(&self, key: impl AsRef<str>) -> Option<String> {
        for header_item in &self.headers {
            if header_item.key.eq_ignore_ascii_case(key.as_ref()) {
                return Some(header_item.value.clone());
            }
        }
        None
    }
}

impl From<&Headers> for HeaderMap {
    fn from(value: &Headers) -> Self {
        let mut header_map = HeaderMap::new();

        for header_item in &value.headers {
            match (
                header_item.key.parse::<reqwest::header::HeaderName>(),
                HeaderValue::from_str(&header_item.value),
            ) {
                (Ok(name), Ok(value)) => {
                    // Use `append` for headers that can contain multiple values.
                    let name_str = name.as_str().to_lowercase();
                    if matches!(
                        name_str.as_str(),
                        // Cookie-related
                        "set-cookie" | "cookie" |
                        // Content negotiation
                        "accept" | "accept-encoding" | "accept-language" | "accept-charset" |
                        // Cache control
                        "cache-control" | "pragma" |
                        // Links and forwarding
                        "link" | "forwarded" | "x-forwarded-for" | "x-forwarded-proto" |
                        // Security policies
                        "content-security-policy" | "x-content-security-policy" |
                        "x-webkit-csp" | "feature-policy" | "permissions-policy" |
                        // CORS
                        "access-control-allow-origin" | "access-control-allow-methods" |
                        "access-control-allow-headers" | "access-control-expose-headers" |
                        // Authentication
                        "www-authenticate" | "proxy-authenticate" |
                        // Variants and content
                        "vary" | "via" | "warning" |
                        // Custom and extension headers
                        "x-forwarded-host" | "x-real-ip" | "x-original-forwarded-for"
                    ) {
                        header_map.append(name, value);
                    } else {
                        // Overwrite behavior for other headers.
                        header_map.insert(name, value);
                    }
                }
                _ => continue,
            }
        }

        header_map
    }
}
impl From<HeaderMap> for Headers {
    fn from(value: HeaderMap) -> Self {
        let headers = value
            .iter()
            .map(|(key, value)| HeaderItem {
                key: key.as_str().to_string(),
                value: value.to_str().unwrap_or("").to_string(),
            })
            .collect();

        Headers { headers }
    }
}

impl CacheAble for Headers{
    fn field() -> impl AsRef<str> {
        "headers"
    }
}


#[test]
fn test() {
    let headers = Headers::new()
        .add("Content-Type", "application/json")
        .add("User-Agent", "MyApp/1.0");

    let header_map: HeaderMap = HeaderMap::from(&headers);

    assert_eq!(header_map.get("Content-Type").unwrap(), "application/json");
    assert_eq!(header_map.get("User-Agent").unwrap(), "MyApp/1.0");
}
