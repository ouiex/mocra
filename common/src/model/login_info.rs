use crate::model::cookies::CookieItem;
use crate::model::headers::HeaderItem;
use crate::model::{Cookies, Headers};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginInfo {
    pub cookies: Vec<CookieItem>,
    pub useragent: String,
    pub extra: serde_json::Value,
}

impl Default for LoginInfo {
    fn default() -> Self {
        LoginInfo{
            cookies: vec![],
            useragent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36".to_string(),
            extra: Default::default(),
        }
    }
}

impl From<&LoginInfo> for Cookies {
    fn from(value: &LoginInfo) -> Self {
        Cookies {
            cookies: value.cookies.clone(),
        }
    }
}

impl From<&LoginInfo> for Headers {
    fn from(value: &LoginInfo) -> Self {
        Headers {
            headers: vec![HeaderItem {
                key: "User-Agent".to_string(),
                value: value.useragent.clone(),
            }],
        }
    }
}
impl LoginInfo {
    pub fn get_extra<T>(&self, key: &str) -> Option<T>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        self.extra
            .get(key)
            .and_then(|v| serde_json::from_value::<T>(v.clone()).ok())
    }
    pub fn get_shop_id(&self) -> Option<i64> {
        self.get_extra("shopid")
    }
}
