use crate::model::login_info::LoginInfo;
use crate::model::meta::MetaData;
use crate::model::ExecutionMark;
use crate::model::ModuleConfig;
use crate::model::{Cookies, Headers};
use proxy::ProxyEnum;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utils::encrypt::md5;
use uuid::Uuid;
use cacheable::CacheAble;
use once_cell::sync::OnceCell;
use crate::interface::storage::{Offloadable, BlobStorage};
use std::sync::Arc;
use async_trait::async_trait;

pub enum RequestMethod {
    Post,
    Get,
    Delete,
    Options,
    Put,
    Head,
    // 目前wss只是一个标志，用于区分WebSocket请求，实际wss请求里不需要该参数
    Wss,
}
impl Display for RequestMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            RequestMethod::Post => "POST".to_string(),
            RequestMethod::Get => "GET".to_string(),
            RequestMethod::Delete => "DELETE".to_string(),
            RequestMethod::Options => "OPTIONS".to_string(),
            RequestMethod::Put => "PUT".to_string(),
            RequestMethod::Head => "HEAD".to_string(),
            RequestMethod::Wss => "WSS".to_string(),
        };
        write!(f, "{str}")
    }
}

impl AsRef<str> for RequestMethod {
    fn as_ref(&self) -> &str {
        match self {
            RequestMethod::Post => "POST",
            RequestMethod::Get => "GET",
            RequestMethod::Delete => "DELETE",
            RequestMethod::Options => "OPTIONS",
            RequestMethod::Put => "PUT",
            RequestMethod::Head => "HEAD",
            RequestMethod::Wss => "WSS",
        }
    }
}
impl From<RequestMethod> for String {
    fn from(val: RequestMethod) -> Self {
        val.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize,)]
pub struct Request {
    pub id: Uuid,
    pub platform: String,
    pub account: String,
    pub module: String,
    pub url: String,
    pub method: String,
    pub headers: Headers,
    pub cookies: Cookies,
    pub retry_times: usize,
    pub task_retry_times: usize,
    pub use_new_client: bool,
    pub timeout: u64,
    /// ParserModel,ErrorModel中的meta会在TaskFactory添加至Task.meta中
    /// meta用于存储额外的信息，在Module.generate中将Task.meta,ShopInfo.extra,ModuleConfig合并至Request.meta中
    /// 所有的meta来自于Task.meta,LoginInfo.extra,ModuleConfig,trait add
    /// 使用自定义的数据字段区分task,login_info,trait由代码添加的不做区分
    pub meta: MetaData,
    pub params: Option<Vec<(String, String)>>,
    #[serde(with = "crate::model::serde_value::option_value")]
    pub json: Option<serde_json::Value>,
    pub body: Option<Vec<u8>>,
    #[serde(with = "crate::model::serde_value::option_value")]
    pub form: Option<serde_json::Value>,
    /// 哪些headers需要缓存
    /// 例如：`Cache-Control`, `Expires`, `ETag`
    pub cache_headers: Option<Vec<String>>,
    pub proxy: Option<ProxyEnum>,
    /// 限流ID
    /// 用于标识同一限流组的请求
    /// 默认使用module_id
    pub limit_id: String,
    pub download_middleware: Vec<String>,
    pub data_middleware: Vec<String>,
    pub task_finished: bool,
    pub time_sleep_secs: Option<u64>,
    pub context: ExecutionMark,
    pub run_id: Uuid,
    pub prefix_request: Uuid,
    /// 自定义的hash字符串，用于覆盖默认的请求hash计算
    pub hash_str: Option<String>,
    pub enable_cache: bool,
    /// Enable distributed lock for this request to ensure serial execution within the same task/run
    pub enable_locker: Option<bool>,
    pub downloader: String,
    #[serde(default)]
    pub priority: crate::model::Priority,
    #[serde(skip)]
    hash_cache: OnceCell<String>,
}

#[async_trait]
impl Offloadable for Request {
    fn should_offload(&self, _threshold: usize) -> bool {
        false
    }
    async fn offload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
    async fn reload(&mut self, _storage: &Arc<dyn BlobStorage>) -> errors::Result<()> {
        Ok(())
    }
}

impl Request {
    pub fn new(url: impl AsRef<str>, method: impl AsRef<str>) -> Self {
        Request {
            id: Uuid::now_v7(),
            platform: "".to_string(),
            account: "".to_string(),
            module: "".to_string(),
            url: url.as_ref().into(),
            method: method.as_ref().into(),
            headers: Headers::default(),
            cookies: Cookies::default(),
            retry_times: 0,
            meta: MetaData::default(),
            params: None,
            json: None,
            body: None,
            form: None,
            timeout: 30, // 默认超时时间为30秒
            cache_headers: None,
            proxy: None,
            limit_id: "".to_string(),
            use_new_client: false,
            download_middleware: vec![],
            data_middleware: vec![],
            task_retry_times: 0,
            task_finished: false,
            time_sleep_secs: None,
            context: Default::default(),
            run_id: Uuid::now_v7(),
            prefix_request: Uuid::nil(),
            hash_str: None,
            enable_cache: false,
            enable_locker: None,
            downloader: "request_downloader".to_string(),
            priority: crate::model::Priority::default(),
            hash_cache: OnceCell::new(),
        }
    }
    pub fn with_priority(mut self, priority: crate::model::Priority) -> Self {
        self.priority = priority;
        self
    }
    pub fn use_proxy(&mut self, proxy: ProxyEnum) -> &mut Request {
        self.proxy = Some(proxy);
        self
    }
    pub fn task_id(&self) -> String {
        format!("{}-{}", self.account, self.platform)
    }
    pub fn module_id(&self) -> String {
        format!("{}-{}-{}", self.account, self.platform, self.module)
    }
    pub fn with_params(mut self, params: Vec<(impl AsRef<str>, impl AsRef<str>)>) -> Self {
        self.params = Some(
            params
                .iter()
                .map(|(a, b)| (a.as_ref().to_string(), b.as_ref().to_string()))
                .collect(),
        );
        self
    }
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers.merge(&headers);
        self
    }
    pub fn with_cookies(mut self, cookies: Cookies) -> Self {
        self.cookies.merge(&cookies);
        self
    }
    pub fn with_json<T: Serialize + ?Sized>(mut self, json: &T) -> Self {
        self.json = Some(serde_json::to_value(json).unwrap());
        self
    }
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = Some(body);
        self
    }
    pub fn with_form<T: Serialize + ?Sized>(mut self, form: &T) -> Self {
        self.form = Some(serde_json::to_value(form).unwrap());
        self
    }
    pub fn with_trait_config<T>(mut self, key: impl AsRef<str>, value: T) -> Self
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        self.meta = self.meta.add_trait_config(key, value);
        self
    }
    pub fn with_login_info(mut self, info: &LoginInfo) -> Self {
        self.meta = self.meta.add_login_info(info);
        self
    }
    pub fn with_task_config<T>(mut self, task_meta: T) -> Self
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        self.meta = self.meta.add_task_config(task_meta);
        self
    }
    pub fn with_module_config(mut self, value: &ModuleConfig) -> Self {
        self.meta = self.meta.add_module_config(value);
        self
    }
    pub fn with_sleep(mut self, secs: u64) -> Self {
        self.time_sleep_secs = Some(secs);
        self
    }
    // Typed Context helpers
    pub fn with_context(mut self, ctx: ExecutionMark) -> Self {
        self.context = ctx;
        self
    }
    pub fn hash(&self) -> String {
        // Build a canonical representation of the request's identity-related fields
        if let Some(hash) = &self.hash_str {
            return hash.to_owned();
        }
        if let Some(cached) = self.hash_cache.get() {
            return cached.clone();
        }
        let canonical = format!(
            "{},{},{},{},{},{},{},{:?},{}",
            self.account,
            self.platform,
            self.module,
            self.url,
            self.method,
            serde_json::to_string(&self.params).unwrap_or_default(),
            self.json.as_ref().unwrap_or_default(),
            self.body.as_deref().unwrap_or(&[]),
            self.form.as_ref().unwrap_or_default()
        );

        // MD5 hash in lowercase hex for stable, compact identity
        let digest = md5(canonical.as_bytes()).to_string();
        let _ = self.hash_cache.set(digest.clone());
        digest
    }
    pub fn enable_cache(mut self, enable: bool) -> Self {
        self.enable_cache = enable;
        self
    }
    pub fn enable_cache_with<T>(mut self, hash_able: &T) -> Self
    where
        T: Serialize,
    {
        if let Ok(hash_str) = serde_json::to_string(hash_able) {
            self.enable_cache = true;
            self.hash_str = Some(md5(hash_str.as_bytes()));
        }
        self
    }
}


impl CacheAble for Request {
    fn field() -> impl AsRef<str> {
        "request"
    }

    fn serialized_size_hint(&self) -> Option<usize> {
        Some(
            self.url.len()
                + self.method.len()
                + self.headers.headers.len() * 64
                + self.cookies.cookies.len() * 64
                + self.body.as_ref().map_or(0, |b| b.len())
                + self.form.as_ref().map_or(0, |_| 512)
                + self.json.as_ref().map_or(0, |_| 512),
        )
    }

    fn clone_for_serialize(&self) -> Option<Self> {
        Some(self.clone())
    }
}

impl crate::model::priority::Prioritizable for Request {
    fn get_priority(&self) -> crate::model::Priority {
        self.priority
    }
}
