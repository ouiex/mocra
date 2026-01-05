use crate::Downloader;
use async_trait::async_trait;
use base64::Engine;
use errors::DownloadError::DownloadFailed;
use errors::{DownloadError, Error, RequestError, Result};
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use semver::{Op, Version};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use uuid::Uuid;
use common::model::cookies::CookieItem;
use common::model::download_config::DownloadConfig;
use common::model::{Request, Response};

/// 由远程下载服务器实现下载功能，目前远程服务器设计的是使用nodejs+express+playwright实现下载功能
///
/// 远程下载服务器的下载方法是打开对应页面时拦截对应的http请求，然后将请求的内容返回给客户端，返回的内容为base64编码的字符串
///
/// 远程下载服务器的地址和端口在配置文件中配置，默认地址为http://localhost:3000
///
/// 远程下载服务器的接口为/download ，请求方法为POST，参数为json格式，包含url和headers字段
///
/// 目前远程下载服务器暂时没有做身份验证，后续会添加身份验证功能
///
/// 本下载方法仅用于无法通过常规http请求下载的资源，如需要执行js脚本才能获取资源的页面、
///
/// 建议使用的场景为：需要通过浏览器环境才能获取资源的页面，如某些反爬虫较强的网站
///
/// 目前远程下载服务器的性能较差，单线程处理请求，且每次请求都需要启动一个浏览器实例，
///
/// 资源消耗较大，尤其是在使用了代理后，响应更慢
///
/// 速度较慢，且不稳定，建议仅在必要时使用
/// ```json
/// {
///   "id": "userA",                 // 必填。Profile 标识（内部以 sha256(id).slice(0,16) 作为目录名）
///   "headless": false,              // 可选。请求级覆盖（若不传，使用服务默认或 options.headless）
///   "options": {
///     "url": "https://example.com", // 必填。目标页面地址
///
///     "cookies": [                  // 可选。Playwright Cookie 数组（注意 sameSite 只能是 Strict/Lax/None）
///       {
///        "name": "a",
///         "value": "1",
///         "domain": ".example.com",
///         "path": "/",
///         "expires": 1760000000,   // 秒级 Unix 时间戳；若传毫秒将被转换为秒
///         "httpOnly": true,
///         "secure": true,
///         "sameSite": "None"      // 仅支持 Strict | Lax | None（不支持 unspecified / no_restriction）
///       }
///     ],
///
///     "js_list": [                  // 可选。按顺序执行的页面脚本
///       { "id": "title", "js_str": "document.title" },
///       { "id": "count", "js_str": "document.links.length" }
///     ],
///
///     "inject_urls": [              // 必填。捕获规则，至少提供 url 或 regex 之一
///       { "id": "cfg",   "url":   "config.json" },         // 子串匹配：includes(url)
///       { "id": "users", "regex": "api\\/users\\/?$" }  // 正则匹配：new RegExp(regex)
///     ],
///
///     "timeoutMs": 30000,           // 可选。整体超时（默认 60000）
///     "firstMatchOnly": false,      // 可选。导航后首个命中即可结束等待
///     "postScriptWaitMs": 0,        // 可选。脚本执行完后额外等待
///     "headless": false,            // 可选。请求内覆盖 headless（与顶层 headless 二选一，任一提供即可）
///
///     "proxy": {                    // 可选。HTTP 代理（用于该 Profile 的持久化上下文）
///       "server": "host:port",
///       "username": "user",       // 可选
///       "password": "pass",       // 可选
///       "bypass": "localhost"     // 可选，逗号分隔
///     },
///
///     "closeContextOnDone": false   // 可选。若该 Profile 使用持久化上下文，任务结束后是否关闭（默认 false）
///   }
/// }
/// ```
#[derive(Clone)]
pub struct RemoteDownloader {
    remote_url: String,
    remote_port: u16,
    client: reqwest::Client,
}
impl RemoteDownloader {
    pub fn new(remote_url: String, remote_port: u16) -> Self {
        let client = reqwest::Client::new();
        Self {
            remote_url,
            remote_port,
            client,
        }
    }

    pub fn get_full_url(&self) -> String {
        format!("{}:{}/download", self.remote_url, self.remote_port)
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub struct InjectItem {
    pub id: String,
    pub url: Option<String>,
    pub regex: Option<String>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct JsItem {
    pub id: String,
    pub js_str: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteDownloadOptions {
    pub url: String,
    pub timeout_ms: Option<u64>,
    pub headless: Option<bool>,
    // 远程服务期望的是 Playwright Cookie 数组，这里直接用 Vec<CookieItem>
    pub cookies: Option<Vec<CookieItem>>,
    pub js_list: Option<Vec<JsItem>>,
    pub inject_urls: Option<Vec<InjectItem>>,
    #[serde(rename = "firstMatchOnly")]
    pub first_match_only: Option<bool>,
    #[serde(rename = "postScriptWaitMs")]
    pub post_script_wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy: Option<HashMap<String, String>>,
}

/// 远程下载请求数据结构
/// 所有的参数需要在request.meta.trait_meta.remote_options中提供
/// request.url为需要打开的页面，需要拦截的请求地址在request.meta.trait_meta.remote_options.inject_urls中提供
#[derive(Serialize, Deserialize, Debug)]
pub struct RemoteDownloadData {
    pub id: String,
    pub headless: Option<bool>,
    pub options: RemoteDownloadOptions,
    #[serde(rename = "closeContextOnDoner")]
    pub close_context_on_done: Option<bool>,
}

impl TryFrom<&Request> for RemoteDownloadData {
    type Error = ();

    fn try_from(value: &Request) -> std::result::Result<Self, Self::Error> {
        if let Some(opts) = value
            .meta
            .get_trait_config::<RemoteDownloadOptions>("remote_options")
        {
            let data = Self {
                id: value.account.clone(),
                headless: Some(false),
                options: opts,
                close_context_on_done: Some(true),
            };
            return Ok(data);
        }
        Err(())
    }
}
#[derive(Serialize, Deserialize)]
pub struct ScriptResult {
    pub id: String,
    pub value: String,
}
#[derive(Serialize, Deserialize)]
pub struct CapturedResult {
    #[serde(rename = "patternId")]
    pub pattern_id: String,
    pub url: String,
    pub status: u16,
    pub headers: HashMap<String, String>,
    #[serde(rename = "contentType")]
    pub content_type: Option<String>,
    #[serde(rename = "bodyBase64")]
    pub body_base64: String,
}

#[derive(Serialize, Deserialize)]
pub struct RemoteDownloadResponse {
    #[serde(rename = "pageUrl")]
    pub page_url: String,
    #[serde(rename = "elapsedMs")]
    pub elapsed_ms: i64,
    pub scripts: Vec<ScriptResult>,
    pub captured: Vec<CapturedResult>,
}
impl RemoteDownloadResponse {
    pub fn into_response(self, request: &Request) -> Response {
        let resp_content = json!({
            "scripts":self.scripts,
            "captured":self.captured
        })
        .to_string();

        Response {
            id: Uuid::now_v7(),
            platform: request.platform.clone(),
            account: request.account.clone(),
            module: request.module.clone(),
            status_code: 200,
            cookies: request.cookies.clone(),
            content: resp_content.into_bytes(),
            headers: request.headers.clone().into(),
            task_retry_times: request.retry_times,
            metadata: request.meta.clone(),
            download_middleware: request.download_middleware.clone(),
            data_middleware: request.data_middleware.clone(),
            task_finished: request.task_finished,
            context: request.context.clone(),
            run_id: request.run_id,
            prefix_request: request.id,
            request_hash: if request.enable_cache {
                Some(request.hash())
            } else {
                None
            },
        }
    }
}

#[async_trait]
impl Downloader for RemoteDownloader {
    async fn set_config(&self, id: &str, config: DownloadConfig) {}

    async fn set_limit(&self, id: &str, limit: f32) {}

    fn name(&self) -> String {
        "remote_downloader".to_string()
    }

    fn version(&self) -> Version {
        Version::parse("0.1.0").unwrap()
    }

    async fn download(&self, request: Request) -> Result<Response> {
        let data = RemoteDownloadData::try_from(&request).map_err(|_| {
            RequestError::InvalidMetaForRemote("Missing remote_options in request.meta".into())
        })?;
        let url = self.get_full_url();
        let response = self
            .client
            .request(Method::POST, url)
            .json(&data)
            .send()
            .await
            .map_err(|e| DownloadError::DownloadFailed(e.into()))?;
        let content = response
            .text()
            .await
            .map_err(|e| DownloadError::DownloadFailed(e.into()))?;
        println!("{content:?}");
        let resp = serde_json::from_str::<RemoteDownloadResponse>(&content)
            .map_err(|e| DownloadError::DownloadFailed(e.into()))?;
        let final_response = resp.into_response(&request);
        Ok(final_response)
    }

    async fn health_check(&self) -> Result<()> {
        Ok(())
    }
}
