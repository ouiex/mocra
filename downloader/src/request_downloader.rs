use crate::Downloader;
use errors::{DownloadError, Error, RequestError, Result};
use log::{info, warn};
use reqwest::Client;
use reqwest::Method;
use reqwest::Proxy;
use reqwest::header::HeaderMap;
use reqwest_cookie_store::CookieStore;
use semver::Version;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::RwLock;
use url::Url;
use utils::distributed_rate_limit::DistributedSlidingWindowRateLimiter;
use utils::redis_lock::DistributedLockManager;
use uuid::Uuid;
use cacheable::{CacheService,CacheAble};
use common::model::{Cookies, Headers, Request, Response};
use common::model::cookies::CookieItem;
use common::model::download_config::DownloadConfig;
use common::model::headers::HeaderItem;
/// RequestDownloader 实现了 Downloader trait，提供 HTTP 请求下载功能
/// 支持 Cookie 和 Header 缓存、请求限速、任务锁等功能。
/// 通过 enable_cache、enable_locker 和 enable_rate_limit 方法可以启用相应的功能。
/// 该下载器使用 reqwest 库进行 HTTP 请求，并使用 Arc 和 Mutex 实现线程安全。
/// task_locks 使用分布式锁

#[derive(Clone)]
pub struct RequestDownloader {
    pub limit: Arc<DistributedSlidingWindowRateLimiter>,
    // 状态记录器：为每个task_id维护一个互斥锁，确保相同task_id的请求串行执行
    pub locker: Arc<DistributedLockManager>,
    cache_service: Arc<CacheService>,
    enable_cache: Arc<RwLock<bool>>,
    enable_locker: Arc<RwLock<bool>>,
    enable_rate_limit: Arc<RwLock<bool>>,
}

impl RequestDownloader {
    pub fn new(
        limit: Arc<DistributedSlidingWindowRateLimiter>,
        locker: Arc<DistributedLockManager>,
        sync: Arc<CacheService>,
    ) -> Self {
        RequestDownloader {
            limit,
            locker,
            cache_service: sync,
            enable_cache: Arc::new(RwLock::new(false)),
            enable_locker: Arc::new(RwLock::new(true)),
            enable_rate_limit: Arc::new(RwLock::new(true)),
        }
    }

    async fn process_request(&self, request: Request) -> Result<Request> {
        let cache_enabled = *self.enable_cache.read().await;
        if !cache_enabled {
            return Ok(request);
        }

        let module_id = request.module_id();
        let mut modified_request = request;

        // 并行处理headers和cookies的缓存加载
        let headers_future = self.load_cached_headers(&module_id, &modified_request.cache_headers);
        let cookies_future = self.load_cached_cookies(&module_id);

        let (cached_headers, cached_cookies) = tokio::try_join!(headers_future, cookies_future)?;

        // 应用缓存的headers
        if let Some(headers) = cached_headers {
            modified_request.headers.merge(&headers);
        }

        // 应用缓存的cookies
        if let Some(cookies) = cached_cookies {
            modified_request.cookies.merge(&cookies);
        }

        Ok(modified_request)
    }

    /// 加载缓存的headers，使用细粒度锁
    async fn load_cached_headers(
        &self,
        module_id: &str,
        cache_headers: &Option<Vec<String>>,
    ) -> Result<Option<Headers>> {
        if let Some(cache_headers) = cache_headers {
            let result = if let Ok(Some(headers)) =
                Headers::sync(module_id, &self.cache_service).await
            {
                let filtered_headers = Headers {
                    headers: headers
                        .headers
                        .into_iter()
                        .filter(|x| cache_headers.contains(&x.key))
                        .collect::<Vec<HeaderItem>>(),
                };
                Some(filtered_headers)
            } else {
                None
            };
            Ok(result)
        } else {
            Ok(None)
        }
    }

    /// 加载缓存的cookies，使用细粒度锁
    async fn load_cached_cookies(&self, module_id: &str) -> Result<Option<Cookies>> {
        let result = if let Ok(Some(cache_cookies)) =
            Cookies::sync(module_id, &self.cache_service).await
        {
            Some(cache_cookies)
        } else {
            None
        };
        Ok(result)
    }
    async fn process_response(
        &self,
        request: &Request,
        response: reqwest::Response,
    ) -> Result<Response> {
        let request_id = request.id;
        // 根据request.cache_headers更新响应头
        let cache_enabled = *self.enable_cache.read().await;
        if cache_enabled
            && let Some(cache_headers) = &request.cache_headers
            && !cache_headers.is_empty()
        {
            let mut update_headers = Headers::default();
            for (name, value) in response.headers().iter() {
                let header_name = name.as_str().to_lowercase();
                // 检查是否在需要缓存的headers列表中
                if cache_headers
                    .iter()
                    .any(|cache_header| cache_header.to_lowercase() == header_name)
                {
                    update_headers.headers.push(HeaderItem {
                        key: name.to_string(),
                        value: value.to_str().unwrap_or("").to_string(),
                    });
                }
            }
            update_headers
                .send(&request.module_id(), &self.cache_service)
                .await
                .ok();
        }
        let status_code = response.status().as_u16();
        let response_headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("").to_string()))
            .collect();
        let response_cookies: Vec<CookieItem> = response
            .cookies()
            .map(|cookie| CookieItem {
                name: cookie.name().to_string(),
                value: cookie.value().to_string(),
                domain: cookie.domain().unwrap_or("").to_string(),
                path: cookie.path().unwrap_or("/").to_string(),
                expires: cookie
                    .expires()
                    .and_then(|exp| exp.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs()),
                max_age: cookie.max_age().map(|d| d.as_secs()),
                secure: cookie.secure(),
                http_only: Some(cookie.http_only()),
            })
            .collect();

        // 然后获取响应体
        let content = response
            .bytes()
            .await
            .map_err(|e| DownloadError::DownloadFailed(e.into()))?
            .to_vec();

        Ok(Response {
            id: request_id,
            platform: request.platform.clone(),
            account: request.account.clone(),
            module: request.module.clone(),
            status_code,
            cookies: Cookies {
                cookies: response_cookies,
            },
            content,
            headers: response_headers,
            task_retry_times: request.task_retry_times,
            metadata: request.meta.clone(),
            download_middleware: request.download_middleware.clone(),
            data_middleware: request.data_middleware.clone(),
            task_finished: request.task_finished,
            context: request.context.clone(),
            run_id: request.run_id,
            prefix_request: request.prefix_request,
            request_hash: if request.enable_cache {
                Some(request.hash())
            } else {
                None
            },
        })
    }

    async fn do_download(&self, request: Request) -> Result<Response> {
        let request_id = request.id;
        if let Some(seconds) = request.time_sleep_secs {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        }
        if *self.enable_rate_limit.read().await {
            loop {
                // 这里有问题，verify的identifier可以自定义，但是默认设置的rate_config是根据module_id来的
                // 已改进，使用rate_limit_id作为identifier,默认值为module_id
                // 如果使用自定义的identifier会导致始终使用的是默认的rate_config
                // 当前置的Response返回的是一个ParserTask的时候，这里会一直等待，
                // fix: 更改任务锁的释放阶段，在Parser完成后释放锁
                let res = self.limit.verify(&request.limit_id).await;
                match res {
                    Ok(Some(u)) => {
                        // warn!(
                        //     "Rate limit exceeded for id: {}, sleeping for {} ms",
                        //     &request.limit_id, u
                        // );
                        tokio::time::sleep(Duration::from_millis(u)).await;
                    }
                    Ok(None) => break,
                    Err(e) => break,
                }
            }
            self.limit.record(&request.limit_id).await.ok();
        }

        let request = self.process_request(request).await?;
        let cookie_store =
            reqwest_cookie_store::CookieStoreRwLock::new(CookieStore::try_from(&request.cookies)?);
        let cookie_store = Arc::new(cookie_store);
        let headers: HeaderMap = HeaderMap::from(&request.headers);
        // Build reqwest client with optional proxy
        let mut client_builder = Client::builder()
            .cookie_store(true)
            .cookie_provider(cookie_store.clone())
            .default_headers(headers)
            .timeout(Duration::from_secs(request.timeout));

        if let Some(proxy) = &request.proxy {
            let proxy_url = proxy.to_string();
            let reqwest_proxy =
                Proxy::all(&proxy_url).map_err(|e| DownloadError::ClientError(e.into()))?;
            client_builder = client_builder.proxy(reqwest_proxy);
        }

        let client = client_builder
            .build()
            .map_err(|e| DownloadError::ClientError(e.into()))?;
        let method =
            Method::from_str(&request.method).map_err(|e| RequestError::InvalidMethod(e.into()))?;
        let url = match &request.params {
            Some(params) => Url::parse_with_params(&request.url, params)
                .map_err(|e| RequestError::InvalidUrl(e.to_string()))?,
            None => {
                Url::parse(&request.url).map_err(|e| RequestError::InvalidUrl(e.to_string()))?
            }
        };

        let mut request_builder = client.request(method, url);
        if let Some(body) = &request.body {
            request_builder = request_builder.body(body.clone());
        }

        if let Some(form) = &request.form {
            request_builder = request_builder.form(&form);
        }

        if let Some(json) = &request.json {
            request_builder = request_builder.json(&json);
        }
        let response = request_builder
            .send()
            .await
            .map_err(|e| DownloadError::DownloadFailed(e.into()))?;
        // 处理cookie，将cookie_store转为cookies并存储到缓存里
        let cache_enabled = *self.enable_cache.read().await;
        if cache_enabled {
            let cookies = {
                if let Ok(store) = cookie_store.read() {
                    Some(Cookies::from(store.clone()))
                } else {
                    None
                }
            };

            if let Some(cookies) = cookies {
                cookies
                    .send(&request.module_id(), &self.cache_service)
                    .await
                    .ok();
            }
        }

        let response_processed = self.process_response(&request, response).await?;
        Ok(response_processed)
    }
    pub async fn set_limit_config(&self, id: &str, limit: f32) {
        self.limit.set_limit(id, limit).await.ok();
    }
}
#[async_trait::async_trait]
impl Downloader for RequestDownloader {
    async fn set_config(&self, id: &str, config: DownloadConfig) {
        if config.enable_cache {
            // self.clear_cache().await;
        }
        if *self.enable_cache.read().await != config.enable_cache {
            *self.enable_cache.write().await = config.enable_cache;
        }
        if *self.enable_locker.read().await != config.enable_locker {
            *self.enable_locker.write().await = config.enable_locker;
        }
        if *self.enable_rate_limit.read().await != config.enable_rate_limit {
            *self.enable_rate_limit.write().await = config.enable_rate_limit;
        }
        self.limit.set_limit(id, config.rate_limit).await.ok();
    }

    async fn set_limit(&self, id: &str, limit: f32) {
        self.set_limit_config(id, limit).await;
    }

    fn name(&self) -> String {
        "request_downloader".to_string()
    }

    fn version(&self) -> Version {
        Version::parse("0.1.0").unwrap()
    }
    async fn download(&self, request: Request) -> Result<Response> {
        if request.enable_cache
            && let Ok(Some(cached_response)) = self
                .cache_service
                .synchronizer
                .sync(&request.hash(), "response_cache")
                .await
            && let Ok(response) = serde_json::from_value::<Response>(cached_response)
        {
            info!("Cache hit for request: {}", request.id);
            return Ok(Response {
                id: request.id,
                platform: request.platform.clone(),
                account: request.account.clone(),
                module: request.module.clone(),
                status_code: response.status_code,
                cookies: Cookies {
                    cookies: response.cookies.cookies,
                },
                content: response.content,
                headers: response.headers,
                task_retry_times: request.task_retry_times,
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
            });
        }
        let locker_enabled = *self.enable_locker.read().await;
        if locker_enabled {
            // 使用 module_id + run_id 作为锁键，确保同一模块在同一次运行中的请求串行执行
            // 但使用更短的超时时间，减少锁竞争
            let key = format!("task-download-{}-{}", request.module_id(), request.run_id);

            // 尝试获取锁，如果失败则直接执行（避免无限等待）
            // 优化：将超时时间从 5s 减少到 100ms，避免长时间阻塞
            match self
                .locker
                .acquire_lock(&key, 30, Duration::from_secs(5))
                .await
            {
                Ok(_) => {
                    let response = self.do_download(request).await;
                    // 确保释放锁
                    self.locker.release_lock(&key).await.ok();
                    response
                }
                Err(_) => {
                    // 如果获取锁失败，可能是锁竞争太激烈，直接执行
                    warn!(
                        "Failed to acquire download lock for task: {}, proceeding without lock",
                        request.task_id()
                    );
                    self.do_download(request).await
                }
            }
        } else {
            self.do_download(request).await
        }
    }
    async fn health_check(&self) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        // self.clear_cache().await;
        Ok(())
    }
}
