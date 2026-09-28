use crate::common::model::{ModuleConfig, Request};
use crate::common::processors::processor::RetryPolicy;
use crate::errors::{DownloadError, Error, Result};
use metrics::counter;
use mocra_proxy::{ProxyEnum, ProxyManager};
use serde_json::{Map, Value};
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Instant;

const FAILED_PROXY_KEY: &str = "mocra_failed_proxy";

pub(super) fn can_rotate(request: &Request) -> bool {
    matches!(
        request.method.to_ascii_uppercase().as_str(),
        "GET" | "HEAD" | "OPTIONS" | "PUT" | "DELETE" | "WSS"
    )
}

pub(super) fn failed_proxy(policy: &RetryPolicy) -> Option<ProxyEnum> {
    serde_json::from_value(policy.meta.get(FAILED_PROXY_KEY)?.clone()).ok()
}

pub(super) fn mark_proxy_failure(policy: &mut RetryPolicy, proxy: &ProxyEnum) {
    if !policy.meta.is_object() {
        policy.meta = Value::Object(Map::new());
    }
    if let Some(map) = policy.meta.as_object_mut() {
        if let Ok(proxy) = serde_json::to_value(proxy) {
            map.insert(FAILED_PROXY_KEY.to_string(), proxy);
        }
    }
}

pub(super) fn clear_proxy_failure(policy: &mut RetryPolicy) {
    if let Some(map) = policy.meta.as_object_mut() {
        map.remove(FAILED_PROXY_KEY);
    }
}

pub(super) async fn select_retry_proxy(
    manager: &Option<Arc<ProxyManager>>,
    request: &mut Request,
    config: &Option<ModuleConfig>,
    policy: &Option<RetryPolicy>,
) {
    let Some(policy) = policy else { return };
    if policy.current_retry == 0 || !can_rotate(request) {
        return;
    }
    let Some(failed) = failed_proxy(policy) else {
        return;
    };
    let allow_explicit = config.as_ref().is_some_and(|config| {
        config
            .get_config::<bool>("auto_rotate_explicit_proxy")
            .unwrap_or(false)
    });
    if !request.proxy_from_pool && !allow_explicit {
        return;
    }
    let Some(manager) = manager else { return };
    match manager.get_proxy_for_attempt(Some(&failed)).await {
        Ok(next) => {
            request.proxy = Some(next);
            request.proxy_from_pool = true;
            counter!("mocra_proxy_rotations_total").increment(1);
        }
        Err(error) => {
            // The existing retry budget still bounds attempts when the pool has one proxy.
            log::warn!("no alternate proxy for request {}: {error}", request.id);
        }
    }
}

pub(super) fn is_proxy_failure(error: &Error) -> bool {
    if error.is_proxy() {
        return true;
    }
    let mut source: Option<&(dyn StdError + 'static)> = error.source();
    while let Some(current) = source {
        if let Some(download) = current.downcast_ref::<DownloadError>() {
            if matches!(
                download,
                DownloadError::NetworkError(_)
                    | DownloadError::TimeoutError(_)
                    | DownloadError::InvalidProxy(_)
            ) {
                return true;
            }
            if matches!(
                download,
                DownloadError::ProxyHandshakeStatus(407) | DownloadError::ProxyRetryStatus(_)
            ) {
                return true;
            }
        }
        if let Some(network) = current.downcast_ref::<reqwest::Error>() {
            return network.is_connect()
                || network.is_timeout()
                || network
                    .status()
                    .is_some_and(|status| status.as_u16() == 407);
        }
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::NotConnected
            ) {
                return true;
            }
        }
        source = current.source();
    }
    false
}

pub(super) fn proxy_status_code(error: &Error) -> Option<u16> {
    let mut source: Option<&(dyn StdError + 'static)> = error.source();
    while let Some(current) = source {
        if let Some(DownloadError::ProxyHandshakeStatus(code)) =
            current.downcast_ref::<DownloadError>()
        {
            return Some(*code);
        }
        source = current.source();
    }
    None
}

pub(super) struct ProxyAttempt {
    manager: Arc<ProxyManager>,
    proxy: ProxyEnum,
    started: Instant,
    reported: bool,
}

impl ProxyAttempt {
    pub(super) async fn begin(
        manager: &Option<Arc<ProxyManager>>,
        request: &Request,
    ) -> Result<Option<Self>> {
        if !request.proxy_from_pool {
            return Ok(None);
        }
        let Some(manager) = manager else {
            return Ok(None);
        };
        let Some(proxy) = request.proxy.clone() else {
            return Ok(None);
        };
        manager
            .begin_proxy_attempt(&proxy)
            .await
            .map_err(Error::from)?;
        Ok(Some(Self {
            manager: manager.clone(),
            proxy,
            started: Instant::now(),
            reported: false,
        }))
    }

    pub(super) async fn finish(&mut self, success: bool) {
        if self.reported {
            return;
        }
        self.reported = true;
        if !success {
            counter!("mocra_proxy_faults_total").increment(1);
        }
        let manager = self.manager.clone();
        let proxy = self.proxy.clone();
        let elapsed = self.started.elapsed();
        let report = tokio::spawn(async move {
            if let Err(error) = manager
                .report_proxy_result(&proxy, success, Some(elapsed))
                .await
            {
                log::warn!("proxy attempt feedback failed: {error}");
            }
        });
        let _ = report.await;
    }
}

impl Drop for ProxyAttempt {
    fn drop(&mut self) {
        if self.reported {
            return;
        }
        self.reported = true;
        counter!("mocra_proxy_faults_total").increment(1);
        let manager = self.manager.clone();
        let proxy = self.proxy.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(error) = manager.report_failure(&proxy).await {
                    log::warn!("cancelled proxy attempt feedback failed: {error}");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::model::ModuleConfig;
    use mocra_proxy::{DirectProxy, PoolConfig, ProxyConfig};
    use serde_json::json;
    use std::time::Duration;

    async fn manager() -> Arc<ProxyManager> {
        Arc::new(
            ProxyManager::from_proxy_config(&ProxyConfig {
                tunnel: None,
                direct: Some(vec![
                    DirectProxy {
                        name: Some("a".into()),
                        url: "http://127.0.0.1:8080".into(),
                        rate_limit: Some(10.0),
                        expire_time: None,
                    },
                    DirectProxy {
                        name: Some("b".into()),
                        url: "http://127.0.0.1:8081".into(),
                        rate_limit: Some(10.0),
                        expire_time: None,
                    },
                ]),
                ip_provider: None,
                pool_config: Some(PoolConfig {
                    max_errors: 2,
                    ..PoolConfig::default()
                }),
            })
            .await
            .unwrap(),
        )
    }

    #[test]
    fn only_transport_errors_are_proxy_faults() {
        assert!(is_proxy_failure(
            &DownloadError::NetworkError("connect".into()).into()
        ));
        assert!(is_proxy_failure(
            &DownloadError::TimeoutError("timeout".into()).into()
        ));
        assert!(is_proxy_failure(
            &DownloadError::ProxyHandshakeStatus(407).into()
        ));
        assert!(!is_proxy_failure(
            &DownloadError::ProxyHandshakeStatus(503).into()
        ));
        assert!(!is_proxy_failure(
            &DownloadError::InvalidResponse("target status".into()).into()
        ));
    }

    #[tokio::test]
    async fn failed_managed_proxy_rotates_for_get_but_not_post() {
        let manager = manager().await;
        let first = manager.get_proxy_for_attempt(None).await.unwrap();
        let mut policy = RetryPolicy::default();
        policy.current_retry = 1;
        mark_proxy_failure(&mut policy, &first);
        let mut request = Request::new("http://example.com", "GET");
        request.proxy = Some(first.clone());
        request.proxy_from_pool = true;
        select_retry_proxy(
            &Some(manager.clone()),
            &mut request,
            &None,
            &Some(policy.clone()),
        )
        .await;
        assert_ne!(request.proxy.unwrap().to_string(), first.to_string());

        let mut post = Request::new("http://example.com", "POST");
        post.proxy = Some(first.clone());
        post.proxy_from_pool = true;
        select_retry_proxy(&Some(manager), &mut post, &None, &Some(policy)).await;
        assert_eq!(post.proxy.unwrap().to_string(), first.to_string());
    }

    #[tokio::test]
    async fn explicit_proxy_rotates_only_with_config() {
        let manager = manager().await;
        let first = manager.get_proxy_for_attempt(None).await.unwrap();
        let mut policy = RetryPolicy::default();
        policy.current_retry = 1;
        mark_proxy_failure(&mut policy, &first);
        let mut request = Request::new("http://example.com", "GET");
        request.proxy = Some(first.clone());
        select_retry_proxy(
            &Some(manager.clone()),
            &mut request,
            &None,
            &Some(policy.clone()),
        )
        .await;
        assert_eq!(
            request.proxy.as_ref().unwrap().to_string(),
            first.to_string()
        );
        let config = ModuleConfig {
            module_config: json!({"auto_rotate_explicit_proxy": true}),
            ..ModuleConfig::default()
        };
        select_retry_proxy(&Some(manager), &mut request, &Some(config), &Some(policy)).await;
        assert!(request.proxy_from_pool);
        assert_ne!(request.proxy.unwrap().to_string(), first.to_string());
    }

    #[tokio::test]
    async fn cancelled_attempt_reports_once() {
        let manager = manager().await;
        let mut request = Request::new("http://example.com", "GET");
        request.proxy = Some(manager.get_proxy_for_attempt(None).await.unwrap());
        request.proxy_from_pool = true;
        let attempt = ProxyAttempt::begin(&Some(manager.clone()), &request)
            .await
            .unwrap()
            .unwrap();
        drop(attempt);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if manager.get_detailed_stats().await.avg_success_rate == 0.5 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let stats = manager.get_detailed_stats().await;
        assert_eq!(
            stats
                .providers
                .values()
                .map(|provider| provider.total_proxies)
                .sum::<usize>(),
            2
        );
        assert_eq!(stats.avg_success_rate, 0.5);
        assert_eq!(stats.valid_proxies, 2);
    }
}
