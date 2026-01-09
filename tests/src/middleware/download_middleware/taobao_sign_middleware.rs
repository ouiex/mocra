use common::interface::DownloadMiddleware;
use common::model::{Request, Response, ModuleConfig};
use std::sync::Arc;
use utils::encrypt::md5;
pub struct TaobaoSignMiddleware {}
#[async_trait::async_trait]
impl DownloadMiddleware for TaobaoSignMiddleware {
    fn name(&self) -> String {
        "taobao_sign_middleware".to_string()
    }

    async fn handle_request(&self, mut request: Request, config: &Option<ModuleConfig>) -> Request {
        let app_key = config
            .as_ref()
            .and_then(|c| c.get_config::<u32>("app_key"))
            .unwrap_or(12574478);

        let token = request
            .cookies
            .cookies
            .iter()
            .find(|c| c.name == "_m_h5_tk")
            .and_then(|c| {
                c.value
                    .clone()
                    .split('_')
                    .nth(0).map(|x| x.to_string())
            });

        let params = request.params.as_mut();
        let t = params.as_ref().and_then(|x| {
            x.iter()
                .filter(|s| s.0 == "t")
                .map(|s| s.1.parse::<u128>().ok())
                .next()
                .flatten()
        });
        let data = params.as_ref().and_then(|x| {
            x.iter()
                .filter(|s| s.0 == "data")
                .map(|s| s.1.clone())
                .next()
        });
        let sign = if let Some(t) = t
            && let Some(data) = data
            && let Some(token) = token
        {
            let sign_str = format!("{token}&{t}&{app_key}&{data}");
            Some(md5(sign_str.as_bytes()))
        } else {
            None
        };
        if let Some(p) = params
            && let Some(sign) = sign
        {
            if let Some((_, v)) = p.iter_mut().find(|(k, _)| k == "sign") {
                *v = sign;
            } else {
                p.push(("sign".to_string(), sign));
            }
        }
        request
    }

    async fn handle_response(&self, response: Response, _config: &Option<ModuleConfig>) -> Response {
        response
    }

    fn default_arc() -> Arc<dyn DownloadMiddleware>
    where
        Self: Sized,
    {
        Arc::new(TaobaoSignMiddleware {})
    }
}
