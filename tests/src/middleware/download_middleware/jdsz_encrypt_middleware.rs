use async_trait::async_trait;
use common::interface::DownloadMiddleware;
use common::model::{Request, Response, ModuleConfig, Headers};
use rand::Rng;
use regex::Regex;
use sha1::{Digest, Sha1};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

struct SignData {
    mup: String,
    mnp: String,
    uuid: String,
}
pub struct JdszEncryptMiddleware;
impl JdszEncryptMiddleware {
    fn params_sign(url: String, cookie: String, ua: String, referer: String) -> SignData {
        let mut rng = rand::rng();
        let s_rand: u64 = rng.random_range(0..1_000_000_000u64);
        let p_ms: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let bw: u32 = rng.random_range(1000..=2000);
        let bh: u32 = rng.random_range(1000..=2000);

        let c = format!(
            "{}{}-{}-{}-{}-{}-{}-{}",
            bw, bh, referer, ua, cookie, s_rand, p_ms, url
        );

        let mut hasher = Sha1::new();
        hasher.update(c.as_bytes());
        let sha1_hex: String = hasher
            .finalize()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        let t = format!("{}-{:x}", &sha1_hex[..20], p_ms);

        let re_host = Regex::new(r"(?:http:|https:)?//[^/]+/").unwrap();
        let mut url2 = re_host.replace(&url, "/").into_owned();
        let re_ws = Regex::new(r"\s+").unwrap();
        url2 = re_ws.replace(&url2, "").into_owned();
        let re_qs = Regex::new(r"\?.*").unwrap();
        url2 = re_qs.replace(&url2, "").into_owned();

        let r_now: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let sign_src = format!("{}{}{}372ad2c2b6", url2, t, r_now);
        let md5_hex = format!("{:x}", md5::compute(sign_src.as_bytes()));
        SignData {
            mup: r_now.to_string(),
            mnp: md5_hex,
            uuid: t,
        }
    }
    fn headers_sign(url: String) -> SignData {
        let re_host = Regex::new(r"(?:http:|https:)?//[^/]+/").unwrap();
        let mut url2 = re_host.replace(&url, "/").into_owned();
        let re_ws = Regex::new(r"\s+").unwrap();
        url2 = re_ws.replace(&url2, "").into_owned();
        let re_qs = Regex::new(r"\?.*").unwrap();
        url2 = re_qs.replace(&url2, "").into_owned();
        let r_now: u128 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let uuid = Uuid::new_v4().to_string();
        let r = format!("{url2}{uuid}{r_now}372ad2c2b6");
        SignData {
            mup: r_now.to_string(),
            mnp: format!("{:x}", md5::compute(r.as_bytes())),
            uuid,
        }
    }

}
#[async_trait]
impl DownloadMiddleware for JdszEncryptMiddleware {
    fn name(&self) -> String {
        "jdsz_encrypt_middleware".to_string()
    }

    async fn handle_request(&self, request: Request, _config: &Option<ModuleConfig>) -> Request {
        let mut request = request;
        let headers = &mut request.headers;
        let url = request.url.clone();

        let ua = headers
            .headers
            .iter()
            .find(|x| x.key.eq_ignore_ascii_case("user-agent"))
            .map(|x| x.value.clone())
            .unwrap_or_default();

        if headers.contains("uuid") {
            let encrypt_data = JdszEncryptMiddleware::headers_sign(url.clone());
            let encrypt_headers = Headers::new()
                .add("user-mup", encrypt_data.mup)
                .add("user-mnp", encrypt_data.mnp)
                .add("uuid", encrypt_data.uuid);
            headers.merge(&encrypt_headers);
            return request;
        }
        let cookie_str = request.cookies.str_by_domain(&[".jd.com"]);
        let referer = headers.get("referer").unwrap_or_default();
        let encrypt_data =
            JdszEncryptMiddleware::params_sign(url.clone(), cookie_str, ua, referer);
        if let Some(params) = &mut request.params
            && params.iter().any(|(k, _)| k.eq_ignore_ascii_case("uuid"))
        {
            if let Some((_, v)) = params
                .iter_mut()
                .find(|(k, _)| k.eq_ignore_ascii_case("user-mup"))
            {
                *v = encrypt_data.mup;
            } else {
                params.push(("User-mup".to_string(), encrypt_data.mup));
            }
            if let Some((_, v)) = params
                .iter_mut()
                .find(|(k, _)| k.eq_ignore_ascii_case("user-mnp"))
            {
                *v = encrypt_data.mnp;
            } else {
                params.push(("User-mnp".to_string(), encrypt_data.mnp));
            }
            if let Some((_, v)) = params
                .iter_mut()
                .find(|(k, _)| k.eq_ignore_ascii_case("uuid"))
            {
                *v = encrypt_data.uuid;
            } else {
                params.push(("uuid".to_string(), encrypt_data.uuid));
            }
            if let Some((_,v)) = params.iter_mut().find(|(k, _)| k.eq_ignore_ascii_case("_t")) {
                *v = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    .to_string();
            }
            return request;
        }
        if let Some(data) = &mut request.form
            && let Some(form) = data.as_object_mut()
            && form.contains_key("uuid")
        {
            if let Some(uuid) = form.get_mut("uuid") {
                *uuid = serde_json::Value::String(encrypt_data.uuid);
            }
            else {
                form.insert("uuid".to_string(), serde_json::Value::String(encrypt_data.uuid));
            }
            if let Some(data) = form.get_mut("User-mnp") {
                *data = serde_json::Value::String(encrypt_data.mnp);
            }
            else {
                form.insert("User-mnp".to_string(), serde_json::Value::String(encrypt_data.mnp));
            }
            if let Some(data) = form.get_mut("User-mup") {
                *data = serde_json::Value::String(encrypt_data.mup);
            }
            else {
                form.insert("User-mup".to_string(), serde_json::Value::String(encrypt_data.mup));
            }
            return request;
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
        Arc::new(JdszEncryptMiddleware {})
    }
}
