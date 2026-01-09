use async_trait::async_trait;
use common::interface::DownloadMiddleware;
use common::model::{Request, Response, ModuleConfig};
use log::warn;
use std::path::Path;
use std::sync::Arc;
use js_v8::JsWorker;

pub(crate) struct DouyinEncryptMiddleware {
    worker: JsWorker,
}

impl DouyinEncryptMiddleware {
    pub fn new(js_path: &Path) -> Self {
        let worker = JsWorker::new(js_path).unwrap_or_else(|e| {
            eprintln!("Failed to init JsWorker with {}: {}", js_path.display(), e);
            std::process::exit(1);
        });
        Self { worker }
    }
    #[inline]
    fn base36(mut v: u128) -> String {
        // Fast path for zero
        if v == 0 {
            return "0".to_string();
        }

        // u128 fits in at most 25 base36 digits
        let mut buf: Vec<u8> = Vec::with_capacity(25);
        while v > 0 {
            let rem = (v % 36) as u8;
            let b = if rem < 10 {
                b'0' + rem
            } else {
                b'a' + (rem - 10)
            };
            buf.push(b);
            v /= 36;
        }
        buf.reverse();
        // ASCII-only, so this cannot fail
        String::from_utf8(buf).unwrap()
    }

    #[inline]
    fn fp_token() -> String {
        use rand::Rng;

        // Static character table: 0-9, A-Z, a-z
        const E: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        // Python: n = convert_to_base36(int(time.time() * 1000))
        let n = Self::base36(now.as_millis());

        // r buffer (36 chars). Use 0 as sentinel for "unset".
        let mut r = [0u8; 36];

        // r[8] = r[13] = r[18] = r[23] = "_"; r[14] = "4"
        r[8] = b'_';
        r[13] = b'_';
        r[18] = b'_';
        r[23] = b'_';
        r[14] = b'4';

        let mut rng = rand::thread_rng();
        let t = E.len();
        for i in 0..36 {
            if r[i] == 0 {
                let o: usize = rng.gen_range(0..t);
                r[i] = if i == 19 {
                    E[((o & 3) | 8) as usize]
                } else {
                    E[o]
                };
            }
        }

        // Preallocate final string: "verify_" + n + '_' + 36
        let mut out = String::with_capacity("verify_".len() + n.len() + 1 + 36);
        out.push_str("verify_");
        out.push_str(&n);
        out.push('_');
        out.push_str(std::str::from_utf8(&r).unwrap());
        out
    }
    #[inline]
    fn lid_token() -> String {
        use rand::Rng;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();

        let ms_str = now_ms.to_string();
        // Equivalent to Python: str(int(time.time() * 1000))[5:]
        let start = ms_str.len().min(5);
        let tail = &ms_str[start..];

        // Equivalent to Python: str(random.random())[2:6]
        // Use a 4-digit zero-padded random number [0000-9999]
        let mut rng = rand::thread_rng();
        let four = rng.gen_range(0..10000);
        let rand_part = format!("{:04}", four);

        let mut out = String::with_capacity(tail.len() + 4);
        out.push_str(tail);
        out.push_str(&rand_part);
        out
    }
    #[inline]
    fn ms_token() -> String {
        use rand::Rng;
        // baseStr from Python version
        const BASE: &[u8] = b"ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789=_-";
        let mut rng = rand::thread_rng();
        // default random length = 132
        let len = 184usize;
        let mut out = String::with_capacity(len);
        for _ in 0..len {
            let idx = rng.gen_range(0..BASE.len());
            out.push(BASE[idx] as char);
        }
        out
    }
}

#[async_trait]
impl DownloadMiddleware for DouyinEncryptMiddleware {
    fn name(&self) -> String {
        "douyin_encrypt_middleware".to_string()
    }

    async fn handle_request(&self, request: Request, _config: &Option<ModuleConfig>) -> Request {
        let mut request = request;
        let fp = request
            .cookies
            .cookies
            .iter()
            .filter(|x| x.name == "s_v_web_id")
            .map(|x| x.value.clone())
            .next()
            .unwrap_or_else(|| DouyinEncryptMiddleware::fp_token());
        let lid = DouyinEncryptMiddleware::lid_token();
        let ms = DouyinEncryptMiddleware::ms_token();
        let params = if let Some(p) = request.params.as_mut() {
            p
        } else {
            return request;
        };

        if let Some((_, v)) = params.iter_mut().find(|(k, _)| *k == "fp") {
            *v = fp.clone();
        } else {
            params.push(("fp".to_string(), fp.clone()));
        }
        if let Some((_, v)) = params.iter_mut().find(|(k, _)| *k == "verifyFp") {
            *v = fp;
        } else {
            params.push(("verifyFp".to_string(), fp));
        }
        if let Some((_, v)) = params.iter_mut().find(|(k, _)| *k == "_lid") {
            *v = lid;
        } else {
            params.push(("_lid".to_string(), lid));
        }
        if let Some((_, v)) = params.iter_mut().find(|(k, _)| *k == "msToken") {
            *v = ms;
        } else {
            params.push(("msToken".to_string(), ms));
        }

        let data = request
            .json
            .clone()
            .map(|x| x.to_string())
            .unwrap_or("".to_string());
        let user_agent = request
            .headers
            .headers
            .iter()
            .find(|k| k.key.to_lowercase() == "user-agent")
            .map(|v| v.value.clone())
            .unwrap_or("".to_string());
        let args = vec![
            serde_urlencoded::to_string(&params).unwrap_or_default(),
            data,
            user_agent,
        ];
        let bogus = self.worker.call_js("getABogus", args).await;
        match bogus {
            Ok(body) => {
                if let Some((_, v)) = params.iter_mut().find(|(k, _)| *k == "a_bogus") {
                    *v = body;
                } else {
                    params.push(("a_bogus".to_string(), body));
                }
            }
            Err(e) => {
                warn!("add a_bogus failed: {}", e);
                return request;
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
        // Default to loading bdms.js from the repository js folder (adjust if necessary)
        let path = Path::new("/Users/eason/RustroverProjects/mocra/tests/js/bdms.js");
        Arc::new(DouyinEncryptMiddleware::new(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use serde_urlencoded;
    #[tokio::test]
    async fn test() {
        let fp = DouyinEncryptMiddleware::fp_token();
        let lid = DouyinEncryptMiddleware::lid_token();
        let ms = DouyinEncryptMiddleware::ms_token();
        println!("fp: {}, len={}", fp, fp.len());
        let worker = JsWorker::new(Path::new("/Users/eason/crawler/crawler/js/bdms.js")).unwrap();

        let js = json!(
            {"a_type":0,"date_type":24,"begin_date":1760371200,"end_date":1760371200,"version":"qc"}
        )
        .to_string();

        let mut params = vec![
            (
                "file_name",
                "抖音直播明细_2025-07-17~2025-10-14_数据更新日期2025-10-14.xlsx",
            ),
            (
                "download_scene_identity",
                "compass_live_live_list_liveroom_detail",
            ),
            ("req_json_str", &js),
            ("sheet_name_list", "抖音直播"),
            ("fp", &fp),
            ("lid", &lid),
            ("verifyFp", &fp),
            ("_lid", &lid),
            ("msToken", &ms),
        ];
        let args = vec![
            serde_urlencoded::to_string(&params).unwrap(),
            "".to_string(),
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36".to_string()
        ];
        let bogus = worker.call_js("getABogus", args).await.unwrap();
        params.push(("a_bogus", bogus.as_str()));
        println!("params: {:#?}", params);
    }
}
