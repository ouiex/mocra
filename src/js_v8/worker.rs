use crate::js_v8::v8::{JsReturn, V8Engine};
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

/// A job submitted to the single-threaded JS worker.
pub struct JsJob {
    pub func: String,
    pub args: Vec<String>,
    pub reply: oneshot::Sender<Result<String, String>>, // normalized string result
}

/// A pool of JS workers to handle concurrent JS execution.
#[derive(Clone)]
pub struct JsWorkerPool {
    workers: Vec<JsWorker>,
    next: Arc<AtomicUsize>,
}

impl JsWorkerPool {
    /// Create a new pool with `size` workers, all loading the same JS file.
    pub fn new(js_path: &Path, size: usize) -> Result<Self, String> {
        let path = js_path.to_path_buf();
        let source = std::fs::read_to_string(&path)
            .map_err(|e| format!("read js file failed: {}: {}", path.display(), e))?;
        Self::new_with_source(source, size)
    }

    /// Create a new pool with `size` workers, all loading the same JS source string.
    pub fn new_with_source(source: String, size: usize) -> Result<Self, String> {
        if size == 0 {
            return Err("Pool size must be greater than 0".to_string());
        }
        let mut workers = Vec::with_capacity(size);
        for i in 0..size {
            workers.push(JsWorker::new_with_source(source.clone()).map_err(|e| format!("Failed to create worker {}: {}", i, e))?);
        }
        Ok(Self {
            workers,
            next: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Call a JS function using a worker from the pool (Round Robin).
    pub async fn call_js(&self, func: &str, args: Vec<String>) -> Result<String, String> {
        if self.workers.is_empty() {
            return Err("No workers available".to_string());
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[idx].call_js(func, args).await
    }
}

/// Single-threaded V8 worker. Owns the V8 engine on a dedicated OS thread.
/// Safe to clone and share; communicates via channels only.
#[derive(Clone)]
pub struct JsWorker {
    tx: Arc<Mutex<mpsc::Sender<JsJob>>>,
}

impl JsWorker {
    /// Spawn a worker thread and load the given JS file into V8.
    pub fn new(js_path: &Path) -> Result<Self, String> {
        let path = js_path.to_path_buf();
        let source = std::fs::read_to_string(&path)
            .map_err(|e| format!("read js file failed: {}: {}", path.display(), e))?;
        Self::new_with_source(source)
    }

    /// Spawn a worker thread and execute the given JS source string.
    pub fn new_with_source(source: String) -> Result<Self, String> {
        let (tx, rx) = mpsc::channel::<JsJob>();
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), String>>();

        std::thread::Builder::new()
            .name("v8-js-worker".to_string())
            .spawn(move || {
                let mut engine = match V8Engine::new() {
                    Ok(e) => e,
                    Err(e) => {
                        let _ = ready_tx.send(Err(format!("init V8 failed: {e}")));
                        return;
                    }
                };
                if let Err(e) = engine.exec_script(&source, Some("worker.js")) {
                    let _ = ready_tx.send(Err(format!("exec script failed: {e}")));
                    return;
                }
                let _ = ready_tx.send(Ok(()));

                while let Ok(job) = rx.recv() {
                    let res = Self::call_engine(&mut engine, &job.func, &job.args)
                        .map_err(|e| e);
                    let _ = job.reply.send(res);
                }
            })
            .map_err(|e| format!("spawn worker failed: {e}"))?;

        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self { tx: Arc::new(Mutex::new(tx)) }),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(format!("worker init channel closed: {e}")),
        }
    }

    /// Call a global JS function with stringified return value.
    pub async fn call_js(&self, func: &str, args: Vec<String>) -> Result<String, String> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job = JsJob { func: func.to_string(), args, reply: reply_tx };
        self.tx
            .lock()
            .map_err(|_| "worker sender poisoned".to_string())?
            .send(job)
            .map_err(|e| format!("send job failed: {e}"))?;
        reply_rx.await.map_err(|e| format!("recv reply failed: {e}"))?
    }

    fn call_engine(engine: &mut V8Engine, func: &str, args: &[String]) -> Result<String, String> {
        let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        match engine.call_function(func, &arg_refs) {
            Ok(JsReturn::Text(s)) => Ok(s),
            Ok(JsReturn::Number(n)) => Ok(n.to_string()),
            Ok(JsReturn::Bool(b)) => Ok(b.to_string()),
            Ok(JsReturn::Json(v)) => Ok(v.to_string()),
            Ok(JsReturn::Bytes(bytes)) => Ok(Self::hex(&bytes)),
            Err(e) => Err(format!("call_function error: {e}")),
        }
    }

    #[inline]
    fn hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_worker_pool() {
        // Create a temporary JS file
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "function add(a, b) {{ return parseInt(a) + parseInt(b); }}").unwrap();
        let path = file.path();

        // Create a pool with 2 workers
        let pool = JsWorkerPool::new(path, 2).unwrap();

        // Test concurrent calls
        let mut handles = vec![];
        for i in 0..10 {
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let res = pool.call_js("add", vec![i.to_string(), "10".to_string()]).await;
                (i, res)
            }));
        }

        for handle in handles {
            let (i, res) = handle.await.unwrap();
            assert_eq!(res.unwrap(), (i + 10).to_string());
        }
    }
}

