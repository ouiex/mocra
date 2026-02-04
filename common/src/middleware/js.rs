use crate::interface::DataMiddleware;
use crate::model::data::Data;
use crate::model::ModuleConfig;
use async_trait::async_trait;
use js_v8::JsWorkerPool;
use log::{error, info};
use redis::AsyncCommands;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::Duration;

/// Middleware that executes JavaScript code to process data.
///
/// Uses V8 engine to execute scripts stored in Redis.
/// Scripts are automatically reloaded when they change in Redis.
pub struct JsMiddleware {
    pool: Arc<RwLock<JsWorkerPool>>,
    name: String,
}

impl JsMiddleware {
    /// Creates a new JsMiddleware instance.
    ///
    /// # Arguments
    /// * `redis_url` - URL of the Redis server
    /// * `script_key` - Redis key containing the JavaScript source code
    /// * `worker_count` - Number of V8 isolates to create
    pub async fn new(redis_url: &str, script_key: &str, worker_count: usize) -> Result<Self, String> {
        let client = redis::Client::open(redis_url).map_err(|e| e.to_string())?;
        let mut con = client.get_multiplexed_async_connection().await.map_err(|e| e.to_string())?;
        
        let script: String = con.get(script_key).await.map_err(|e| e.to_string())?;
        
        let pool = JsWorkerPool::new_with_source(script.clone(), worker_count)?;
        let pool_arc = Arc::new(RwLock::new(pool));
        
        // Spawn watcher to reload script on change
        let pool_clone = pool_arc.clone();
        let client_clone = client.clone();
        let key_clone = script_key.to_string();
        
        tokio::spawn(async move {
            let mut last_script = script;
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let mut con = match client_clone.get_multiplexed_async_connection().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("JsMiddleware: Failed to connect to redis: {}", e);
                        continue;
                    }
                };
                
                let current_script: String = match con.get(&key_clone).await {
                    Ok(s) => s,
                    Err(e) => {
                         error!("JsMiddleware: Failed to get script: {}", e);
                         continue;
                    }
                };
                
                if current_script != last_script {
                    info!("JsMiddleware: Script changed, reloading...");
                    match JsWorkerPool::new_with_source(current_script.clone(), worker_count) {
                        Ok(new_pool) => {
                            let mut w = pool_clone.write().await;
                            *w = new_pool;
                            last_script = current_script;
                            info!("JsMiddleware: Reloaded successfully");
                        },
                        Err(e) => error!("JsMiddleware: Failed to create new pool: {}", e),
                    }
                }
            }
        });

        Ok(Self {
            pool: pool_arc,
            name: format!("JsMiddleware:{}", script_key),
        })
    }
}

#[async_trait]
impl DataMiddleware for JsMiddleware {
    fn name(&self) -> String {
        self.name.clone()
    }

    async fn handle_data(&self, data: Data, _config: &Option<ModuleConfig>) -> Data {
        let pool = self.pool.read().await;
        // Serialize data to JSON string to pass to JS
        let json_data = match serde_json::to_string(&data) {
            Ok(s) => s,
            Err(e) => {
                error!("JsMiddleware: Failed to serialize data: {}", e);
                return data;
            }
        };
        
        // Call "process" function in JS
        match pool.call_js("process", vec![json_data]).await {
            Ok(res_str) => {
                // Parse result back to Data
                // If JS returns string, we might need to parse it. 
                // Assuming JS returns modified Data object as JSON.
                match serde_json::from_str::<Data>(&res_str) {
                    Ok(new_data) => new_data,
                    Err(e) => {
                        error!("JsMiddleware: Failed to parse result: {}", e);
                        data
                    }
                }
            },
            Err(e) => {
                 error!("JsMiddleware: JS execution failed: {}", e);
                 data
            }
        }
    }
    
    fn default_arc() -> Arc<dyn DataMiddleware> where Self: Sized {
        // This is tricky as we need async initialization.
        // For now panic or return a placeholder?
        // Traits usually don't support async new in default_arc.
        // We might not use this method.
        unimplemented!("JsMiddleware cannot be created synchronously via default_arc")
    }
}
