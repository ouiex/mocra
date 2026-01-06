use tokio::sync::mpsc;

#[async_trait::async_trait]
pub trait MqBackend: Send + Sync {
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), String>;
    async fn subscribe(&self, topic: &str) -> Result<mpsc::Receiver<Vec<u8>>, String>;
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), String>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    // Optimistic Lock (CAS)
    async fn cas(&self, key: &str, old_val: Option<&[u8]>, new_val: &[u8]) -> Result<bool, String>;
}
