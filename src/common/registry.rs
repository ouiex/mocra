use crate::cacheable::{CacheAble, CacheService};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Information about a registered node in the distributed system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique identifier for the node
    pub id: String,
    /// IP address of the node
    pub ip: String,
    /// Hostname of the node
    pub hostname: String,
    /// Timestamp of the last heartbeat
    pub last_heartbeat: u64,
    /// Software version of the node
    pub version: String,
}

impl CacheAble for NodeInfo {
    fn field() -> impl AsRef<str> {
        "nodes".to_string()
    }
}

/// Registry for managing active nodes in the cluster.
/// 
/// Handles registration, heartbeats, and discovery of other nodes via Redis.
pub struct NodeRegistry {
    cache: Arc<CacheService>,
    node_id: String,
    ttl: Duration,
}

impl NodeRegistry {
    /// Creates a new NodeRegistry instance.
    pub fn new(cache: Arc<CacheService>, node_id: String, ttl: Duration) -> Self {
        Self { cache, node_id, ttl }
    }

    /// Sends a heartbeat to the registry to indicate this node is alive.
    /// 
    /// Updates the node information in Redis with a TTL.
    pub async fn heartbeat(&self, ip: &str, hostname: &str, version: &str) -> Result<(), crate::errors::Error> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let info = NodeInfo {
            id: self.node_id.clone(),
            ip: ip.to_string(),
            hostname: hostname.to_string(),
            last_heartbeat: now,
            version: version.to_string(),
        };
        
        // 1. Save detailed info with TTL (Key retrieval)
        info.send_with_ttl(&self.node_id, &self.cache, self.ttl).await.map_err(crate::errors::Error::from)?;

        // 2. Update ZSET index for efficient querying (Range retrieval)
        // Key: namespace:registry:nodes_index
        let index_key = format!("{}:registry:nodes_index", self.cache.namespace());
        let _ = self.cache.zadd(&index_key, now as f64, self.node_id.as_bytes()).await.map_err(crate::errors::Error::from)?;
        
        Ok(())
    }

    /// Retrieves a list of all currently active nodes in the cluster.
    pub async fn get_active_nodes(&self) -> Result<Vec<NodeInfo>, crate::errors::Error> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let cutoff = now.saturating_sub(self.ttl.as_secs());
        let index_key = format!("{}:registry:nodes_index", self.cache.namespace());

        // 1. Lazy Cleanup: Remove expired nodes from index
        // We do this on read or via a separate maintenance task. Doing on read is fine for registry.
        let _ = self.cache.zremrangebyscore(&index_key, f64::NEG_INFINITY, cutoff as f64).await;

        // 2. Get active Node IDs
        let active_ids_bytes = self.cache.zrangebyscore(&index_key, cutoff as f64, f64::INFINITY).await.map_err(crate::errors::Error::from)?;
        
        if active_ids_bytes.is_empty() {
             return Ok(Vec::new());
        }

        let mut keys = Vec::with_capacity(active_ids_bytes.len());
        for id_bytes in active_ids_bytes {
            let id = String::from_utf8(id_bytes).unwrap_or_default();
            if !id.is_empty() {
                 // Reconstruct the key used by NodeInfo::send_with_ttl
                 // Format: {namespace}:nodes:{id}
                 keys.push(format!("{}:nodes:{}", self.cache.namespace(), id));
            }
        }

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let results = self.cache.mget(&key_refs).await.map_err(crate::errors::Error::from)?;

        let mut nodes = Vec::with_capacity(results.len());
        for val in results {
            if let Some(bytes) = val {
                if let Ok(node) = serde_json::from_slice::<NodeInfo>(&bytes) {
                    nodes.push(node);
                }
            }
        }
        Ok(nodes)
    }

    pub async fn deregister(&self) -> Result<(), crate::errors::Error> {
        NodeInfo::delete(&self.node_id, &self.cache)
            .await
            .map_err(crate::errors::Error::from)
    }
}
