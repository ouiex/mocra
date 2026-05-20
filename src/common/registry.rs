use serde::{Deserialize, Serialize};

/// Shared node metadata persisted in the control-plane state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeInfo {
    /// Unique identifier for the node
    pub id: String,
    /// IP address of the node
    pub ip: String,
    /// Hostname of the node
    pub hostname: String,
    /// HTTP API port for protected node-to-node debug/control calls.
    #[serde(default)]
    pub api_port: Option<u16>,
    /// Timestamp of the last heartbeat
    pub last_heartbeat: u64,
    /// Software version of the node
    pub version: String,
}

#[cfg(test)]
mod tests {
    use super::NodeInfo;

    #[test]
    fn node_info_deserializes_payload_without_api_port() {
        let node: NodeInfo = serde_json::from_str(
            r#"{
                "id": "node-a",
                "ip": "127.0.0.1",
                "hostname": "host-a",
                "last_heartbeat": 42,
                "version": "0.2.16"
            }"#,
        )
        .expect("node info should deserialize");

        assert_eq!(node.api_port, None);
        assert_eq!(node.id, "node-a");
    }

    #[test]
    fn node_info_msgpack_round_trips_without_api_port() {
        let node = NodeInfo {
            id: "node-a".to_string(),
            ip: "127.0.0.1".to_string(),
            hostname: "host-a".to_string(),
            api_port: None,
            last_heartbeat: 42,
            version: "0.2.16".to_string(),
        };

        let bytes = rmp_serde::to_vec(&node).expect("node info should serialize to msgpack");
        let decoded: NodeInfo =
            rmp_serde::from_slice(&bytes).expect("node info should deserialize from msgpack");

        assert_eq!(decoded.api_port, None);
        assert_eq!(decoded.id, "node-a");
        assert_eq!(decoded.last_heartbeat, 42);
    }
}
