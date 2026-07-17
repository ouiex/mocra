//! Embedded cluster example: bring up a **self-forming distributed collection node**, with no
//! external coordinator.
//!
//! The control plane (leader election / locks / KV / membership / partition ownership) uses
//! embedded redb+Raft; the data plane is a pluggable MQ (single-node in-memory here). "Register
//! with any node to join the network": every node after the first just needs a seed address.
//!
//! Bring up a 3-node cluster (three terminals; the first node bootstraps, the rest join):
//!
//! ```bash
//! # The first core node (seeds empty → bootstraps a new cluster)
//! cargo run --example cluster_quickstart --features cluster-embedded -- 1 127.0.0.1:7001
//! # The remaining nodes (third argument = seed = the first node's address → join)
//! cargo run --example cluster_quickstart --features cluster-embedded -- 2 127.0.0.1:7002 127.0.0.1:7001
//! cargo run --example cluster_quickstart --features cluster-embedded -- 3 127.0.0.1:7003 127.0.0.1:7001
//! ```
//!
//! Containerized deployments can use environment variables instead (the same image across nodes,
//! with only the env changing): `MOCRA_NODE_ID` / `MOCRA_HTTP_ADDR` / `MOCRA_DATA_DIR` /
//! `MOCRA_SEEDS`, read in code via [`ClusterConfig::from_env`].

use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct Page {
    url: String,
    status: u16,
}

struct Httpbin;

#[async_trait]
impl Spider for Httpbin {
    type Item = Page;

    fn name(&self) -> &str {
        "httpbin"
    }

    async fn start(&self, s: &mut Seeds) {
        s.get("https://httpbin.org/get");
    }

    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        cx.emit(Page {
            url: res.module_id(),
            status: res.status_code,
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Usage: cluster_quickstart <node_id> <http_addr> [seed_addr]
    // With no command-line arguments, fall back to environment variables (the containerized
    // deployment path).
    let args: Vec<String> = std::env::args().collect();
    let cluster = if args.len() >= 3 {
        let node_id: u64 = args[1].parse().unwrap_or(1);
        let http_addr = args[2].clone();
        let data_dir = format!("./mocra-data/node-{node_id}");
        match args.get(3) {
            Some(seed) => ClusterConfig::join(node_id, http_addr, data_dir, seed.clone()),
            None => ClusterConfig::bootstrap(node_id, http_addr, data_dir),
        }
    } else {
        // Read from MOCRA_NODE_ID / MOCRA_HTTP_ADDR / MOCRA_DATA_DIR / MOCRA_SEEDS.
        ClusterConfig::from_env().map_err(|e| Error::new(ErrorKind::Service, Some(e)))?
    };
    println!(
        "starting mocra cluster node {} @ {} ({})",
        cluster.node_id,
        cluster.http_addr,
        if cluster.seeds.is_empty() {
            "bootstrap"
        } else {
            "join"
        }
    );

    Mocra::builder()
        .spider(
            Httpbin,
            on_item(|p: Page| async move {
                println!("[item] {} -> {}", p.url, p.status);
            }),
        )
        .cluster(cluster)
        .run()
        .await
}
