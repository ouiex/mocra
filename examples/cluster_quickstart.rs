//! 内嵌集群示例:起一个**自组网的分布式采集节点**,无需外部协调器。
//!
//! 控制面(选举 / 锁 / KV / 成员 / 分区归属)用内嵌 redb+Raft;数据面可插拔 MQ
//! (此处单机内存)。「注册到任意节点即入网」:非首节点带上种子地址即可 join。
//!
//! 起一个 3 节点集群(三个终端,首节点自举,其余 join):
//!
//! ```bash
//! # 首个核心节点(seeds 为空 → 自举新集群)
//! cargo run --example cluster_quickstart --features cluster-embedded -- 1 127.0.0.1:7001
//! # 其余节点(第三个参数 = 种子 = 首节点地址 → join)
//! cargo run --example cluster_quickstart --features cluster-embedded -- 2 127.0.0.1:7002 127.0.0.1:7001
//! cargo run --example cluster_quickstart --features cluster-embedded -- 3 127.0.0.1:7003 127.0.0.1:7001
//! ```
//!
//! 容器化部署可改用环境变量(同一镜像跨节点,仅换 env):
//! `MOCRA_NODE_ID` / `MOCRA_HTTP_ADDR` / `MOCRA_DATA_DIR` / `MOCRA_SEEDS`,
//! 代码里用 [`ClusterConfig::from_env`] 读取。

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
    // 用法:cluster_quickstart <node_id> <http_addr> [seed_addr]
    // 无命令行参数时回退到环境变量(容器化部署路径)。
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
        // 从 MOCRA_NODE_ID / MOCRA_HTTP_ADDR / MOCRA_DATA_DIR / MOCRA_SEEDS 读取。
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
