//! Docker cluster acceptance node for the single `mocra` package.
//!
//! Run the complete three-voter Raft + NATS crawling and recovery checks from the repository:
//! `python3 examples/cluster_acceptance/run.py`.
//!
//! `init` initializes the three-voter membership before `crawl` restores the same Raft volumes.
//! `blob-probe` publishes an offloaded response for another container to reload and parse.
//! Runtime files are provided by the example's Compose mounts; see its README for prerequisites.

use async_trait::async_trait;
use mocra::cluster::RaftControlPlane;
use mocra::prelude::*;
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const QUOTES: &str = "https://quotes.toscrape.com";
const CORPUS: &str = "http://site:8000";
type AnyError = Box<dyn std::error::Error + Send + Sync>;

fn parse_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Parser, Some(message.into()))
}

fn tracked_get(url: String) -> Request {
    let mut request = Request::new(&url, RequestMethod::Get).add_meta("source_url", url);
    request.timeout = 20;
    request.headers = Headers::new().add(
        "user-agent",
        concat!("mocra-cluster-acceptance/", env!("CARGO_PKG_VERSION")),
    );
    request
}

fn response_url(response: &Response) -> Result<String> {
    response
        .get_meta("source_url")
        .ok_or_else(|| parse_error("response is missing source_url metadata"))
}

#[derive(Clone)]
struct JsonlSink {
    output: Arc<Mutex<tokio::fs::File>>,
}

#[async_trait]
impl DataSink<Value> for JsonlSink {
    async fn write(&self, record: Value) -> Result<()> {
        let mut line = serde_json::to_vec(&record)
            .map_err(|error| Error::new(ErrorKind::DataStore, Some(error)))?;
        line.push(b'\n');
        let mut output = self.output.lock().await;
        output.write_all(&line).await?;
        output.flush().await?;
        Ok(())
    }
}

struct QuotesSpider {
    node_id: u64,
}

#[async_trait]
impl Spider for QuotesSpider {
    type Item = Value;

    fn name(&self) -> &str {
        "quotes"
    }

    async fn start(&self, seeds: &mut Seeds) {
        seeds.add(tracked_get(format!("{QUOTES}/")));
    }

    async fn parse(&self, response: Response, cx: &mut Ctx<Value>) -> Result<()> {
        let source_url = response_url(&response)?;
        if response.status_code != 200 {
            return Err(parse_error(format!(
                "HTTP {} for {source_url}",
                response.status_code
            )));
        }
        let document = Html::parse_document(response.text()?);
        let quote_selector = Selector::parse("div.quote").unwrap();
        let text_selector = Selector::parse("span.text").unwrap();
        let author_selector = Selector::parse("small.author").unwrap();
        let next_selector = Selector::parse("li.next a").unwrap();
        let mut count = 0;
        for (index, quote) in document.select(&quote_selector).enumerate() {
            let text = quote
                .select(&text_selector)
                .next()
                .ok_or_else(|| parse_error("quote has no text"))?
                .text()
                .collect::<String>();
            let author = quote
                .select(&author_selector)
                .next()
                .ok_or_else(|| parse_error("quote has no author"))?
                .text()
                .collect::<String>();
            if text.trim().is_empty() || author.trim().is_empty() {
                return Err(parse_error("quote text and author must be nonempty"));
            }
            cx.emit(json!({
                "kind": "quote",
                "text": text.trim(),
                "author": author.trim(),
                "index": index,
                "source_url": source_url,
                "node_id": self.node_id,
                "request_id": response.id.to_string(),
                "run_id": response.run_id.to_string(),
                "http_status": response.status_code
            }));
            count += 1;
        }
        if count == 0 {
            return Err(parse_error(format!("no quotes found at {source_url}")));
        }
        if let Some(next) = document.select(&next_selector).next() {
            let href = next
                .value()
                .attr("href")
                .ok_or_else(|| parse_error("next-page link has no href"))?;
            if !href.starts_with("/page/") {
                return Err(parse_error(format!("unexpected next-page URL: {href}")));
            }
            cx.follow(tracked_get(format!("{QUOTES}{href}")));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Manifest {
    phase: String,
    count: usize,
}

struct CorpusSpider {
    node_id: u64,
}

#[async_trait]
impl Spider for CorpusSpider {
    type Item = Value;

    fn name(&self) -> &str {
        "corpus"
    }

    async fn start(&self, seeds: &mut Seeds) {
        let bytes = tokio::fs::read("/control/manifest.json")
            .await
            .expect("cannot read /control/manifest.json");
        let manifest: Manifest =
            serde_json::from_slice(&bytes).expect("invalid corpus manifest JSON");
        assert!(manifest.count > 0, "corpus count must be positive");
        assert!(
            !manifest.phase.is_empty()
                && manifest
                    .phase
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
            "corpus phase must contain only letters, digits, underscores and hyphens"
        );
        for id in 0..manifest.count {
            seeds.add(
                tracked_get(format!("{CORPUS}/page/{}/{id}", manifest.phase))
                    .add_meta("phase", &manifest.phase)
                    .add_meta("id", id)
                    .add_meta("kind", "page"),
            );
        }
    }

    async fn parse(&self, response: Response, cx: &mut Ctx<Value>) -> Result<()> {
        let source_url = response_url(&response)?;
        if response.status_code != 200 {
            return Err(parse_error(format!(
                "HTTP {} for {source_url}",
                response.status_code
            )));
        }
        let phase = response
            .get_meta::<String>("phase")
            .ok_or_else(|| parse_error("corpus response has no phase metadata"))?;
        let id = response
            .get_meta::<usize>("id")
            .ok_or_else(|| parse_error("corpus response has no id metadata"))?;
        let kind = response
            .get_meta::<String>("kind")
            .ok_or_else(|| parse_error("corpus response has no kind metadata"))?;
        if kind != "page" && kind != "detail" {
            return Err(parse_error(format!("unknown corpus response kind: {kind}")));
        }
        let document = Html::parse_document(response.text()?);
        let article_selector = Selector::parse("article").unwrap();
        let value_selector = Selector::parse("span.value").unwrap();
        let detail_selector = Selector::parse("a.detail").unwrap();
        let article = document
            .select(&article_selector)
            .next()
            .ok_or_else(|| parse_error("corpus response has no article"))?;
        let id_text = id.to_string();
        if article.value().attr("data-id") != Some(id_text.as_str())
            || article.value().attr("data-phase") != Some(phase.as_str())
            || article.value().attr("data-kind") != Some(kind.as_str())
            || source_url != format!("{CORPUS}/{kind}/{phase}/{id}")
        {
            return Err(parse_error(format!(
                "corpus article identity does not match request metadata: {source_url}"
            )));
        }
        let value = article
            .select(&value_selector)
            .next()
            .ok_or_else(|| parse_error("corpus article has no value"))?
            .text()
            .collect::<String>();
        if value != format!("value-{id}") {
            return Err(parse_error(format!(
                "unexpected corpus value at {source_url}"
            )));
        }
        cx.emit(json!({
            "kind": kind,
            "phase": phase,
            "id": id,
            "value": value,
            "source_url": source_url,
            "node_id": self.node_id,
            "request_id": response.id.to_string(),
            "run_id": response.run_id.to_string(),
            "http_status": response.status_code
        }));
        if kind == "page" {
            let href = article
                .select(&detail_selector)
                .next()
                .and_then(|link| link.value().attr("href"))
                .ok_or_else(|| parse_error("corpus page has no detail link"))?;
            if href != format!("/detail/{phase}/{id}") {
                return Err(parse_error(format!(
                    "unexpected corpus detail link: {href}"
                )));
            }
            cx.follow(
                tracked_get(format!("{CORPUS}{href}"))
                    .add_meta("phase", phase)
                    .add_meta("id", id)
                    .add_meta("kind", "detail"),
            );
        }
        Ok(())
    }
}

async fn initialize(cluster: ClusterConfig) -> std::result::Result<(), AnyError> {
    let cp = RaftControlPlane::start_cluster_node(
        cluster.node_id,
        &cluster.data_dir,
        &cluster.http_addr,
    )
    .await?;
    let ready = async {
        if cluster.node_id == 1 && !cp.raft().is_initialized().await? {
            cp.init_cluster(BTreeMap::from([
                (1, "node1:7000".to_string()),
                (2, "node2:7000".to_string()),
                (3, "node3:7000".to_string()),
            ]))
            .await?;
        }
        cp.wait_ready(Duration::from_secs(30)).await?;
        let status = cp.status();
        if status.member_count != 3 || status.voter_count != 3 {
            return Err(std::io::Error::other(format!(
                "node {} initialized with {} members and {} voters; expected three of each",
                status.node_id, status.member_count, status.voter_count
            ))
            .into());
        }
        println!(
            "{}",
            json!({
                "event": "INITIALIZED",
                "package_version": env!("CARGO_PKG_VERSION"),
                "node_id": status.node_id,
                "member_count": status.member_count,
                "voter_count": status.voter_count,
                "current_leader": status.current_leader,
                "term": status.term
            })
        );
        tokio::signal::ctrl_c().await?;
        Ok::<(), AnyError>(())
    }
    .await;
    let stopped = cp.shutdown().await;
    drop(cp);
    ready?;
    stopped?;
    Ok(())
}

async fn blob_probe() -> std::result::Result<(), AnyError> {
    use mocra::common::model::config::Config;
    use mocra::queue::{HEADER_ATTEMPT, HEADER_CREATED_AT, MqBackend, NackPolicy, nats::NatsQueue};
    use mocra::utils::storage::{BlobStorage, FileSystemBlobStorage, Offloadable};
    let config = Config::load("/config/node.toml").map_err(std::io::Error::other)?;
    let blobs = config
        .channel_config
        .blob_storage
        .as_ref()
        .filter(|blobs| blobs.shared_path)
        .and_then(|blobs| blobs.path.as_deref())
        .ok_or_else(|| std::io::Error::other("configured shared blob path is required"))?;
    let storage: Arc<dyn BlobStorage> = Arc::new(FileSystemBlobStorage::new(blobs));
    let request = tracked_get(format!("{CORPUS}/detail/blob_remote/42"))
        .add_meta("phase", "blob_remote")
        .add_meta("id", 42usize)
        .add_meta("kind", "detail");
    let mut content = b"<article data-kind='detail' data-id='42' data-phase='blob_remote'><span class='value'>value-42</span></article>".to_vec();
    content.resize(70 * 1024, b' ');
    let content_bytes = content.len();
    let mut response = Response {
        id: request.id,
        platform: "blob_remote".into(),
        account: "acceptance".into(),
        module: "corpus".into(),
        status_code: 200,
        cookies: request.cookies,
        content,
        storage_path: None,
        headers: vec![("content-type".into(), "text/html; charset=utf-8".into())],
        task_retry_times: request.task_retry_times,
        metadata: request.meta,
        download_middleware: request.download_middleware,
        data_middleware: request.data_middleware,
        task_finished: request.task_finished,
        context: request.context,
        run_id: request.id,
        prefix_request: request.prefix_request,
        request_hash: None,
        priority: request.priority,
    };
    response.offload(&storage).await?;
    let encoded = rmp_serde::to_vec(&response)?;
    let encoded_bytes = encoded.len();
    let payload = mocra::queue::compression::compress_payload_owned(
        encoded,
        config.channel_config.compression_threshold.unwrap_or(1024),
    );
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let headers = std::collections::HashMap::from([
        (HEADER_ATTEMPT.to_string(), "0".into()),
        (HEADER_CREATED_AT.to_string(), created_at),
    ]);
    let nats = config
        .channel_config
        .nats
        .as_ref()
        .ok_or_else(|| std::io::Error::other("configured NATS data plane is required"))?;
    let queue = NatsQueue::new(
        nats,
        config.channel_config.minid_time,
        &config.name,
        NackPolicy::default(),
    )?;
    queue
        .publish_with_headers(
            "response-normal",
            Some(&response.id.to_string()),
            &payload,
            &headers,
        )
        .await?;
    println!(
        "{}",
        json!({"event": "BLOB_PROBE_PUBLISHED", "storage_root": blobs,
        "storage_path": response.storage_path, "content_bytes": content_bytes,
        "serialized_payload_bytes": encoded_bytes, "queue_payload_bytes": payload.len(),
        "request_id": response.id.to_string(), "run_id": response.run_id.to_string()})
    );
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), AnyError> {
    let command = std::env::args()
        .nth(1)
        .ok_or_else(|| std::io::Error::other("usage: cluster_acceptance init|crawl|blob-probe"))?;
    let cluster = ClusterConfig::from_env().map_err(std::io::Error::other)?;
    println!(
        "{}",
        json!({
            "event": "STARTING",
            "package_version": env!("CARGO_PKG_VERSION"),
            "command": command,
            "node_id": cluster.node_id
        })
    );
    match command.as_str() {
        "init" => initialize(cluster).await,
        "blob-probe" => blob_probe().await,
        "crawl" => {
            let node_id = cluster.node_id;
            tokio::fs::create_dir_all("/output").await?;
            let output = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("/output/node-{node_id}.jsonl"))
                .await?;
            let sink = JsonlSink {
                output: Arc::new(Mutex::new(output)),
            };
            Mocra::builder()
                .from_toml("/config/node.toml")
                .cluster(cluster)
                .spider(QuotesSpider { node_id }, sink.clone())
                .spider(CorpusSpider { node_id }, sink)
                .run()
                .await?;
            Ok(())
        }
        _ => Err(std::io::Error::other(format!(
            "unknown command: {command}; use init|crawl|blob-probe"
        ))
        .into()),
    }
}
