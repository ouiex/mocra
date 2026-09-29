//! Advanced quotes.toscrape.com project: Engine → Module → download/data/store middleware.
//!
//! Run from the repository with `cargo run --example quotes_project`. Listing pages stop at the
//! site's last Next link or page 50, whichever comes first. Unique author pages are fetched too.
//! Each run writes two JSONL files under `data/quotes_project/<run-id>/`, preserving prior runs.
//! Configuration lives in `examples/quotes_project.toml`; no database or broker is required.

use async_trait::async_trait;
use mocra::common::interface::{
    DataMiddlewareHandle, DataStoreMiddlewareHandle, DownloadMiddlewareHandle,
};
use mocra::common::model::data::{DataType, FileStore};
use mocra::common::model::login_info::LoginInfo;
use mocra::common::model::message::TaskEvent;
use mocra::common::state::State;
use mocra::prelude::common::ToSyncBoxStream;
use mocra::prelude::engine::Engine;
use mocra::prelude::*;
use mocra::queue::QueuedItem;
use scraper::{ElementRef, Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex as SyncMutex};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const BASE: &str = "https://quotes.toscrape.com";
const MAX_PAGES: u32 = 50;
const OUTPUT_DIR: &str = "data/quotes_project";
const MODULE: &str = "quotes_project";
const DOWNLOAD_MIDDLEWARE: &str = "quotes_headers";
const DATA_MIDDLEWARE: &str = "normalize_record";
const STORE_MIDDLEWARE: &str = "jsonl_store";

static QUOTE: LazyLock<Selector> = LazyLock::new(|| Selector::parse("div.quote").unwrap());
static TEXT: LazyLock<Selector> = LazyLock::new(|| Selector::parse("span.text").unwrap());
static AUTHOR: LazyLock<Selector> = LazyLock::new(|| Selector::parse("small.author").unwrap());
static AUTHOR_LINK: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse(r#"a[href^="/author/"]"#).unwrap());
static TAG: LazyLock<Selector> = LazyLock::new(|| Selector::parse("div.tags a.tag").unwrap());
static NEXT: LazyLock<Selector> = LazyLock::new(|| Selector::parse("li.next a").unwrap());
static AUTHOR_NAME: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("h3.author-title").unwrap());
static BORN_DATE: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("span.author-born-date").unwrap());
static BORN_LOCATION: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("span.author-born-location").unwrap());
static BIO: LazyLock<Selector> =
    LazyLock::new(|| Selector::parse("div.author-description").unwrap());

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Record {
    Quote {
        page: u32,
        text: String,
        author: String,
        author_url: String,
        tags: Vec<String>,
    },
    Author {
        name: String,
        born_date: String,
        born_location: String,
        bio: String,
    },
}

#[derive(Default)]
struct Progress {
    seen_authors: SyncMutex<HashSet<String>>,
    pages: AtomicUsize,
    quotes_seen: AtomicUsize,
    authors_queued: AtomicUsize,
    quotes_saved: AtomicUsize,
    authors_saved: AtomicUsize,
    downloads: AtomicUsize,
    normalized: AtomicUsize,
    finished: AtomicBool,
}

fn text_of(element: ElementRef<'_>) -> String {
    element.text().collect::<Vec<_>>().join(" ")
}

fn normalize(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn error(kind: ErrorKind, message: impl Into<String>) -> Error {
    Error::new(kind, Some(message.into()))
}

fn record_file(response: &Response, record: &Record, file_name: &str) -> Result<FileStore> {
    let bytes = serde_json::to_vec(record).map_err(|e| error(ErrorKind::Parser, e.to_string()))?;
    Ok(DataEvent::from(response)
        .with_file(bytes)
        .with_name(file_name))
}

struct QuotesModule {
    progress: Arc<Progress>,
}

#[async_trait]
impl ModuleTrait for QuotesModule {
    fn name(&self) -> String {
        MODULE.into()
    }

    fn version(&self) -> i32 {
        1
    }

    fn should_login(&self) -> bool {
        false
    }

    fn default_arc() -> Arc<dyn ModuleTrait> {
        Arc::new(Self {
            progress: Arc::new(Progress::default()),
        })
    }

    async fn add_step(&self) -> Vec<Arc<dyn ModuleNodeTrait>> {
        vec![Arc::new(QuotesNode {
            progress: Arc::clone(&self.progress),
        })]
    }
}

struct QuotesNode {
    progress: Arc<Progress>,
}

#[async_trait]
impl ModuleNodeTrait for QuotesNode {
    async fn generate(
        &self,
        _config: Arc<ModuleConfig>,
        params: Map<String, Value>,
        _login: Option<LoginInfo>,
    ) -> Result<SyncBoxStream<'static, Request>> {
        let request = if let Some(path) = params.get("author_path").and_then(Value::as_str) {
            if !path.starts_with("/author/") {
                return Err(error(ErrorKind::Request, "invalid author path"));
            }
            Request::new(format!("{BASE}{path}"), RequestMethod::Get).add_meta("kind", "author")
        } else {
            let page = params.get("page").and_then(Value::as_u64).unwrap_or(1);
            if !(1..=u64::from(MAX_PAGES)).contains(&page) {
                return Err(error(ErrorKind::Request, format!("invalid page {page}")));
            }
            let url = if page == 1 {
                format!("{BASE}/")
            } else {
                format!("{BASE}/page/{page}/")
            };
            Request::new(url, RequestMethod::Get)
                .add_meta("kind", "listing")
                .add_meta("page", page)
        };

        let mut request = request;
        request.download_middleware.push(DOWNLOAD_MIDDLEWARE.into());
        request.data_middleware = vec![DATA_MIDDLEWARE.into(), STORE_MIDDLEWARE.into()];
        vec![request].into_stream_ok()
    }

    async fn parser(
        &self,
        response: Response,
        _config: Option<Arc<ModuleConfig>>,
    ) -> Result<TaskOutputEvent> {
        if response.status_code != 200 {
            return Err(error(
                ErrorKind::Parser,
                format!("HTTP {} while parsing {MODULE}", response.status_code),
            ));
        }
        let html = Html::parse_document(&response.text_lossy());
        match response.get_meta::<String>("kind").as_deref() {
            Some("listing") => self.parse_listing(&response, &html),
            Some("author") => self.parse_author(&response, &html),
            _ => Err(error(ErrorKind::Parser, "missing page kind")),
        }
    }

    fn stable_node_key(&self) -> &'static str {
        "quotes"
    }
}

impl QuotesNode {
    fn parse_listing(&self, response: &Response, html: &Html) -> Result<TaskOutputEvent> {
        let page = response
            .get_meta::<u32>("page")
            .ok_or_else(|| error(ErrorKind::Parser, "missing listing page number"))?;
        let mut records = Vec::new();
        let mut tasks = Vec::new();
        let mut author_paths = Vec::new();

        for quote in html.select(&QUOTE) {
            let text = quote.select(&TEXT).next().map(text_of).unwrap_or_default();
            let author = quote
                .select(&AUTHOR)
                .next()
                .map(text_of)
                .unwrap_or_default();
            let tags = quote.select(&TAG).map(text_of).collect();
            let author_path = quote
                .select(&AUTHOR_LINK)
                .next()
                .and_then(|link| link.value().attr("href"))
                .filter(|path| path.starts_with("/author/"))
                .ok_or_else(|| error(ErrorKind::Parser, "quote has no author link"))?;
            records.push(record_file(
                response,
                &Record::Quote {
                    page,
                    text,
                    author,
                    author_url: format!("{BASE}{author_path}"),
                    tags,
                },
                "quotes.jsonl",
            )?);
            author_paths.push(author_path.to_string());
        }

        if records.is_empty() {
            return Err(error(
                ErrorKind::Parser,
                format!("listing page {page} contained no quotes"),
            ));
        }
        {
            let mut seen = self
                .progress
                .seen_authors
                .lock()
                .expect("seen_authors poisoned");
            for path in author_paths {
                if seen.insert(path.clone()) {
                    self.progress.authors_queued.fetch_add(1, Ordering::Relaxed);
                    tasks.push(
                        TaskParserEvent::from(response)
                            .add_meta("author_path", path)
                            .stay_current_step(),
                    );
                }
            }
        }
        self.progress
            .quotes_seen
            .fetch_add(records.len(), Ordering::Relaxed);
        self.progress.pages.fetch_add(1, Ordering::Relaxed);
        if page < MAX_PAGES && html.select(&NEXT).next().is_some() {
            tasks.push(
                TaskParserEvent::from(response)
                    .add_meta("page", page + 1)
                    .stay_current_step(),
            );
            println!(
                "[page {page}] {} quotes; following page {}",
                records.len(),
                page + 1
            );
        } else {
            self.progress.finished.store(true, Ordering::Relaxed);
            println!(
                "[page {page}] {} quotes; pagination complete",
                records.len()
            );
        }

        Ok(TaskOutputEvent::default()
            .with_data(records)
            .with_tasks(tasks))
    }

    fn parse_author(&self, response: &Response, html: &Html) -> Result<TaskOutputEvent> {
        let name = html
            .select(&AUTHOR_NAME)
            .next()
            .map(text_of)
            .ok_or_else(|| error(ErrorKind::Parser, "author page has no name"))?;
        let record = Record::Author {
            name,
            born_date: html
                .select(&BORN_DATE)
                .next()
                .map(text_of)
                .unwrap_or_default(),
            born_location: html
                .select(&BORN_LOCATION)
                .next()
                .map(text_of)
                .unwrap_or_default(),
            bio: html.select(&BIO).next().map(text_of).unwrap_or_default(),
        };
        Ok(TaskOutputEvent::default().with_data(vec![record_file(
            response,
            &record,
            "authors.jsonl",
        )?]))
    }
}

struct SiteHeaders {
    progress: Arc<Progress>,
}

#[async_trait]
impl DownloadMiddleware for SiteHeaders {
    fn name(&self) -> String {
        DOWNLOAD_MIDDLEWARE.into()
    }

    async fn before_request(
        &mut self,
        mut request: Request,
        _config: &Option<ModuleConfig>,
    ) -> Option<Request> {
        request.headers = request
            .headers
            .add("user-agent", "mocra-quotes-project/1.0")
            .add("accept-language", "en-US,en;q=0.9");
        request.timeout = 20;
        Some(request)
    }

    async fn after_response(
        &mut self,
        response: Response,
        _config: &Option<ModuleConfig>,
    ) -> Option<Response> {
        self.progress.downloads.fetch_add(1, Ordering::Relaxed);
        Some(response)
    }

    fn default_arc() -> DownloadMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self {
            progress: Arc::new(Progress::default()),
        })))
    }
}

struct NormalizeRecord {
    progress: Arc<Progress>,
}

#[async_trait]
impl DataMiddleware for NormalizeRecord {
    fn name(&self) -> String {
        DATA_MIDDLEWARE.into()
    }

    async fn handle_data(
        &mut self,
        mut data: DataEvent,
        _config: &Option<ModuleConfig>,
    ) -> Option<DataEvent> {
        let DataType::File(file) = &mut data.data else {
            eprintln!("[data] unexpected non-file payload");
            return None;
        };
        let mut record: Record = match serde_json::from_slice(&file.content) {
            Ok(record) => record,
            Err(error) => {
                eprintln!("[data] invalid JSON record: {error}");
                return None;
            }
        };
        match &mut record {
            Record::Quote { text, author, .. } => {
                *text = normalize(text);
                *author = normalize(author);
                if text.is_empty() || author.is_empty() {
                    eprintln!("[data] dropping quote without text or author");
                    return None;
                }
            }
            Record::Author { name, bio, .. } => {
                *name = normalize(name);
                *bio = normalize(bio);
                if name.is_empty() {
                    eprintln!("[data] dropping author without name");
                    return None;
                }
            }
        }
        file.content = serde_json::to_vec(&record).expect("serializing Record cannot fail");
        self.progress.normalized.fetch_add(1, Ordering::Relaxed);
        Some(data)
    }

    fn default_arc() -> DataMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self {
            progress: Arc::new(Progress::default()),
        })))
    }
}

struct JsonlStore {
    dir: PathBuf,
    progress: Arc<Progress>,
}

#[async_trait]
impl DataMiddleware for JsonlStore {
    fn name(&self) -> String {
        STORE_MIDDLEWARE.into()
    }

    async fn handle_data(
        &mut self,
        data: DataEvent,
        _config: &Option<ModuleConfig>,
    ) -> Option<DataEvent> {
        Some(data)
    }

    fn default_arc() -> DataMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self {
            dir: PathBuf::from(OUTPUT_DIR),
            progress: Arc::new(Progress::default()),
        })))
    }
}

#[async_trait]
impl DataStoreMiddleware for JsonlStore {
    async fn before_store(&mut self, _config: &Option<ModuleConfig>) -> Result<()> {
        tokio::fs::create_dir_all(&self.dir).await?;
        Ok(())
    }

    async fn store_data(&mut self, data: DataEvent, _config: &Option<ModuleConfig>) -> Result<()> {
        let DataType::File(file) = data.data else {
            return Err(error(ErrorKind::DataStore, "expected a JSONL file record"));
        };
        let counter = match file.file_name.as_str() {
            "quotes.jsonl" => &self.progress.quotes_saved,
            "authors.jsonl" => &self.progress.authors_saved,
            _ => return Err(error(ErrorKind::DataStore, "unexpected output file")),
        };
        let mut output = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(&file.file_name))
            .await?;
        output.write_all(&file.content).await?;
        output.write_all(b"\n").await?;
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn default_arc() -> DataStoreMiddlewareHandle {
        Arc::new(Mutex::new(Box::new(Self {
            dir: PathBuf::from(OUTPUT_DIR),
            progress: Arc::new(Progress::default()),
        })))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let run_id = uuid::Uuid::now_v7();
    let output_dir = root.join(OUTPUT_DIR).join(run_id.to_string());

    let config = root.join("examples/quotes_project.toml");
    let config = config.to_string_lossy();
    let state = State::try_new(&config)
        .await
        .map_err(|e| error(ErrorKind::Service, e.to_string()))?;
    let engine = Engine::new(Arc::new(state), None).await?;
    let progress = Arc::new(Progress::default());

    engine
        .register_download_middleware(Arc::new(Mutex::new(Box::new(SiteHeaders {
            progress: Arc::clone(&progress),
        }))))
        .await;
    engine
        .register_data_middleware(Arc::new(Mutex::new(Box::new(NormalizeRecord {
            progress: Arc::clone(&progress),
        }))))
        .await;
    engine
        .register_store_middleware(Arc::new(Mutex::new(Box::new(JsonlStore {
            dir: output_dir.clone(),
            progress: Arc::clone(&progress),
        }))))
        .await;
    engine
        .register_module(Arc::new(QuotesModule {
            progress: Arc::clone(&progress),
        }))
        .await;

    engine
        .queue_manager
        .get_task_push_channel()
        .send(QueuedItem::new(TaskEvent {
            account: "demo".into(),
            platform: "quotes.toscrape.com".into(),
            module: Some(vec![MODULE.into()]),
            priority: Default::default(),
            run_id,
        }))
        .await
        .map_err(|e| error(ErrorKind::Queue, e.to_string()))?;

    println!(
        "Crawling at most {MAX_PAGES} listing pages; output: {}",
        output_dir.display()
    );
    engine.start().await?;

    let pages = progress.pages.load(Ordering::Relaxed);
    let quotes = progress.quotes_saved.load(Ordering::Relaxed);
    let authors = progress.authors_saved.load(Ordering::Relaxed);
    if !progress.finished.load(Ordering::Relaxed)
        || quotes == 0
        || quotes != progress.quotes_seen.load(Ordering::Relaxed)
        || authors < progress.authors_queued.load(Ordering::Relaxed)
    {
        return Err(error(
            ErrorKind::Service,
            "crawl stopped before pagination and author details completed",
        ));
    }
    println!(
        "Completed: {pages} listing pages, {quotes} quotes, {authors} authors; {} downloads, {} data transformations",
        progress.downloads.load(Ordering::Relaxed),
        progress.normalized.load(Ordering::Relaxed),
    );
    Ok(())
}
