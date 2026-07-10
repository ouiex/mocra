#!/usr/bin/env bash
# Scaffold a runnable mocra data-collection project.
#
#   scaffold.sh <project-dir> [SpiderName]
#
# Creates <project-dir>/ with Cargo.toml + src/main.rs — a compiling Spider skeleton with both
# the HTML (scraper) and JSON (res.json) parse paths stubbed. Then:
#
#   cd <project-dir> && cargo run
#
set -euo pipefail

DIR="${1:?usage: scaffold.sh <project-dir> [SpiderName]}"
SPIDER="${2:-MySpider}"
NAME="$(basename "$DIR")"

if [ -e "$DIR" ]; then
  echo "error: '$DIR' already exists" >&2
  exit 1
fi
mkdir -p "$DIR/src"

cat > "$DIR/Cargo.toml" <<EOF
[package]
name = "$NAME"
version = "0.1.0"
edition = "2021"

[dependencies]
mocra = "0.2"
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
# HTML parsing — mocra hands you the raw Response, so bring your own CSS selectors.
# Delete this if you only hit JSON APIs (use res.json::<T>() instead).
scraper = "0.20"
EOF

cat > "$DIR/src/main.rs" <<EOF
use async_trait::async_trait;
use mocra::prelude::*;
use serde::Serialize;

/// One collected record — these fields are your output schema.
#[derive(Debug, Serialize)]
struct Item {
    title: String,
    url: String,
}

struct ${SPIDER};

#[async_trait]
impl Spider for ${SPIDER} {
    type Item = Item;

    fn name(&self) -> &str {
        "${NAME}"
    }

    /// Seed the crawl with one or more start URLs.
    async fn start(&self, s: &mut Seeds) {
        s.get("https://example.com/");
        // Custom headers:
        // let req = s.get("https://api.example.com/items");
        // req.headers = Headers::new().add("User-Agent", "${NAME}/0.1");
    }

    /// Parse each downloaded Response into typed items (and optionally follow links).
    async fn parse(&self, res: Response, cx: &mut Ctx<Self::Item>) -> Result<()> {
        // ---- HTML path (uses the \`scraper\` crate) ----
        let html = res.text_lossy();
        let doc = scraper::Html::parse_document(&html);
        let link = scraper::Selector::parse("a").unwrap();
        for a in doc.select(&link) {
            let title = a.text().collect::<String>().trim().to_string();
            if let Some(href) = a.value().attr("href") {
                if !title.is_empty() {
                    cx.emit(Item { title, url: href.to_string() });
                }
            }
        }

        // ---- JSON path (delete the HTML block above and use this for APIs) ----
        // #[derive(serde::Deserialize)]
        // struct Api { items: Vec<Row> }
        // #[derive(serde::Deserialize)]
        // struct Row { title: String, url: String }
        // let api: Api = res.json()?;
        // for row in api.items { cx.emit(Item { title: row.title, url: row.url }); }

        // ---- follow pagination ----
        // cx.follow_get("https://example.com/page/2");

        let _ = res.status_code; // u16, if you want to branch on it
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // No DB, no Redis: single-node in-memory. Auto-seeds and exits when idle.
    Mocra::builder()
        .spider(
            ${SPIDER},
            on_item(|item: Item| async move {
                println!("[item] {} -> {}", item.title, item.url);
            }),
        )
        .run()
        .await
}
EOF

echo "scaffolded $DIR"
echo "next:  cd $DIR && cargo run"
