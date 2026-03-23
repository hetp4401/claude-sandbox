mod hashlist;

use axum::{extract::State, response::Html, routing::get, Router};
use chrono::Utc;
use hashlist::{parse_hashlist_html, decode_hashlist};
use serde::Serialize;
use std::sync::{Arc, RwLock};

/// A processed magnet record ready for persistence.
#[derive(Debug, Clone, Serialize)]
struct MagnetRecord {
    filename: String,
    hash: String,
    magnet_uri: String,
    size_bytes: u64,
    source_hashlist: String,
    processed_at: String,
}

/// Status of a processed hashlist.
#[derive(Debug, Clone, Serialize)]
struct HashlistStatus {
    name: String,
    record_count: usize,
    status: String,
    processed_at: String,
}

#[derive(Default, Clone)]
struct AppState {
    records: Arc<RwLock<Vec<MagnetRecord>>>,
    hashlists: Arc<RwLock<Vec<HashlistStatus>>>,
}

/// Example HTML hashlist files (as they appear in the GitHub repo).
const EXAMPLE_HASHLISTS: &[(&str, &str)] = &[
    (
        "00011863-single.html",
        r#"<!doctype html><html><head><meta charset=UTF-8><title>Debrid Media Manager Hash List</title><style>iframe{border:none;position:absolute;top:0;left:0;width:100%;height:100%}</style></head><body><iframe src="https://debridmediamanager.com/hashlist#N4IgNglgzgLiBcBtUAzCYCmA7AhgWwwRACEIBzAOmIFcBjAayuqywE8KAmABi4A4KAjHy4AHKmGoAlHOwAeHAGwAWEABoQACxxQNRACZ7eHAKzGMtPbQDstHKZQAjLg4cpeAgMwcPhq1wUceg4CKAp6ArRqIFAQAF6E8AJKCrwKSQrGCgC+ALpZQA"></iframe></body></html>"#,
    ),
    (
        "2f16a13d-multi.html",
        r#"<!doctype html><html><head><meta charset=UTF-8><title>Debrid Media Manager Hash List</title><style>iframe{border:none;position:absolute;top:0;left:0;width:100%;height:100%}</style></head><body><iframe src="https://debridmediamanager.com/hashlist#N4IgLglmA2CmIC4QBVYGcwAIDCB7acAxpLgHYgA0I0EGiA2qAGYRykCGAtvEgMoSkwsaADoATAAYAjBJEAWANIiAqgAkAIiIBC0AK4AldgE8RADzEA2AKwBaVQFEAatkogAFuzRvEIC+wCcAOxW-gBGTEwAJlaEEuxM1gDMUv5B-kyhgQAciWJSWelyTImEiVaRrmgQAF48cmL+cv4WgQ0WAL4UzKywHNw+qOwATmgiuEwivELC4tJiIjJZEgAOIgDq9lo26gAyIgCC+9jzsqqWcq4eXj7sWVlR7CFWcrAFUtmlrYFSfn7ZoVl8hJYJE5JF2JFEpUajxAok5BIJLlEZ1umwuDwQPY4MsPIJRuohrAuLMJBYRK0Vto9IYTOpkLwzOdLp5vEhQokmFJYIlQYQrOwLJkCnN4VYWgV2KFCJFYExRXJxYFobVEHk5IE5DkLFr2gBddpAA"></iframe></body></html>"#,
    ),
    (
        "a4b5c6d7-dmm.html",
        r#"<!doctype html><html><head><meta charset=UTF-8><title>Debrid Media Manager Hash List</title><style>iframe{border:none;position:absolute;top:0;left:0;width:100%;height:100%}</style></head><body><iframe src="https://debridmediamanager.com/hashlist#N4IgLglmA2CmIC4QBECyqAEAxA9gJwFsBDMEAGnHz1gDswBnRAbVADMI4aiD4lUcAbhFgA6AEwAGMQBYRARgkAOCQAcRBANYDyIABZF6uxCCJEARmYDGlgCY25csWIDMz6dICsHgGzeA7H6KigCcwRISphbWdg5OOmYAnmCwjAiS4RkZAL4AullAA"></iframe></body></html>"#,
    ),
];

const BARE_ARRAY_ENCODED: &str = "NobwRAZglgNgpgOwIYFs5gFxgMoAskBOcAJgHQCyA9gG5RykBMADAwCykoDW1YANGPgDOuTGACMDAMysArADYA7AA4AnEyQAjAMbE4ECdPnK1mnXoOzFSvmA0BPAC5xBmGU3cf3AXwC6QA";

fn format_size(bytes: u64) -> String {
    const GB: f64 = 1_073_741_824.0;
    const MB: f64 = 1_048_576.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else {
        format!("{:.2} MB", b / MB)
    }
}

fn ingest_hashlists(state: &AppState) {
    // Process HTML hashlists
    for (name, html) in EXAMPLE_HASHLISTS {
        let now = Utc::now().to_rfc3339();
        match parse_hashlist_html(html) {
            Ok(hl) => {
                let count = hl.list.len();
                for entry in &hl.list {
                    state.records.write().unwrap().push(MagnetRecord {
                        filename: entry.filename.clone(),
                        hash: entry.hash.clone(),
                        magnet_uri: format!("magnet:?xt=urn:btih:{}", entry.hash),
                        size_bytes: entry.size,
                        source_hashlist: name.to_string(),
                        processed_at: Utc::now().to_rfc3339(),
                    });
                }
                state.hashlists.write().unwrap().push(HashlistStatus {
                    name: name.to_string(),
                    record_count: count,
                    status: "done".into(),
                    processed_at: now,
                });
                println!("[OK] {name}: {count} records ingested");
            }
            Err(e) => {
                state.hashlists.write().unwrap().push(HashlistStatus {
                    name: name.to_string(),
                    record_count: 0,
                    status: format!("error: {e}"),
                    processed_at: now,
                });
                println!("[ERROR] {name}: {e}");
            }
        }
    }

    // Process bare-array hashlist
    let now = Utc::now().to_rfc3339();
    match decode_hashlist(BARE_ARRAY_ENCODED) {
        Ok(hl) => {
            let count = hl.list.len();
            for entry in &hl.list {
                state.records.write().unwrap().push(MagnetRecord {
                    filename: entry.filename.clone(),
                    hash: entry.hash.clone(),
                    magnet_uri: format!("magnet:?xt=urn:btih:{}", entry.hash),
                    size_bytes: entry.size,
                    source_hashlist: "bare-array-shared".into(),
                    processed_at: Utc::now().to_rfc3339(),
                });
            }
            state.hashlists.write().unwrap().push(HashlistStatus {
                name: "bare-array-shared".into(),
                record_count: count,
                status: "done".into(),
                processed_at: now,
            });
            println!("[OK] bare-array-shared: {count} records ingested");
        }
        Err(e) => println!("[ERROR] bare-array: {e}"),
    }
}

async fn dashboard(State(state): State<AppState>) -> Html<String> {
    let records = state.records.read().unwrap();
    let hashlists = state.hashlists.read().unwrap();

    // Records sorted by latest processed_at first
    let mut sorted_records: Vec<_> = records.iter().collect();
    sorted_records.sort_by(|a, b| b.processed_at.cmp(&a.processed_at));

    let mut record_rows = String::new();
    for r in &sorted_records {
        record_rows.push_str(&format!(
            r#"<tr>
  <td class="fn">{}</td>
  <td><code>{}</code></td>
  <td>{}</td>
  <td class="src">{}</td>
  <td class="ts">{}</td>
</tr>"#,
            html_escape(&r.filename),
            &r.hash,
            format_size(r.size_bytes),
            html_escape(&r.source_hashlist),
            &r.processed_at,
        ));
    }

    let mut hl_rows = String::new();
    let mut sorted_hls: Vec<_> = hashlists.iter().collect();
    sorted_hls.sort_by(|a, b| b.processed_at.cmp(&a.processed_at));
    for h in &sorted_hls {
        let status_class = if h.status == "done" { "done" } else { "err" };
        hl_rows.push_str(&format!(
            r#"<tr>
  <td>{}</td>
  <td>{}</td>
  <td class="{status_class}">{}</td>
  <td class="ts">{}</td>
</tr>"#,
            html_escape(&h.name),
            h.record_count,
            html_escape(&h.status),
            &h.processed_at,
        ));
    }

    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="5">
<title>DMM Ingestor Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e1e4e8; padding: 24px; }}
  h1 {{ color: #58a6ff; margin-bottom: 4px; font-size: 1.6em; }}
  .subtitle {{ color: #8b949e; margin-bottom: 24px; font-size: 0.9em; }}
  .refresh {{ color: #3fb950; font-size: 0.8em; }}
  .section {{ margin-bottom: 32px; }}
  h2 {{ color: #c9d1d9; margin-bottom: 12px; font-size: 1.2em; border-bottom: 1px solid #21262d; padding-bottom: 8px; }}
  .stats {{ display: flex; gap: 16px; margin-bottom: 24px; }}
  .stat {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 16px 24px; }}
  .stat .num {{ font-size: 2em; font-weight: bold; color: #58a6ff; }}
  .stat .label {{ color: #8b949e; font-size: 0.85em; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; }}
  th {{ background: #21262d; color: #8b949e; text-align: left; padding: 10px 14px; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.05em; }}
  td {{ padding: 10px 14px; border-bottom: 1px solid #21262d; font-size: 0.9em; }}
  tr:hover {{ background: #1c2128; }}
  code {{ background: #1c2128; padding: 2px 6px; border-radius: 4px; font-size: 0.82em; color: #79c0ff; }}
  .fn {{ max-width: 350px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .src {{ color: #d2a8ff; }}
  .ts {{ color: #8b949e; font-size: 0.82em; white-space: nowrap; }}
  .done {{ color: #3fb950; font-weight: 600; }}
  .err {{ color: #f85149; font-weight: 600; }}
</style>
</head>
<body>
<h1>DMM Hashlist Ingestor</h1>
<p class="subtitle">Processed records dashboard <span class="refresh">(auto-refreshes every 5s)</span></p>

<div class="stats">
  <div class="stat"><div class="num">{}</div><div class="label">Total Records</div></div>
  <div class="stat"><div class="num">{}</div><div class="label">Hashlists Processed</div></div>
  <div class="stat"><div class="num">{}</div><div class="label">Hashlists Done</div></div>
</div>

<div class="section">
<h2>Hashlists</h2>
<table>
<tr><th>Name</th><th>Records</th><th>Status</th><th>Processed At</th></tr>
{hl_rows}
</table>
</div>

<div class="section">
<h2>Magnet Records (sorted by latest)</h2>
<table>
<tr><th>Filename</th><th>Info Hash</th><th>Size</th><th>Source</th><th>Processed At</th></tr>
{record_rows}
</table>
</div>

</body>
</html>"##,
        records.len(),
        hashlists.len(),
        hashlists.iter().filter(|h| h.status == "done").count(),
    );

    Html(html)
}

async fn api_records(State(state): State<AppState>) -> axum::Json<Vec<MagnetRecord>> {
    let records = state.records.read().unwrap();
    let mut sorted: Vec<_> = records.clone();
    sorted.sort_by(|a, b| b.processed_at.cmp(&a.processed_at));
    axum::Json(sorted)
}

async fn api_hashlists(State(state): State<AppState>) -> axum::Json<Vec<HashlistStatus>> {
    let hashlists = state.hashlists.read().unwrap();
    axum::Json(hashlists.clone())
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[tokio::main]
async fn main() {
    let state = AppState::default();

    println!("=== DMM Hashlist Ingestor ===");
    println!();
    ingest_hashlists(&state);

    let total = state.records.read().unwrap().len();
    println!();
    println!("Ingested {total} records total.");
    println!("Starting web dashboard on http://0.0.0.0:3000");

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/records", get(api_records))
        .route("/api/hashlists", get(api_hashlists))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
