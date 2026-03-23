mod db;
mod dht;
mod hashlist;
mod ingestor;
mod pipeline;
mod title_parser;

use axum::{extract::State, response::Html, routing::get, Router};
use ingestor::{AppState, HashlistStatus, MagnetRecord};

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

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn opt_str(val: &Option<String>) -> &str {
    val.as_deref().unwrap_or("—")
}

fn opt_num(val: Option<u32>) -> String {
    val.map(|n| n.to_string()).unwrap_or_else(|| "—".into())
}

async fn dashboard(State(state): State<AppState>) -> Html<String> {
    let records = state.db.get_all_records().unwrap_or_default();
    let hashlists = state.db.get_all_hashlists().unwrap_or_default();
    let counts = state.db.count_records().unwrap_or(db::RecordCounts {
        total: 0,
        movies: 0,
        episodes: 0,
        seasons: 0,
    });
    let done_count = state.db.count_hashlists_done().unwrap_or(0);

    let mut record_rows = String::new();
    for r in &records {
        let type_class = match r.content_type.as_deref() {
            Some("movie") => "type-movie",
            Some("episode") => "type-ep",
            Some("season") => "type-season",
            _ => "type-unk",
        };
        record_rows.push_str(&format!(
            r#"<tr>
  <td class="fn">{}</td>
  <td><code>{}</code></td>
  <td>{}</td>
  <td class="{type_class}">{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td class="src">{}</td>
</tr>"#,
            html_escape(&r.filename),
            &r.hash[..std::cmp::min(8, r.hash.len())],
            format_size(r.size_bytes),
            html_escape(opt_str(&r.content_type)),
            html_escape(opt_str(&r.title)),
            opt_num(r.season),
            opt_num(r.episode),
            opt_num(r.file_index),
            html_escape(opt_str(&r.imdb_tag)),
        ));
    }

    let mut hl_rows = String::new();
    for h in &hashlists {
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
<title>DMM Ingestor Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e1e4e8; padding: 24px; }}
  h1 {{ color: #58a6ff; margin-bottom: 4px; font-size: 1.6em; }}
  .subtitle {{ color: #8b949e; margin-bottom: 24px; font-size: 0.9em; }}
  .refresh {{ color: #3fb950; font-size: 0.8em; }}
  .section {{ margin-bottom: 32px; }}
  h2 {{ color: #c9d1d9; margin-bottom: 12px; font-size: 1.2em; border-bottom: 1px solid #21262d; padding-bottom: 8px; }}
  .stats {{ display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }}
  .stat {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 16px 24px; min-width: 120px; }}
  .stat .num {{ font-size: 2em; font-weight: bold; color: #58a6ff; }}
  .stat .label {{ color: #8b949e; font-size: 0.85em; }}
  table {{ width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; }}
  th {{ background: #21262d; color: #8b949e; text-align: left; padding: 10px 14px; font-size: 0.8em; text-transform: uppercase; letter-spacing: 0.05em; }}
  td {{ padding: 10px 14px; border-bottom: 1px solid #21262d; font-size: 0.9em; }}
  tr:hover {{ background: #1c2128; }}
  code {{ background: #1c2128; padding: 2px 6px; border-radius: 4px; font-size: 0.82em; color: #79c0ff; }}
  .fn {{ max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  .src {{ color: #d2a8ff; }}
  .ts {{ color: #8b949e; font-size: 0.82em; white-space: nowrap; }}
  .done {{ color: #3fb950; font-weight: 600; }}
  .err {{ color: #f85149; font-weight: 600; }}
  .type-movie {{ color: #79c0ff; font-weight: 600; }}
  .type-ep {{ color: #3fb950; font-weight: 600; }}
  .type-season {{ color: #d2a8ff; font-weight: 600; }}
  .type-unk {{ color: #8b949e; }}
  .live {{ display: inline-block; width: 8px; height: 8px; background: #3fb950; border-radius: 50%; animation: pulse 2s infinite; margin-right: 6px; }}
  @keyframes pulse {{ 0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.4; }} }}
  .tbl-wrap {{ max-height: 600px; overflow-y: auto; border-radius: 8px; }}
</style>
<script>
setTimeout(() => location.reload(), 3000);
</script>
</head>
<body>
<h1><span class="live"></span>DMM Hashlist Ingestor</h1>
<p class="subtitle">Live dashboard <span class="refresh">(auto-refreshes every 3s)</span></p>

<div class="stats">
  <div class="stat"><div class="num">{total_records}</div><div class="label">Total Records</div></div>
  <div class="stat"><div class="num">{movie_count}</div><div class="label">Movies</div></div>
  <div class="stat"><div class="num">{ep_count}</div><div class="label">Episodes</div></div>
  <div class="stat"><div class="num">{season_count}</div><div class="label">Season Packs</div></div>
  <div class="stat"><div class="num">{total_hl}</div><div class="label">Hashlists</div></div>
  <div class="stat"><div class="num">{done_hl}</div><div class="label">Done</div></div>
</div>

<div class="section">
<h2>Hashlists (latest first)</h2>
<div class="tbl-wrap">
<table>
<tr><th>Name</th><th>Records</th><th>Status</th><th>Processed At</th></tr>
{hl_rows}
</table>
</div>
</div>

<div class="section">
<h2>Magnet Records (latest first)</h2>
<div class="tbl-wrap">
<table>
<tr><th>Filename</th><th>Hash</th><th>Size</th><th>Type</th><th>Title</th><th>S</th><th>E</th><th>Idx</th><th>IMDb</th></tr>
{record_rows}
</table>
</div>
</div>

</body>
</html>"##,
        total_records = counts.total,
        movie_count = counts.movies,
        ep_count = counts.episodes,
        season_count = counts.seasons,
        total_hl = hashlists.len(),
        done_hl = done_count,
    );

    Html(html)
}

async fn api_records(State(state): State<AppState>) -> axum::Json<Vec<MagnetRecord>> {
    let records = state.db.get_all_records().unwrap_or_default();
    axum::Json(records)
}

async fn api_hashlists(State(state): State<AppState>) -> axum::Json<Vec<HashlistStatus>> {
    let hashlists = state.db.get_all_hashlists().unwrap_or_default();
    axum::Json(hashlists)
}

#[tokio::main]
async fn main() {
    println!("=== DMM Hashlist Ingestor ===");
    println!();

    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "/data/dmm.db".into());
    let poll_secs: u64 = std::env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3600);

    // Ensure data directory exists
    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let database = db::Db::open(&db_path).expect("Failed to open database");
    println!("Database: {db_path}");

    let state = AppState::new(database);

    // Log existing state
    if let Ok(counts) = state.db.count_records() {
        if counts.total > 0 {
            println!(
                "Resuming with {} records ({} movies, {} episodes, {} seasons)",
                counts.total, counts.movies, counts.episodes, counts.seasons
            );
        }
    }

    let ingest_state = state.clone();
    tokio::spawn(async move {
        ingestor::run_ingest_loop(
            ingest_state,
            std::time::Duration::from_secs(poll_secs),
        )
        .await;
    });

    println!("Starting web dashboard on http://0.0.0.0:3000");

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/records", get(api_records))
        .route("/api/hashlists", get(api_hashlists))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
