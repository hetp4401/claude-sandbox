mod db;
mod dht;
mod hashlist;
mod imdb_pipeline;
mod imdb_resolver;
mod ingestor;
mod logs;
mod pipeline;
mod title_parser;

use axum::{extract::Query, extract::State, response::Html, routing::get, Router};
use imdb_resolver::ImdbResolver;
use ingestor::AppState;
use logs::LogBuffer;
use serde::Deserialize;

async fn dashboard(State(_state): State<AppState>) -> Html<String> {
    Html(DASHBOARD_HTML.to_string())
}

#[derive(Deserialize)]
struct PageQuery {
    page: Option<i64>,
    per_page: Option<i64>,
}

async fn api_torrents(
    State(state): State<AppState>,
    Query(params): Query<PageQuery>,
) -> axum::Json<serde_json::Value> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    match state.db.get_torrents_page(page, per_page).await {
        Ok(p) => axum::Json(serde_json::json!({
            "items": p.items,
            "total": p.total,
            "page": p.page,
            "per_page": p.per_page,
        })),
        Err(e) => axum::Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn api_parsed(
    State(state): State<AppState>,
    Query(params): Query<PageQuery>,
) -> axum::Json<serde_json::Value> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    match state.db.get_parsed_metadata_page(page, per_page).await {
        Ok(p) => axum::Json(serde_json::json!({
            "items": p.items,
            "total": p.total,
            "page": p.page,
            "per_page": p.per_page,
        })),
        Err(e) => axum::Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn api_imdb(
    State(state): State<AppState>,
    Query(params): Query<PageQuery>,
) -> axum::Json<serde_json::Value> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    match state.db.get_imdb_mappings_page(page, per_page).await {
        Ok(p) => axum::Json(serde_json::json!({
            "items": p.items,
            "total": p.total,
            "page": p.page,
            "per_page": p.per_page,
        })),
        Err(e) => axum::Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn api_stats(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    let counts = state.db.counts().await.unwrap_or(db::RecordCounts {
        total_torrents: 0,
        total_parsed: 0,
        total_imdb: 0,
    });
    let total_hl = state.db.count_hashlists().await.unwrap_or(0);
    let done_hl = state.db.count_hashlists_done().await.unwrap_or(0);

    axum::Json(serde_json::json!({
        "total_torrents": counts.total_torrents,
        "total_parsed": counts.total_parsed,
        "total_imdb": counts.total_imdb,
        "total_hashlists": total_hl,
        "done_hashlists": done_hl,
    }))
}

#[derive(Deserialize)]
struct LogsQuery {
    after: Option<usize>,
}

async fn api_logs(
    State(state): State<AppState>,
    Query(params): Query<LogsQuery>,
) -> axum::Json<serde_json::Value> {
    let after = params.after.unwrap_or(0);
    let (lines, next) = state.logs.get_since(after);
    axum::Json(serde_json::json!({
        "lines": lines,
        "next": next,
    }))
}

#[tokio::main]
async fn main() {
    let log_buf = LogBuffer::new();

    println!("=== DMM Hashlist Ingestor ===");
    println!();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://dmm:dmm@localhost:5432/dmm".into());
    let poll_secs: u64 = std::env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3600);

    let database = db::Db::connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");
    log!(log_buf, "Connected to PostgreSQL");

    let state = AppState::new(database, log_buf);

    // Build a shared HTTP client and IMDb resolver for the pipeline
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .expect("Failed to build HTTP client");
    let resolver = ImdbResolver::new(http_client);

    let ingest_state = state.clone();
    let ingest_resolver = resolver.clone();
    tokio::spawn(async move {
        ingestor::run_ingest_loop(
            ingest_state,
            ingest_resolver,
            std::time::Duration::from_secs(poll_secs),
        )
        .await;
    });

    log!(state.logs, "Starting web dashboard on http://0.0.0.0:3000");

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/torrents", get(api_torrents))
        .route("/api/parsed", get(api_parsed))
        .route("/api/imdb", get(api_imdb))
        .route("/api/stats", get(api_stats))
        .route("/api/logs", get(api_logs))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

const DASHBOARD_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>DMM Ingestor Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e1e4e8; padding: 24px; }
  h1 { color: #58a6ff; margin-bottom: 4px; font-size: 1.6em; }
  .subtitle { color: #8b949e; margin-bottom: 16px; font-size: 0.9em; }
  .stats { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; }
  .stat { background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 14px 20px; min-width: 110px; }
  .stat .num { font-size: 1.8em; font-weight: bold; color: #58a6ff; }
  .stat .label { color: #8b949e; font-size: 0.82em; }

  .tabs { display: flex; gap: 0; margin-bottom: 0; border-bottom: 2px solid #21262d; }
  .tab { padding: 10px 24px; cursor: pointer; color: #8b949e; font-size: 0.9em; font-weight: 500;
         border-bottom: 2px solid transparent; margin-bottom: -2px; transition: all 0.15s; user-select: none; }
  .tab:hover { color: #c9d1d9; }
  .tab.active { color: #58a6ff; border-bottom-color: #58a6ff; }
  .tab-content { display: none; }
  .tab-content.active { display: block; }
  .tab-panel { padding-top: 16px; }

  .refresh-btn { background: #21262d; color: #58a6ff; border: 1px solid #30363d; border-radius: 6px;
                 padding: 6px 16px; cursor: pointer; font-size: 0.85em; margin-left: 12px; transition: all 0.15s; }
  .refresh-btn:hover { background: #30363d; }
  .refresh-btn:active { transform: scale(0.97); }
  .refresh-btn.loading { opacity: 0.6; pointer-events: none; }
  .topbar { display: flex; align-items: center; margin-bottom: 20px; }
  .topbar h1 { flex: 1; }

  table { width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; }
  th { background: #21262d; color: #8b949e; text-align: left; padding: 10px 14px; font-size: 0.78em;
       text-transform: uppercase; letter-spacing: 0.05em; position: sticky; top: 0; z-index: 1; }
  td { padding: 10px 14px; border-bottom: 1px solid #21262d; font-size: 0.88em; }
  tr:hover { background: #1c2128; }
  code { background: #1c2128; padding: 2px 6px; border-radius: 4px; font-size: 0.82em; color: #79c0ff; }
  .fn { max-width: 340px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ts { color: #8b949e; font-size: 0.82em; white-space: nowrap; }
  .imdb-link { color: #f5c518; text-decoration: none; font-weight: 600; }
  .imdb-link:hover { text-decoration: underline; }
  .tbl-wrap { max-height: 600px; overflow-y: auto; border-radius: 8px; }
  .empty { color: #484f58; padding: 40px; text-align: center; font-size: 0.95em; }

  .pagination { display: flex; align-items: center; gap: 8px; margin-top: 12px; justify-content: center; }
  .pagination button { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
                        padding: 6px 14px; cursor: pointer; font-size: 0.82em; transition: all 0.15s; }
  .pagination button:hover:not(:disabled) { background: #30363d; }
  .pagination button:disabled { opacity: 0.4; cursor: default; }
  .pagination .page-info { color: #8b949e; font-size: 0.85em; }

  #log-box { background: #0d1117; border: 1px solid #21262d; border-radius: 8px; padding: 12px;
             font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.8em; line-height: 1.6;
             max-height: 400px; overflow-y: auto; color: #8b949e; white-space: pre-wrap; word-break: break-all; }
  #log-box .log-ok { color: #3fb950; }
  #log-box .log-err { color: #f85149; }
  #log-box .log-warn { color: #d29922; }
  #log-box .log-info { color: #58a6ff; }
  .log-controls { display: flex; align-items: center; gap: 12px; margin-bottom: 10px; }
  .log-controls label { color: #8b949e; font-size: 0.85em; }
  .log-status { color: #484f58; font-size: 0.8em; margin-left: auto; }

  .live { display: inline-block; width: 8px; height: 8px; background: #3fb950; border-radius: 50%;
          animation: pulse 2s infinite; margin-right: 6px; }
  @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
</style>
</head>
<body>

<div class="topbar">
  <h1><span class="live"></span>DMM Hashlist Ingestor</h1>
  <button class="refresh-btn" onclick="refreshAll()" id="refresh-btn">Refresh</button>
</div>
<p class="subtitle" id="subtitle">Loading...</p>

<div class="stats" id="stats"></div>

<div class="tabs">
  <div class="tab active" data-tab="torrents">Torrents</div>
  <div class="tab" data-tab="parsed">Parsed Metadata</div>
  <div class="tab" data-tab="imdb">IMDb Mappings</div>
  <div class="tab" data-tab="logs">Logs</div>
</div>

<div id="tab-torrents" class="tab-content active">
  <div class="tab-panel">
    <div class="tbl-wrap" id="torrents-table"></div>
    <div class="pagination" id="torrents-pag"></div>
  </div>
</div>

<div id="tab-parsed" class="tab-content">
  <div class="tab-panel">
    <div class="tbl-wrap" id="parsed-table"></div>
    <div class="pagination" id="parsed-pag"></div>
  </div>
</div>

<div id="tab-imdb" class="tab-content">
  <div class="tab-panel">
    <div class="tbl-wrap" id="imdb-table"></div>
    <div class="pagination" id="imdb-pag"></div>
  </div>
</div>

<div id="tab-logs" class="tab-content">
  <div class="tab-panel">
    <div class="log-controls">
      <label><input type="checkbox" id="log-autoscroll" checked> Auto-scroll</label>
      <label><input type="checkbox" id="log-autopoll" checked> Auto-refresh (3s)</label>
      <span class="log-status" id="log-status"></span>
    </div>
    <div id="log-box"></div>
  </div>
</div>

<script>
let torrentsPage = 1, parsedPage = 1, imdbPage = 1;
const PER_PAGE = 50;

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
  });
});

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function fmtSize(bytes) {
  const gb = 1073741824, mb = 1048576;
  return bytes >= gb ? (bytes / gb).toFixed(2) + ' GB' : (bytes / mb).toFixed(2) + ' MB';
}

function fmtTime(iso) {
  try { return new Date(iso).toLocaleString(); } catch(e) { return iso; }
}

function renderPagination(containerId, page, total, perPage, onPageChange) {
  const totalPages = Math.max(1, Math.ceil(total / perPage));
  const el = document.getElementById(containerId);
  el.innerHTML = `
    <button ${page <= 1 ? 'disabled' : ''} onclick="(${onPageChange})(${page - 1})">Prev</button>
    <span class="page-info">Page ${page} of ${totalPages} (${total} total)</span>
    <button ${page >= totalPages ? 'disabled' : ''} onclick="(${onPageChange})(${page + 1})">Next</button>`;
}

async function loadStats() {
  try {
    const r = await fetch('/api/stats');
    const s = await r.json();
    document.getElementById('stats').innerHTML = `
      <div class="stat"><div class="num">${s.total_torrents}</div><div class="label">Torrents</div></div>
      <div class="stat"><div class="num">${s.total_parsed}</div><div class="label">Parsed</div></div>
      <div class="stat"><div class="num">${s.total_imdb}</div><div class="label">IMDb</div></div>
      <div class="stat"><div class="num">${s.total_hashlists}</div><div class="label">Hashlists</div></div>
      <div class="stat"><div class="num">${s.done_hashlists}</div><div class="label">Done</div></div>`;
    document.getElementById('subtitle').textContent =
      `${s.total_torrents} torrents, ${s.total_parsed} parsed, ${s.total_imdb} IMDb mapped, ${s.total_hashlists} hashlists`;
  } catch(e) {
    document.getElementById('subtitle').textContent = 'Failed to load stats';
  }
}

async function loadTorrents(page) {
  if (page !== undefined) torrentsPage = page;
  try {
    const r = await fetch(`/api/torrents?page=${torrentsPage}&per_page=${PER_PAGE}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('torrents-table').innerHTML = '<div class="empty">No torrents yet</div>';
      document.getElementById('torrents-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Hash</th><th>Filename</th><th>Size</th><th>Last Updated</th></tr>`;
    for (const t of data.items) {
      html += `<tr>
        <td><code>${esc(t.hash.slice(0,12))}</code></td>
        <td class="fn">${esc(t.filename)}</td>
        <td>${fmtSize(t.size_bytes)}</td>
        <td class="ts">${fmtTime(t.last_updated)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('torrents-table').innerHTML = html;
    renderPagination('torrents-pag', data.page, data.total, data.per_page, loadTorrents);
  } catch(e) {
    document.getElementById('torrents-table').innerHTML = '<div class="empty">Failed to load torrents</div>';
  }
}

async function loadParsed(page) {
  if (page !== undefined) parsedPage = page;
  try {
    const r = await fetch(`/api/parsed?page=${parsedPage}&per_page=${PER_PAGE}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('parsed-table').innerHTML = '<div class="empty">No parsed metadata yet</div>';
      document.getElementById('parsed-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Hash</th><th>Title</th><th>Year</th><th>Season</th><th>Episode</th><th>Last Updated</th></tr>`;
    for (const p of data.items) {
      html += `<tr>
        <td><code>${esc(p.hash.slice(0,12))}</code></td>
        <td>${esc(p.title)}</td>
        <td>${p.year != null ? p.year : '\u2014'}</td>
        <td>${p.season != null ? p.season : '\u2014'}</td>
        <td>${p.episode != null ? p.episode : '\u2014'}</td>
        <td class="ts">${fmtTime(p.last_updated)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('parsed-table').innerHTML = html;
    renderPagination('parsed-pag', data.page, data.total, data.per_page, loadParsed);
  } catch(e) {
    document.getElementById('parsed-table').innerHTML = '<div class="empty">Failed to load parsed metadata</div>';
  }
}

async function loadImdb(page) {
  if (page !== undefined) imdbPage = page;
  try {
    const r = await fetch(`/api/imdb?page=${imdbPage}&per_page=${PER_PAGE}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('imdb-table').innerHTML = '<div class="empty">No IMDb mappings yet</div>';
      document.getElementById('imdb-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Hash</th><th>IMDb ID</th><th>Last Updated</th></tr>`;
    for (const m of data.items) {
      html += `<tr>
        <td><code>${esc(m.hash.slice(0,12))}</code></td>
        <td><a class="imdb-link" href="https://www.imdb.com/title/${esc(m.imdb_id)}/" target="_blank" rel="noopener">${esc(m.imdb_id)}</a></td>
        <td class="ts">${fmtTime(m.last_updated)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('imdb-table').innerHTML = html;
    renderPagination('imdb-pag', data.page, data.total, data.per_page, loadImdb);
  } catch(e) {
    document.getElementById('imdb-table').innerHTML = '<div class="empty">Failed to load IMDb mappings</div>';
  }
}

let logCursor = 0;
async function pollLogs() {
  try {
    const r = await fetch('/api/logs?after=' + logCursor);
    const data = await r.json();
    if (data.lines.length > 0) {
      const box = document.getElementById('log-box');
      for (const line of data.lines) {
        const span = document.createElement('div');
        if (line.includes('[OK]')) span.className = 'log-ok';
        else if (line.includes('[ERROR]')) span.className = 'log-err';
        else if (line.includes('[WARN]')) span.className = 'log-warn';
        else if (line.includes('[INGESTOR]') || line.includes('[PIPELINE]') || line.includes('[IMDB]')) span.className = 'log-info';
        span.textContent = line;
        box.appendChild(span);
      }
      if (document.getElementById('log-autoscroll').checked) {
        box.scrollTop = box.scrollHeight;
      }
    }
    logCursor = data.next;
    document.getElementById('log-status').textContent = `${logCursor} lines`;
  } catch(e) {
    document.getElementById('log-status').textContent = 'connection error';
  }
}

async function refreshAll() {
  const btn = document.getElementById('refresh-btn');
  btn.classList.add('loading');
  btn.textContent = 'Loading...';
  await Promise.all([loadStats(), loadTorrents(), loadParsed(), loadImdb(), pollLogs()]);
  btn.classList.remove('loading');
  btn.textContent = 'Refresh';
}

let logInterval = null;
function startLogPolling() {
  if (logInterval) clearInterval(logInterval);
  logInterval = setInterval(() => {
    if (document.getElementById('log-autopoll').checked) pollLogs();
  }, 3000);
}
document.getElementById('log-autopoll').addEventListener('change', (e) => {
  if (e.target.checked) startLogPolling();
  else if (logInterval) { clearInterval(logInterval); logInterval = null; }
});

refreshAll();
startLogPolling();
</script>
</body>
</html>"##;
