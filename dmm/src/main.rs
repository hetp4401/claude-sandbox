mod db;
mod dht;
mod hashlist;
mod ingestor;
mod logs;
mod pipeline;
mod title_parser;

use axum::{extract::Query, extract::State, response::Html, routing::get, Router};
use ingestor::{AppState, HashlistStatus, MagnetRecord};
use logs::LogBuffer;
use serde::Deserialize;

async fn dashboard(State(_state): State<AppState>) -> Html<String> {
    Html(DASHBOARD_HTML.to_string())
}

async fn api_records(State(state): State<AppState>) -> axum::Json<Vec<MagnetRecord>> {
    axum::Json(state.db.get_all_records().unwrap_or_default())
}

async fn api_hashlists(State(state): State<AppState>) -> axum::Json<Vec<HashlistStatus>> {
    axum::Json(state.db.get_all_hashlists().unwrap_or_default())
}

async fn api_stats(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    let counts = state.db.count_records().unwrap_or(db::RecordCounts {
        total: 0, movies: 0, episodes: 0, seasons: 0,
    });
    let total_hl = state.db.count_hashlists().unwrap_or(0);
    let done_hl = state.db.count_hashlists_done().unwrap_or(0);

    axum::Json(serde_json::json!({
        "total_records": counts.total,
        "movies": counts.movies,
        "episodes": counts.episodes,
        "seasons": counts.seasons,
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

    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "/data/dmm.db".into());
    let poll_secs: u64 = std::env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3600);

    if let Some(parent) = std::path::Path::new(&db_path).parent() {
        std::fs::create_dir_all(parent).ok();
    }

    let database = db::Db::open(&db_path).expect("Failed to open database");
    log!(log_buf, "Database: {db_path}");

    let state = AppState::new(database, log_buf);

    if let Ok(counts) = state.db.count_records() {
        if counts.total > 0 {
            log!(
                state.logs,
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

    log!(state.logs, "Starting web dashboard on http://0.0.0.0:3000");

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/records", get(api_records))
        .route("/api/hashlists", get(api_hashlists))
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

  /* Tabs */
  .tabs { display: flex; gap: 0; margin-bottom: 0; border-bottom: 2px solid #21262d; }
  .tab { padding: 10px 24px; cursor: pointer; color: #8b949e; font-size: 0.9em; font-weight: 500;
         border-bottom: 2px solid transparent; margin-bottom: -2px; transition: all 0.15s; user-select: none; }
  .tab:hover { color: #c9d1d9; }
  .tab.active { color: #58a6ff; border-bottom-color: #58a6ff; }
  .tab-content { display: none; }
  .tab-content.active { display: block; }
  .tab-panel { padding-top: 16px; }

  /* Refresh button */
  .refresh-btn { background: #21262d; color: #58a6ff; border: 1px solid #30363d; border-radius: 6px;
                 padding: 6px 16px; cursor: pointer; font-size: 0.85em; margin-left: 12px; transition: all 0.15s; }
  .refresh-btn:hover { background: #30363d; }
  .refresh-btn:active { transform: scale(0.97); }
  .refresh-btn.loading { opacity: 0.6; pointer-events: none; }
  .topbar { display: flex; align-items: center; margin-bottom: 20px; }
  .topbar h1 { flex: 1; }

  /* Tables */
  table { width: 100%; border-collapse: collapse; background: #161b22; border-radius: 8px; overflow: hidden; }
  th { background: #21262d; color: #8b949e; text-align: left; padding: 10px 14px; font-size: 0.78em;
       text-transform: uppercase; letter-spacing: 0.05em; position: sticky; top: 0; z-index: 1; }
  td { padding: 10px 14px; border-bottom: 1px solid #21262d; font-size: 0.88em; }
  tr:hover { background: #1c2128; }
  code { background: #1c2128; padding: 2px 6px; border-radius: 4px; font-size: 0.82em; color: #79c0ff; }
  .fn { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .src { color: #d2a8ff; }
  .ts { color: #8b949e; font-size: 0.82em; white-space: nowrap; }
  .done { color: #3fb950; font-weight: 600; }
  .err { color: #f85149; font-weight: 600; }
  .type-movie { color: #79c0ff; font-weight: 600; }
  .type-ep { color: #3fb950; font-weight: 600; }
  .type-season { color: #d2a8ff; font-weight: 600; }
  .type-unk { color: #8b949e; }
  .tbl-wrap { max-height: 600px; overflow-y: auto; border-radius: 8px; }
  .empty { color: #484f58; padding: 40px; text-align: center; font-size: 0.95em; }

  /* Logs */
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
  <div class="tab active" data-tab="records">Records</div>
  <div class="tab" data-tab="hashlists">Hashlists</div>
  <div class="tab" data-tab="logs">Logs</div>
</div>

<div id="tab-records" class="tab-content active">
  <div class="tab-panel">
    <div class="tbl-wrap" id="records-table"></div>
  </div>
</div>

<div id="tab-hashlists" class="tab-content">
  <div class="tab-panel">
    <div class="tbl-wrap" id="hashlists-table"></div>
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
// Tab switching
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

function typeClass(t) {
  if (t === 'movie') return 'type-movie';
  if (t === 'episode') return 'type-ep';
  if (t === 'season') return 'type-season';
  return 'type-unk';
}

// Stats
async function loadStats() {
  try {
    const r = await fetch('/api/stats');
    const s = await r.json();
    document.getElementById('stats').innerHTML = `
      <div class="stat"><div class="num">${s.total_records}</div><div class="label">Total Records</div></div>
      <div class="stat"><div class="num">${s.movies}</div><div class="label">Movies</div></div>
      <div class="stat"><div class="num">${s.episodes}</div><div class="label">Episodes</div></div>
      <div class="stat"><div class="num">${s.seasons}</div><div class="label">Season Packs</div></div>
      <div class="stat"><div class="num">${s.total_hashlists}</div><div class="label">Hashlists</div></div>
      <div class="stat"><div class="num">${s.done_hashlists}</div><div class="label">Done</div></div>`;
    document.getElementById('subtitle').textContent =
      `${s.total_records} records across ${s.total_hashlists} hashlists`;
  } catch(e) {
    document.getElementById('subtitle').textContent = 'Failed to load stats';
  }
}

// Records table
async function loadRecords() {
  try {
    const r = await fetch('/api/records');
    const records = await r.json();
    if (records.length === 0) {
      document.getElementById('records-table').innerHTML = '<div class="empty">No records yet</div>';
      return;
    }
    let html = `<table><tr><th>Filename</th><th>Hash</th><th>Size</th><th>Type</th><th>Title</th><th>S</th><th>E</th><th>Idx</th><th>IMDb</th></tr>`;
    for (const r of records) {
      const tc = typeClass(r.content_type);
      html += `<tr>
        <td class="fn">${esc(r.filename)}</td>
        <td><code>${esc(r.hash.slice(0,8))}</code></td>
        <td>${fmtSize(r.size_bytes)}</td>
        <td class="${tc}">${esc(r.content_type || '\u2014')}</td>
        <td>${esc(r.title || '\u2014')}</td>
        <td>${r.season != null ? r.season : '\u2014'}</td>
        <td>${r.episode != null ? r.episode : '\u2014'}</td>
        <td>${r.file_index != null ? r.file_index : '\u2014'}</td>
        <td class="src">${esc(r.imdb_tag || '\u2014')}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('records-table').innerHTML = html;
  } catch(e) {
    document.getElementById('records-table').innerHTML = '<div class="empty">Failed to load records</div>';
  }
}

// Hashlists table
async function loadHashlists() {
  try {
    const r = await fetch('/api/hashlists');
    const hls = await r.json();
    if (hls.length === 0) {
      document.getElementById('hashlists-table').innerHTML = '<div class="empty">No hashlists processed yet</div>';
      return;
    }
    let html = `<table><tr><th>Name</th><th>Records</th><th>Status</th><th>Processed At</th></tr>`;
    for (const h of hls) {
      const sc = h.status === 'done' ? 'done' : 'err';
      html += `<tr>
        <td>${esc(h.name)}</td>
        <td>${h.record_count}</td>
        <td class="${sc}">${esc(h.status)}</td>
        <td class="ts">${esc(h.processed_at)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('hashlists-table').innerHTML = html;
  } catch(e) {
    document.getElementById('hashlists-table').innerHTML = '<div class="empty">Failed to load hashlists</div>';
  }
}

// Logs
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
        else if (line.includes('[INGESTOR]') || line.includes('[PIPELINE]')) span.className = 'log-info';
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

// Refresh all data
async function refreshAll() {
  const btn = document.getElementById('refresh-btn');
  btn.classList.add('loading');
  btn.textContent = 'Loading...';
  await Promise.all([loadStats(), loadRecords(), loadHashlists(), pollLogs()]);
  btn.classList.remove('loading');
  btn.textContent = 'Refresh';
}

// Log auto-polling
let logInterval = null;
function startLogPolling() {
  if (logInterval) clearInterval(logInterval);
  logInterval = setInterval(() => {
    if (document.getElementById('log-autopoll').checked) {
      pollLogs();
    }
  }, 3000);
}
document.getElementById('log-autopoll').addEventListener('change', (e) => {
  if (e.target.checked) startLogPolling();
  else if (logInterval) { clearInterval(logInterval); logInterval = null; }
});

// Initial load
refreshAll();
startLogPolling();
</script>
</body>
</html>"##;
