mod db;
mod dht;
mod hashlist;
mod imdb_pipeline;
mod imdb_resolver;
mod ingestor;
mod logs;
mod metrics;
mod packs_worker;
mod pipeline;
mod singles_worker;
mod title_parser;
mod torrent_cache;

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

async fn api_hashlists(
    State(state): State<AppState>,
    Query(params): Query<PageQuery>,
) -> axum::Json<serde_json::Value> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    match state.db.get_hashlists_page(page, per_page).await {
        Ok(p) => axum::Json(serde_json::json!({ "items": p.items, "total": p.total, "page": p.page, "per_page": p.per_page })),
        Err(e) => axum::Json(serde_json::json!({ "error": e.to_string() })),
    }
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
    match state.db.get_parsed_page(page, per_page).await {
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
    match state.db.get_imdb_page(page, per_page).await {
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
        hashlists: 0, torrents: 0, parsed: 0, imdb: 0, streams: 0,
    });
    axum::Json(serde_json::json!({
        "hashlists": counts.hashlists,
        "torrents": counts.torrents,
        "parsed": counts.parsed,
        "imdb": counts.imdb,
        "streams": counts.streams,
    }))
}

async fn api_queues(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    use std::sync::atomic::Ordering;

    let z = db::PipelineStats { unresolved: 0, completed: 0, exhausted: 0 };
    let d = state.db.get_queue_depths().await.unwrap_or(db::QueueDepths {
        hashlists: z.clone(), extract: z.clone(), parse: z.clone(),
        imdb: z.clone(), singles: z.clone(), packs: z,
    });

    axum::Json(serde_json::json!({
        "hashlists":  { "active": state.queues.hashlists.load(Ordering::Relaxed), "unresolved": d.hashlists.unresolved, "completed": d.hashlists.completed, "exhausted": d.hashlists.exhausted },
        "extract":    { "active": state.queues.extract.load(Ordering::Relaxed), "unresolved": d.extract.unresolved, "completed": d.extract.completed, "exhausted": d.extract.exhausted },
        "parse":      { "active": state.queues.parse.load(Ordering::Relaxed), "unresolved": d.parse.unresolved, "completed": d.parse.completed, "exhausted": d.parse.exhausted },
        "imdb":       { "active": state.queues.imdb.load(Ordering::Relaxed), "unresolved": d.imdb.unresolved, "completed": d.imdb.completed, "exhausted": d.imdb.exhausted },
        "singles":    { "active": state.queues.singles.load(Ordering::Relaxed), "unresolved": d.singles.unresolved, "completed": d.singles.completed, "exhausted": d.singles.exhausted },
        "packs":      { "active": state.queues.packs.load(Ordering::Relaxed), "unresolved": d.packs.unresolved, "completed": d.packs.completed, "exhausted": d.packs.exhausted },
    }))
}

async fn api_streams(
    State(state): State<AppState>,
    Query(params): Query<PageQuery>,
) -> axum::Json<serde_json::Value> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);
    match state.db.get_streams_page(page, per_page).await {
        Ok(p) => axum::Json(serde_json::json!({
            "items": p.items,
            "total": p.total,
            "page": p.page,
            "per_page": p.per_page,
        })),
        Err(e) => axum::Json(serde_json::json!({ "error": e.to_string() })),
    }
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


async fn api_metrics(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    let histories = state.metrics.get_all().await;
    axum::Json(serde_json::json!({ "counters": histories }))
}

#[derive(Deserialize)]
struct PipelineAction {
    pipeline: String,
    action: String, // "pause" or "resume"
}

async fn api_pipeline_control(
    State(state): State<AppState>,
    axum::Json(body): axum::Json<PipelineAction>,
) -> axum::Json<serde_json::Value> {
    let paused = body.action == "pause";
    state.metrics.controls.set_paused(&body.pipeline, paused);
    log!(
        state.logs,
        "[CONTROL] {} {}",
        body.pipeline,
        if paused { "PAUSED" } else { "RESUMED" }
    );
    let status: Vec<_> = state
        .metrics
        .controls
        .status()
        .into_iter()
        .map(|(name, enabled)| serde_json::json!({ "name": name, "enabled": enabled }))
        .collect();
    axum::Json(serde_json::json!({ "pipelines": status }))
}

#[derive(Deserialize)]
struct ResetAttemptsRequest {
    table: String,
}

async fn api_reset_attempts(
    State(state): State<AppState>,
    axum::Json(body): axum::Json<ResetAttemptsRequest>,
) -> axum::Json<serde_json::Value> {
    match state.db.reset_attempts(&body.table).await {
        Ok(count) => {
            log!(state.logs, "[CONTROL] Reset attempts for {}: {} rows", body.table, count);
            axum::Json(serde_json::json!({ "reset": count }))
        }
        Err(e) => axum::Json(serde_json::json!({ "error": e.to_string() })),
    }
}

async fn api_pipeline_status(State(state): State<AppState>) -> axum::Json<serde_json::Value> {
    let status: Vec<_> = state
        .metrics
        .controls
        .status()
        .into_iter()
        .map(|(name, enabled)| serde_json::json!({ "name": name, "enabled": enabled }))
        .collect();
    axum::Json(serde_json::json!({ "pipelines": status }))
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

    // Start metrics snapshot task (every 60s)
    state.metrics.clone().start_snapshot_task();

    // Build a shared HTTP client and IMDb resolver for the pipeline
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .expect("Failed to build HTTP client");
    let resolver = ImdbResolver::new(http_client);

    // Spawn 6 pipeline workers
    let s1 = state.clone();
    tokio::spawn(async move {
        ingestor::run_pipeline1_discover(s1, std::time::Duration::from_secs(poll_secs)).await;
    });

    let s2 = state.clone();
    tokio::spawn(async move {
        ingestor::run_pipeline2_extract(s2).await;
    });

    let s3 = state.clone();
    tokio::spawn(async move {
        ingestor::run_pipeline3_parse(s3).await;
    });

    let s4 = state.clone();
    let imdb_resolver = resolver.clone();
    tokio::spawn(async move {
        imdb_pipeline::run_pipeline4_imdb(s4, imdb_resolver).await;
    });

    let s5 = state.clone();
    tokio::spawn(async move {
        singles_worker::run_pipeline5_singles(s5).await;
    });

    let s6 = state.clone();
    tokio::spawn(async move {
        packs_worker::run_pipeline6_packs(s6).await;
    });

    log!(state.logs, "Starting web dashboard on http://0.0.0.0:3000");

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/api/hashlists", get(api_hashlists))
        .route("/api/torrents", get(api_torrents))
        .route("/api/parsed", get(api_parsed))
        .route("/api/imdb", get(api_imdb))
        .route("/api/streams", get(api_streams))
        .route("/api/stats", get(api_stats))
        .route("/api/queues", get(api_queues))
        .route("/api/logs", get(api_logs))
        .route("/api/metrics", get(api_metrics))
        .route("/api/pipelines", get(api_pipeline_status))
        .route("/api/pipelines/control", axum::routing::post(api_pipeline_control))
        .route("/api/pipelines/reset-attempts", axum::routing::post(api_reset_attempts))
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

  .queues { display: flex; gap: 14px; margin-bottom: 20px; flex-wrap: wrap; }
  .queue-card { background: #161b22; border: 1px solid #21262d; border-radius: 10px; padding: 16px 20px;
                flex: 1; min-width: 220px; }
  .q-header { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
  .q-name { font-weight: 600; font-size: 0.95em; color: #c9d1d9; }
  .q-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .q-dot.active { background: #3fb950; animation: pulse 2s infinite; }
  .q-dot.idle { background: #484f58; }
  .q-status { font-size: 0.75em; color: #8b949e; margin-left: auto; text-transform: uppercase;
              letter-spacing: 0.05em; font-weight: 500; }
  .q-status.active { color: #3fb950; }
  .q-rows { display: flex; flex-direction: column; gap: 6px; }
  .q-row { display: flex; justify-content: space-between; align-items: center; }
  .q-label { color: #8b949e; font-size: 0.82em; }
  .q-val { font-weight: 600; font-size: 0.95em; }
  .q-val.pending { color: #d29922; }
  .q-val.done { color: #3fb950; }
  .q-val.failed { color: #f85149; }
  .q-bar { height: 4px; background: #21262d; border-radius: 2px; margin-top: 10px; overflow: hidden; }
  .q-bar-fill { height: 100%; border-radius: 2px; transition: width 0.5s ease; }
  .q-bar-fill.green { background: linear-gradient(90deg, #238636, #3fb950); }
  .q-bar-fill.yellow { background: linear-gradient(90deg, #9e6a03, #d29922); }

  .section-label { color: #8b949e; font-size: 0.75em; text-transform: uppercase; letter-spacing: 0.08em;
                   margin-bottom: 8px; font-weight: 600; }

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
  .loading-msg { color: #8b949e; padding: 40px; text-align: center; font-size: 0.95em; }

  .pagination { display: flex; align-items: center; gap: 8px; margin-top: 12px; justify-content: center; }
  .pagination button { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
                        padding: 6px 14px; cursor: pointer; font-size: 0.82em; transition: all 0.15s; }
  .pagination button:hover:not(:disabled) { background: #30363d; }
  .pagination button:disabled { opacity: 0.4; cursor: default; }
  .pagination .page-info { color: #8b949e; font-size: 0.85em; }
  .pagination select { background: #21262d; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px;
                        padding: 4px 8px; font-size: 0.82em; }

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

<div class="section-label">Pipeline Queues</div>
<div class="queues" id="queues">
  <div class="queue-card" id="q-hashlists"><div class="q-header"><span class="q-dot idle" id="q-hashlists-dot"></span><span class="q-name">P1: Discover</span><span class="q-status" id="q-hashlists-status">idle</span></div><div class="q-rows"><div class="q-row"><span class="q-label">Unresolved</span><span class="q-val pending" id="q-hashlists-unresolved">-</span></div><div class="q-row"><span class="q-label">Completed</span><span class="q-val done" id="q-hashlists-completed">-</span></div><div class="q-row"><span class="q-label">Exhausted</span><span class="q-val failed" id="q-hashlists-exhausted">-</span></div></div><div class="q-bar"><div class="q-bar-fill green" id="q-hashlists-bar" style="width:0%"></div></div></div>
  <div class="queue-card" id="q-extract"><div class="q-header"><span class="q-dot idle" id="q-extract-dot"></span><span class="q-name">P2: Extract</span><span class="q-status" id="q-extract-status">idle</span></div><div class="q-rows"><div class="q-row"><span class="q-label">Unresolved</span><span class="q-val pending" id="q-extract-unresolved">-</span></div><div class="q-row"><span class="q-label">Completed</span><span class="q-val done" id="q-extract-completed">-</span></div><div class="q-row"><span class="q-label">Exhausted</span><span class="q-val failed" id="q-extract-exhausted">-</span></div></div><div class="q-bar"><div class="q-bar-fill green" id="q-extract-bar" style="width:0%"></div></div></div>
  <div class="queue-card" id="q-parse"><div class="q-header"><span class="q-dot idle" id="q-parse-dot"></span><span class="q-name">P3: Parse</span><span class="q-status" id="q-parse-status">idle</span></div><div class="q-rows"><div class="q-row"><span class="q-label">Unresolved</span><span class="q-val pending" id="q-parse-unresolved">-</span></div><div class="q-row"><span class="q-label">Completed</span><span class="q-val done" id="q-parse-completed">-</span></div><div class="q-row"><span class="q-label">Exhausted</span><span class="q-val failed" id="q-parse-exhausted">-</span></div></div><div class="q-bar"><div class="q-bar-fill green" id="q-parse-bar" style="width:0%"></div></div></div>
  <div class="queue-card" id="q-imdb"><div class="q-header"><span class="q-dot idle" id="q-imdb-dot"></span><span class="q-name">P4: IMDb</span><span class="q-status" id="q-imdb-status">idle</span></div><div class="q-rows"><div class="q-row"><span class="q-label">Unresolved</span><span class="q-val pending" id="q-imdb-unresolved">-</span></div><div class="q-row"><span class="q-label">Completed</span><span class="q-val done" id="q-imdb-completed">-</span></div><div class="q-row"><span class="q-label">Exhausted</span><span class="q-val failed" id="q-imdb-exhausted">-</span></div></div><div class="q-bar"><div class="q-bar-fill green" id="q-imdb-bar" style="width:0%"></div></div></div>
  <div class="queue-card" id="q-singles"><div class="q-header"><span class="q-dot idle" id="q-singles-dot"></span><span class="q-name">P5: Singles</span><span class="q-status" id="q-singles-status">idle</span></div><div class="q-rows"><div class="q-row"><span class="q-label">Unresolved</span><span class="q-val pending" id="q-singles-unresolved">-</span></div><div class="q-row"><span class="q-label">Completed</span><span class="q-val done" id="q-singles-completed">-</span></div><div class="q-row"><span class="q-label">Exhausted</span><span class="q-val failed" id="q-singles-exhausted">-</span></div></div><div class="q-bar"><div class="q-bar-fill green" id="q-singles-bar" style="width:0%"></div></div></div>
  <div class="queue-card" id="q-packs"><div class="q-header"><span class="q-dot idle" id="q-packs-dot"></span><span class="q-name">P6: Packs</span><span class="q-status" id="q-packs-status">idle</span></div><div class="q-rows"><div class="q-row"><span class="q-label">Unresolved</span><span class="q-val pending" id="q-packs-unresolved">-</span></div><div class="q-row"><span class="q-label">Completed</span><span class="q-val done" id="q-packs-completed">-</span></div><div class="q-row"><span class="q-label">Exhausted</span><span class="q-val failed" id="q-packs-exhausted">-</span></div></div><div class="q-bar"><div class="q-bar-fill green" id="q-packs-bar" style="width:0%"></div></div></div>
</div>

<div class="tabs">
  <div class="tab active" data-tab="hashlists">Hashlists</div>
  <div class="tab" data-tab="torrents">Torrents</div>
  <div class="tab" data-tab="parsed">Parsed</div>
  <div class="tab" data-tab="imdb">IMDb</div>
  <div class="tab" data-tab="streams">Streams</div>
  <div class="tab" data-tab="controls">Controls</div>
  <div class="tab" data-tab="metrics">Metrics</div>
  <div class="tab" data-tab="logs">Logs</div>
</div>

<div id="tab-hashlists" class="tab-content active">
  <div class="tab-panel">
    <div class="tbl-wrap" id="hashlists-table"><div class="loading-msg">Loading...</div></div>
    <div class="pagination" id="hashlists-pag"></div>
  </div>
</div>

<div id="tab-torrents" class="tab-content">
  <div class="tab-panel">
    <div class="tbl-wrap" id="torrents-table"><div class="loading-msg">Loading...</div></div>
    <div class="pagination" id="torrents-pag"></div>
  </div>
</div>

<div id="tab-parsed" class="tab-content">
  <div class="tab-panel">
    <div class="tbl-wrap" id="parsed-table"><div class="loading-msg">Click to load</div></div>
    <div class="pagination" id="parsed-pag"></div>
  </div>
</div>

<div id="tab-imdb" class="tab-content">
  <div class="tab-panel">
    <div class="tbl-wrap" id="imdb-table"><div class="loading-msg">Click to load</div></div>
    <div class="pagination" id="imdb-pag"></div>
  </div>
</div>

<div id="tab-streams" class="tab-content">
  <div class="tab-panel">
    <div class="tbl-wrap" id="streams-table"><div class="loading-msg">Click to load</div></div>
    <div class="pagination" id="streams-pag"></div>
  </div>
</div>

<div id="tab-controls" class="tab-content">
  <div class="tab-panel">
    <div class="section-label">Pipeline Controls</div>
    <div id="pipeline-controls" style="display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap"></div>
    <div class="section-label">Reset Attempts</div>
    <div style="display:flex;gap:10px;flex-wrap:wrap" id="reset-buttons">
      <button onclick="resetAttempts('dmm_hashlists')" style="background:#21262d;color:#58a6ff;border:1px solid #30363d;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:0.82em">Reset Hashlists</button>
      <button onclick="resetAttempts('torrents')" style="background:#21262d;color:#58a6ff;border:1px solid #30363d;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:0.82em">Reset Torrents</button>
      <button onclick="resetAttempts('parsed_torrents')" style="background:#21262d;color:#58a6ff;border:1px solid #30363d;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:0.82em">Reset Parsed</button>
      <button onclick="resetAttempts('imdb_mappings')" style="background:#21262d;color:#58a6ff;border:1px solid #30363d;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:0.82em">Reset IMDb</button>
    </div>
  </div>
</div>

<div id="tab-metrics" class="tab-content">
  <div class="tab-panel">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px">
      <span class="section-label" style="margin-bottom:0">Throughput (per minute)</span>
      <select id="metrics-window" onchange="loadMetrics()" style="background:#21262d;color:#c9d1d9;border:1px solid #30363d;border-radius:6px;padding:4px 10px;font-size:0.82em">
        <option value="1">Last 1 min</option>
        <option value="5">Last 5 min</option>
        <option value="30" selected>Last 30 min</option>
        <option value="60">Last 60 min</option>
      </select>
      <label style="color:#8b949e;font-size:0.82em"><input type="checkbox" id="metrics-autorefresh" checked> Auto-refresh (5s)</label>
    </div>
    <div id="metrics-charts"></div>
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
let torrentsPage = 1, parsedPage = 1, imdbPage = 1, hashlistsPage = 1;
let perPage = 25;
const tabLoaded = { hashlists: false, torrents: false, parsed: false, imdb: false, streams: false, controls: false, metrics: false, logs: false };
let activeTab = 'hashlists';

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    tab.classList.add('active');
    activeTab = tab.dataset.tab;
    document.getElementById('tab-' + activeTab).classList.add('active');
    loadActiveTab();
  });
});

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function fmtSize(bytes) {
  if (bytes == null || bytes === 0) return '0 B';
  const gb = 1073741824, mb = 1048576, kb = 1024;
  if (bytes >= gb) return (bytes / gb).toFixed(2) + ' GB';
  if (bytes >= mb) return (bytes / mb).toFixed(1) + ' MB';
  return (bytes / kb).toFixed(0) + ' KB';
}

function fmtTime(iso) {
  try { return new Date(iso).toLocaleString(); } catch(e) { return iso; }
}

function fmtNum(n) {
  return (n || 0).toLocaleString();
}

function renderPagination(containerId, page, total, pp, onPageFn) {
  const totalPages = Math.max(1, Math.ceil(total / pp));
  const el = document.getElementById(containerId);
  const pageSizes = [25, 50, 100];
  const opts = pageSizes.map(s => `<option value="${s}" ${s === pp ? 'selected' : ''}>${s}/page</option>`).join('');
  el.innerHTML = `
    <button ${page <= 1 ? 'disabled' : ''} onclick="${onPageFn}(1)">First</button>
    <button ${page <= 1 ? 'disabled' : ''} onclick="${onPageFn}(${page - 1})">Prev</button>
    <span class="page-info">Page ${page} of ${fmtNum(totalPages)} (${fmtNum(total)} rows)</span>
    <button ${page >= totalPages ? 'disabled' : ''} onclick="${onPageFn}(${page + 1})">Next</button>
    <button ${page >= totalPages ? 'disabled' : ''} onclick="${onPageFn}(${totalPages})">Last</button>
    <select onchange="perPage=+this.value;${onPageFn}(1)">${opts}</select>`;
}

function setQueueCard(prefix, data) {
  const dot = document.getElementById(prefix + '-dot');
  const status = document.getElementById(prefix + '-status');
  if (data.active) {
    dot.className = 'q-dot active';
    status.className = 'q-status active';
    status.textContent = 'active';
  } else {
    dot.className = 'q-dot idle';
    status.className = 'q-status';
    status.textContent = 'idle';
  }

  const unEl = document.getElementById(prefix + '-unresolved');
  const compEl = document.getElementById(prefix + '-completed');
  const exEl = document.getElementById(prefix + '-exhausted');
  if (unEl) unEl.textContent = fmtNum(data.unresolved);
  if (compEl) compEl.textContent = fmtNum(data.completed);
  if (exEl) exEl.textContent = fmtNum(data.exhausted);

  const bar = document.getElementById(prefix + '-bar');
  if (bar) {
    const total = (data.completed || 0) + (data.unresolved || 0) + (data.exhausted || 0);
    const pct = total > 0 ? Math.round(((data.completed || 0) / total) * 100) : 0;
    bar.style.width = pct + '%';
    bar.className = 'q-bar-fill ' + (pct >= 80 ? 'green' : 'yellow');
  }
}

async function loadQueues() {
  try {
    const r = await fetch('/api/queues');
    const q = await r.json();
    setQueueCard('q-hashlists', q.hashlists);
    setQueueCard('q-extract', q.extract);
    setQueueCard('q-parse', q.parse);
    setQueueCard('q-imdb', q.imdb);
    setQueueCard('q-singles', q.singles);
    setQueueCard('q-packs', q.packs);
  } catch(e) {}
}

async function loadStats() {
  try {
    const r = await fetch('/api/stats');
    const s = await r.json();
    document.getElementById('stats').innerHTML = `
      <div class="stat"><div class="num">${fmtNum(s.hashlists)}</div><div class="label">Hashlists</div></div>
      <div class="stat"><div class="num">${fmtNum(s.torrents)}</div><div class="label">Torrents</div></div>
      <div class="stat"><div class="num">${fmtNum(s.parsed)}</div><div class="label">Parsed</div></div>
      <div class="stat"><div class="num">${fmtNum(s.imdb)}</div><div class="label">IMDb</div></div>
      <div class="stat"><div class="num">${fmtNum(s.streams)}</div><div class="label">Streams</div></div>`;
    document.getElementById('subtitle').textContent =
      `${fmtNum(s.hashlists)} hashlists | ${fmtNum(s.torrents)} torrents | ${fmtNum(s.parsed)} parsed | ${fmtNum(s.imdb)} IMDb | ${fmtNum(s.streams)} streams`;
  } catch(e) {
    document.getElementById('subtitle').textContent = 'Failed to load stats';
  }
}

async function loadHashlists(page) {
  if (page !== undefined) hashlistsPage = page;
  document.getElementById('hashlists-table').innerHTML = '<div class="loading-msg">Loading...</div>';
  try {
    const r = await fetch(`/api/hashlists?page=${hashlistsPage}&per_page=${perPage}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('hashlists-table').innerHTML = '<div class="empty">No hashlists yet</div>';
      document.getElementById('hashlists-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Name</th><th>Completed</th><th>Attempts</th><th>Last Processed</th></tr>`;
    for (const h of data.items) {
      const badge = h.completed ? '<span style="color:#3fb950">yes</span>' : '<span style="color:#d29922">no</span>';
      html += `<tr>
        <td><code>${esc(h.name)}</code></td>
        <td>${badge}</td>
        <td>${h.attempts}</td>
        <td class="ts">${fmtTime(h.last_processed)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('hashlists-table').innerHTML = html;
    renderPagination('hashlists-pag', data.page, data.total, data.per_page, 'loadHashlists');
    tabLoaded.hashlists = true;
  } catch(e) {
    document.getElementById('hashlists-table').innerHTML = '<div class="empty">Failed to load hashlists</div>';
  }
}

async function loadTorrents(page) {
  if (page !== undefined) torrentsPage = page;
  document.getElementById('torrents-table').innerHTML = '<div class="loading-msg">Loading...</div>';
  try {
    const r = await fetch(`/api/torrents?page=${torrentsPage}&per_page=${perPage}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('torrents-table').innerHTML = '<div class="empty">No torrents yet</div>';
      document.getElementById('torrents-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Hash</th><th>Filename</th><th>Size</th><th>Done</th><th>Attempts</th><th>Last Processed</th></tr>`;
    for (const t of data.items) {
      html += `<tr>
        <td><code>${esc(t.hash.slice(0,16))}</code></td>
        <td class="fn" title="${esc(t.filename)}">${esc(t.filename)}</td>
        <td>${fmtSize(t.size_bytes)}</td>
        <td>${t.completed ? '<span style="color:#3fb950">yes</span>' : '<span style="color:#d29922">no</span>'}</td>
        <td>${t.attempts || 0}</td>
        <td class="ts">${fmtTime(t.last_processed)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('torrents-table').innerHTML = html;
    renderPagination('torrents-pag', data.page, data.total, data.per_page, 'loadTorrents');
    tabLoaded.torrents = true;
  } catch(e) {
    document.getElementById('torrents-table').innerHTML = '<div class="empty">Failed to load torrents</div>';
  }
}

async function loadParsed(page) {
  if (page !== undefined) parsedPage = page;
  document.getElementById('parsed-table').innerHTML = '<div class="loading-msg">Loading...</div>';
  try {
    const r = await fetch(`/api/parsed?page=${parsedPage}&per_page=${perPage}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('parsed-table').innerHTML = '<div class="empty">No parsed metadata yet</div>';
      document.getElementById('parsed-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Hash</th><th>Title</th><th>Year</th><th>S</th><th>E</th><th>Done</th><th>Attempts</th><th>Last Processed</th></tr>`;
    for (const p of data.items) {
      html += `<tr>
        <td><code>${esc(p.hash.slice(0,16))}</code></td>
        <td class="fn" title="${esc(p.title)}">${esc(p.title)}</td>
        <td>${p.year != null ? p.year : '-'}</td>
        <td>${p.season != null ? p.season : '-'}</td>
        <td>${p.episode != null ? p.episode : '-'}</td>
        <td>${p.completed ? '<span style="color:#3fb950">yes</span>' : '<span style="color:#d29922">no</span>'}</td>
        <td>${p.attempts || 0}</td>
        <td class="ts">${fmtTime(p.last_processed)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('parsed-table').innerHTML = html;
    renderPagination('parsed-pag', data.page, data.total, data.per_page, 'loadParsed');
    tabLoaded.parsed = true;
  } catch(e) {
    document.getElementById('parsed-table').innerHTML = '<div class="empty">Failed to load parsed metadata</div>';
  }
}

async function loadImdb(page) {
  if (page !== undefined) imdbPage = page;
  document.getElementById('imdb-table').innerHTML = '<div class="loading-msg">Loading...</div>';
  try {
    const r = await fetch(`/api/imdb?page=${imdbPage}&per_page=${perPage}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('imdb-table').innerHTML = '<div class="empty">No IMDb mappings yet</div>';
      document.getElementById('imdb-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Hash</th><th>IMDb ID</th><th>Type</th><th>Done</th><th>Attempts</th><th>Last Processed</th></tr>`;
    for (const m of data.items) {
      const badge = m.content_type === 'series'
        ? '<span style="color:#58a6ff;font-weight:600">series</span>'
        : '<span style="color:#d29922;font-weight:600">movie</span>';
      html += `<tr>
        <td><code>${esc(m.hash.slice(0,16))}</code></td>
        <td><a class="imdb-link" href="https://www.imdb.com/title/${esc(m.imdb_id)}/" target="_blank" rel="noopener">${esc(m.imdb_id)}</a></td>
        <td>${badge}</td>
        <td>${m.completed ? '<span style="color:#3fb950">yes</span>' : '<span style="color:#d29922">no</span>'}</td>
        <td>${m.attempts || 0}</td>
        <td class="ts">${fmtTime(m.last_processed)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('imdb-table').innerHTML = html;
    renderPagination('imdb-pag', data.page, data.total, data.per_page, 'loadImdb');
    tabLoaded.imdb = true;
  } catch(e) {
    document.getElementById('imdb-table').innerHTML = '<div class="empty">Failed to load IMDb mappings</div>';
  }
}

let streamsPage = 1;

async function loadStreams(page) {
  if (page !== undefined) streamsPage = page;
  document.getElementById('streams-table').innerHTML = '<div class="loading-msg">Loading...</div>';
  try {
    const r = await fetch(`/api/streams?page=${streamsPage}&per_page=${perPage}`);
    const data = await r.json();
    if (!data.items || data.items.length === 0) {
      document.getElementById('streams-table').innerHTML = '<div class="empty">No streams yet</div>';
      document.getElementById('streams-pag').innerHTML = '';
      return;
    }
    let html = `<table><tr><th>Hash</th><th>Filename</th><th>IMDb</th><th>Type</th><th>Size</th><th>Idx</th><th>S</th><th>E</th><th>Updated</th></tr>`;
    for (const s of data.items) {
      const badge = s.stream_type === 'series'
        ? '<span style="color:#58a6ff;font-weight:600">series</span>'
        : '<span style="color:#d29922;font-weight:600">movie</span>';
      html += `<tr>
        <td><code>${esc(s.torrent_hash.slice(0,12))}</code></td>
        <td class="fn" title="${esc(s.filename)}">${esc(s.filename)}</td>
        <td><a class="imdb-link" href="https://www.imdb.com/title/${esc(s.imdb_id)}/" target="_blank" rel="noopener">${esc(s.imdb_id)}</a></td>
        <td>${badge}</td>
        <td>${fmtSize(s.size_bytes)}</td>
        <td>${s.file_index != null ? s.file_index : '-'}</td>
        <td>${s.season != null ? s.season : '-'}</td>
        <td>${s.episode != null ? s.episode : '-'}</td>
        <td class="ts">${fmtTime(s.created_at)}</td>
      </tr>`;
    }
    html += '</table>';
    document.getElementById('streams-table').innerHTML = html;
    renderPagination('streams-pag', data.page, data.total, data.per_page, 'loadStreams');
    tabLoaded.streams = true;
  } catch(e) {
    document.getElementById('streams-table').innerHTML = '<div class="empty">Failed to load streams</div>';
  }
}

async function loadMetrics() {
  try {
    const mr = await fetch('/api/metrics');
    const md = await mr.json();
    const chartsEl = document.getElementById('metrics-charts');
    const window = parseInt(document.getElementById('metrics-window').value) || 30;
    chartsEl.innerHTML = md.counters.map(c => {
      const pts = c.points.slice(-window);
      const windowTotal = pts.reduce((a, b) => a + b, 0);
      const w = 480, h = 60, pad = 2;
      const max = Math.max(...pts, 1);
      const coords = pts.map((v, i) => {
        const x = pad + (i / (pts.length - 1 || 1)) * (w - pad * 2);
        const y = h - pad - (v / max) * (h - pad * 2);
        return `${x},${y}`;
      });
      const line = coords.join(' ');
      const area = `${pad},${h - pad} ${line} ${w - pad},${h - pad}`;
      const lastPt = coords.length > 0 ? coords[coords.length - 1].split(',') : null;
      const svg = `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" style="width:100%;height:${h}px">
        <polygon points="${area}" fill="#58a6ff11" />
        <polyline points="${line}" fill="none" stroke="#58a6ff" stroke-width="1.5" stroke-linejoin="round" />
        ${lastPt ? `<circle cx="${lastPt[0]}" cy="${lastPt[1]}" r="3" fill="#58a6ff" />` : ''}
      </svg>`;
      return `<div style="background:#161b22;border:1px solid #21262d;border-radius:8px;padding:14px 18px;margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
          <span style="font-weight:600;font-size:0.88em;color:#c9d1d9">${esc(c.name)}</span>
          <span style="font-size:0.82em;color:#8b949e">last ${window}m: <b style="color:#58a6ff">${fmtNum(windowTotal)}</b> | total: <b style="color:#3fb950">${fmtNum(c.total)}</b></span>
        </div>
        ${svg}
      </div>`;
    }).join('');
  } catch(e) {
    document.getElementById('metrics-charts').innerHTML = '<div class="empty">Failed to load metrics</div>';
  }
}

async function resetAttempts(table) {
  if (!confirm(`Reset all attempts to 0 for ${table}?`)) return;
  const r = await fetch('/api/pipelines/reset-attempts', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({table})
  });
  const data = await r.json();
  alert(`Reset ${data.reset || 0} rows in ${table}`);
}

async function togglePipeline(name, action) {
  await fetch('/api/pipelines/control', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({pipeline: name, action: action})
  });
  loadMetrics();
}

async function loadControls() {
  try {
    const pr = await fetch('/api/pipelines');
    const ps = await pr.json();
    document.getElementById('pipeline-controls').innerHTML = ps.pipelines.map(p => {
      const color = p.enabled ? '#3fb950' : '#f85149';
      const label = p.enabled ? 'Running' : 'Paused';
      const action = p.enabled ? 'pause' : 'resume';
      return `<div style="background:#161b22;border:1px solid #21262d;border-radius:8px;padding:14px 20px;min-width:160px">
        <div style="font-weight:600;font-size:0.95em;color:#c9d1d9;margin-bottom:10px">${esc(p.name)}</div>
        <div style="display:flex;align-items:center;gap:10px">
          <span style="color:${color};font-size:0.85em;font-weight:600">${label}</span>
          <button onclick="togglePipeline('${p.name}','${action}')"
            style="background:${p.enabled ? '#f8514922' : '#3fb95022'};color:${p.enabled ? '#f85149' : '#3fb950'};
            border:1px solid ${p.enabled ? '#f8514944' : '#3fb95044'};border-radius:6px;padding:5px 14px;
            cursor:pointer;font-size:0.82em;font-weight:600">${p.enabled ? 'Pause' : 'Resume'}</button>
        </div>
      </div>`;
    }).join('');
  } catch(e) {}
}

function loadActiveTab() {
  switch(activeTab) {
    case 'hashlists': loadHashlists(); break;
    case 'torrents': loadTorrents(); break;
    case 'parsed': loadParsed(); break;
    case 'imdb': loadImdb(); break;
    case 'streams': loadStreams(); break;
    case 'controls': loadControls(); break;
    case 'metrics': loadMetrics(); break;
    case 'logs': if (!tabLoaded.logs) { tabLoaded.logs = true; pollLogs(); } break;
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
        else if (line.match(/\[(HASHLIST-Q|IMDB-Q|SINGLES-Q|PACKS-Q|INGESTOR|PIPELINE|IMDB)\]/)) span.className = 'log-info';
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
  await Promise.all([loadStats(), loadQueues(), loadActiveTab()]);
  btn.classList.remove('loading');
  btn.textContent = 'Refresh';
}

let pollInterval = null;
let metricsInterval = null;
function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(() => {
    if (document.getElementById('log-autopoll').checked) {
      loadStats();
      loadQueues();
      if (activeTab === 'logs') pollLogs();
    }
  }, 3000);
  if (metricsInterval) clearInterval(metricsInterval);
  metricsInterval = setInterval(() => {
    if (activeTab === 'metrics' && document.getElementById('metrics-autorefresh').checked) {
      loadMetrics();
    }
  }, 5000);
}
document.getElementById('log-autopoll').addEventListener('change', (e) => {
  if (e.target.checked) startPolling();
  else if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
});

refreshAll();
startPolling();
</script>
</body>
</html>"##;
