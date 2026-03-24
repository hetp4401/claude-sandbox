use crate::db::Db;
use crate::hashlist;
use crate::log;
use crate::logs::LogBuffer;
use crate::title_parser;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct QueueActivity {
    pub hashlists: Arc<AtomicBool>,
    pub extract: Arc<AtomicBool>,
    pub parse: Arc<AtomicBool>,
    pub imdb: Arc<AtomicBool>,
    pub singles: Arc<AtomicBool>,
    pub packs: Arc<AtomicBool>,
}

impl QueueActivity {
    pub fn new() -> Self {
        Self {
            hashlists: Arc::new(AtomicBool::new(false)),
            extract: Arc::new(AtomicBool::new(false)),
            parse: Arc::new(AtomicBool::new(false)),
            imdb: Arc::new(AtomicBool::new(false)),
            singles: Arc::new(AtomicBool::new(false)),
            packs: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub logs: LogBuffer,
    pub queues: QueueActivity,
    pub metrics: crate::metrics::Metrics,
}

impl AppState {
    pub fn new(db: Db, logs: LogBuffer) -> Self {
        Self {
            db,
            logs,
            queues: QueueActivity::new(),
            metrics: crate::metrics::Metrics::new(),
        }
    }
}

// =========================================================================
// GitHub API types
// =========================================================================

#[derive(Debug, Clone, serde::Deserialize, Serialize)]
pub struct GitTreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
}

#[derive(Debug, Clone, serde::Deserialize, Serialize)]
pub struct GitTreeResponse {
    pub tree: Vec<GitTreeEntry>,
    pub truncated: bool,
}

pub fn filter_hashlist_names(tree_resp: &GitTreeResponse) -> Vec<String> {
    tree_resp
        .tree
        .iter()
        .filter(|e| e.entry_type == "blob" && e.path.ends_with(".html") && e.path != "index.html")
        .map(|e| e.path.clone())
        .collect()
}

async fn fetch_hashlist_names(client: &reqwest::Client) -> Result<Vec<String>, String> {
    let url = "https://api.github.com/repos/debridmediamanager/hashlists/git/trees/main?recursive=1";
    let mut req = client
        .get(url)
        .header("User-Agent", "dmm-ingestor/0.1")
        .header("Accept", "application/vnd.github.v3+json");
    if let Ok(token) = std::env::var("GITHUB_TOKEN") {
        if !token.is_empty() {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("GitHub API request failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GitHub API returned {}", resp.status()));
    }
    let tree_resp: GitTreeResponse = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse GitHub response: {e}"))?;
    Ok(filter_hashlist_names(&tree_resp))
}

// =========================================================================
// Pipeline 1: GitHub → dmm_hashlists (runs every poll_interval)
// =========================================================================

pub async fn run_pipeline1_discover(state: AppState, poll_interval: std::time::Duration) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .expect("Failed to build HTTP client");

    log!(state.logs, "[P1-DISCOVER] Started, poll interval: {}s", poll_interval.as_secs());

    loop {
        if state.metrics.controls.is_paused("hashlists") {
            state.queues.hashlists.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        state.queues.hashlists.store(true, Ordering::Relaxed);
        log!(state.logs, "[P1-DISCOVER] Polling GitHub...");

        match fetch_hashlist_names(&client).await {
            Ok(names) => {
                let total = names.len();
                let mut new_count = 0u64;
                for name in &names {
                    if let Ok(()) = state.db.insert_hashlist_name(name).await {
                        new_count += 1;
                    }
                }
                // new_count includes conflicts (already exists), but that's fine
                log!(state.logs, "[P1-DISCOVER] Found {total} hashlists on GitHub");
                state.metrics.bump("hashlists_discovered", new_count);
            }
            Err(e) => {
                log!(state.logs, "[P1-DISCOVER] GitHub error: {e}, retrying in 30s...");
                state.queues.hashlists.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
        }

        state.queues.hashlists.store(false, Ordering::Relaxed);
        log!(state.logs, "[P1-DISCOVER] Sleeping {}s...", poll_interval.as_secs());
        tokio::time::sleep(poll_interval).await;
    }
}

// =========================================================================
// Pipeline 2: dmm_hashlists → torrents (continuous)
// =========================================================================


async fn fetch_and_parse_hashlist(
    client: &reqwest::Client,
    name: &str,
) -> Result<hashlist::Hashlist, hashlist::ParseError> {
    let url = format!(
        "https://raw.githubusercontent.com/debridmediamanager/hashlists/main/{name}"
    );
    let html = client
        .get(&url)
        .header("User-Agent", "dmm-ingestor/0.1")
        .send()
        .await
        .map_err(|_| hashlist::ParseError::DecompressionFailed)?
        .text()
        .await
        .map_err(|_| hashlist::ParseError::DecompressionFailed)?;
    hashlist::parse_hashlist_html(&html)
}

pub async fn run_pipeline2_extract(state: AppState) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");

    let cfg = &state.db.config.extract;
    let batch_size = cfg.batch_size;
    let concurrency = cfg.concurrency;
    let semaphore = Arc::new(Semaphore::new(concurrency));

    log!(state.logs, "[P2-EXTRACT] Started (batch: {batch_size}, concurrency: {concurrency})");

    loop {
        if state.metrics.controls.is_paused("extract") {
            state.queues.extract.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        state.queues.extract.store(true, Ordering::Relaxed);

        let names = match state.db.get_pending_hashlists(batch_size).await {
            Ok(n) => n,
            Err(e) => {
                log!(state.logs, "[P2-EXTRACT] Query error: {e}");
                state.queues.extract.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        };

        if names.is_empty() {
            state.queues.extract.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            continue;
        }

        let mut handles = Vec::with_capacity(names.len());

        for name in names {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let client = client.clone();
            let state = state.clone();

            handles.push(tokio::spawn(async move {
                // Touch before processing
                let _ = state.db.touch_hashlist(&name).await;

                match fetch_and_parse_hashlist(&client, &name).await {
                    Ok(hl) => {
                        let count = hl.list.len();
                        for entry in &hl.list {
                            let _ = state.db.upsert_torrent(&entry.hash, &entry.filename, entry.size as i64).await;
                        }
                        let _ = state.db.complete_hashlist(&name).await;
                        state.metrics.bump("hashlists_extracted", 1);
                        state.metrics.bump("torrents_ingested", count as u64);
                        log!(state.logs, "[OK] {name}: {count} torrents");
                    }
                    Err(e) => {
                        log!(state.logs, "[P2-EXTRACT] {name}: {e}");
                        // last_processed already touched, will retry later
                    }
                }
                drop(permit);
            }));
        }

        for h in handles {
            let _ = h.await;
        }

        state.queues.extract.store(false, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

// =========================================================================
// Pipeline 3: torrents → parsed_torrents (continuous)
// =========================================================================

pub async fn run_pipeline3_parse(state: AppState) {
    let batch_size = state.db.config.parse.batch_size;
    log!(state.logs, "[P3-PARSE] Started (batch: {batch_size})");

    loop {
        if state.metrics.controls.is_paused("parse") {
            state.queues.parse.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        state.queues.parse.store(true, Ordering::Relaxed);

        let torrents = match state.db.get_pending_torrents(batch_size).await {
            Ok(t) => t,
            Err(e) => {
                log!(state.logs, "[P3-PARSE] Query error: {e}");
                state.queues.parse.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        };

        if torrents.is_empty() {
            state.queues.parse.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            continue;
        }

        let mut ok = 0u64;
        for (hash, filename) in &torrents {
            let _ = state.db.touch_torrent(hash).await;

            if let Some(meta) = title_parser::parse(filename) {
                if let Some(ref title) = meta.title {
                    if state.db.upsert_parsed_torrent(
                        hash,
                        title,
                        meta.year.map(|y| y as i32),
                        meta.season.map(|s| s as i32),
                        meta.episode.map(|e| e as i32),
                    ).await.is_ok() {
                        ok += 1;
                    }
                }
            }
            let _ = state.db.complete_torrent(hash).await;
        }

        state.metrics.bump("torrents_parsed", ok);
        log!(state.logs, "[P3-PARSE] Parsed {ok}/{} torrents", torrents.len());

        state.queues.parse.store(false, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tree_response(entries: Vec<(&str, &str)>, truncated: bool) -> GitTreeResponse {
        GitTreeResponse {
            tree: entries
                .into_iter()
                .map(|(path, typ)| GitTreeEntry {
                    path: path.to_string(),
                    entry_type: typ.to_string(),
                })
                .collect(),
            truncated,
        }
    }

    #[test]
    fn test_filter_hashlist_names_excludes_index() {
        let resp = make_tree_response(vec![("index.html", "blob"), ("abc.html", "blob")], false);
        let names = filter_hashlist_names(&resp);
        assert_eq!(names, vec!["abc.html"]);
    }

    #[test]
    fn test_filter_hashlist_names_excludes_non_html() {
        let resp = make_tree_response(vec![("readme.md", "blob"), ("abc.html", "blob")], false);
        let names = filter_hashlist_names(&resp);
        assert_eq!(names, vec!["abc.html"]);
    }

    #[test]
    fn test_filter_hashlist_names_excludes_directories() {
        let resp = make_tree_response(vec![("subdir", "tree"), ("abc.html", "blob")], false);
        let names = filter_hashlist_names(&resp);
        assert_eq!(names, vec!["abc.html"]);
    }
}
