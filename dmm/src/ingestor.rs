use crate::db::Db;
use crate::hashlist::{self, Hashlist, ParseError};
use crate::log;
use crate::logs::LogBuffer;
use crate::title_parser;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Status of a processed hashlist.
#[derive(Debug, Clone, Serialize)]
pub struct HashlistStatus {
    pub name: String,
    pub record_count: usize,
    pub status: String,
}

/// Shared application state backed by PostgreSQL.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub logs: LogBuffer,
}

impl AppState {
    pub fn new(db: Db, logs: LogBuffer) -> Self {
        Self { db, logs }
    }

    pub async fn is_processed(&self, name: &str) -> bool {
        self.db.is_processed(name).await.unwrap_or(false)
    }

    pub async fn mark_processed(&self, name: &str) {
        if let Err(e) = self.db.mark_processed(name).await {
            log!(self.logs, "[ERROR] Failed to mark {name} as processed: {e}");
        }
    }

    /// Ingest a hashlist result into both tables:
    /// 1. `torrents` table: raw hash, filename, size_bytes
    /// 2. `parsed_metadata` table: parsed title, year, season, episode
    pub async fn add_hashlist_result(&self, name: &str, result: Result<Hashlist, ParseError>) {
        match result {
            Ok(hl) => {
                let count = hl.list.len();

                for entry in &hl.list {
                    // Phase 1: upsert into torrents table
                    if let Err(e) = self
                        .db
                        .upsert_torrent(&entry.hash, &entry.filename, entry.size as i64)
                        .await
                    {
                        log!(
                            self.logs,
                            "[ERROR] Failed to upsert torrent {} for {name}: {e}",
                            &entry.hash[..8.min(entry.hash.len())]
                        );
                        continue;
                    }

                    // Phase 2: parse filename and upsert into parsed_metadata
                    if let Some(meta) = title_parser::parse(&entry.filename) {
                        if let Some(ref title) = meta.title {
                            if let Err(e) = self
                                .db
                                .upsert_parsed_metadata(
                                    &entry.hash,
                                    title,
                                    meta.year.map(|y| y as i32),
                                    meta.season.map(|s| s as i32),
                                    meta.episode.map(|e| e as i32),
                                )
                                .await
                            {
                                log!(
                                    self.logs,
                                    "[ERROR] Failed to upsert parsed metadata {} for {name}: {e}",
                                    &entry.hash[..8.min(entry.hash.len())]
                                );
                            }
                        }
                    }
                }

                let status = HashlistStatus {
                    name: name.to_string(),
                    record_count: count,
                    status: "done".into(),
                };
                if let Err(e) = self.db.insert_hashlist(&status).await {
                    log!(
                        self.logs,
                        "[ERROR] Failed to insert hashlist status for {name}: {e}"
                    );
                }
                self.mark_processed(name).await;
                log!(self.logs, "[OK] {name}: {count} records ingested");
            }
            Err(e) => {
                let status = HashlistStatus {
                    name: name.to_string(),
                    record_count: 0,
                    status: format!("error: {e}"),
                };
                if let Err(db_err) = self.db.insert_hashlist(&status).await {
                    log!(
                        self.logs,
                        "[ERROR] Failed to insert error status for {name}: {db_err}"
                    );
                }
                log!(self.logs, "[ERROR] {name}: {e}");
            }
        }
    }
}

/// A file entry from the GitHub Trees API.
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

/// Filter a tree response down to hashlist HTML filenames.
pub fn filter_hashlist_names(tree_resp: &GitTreeResponse) -> Vec<String> {
    tree_resp
        .tree
        .iter()
        .filter(|e| e.entry_type == "blob" && e.path.ends_with(".html") && e.path != "index.html")
        .map(|e| e.path.clone())
        .collect()
}

/// Fetch the list of .html hashlist filenames from the GitHub repo.
async fn fetch_hashlist_names(client: &reqwest::Client) -> Result<Vec<String>, String> {
    let url =
        "https://api.github.com/repos/debridmediamanager/hashlists/git/trees/main?recursive=1";

    let resp = client
        .get(url)
        .header("User-Agent", "dmm-ingestor/0.1")
        .header("Accept", "application/vnd.github.v3+json")
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

    if tree_resp.truncated {
        println!("[WARN] GitHub tree response was truncated, some files may be missing");
    }

    Ok(filter_hashlist_names(&tree_resp))
}

/// Fetch and parse a single hashlist file from GitHub.
async fn fetch_and_parse_hashlist(
    client: &reqwest::Client,
    name: &str,
) -> Result<Hashlist, ParseError> {
    let url = format!(
        "https://raw.githubusercontent.com/debridmediamanager/hashlists/main/{name}"
    );

    let html = client
        .get(&url)
        .header("User-Agent", "dmm-ingestor/0.1")
        .send()
        .await
        .map_err(|_| ParseError::DecompressionFailed)?
        .text()
        .await
        .map_err(|_| ParseError::DecompressionFailed)?;

    hashlist::parse_hashlist_html(&html)
}

/// Run the ingestion loop: poll GitHub, process new hashlists concurrently,
/// then run IMDb resolution pipeline.
pub async fn run_ingest_loop(
    state: AppState,
    resolver: crate::imdb_resolver::ImdbResolver,
    poll_interval: std::time::Duration,
) {
    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    log!(
        state.logs,
        "[INGESTOR] Workers: {num_workers} (auto-detected CPU cores)"
    );
    log!(
        state.logs,
        "[INGESTOR] Poll interval: {}s",
        poll_interval.as_secs()
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");

    let semaphore = Arc::new(Semaphore::new(num_workers));

    loop {
        log!(state.logs, "[INGESTOR] Polling GitHub for new hashlists...");

        match fetch_hashlist_names(&client).await {
            Ok(names) => {
                let total = names.len();
                let mut new_names = Vec::new();
                for name in names {
                    if !state.is_processed(&name).await {
                        new_names.push(name);
                    }
                }
                let new_count = new_names.len();
                log!(
                    state.logs,
                    "[INGESTOR] Found {total} hashlists total, {new_count} new to process"
                );

                if new_count > 0 {
                    let mut handles = Vec::with_capacity(new_count);

                    for name in new_names {
                        let permit = semaphore.clone().acquire_owned().await.unwrap();
                        let client = client.clone();
                        let state = state.clone();

                        let handle = tokio::spawn(async move {
                            let result = fetch_and_parse_hashlist(&client, &name).await;
                            state.add_hashlist_result(&name, result).await;
                            drop(permit);
                        });

                        handles.push(handle);
                    }

                    let mut ok_count = 0;
                    let mut err_count = 0;
                    for handle in handles {
                        match handle.await {
                            Ok(()) => ok_count += 1,
                            Err(e) => {
                                err_count += 1;
                                log!(state.logs, "[ERROR] Task panicked: {e}");
                            }
                        }
                    }
                    log!(
                        state.logs,
                        "[INGESTOR] Batch complete: {ok_count} succeeded, {err_count} failed"
                    );
                }
            }
            Err(e) => {
                log!(state.logs, "[INGESTOR] Failed to poll GitHub: {e}");
            }
        }

        // Run IMDb resolution pipeline
        crate::imdb_pipeline::run_imdb_pipeline(&state, &resolver).await;

        let counts = state.db.counts().await.unwrap_or(crate::db::RecordCounts {
            total_torrents: 0,
            total_parsed: 0,
            total_imdb: 0,
        });
        let hl_count = state.db.count_hashlists().await.unwrap_or(0);
        log!(
            state.logs,
            "[INGESTOR] State: {} torrents, {} parsed, {} IMDb, {hl_count} hashlists",
            counts.total_torrents,
            counts.total_parsed,
            counts.total_imdb
        );
        log!(
            state.logs,
            "[INGESTOR] Sleeping {}s until next poll...",
            poll_interval.as_secs()
        );
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // === GitHub Tree API response parsing ===

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
    fn test_parse_github_tree_response_json() {
        let json = r#"{
            "sha": "abc123",
            "url": "https://api.github.com/repos/debridmediamanager/hashlists/git/trees/main",
            "tree": [
                {"path": "index.html", "mode": "100644", "type": "blob", "sha": "aaa", "size": 100, "url": "..."},
                {"path": "00011863-7c82-4d3c-846a-deeb5ac0d6be.html", "mode": "100644", "type": "blob", "sha": "bbb", "size": 5000, "url": "..."},
                {"path": "2f16a13d-7dd7-414d-a9c5-aa30a32fe9de.html", "mode": "100644", "type": "blob", "sha": "ccc", "size": 3000, "url": "..."},
                {"path": ".gitattributes", "mode": "100644", "type": "blob", "sha": "ddd", "size": 50, "url": "..."}
            ],
            "truncated": false
        }"#;

        let resp: GitTreeResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.tree.len(), 4);
        assert!(!resp.truncated);
    }

    #[test]
    fn test_filter_hashlist_names_excludes_index() {
        let resp = make_tree_response(
            vec![
                ("index.html", "blob"),
                ("abc.html", "blob"),
                ("def.html", "blob"),
            ],
            false,
        );
        let names = filter_hashlist_names(&resp);
        assert_eq!(names, vec!["abc.html", "def.html"]);
    }

    #[test]
    fn test_filter_hashlist_names_excludes_non_html() {
        let resp = make_tree_response(
            vec![
                ("readme.md", "blob"),
                (".gitattributes", "blob"),
                ("abc.html", "blob"),
            ],
            false,
        );
        let names = filter_hashlist_names(&resp);
        assert_eq!(names, vec!["abc.html"]);
    }

    #[test]
    fn test_filter_hashlist_names_excludes_directories() {
        let resp = make_tree_response(vec![("subdir", "tree"), ("abc.html", "blob")], false);
        let names = filter_hashlist_names(&resp);
        assert_eq!(names, vec!["abc.html"]);
    }

    #[test]
    fn test_filter_hashlist_names_empty_tree() {
        let resp = make_tree_response(vec![], false);
        let names = filter_hashlist_names(&resp);
        assert!(names.is_empty());
    }

    #[test]
    fn test_filter_hashlist_names_uuid_pattern() {
        let resp = make_tree_response(
            vec![
                ("00011863-7c82-4d3c-846a-deeb5ac0d6be.html", "blob"),
                ("2f16a13d-7dd7-414d-a9c5-aa30a32fe9de.html", "blob"),
                ("78cbef20-0923-4d11-9d1c-95ffab29664e.html", "blob"),
            ],
            false,
        );
        let names = filter_hashlist_names(&resp);
        assert_eq!(names.len(), 3);
        assert!(names.iter().all(|n| n.ends_with(".html")));
    }

    #[test]
    fn test_truncated_tree_response() {
        let resp = make_tree_response(vec![("a.html", "blob")], true);
        assert!(resp.truncated);
        assert_eq!(filter_hashlist_names(&resp).len(), 1);
    }
}
