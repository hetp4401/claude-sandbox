use crate::db::Db;
use crate::hashlist::{self, Hashlist, ParseError};
use crate::title_parser;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// A processed magnet record.
#[derive(Debug, Clone, Serialize)]
pub struct MagnetRecord {
    pub filename: String,
    pub hash: String,
    pub magnet_uri: String,
    pub size_bytes: u64,
    pub source_hashlist: String,
    pub processed_at: String,
    /// Parsed content type: "movie", "episode", or "season"
    pub content_type: Option<String>,
    /// Parsed title from the filename
    pub title: Option<String>,
    /// Season number (if episode or season pack)
    pub season: Option<u32>,
    /// Episode number (if episode)
    pub episode: Option<u32>,
    /// File index within the torrent (set by DHT for season pack expansion)
    pub file_index: Option<u32>,
    /// IMDb tag — null until metadata enrichment processes it
    pub imdb_tag: Option<String>,
}

/// Status of a processed hashlist.
#[derive(Debug, Clone, Serialize)]
pub struct HashlistStatus {
    pub name: String,
    pub record_count: usize,
    pub status: String,
    pub processed_at: String,
}

/// Shared application state backed by SQLite.
#[derive(Clone)]
pub struct AppState {
    pub db: Db,
}

impl AppState {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub fn is_processed(&self, name: &str) -> bool {
        self.db.is_processed(name).unwrap_or(false)
    }

    pub fn mark_processed(&self, name: &str) {
        if let Err(e) = self.db.mark_processed(name) {
            println!("[ERROR] Failed to mark {name} as processed: {e}");
        }
    }

    pub fn add_hashlist_result(
        &self,
        name: &str,
        result: Result<Hashlist, ParseError>,
    ) {
        let now = Utc::now().to_rfc3339();
        match result {
            Ok(hl) => {
                let count = hl.list.len();
                let records: Vec<MagnetRecord> = hl
                    .list
                    .iter()
                    .map(|entry| {
                        let meta = title_parser::parse(&entry.filename);
                        MagnetRecord {
                            filename: entry.filename.clone(),
                            hash: entry.hash.clone(),
                            magnet_uri: format!("magnet:?xt=urn:btih:{}", entry.hash),
                            size_bytes: entry.size,
                            source_hashlist: name.to_string(),
                            processed_at: Utc::now().to_rfc3339(),
                            content_type: meta.as_ref().map(|m| m.content_type.to_string()),
                            title: meta.as_ref().and_then(|m| m.title.clone()),
                            season: meta.as_ref().and_then(|m| m.season),
                            episode: meta.as_ref().and_then(|m| m.episode),
                            file_index: None,
                            imdb_tag: None,
                        }
                    })
                    .collect();

                if let Err(e) = self.db.insert_records(&records) {
                    println!("[ERROR] Failed to insert records for {name}: {e}");
                    return;
                }

                let status = HashlistStatus {
                    name: name.to_string(),
                    record_count: count,
                    status: "done".into(),
                    processed_at: now,
                };
                if let Err(e) = self.db.insert_hashlist(&status) {
                    println!("[ERROR] Failed to insert hashlist status for {name}: {e}");
                }
                self.mark_processed(name);
                println!("[OK] {name}: {count} records ingested");
            }
            Err(e) => {
                let status = HashlistStatus {
                    name: name.to_string(),
                    record_count: 0,
                    status: format!("error: {e}"),
                    processed_at: now,
                };
                if let Err(db_err) = self.db.insert_hashlist(&status) {
                    println!("[ERROR] Failed to insert error status for {name}: {db_err}");
                }
                println!("[ERROR] {name}: {e}");
            }
        }
    }
}

/// A file entry from the GitHub Trees API.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GitTreeEntry {
    pub path: String,
    #[serde(rename = "type")]
    pub entry_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    let url = "https://api.github.com/repos/debridmediamanager/hashlists/git/trees/main?recursive=1";

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

/// Run the ingestion loop: poll GitHub, process new hashlists concurrently.
pub async fn run_ingest_loop(state: AppState, poll_interval: std::time::Duration) {
    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    println!("[INGESTOR] Workers: {num_workers} (auto-detected CPU cores)");
    println!(
        "[INGESTOR] Poll interval: {}s",
        poll_interval.as_secs()
    );

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .expect("Failed to build HTTP client");

    let semaphore = Arc::new(Semaphore::new(num_workers));

    loop {
        println!("\n[INGESTOR] Polling GitHub for new hashlists...");

        match fetch_hashlist_names(&client).await {
            Ok(names) => {
                let total = names.len();
                let new_names: Vec<String> = names
                    .into_iter()
                    .filter(|name| !state.is_processed(name))
                    .collect();
                let new_count = new_names.len();
                println!(
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
                            // DB operations are sync but fast; run on blocking pool
                            let state_clone = state.clone();
                            let name_clone = name.clone();
                            tokio::task::spawn_blocking(move || {
                                state_clone.add_hashlist_result(&name_clone, result);
                            })
                            .await
                            .ok();
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
                                println!("[ERROR] Task panicked: {e}");
                            }
                        }
                    }
                    println!(
                        "[INGESTOR] Batch complete: {ok_count} succeeded, {err_count} failed"
                    );
                }
            }
            Err(e) => {
                println!("[INGESTOR] Failed to poll GitHub: {e}");
            }
        }

        // Run the metadata enrichment pipeline (expand season packs via DHT)
        crate::pipeline::run_enrichment_pipeline(&state).await;

        let counts = state.db.count_records().unwrap_or(crate::db::RecordCounts {
            total: 0,
            movies: 0,
            episodes: 0,
            seasons: 0,
        });
        let hl_count = state.db.count_hashlists().unwrap_or(0);
        println!(
            "[INGESTOR] State: {} total records, {hl_count} hashlists processed",
            counts.total
        );
        println!(
            "[INGESTOR] Sleeping {}s until next poll...",
            poll_interval.as_secs()
        );
        tokio::time::sleep(poll_interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hashlist::Hashlist;

    fn test_state() -> AppState {
        AppState::new(Db::open_in_memory().unwrap())
    }

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
        let resp = make_tree_response(
            vec![("subdir", "tree"), ("abc.html", "blob")],
            false,
        );
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

    // === AppState tests ===

    #[test]
    fn test_app_state_new_is_empty() {
        let state = test_state();
        assert_eq!(state.db.get_all_records().unwrap().len(), 0);
        assert_eq!(state.db.get_all_hashlists().unwrap().len(), 0);
    }

    #[test]
    fn test_mark_and_check_processed() {
        let state = test_state();
        assert!(!state.is_processed("test.html"));
        state.mark_processed("test.html");
        assert!(state.is_processed("test.html"));
        assert!(!state.is_processed("other.html"));
    }

    #[test]
    fn test_add_hashlist_result_success() {
        let state = test_state();
        let hashlist = Hashlist {
            title: Some("Test".into()),
            list: vec![
                crate::hashlist::HashlistEntry {
                    filename: "movie.mkv".into(),
                    hash: "aa".repeat(20),
                    size: 1000,
                },
                crate::hashlist::HashlistEntry {
                    filename: "show.mkv".into(),
                    hash: "bb".repeat(20),
                    size: 2000,
                },
            ],
        };

        state.add_hashlist_result("test.html", Ok(hashlist));

        let records = state.db.get_all_records().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.iter().any(|r| r.filename == "movie.mkv"));
        assert!(records.iter().any(|r| r.filename == "show.mkv"));
        assert!(records[0].magnet_uri.starts_with("magnet:?xt=urn:btih:"));

        let hashlists = state.db.get_all_hashlists().unwrap();
        assert_eq!(hashlists.len(), 1);
        assert_eq!(hashlists[0].status, "done");
        assert_eq!(hashlists[0].record_count, 2);

        assert!(state.is_processed("test.html"));
    }

    #[test]
    fn test_add_hashlist_result_error() {
        let state = test_state();
        state.add_hashlist_result("bad.html", Err(ParseError::DecompressionFailed));

        assert!(state.db.get_all_records().unwrap().is_empty());

        let hashlists = state.db.get_all_hashlists().unwrap();
        assert_eq!(hashlists.len(), 1);
        assert!(hashlists[0].status.starts_with("error:"));
        assert_eq!(hashlists[0].record_count, 0);

        // Errors are NOT marked as processed (so they can be retried)
        assert!(!state.is_processed("bad.html"));
    }

    #[test]
    fn test_add_hashlist_result_concurrent() {
        let state = test_state();

        // Simulate concurrent inserts (serial here since SQLite is single-writer)
        for i in 0..10 {
            let hashlist = Hashlist {
                title: None,
                list: vec![crate::hashlist::HashlistEntry {
                    filename: format!("file_{i}.mkv"),
                    hash: format!("{:0>40}", i),
                    size: (i as u64) * 1000,
                }],
            };
            state.add_hashlist_result(&format!("hl_{i}.html"), Ok(hashlist));
        }

        assert_eq!(state.db.get_all_records().unwrap().len(), 10);

        let hashlists = state.db.get_all_hashlists().unwrap();
        assert_eq!(hashlists.len(), 10);
        assert!(hashlists.iter().all(|h| h.status == "done"));
    }

    #[test]
    fn test_already_processed_hashlists_are_skipped() {
        let state = test_state();
        state.mark_processed("old.html");

        let names = vec![
            "old.html".to_string(),
            "new1.html".to_string(),
            "new2.html".to_string(),
        ];

        let new_names: Vec<_> = names
            .iter()
            .filter(|name| !state.is_processed(name))
            .cloned()
            .collect();

        assert_eq!(new_names, vec!["new1.html", "new2.html"]);
    }

    // === Magnet URI construction ===

    #[test]
    fn test_magnet_uri_format() {
        let state = test_state();
        let hashlist = Hashlist {
            title: None,
            list: vec![crate::hashlist::HashlistEntry {
                filename: "test.mkv".into(),
                hash: "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c".into(),
                size: 100,
            }],
        };

        state.add_hashlist_result("t.html", Ok(hashlist));

        let records = state.db.get_all_records().unwrap();
        assert_eq!(
            records[0].magnet_uri,
            "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c"
        );
    }

    // === Semaphore test (independent of DB) ===

    #[tokio::test]
    async fn test_semaphore_bounded_concurrency() {
        let semaphore = Arc::new(Semaphore::new(2));
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let sem = semaphore.clone();
            let active = active.clone();
            let max_active = max_active.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let current = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                max_active.fetch_max(current, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let max = max_active.load(std::sync::atomic::Ordering::SeqCst);
        assert!(max <= 2, "Max concurrent tasks was {max}, expected <= 2");
    }
}
