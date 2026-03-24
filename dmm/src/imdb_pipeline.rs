use crate::imdb_resolver::ImdbResolver;
use crate::ingestor::AppState;
use crate::log;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Pipeline 4: parsed_torrents → imdb_mappings
pub async fn run_pipeline4_imdb(state: AppState, resolver: ImdbResolver) {
    let cfg = &state.db.config.imdb;
    let batch_size = cfg.batch_size;
    let concurrency = cfg.concurrency;
    log!(state.logs, "[P4-IMDB] Started (batch: {batch_size}, concurrency: {concurrency})");

    loop {
        if state.metrics.controls.is_paused("imdb") {
            state.queues.imdb.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        state.queues.imdb.store(true, Ordering::Relaxed);

        let entries = match state.db.get_pending_parsed_torrents(batch_size).await {
            Ok(e) => e,
            Err(e) => {
                log!(state.logs, "[P4-IMDB] Query error: {e}");
                state.queues.imdb.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        };

        if entries.is_empty() {
            state.queues.imdb.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            continue;
        }

        log!(state.logs, "[P4-IMDB] Processing {} entries", entries.len());

        // Touch all before processing
        for entry in &entries {
            let _ = state.db.touch_parsed_torrent(&entry.hash).await;
        }

        // Group by (title, year, type_hint) to deduplicate API calls
        let mut groups: HashMap<(String, Option<i32>, Option<String>), Vec<String>> = HashMap::new();
        for entry in &entries {
            let type_hint = if entry.season.is_some() || entry.episode.is_some() {
                Some("series".to_string())
            } else {
                None
            };
            let key = (entry.title.to_lowercase(), entry.year, type_hint);
            groups.entry(key).or_default().push(entry.hash.clone());
        }

        log!(state.logs, "[P4-IMDB] {} unique titles", groups.len());

        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut handles = Vec::with_capacity(groups.len());

        for ((title, year, type_hint), hashes) in groups {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let resolver = resolver.clone();
            let state = state.clone();
            let title_clone = title.clone();

            handles.push(tokio::spawn(async move {
                let result = resolver.resolve(&title_clone, year, type_hint.as_deref()).await;
                let (resolved, missed) = match result {
                    Some(imdb_result) => {
                        let final_type = type_hint.as_deref().unwrap_or(&imdb_result.content_type);
                        for hash in &hashes {
                            let _ = state.db.upsert_imdb_mapping(hash, &imdb_result.imdb_id, final_type).await;
                            let _ = state.db.complete_parsed_torrent(hash).await;
                        }
                        (hashes.len() as u64, 0u64)
                    }
                    None => {
                        for hash in &hashes {
                            let _ = state.db.mark_imdb_failure(hash).await;
                        }
                        (0u64, hashes.len() as u64)
                    }
                };
                drop(permit);
                (resolved, missed)
            }));
        }

        let mut total_resolved = 0u64;
        let mut total_missed = 0u64;
        for handle in handles {
            match handle.await {
                Ok((r, m)) => {
                    total_resolved += r;
                    total_missed += m;
                }
                Err(e) => log!(state.logs, "[P4-IMDB] Task panicked: {e}"),
            }
        }

        state.metrics.bump("imdb_resolved", total_resolved);
        state.metrics.bump("imdb_failed", total_missed);
        let cache_size = resolver.cache_len().await;
        state.metrics.bump("imdb_cache_hits", cache_size as u64);
        log!(
            state.logs,
            "[P4-IMDB] Batch done: {total_resolved} resolved, {total_missed} failed (cache: {cache_size})"
        );

        state.queues.imdb.store(false, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
