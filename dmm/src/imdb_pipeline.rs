use crate::imdb_resolver::ImdbResolver;
use crate::ingestor::AppState;
use crate::log;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Semaphore;

const BATCH_SIZE: i64 = 2000;
const CONCURRENCY: usize = 30;

/// Run the IMDb resolution worker independently.
/// Continuously picks up unmapped parsed_metadata entries, resolves them
/// via Cinemeta/IMDb suggest APIs with concurrent lookups, and tracks failures.
pub async fn run_imdb_worker(state: AppState, resolver: ImdbResolver) {
    log!(
        state.logs,
        "[IMDB-Q] Started (batch: {BATCH_SIZE}, concurrency: {CONCURRENCY})"
    );

    loop {
        if state.metrics.controls.is_paused("imdb") {
            state.queues.imdb.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        state.queues.imdb.store(true, Ordering::Relaxed);

        let entries = match state.db.get_unmapped_entries_for_resolution(BATCH_SIZE).await {
            Ok(e) => e,
            Err(e) => {
                log!(state.logs, "[IMDB-Q] Query error: {e}");
                state.queues.imdb.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
        };

        if entries.is_empty() {
            state.queues.imdb.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            continue;
        }

        log!(
            state.logs,
            "[IMDB-Q] Processing {} entries",
            entries.len()
        );

        // Group hashes by (title_lower, year, type_hint) to deduplicate API calls.
        // Type hint is derived from parsed metadata: if season or episode is present → "series".
        let mut groups: HashMap<(String, Option<i32>, Option<String>), Vec<String>> =
            HashMap::new();
        for entry in &entries {
            let type_hint = if entry.season.is_some() || entry.episode.is_some() {
                Some("series".to_string())
            } else {
                None
            };
            let key = (entry.title.to_lowercase(), entry.year, type_hint);
            groups
                .entry(key)
                .or_default()
                .push(entry.hash.clone());
        }

        log!(
            state.logs,
            "[IMDB-Q] {} unique titles to look up",
            groups.len()
        );

        // Resolve concurrently using a semaphore
        let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
        let mut handles = Vec::with_capacity(groups.len());

        for ((title, year, type_hint), hashes) in groups {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let resolver = resolver.clone();
            let state = state.clone();
            let title_clone = title.clone();

            let handle = tokio::spawn(async move {
                let result = resolver
                    .resolve(&title_clone, year, type_hint.as_deref())
                    .await;
                let (resolved, missed) = match result {
                    Some(imdb_result) => {
                        // Type precedence: parsed metadata hint overrides API-detected type.
                        // If metadata says it has season/episode, it's a series — period.
                        let final_type = type_hint
                            .as_deref()
                            .unwrap_or(&imdb_result.content_type);
                        for hash in &hashes {
                            if let Err(e) = state
                                .db
                                .upsert_imdb_mapping(hash, &imdb_result.imdb_id, final_type)
                                .await
                            {
                                log!(
                                    state.logs,
                                    "[IMDB-Q] Failed to insert mapping for {}: {e}",
                                    &hash[..8.min(hash.len())]
                                );
                            }
                        }
                        (hashes.len() as u64, 0u64)
                    }
                    None => {
                        // Mark all hashes in this group as failed
                        for hash in &hashes {
                            if let Err(e) =
                                state.db.mark_imdb_failure(hash, &title_clone, year).await
                            {
                                log!(
                                    state.logs,
                                    "[IMDB-Q] Failed to mark failure for {}: {e}",
                                    &hash[..8.min(hash.len())]
                                );
                            }
                        }
                        (0u64, hashes.len() as u64)
                    }
                };
                drop(permit);
                (resolved, missed)
            });

            handles.push(handle);
        }

        // Collect results
        let mut total_resolved = 0u64;
        let mut total_missed = 0u64;
        for handle in handles {
            match handle.await {
                Ok((r, m)) => {
                    total_resolved += r;
                    total_missed += m;
                }
                Err(e) => {
                    log!(state.logs, "[IMDB-Q] Task panicked: {e}");
                }
            }
        }

        state.metrics.bump("imdb_resolved", total_resolved);
        state.metrics.bump("imdb_failed", total_missed);

        log!(
            state.logs,
            "[IMDB-Q] Batch done: {total_resolved} resolved, {total_missed} failed (cache: {})",
            resolver.cache_len().await
        );

        state.queues.imdb.store(false, Ordering::Relaxed);

        // Brief pause between batches to avoid API hammering
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grouping_logic() {
        // Simulate the grouping logic
        let entries = vec![
            ("hash1", "The Matrix", Some(1999i32)),
            ("hash2", "the matrix", Some(1999)),
            ("hash3", "Inception", Some(2010)),
            ("hash4", "The Matrix", None),
        ];

        let mut groups: HashMap<(String, Option<i32>), Vec<String>> = HashMap::new();
        for (hash, title, year) in entries {
            let key = (title.to_lowercase(), year);
            groups.entry(key).or_default().push(hash.to_string());
        }

        // "The Matrix" + 1999 should have 2 hashes (case-insensitive grouping)
        assert_eq!(
            groups
                .get(&("the matrix".to_string(), Some(1999)))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            groups
                .get(&("inception".to_string(), Some(2010)))
                .unwrap()
                .len(),
            1
        );
        // "The Matrix" without year is a separate group
        assert_eq!(
            groups
                .get(&("the matrix".to_string(), None))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(groups.len(), 3);
    }
}
