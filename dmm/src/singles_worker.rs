use crate::ingestor::AppState;
use crate::log;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::Semaphore;

const BATCH_SIZE: i64 = 5000;
const DB_CONCURRENCY: usize = 50;

/// Run the singles ingestor worker.
/// Picks up movies and single episodes (series with season+episode) from
/// parsed_metadata + imdb_mappings and writes them to the `streams` table.
pub async fn run_singles_worker(state: AppState) {
    log!(state.logs, "[SINGLES-Q] Started (batch: {BATCH_SIZE})");

    loop {
        if state.metrics.controls.is_paused("singles") {
            state.queues.singles.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        state.queues.singles.store(true, Ordering::Relaxed);

        let candidates = match state.db.get_singles_candidates(BATCH_SIZE).await {
            Ok(c) => c,
            Err(e) => {
                log!(state.logs, "[SINGLES-Q] Query error: {e}");
                state.queues.singles.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
        };

        if candidates.is_empty() {
            state.queues.singles.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            continue;
        }

        log!(
            state.logs,
            "[SINGLES-Q] Processing {} candidates",
            candidates.len()
        );

        let semaphore = Arc::new(Semaphore::new(DB_CONCURRENCY));
        let mut handles = Vec::with_capacity(candidates.len());

        for c in candidates {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let state = state.clone();
            handles.push(tokio::spawn(async move {
                let result = state
                    .db
                    .upsert_stream(
                        &c.hash,
                        &c.imdb_id,
                        &c.content_type,
                        c.size_bytes,
                        None,
                        c.season,
                        c.episode,
                    )
                    .await;
                let ok = match result {
                    Ok(()) => state.db.mark_stream_processed(&c.hash).await.is_ok(),
                    Err(_) => false,
                };
                drop(permit);
                ok
            }));
        }

        let mut ok_count = 0u64;
        let mut err_count = 0u64;
        for handle in handles {
            match handle.await {
                Ok(true) => ok_count += 1,
                _ => err_count += 1,
            }
        }

        state.metrics.bump("singles_inserted", ok_count);

        log!(
            state.logs,
            "[SINGLES-Q] Batch done: {ok_count} inserted, {err_count} errors"
        );

        state.queues.singles.store(false, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Determine the stream type for a singles candidate.
pub fn classify_single(content_type: &str, season: Option<i32>, episode: Option<i32>) -> Option<&'static str> {
    match content_type {
        "movie" => Some("movie"),
        "series" if season.is_some() && episode.is_some() => Some("series"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_single_movie() {
        assert_eq!(classify_single("movie", None, None), Some("movie"));
        assert_eq!(classify_single("movie", Some(1), Some(1)), Some("movie"));
    }

    #[test]
    fn test_classify_single_episode() {
        assert_eq!(classify_single("series", Some(1), Some(5)), Some("series"));
        assert_eq!(classify_single("series", Some(3), Some(12)), Some("series"));
    }

    #[test]
    fn test_classify_single_season_pack_rejected() {
        // Season pack (no episode) should NOT be classified as a single
        assert_eq!(classify_single("series", Some(1), None), None);
    }

    #[test]
    fn test_classify_single_series_no_metadata() {
        assert_eq!(classify_single("series", None, None), None);
    }

    #[test]
    fn test_classify_single_unknown_type() {
        assert_eq!(classify_single("other", None, None), None);
    }
}
