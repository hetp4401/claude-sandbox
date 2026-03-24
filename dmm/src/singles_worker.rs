use crate::ingestor::AppState;
use crate::log;
use std::sync::atomic::Ordering;

/// Pipeline 5: imdb_mappings (non-season-pack) → streams
pub async fn run_pipeline5_singles(state: AppState) {
    let batch_size = state.db.config.singles.batch_size;
    log!(state.logs, "[P5-SINGLES] Started (batch: {batch_size})");

    loop {
        if state.metrics.controls.is_paused("singles") {
            state.queues.singles.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            continue;
        }
        state.queues.singles.store(true, Ordering::Relaxed);

        let candidates = match state.db.get_pending_singles(batch_size).await {
            Ok(c) => c,
            Err(e) => {
                log!(state.logs, "[P5-SINGLES] Query error: {e}");
                state.queues.singles.store(false, Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }
        };

        if candidates.is_empty() {
            state.queues.singles.store(false, Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            continue;
        }

        let mut ok_count = 0u64;
        for (hash, imdb_id, content_type, size_bytes, season, episode) in &candidates {
            let _ = state.db.touch_imdb_mapping(hash).await;

            if state.db.upsert_stream(hash, imdb_id, content_type, *size_bytes, None, *season, *episode).await.is_ok() {
                let _ = state.db.complete_imdb_mapping(hash).await;
                ok_count += 1;
            }
        }

        state.metrics.bump("singles_inserted", ok_count);
        log!(state.logs, "[P5-SINGLES] Batch done: {ok_count}/{}", candidates.len());

        state.queues.singles.store(false, Ordering::Relaxed);
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
