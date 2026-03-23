use crate::dht::TorrentFile;
use crate::ingestor::AppState;
use crate::log;
use crate::pipeline;
use crate::rqbit_client::RqbitResolver;
use crate::title_parser;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

const BATCH_SIZE: i64 = 50;
const CONCURRENCY: usize = 10;
const RQBIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Run the DHT lookup worker.
/// Picks up hashes from the dht_queue (failed cache+RD resolution),
/// uses librqbit to connect to the swarm and fetch metadata.
pub async fn run_dht_worker(state: AppState) {
    log!(
        state.logs,
        "[UNPACK-Q] Started (batch: {BATCH_SIZE}, concurrency: {CONCURRENCY}, timeout: {}s)",
        RQBIT_TIMEOUT.as_secs()
    );

    let rqbit = match RqbitResolver::new().await {
        Ok(r) => {
            log!(state.logs, "[UNPACK-Q] librqbit session ready");
            Arc::new(r)
        }
        Err(e) => {
            log!(state.logs, "[UNPACK-Q] FATAL: librqbit init failed: {e}");
            return;
        }
    };

    loop {
        if state.metrics.controls.is_paused("dht") {
            state.queues.dht.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        state.queues.dht.store(true, Ordering::Relaxed);

        let candidates = match state.db.get_dht_queue_candidates(BATCH_SIZE).await {
            Ok(c) => c,
            Err(e) => {
                log!(state.logs, "[UNPACK-Q] Query error: {e}");
                state.queues.dht.store(false, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        if candidates.is_empty() {
            state.queues.dht.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(30)).await;
            continue;
        }

        log!(
            state.logs,
            "[UNPACK-Q] Processing {} hashes via swarm",
            candidates.len()
        );

        let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
        let mut handles = Vec::with_capacity(candidates.len());
        let candidate_hashes: Vec<String> = candidates.iter().map(|c| c.hash.clone()).collect();

        for candidate in candidates {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let state = state.clone();
            let rqbit = rqbit.clone();

            let handle = tokio::spawn(async move {
                let result = process_dht_pack(&state, &rqbit, &candidate).await;
                drop(permit);
                result
            });

            handles.push(handle);
        }

        let mut ok_count = 0u64;
        let mut err_count = 0u64;
        for (handle, hash) in handles.into_iter().zip(candidate_hashes.iter()) {
            match handle.await {
                Ok(Ok(n)) => ok_count += n,
                Ok(Err(e)) => {
                    err_count += 1;
                    log!(state.logs, "[UNPACK-Q] {e}");
                    // Bump attempts. Packs cache worker will re-pick after 1hr cooldown
                    // if attempts < 5. At attempts >= 5 it's marked lost.
                    let _ = state.db.mark_dht_attempted(hash).await;
                }
                Err(e) => {
                    err_count += 1;
                    log!(state.logs, "[UNPACK-Q] Task panicked: {e}");
                    let _ = state.db.mark_dht_attempted(hash).await;
                }
            }
        }

        state.metrics.bump("swarm_hits", ok_count);
        state.metrics.bump("packs_failed", err_count);

        log!(
            state.logs,
            "[UNPACK-Q] Batch done: {ok_count} resolved, {err_count} failed"
        );

        state.queues.dht.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Process a single hash from the DHT queue via librqbit swarm.
async fn process_dht_pack(
    state: &AppState,
    rqbit: &RqbitResolver,
    candidate: &crate::db::SeasonPackCandidate,
) -> Result<u64, String> {
    let hash = &candidate.hash;
    let short_hash = &hash[..8.min(hash.len())];

    let files = rqbit
        .fetch_torrent_files(hash, RQBIT_TIMEOUT)
        .await
        .map_err(|e| format!("{short_hash}: {e}"))?;

    let video_files = pipeline::filter_video_files(&files);

    if video_files.is_empty() {
        state
            .db
            .mark_stream_processed(hash)
            .await
            .map_err(|e| format!("{short_hash}: mark processed: {e}"))?;
        let _ = state.db.mark_dht_resolved(hash).await;
        return Ok(0);
    }

    let mut created = 0u64;
    for file in &video_files {
        let episode_info = crate::packs_worker::extract_episode_info(file, candidate.season);
        let (season, episode) = match episode_info {
            Some((s, e)) => (Some(s), Some(e)),
            None => (Some(candidate.season), None),
        };

        if let Ok(()) = state
            .db
            .upsert_stream(
                hash,
                &candidate.imdb_id,
                "series",
                file.size as i64,
                Some(file.index as i32),
                season,
                episode,
            )
            .await
        {
            created += 1;
        }
    }

    state
        .db
        .mark_stream_processed(hash)
        .await
        .map_err(|e| format!("{short_hash}: mark processed: {e}"))?;
    let _ = state.db.mark_dht_resolved(hash).await;

    Ok(created)
}
