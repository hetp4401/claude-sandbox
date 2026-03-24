use crate::dht::TorrentFile;
use crate::ingestor::AppState;
use crate::log;
use crate::pipeline;
use crate::title_parser;
use crate::torrent_cache;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Pipeline 6: imdb_mappings (season packs) → streams
/// Uses iTorrents + BT4G + Real-Debrid to get file lists.
pub async fn run_pipeline6_packs(state: AppState) {
    let cfg = &state.db.config.packs;
    let batch_size = cfg.batch_size;
    let concurrency = cfg.concurrency;
    let rd_api_key = std::env::var("RD_API_KEY").ok().filter(|k| !k.is_empty());
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("Failed to build HTTP client");

    log!(
        state.logs,
        "[P6-PACKS] Started (batch: {batch_size}, concurrency: {concurrency}) — Tier1: iTorrents+BT4G | Tier2: {}",
        if rd_api_key.is_some() { "Real-Debrid" } else { "disabled" }
    );

    loop {
        if state.metrics.controls.is_paused("packs") {
            state.queues.packs.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        state.queues.packs.store(true, Ordering::Relaxed);

        let candidates = match state.db.get_pending_packs(batch_size).await {
            Ok(c) => c,
            Err(e) => {
                log!(state.logs, "[P6-PACKS] Query error: {e}");
                state.queues.packs.store(false, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        if candidates.is_empty() {
            state.queues.packs.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        log!(state.logs, "[P6-PACKS] Processing {} packs", candidates.len());

        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut handles = Vec::with_capacity(candidates.len());

        for (hash, imdb_id, season, size_bytes) in candidates {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let state = state.clone();
            let http_client = http_client.clone();
            let rd_key = rd_api_key.clone();

            handles.push(tokio::spawn(async move {
                let _ = state.db.touch_imdb_mapping(&hash).await;

                let result = process_pack(
                    &state, &http_client, rd_key.as_deref(),
                    &hash, &imdb_id, season, size_bytes,
                ).await;

                match result {
                    Ok(n) => {
                        let _ = state.db.complete_imdb_mapping(&hash).await;
                        state.metrics.bump("packs_resolved", 1);
                        n
                    }
                    Err(_) => {
                        // touch_imdb_mapping already bumped attempts+timestamp
                        0u64
                    }
                }
            }));

            // Release permit inside the spawn
            handles.last_mut().unwrap(); // just to avoid unused warning on permit
        }

        let mut ok_count = 0u64;
        let mut miss_count = 0u64;
        for h in handles {
            match h.await {
                Ok(n) if n > 0 => ok_count += n,
                Ok(_) => miss_count += 1,
                Err(e) => {
                    log!(state.logs, "[P6-PACKS] Task panicked: {e}");
                    miss_count += 1;
                }
            }
        }

        state.metrics.bump("packs_missed", miss_count);
        log!(state.logs, "[P6-PACKS] Batch done: {ok_count} streams, {miss_count} missed");

        state.queues.packs.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn process_pack(
    state: &AppState,
    http_client: &reqwest::Client,
    rd_api_key: Option<&str>,
    hash: &str,
    imdb_id: &str,
    season: i32,
    _size_bytes: i64,
) -> Result<u64, String> {
    let short = &hash[..8.min(hash.len())];

    // Tier 1: iTorrents + BT4G
    let files = match torrent_cache::fetch_from_free_caches(http_client, hash).await {
        Ok(files) if !files.is_empty() => {
            state.metrics.bump("cache_hits", 1);
            files
        }
        _ => {
            // Tier 2: Real-Debrid
            if let Some(key) = rd_api_key {
                match torrent_cache::fetch_from_rd(http_client, hash, key).await {
                    Ok(files) if !files.is_empty() => {
                        state.metrics.bump("rd_hits", 1);
                        files
                    }
                    _ => return Err(format!("{short}: cache+RD miss")),
                }
            } else {
                return Err(format!("{short}: cache miss"));
            }
        }
    };

    let video_files = pipeline::filter_video_files(&files);
    if video_files.is_empty() {
        return Ok(0);
    }

    let mut created = 0u64;
    for file in &video_files {
        let (s, e) = match extract_episode_info(file, season) {
            Some((s, e)) => (Some(s), Some(e)),
            None => (Some(season), None),
        };
        if state.db.upsert_stream(hash, imdb_id, "series", file.size as i64, Some(file.index as i32), s, e).await.is_ok() {
            created += 1;
        }
    }

    Ok(created)
}

/// Extract (season, episode) from a video file within a season pack.
pub fn extract_episode_info(file: &TorrentFile, parent_season: i32) -> Option<(i32, i32)> {
    let filename = file.path.rsplit('/').next().unwrap_or(&file.path);

    if let Some(meta) = title_parser::parse(filename) {
        if let (Some(season), Some(episode)) = (meta.season, meta.episode) {
            return Some((season as i32, episode as i32));
        }
    }

    let re = regex::Regex::new(r"(?i)(?:^|[.\s_-])E(\d{1,3})(?:[.\s_-]|$)").unwrap();
    if let Some(caps) = re.captures(filename) {
        let episode: i32 = caps[1].parse().ok()?;
        return Some((parent_season, episode));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::TorrentFile;

    #[test]
    fn test_extract_episode_standard() {
        let file = TorrentFile { path: "Breaking.Bad.S05E01.720p.mkv".into(), size: 500_000_000, index: 0 };
        assert_eq!(extract_episode_info(&file, 5), Some((5, 1)));
    }

    #[test]
    fn test_extract_episode_bare_e() {
        let file = TorrentFile { path: "E05.mkv".into(), size: 500_000_000, index: 4 };
        assert_eq!(extract_episode_info(&file, 2), Some((2, 5)));
    }

    #[test]
    fn test_extract_episode_none() {
        let file = TorrentFile { path: "random.mkv".into(), size: 500_000_000, index: 0 };
        assert!(extract_episode_info(&file, 1).is_none());
    }

    #[test]
    fn test_season_pack_pipeline() {
        let files = vec![
            TorrentFile { path: "S01E01.mkv".into(), size: 500_000_000, index: 0 },
            TorrentFile { path: "S01E02.mkv".into(), size: 500_000_000, index: 1 },
            TorrentFile { path: "sample.mkv".into(), size: 10_000_000, index: 2 },
            TorrentFile { path: "info.nfo".into(), size: 500, index: 3 },
        ];
        let videos = pipeline::filter_video_files(&files);
        assert_eq!(videos.len(), 2);
        let eps: Vec<_> = videos.iter().filter_map(|f| extract_episode_info(f, 1)).collect();
        assert_eq!(eps, vec![(1, 1), (1, 2)]);
    }
}
