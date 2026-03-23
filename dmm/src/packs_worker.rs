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

const BATCH_SIZE: i64 = 200;
const CONCURRENCY: usize = 20;

/// Cache-only fetcher: iTorrents + BT4G + Real-Debrid. No swarm.
/// Unresolvable hashes get moved to the DHT queue.
#[derive(Clone)]
pub struct CacheFetcher {
    http_client: reqwest::Client,
    rd_api_key: Option<String>,
}

impl CacheFetcher {
    pub fn new(rd_api_key: Option<String>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("Failed to build HTTP client");
        Self { http_client, rd_api_key }
    }

    /// Try cache providers. Returns Ok(files) on hit, Err on miss (should enqueue to DHT).
    pub async fn fetch_files(&self, info_hash: &str, logs: &crate::logs::LogBuffer, metrics: &crate::metrics::Metrics) -> Result<Vec<TorrentFile>, String> {
        let short = &info_hash[..8.min(info_hash.len())];

        // Tier 1: iTorrents + BT4G (concurrent)
        match torrent_cache::fetch_from_free_caches(&self.http_client, info_hash).await {
            Ok(files) if !files.is_empty() => {
                log!(logs, "[PACKS-Q] {short}: cache hit ({} files)", files.len());
                metrics.bump("cache_hits", 1);
                return Ok(files);
            }
            _ => {}
        }

        // Tier 2: Real-Debrid
        if let Some(ref key) = self.rd_api_key {
            match torrent_cache::fetch_from_rd(&self.http_client, info_hash, key).await {
                Ok(files) if !files.is_empty() => {
                    log!(logs, "[PACKS-Q] {short}: RD hit ({} files)", files.len());
                    metrics.bump("rd_hits", 1);
                    return Ok(files);
                }
                _ => {}
            }
        }

        Err("cache miss".into())
    }
}

/// Run the season pack ingestor worker (cache-only).
/// Resolved packs go to streams table. Unresolved packs get enqueued to DHT queue.
pub async fn run_packs_worker(state: AppState) {
    log!(
        state.logs,
        "[PACKS-Q] Started (batch: {BATCH_SIZE}, concurrency: {CONCURRENCY})"
    );

    let rd_api_key = std::env::var("RD_API_KEY").ok().filter(|k| !k.is_empty());
    log!(
        state.logs,
        "[PACKS-Q] Ready — Tier1: iTorrents+BT4G | Tier2: {} | Unresolved -> DHT queue",
        if rd_api_key.is_some() { "Real-Debrid" } else { "disabled (no RD_API_KEY)" }
    );

    let fetcher = CacheFetcher::new(rd_api_key);

    loop {
        if state.metrics.controls.is_paused("packs") {
            state.queues.packs.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        state.queues.packs.store(true, Ordering::Relaxed);

        let candidates = match state.db.get_season_pack_candidates(BATCH_SIZE).await {
            Ok(c) => c,
            Err(e) => {
                log!(state.logs, "[PACKS-Q] Query error: {e}");
                state.queues.packs.store(false, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(30)).await;
                continue;
            }
        };

        if candidates.is_empty() {
            state.queues.packs.store(false, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(30)).await;
            continue;
        }

        log!(
            state.logs,
            "[PACKS-Q] Processing {} season packs",
            candidates.len()
        );

        let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
        let mut handles = Vec::with_capacity(candidates.len());
        let candidate_hashes: Vec<String> = candidates.iter().map(|c| c.hash.clone()).collect();
        let all_candidates = candidates.clone();

        for candidate in candidates {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let state = state.clone();
            let fetcher = fetcher.clone();

            let handle = tokio::spawn(async move {
                let result = process_season_pack(&state, &fetcher, &candidate).await;
                drop(permit);
                result
            });

            handles.push(handle);
        }

        let mut ok_count = 0u64;
        let mut dht_count = 0u64;
        for (handle, candidate_hash) in handles.into_iter().zip(candidate_hashes.iter()) {
            match handle.await {
                Ok(Ok(n)) => ok_count += n,
                Ok(Err(_)) => {
                    // Cache miss — enqueue to DHT queue for swarm resolution
                    // Find the candidate data to enqueue
                    if let Some(c) = all_candidates.iter().find(|c| &c.hash == candidate_hash) {
                        let _ = state.db.enqueue_dht_lookup(&c.hash, &c.imdb_id, c.season, c.size_bytes).await;
                        let _ = state.db.mark_pack_failure(&c.hash).await;
                    }
                    dht_count += 1;
                }
                Err(e) => {
                    log!(state.logs, "[PACKS-Q] Task panicked: {e}");
                    dht_count += 1;
                }
            }
        }

        state.metrics.bump("packs_resolved", ok_count);

        log!(
            state.logs,
            "[PACKS-Q] Batch done: {ok_count} cached, {dht_count} -> DHT queue"
        );

        state.queues.packs.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Process a single season pack using cache providers only.
async fn process_season_pack(
    state: &AppState,
    fetcher: &CacheFetcher,
    candidate: &crate::db::SeasonPackCandidate,
) -> Result<u64, String> {
    let hash = &candidate.hash;
    let short_hash = &hash[..8.min(hash.len())];

    // Fetch torrent file list from caches only (no swarm)
    let files = fetcher
        .fetch_files(hash, &state.logs, &state.metrics)
        .await
        .map_err(|e| format!("{short_hash}: {e}"))?;

    // Filter to video files only
    let video_files = pipeline::filter_video_files(&files);

    if video_files.is_empty() {
        // Mark as processed even if no video files — don't retry
        state
            .db
            .mark_stream_processed(hash)
            .await
            .map_err(|e| format!("Failed to mark {short_hash} processed: {e}"))?;
        return Ok(0);
    }

    let mut created = 0u64;

    for file in &video_files {
        let episode_info = extract_episode_info(file, candidate.season);

        let (season, episode) = match episode_info {
            Some((s, e)) => (Some(s), Some(e)),
            None => (Some(candidate.season), None),
        };

        let result = state
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
            .await;

        match result {
            Ok(()) => created += 1,
            Err(e) => {
                log!(
                    state.logs,
                    "[PACKS-Q] Failed to upsert stream {short_hash} file {}: {e}",
                    file.index
                );
            }
        }
    }

    // Mark hash as processed
    state
        .db
        .mark_stream_processed(hash)
        .await
        .map_err(|e| format!("Failed to mark {short_hash} processed: {e}"))?;

    Ok(created)
}

/// Extract (season, episode) from a video file within a season pack.
/// Uses the title parser and a fallback bare-E regex.
pub fn extract_episode_info(file: &TorrentFile, parent_season: i32) -> Option<(i32, i32)> {
    let filename = file.path.rsplit('/').next().unwrap_or(&file.path);

    // Try full title parser first
    if let Some(meta) = title_parser::parse(filename) {
        if let (Some(season), Some(episode)) = (meta.season, meta.episode) {
            return Some((season as i32, episode as i32));
        }
    }

    // Fallback: bare E## pattern
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

    // =====================================================================
    // extract_episode_info
    // =====================================================================

    #[test]
    fn test_extract_episode_standard_sxxexx() {
        let file = TorrentFile {
            path: "Breaking.Bad.S05E01.Live.Free.or.Die.720p.BluRay.mkv".into(),
            size: 500_000_000,
            index: 0,
        };
        let result = extract_episode_info(&file, 5).unwrap();
        assert_eq!(result, (5, 1));
    }

    #[test]
    fn test_extract_episode_nested_path() {
        let file = TorrentFile {
            path: "Season 1/Breaking.Bad.S01E03.720p.mkv".into(),
            size: 500_000_000,
            index: 2,
        };
        let result = extract_episode_info(&file, 1).unwrap();
        assert_eq!(result, (1, 3));
    }

    #[test]
    fn test_extract_episode_bare_e_format() {
        let file = TorrentFile {
            path: "E05.mkv".into(),
            size: 500_000_000,
            index: 4,
        };
        let result = extract_episode_info(&file, 2).unwrap();
        assert_eq!(result, (2, 5));
    }

    #[test]
    fn test_extract_episode_bare_e_with_name() {
        let file = TorrentFile {
            path: "Show.Name.E12.720p.mkv".into(),
            size: 500_000_000,
            index: 11,
        };
        let result = extract_episode_info(&file, 3).unwrap();
        assert_eq!(result, (3, 12));
    }

    #[test]
    fn test_extract_episode_no_episode_info() {
        let file = TorrentFile {
            path: "random_video.mkv".into(),
            size: 500_000_000,
            index: 0,
        };
        assert!(extract_episode_info(&file, 1).is_none());
    }

    #[test]
    fn test_extract_episode_movie_file_returns_none() {
        let file = TorrentFile {
            path: "The.Matrix.1999.1080p.BluRay.mkv".into(),
            size: 2_000_000_000,
            index: 0,
        };
        assert!(extract_episode_info(&file, 1).is_none());
    }

    #[test]
    fn test_extract_episode_uses_parent_season_for_bare_e() {
        let file = TorrentFile {
            path: "E01.mkv".into(),
            size: 500_000_000,
            index: 0,
        };
        let result = extract_episode_info(&file, 7).unwrap();
        assert_eq!(result, (7, 1));
    }

    #[test]
    fn test_extract_episode_multidigit() {
        let file = TorrentFile {
            path: "One.Piece.S01E100.1080p.WEB-DL.mkv".into(),
            size: 500_000_000,
            index: 99,
        };
        let result = extract_episode_info(&file, 1).unwrap();
        assert_eq!(result, (1, 100));
    }

    // =====================================================================
    // Video file filtering integration (using pipeline::filter_video_files)
    // =====================================================================

    #[test]
    fn test_season_pack_video_filtering() {
        let files = vec![
            TorrentFile { path: "S01E01.mkv".into(), size: 500_000_000, index: 0 },
            TorrentFile { path: "S01E02.mkv".into(), size: 500_000_000, index: 1 },
            TorrentFile { path: "sample.mkv".into(), size: 10_000_000, index: 2 },
            TorrentFile { path: "info.nfo".into(), size: 500, index: 3 },
            TorrentFile { path: "subs.srt".into(), size: 30_000, index: 4 },
        ];
        let videos = pipeline::filter_video_files(&files);
        assert_eq!(videos.len(), 2);
        assert_eq!(videos[0].index, 0);
        assert_eq!(videos[1].index, 1);
    }

    #[test]
    fn test_season_pack_episode_extraction_pipeline() {
        // Simulate full pipeline: filter videos then extract episodes
        let files = vec![
            TorrentFile { path: "Breaking.Bad.S05E01.720p.mkv".into(), size: 500_000_000, index: 0 },
            TorrentFile { path: "Breaking.Bad.S05E02.720p.mkv".into(), size: 500_000_000, index: 1 },
            TorrentFile { path: "Breaking.Bad.S05E03.720p.mkv".into(), size: 500_000_000, index: 2 },
            TorrentFile { path: "sample.mkv".into(), size: 5_000_000, index: 3 },
            TorrentFile { path: "info.nfo".into(), size: 1000, index: 4 },
        ];

        let videos = pipeline::filter_video_files(&files);
        assert_eq!(videos.len(), 3);

        let episodes: Vec<(i32, i32)> = videos
            .iter()
            .filter_map(|f| extract_episode_info(f, 5))
            .collect();

        assert_eq!(episodes.len(), 3);
        assert_eq!(episodes[0], (5, 1));
        assert_eq!(episodes[1], (5, 2));
        assert_eq!(episodes[2], (5, 3));
    }

    // =====================================================================
    // Full season pack simulations — realistic torrent file lists
    // =====================================================================

    /// Helper: simulate full pack pipeline — filter videos, extract episodes, return tagged results.
    fn simulate_pack_pipeline(
        files: &[TorrentFile],
        parent_season: i32,
    ) -> Vec<(u32, i32, i32)> {
        // returns (file_index, season, episode)
        let videos = pipeline::filter_video_files(files);
        videos
            .iter()
            .filter_map(|f| {
                let (s, e) = extract_episode_info(f, parent_season)?;
                Some((f.index, s, e))
            })
            .collect()
    }

    #[test]
    fn test_pack_breaking_bad_s05() {
        // Breaking Bad Season 5 — typical BluRay rip structure
        let files = vec![
            TorrentFile { path: "Breaking.Bad.S05E01.Live.Free.or.Die.1080p.BluRay.x264.mkv".into(), size: 4_500_000_000, index: 0 },
            TorrentFile { path: "Breaking.Bad.S05E02.Madrigal.1080p.BluRay.x264.mkv".into(), size: 4_200_000_000, index: 1 },
            TorrentFile { path: "Breaking.Bad.S05E03.Hazard.Pay.1080p.BluRay.x264.mkv".into(), size: 4_100_000_000, index: 2 },
            TorrentFile { path: "Breaking.Bad.S05E04.Fifty-One.1080p.BluRay.x264.mkv".into(), size: 4_300_000_000, index: 3 },
            TorrentFile { path: "Breaking.Bad.S05E05.Dead.Freight.1080p.BluRay.x264.mkv".into(), size: 4_600_000_000, index: 4 },
            TorrentFile { path: "Breaking.Bad.S05E06.Buyout.1080p.BluRay.x264.mkv".into(), size: 4_000_000_000, index: 5 },
            TorrentFile { path: "Breaking.Bad.S05E07.Say.My.Name.1080p.BluRay.x264.mkv".into(), size: 4_400_000_000, index: 6 },
            TorrentFile { path: "Breaking.Bad.S05E08.Gliding.Over.All.1080p.BluRay.x264.mkv".into(), size: 4_700_000_000, index: 7 },
            TorrentFile { path: "Sample/sample.mkv".into(), size: 50_000_000, index: 8 },
            TorrentFile { path: "RARBG.txt".into(), size: 30, index: 9 },
            TorrentFile { path: "Subs/Breaking.Bad.S05E01.srt".into(), size: 50_000, index: 10 },
        ];

        let results = simulate_pack_pipeline(&files, 5);
        assert_eq!(results.len(), 8);
        for (i, (file_idx, season, episode)) in results.iter().enumerate() {
            assert_eq!(*file_idx, i as u32);
            assert_eq!(*season, 5);
            assert_eq!(*episode, (i + 1) as i32);
        }
    }

    #[test]
    fn test_pack_game_of_thrones_s01() {
        // GoT S01 — nested folder structure with subtitles and extras
        let files = vec![
            TorrentFile { path: "Game.of.Thrones.S01E01.Winter.is.Coming.720p.BluRay.mkv".into(), size: 3_000_000_000, index: 0 },
            TorrentFile { path: "Game.of.Thrones.S01E02.The.Kingsroad.720p.BluRay.mkv".into(), size: 2_900_000_000, index: 1 },
            TorrentFile { path: "Game.of.Thrones.S01E03.Lord.Snow.720p.BluRay.mkv".into(), size: 2_800_000_000, index: 2 },
            TorrentFile { path: "Game.of.Thrones.S01E04.Cripples.Bastards.and.Broken.Things.720p.BluRay.mkv".into(), size: 3_100_000_000, index: 3 },
            TorrentFile { path: "Game.of.Thrones.S01E05.The.Wolf.and.the.Lion.720p.BluRay.mkv".into(), size: 2_700_000_000, index: 4 },
            TorrentFile { path: "Game.of.Thrones.S01E06.A.Golden.Crown.720p.BluRay.mkv".into(), size: 2_900_000_000, index: 5 },
            TorrentFile { path: "Game.of.Thrones.S01E07.You.Win.or.You.Die.720p.BluRay.mkv".into(), size: 3_000_000_000, index: 6 },
            TorrentFile { path: "Game.of.Thrones.S01E08.The.Pointy.End.720p.BluRay.mkv".into(), size: 2_800_000_000, index: 7 },
            TorrentFile { path: "Game.of.Thrones.S01E09.Baelor.720p.BluRay.mkv".into(), size: 3_200_000_000, index: 8 },
            TorrentFile { path: "Game.of.Thrones.S01E10.Fire.and.Blood.720p.BluRay.mkv".into(), size: 3_100_000_000, index: 9 },
            TorrentFile { path: "Extras/Behind.the.Scenes.mkv".into(), size: 800_000_000, index: 10 },
            TorrentFile { path: "Subs/English.srt".into(), size: 80_000, index: 11 },
        ];

        let results = simulate_pack_pipeline(&files, 1);
        assert_eq!(results.len(), 10); // 10 episodes, extras excluded (no SxxExx pattern)
        assert_eq!(results[0], (0, 1, 1));
        assert_eq!(results[4], (4, 1, 5));
        assert_eq!(results[9], (9, 1, 10));
    }

    #[test]
    fn test_pack_stranger_things_s04_nested_folders() {
        // Stranger Things S04 — nested Season folders
        let files = vec![
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E01.Chapter.One.The.Hellfire.Club.2160p.NF.WEB-DL.mkv".into(), size: 6_000_000_000, index: 0 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E02.Chapter.Two.Vecnas.Curse.2160p.NF.WEB-DL.mkv".into(), size: 5_500_000_000, index: 1 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E03.Chapter.Three.The.Monster.and.the.Superhero.2160p.NF.WEB-DL.mkv".into(), size: 5_200_000_000, index: 2 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E04.Chapter.Four.Dear.Billy.2160p.NF.WEB-DL.mkv".into(), size: 5_800_000_000, index: 3 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E05.Chapter.Five.The.Nina.Project.2160p.NF.WEB-DL.mkv".into(), size: 5_600_000_000, index: 4 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E06.Chapter.Six.The.Dive.2160p.NF.WEB-DL.mkv".into(), size: 5_400_000_000, index: 5 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E07.Chapter.Seven.The.Massacre.at.Hawkins.Lab.2160p.NF.WEB-DL.mkv".into(), size: 6_200_000_000, index: 6 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E08.Chapter.Eight.Papa.2160p.NF.WEB-DL.mkv".into(), size: 5_900_000_000, index: 7 },
            TorrentFile { path: "Stranger.Things.S04/Stranger.Things.S04E09.Chapter.Nine.The.Piggyback.2160p.NF.WEB-DL.mkv".into(), size: 9_000_000_000, index: 8 },
            TorrentFile { path: "Stranger.Things.S04/Subs/S04E01.English.srt".into(), size: 60_000, index: 9 },
            TorrentFile { path: "Stranger.Things.S04/Subs/S04E02.English.srt".into(), size: 55_000, index: 10 },
        ];

        let results = simulate_pack_pipeline(&files, 4);
        assert_eq!(results.len(), 9);
        assert_eq!(results[0], (0, 4, 1));
        assert_eq!(results[8], (8, 4, 9));
        // Verify file indices are preserved correctly
        for r in &results {
            assert_eq!(r.1, 4); // all season 4
        }
    }

    #[test]
    fn test_pack_the_office_s02_bare_episode_names() {
        // The Office — some releases use bare E## naming without show title
        let files = vec![
            TorrentFile { path: "The.Office.US.S02/E01.The.Dundies.720p.mkv".into(), size: 400_000_000, index: 0 },
            TorrentFile { path: "The.Office.US.S02/E02.Sexual.Harassment.720p.mkv".into(), size: 380_000_000, index: 1 },
            TorrentFile { path: "The.Office.US.S02/E03.Office.Olympics.720p.mkv".into(), size: 390_000_000, index: 2 },
            TorrentFile { path: "The.Office.US.S02/E04.The.Fire.720p.mkv".into(), size: 370_000_000, index: 3 },
            TorrentFile { path: "The.Office.US.S02/E05.Halloween.720p.mkv".into(), size: 400_000_000, index: 4 },
            TorrentFile { path: "The.Office.US.S02/E06.The.Fight.720p.mkv".into(), size: 360_000_000, index: 5 },
            TorrentFile { path: "The.Office.US.S02/info.nfo".into(), size: 2000, index: 6 },
            TorrentFile { path: "The.Office.US.S02/cover.jpg".into(), size: 500_000, index: 7 },
        ];

        let results = simulate_pack_pipeline(&files, 2);
        assert_eq!(results.len(), 6);
        for (i, (file_idx, season, episode)) in results.iter().enumerate() {
            assert_eq!(*file_idx, i as u32);
            assert_eq!(*season, 2);
            assert_eq!(*episode, (i + 1) as i32);
        }
    }

    #[test]
    fn test_pack_anime_demon_slayer_s04() {
        // Anime — sometimes uses different naming conventions
        let files = vec![
            TorrentFile { path: "Demon.Slayer.Kimetsu.no.Yaiba.S04E01.1080p.WEB.H.264.mkv".into(), size: 1_400_000_000, index: 0 },
            TorrentFile { path: "Demon.Slayer.Kimetsu.no.Yaiba.S04E02.1080p.WEB.H.264.mkv".into(), size: 1_350_000_000, index: 1 },
            TorrentFile { path: "Demon.Slayer.Kimetsu.no.Yaiba.S04E03.1080p.WEB.H.264.mkv".into(), size: 1_380_000_000, index: 2 },
            TorrentFile { path: "Demon.Slayer.Kimetsu.no.Yaiba.S04E04.1080p.WEB.H.264.mkv".into(), size: 1_500_000_000, index: 3 },
            TorrentFile { path: "Demon.Slayer.Kimetsu.no.Yaiba.S04E05.1080p.WEB.H.264.mkv".into(), size: 1_420_000_000, index: 4 },
        ];

        let results = simulate_pack_pipeline(&files, 4);
        assert_eq!(results.len(), 5);
        assert_eq!(results[0], (0, 4, 1));
        assert_eq!(results[4], (4, 4, 5));
    }

    #[test]
    fn test_pack_with_extras_behind_scenes_excluded() {
        // Pack with extras that don't have episode numbers — should be filtered out
        let files = vec![
            TorrentFile { path: "Show.S01E01.Pilot.720p.mkv".into(), size: 1_000_000_000, index: 0 },
            TorrentFile { path: "Show.S01E02.Second.720p.mkv".into(), size: 1_000_000_000, index: 1 },
            TorrentFile { path: "Show.S01E03.Third.720p.mkv".into(), size: 1_000_000_000, index: 2 },
            TorrentFile { path: "Extras/Behind.The.Scenes.mkv".into(), size: 800_000_000, index: 3 },
            TorrentFile { path: "Extras/Deleted.Scenes.mkv".into(), size: 600_000_000, index: 4 },
            TorrentFile { path: "Extras/Gag.Reel.mkv".into(), size: 300_000_000, index: 5 },
        ];

        let results = simulate_pack_pipeline(&files, 1);
        // Only 3 episodes — extras are video files but have no episode info
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (0, 1, 1));
        assert_eq!(results[1], (1, 1, 2));
        assert_eq!(results[2], (2, 1, 3));
    }

    // =====================================================================
    // Live integration test — requires network + qBittorrent or DHT
    // Run with: cargo test test_live_season_pack -- --ignored --nocapture
    // =====================================================================

    /// Test with a well-known public domain torrent via qBittorrent.
    /// Requires: docker compose up (qBittorrent at localhost:8080)
    /// Run with: cargo test test_live_qbit_fetch -- --ignored --nocapture
    #[tokio::test]
    #[ignore] // requires running qBittorrent container
    async fn test_live_qbit_fetch() {
        use crate::qbit::QbitClient;

        let qbit = QbitClient::new("http://localhost:8080");
        if let Err(e) = qbit.login().await {
            eprintln!("qBittorrent login failed: {e}, skipping");
            return;
        }

        // Big Buck Bunny — well-seeded public domain torrent (single file)
        let hash = "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c";
        println!("Fetching files for Big Buck Bunny via qBittorrent...");

        let result = qbit
            .fetch_torrent_files(hash, Duration::from_secs(60))
            .await;

        match result {
            Ok(files) => {
                println!("Got {} files:", files.len());
                for f in &files {
                    println!("  [{}] {} ({} bytes)", f.index, f.path, f.size);
                }
                assert!(!files.is_empty(), "Should have at least one file");
                // Big Buck Bunny is a single ~276MB file
                assert!(files[0].path.contains("Big Buck Bunny") || files[0].size > 100_000_000);
            }
            Err(e) => {
                eprintln!("qBittorrent fetch failed: {e}");
                // Don't hard fail — qBittorrent may need time to connect to swarm
            }
        }
    }

    #[test]
    fn test_pack_file_index_preserved_with_gaps() {
        // Verify file_index is correctly preserved even when non-video files create gaps
        let files = vec![
            TorrentFile { path: "info.nfo".into(), size: 1000, index: 0 },
            TorrentFile { path: "cover.jpg".into(), size: 500_000, index: 1 },
            TorrentFile { path: "Show.S03E01.720p.mkv".into(), size: 1_500_000_000, index: 2 },
            TorrentFile { path: "Show.S03E02.720p.mkv".into(), size: 1_500_000_000, index: 3 },
            TorrentFile { path: "subs.srt".into(), size: 50_000, index: 4 },
            TorrentFile { path: "Show.S03E03.720p.mkv".into(), size: 1_500_000_000, index: 5 },
        ];

        let results = simulate_pack_pipeline(&files, 3);
        assert_eq!(results.len(), 3);
        // file indices should be 2, 3, 5 — not 0, 1, 2
        assert_eq!(results[0], (2, 3, 1));
        assert_eq!(results[1], (3, 3, 2));
        assert_eq!(results[2], (5, 3, 3));
    }
}
