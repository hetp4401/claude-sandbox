use crate::dht::{self, TorrentFile};
use crate::ingestor::{AppState, MagnetRecord};
use crate::title_parser::{self, ContentType};
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Filter video files from a torrent file list (skip .nfo, .txt, .srt, samples, etc.)
pub fn filter_video_files(files: &[TorrentFile]) -> Vec<&TorrentFile> {
    let video_exts = [".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts"];

    files
        .iter()
        .filter(|f| {
            let lower = f.path.to_lowercase();
            let is_video = video_exts.iter().any(|ext| lower.ends_with(ext));
            let is_sample = lower.contains("sample") && f.size < 100_000_000;
            is_video && !is_sample
        })
        .collect()
}

/// Try to extract episode info from a file within a season pack.
/// Returns (title, season, episode) if the filename parses as an episode.
pub fn parse_episode_from_file(file: &TorrentFile, parent_title: &str, parent_season: u32) -> Option<(String, u32, u32)> {
    let filename = file.path.rsplit('/').next().unwrap_or(&file.path);

    if let Some(meta) = title_parser::parse(filename) {
        if meta.content_type == ContentType::Episode {
            if let (Some(season), Some(episode)) = (meta.season, meta.episode) {
                let title = meta.title.unwrap_or_else(|| parent_title.to_string());
                return Some((title, season, episode));
            }
        }
    }

    let re = regex::Regex::new(r"(?i)(?:^|[.\s_-])E(\d{1,3})(?:[.\s_-]|$)").unwrap();
    if let Some(caps) = re.captures(filename) {
        let episode: u32 = caps[1].parse().ok()?;
        return Some((parent_title.to_string(), parent_season, episode));
    }

    None
}

/// Process a single season-pack record: fetch file list via DHT, create episode records.
async fn expand_season_pack(
    record: &MagnetRecord,
    dht_timeout: Duration,
) -> Vec<MagnetRecord> {
    let parent_title = match &record.title {
        Some(t) => t.clone(),
        None => return vec![],
    };
    let parent_season = match record.season {
        Some(s) => s,
        None => return vec![],
    };

    println!(
        "[PIPELINE] Expanding season pack: {} S{:02} ({})",
        parent_title, parent_season, &record.hash[..8]
    );

    let files = match dht::fetch_torrent_files(&record.hash, dht_timeout).await {
        Ok(f) => f,
        Err(e) => {
            println!(
                "[PIPELINE] DHT fetch failed for {}: {e}",
                &record.hash[..8]
            );
            return vec![];
        }
    };

    let video_files = filter_video_files(&files);
    println!(
        "[PIPELINE] Found {} files ({} video) in {}",
        files.len(),
        video_files.len(),
        &record.hash[..8]
    );

    let mut episodes = Vec::new();
    for file in video_files {
        if let Some((title, season, episode)) = parse_episode_from_file(file, &parent_title, parent_season) {
            episodes.push(MagnetRecord {
                filename: file.path.clone(),
                hash: record.hash.clone(),
                magnet_uri: record.magnet_uri.clone(),
                size_bytes: file.size,
                source_hashlist: record.source_hashlist.clone(),
                processed_at: Utc::now().to_rfc3339(),
                content_type: Some("episode".into()),
                title: Some(title),
                season: Some(season),
                episode: Some(episode),
                file_index: Some(file.index),
                imdb_tag: None,
            });
        }
    }

    println!(
        "[PIPELINE] Created {} episode records from season pack {}",
        episodes.len(),
        &record.hash[..8]
    );

    episodes
}

/// Run the metadata enrichment pipeline.
pub async fn run_enrichment_pipeline(state: &AppState) {
    let num_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let semaphore = Arc::new(Semaphore::new(num_workers));
    let dht_timeout = Duration::from_secs(30);

    let season_packs = match state.db.get_unprocessed_season_packs() {
        Ok(packs) => packs,
        Err(e) => {
            println!("[PIPELINE] Failed to query season packs: {e}");
            return;
        }
    };

    if season_packs.is_empty() {
        return;
    }

    println!(
        "[PIPELINE] Found {} season packs to expand",
        season_packs.len()
    );

    let mut handles = Vec::new();
    for record in season_packs {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let state = state.clone();

        let handle = tokio::spawn(async move {
            let episodes = expand_season_pack(&record, dht_timeout).await;
            let hash = record.hash.clone();

            // Insert episode records and mark season pack as processed
            let state_clone = state.clone();
            tokio::task::spawn_blocking(move || {
                if !episodes.is_empty() {
                    if let Err(e) = state_clone.db.insert_records(&episodes) {
                        println!("[PIPELINE] Failed to insert episodes: {e}");
                    }
                }
                if let Err(e) = state_clone.db.mark_season_pack_processed(&hash) {
                    println!("[PIPELINE] Failed to mark season pack processed: {e}");
                }
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
                println!("[PIPELINE] Task panicked: {e}");
            }
        }
    }

    println!(
        "[PIPELINE] Season expansion complete: {ok_count} succeeded, {err_count} failed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::dht::TorrentFile;
    use crate::hashlist::HashlistEntry;
    use crate::ingestor::AppState;

    fn test_state() -> AppState {
        AppState::new(Db::open_in_memory().unwrap())
    }

    // =====================================================================
    // filter_video_files
    // =====================================================================

    #[test]
    fn test_filter_video_files_keeps_mkv() {
        let files = vec![TorrentFile {
            path: "episode.mkv".into(),
            size: 500_000_000,
            index: 0,
        }];
        assert_eq!(filter_video_files(&files).len(), 1);
    }

    #[test]
    fn test_filter_video_files_keeps_mp4() {
        let files = vec![TorrentFile {
            path: "movie.mp4".into(),
            size: 1_000_000_000,
            index: 0,
        }];
        assert_eq!(filter_video_files(&files).len(), 1);
    }

    #[test]
    fn test_filter_video_files_removes_nfo() {
        let files = vec![TorrentFile { path: "info.nfo".into(), size: 1000, index: 0 }];
        assert!(filter_video_files(&files).is_empty());
    }

    #[test]
    fn test_filter_video_files_removes_srt() {
        let files = vec![TorrentFile { path: "subs.srt".into(), size: 50000, index: 0 }];
        assert!(filter_video_files(&files).is_empty());
    }

    #[test]
    fn test_filter_video_files_removes_txt() {
        let files = vec![TorrentFile { path: "readme.txt".into(), size: 100, index: 0 }];
        assert!(filter_video_files(&files).is_empty());
    }

    #[test]
    fn test_filter_video_files_removes_small_sample() {
        let files = vec![TorrentFile {
            path: "Sample/sample.mkv".into(),
            size: 50_000_000,
            index: 0,
        }];
        assert!(filter_video_files(&files).is_empty());
    }

    #[test]
    fn test_filter_video_files_keeps_large_sample_named() {
        let files = vec![TorrentFile {
            path: "The.Sample.Movie.mkv".into(),
            size: 1_500_000_000,
            index: 0,
        }];
        assert_eq!(filter_video_files(&files).len(), 1);
    }

    #[test]
    fn test_filter_video_files_mixed() {
        let files = vec![
            TorrentFile { path: "S01E01.mkv".into(), size: 500_000_000, index: 0 },
            TorrentFile { path: "S01E02.mkv".into(), size: 500_000_000, index: 1 },
            TorrentFile { path: "sample.mkv".into(), size: 10_000_000, index: 2 },
            TorrentFile { path: "info.nfo".into(), size: 500, index: 3 },
            TorrentFile { path: "subs.srt".into(), size: 30_000, index: 4 },
            TorrentFile { path: "S01E03.mkv".into(), size: 500_000_000, index: 5 },
        ];
        let result = filter_video_files(&files);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].path, "S01E01.mkv");
        assert_eq!(result[1].path, "S01E02.mkv");
        assert_eq!(result[2].path, "S01E03.mkv");
    }

    // =====================================================================
    // parse_episode_from_file
    // =====================================================================

    #[test]
    fn test_parse_episode_standard_name() {
        let file = TorrentFile {
            path: "Breaking.Bad.S05E01.Live.Free.or.Die.720p.BluRay.mkv".into(),
            size: 500_000_000, index: 0,
        };
        let result = parse_episode_from_file(&file, "Breaking Bad", 5).unwrap();
        assert_eq!(result.0, "Breaking Bad");
        assert_eq!(result.1, 5);
        assert_eq!(result.2, 1);
    }

    #[test]
    fn test_parse_episode_nested_path() {
        let file = TorrentFile {
            path: "Season 1/Breaking.Bad.S01E03.720p.mkv".into(),
            size: 500_000_000, index: 2,
        };
        let result = parse_episode_from_file(&file, "Breaking Bad", 1).unwrap();
        assert_eq!(result.1, 1);
        assert_eq!(result.2, 3);
    }

    #[test]
    fn test_parse_episode_bare_e_format() {
        let file = TorrentFile { path: "E05.mkv".into(), size: 500_000_000, index: 4 };
        let result = parse_episode_from_file(&file, "My Show", 2).unwrap();
        assert_eq!(result.0, "My Show");
        assert_eq!(result.1, 2);
        assert_eq!(result.2, 5);
    }

    #[test]
    fn test_parse_episode_bare_e_with_dots() {
        let file = TorrentFile {
            path: "Show.Name.E12.720p.mkv".into(),
            size: 500_000_000, index: 11,
        };
        let result = parse_episode_from_file(&file, "Show Name", 3).unwrap();
        assert_eq!(result.2, 12);
    }

    #[test]
    fn test_parse_episode_no_episode_info() {
        let file = TorrentFile { path: "random_video.mkv".into(), size: 500_000_000, index: 0 };
        assert!(parse_episode_from_file(&file, "Show", 1).is_none());
    }

    #[test]
    fn test_parse_episode_movie_file_returns_none() {
        let file = TorrentFile {
            path: "The.Matrix.1999.1080p.BluRay.mkv".into(),
            size: 2_000_000_000, index: 0,
        };
        assert!(parse_episode_from_file(&file, "The Matrix", 1).is_none());
    }

    #[test]
    fn test_parse_episode_uses_parent_title_for_bare_e() {
        let file = TorrentFile { path: "E01.mkv".into(), size: 500_000_000, index: 0 };
        let result = parse_episode_from_file(&file, "Parent Title", 7).unwrap();
        assert_eq!(result.0, "Parent Title");
        assert_eq!(result.1, 7);
        assert_eq!(result.2, 1);
    }

    // =====================================================================
    // Pipeline integration tests
    // =====================================================================

    #[tokio::test]
    async fn test_pipeline_skips_when_no_season_packs() {
        let state = test_state();
        state.db.insert_record(&MagnetRecord {
            filename: "Movie.mkv".into(),
            hash: "a".repeat(40),
            magnet_uri: "magnet:?xt=urn:btih:aaaa".into(),
            size_bytes: 1000,
            source_hashlist: "test.html".into(),
            processed_at: Utc::now().to_rfc3339(),
            content_type: Some("movie".into()),
            title: Some("Movie".into()),
            season: None, episode: None, file_index: None, imdb_tag: None,
        }).unwrap();

        run_enrichment_pipeline(&state).await;
        assert_eq!(state.db.get_all_records().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_pipeline_skips_already_processed() {
        let state = test_state();
        state.db.insert_record(&MagnetRecord {
            filename: "Show.S01.mkv".into(),
            hash: "b".repeat(40),
            magnet_uri: "magnet:?xt=urn:btih:bbbb".into(),
            size_bytes: 5000,
            source_hashlist: "test.html".into(),
            processed_at: Utc::now().to_rfc3339(),
            content_type: Some("season".into()),
            title: Some("Show".into()),
            season: Some(1), episode: None, file_index: None,
            imdb_tag: Some("processed".into()),
        }).unwrap();

        run_enrichment_pipeline(&state).await;
        assert_eq!(state.db.get_all_records().unwrap().len(), 1);
    }

    // =====================================================================
    // Content type classification integration
    // =====================================================================

    #[test]
    fn test_classify_and_parse_movie_filename() {
        let meta = title_parser::parse("Inception.2010.1080p.BluRay.x264-GROUP").unwrap();
        assert_eq!(meta.content_type, ContentType::Movie);
        assert_eq!(meta.title.as_deref(), Some("Inception"));
    }

    #[test]
    fn test_classify_and_parse_episode_filename() {
        let meta = title_parser::parse("Breaking.Bad.S05E16.1080p.BluRay").unwrap();
        assert_eq!(meta.content_type, ContentType::Episode);
        assert_eq!(meta.season, Some(5));
        assert_eq!(meta.episode, Some(16));
    }

    #[test]
    fn test_classify_and_parse_season_filename() {
        let meta = title_parser::parse("Breaking.Bad.S05.1080p.BluRay").unwrap();
        assert_eq!(meta.content_type, ContentType::Season);
        assert_eq!(meta.season, Some(5));
        assert_eq!(meta.episode, None);
    }

    // =====================================================================
    // End-to-end pipeline data flow
    // =====================================================================

    #[test]
    fn test_season_pack_record_has_correct_fields() {
        let state = test_state();

        let entry = HashlistEntry {
            filename: "House.of.the.Dragon.S02.1080p.MAX.WEB-DL.DDP5.1.Atmos.H.264-FLUX".into(),
            hash: "c".repeat(40),
            size: 10_000_000_000,
        };

        let meta = title_parser::parse(&entry.filename);
        let record = MagnetRecord {
            filename: entry.filename.clone(),
            hash: entry.hash.clone(),
            magnet_uri: format!("magnet:?xt=urn:btih:{}", entry.hash),
            size_bytes: entry.size,
            source_hashlist: "test.html".into(),
            processed_at: Utc::now().to_rfc3339(),
            content_type: meta.as_ref().map(|m| m.content_type.to_string()),
            title: meta.as_ref().and_then(|m| m.title.clone()),
            season: meta.as_ref().and_then(|m| m.season),
            episode: meta.as_ref().and_then(|m| m.episode),
            file_index: None, imdb_tag: None,
        };

        assert_eq!(record.content_type.as_deref(), Some("season"));
        assert_eq!(record.title.as_deref(), Some("House of the Dragon"));
        assert_eq!(record.season, Some(2));

        state.db.insert_record(&record).unwrap();

        let packs = state.db.get_unprocessed_season_packs().unwrap();
        assert_eq!(packs.len(), 1);
    }

    #[test]
    fn test_episode_record_from_season_expansion() {
        let file = TorrentFile {
            path: "House.of.the.Dragon.S02E01.A.Son.for.a.Son.1080p.mkv".into(),
            size: 2_000_000_000, index: 0,
        };

        let (title, season, episode) =
            parse_episode_from_file(&file, "House of the Dragon", 2).unwrap();

        assert_eq!(title, "House of the Dragon");
        assert_eq!(season, 2);
        assert_eq!(episode, 1);

        let record = MagnetRecord {
            filename: file.path.clone(),
            hash: "c".repeat(40),
            magnet_uri: "magnet:?xt=urn:btih:cccc".into(),
            size_bytes: file.size,
            source_hashlist: "test.html".into(),
            processed_at: Utc::now().to_rfc3339(),
            content_type: Some("episode".into()),
            title: Some(title),
            season: Some(season),
            episode: Some(episode),
            file_index: Some(file.index),
            imdb_tag: None,
        };

        assert_eq!(record.content_type.as_deref(), Some("episode"));
        assert_eq!(record.file_index, Some(0));
        assert_eq!(record.episode, Some(1));
    }
}
