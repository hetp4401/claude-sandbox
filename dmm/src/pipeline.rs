use crate::dht::TorrentFile;
use crate::title_parser::{self, ContentType};

/// Filter video files from a torrent file list (skip .nfo, .txt, .srt, samples, etc.)
pub fn filter_video_files(files: &[TorrentFile]) -> Vec<&TorrentFile> {
    let video_exts = [
        ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v", ".ts",
    ];

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
pub fn parse_episode_from_file(
    file: &TorrentFile,
    parent_title: &str,
    parent_season: u32,
) -> Option<(String, u32, u32)> {
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::TorrentFile;

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
        let files = vec![TorrentFile {
            path: "info.nfo".into(),
            size: 1000,
            index: 0,
        }];
        assert!(filter_video_files(&files).is_empty());
    }

    #[test]
    fn test_filter_video_files_removes_srt() {
        let files = vec![TorrentFile {
            path: "subs.srt".into(),
            size: 50000,
            index: 0,
        }];
        assert!(filter_video_files(&files).is_empty());
    }

    #[test]
    fn test_filter_video_files_removes_txt() {
        let files = vec![TorrentFile {
            path: "readme.txt".into(),
            size: 100,
            index: 0,
        }];
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
            TorrentFile {
                path: "S01E01.mkv".into(),
                size: 500_000_000,
                index: 0,
            },
            TorrentFile {
                path: "S01E02.mkv".into(),
                size: 500_000_000,
                index: 1,
            },
            TorrentFile {
                path: "sample.mkv".into(),
                size: 10_000_000,
                index: 2,
            },
            TorrentFile {
                path: "info.nfo".into(),
                size: 500,
                index: 3,
            },
            TorrentFile {
                path: "subs.srt".into(),
                size: 30_000,
                index: 4,
            },
            TorrentFile {
                path: "S01E03.mkv".into(),
                size: 500_000_000,
                index: 5,
            },
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
            size: 500_000_000,
            index: 0,
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
            size: 500_000_000,
            index: 2,
        };
        let result = parse_episode_from_file(&file, "Breaking Bad", 1).unwrap();
        assert_eq!(result.1, 1);
        assert_eq!(result.2, 3);
    }

    #[test]
    fn test_parse_episode_bare_e_format() {
        let file = TorrentFile {
            path: "E05.mkv".into(),
            size: 500_000_000,
            index: 4,
        };
        let result = parse_episode_from_file(&file, "My Show", 2).unwrap();
        assert_eq!(result.0, "My Show");
        assert_eq!(result.1, 2);
        assert_eq!(result.2, 5);
    }

    #[test]
    fn test_parse_episode_bare_e_with_dots() {
        let file = TorrentFile {
            path: "Show.Name.E12.720p.mkv".into(),
            size: 500_000_000,
            index: 11,
        };
        let result = parse_episode_from_file(&file, "Show Name", 3).unwrap();
        assert_eq!(result.2, 12);
    }

    #[test]
    fn test_parse_episode_no_episode_info() {
        let file = TorrentFile {
            path: "random_video.mkv".into(),
            size: 500_000_000,
            index: 0,
        };
        assert!(parse_episode_from_file(&file, "Show", 1).is_none());
    }

    #[test]
    fn test_parse_episode_movie_file_returns_none() {
        let file = TorrentFile {
            path: "The.Matrix.1999.1080p.BluRay.mkv".into(),
            size: 2_000_000_000,
            index: 0,
        };
        assert!(parse_episode_from_file(&file, "The Matrix", 1).is_none());
    }

    #[test]
    fn test_parse_episode_uses_parent_title_for_bare_e() {
        let file = TorrentFile {
            path: "E01.mkv".into(),
            size: 500_000_000,
            index: 0,
        };
        let result = parse_episode_from_file(&file, "Parent Title", 7).unwrap();
        assert_eq!(result.0, "Parent Title");
        assert_eq!(result.1, 7);
        assert_eq!(result.2, 1);
    }

    // =====================================================================
    // Content type classification integration
    // =====================================================================

    #[test]
    fn test_classify_and_parse_unknown_filename() {
        let meta = title_parser::parse("Inception.2010.1080p.BluRay.x264-GROUP").unwrap();
        assert_eq!(meta.content_type, ContentType::Unknown);
        assert_eq!(meta.title.as_deref(), Some("Inception"));
        assert_eq!(meta.year, Some(2010));
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
}
