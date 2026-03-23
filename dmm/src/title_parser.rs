use std::fmt;

/// Parsed metadata from a torrent filename.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentMeta {
    pub title: Option<String>,
    pub year: Option<u32>,
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub content_type: ContentType,
}

/// Classification of a torrent based on parsed metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Episode,
    Season,
    Unknown,
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentType::Episode => write!(f, "episode"),
            ContentType::Season => write!(f, "season"),
            ContentType::Unknown => write!(f, "unknown"),
        }
    }
}

/// Regex-based patterns that indicate known tokens to strip when extracting the title.
/// These are applied in order; matched text is removed from the filename before
/// the remaining leading text is taken as the title.
const NOISE_PATTERNS: &[&str] = &[
    // Resolution
    r"(?i)\b(?:2160|1080|720|480|360|240)[pi]\b",
    r"(?i)\b4[Kk]\b",
    // Source / quality
    r"(?i)\b(?:WEB[-. ]?DL|WEB[-. ]?Rip|WEBRip|HDTV|BDRip|BRRip|BluRay|Blu[-. ]?Ray|DVDRip|DVD(?:Scr)?|HDRip|PDTV|SDTV|CAM|TS|TC|TELESYNC|TELECINE|SCR|R5|HQ)\b",
    // Codec
    r"(?i)\b(?:x264|x265|[Hh]\.?264|[Hh]\.?265|HEVC|AVC|XviD|DivX|VP9|AV1|MPEG[24]?)\b",
    // Audio
    r"(?i)\b(?:AAC(?:[. ]?2[. ]?0)?|AC3|DD[P+]?[. ]?[257][. ]?[01]?|DTS(?:[-. ]?HD)?(?:[-. ]?MA)?|FLAC|MP3|TrueHD|Atmos|EAC3|LPCM|Opus|Vorbis)\b",
    // HDR
    r"(?i)\b(?:HDR(?:10(?:\+|Plus)?)?|Dolby[-. ]?Vision|DV|HLG)\b",
    // Release group: hyphen-prefixed at end (e.g., -SPARKS, -FGT)
    r"-[A-Za-z0-9]{2,10}(?:\[[^\]]*\])?$",
    // Release tags in brackets at the end
    r"\[[^\]]*\]$",
    // Common tags
    r"(?i)\b(?:PROPER|REPACK|RERIP|REAL|INTERNAL|LIMITED|EXTENDED|UNRATED|REMASTERED|DC|DIRECTORS[. ]?CUT|THEATRICAL|IMAX|SUBBED|DUBBED|MULTI|DUAL|HYBRID|REMUX|COMPLETE)\b",
    // File extensions
    r"(?i)\.(?:mkv|mp4|avi|mov|wmv|flv|webm|m4v|ts|srt|sub|idx|nfo)$",
    // Bit depth
    r"(?i)\b(?:8|10|12)[-. ]?bit\b",
    // FPS
    r"(?i)\b(?:23\.976|24|25|29\.97|30|50|60)fps\b",
    // Size patterns like "1.5GB"
    r"(?i)\b\d+(?:\.\d+)?\s*(?:GB|MB|TB)\b",
    // 3D tags
    r"(?i)\b(?:3D|SBS|Half[-. ]?SBS|OU|Half[-. ]?OU)\b",
    // Year (4 digits, 1900-2099) — preceded by a space (not at position 0)
    r"\s(?:19|20)\d{2}\b",
    // Scene dots/underscores used as separators (handled in title cleanup)
];

/// Season+Episode patterns in priority order.
/// Each returns (season, episode) or (season, None) for packs.
struct SeasonEpisodeMatch {
    season: u32,
    episode: Option<u32>,
    start: usize,
}

/// Try to extract season/episode info from the filename.
fn extract_season_episode(input: &str) -> Option<SeasonEpisodeMatch> {
    use regex::Regex;

    // S01E02 or S1E2 (most common)
    let re_se = Regex::new(r"(?i)\bS(\d{1,3})[\s._-]*E(\d{1,3})\b").unwrap();
    if let Some(cap) = re_se.find(input) {
        let caps = re_se.captures(input).unwrap();
        return Some(SeasonEpisodeMatch {
            season: caps[1].parse().unwrap(),
            episode: Some(caps[2].parse().unwrap()),
            start: cap.start(),
        });
    }

    // S01E02E03 multi-episode — take first episode
    let re_multi = Regex::new(r"(?i)\bS(\d{1,3})[\s._-]*E(\d{1,3})(?:[\s._-]*E\d{1,3})+\b").unwrap();
    if let Some(cap) = re_multi.find(input) {
        let caps = re_multi.captures(input).unwrap();
        return Some(SeasonEpisodeMatch {
            season: caps[1].parse().unwrap(),
            episode: Some(caps[2].parse().unwrap()),
            start: cap.start(),
        });
    }

    // Season 1 Episode 2 (spelled out)
    let re_spelled = Regex::new(r"(?i)\bSeason[\s._-]*(\d{1,3})[\s._-]*Episode[\s._-]*(\d{1,3})\b").unwrap();
    if let Some(cap) = re_spelled.find(input) {
        let caps = re_spelled.captures(input).unwrap();
        return Some(SeasonEpisodeMatch {
            season: caps[1].parse().unwrap(),
            episode: Some(caps[2].parse().unwrap()),
            start: cap.start(),
        });
    }

    // 1x02 format
    let re_x = Regex::new(r"(?i)\b(\d{1,2})x(\d{2,3})\b").unwrap();
    if let Some(cap) = re_x.find(input) {
        let caps = re_x.captures(input).unwrap();
        let s: u32 = caps[1].parse().unwrap();
        let e: u32 = caps[2].parse().unwrap();
        // Avoid matching years like "2024" (would be 20x24)
        if s > 0 && s < 100 {
            return Some(SeasonEpisodeMatch {
                season: s,
                episode: Some(e),
                start: cap.start(),
            });
        }
    }

    // S01 without episode — season pack
    let re_s = Regex::new(r"(?i)\bS(\d{1,3})\b").unwrap();
    if let Some(cap) = re_s.find(input) {
        let caps = re_s.captures(input).unwrap();
        // Make sure this isn't part of a larger word
        return Some(SeasonEpisodeMatch {
            season: caps[1].parse().unwrap(),
            episode: None,
            start: cap.start(),
        });
    }

    // "Season 1" or "Season 01" (spelled out, no episode)
    let re_season_only = Regex::new(r"(?i)\bSeason[\s._-]*(\d{1,3})\b").unwrap();
    if let Some(cap) = re_season_only.find(input) {
        let caps = re_season_only.captures(input).unwrap();
        return Some(SeasonEpisodeMatch {
            season: caps[1].parse().unwrap(),
            episode: None,
            start: cap.start(),
        });
    }

    // "Complete Series" or "Complete Season" — no season/ep (skip these)
    None
}

/// Clean an already-normalized (spaces) title string.
fn clean_title_normalized(raw: &str) -> Option<String> {
    // Remove leading/trailing whitespace and punctuation
    let trimmed = raw
        .trim_matches(|c: char| c.is_whitespace() || c == '-' || c == '(' || c == ')' || c == '[' || c == ']')
        .to_string();

    // Remove any trailing year if present
    let trimmed = {
        let re = regex::Regex::new(r"\s+(?:19|20)\d{2}\s*$").unwrap();
        re.replace(&trimmed, "").to_string()
    };

    let trimmed = trimmed.trim().to_string();

    if trimmed.is_empty() || trimmed.len() < 2 {
        None
    } else {
        Some(trimmed)
    }
}

/// Parse a torrent filename and extract metadata.
///
/// Returns `None` if no title can be extracted.
pub fn parse(filename: &str) -> Option<TorrentMeta> {
    // Strip file extension first
    let name = {
        let re = regex::Regex::new(r"(?i)\.(?:mkv|mp4|avi|mov|wmv|flv|webm|m4v|ts)$").unwrap();
        re.replace(filename, "").to_string()
    };

    // Normalize dots and underscores to spaces so noise patterns match properly
    let normalized = name.replace('.', " ").replace('_', " ");

    // Extract season/episode info from normalized string
    let se_match = extract_season_episode(&normalized);

    // Determine where the title ends: either at the season/episode marker
    // or at the first noise pattern match
    let title_end = if let Some(ref se) = se_match {
        se.start
    } else {
        find_first_noise_position(&normalized).unwrap_or(normalized.len())
    };

    let year = extract_year(&normalized);

    let raw_title = &normalized[..title_end];
    let title = clean_title_normalized(raw_title)?;

    let (content_type, season, episode) = match &se_match {
        Some(se) => match se.episode {
            Some(ep) => (ContentType::Episode, Some(se.season), Some(ep)),
            None => (ContentType::Season, Some(se.season), None),
        },
        None => (ContentType::Unknown, None, None),
    };

    Some(TorrentMeta {
        title: Some(title),
        year,
        season,
        episode,
        content_type,
    })
}

/// Try to extract a 4-digit year (1900–2099) from the normalized filename.
/// Looks for year preceded by whitespace (not at position 0, to avoid titles like "2001").
fn extract_year(input: &str) -> Option<u32> {
    let re = regex::Regex::new(r"\s((?:19|20)\d{2})\b").unwrap();
    re.captures(input)
        .and_then(|caps| caps[1].parse().ok())
}

/// Find the position of the first noise token in the string.
fn find_first_noise_position(input: &str) -> Option<usize> {
    let mut earliest: Option<usize> = None;

    for pattern in NOISE_PATTERNS {
        if let Ok(re) = regex::Regex::new(pattern) {
            if let Some(m) = re.find(input) {
                let pos = m.start();
                if earliest.is_none() || pos < earliest.unwrap() {
                    earliest = Some(pos);
                }
            }
        }
    }

    earliest
}

#[cfg(test)]
mod tests {
    use super::*;

    // =====================================================================
    // Title extraction
    // =====================================================================

    #[test]
    fn test_movie_simple_dots() {
        let m = parse("The.Matrix.1999.1080p.BluRay.x264-GROUP").unwrap();
        assert_eq!(m.title.as_deref(), Some("The Matrix"));
        assert_eq!(m.content_type, ContentType::Unknown);
        assert_eq!(m.season, None);
        assert_eq!(m.episode, None);
    }

    #[test]
    fn test_movie_with_spaces() {
        let m = parse("Inception 2010 720p BRRip").unwrap();
        assert_eq!(m.title.as_deref(), Some("Inception"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_movie_underscores() {
        let m = parse("The_Dark_Knight_2008_1080p_BluRay").unwrap();
        assert_eq!(m.title.as_deref(), Some("The Dark Knight"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_movie_with_extension() {
        let m = parse("Interstellar.2014.2160p.WEB-DL.x265.mkv").unwrap();
        assert_eq!(m.title.as_deref(), Some("Interstellar"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_movie_no_year() {
        let m = parse("Big.Buck.Bunny.1080p.BluRay").unwrap();
        assert_eq!(m.title.as_deref(), Some("Big Buck Bunny"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    // =====================================================================
    // Episode extraction (S##E##)
    // =====================================================================

    #[test]
    fn test_episode_standard_format() {
        let m = parse("Game.of.Thrones.S01E01.720p.HDTV.x264-CTU").unwrap();
        assert_eq!(m.title.as_deref(), Some("Game of Thrones"));
        assert_eq!(m.content_type, ContentType::Episode);
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(1));
    }

    #[test]
    fn test_episode_two_digit() {
        let m = parse("Breaking.Bad.S05E16.Felina.720p.BluRay.x264").unwrap();
        assert_eq!(m.title.as_deref(), Some("Breaking Bad"));
        assert_eq!(m.season, Some(5));
        assert_eq!(m.episode, Some(16));
        assert_eq!(m.content_type, ContentType::Episode);
    }

    #[test]
    fn test_episode_lowercase() {
        let m = parse("the.office.s02e05.hdtv.x264").unwrap();
        assert_eq!(m.title.as_deref(), Some("the office"));
        assert_eq!(m.season, Some(2));
        assert_eq!(m.episode, Some(5));
    }

    #[test]
    fn test_episode_no_dots() {
        let m = parse("The Walking Dead S05E03 720p HDTV x264-ASAP[ettv]").unwrap();
        assert_eq!(m.title.as_deref(), Some("The Walking Dead"));
        assert_eq!(m.season, Some(5));
        assert_eq!(m.episode, Some(3));
        assert_eq!(m.content_type, ContentType::Episode);
    }

    #[test]
    fn test_episode_with_episode_name() {
        let m = parse("Stranger.Things.S04E09.The.Piggyback.2160p.NF.WEB-DL.DDP5.1.Atmos.DV.HDR.H.265-FLUX").unwrap();
        assert_eq!(m.title.as_deref(), Some("Stranger Things"));
        assert_eq!(m.season, Some(4));
        assert_eq!(m.episode, Some(9));
    }

    #[test]
    fn test_episode_x_format() {
        let m = parse("Friends.1x01.The.One.Where.Monica.Gets.a.Roommate.DVDRip").unwrap();
        assert_eq!(m.title.as_deref(), Some("Friends"));
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(1));
        assert_eq!(m.content_type, ContentType::Episode);
    }

    #[test]
    fn test_episode_x_format_double_digit() {
        let m = parse("Seinfeld.4x15.720p.HDTV").unwrap();
        assert_eq!(m.title.as_deref(), Some("Seinfeld"));
        assert_eq!(m.season, Some(4));
        assert_eq!(m.episode, Some(15));
    }

    #[test]
    fn test_episode_multi_episode() {
        let m = parse("The.100.S03E15E16.720p.HDTV.x264").unwrap();
        assert_eq!(m.title.as_deref(), Some("The 100"));
        assert_eq!(m.season, Some(3));
        // Takes first episode number
        assert_eq!(m.episode, Some(15));
        assert_eq!(m.content_type, ContentType::Episode);
    }

    #[test]
    fn test_episode_spelled_out() {
        let m = parse("House Season 3 Episode 12 720p").unwrap();
        assert_eq!(m.title.as_deref(), Some("House"));
        assert_eq!(m.season, Some(3));
        assert_eq!(m.episode, Some(12));
        assert_eq!(m.content_type, ContentType::Episode);
    }

    #[test]
    fn test_episode_three_digit_episode() {
        let m = parse("One.Piece.S01E100.1080p.WEB-DL").unwrap();
        assert_eq!(m.title.as_deref(), Some("One Piece"));
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(100));
    }

    #[test]
    fn test_episode_with_year_in_title() {
        let m = parse("9-1-1.Lone.Star.S04E01.720p.WEB.H264-CAKES").unwrap();
        assert_eq!(m.title.as_deref(), Some("9-1-1 Lone Star"));
        assert_eq!(m.season, Some(4));
        assert_eq!(m.episode, Some(1));
    }

    // =====================================================================
    // Season pack extraction (S## without E##)
    // =====================================================================

    #[test]
    fn test_season_pack_s_format() {
        let m = parse("Breaking.Bad.S05.1080p.BluRay.x264").unwrap();
        assert_eq!(m.title.as_deref(), Some("Breaking Bad"));
        assert_eq!(m.season, Some(5));
        assert_eq!(m.episode, None);
        assert_eq!(m.content_type, ContentType::Season);
    }

    #[test]
    fn test_season_pack_complete() {
        let m = parse("Game.of.Thrones.S01.COMPLETE.720p.BluRay").unwrap();
        assert_eq!(m.title.as_deref(), Some("Game of Thrones"));
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, None);
        assert_eq!(m.content_type, ContentType::Season);
    }

    #[test]
    fn test_season_pack_spelled_out() {
        let m = parse("The Office Season 3 720p BluRay").unwrap();
        assert_eq!(m.title.as_deref(), Some("The Office"));
        assert_eq!(m.season, Some(3));
        assert_eq!(m.episode, None);
        assert_eq!(m.content_type, ContentType::Season);
    }

    #[test]
    fn test_season_pack_with_spaces() {
        let m = parse("Stranger Things S04 2160p NF WEB-DL DDP5.1 DV HDR H.265").unwrap();
        assert_eq!(m.title.as_deref(), Some("Stranger Things"));
        assert_eq!(m.season, Some(4));
        assert_eq!(m.episode, None);
        assert_eq!(m.content_type, ContentType::Season);
    }

    // =====================================================================
    // Edge cases
    // =====================================================================

    #[test]
    fn test_empty_string_returns_none() {
        assert!(parse("").is_none());
    }

    #[test]
    fn test_only_noise_returns_none() {
        assert!(parse("1080p.BluRay.x264").is_none());
    }

    #[test]
    fn test_title_with_numbers() {
        let m = parse("2001.A.Space.Odyssey.1968.1080p.BluRay").unwrap();
        // The year 2001 would be consumed as a year pattern, but the title
        // should still be parseable
        assert!(m.title.is_some());
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_title_with_apostrophe() {
        let m = parse("Schindler's.List.1993.1080p.BluRay.x264").unwrap();
        assert_eq!(m.title.as_deref(), Some("Schindler's List"));
    }

    #[test]
    fn test_title_with_colon() {
        let m = parse("Star.Wars.Episode.IV.A.New.Hope.1977.1080p").unwrap();
        assert!(m.title.as_deref().unwrap().starts_with("Star Wars"));
    }

    #[test]
    fn test_bracket_group_at_end() {
        let m = parse("The.Mandalorian.S02E08.720p.WEB-DL.x264-GalaxyTV[TGx]").unwrap();
        assert_eq!(m.title.as_deref(), Some("The Mandalorian"));
        assert_eq!(m.season, Some(2));
        assert_eq!(m.episode, Some(8));
    }

    #[test]
    fn test_webrip_source() {
        let m = parse("Loki.S01E01.Glorious.Purpose.1080p.DSNP.WEBRip.DDP5.1.Atmos.x264-MZABI").unwrap();
        assert_eq!(m.title.as_deref(), Some("Loki"));
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(1));
    }

    #[test]
    fn test_dual_audio() {
        let m = parse("Spirited.Away.2001.1080p.BluRay.x264.DTS-FGT").unwrap();
        assert_eq!(m.title.as_deref(), Some("Spirited Away"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_4k_content() {
        let m = parse("Dune.Part.Two.2024.2160p.WEB-DL.DDP5.1.Atmos.DV.x265-FLUX").unwrap();
        assert_eq!(m.title.as_deref(), Some("Dune Part Two"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_remux() {
        let m = parse("Oppenheimer.2023.2160p.UHD.BluRay.REMUX.DV.HDR.HEVC.TrueHD.7.1.Atmos").unwrap();
        assert_eq!(m.title.as_deref(), Some("Oppenheimer"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    // =====================================================================
    // Content type classification
    // =====================================================================

    #[test]
    fn test_classify_movie() {
        let m = parse("The.Shawshank.Redemption.1994.1080p.BluRay").unwrap();
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_classify_episode() {
        let m = parse("The.Boys.S03E01.1080p.AMZN.WEB-DL").unwrap();
        assert_eq!(m.content_type, ContentType::Episode);
    }

    #[test]
    fn test_classify_season() {
        let m = parse("The.Boys.S03.1080p.AMZN.WEB-DL").unwrap();
        assert_eq!(m.content_type, ContentType::Season);
    }

    // =====================================================================
    // Content type display
    // =====================================================================

    #[test]
    fn test_content_type_display() {
        assert_eq!(ContentType::Unknown.to_string(), "unknown");
        assert_eq!(ContentType::Episode.to_string(), "episode");
        assert_eq!(ContentType::Season.to_string(), "season");
    }

    // =====================================================================
    // File name parsing (with various separators)
    // =====================================================================

    #[test]
    fn test_mixed_separators() {
        let m = parse("Mr_Robot_S01E01_1080p.BluRay-GROUP").unwrap();
        assert_eq!(m.title.as_deref(), Some("Mr Robot"));
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(1));
    }

    #[test]
    fn test_hyphenated_title() {
        let m = parse("Spider-Man.Across.the.Spider-Verse.2023.1080p.WEB-DL").unwrap();
        // Hyphens within the title area should be preserved
        assert!(m.title.as_deref().unwrap().contains("Spider"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    // =====================================================================
    // parse() on real-world torrent filenames from DMM hashlists
    // =====================================================================

    #[test]
    fn test_real_world_movie() {
        let m = parse("Furiosa.A.Mad.Max.Saga.2024.2160p.MA.WEB-DL.DDP5.1.Atmos.H.265-FLUX").unwrap();
        assert_eq!(m.title.as_deref(), Some("Furiosa A Mad Max Saga"));
        assert_eq!(m.content_type, ContentType::Unknown);
    }

    #[test]
    fn test_real_world_episode() {
        let m = parse("Shogun.2024.S01E10.A.Dream.of.a.Dream.2160p.DSNP.WEB-DL.DDP5.1.H.265-NTb").unwrap();
        assert_eq!(m.title.as_deref(), Some("Shogun"));
        assert_eq!(m.season, Some(1));
        assert_eq!(m.episode, Some(10));
    }

    #[test]
    fn test_real_world_season_pack() {
        let m = parse("House.of.the.Dragon.S02.1080p.MAX.WEB-DL.DDP5.1.Atmos.H.264-FLUX").unwrap();
        assert_eq!(m.title.as_deref(), Some("House of the Dragon"));
        assert_eq!(m.season, Some(2));
        assert_eq!(m.episode, None);
        assert_eq!(m.content_type, ContentType::Season);
    }

    #[test]
    fn test_real_world_anime() {
        let m = parse("Demon.Slayer.Kimetsu.no.Yaiba.S04E01.1080p.WEB.H.264-VARYG").unwrap();
        assert_eq!(m.title.as_deref(), Some("Demon Slayer Kimetsu no Yaiba"));
        assert_eq!(m.season, Some(4));
        assert_eq!(m.episode, Some(1));
    }

    // =====================================================================
    // Title cleaning edge cases
    // =====================================================================

    #[test]
    fn test_clean_title_trims_punctuation() {
        assert_eq!(clean_title_normalized("  -The Matrix- ").as_deref(), Some("The Matrix"));
    }

    #[test]
    fn test_clean_title_normalized_spaces() {
        assert_eq!(clean_title_normalized("The Matrix").as_deref(), Some("The Matrix"));
    }

    #[test]
    fn test_clean_title_normalized_already_clean() {
        assert_eq!(clean_title_normalized("The Dark Knight").as_deref(), Some("The Dark Knight"));
    }

    #[test]
    fn test_clean_title_empty_returns_none() {
        assert!(clean_title_normalized("").is_none());
    }

    #[test]
    fn test_clean_title_single_char_returns_none() {
        assert!(clean_title_normalized("X").is_none());
    }

    #[test]
    fn test_clean_title_strips_trailing_year() {
        assert_eq!(clean_title_normalized("The Matrix 1999").as_deref(), Some("The Matrix"));
    }

    #[test]
    fn test_clean_title_preserves_year_in_middle() {
        // "2001 A Space Odyssey" — 2001 is at the start, should be kept
        assert_eq!(
            clean_title_normalized("2001 A Space Odyssey").as_deref(),
            Some("2001 A Space Odyssey")
        );
    }

    // =====================================================================
    // Season/episode extraction edge cases
    // =====================================================================

    #[test]
    fn test_se_extract_standard() {
        let se = extract_season_episode("Something.S01E02.rest").unwrap();
        assert_eq!(se.season, 1);
        assert_eq!(se.episode, Some(2));
    }

    #[test]
    fn test_se_extract_no_match() {
        assert!(extract_season_episode("Just.A.Movie.2024").is_none());
    }

    #[test]
    fn test_se_extract_season_only() {
        let se = extract_season_episode("Show.S03.COMPLETE").unwrap();
        assert_eq!(se.season, 3);
        assert_eq!(se.episode, None);
    }

    #[test]
    fn test_se_extract_x_format() {
        let se = extract_season_episode("Show.2x05.DVDRip").unwrap();
        assert_eq!(se.season, 2);
        assert_eq!(se.episode, Some(5));
    }

    #[test]
    fn test_se_extract_spelled_out_full() {
        let se = extract_season_episode("Show Season 2 Episode 5 720p").unwrap();
        assert_eq!(se.season, 2);
        assert_eq!(se.episode, Some(5));
    }

    #[test]
    fn test_se_extract_spelled_out_season_only() {
        let se = extract_season_episode("Show Season 2 720p").unwrap();
        assert_eq!(se.season, 2);
        assert_eq!(se.episode, None);
    }
}
