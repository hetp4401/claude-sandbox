use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Result of a successful IMDb resolution.
#[derive(Debug, Clone)]
pub struct ImdbResult {
    pub imdb_id: String,
    /// "movie" or "series"
    pub content_type: String,
}

/// In-memory cached IMDb resolver using Cinemeta + IMDb suggest APIs.
#[derive(Clone)]
pub struct ImdbResolver {
    client: reqwest::Client,
    /// Cache keyed by "title_lower|year_or_0" → Some(result) or None (miss).
    cache: Arc<RwLock<HashMap<String, Option<ImdbResult>>>>,
}

fn cache_key(title: &str, year: Option<i32>, type_hint: Option<&str>) -> String {
    format!(
        "{}|{}|{}",
        title.to_lowercase().trim(),
        year.unwrap_or(0),
        type_hint.unwrap_or("any")
    )
}

/// Simplify a name for comparison, matching name-to-imdb's simplifyName.
/// Lowercases, strips trailing parenthetical, replaces & with "and",
/// removes non-alphanumeric chars, collapses whitespace.
pub fn simplify_name(name: &str) -> String {
    let s = name.to_lowercase();
    // Remove trailing parenthetical like "(2024)"
    let re_paren = regex::Regex::new(r"\([^(]+\)$").unwrap();
    let s = re_paren.replace(&s, "").to_string();
    let s = s.replace('&', "and");
    // Keep only alphanumeric and spaces
    let re_nonalpha = regex::Regex::new(r"[^0-9a-z ]+").unwrap();
    let s = re_nonalpha.replace_all(&s, " ").to_string();
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Levenshtein distance between two strings.
fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();
    if a_len == 0 { return b_len; }
    if b_len == 0 { return a_len; }

    let mut prev: Vec<usize> = (0..=b_len).collect();
    let mut curr = vec![0; b_len + 1];

    for (i, ca) in a.chars().enumerate() {
        curr[0] = i + 1;
        for (j, cb) in b.chars().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr[j + 1] = (prev[j + 1] + 1)
                .min(curr[j] + 1)
                .min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b_len]
}

/// Similarity score between two names (0.0 to 1.0) using Levenshtein distance.
/// Matches name-to-imdb's nameSimilar approach.
pub fn name_similarity(a: &str, b: &str) -> f64 {
    let sa = simplify_name(a);
    let sb = simplify_name(b);
    if sa.is_empty() || sb.is_empty() {
        return 0.0;
    }
    let max_len = sa.len().max(sb.len());
    let dist = levenshtein(&sa, &sb);
    1.0 - (dist as f64 / max_len as f64)
}

/// Check if one simplified name starts/ends with the other (nameAlmostSimilar).
pub fn name_almost_similar(a: &str, b: &str) -> bool {
    let sa = simplify_name(a);
    let sb = simplify_name(b);
    sa.starts_with(&sb) || sa.ends_with(&sb) || sb.starts_with(&sa) || sb.ends_with(&sa)
}

/// Year is "similar" if within ±1, or if either is missing (matches name-to-imdb).
pub fn year_similar(parsed_year: Option<i32>, alt_year: Option<i32>) -> bool {
    match (parsed_year, alt_year) {
        (None, _) | (_, None) => true,
        (Some(a), Some(b)) => (a - b).abs() <= 1,
    }
}

/// Check if an IMDb suggest entry matches a given type hint.
fn qid_matches_type(qid: &Option<String>, type_hint: &str) -> bool {
    match type_hint {
        "movie" => matches!(
            qid.as_deref(),
            Some("movie") | Some("feature") | Some("short") | Some("video") | Some("tvMovie") | Some("tvSpecial")
        ),
        "series" => matches!(
            qid.as_deref(),
            Some("tvSeries") | Some("tvMiniSeries")
        ),
        _ => true,
    }
}

impl ImdbResolver {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// GET with 3 retries on 429/5xx, backoff = 1000ms * attempt.
    async fn get_with_backoff(&self, url: &str) -> Option<reqwest::Response> {
        for attempt in 1..=3 {
            let resp = self
                .client
                .get(url)
                .header("User-Agent", "dmm-ingestor/0.1")
                .send()
                .await
                .ok()?;

            let status = resp.status();
            if status.is_success() {
                return Some(resp);
            }
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                tokio::time::sleep(std::time::Duration::from_millis(1000 * attempt)).await;
                continue;
            }
            return None; // 4xx other than 429
        }
        None
    }

    /// Resolve a title + optional year + optional type hint to an IMDb result.
    ///
    /// `type_hint`: If `Some("series")`, searches Cinemeta series first and filters
    /// IMDb suggest results to TV types. If `Some("movie")`, searches movie first.
    /// If `None`, tries both in order.
    ///
    /// This matches the behavior of the `name-to-imdb` npm library used by Torrentio:
    /// the caller infers the type from parsed metadata (season/episode presence) and
    /// passes it as a hint to constrain the search.
    pub async fn resolve(
        &self,
        title: &str,
        year: Option<i32>,
        type_hint: Option<&str>,
    ) -> Option<ImdbResult> {
        let key = cache_key(title, year, type_hint);

        // Check cache
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(&key) {
                return cached.clone();
            }
        }

        // Determine Cinemeta search order based on type hint
        let search_types: Vec<&str> = match type_hint {
            Some("series") => vec!["series", "movie"],
            Some("movie") => vec!["movie", "series"],
            _ => vec!["movie", "series"],
        };

        // Try Cinemeta
        for media_type in &search_types {
            if let Some(id) = self.search_cinemeta(title, year, media_type).await {
                let result = ImdbResult {
                    imdb_id: id,
                    content_type: media_type.to_string(),
                };
                self.cache.write().await.insert(key, Some(result.clone()));
                return Some(result);
            }
        }

        // Try IMDb suggest API (with type filtering)
        if let Some(result) = self.search_imdb_suggest(title, year, type_hint).await {
            self.cache.write().await.insert(key, Some(result.clone()));
            return Some(result);
        }

        // If we had a type hint and got no results, retry without the hint
        // (fallback: better to get a wrong-type match than no match)
        if type_hint.is_some() {
            if let Some(result) = self.search_imdb_suggest(title, year, None).await {
                // Override the content type based on the hint — the hint (from parsed metadata)
                // is more reliable than the API for type determination
                let result = ImdbResult {
                    content_type: type_hint.unwrap().to_string(),
                    ..result
                };
                self.cache.write().await.insert(key, Some(result.clone()));
                return Some(result);
            }
        }

        // Cache the miss
        self.cache.write().await.insert(key, None);
        None
    }

    /// Query Cinemeta catalog search.
    /// Endpoint: `https://v3-cinemeta.strem.io/catalog/{type}/top/search={query}.json`
    async fn search_cinemeta(
        &self,
        title: &str,
        year: Option<i32>,
        media_type: &str,
    ) -> Option<String> {
        let encoded = urlencoding::encode(title);
        let url = format!(
            "https://v3-cinemeta.strem.io/catalog/{media_type}/top/search={encoded}.json"
        );

        let resp = self.get_with_backoff(&url).await?;
        let body = resp.json::<CinemetaResponse>().await.ok()?;
        pick_cinemeta_match(&body.metas, title, year)
    }

    /// Query the IMDb suggestion/autocomplete API.
    /// Uses the same endpoint as name-to-imdb: `https://sg.media-imdb.com/suggests/{first_char}/{query}.json`
    /// Falls back to v3 endpoint if first fails.
    async fn search_imdb_suggest(
        &self,
        title: &str,
        year: Option<i32>,
        type_hint: Option<&str>,
    ) -> Option<ImdbResult> {
        // First try with year (like name-to-imdb does)
        if let Some(y) = year {
            let query = format!("{title} {y}");
            if let Some(result) = self.query_imdb_suggest(&query, title, year, type_hint).await {
                return Some(result);
            }
        }

        // Retry without year (loose search, like name-to-imdb's retry)
        self.query_imdb_suggest(title, title, year, type_hint).await
    }

    async fn query_imdb_suggest(
        &self,
        search_term: &str,
        match_title: &str,
        year: Option<i32>,
        type_hint: Option<&str>,
    ) -> Option<ImdbResult> {
        let encoded = urlencoding::encode(search_term);
        let first_char = search_term
            .chars()
            .next()
            .unwrap_or('a')
            .to_lowercase()
            .next()
            .unwrap_or('a');
        let url = format!(
            "https://v3.sg.media-imdb.com/suggestion/{first_char}/{encoded}.json"
        );

        let resp = self.get_with_backoff(&url).await?;
        let body = resp.json::<ImdbSuggestResponse>().await.ok()?;
        pick_imdb_suggest_match(&body.d, match_title, year, type_hint)
    }

    pub async fn cache_len(&self) -> usize {
        self.cache.read().await.len()
    }
}

// =========================================================================
// Cinemeta types & matching
// =========================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct CinemetaResponse {
    #[serde(default)]
    pub metas: Vec<CinemetaMeta>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CinemetaMeta {
    /// IMDb ID like "tt1234567"
    pub id: String,
    /// Display name
    #[serde(default)]
    pub name: String,
    /// Release year (sometimes a string like "2024", sometimes absent)
    #[serde(default)]
    pub year: Option<String>,
    #[serde(default, rename = "releaseInfo")]
    pub release_info: Option<String>,
}

/// Pick the best Cinemeta match from a list of metas using Levenshtein similarity
/// and year proximity, matching name-to-imdb's approach.
pub fn pick_cinemeta_match(
    metas: &[CinemetaMeta],
    title: &str,
    year: Option<i32>,
) -> Option<String> {
    if metas.is_empty() {
        return None;
    }

    const SIMILARITY_GOAL: f64 = 0.6;

    let mut pick: Option<(f64, &CinemetaMeta)> = None;
    let mut second_best: Option<&CinemetaMeta> = None;

    for meta in metas {
        if !meta.id.starts_with("tt") {
            continue;
        }

        // Year check
        let meta_year = parse_year_from_meta(meta);
        if !year_similar(year, meta_year) {
            continue;
        }

        let similarity = name_similarity(title, &meta.name);

        if similarity > SIMILARITY_GOAL {
            if pick.is_none() || similarity > pick.unwrap().0 {
                pick = Some((similarity, meta));
            }
        }

        if second_best.is_none() && name_almost_similar(title, &meta.name) {
            second_best = Some(meta);
        }
    }

    // Prefer second_best if pick doesn't "almost match" (same as name-to-imdb)
    if let (Some((_, pick_meta)), Some(sb)) = (&pick, second_best) {
        if !name_almost_similar(title, &pick_meta.name) {
            return Some(sb.id.clone());
        }
    }

    pick.map(|(_, m)| m.id.clone())
        .or_else(|| second_best.map(|m| m.id.clone()))
}

/// Extract a year integer from a CinemetaMeta.
fn parse_year_from_meta(meta: &CinemetaMeta) -> Option<i32> {
    // Try the year field first
    if let Some(ref y) = meta.year {
        if let Some(parsed) = parse_first_year(y) {
            return Some(parsed);
        }
    }
    // Try releaseInfo (e.g. "2019-2024")
    if let Some(ref ri) = meta.release_info {
        return parse_first_year(ri);
    }
    None
}

/// Parse the first 4-digit year from a string like "2024" or "2019-2024".
pub fn parse_first_year(s: &str) -> Option<i32> {
    let re = regex::Regex::new(r"((?:19|20)\d{2})").unwrap();
    re.captures(s)
        .and_then(|caps| caps[1].parse::<i32>().ok())
}

// =========================================================================
// IMDb suggest types & matching
// =========================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct ImdbSuggestResponse {
    #[serde(default)]
    pub d: Vec<ImdbSuggestEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImdbSuggestEntry {
    /// IMDb ID like "tt1234567"
    pub id: String,
    /// Title
    #[serde(default)]
    pub l: String,
    /// Year
    pub y: Option<i32>,
    /// Type category (e.g. "feature", "TV series")
    #[serde(default)]
    pub qid: Option<String>,
}

/// Normalize IMDb suggest `qid` field to "movie" or "series".
fn normalize_qid(qid: &Option<String>) -> String {
    match qid.as_deref() {
        Some("movie") | Some("feature") | Some("short") | Some("video") => "movie".into(),
        Some("tvSeries") | Some("tvMiniSeries") | Some("tvMovie") | Some("tvSpecial") => {
            "series".into()
        }
        _ => "movie".into(), // default to movie if unknown
    }
}

/// Pick the best IMDb suggest match, closely following name-to-imdb's matchSimilar logic:
/// 1. Filter to tt-prefixed entries that match the type hint
/// 2. Check year similarity (±1)
/// 3. Score by Levenshtein similarity (threshold 0.6)
/// 4. Fallback to "almost similar" (prefix/suffix) match
/// 5. Last resort: first valid result
pub fn pick_imdb_suggest_match(
    entries: &[ImdbSuggestEntry],
    title: &str,
    year: Option<i32>,
    type_hint: Option<&str>,
) -> Option<ImdbResult> {
    if entries.is_empty() {
        return None;
    }

    const SIMILARITY_GOAL: f64 = 0.6;

    let mut pick: Option<(f64, &ImdbSuggestEntry)> = None;
    let mut second_best: Option<&ImdbSuggestEntry> = None;
    let mut first_result: Option<&ImdbSuggestEntry> = None;

    for entry in entries {
        if !entry.id.starts_with("tt") {
            continue;
        }

        // Type filtering: if we have a hint, filter to matching types
        if let Some(hint) = type_hint {
            if !qid_matches_type(&entry.qid, hint) {
                continue;
            }
        }

        // Year check (±1, or pass if either is missing)
        if !year_similar(year, entry.y) {
            continue;
        }

        // Levenshtein similarity scoring
        let similarity = name_similarity(title, &entry.l);

        if similarity > SIMILARITY_GOAL {
            if pick.is_none() || similarity > pick.unwrap().0 {
                pick = Some((similarity, entry));
            }
        }

        // Almost similar (prefix/suffix) as secondary
        if second_best.is_none() && name_almost_similar(title, &entry.l) {
            second_best = Some(entry);
        }

        // First valid result as last resort
        if first_result.is_none() {
            first_result = Some(entry);
        }
    }

    // If pick exists but doesn't "almost match", prefer second_best (name-to-imdb logic)
    if let (Some((_, pick_entry)), Some(sb)) = (&pick, second_best) {
        if !name_almost_similar(title, &pick_entry.l) {
            return Some(ImdbResult {
                imdb_id: sb.id.clone(),
                content_type: normalize_qid(&sb.qid),
            });
        }
    }

    let chosen = pick.map(|(_, e)| e).or(second_best).or(first_result)?;

    Some(ImdbResult {
        imdb_id: chosen.id.clone(),
        content_type: normalize_qid(&chosen.qid),
    })
}

// =========================================================================
// Tests
// =========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // === cache_key ===

    #[test]
    fn test_cache_key_with_year() {
        assert_eq!(cache_key("The Matrix", Some(1999), None), "the matrix|1999|any");
    }

    #[test]
    fn test_cache_key_without_year() {
        assert_eq!(cache_key("Inception", None, None), "inception|0|any");
    }

    #[test]
    fn test_cache_key_trims() {
        assert_eq!(cache_key("  Dune  ", Some(2021), None), "dune|2021|any");
    }

    #[test]
    fn test_cache_key_case_insensitive() {
        assert_eq!(
            cache_key("THE MATRIX", Some(1999), None),
            cache_key("the matrix", Some(1999), None)
        );
    }

    #[test]
    fn test_cache_key_with_type_hint() {
        assert_ne!(
            cache_key("Bel-Air", Some(2022), Some("series")),
            cache_key("Bel-Air", Some(2022), Some("movie"))
        );
    }

    // === simplify_name ===

    #[test]
    fn test_simplify_name_basic() {
        assert_eq!(simplify_name("The Matrix"), "the matrix");
    }

    #[test]
    fn test_simplify_name_strips_special_chars() {
        assert_eq!(simplify_name("Bel-Air"), "bel air");
    }

    #[test]
    fn test_simplify_name_ampersand() {
        assert_eq!(simplify_name("Law & Order"), "law and order");
    }

    #[test]
    fn test_simplify_name_trailing_parens() {
        assert_eq!(simplify_name("Dune (2021)"), "dune");
    }

    // === name_similarity ===

    #[test]
    fn test_name_similarity_exact() {
        assert!(name_similarity("The Matrix", "The Matrix") > 0.99);
    }

    #[test]
    fn test_name_similarity_case_insensitive() {
        assert!(name_similarity("the matrix", "THE MATRIX") > 0.99);
    }

    #[test]
    fn test_name_similarity_different() {
        assert!(name_similarity("The Matrix", "Inception") < 0.5);
    }

    #[test]
    fn test_name_similarity_close() {
        assert!(name_similarity("Bel-Air", "Bel-Air") > 0.99);
    }

    // === year_similar ===

    #[test]
    fn test_year_similar_exact() {
        assert!(year_similar(Some(1999), Some(1999)));
    }

    #[test]
    fn test_year_similar_off_by_one() {
        assert!(year_similar(Some(1999), Some(2000)));
        assert!(year_similar(Some(1999), Some(1998)));
    }

    #[test]
    fn test_year_similar_missing() {
        assert!(year_similar(None, Some(2000)));
        assert!(year_similar(Some(1999), None));
        assert!(year_similar(None, None));
    }

    #[test]
    fn test_year_similar_too_far() {
        assert!(!year_similar(Some(1999), Some(2005)));
    }

    // === qid_matches_type ===

    #[test]
    fn test_qid_matches_movie() {
        assert!(qid_matches_type(&Some("movie".into()), "movie"));
        assert!(qid_matches_type(&Some("feature".into()), "movie"));
        assert!(!qid_matches_type(&Some("tvSeries".into()), "movie"));
    }

    #[test]
    fn test_qid_matches_series() {
        assert!(qid_matches_type(&Some("tvSeries".into()), "series"));
        assert!(qid_matches_type(&Some("tvMiniSeries".into()), "series"));
        assert!(!qid_matches_type(&Some("movie".into()), "series"));
        assert!(!qid_matches_type(&Some("feature".into()), "series"));
    }

    // === parse_first_year ===

    #[test]
    fn test_parse_first_year_simple() {
        assert_eq!(parse_first_year("2024"), Some(2024));
    }

    #[test]
    fn test_parse_first_year_range() {
        assert_eq!(parse_first_year("2019-2024"), Some(2019));
    }

    #[test]
    fn test_parse_first_year_empty() {
        assert_eq!(parse_first_year(""), None);
    }

    #[test]
    fn test_parse_first_year_no_year() {
        assert_eq!(parse_first_year("abc"), None);
    }

    #[test]
    fn test_parse_first_year_old() {
        assert_eq!(parse_first_year("1994"), Some(1994));
    }

    // === Cinemeta response parsing ===

    #[test]
    fn test_parse_cinemeta_response() {
        let json = r#"{
            "metas": [
                {"id": "tt0133093", "name": "The Matrix", "year": "1999", "type": "movie"},
                {"id": "tt10838180", "name": "The Matrix Resurrections", "year": "2021", "type": "movie"}
            ]
        }"#;
        let resp: CinemetaResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.metas.len(), 2);
        assert_eq!(resp.metas[0].id, "tt0133093");
        assert_eq!(resp.metas[0].name, "The Matrix");
    }

    #[test]
    fn test_parse_cinemeta_empty() {
        let json = r#"{"metas": []}"#;
        let resp: CinemetaResponse = serde_json::from_str(json).unwrap();
        assert!(resp.metas.is_empty());
    }

    #[test]
    fn test_parse_cinemeta_no_metas_field() {
        let json = r#"{}"#;
        let resp: CinemetaResponse = serde_json::from_str(json).unwrap();
        assert!(resp.metas.is_empty());
    }

    // === pick_cinemeta_match ===

    fn make_cinemeta(id: &str, name: &str, year: Option<&str>) -> CinemetaMeta {
        CinemetaMeta {
            id: id.into(),
            name: name.into(),
            year: year.map(|y| y.into()),
            release_info: None,
        }
    }

    #[test]
    fn test_cinemeta_match_exact_title_and_year() {
        let metas = vec![
            make_cinemeta("tt0133093", "The Matrix", Some("1999")),
            make_cinemeta("tt10838180", "The Matrix Resurrections", Some("2021")),
        ];
        let result = pick_cinemeta_match(&metas, "The Matrix", Some(1999));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_cinemeta_match_exact_title_no_year() {
        let metas = vec![
            make_cinemeta("tt0133093", "The Matrix", Some("1999")),
            make_cinemeta("tt10838180", "The Matrix Resurrections", Some("2021")),
        ];
        let result = pick_cinemeta_match(&metas, "The Matrix", None);
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_cinemeta_match_prefers_year_match() {
        let metas = vec![
            make_cinemeta("tt0111161", "The Shawshank Redemption", Some("1994")),
            make_cinemeta("tt9999999", "The Shawshank Redemption", Some("2024")),
        ];
        let result = pick_cinemeta_match(&metas, "The Shawshank Redemption", Some(1994));
        assert_eq!(result.as_deref(), Some("tt0111161"));
    }

    #[test]
    fn test_cinemeta_match_partial_title() {
        let metas = vec![make_cinemeta("tt0468569", "The Dark Knight", Some("2008"))];
        let result = pick_cinemeta_match(&metas, "Dark Knight", Some(2008));
        assert_eq!(result.as_deref(), Some("tt0468569"));
    }

    #[test]
    fn test_cinemeta_match_case_insensitive() {
        let metas = vec![make_cinemeta("tt0133093", "The Matrix", Some("1999"))];
        let result = pick_cinemeta_match(&metas, "the matrix", Some(1999));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_cinemeta_match_empty_list() {
        let result = pick_cinemeta_match(&[], "anything", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_cinemeta_match_no_title_match() {
        let metas = vec![make_cinemeta("tt0133093", "The Matrix", Some("1999"))];
        let result = pick_cinemeta_match(&metas, "Inception", Some(2010));
        assert!(result.is_none());
    }

    #[test]
    fn test_cinemeta_match_skips_non_tt_ids() {
        let metas = vec![
            CinemetaMeta {
                id: "kitsu:1234".into(),
                name: "The Matrix".into(),
                year: Some("1999".into()),
                release_info: None,
            },
            make_cinemeta("tt0133093", "The Matrix", Some("1999")),
        ];
        let result = pick_cinemeta_match(&metas, "The Matrix", Some(1999));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_cinemeta_match_release_info_year() {
        let metas = vec![CinemetaMeta {
            id: "tt0944947".into(),
            name: "Game of Thrones".into(),
            year: None,
            release_info: Some("2011-2019".into()),
        }];
        let result = pick_cinemeta_match(&metas, "Game of Thrones", Some(2011));
        assert_eq!(result.as_deref(), Some("tt0944947"));
    }

    #[test]
    fn test_cinemeta_match_year_off_by_one() {
        let metas = vec![make_cinemeta("tt0133093", "The Matrix", Some("1999"))];
        let result = pick_cinemeta_match(&metas, "The Matrix", Some(2000));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    // === IMDb suggest response parsing ===

    #[test]
    fn test_parse_imdb_suggest_response() {
        let json = r#"{
            "d": [
                {"id": "tt0133093", "l": "The Matrix", "y": 1999, "qid": "movie", "q": "feature"},
                {"id": "tt10838180", "l": "The Matrix Resurrections", "y": 2021, "qid": "movie"}
            ],
            "q": "the matrix",
            "v": 1
        }"#;
        let resp: ImdbSuggestResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.d.len(), 2);
        assert_eq!(resp.d[0].id, "tt0133093");
        assert_eq!(resp.d[0].l, "The Matrix");
        assert_eq!(resp.d[0].y, Some(1999));
    }

    #[test]
    fn test_parse_imdb_suggest_empty() {
        let json = r#"{"d": [], "q": "xxxx", "v": 1}"#;
        let resp: ImdbSuggestResponse = serde_json::from_str(json).unwrap();
        assert!(resp.d.is_empty());
    }

    #[test]
    fn test_parse_imdb_suggest_no_d_field() {
        let json = r#"{"q": "test", "v": 1}"#;
        let resp: ImdbSuggestResponse = serde_json::from_str(json).unwrap();
        assert!(resp.d.is_empty());
    }

    // === pick_imdb_suggest_match ===

    fn make_suggest(id: &str, title: &str, year: Option<i32>) -> ImdbSuggestEntry {
        ImdbSuggestEntry {
            id: id.into(),
            l: title.into(),
            y: year,
            qid: Some("movie".into()),
        }
    }

    #[test]
    fn test_suggest_match_exact() {
        let entries = vec![
            make_suggest("tt0133093", "The Matrix", Some(1999)),
            make_suggest("tt10838180", "The Matrix Resurrections", Some(2021)),
        ];
        let result = pick_imdb_suggest_match(&entries, "The Matrix", Some(1999), None);
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_no_year() {
        let entries = vec![make_suggest("tt0133093", "The Matrix", Some(1999))];
        let result = pick_imdb_suggest_match(&entries, "The Matrix", None, None);
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_case_insensitive() {
        let entries = vec![make_suggest("tt0133093", "The Matrix", Some(1999))];
        let result = pick_imdb_suggest_match(&entries, "the matrix", Some(1999), None);
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_empty() {
        let result = pick_imdb_suggest_match(&[], "anything", None, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_suggest_match_no_title_match() {
        let entries = vec![make_suggest("tt0133093", "The Matrix", Some(1999))];
        let result = pick_imdb_suggest_match(&entries, "Inception", Some(2010), None);
        assert!(result.is_none());
    }

    #[test]
    fn test_suggest_match_skips_non_tt() {
        let entries = vec![
            ImdbSuggestEntry {
                id: "nm0000206".into(), // person ID
                l: "Keanu Reeves".into(),
                y: None,
                qid: Some("name".into()),
            },
            make_suggest("tt0133093", "The Matrix", Some(1999)),
        ];
        let result = pick_imdb_suggest_match(&entries, "The Matrix", Some(1999), None);
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_prefers_year() {
        let entries = vec![
            make_suggest("tt0234215", "The Matrix Reloaded", Some(2003)),
            make_suggest("tt0133093", "The Matrix", Some(1999)),
        ];
        let result = pick_imdb_suggest_match(&entries, "The Matrix", Some(1999), None);
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_partial_title() {
        let entries = vec![make_suggest("tt0468569", "The Dark Knight", Some(2008))];
        let result = pick_imdb_suggest_match(&entries, "Dark Knight", Some(2008), None);
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0468569"));
    }

    // === normalize_qid ===

    #[test]
    fn test_normalize_qid_movie() {
        assert_eq!(normalize_qid(&Some("movie".into())), "movie");
        assert_eq!(normalize_qid(&Some("feature".into())), "movie");
        assert_eq!(normalize_qid(&Some("short".into())), "movie");
        assert_eq!(normalize_qid(&Some("video".into())), "movie");
    }

    #[test]
    fn test_normalize_qid_series() {
        assert_eq!(normalize_qid(&Some("tvSeries".into())), "series");
        assert_eq!(normalize_qid(&Some("tvMiniSeries".into())), "series");
        assert_eq!(normalize_qid(&Some("tvMovie".into())), "series");
        assert_eq!(normalize_qid(&Some("tvSpecial".into())), "series");
    }

    #[test]
    fn test_normalize_qid_unknown_defaults_movie() {
        assert_eq!(normalize_qid(&None), "movie");
        assert_eq!(normalize_qid(&Some("other".into())), "movie");
    }

    #[test]
    fn test_suggest_match_returns_series_type() {
        let entries = vec![ImdbSuggestEntry {
            id: "tt0944947".into(),
            l: "Game of Thrones".into(),
            y: Some(2011),
            qid: Some("tvSeries".into()),
        }];
        let result = pick_imdb_suggest_match(&entries, "Game of Thrones", Some(2011), None).unwrap();
        assert_eq!(result.imdb_id, "tt0944947");
        assert_eq!(result.content_type, "series");
    }

    #[test]
    fn test_suggest_match_returns_movie_type() {
        let entries = vec![ImdbSuggestEntry {
            id: "tt0133093".into(),
            l: "The Matrix".into(),
            y: Some(1999),
            qid: Some("movie".into()),
        }];
        let result = pick_imdb_suggest_match(&entries, "The Matrix", Some(1999), None).unwrap();
        assert_eq!(result.imdb_id, "tt0133093");
        assert_eq!(result.content_type, "movie");
    }

    // === Type hint filtering ===

    #[test]
    fn test_suggest_type_hint_filters_to_series() {
        let entries = vec![
            ImdbSuggestEntry {
                id: "tt1234567".into(),
                l: "Bel-Air".into(),
                y: Some(2022),
                qid: Some("movie".into()),
            },
            ImdbSuggestEntry {
                id: "tt13411094".into(),
                l: "Bel-Air".into(),
                y: Some(2022),
                qid: Some("tvSeries".into()),
            },
        ];
        let result = pick_imdb_suggest_match(&entries, "Bel-Air", Some(2022), Some("series")).unwrap();
        assert_eq!(result.imdb_id, "tt13411094");
        assert_eq!(result.content_type, "series");
    }

    #[test]
    fn test_suggest_type_hint_filters_to_movie() {
        let entries = vec![
            ImdbSuggestEntry {
                id: "tt13411094".into(),
                l: "Bel-Air".into(),
                y: Some(2022),
                qid: Some("tvSeries".into()),
            },
            ImdbSuggestEntry {
                id: "tt1234567".into(),
                l: "Bel-Air".into(),
                y: Some(2022),
                qid: Some("movie".into()),
            },
        ];
        let result = pick_imdb_suggest_match(&entries, "Bel-Air", Some(2022), Some("movie")).unwrap();
        assert_eq!(result.imdb_id, "tt1234567");
        assert_eq!(result.content_type, "movie");
    }

    // === ImdbResolver cache ===

    #[tokio::test]
    async fn test_resolver_cache_key_deduplication() {
        let resolver = ImdbResolver::new(reqwest::Client::new());

        // Pre-populate cache
        {
            let mut cache = resolver.cache.write().await;
            cache.insert(
                cache_key("The Matrix", Some(1999), None),
                Some(ImdbResult { imdb_id: "tt0133093".into(), content_type: "movie".into() }),
            );
            cache.insert(cache_key("Inception", Some(2010), None), None); // cached miss
        }

        // Cache hit
        let result = resolver.resolve("The Matrix", Some(1999), None).await;
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0133093"));

        // Cached miss
        let result = resolver.resolve("Inception", Some(2010), None).await;
        assert!(result.is_none());

        assert_eq!(resolver.cache_len().await, 2);
    }

    #[tokio::test]
    async fn test_resolver_cache_case_insensitive() {
        let resolver = ImdbResolver::new(reqwest::Client::new());

        {
            let mut cache = resolver.cache.write().await;
            cache.insert(
                cache_key("the matrix", Some(1999), None),
                Some(ImdbResult { imdb_id: "tt0133093".into(), content_type: "movie".into() }),
            );
        }

        // Different case should hit same cache entry
        let result = resolver.resolve("THE MATRIX", Some(1999), None).await;
        assert_eq!(result.as_ref().map(|r| r.imdb_id.as_str()), Some("tt0133093"));
    }
}
