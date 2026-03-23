use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory cached IMDb resolver using Cinemeta + IMDb suggest APIs.
#[derive(Clone)]
pub struct ImdbResolver {
    client: reqwest::Client,
    /// Cache keyed by "title_lower|year_or_0" → Some(imdb_id) or None (miss).
    cache: Arc<RwLock<HashMap<String, Option<String>>>>,
}

fn cache_key(title: &str, year: Option<i32>) -> String {
    format!("{}|{}", title.to_lowercase().trim(), year.unwrap_or(0))
}

impl ImdbResolver {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Resolve a title + optional year to an IMDb ID (e.g. "tt1234567").
    /// Returns cached result if available, otherwise queries Cinemeta then IMDb suggest.
    pub async fn resolve(&self, title: &str, year: Option<i32>) -> Option<String> {
        let key = cache_key(title, year);

        // Check cache
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(&key) {
                return cached.clone();
            }
        }

        // Try Cinemeta (movie first, then series)
        for media_type in &["movie", "series"] {
            if let Some(id) = self.search_cinemeta(title, year, media_type).await {
                self.cache.write().await.insert(key, Some(id.clone()));
                return Some(id);
            }
        }

        // Try IMDb suggest API
        if let Some(id) = self.search_imdb_suggest(title, year).await {
            self.cache.write().await.insert(key, Some(id.clone()));
            return Some(id);
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

        let resp = self
            .client
            .get(&url)
            .header("User-Agent", "dmm-ingestor/0.1")
            .send()
            .await
            .ok()?;

        if !resp.status().is_success() {
            return None;
        }

        let body = resp.json::<CinemetaResponse>().await.ok()?;
        pick_cinemeta_match(&body.metas, title, year)
    }

    /// Query the IMDb suggestion/autocomplete API.
    /// Endpoint: `https://v3.sg.media-imdb.com/suggestion/x/{query}.json`
    async fn search_imdb_suggest(&self, title: &str, year: Option<i32>) -> Option<String> {
        let query = if let Some(y) = year {
            format!("{title} {y}")
        } else {
            title.to_string()
        };
        let encoded = urlencoding::encode(&query);
        let url = format!("https://v3.sg.media-imdb.com/suggestion/x/{encoded}.json");

        let resp = self
            .client
            .get(&url)
            .header("User-Agent", "dmm-ingestor/0.1")
            .send()
            .await
            .ok()?;

        if !resp.status().is_success() {
            return None;
        }

        let body = resp.json::<ImdbSuggestResponse>().await.ok()?;
        pick_imdb_suggest_match(&body.d, title, year)
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

/// Pick the best Cinemeta match from a list of metas.
pub fn pick_cinemeta_match(
    metas: &[CinemetaMeta],
    title: &str,
    year: Option<i32>,
) -> Option<String> {
    if metas.is_empty() {
        return None;
    }

    let title_lower = title.to_lowercase();

    // Score each result
    let mut best: Option<(i32, &CinemetaMeta)> = None;
    for meta in metas {
        if !meta.id.starts_with("tt") {
            continue;
        }

        let mut score: i32 = 0;

        let meta_name_lower = meta.name.to_lowercase();
        if meta_name_lower == title_lower {
            score += 100;
        } else if meta_name_lower.contains(&title_lower) || title_lower.contains(&meta_name_lower)
        {
            score += 50;
        } else {
            continue; // Name doesn't match at all, skip
        }

        // Year matching
        if let Some(expected_year) = year {
            let meta_year = parse_year_from_meta(meta);
            if let Some(my) = meta_year {
                if my == expected_year {
                    score += 30;
                } else if (my - expected_year).abs() <= 1 {
                    score += 10;
                }
            }
        }

        if best.is_none() || score > best.unwrap().0 {
            best = Some((score, meta));
        }
    }

    best.map(|(_, m)| m.id.clone())
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

/// Pick the best IMDb suggest match.
pub fn pick_imdb_suggest_match(
    entries: &[ImdbSuggestEntry],
    title: &str,
    year: Option<i32>,
) -> Option<String> {
    if entries.is_empty() {
        return None;
    }

    let title_lower = title.to_lowercase();

    let mut best: Option<(i32, &ImdbSuggestEntry)> = None;
    for entry in entries {
        if !entry.id.starts_with("tt") {
            continue;
        }

        let mut score: i32 = 0;

        let entry_title_lower = entry.l.to_lowercase();
        if entry_title_lower == title_lower {
            score += 100;
        } else if entry_title_lower.contains(&title_lower)
            || title_lower.contains(&entry_title_lower)
        {
            score += 50;
        } else {
            continue;
        }

        if let Some(expected_year) = year {
            if let Some(ey) = entry.y {
                if ey == expected_year {
                    score += 30;
                } else if (ey - expected_year).abs() <= 1 {
                    score += 10;
                }
            }
        }

        if best.is_none() || score > best.unwrap().0 {
            best = Some((score, entry));
        }
    }

    best.map(|(_, e)| e.id.clone())
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
        assert_eq!(cache_key("The Matrix", Some(1999)), "the matrix|1999");
    }

    #[test]
    fn test_cache_key_without_year() {
        assert_eq!(cache_key("Inception", None), "inception|0");
    }

    #[test]
    fn test_cache_key_trims() {
        assert_eq!(cache_key("  Dune  ", Some(2021)), "dune|2021");
    }

    #[test]
    fn test_cache_key_case_insensitive() {
        assert_eq!(
            cache_key("THE MATRIX", Some(1999)),
            cache_key("the matrix", Some(1999))
        );
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
        // Without year, exact match on title wins
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
        // "Dark Knight" is contained in "The Dark Knight"
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
        // Year off by 1 still gives partial score
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
        let result = pick_imdb_suggest_match(&entries, "The Matrix", Some(1999));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_no_year() {
        let entries = vec![make_suggest("tt0133093", "The Matrix", Some(1999))];
        let result = pick_imdb_suggest_match(&entries, "The Matrix", None);
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_case_insensitive() {
        let entries = vec![make_suggest("tt0133093", "The Matrix", Some(1999))];
        let result = pick_imdb_suggest_match(&entries, "the matrix", Some(1999));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_empty() {
        let result = pick_imdb_suggest_match(&[], "anything", None);
        assert!(result.is_none());
    }

    #[test]
    fn test_suggest_match_no_title_match() {
        let entries = vec![make_suggest("tt0133093", "The Matrix", Some(1999))];
        let result = pick_imdb_suggest_match(&entries, "Inception", Some(2010));
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
        let result = pick_imdb_suggest_match(&entries, "The Matrix", Some(1999));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_prefers_year() {
        let entries = vec![
            make_suggest("tt0234215", "The Matrix Reloaded", Some(2003)),
            make_suggest("tt0133093", "The Matrix", Some(1999)),
        ];
        let result = pick_imdb_suggest_match(&entries, "The Matrix", Some(1999));
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }

    #[test]
    fn test_suggest_match_partial_title() {
        let entries = vec![make_suggest("tt0468569", "The Dark Knight", Some(2008))];
        let result = pick_imdb_suggest_match(&entries, "Dark Knight", Some(2008));
        assert_eq!(result.as_deref(), Some("tt0468569"));
    }

    // === ImdbResolver cache ===

    #[tokio::test]
    async fn test_resolver_cache_key_deduplication() {
        let resolver = ImdbResolver::new(reqwest::Client::new());

        // Pre-populate cache
        {
            let mut cache = resolver.cache.write().await;
            cache.insert(cache_key("The Matrix", Some(1999)), Some("tt0133093".into()));
            cache.insert(cache_key("Inception", Some(2010)), None); // cached miss
        }

        // Cache hit
        let result = resolver.resolve("The Matrix", Some(1999)).await;
        assert_eq!(result.as_deref(), Some("tt0133093"));

        // Cached miss
        let result = resolver.resolve("Inception", Some(2010)).await;
        assert!(result.is_none());

        assert_eq!(resolver.cache_len().await, 2);
    }

    #[tokio::test]
    async fn test_resolver_cache_case_insensitive() {
        let resolver = ImdbResolver::new(reqwest::Client::new());

        {
            let mut cache = resolver.cache.write().await;
            cache.insert(cache_key("the matrix", Some(1999)), Some("tt0133093".into()));
        }

        // Different case should hit same cache entry
        let result = resolver.resolve("THE MATRIX", Some(1999)).await;
        assert_eq!(result.as_deref(), Some("tt0133093"));
    }
}
