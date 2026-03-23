use crate::imdb_resolver::ImdbResolver;
use crate::ingestor::AppState;
use crate::log;
use std::collections::HashMap;

const BATCH_SIZE: i64 = 500;

/// Run the IMDb resolution pipeline.
/// Fetches parsed_metadata entries that have no imdb_mappings row,
/// groups by (title, year) to deduplicate lookups, resolves via
/// Cinemeta/IMDb suggest, and inserts results.
pub async fn run_imdb_pipeline(state: &AppState, resolver: &ImdbResolver) {
    let entries = match state.db.get_unmapped_parsed_entries(BATCH_SIZE).await {
        Ok(e) => e,
        Err(e) => {
            log!(state.logs, "[IMDB] Failed to query unmapped entries: {e}");
            return;
        }
    };

    if entries.is_empty() {
        return;
    }

    log!(
        state.logs,
        "[IMDB] Found {} entries to resolve",
        entries.len()
    );

    // Group hashes by (title_lower, year) to deduplicate API calls
    let mut groups: HashMap<(String, Option<i32>), Vec<String>> = HashMap::new();
    for entry in &entries {
        let key = (entry.title.to_lowercase(), entry.year);
        groups
            .entry(key)
            .or_default()
            .push(entry.hash.clone());
    }

    log!(
        state.logs,
        "[IMDB] {} unique (title, year) groups to look up",
        groups.len()
    );

    let mut resolved = 0u64;
    let mut missed = 0u64;

    for ((title, year), hashes) in &groups {
        match resolver.resolve(title, *year).await {
            Some(imdb_id) => {
                for hash in hashes {
                    if let Err(e) = state.db.upsert_imdb_mapping(hash, &imdb_id).await {
                        log!(
                            state.logs,
                            "[IMDB] Failed to insert mapping for {}: {e}",
                            &hash[..8.min(hash.len())]
                        );
                    }
                }
                resolved += hashes.len() as u64;
            }
            None => {
                missed += hashes.len() as u64;
            }
        }
    }

    log!(
        state.logs,
        "[IMDB] Resolved {resolved} entries, {missed} unresolved (cache size: {})",
        resolver.cache_len().await
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grouping_logic() {
        // Simulate the grouping logic
        let entries = vec![
            ("hash1", "The Matrix", Some(1999i32)),
            ("hash2", "the matrix", Some(1999)),
            ("hash3", "Inception", Some(2010)),
            ("hash4", "The Matrix", None),
        ];

        let mut groups: HashMap<(String, Option<i32>), Vec<String>> = HashMap::new();
        for (hash, title, year) in entries {
            let key = (title.to_lowercase(), year);
            groups.entry(key).or_default().push(hash.to_string());
        }

        // "The Matrix" + 1999 should have 2 hashes (case-insensitive grouping)
        assert_eq!(
            groups
                .get(&("the matrix".to_string(), Some(1999)))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            groups
                .get(&("inception".to_string(), Some(2010)))
                .unwrap()
                .len(),
            1
        );
        // "The Matrix" without year is a separate group
        assert_eq!(
            groups
                .get(&("the matrix".to_string(), None))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(groups.len(), 3);
    }
}
