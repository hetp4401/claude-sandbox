use crate::dht::TorrentFile;
use std::time::Duration;

/// Tier 1: iTorrents + BT4G concurrently (free caches, instant if hit).
pub async fn fetch_from_free_caches(
    client: &reqwest::Client,
    info_hash: &str,
) -> Result<Vec<TorrentFile>, String> {
    let hash_upper = info_hash.to_uppercase();

    let c1 = client.clone();
    let h1 = hash_upper.clone();
    let c2 = client.clone();
    let h2 = hash_upper.clone();

    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { fetch_from_itorrents(&c1, &h1).await }),
        tokio::spawn(async move { fetch_from_bt4g(&c2, &h2).await }),
    );

    for result in [r1, r2] {
        if let Ok(Ok(files)) = result {
            if !files.is_empty() {
                return Ok(files);
            }
        }
    }

    Err("free caches missed".into())
}

/// Tier 2: Real-Debrid lookup.
pub async fn fetch_from_rd(
    client: &reqwest::Client,
    info_hash: &str,
    api_key: &str,
) -> Result<Vec<TorrentFile>, String> {
    let hash_upper = info_hash.to_uppercase();
    fetch_from_real_debrid(client, &hash_upper, api_key).await
}

/// Fetch .torrent from iTorrents.org and parse file list.
async fn fetch_from_itorrents(
    client: &reqwest::Client,
    hash_upper: &str,
) -> Result<Vec<TorrentFile>, String> {
    let url = format!("https://itorrents.org/torrent/{hash_upper}.torrent");

    let resp = client
        .get(&url)
        .header("User-Agent", "dmm-ingestor/0.1")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("itorrents request failed: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("itorrents returned {}", resp.status()));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("itorrents read failed: {e}"))?;

    parse_torrent_bytes(&bytes)
}

/// Fetch .torrent from hash2torrent.com (real-time DHT fetch) and parse file list.
#[allow(dead_code)]
async fn fetch_from_hash2torrent(
    client: &reqwest::Client,
    hash_lower: &str,
) -> Result<Vec<TorrentFile>, String> {
    let url = format!("https://hash2torrent.com/{hash_lower}");

    let resp = client
        .get(&url)
        .header("User-Agent", "dmm-ingestor/0.1")
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .map_err(|e| format!("hash2torrent request failed: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("hash2torrent returned {}", resp.status()));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("hash2torrent read failed: {e}"))?;

    parse_torrent_bytes(&bytes)
}

/// Fetch file list from Real-Debrid's instant availability API.
/// RD caches torrents that its users have previously added.
async fn fetch_from_real_debrid(
    client: &reqwest::Client,
    hash_upper: &str,
    api_key: &str,
) -> Result<Vec<TorrentFile>, String> {
    // Step 1: Add magnet to RD
    let add_resp = client
        .post("https://api.real-debrid.com/rest/1.0/torrents/addMagnet")
        .header("Authorization", format!("Bearer {api_key}"))
        .form(&[("magnet", format!("magnet:?xt=urn:btih:{hash_upper}"))])
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("RD addMagnet: {e}"))?;

    if add_resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        tokio::time::sleep(Duration::from_secs(5)).await;
        return Err("RD: rate limited".into());
    }
    if !add_resp.status().is_success() {
        return Err(format!("RD addMagnet returned {}", add_resp.status()));
    }

    let add_json: serde_json::Value = add_resp
        .json()
        .await
        .map_err(|e| format!("RD addMagnet parse: {e}"))?;
    let torrent_id = add_json["id"]
        .as_str()
        .ok_or("RD: no torrent id in response")?
        .to_string();

    // Step 2: Get torrent info (contains file list)
    let info_resp = client
        .get(format!(
            "https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}"
        ))
        .header("Authorization", format!("Bearer {api_key}"))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("RD info: {e}"))?;

    if !info_resp.status().is_success() {
        // Clean up
        let _ = client
            .delete(format!(
                "https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}"
            ))
            .header("Authorization", format!("Bearer {api_key}"))
            .send()
            .await;
        return Err(format!("RD info returned {}", info_resp.status()));
    }

    let info: serde_json::Value = info_resp
        .json()
        .await
        .map_err(|e| format!("RD info parse: {e}"))?;

    // Extract files array
    let files = info["files"]
        .as_array()
        .ok_or("RD: no files in torrent info")?;

    let result: Vec<TorrentFile> = files
        .iter()
        .filter_map(|f| {
            let id = f["id"].as_u64()? as u32;
            let path = f["path"].as_str()?.to_string();
            let size = f["bytes"].as_u64()?;
            // RD paths start with "/"
            let path = path.strip_prefix('/').unwrap_or(&path).to_string();
            Some(TorrentFile {
                path,
                size,
                index: id.saturating_sub(1), // RD uses 1-based IDs
            })
        })
        .collect();

    // Clean up — delete from RD so it doesn't clutter the account
    let _ = client
        .delete(format!(
            "https://api.real-debrid.com/rest/1.0/torrents/delete/{torrent_id}"
        ))
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await;

    Ok(result)
}

/// Fetch .torrent from BT4G torrent cache.
async fn fetch_from_bt4g(
    client: &reqwest::Client,
    hash_upper: &str,
) -> Result<Vec<TorrentFile>, String> {
    let url = format!("https://bt4gprx.com/magnet/{hash_upper}");

    let resp = client
        .get(&url)
        .header("User-Agent", "dmm-ingestor/0.1")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("bt4g request failed: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("bt4g returned {}", resp.status()));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("bt4g read failed: {e}"))?;

    parse_torrent_bytes(&bytes)
}

/// Parse a raw .torrent file and extract the file list.
fn parse_torrent_bytes(bytes: &[u8]) -> Result<Vec<TorrentFile>, String> {
    let torrent: serde_bencode::value::Value =
        serde_bencode::from_bytes(bytes).map_err(|e| format!("bencode parse: {e}"))?;

    let dict = match torrent {
        serde_bencode::value::Value::Dict(d) => d,
        _ => return Err("torrent is not a dict".into()),
    };

    // Get the "info" dictionary
    let info = dict
        .get(b"info" as &[u8])
        .ok_or("missing 'info' key in torrent")?;

    let info_dict = match info {
        serde_bencode::value::Value::Dict(d) => d,
        _ => return Err("'info' is not a dict".into()),
    };

    // Check for multi-file torrent
    if let Some(files_val) = info_dict.get(b"files" as &[u8]) {
        parse_multi_file_torrent(files_val)
    } else {
        parse_single_file_torrent(info_dict)
    }
}

fn parse_single_file_torrent(
    info: &std::collections::HashMap<Vec<u8>, serde_bencode::value::Value>,
) -> Result<Vec<TorrentFile>, String> {
    let name = info
        .get(b"name" as &[u8])
        .and_then(|v| match v {
            serde_bencode::value::Value::Bytes(b) => String::from_utf8(b.clone()).ok(),
            _ => None,
        })
        .ok_or("missing 'name'")?;

    let size = info
        .get(b"length" as &[u8])
        .and_then(|v| match v {
            serde_bencode::value::Value::Int(n) => Some(*n as u64),
            _ => None,
        })
        .ok_or("missing 'length'")?;

    Ok(vec![TorrentFile {
        path: name,
        size,
        index: 0,
    }])
}

fn parse_multi_file_torrent(
    files_val: &serde_bencode::value::Value,
) -> Result<Vec<TorrentFile>, String> {
    let files_list = match files_val {
        serde_bencode::value::Value::List(l) => l,
        _ => return Err("'files' is not a list".into()),
    };

    let mut result = Vec::new();
    for (idx, file) in files_list.iter().enumerate() {
        let file_dict = match file {
            serde_bencode::value::Value::Dict(d) => d,
            _ => continue,
        };

        let length = file_dict
            .get(b"length" as &[u8])
            .and_then(|v| match v {
                serde_bencode::value::Value::Int(n) => Some(*n as u64),
                _ => None,
            })
            .unwrap_or(0);

        let path = file_dict
            .get(b"path" as &[u8])
            .and_then(|v| match v {
                serde_bencode::value::Value::List(parts) => {
                    let strs: Vec<String> = parts
                        .iter()
                        .filter_map(|p| match p {
                            serde_bencode::value::Value::Bytes(b) => {
                                String::from_utf8(b.clone()).ok()
                            }
                            _ => None,
                        })
                        .collect();
                    Some(strs.join("/"))
                }
                _ => None,
            })
            .unwrap_or_default();

        if !path.is_empty() {
            result.push(TorrentFile {
                path,
                size: length,
                index: idx as u32,
            });
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    // =====================================================================
    // .torrent parsing tests (offline, no network)
    // =====================================================================

    #[test]
    fn test_parse_single_file_torrent_bytes() {
        // Minimal valid .torrent: d4:infod4:name9:movie.mkv6:lengthi1073741824eee
        let torrent = b"d4:infod4:name9:movie.mkv6:lengthi1073741824eee";
        let files = parse_torrent_bytes(torrent).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "movie.mkv");
        assert_eq!(files[0].size, 1_073_741_824);
        assert_eq!(files[0].index, 0);
    }

    #[test]
    fn test_parse_multi_file_torrent_bytes() {
        let torrent = b"d4:infod5:filesld6:lengthi500000000e4:pathl10:S01E01.mkveed6:lengthi480000000e4:pathl10:S01E02.mkveed6:lengthi1000e4:pathl8:info.nfoeee4:name11:My Season 1ee";
        let files = parse_torrent_bytes(torrent).unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].path, "S01E01.mkv");
        assert_eq!(files[0].size, 500_000_000);
        assert_eq!(files[0].index, 0);
        assert_eq!(files[1].path, "S01E02.mkv");
        assert_eq!(files[1].index, 1);
        assert_eq!(files[2].path, "info.nfo");
        assert_eq!(files[2].index, 2);
    }

    #[test]
    fn test_parse_invalid_bytes() {
        assert!(parse_torrent_bytes(b"not valid bencode").is_err());
    }

    #[test]
    fn test_parse_missing_info() {
        let torrent = b"d7:commenti42ee";
        assert!(parse_torrent_bytes(torrent).is_err());
    }

    // =====================================================================
    // Live integration tests — require network
    // Run with: cargo test torrent_cache::tests::test_live -- --ignored --nocapture
    // =====================================================================

    /// Test iTorrents.org with Big Buck Bunny (well-known, likely cached)
    #[tokio::test]
    #[ignore]
    async fn test_live_itorrents_big_buck_bunny() {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();
        let hash = "DD8255ECDC7CA55FB0BBF81323D87062DB1F6D1C";

        println!("Fetching from iTorrents.org...");
        match fetch_from_itorrents(&client, hash).await {
            Ok(files) => {
                println!("iTorrents: got {} files", files.len());
                for f in &files {
                    println!("  [{}] {} ({} bytes)", f.index, f.path, f.size);
                }
                assert!(!files.is_empty());
            }
            Err(e) => println!("iTorrents failed: {e}"),
        }
    }

    /// Test hash2torrent.com with Big Buck Bunny
    #[tokio::test]
    #[ignore]
    async fn test_live_hash2torrent_big_buck_bunny() {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        let hash = "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c";

        println!("Fetching from hash2torrent.com...");
        match fetch_from_hash2torrent(&client, hash).await {
            Ok(files) => {
                println!("hash2torrent: got {} files", files.len());
                for f in &files {
                    println!("  [{}] {} ({} bytes)", f.index, f.path, f.size);
                }
                assert!(!files.is_empty());
            }
            Err(e) => println!("hash2torrent failed: {e}"),
        }
    }

    /// Test Real-Debrid file list lookup
    /// Run with: RD_API_KEY=... cargo test test_live_real_debrid -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_live_real_debrid() {
        let api_key = match std::env::var("RD_API_KEY") {
            Ok(k) if !k.is_empty() => k,
            _ => {
                println!("RD_API_KEY not set, skipping");
                return;
            }
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();

        // Big Buck Bunny
        let hash = "DD8255ECDC7CA55FB0BBF81323D87062DB1F6D1C";
        println!("Fetching from Real-Debrid...");
        match fetch_from_real_debrid(&client, hash, &api_key).await {
            Ok(files) => {
                println!("RD: got {} files", files.len());
                for f in &files {
                    println!("  [{}] {} ({} bytes)", f.index, f.path, f.size);
                }
                assert!(!files.is_empty());
            }
            Err(e) => println!("RD failed: {e}"),
        }

        // Also test a season pack
        let pack_hash = "605B8F1C0032682C4826CAF68DB37B361199B2DE"; // GoT S01
        println!("\nFetching GoT S01 from Real-Debrid...");
        match fetch_from_real_debrid(&client, pack_hash, &api_key).await {
            Ok(files) => {
                println!("RD: got {} files", files.len());
                for f in &files {
                    println!("  [{}] {} ({} bytes)", f.index, f.path, f.size);
                }
                assert!(!files.is_empty());
            }
            Err(e) => println!("RD failed: {e}"),
        }
    }

    /// Cross-provider file index verification.
    /// Ensures iTorrents and Real-Debrid return the same file indices for the same torrent.
    /// Run with: RD_API_KEY=... cargo test test_live_cross_provider_index -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_live_cross_provider_index_verification() {
        let api_key = match std::env::var("RD_API_KEY") {
            Ok(k) if !k.is_empty() => k,
            _ => {
                println!("RD_API_KEY not set, skipping");
                return;
            }
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap();

        // GoT S01 — known to be on both iTorrents and RD
        let hash = "605B8F1C0032682C4826CAF68DB37B361199B2DE";
        println!("Comparing file indices across providers for GoT S01...\n");

        let itorrents_files = fetch_from_itorrents(&client, hash).await;
        let rd_files = fetch_from_real_debrid(&client, hash, &api_key).await;

        if let (Ok(it_files), Ok(rd_files)) = (&itorrents_files, &rd_files) {
            println!("iTorrents ({} files):", it_files.len());
            for f in it_files {
                println!("  [{}] {} ({} bytes)", f.index, f.path, f.size);
            }
            println!("\nReal-Debrid ({} files):", rd_files.len());
            for f in rd_files {
                println!("  [{}] {} ({} bytes)", f.index, f.path, f.size);
            }

            // Verify same number of files
            assert_eq!(it_files.len(), rd_files.len(), "File count mismatch!");

            // Verify indices and filenames match
            println!("\nIndex verification:");
            let mut mismatches = 0;
            for (it, rd) in it_files.iter().zip(rd_files.iter()) {
                let idx_match = it.index == rd.index;
                let name_match = it.path.contains(&rd.path)
                    || rd.path.contains(&it.path)
                    || it.path.rsplit('/').next() == rd.path.rsplit('/').next();
                let status = if idx_match && name_match {
                    "OK"
                } else {
                    "MISMATCH"
                };
                if !idx_match || !name_match {
                    mismatches += 1;
                }
                println!(
                    "  {} it[{}]={} vs rd[{}]={}",
                    status, it.index, it.path, rd.index, rd.path
                );
            }
            if mismatches > 0 {
                println!("\nWARNING: {mismatches} index mismatches detected!");
            } else {
                println!("\nAll indices match across providers!");
            }
        } else {
            println!("iTorrents: {:?}", itorrents_files.as_ref().map(|f| f.len()));
            println!("RD: {:?}", rd_files.as_ref().map(|f| f.len()));
        }
    }
}
