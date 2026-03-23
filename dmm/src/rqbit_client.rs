use crate::dht::TorrentFile;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Persistent rqbit session that can resolve multiple magnets.
/// Creates one DHT client and reuses it across all lookups.
pub struct RqbitResolver {
    session: Arc<librqbit::Session>,
}

impl RqbitResolver {
    /// Create a new resolver with a persistent session.
    pub async fn new() -> Result<Self, String> {
        let tmp_dir = std::env::temp_dir().join("rqbit-dmm");
        let _ = std::fs::create_dir_all(&tmp_dir);

        let session = librqbit::Session::new_with_opts(
            tmp_dir,
            librqbit::SessionOptions {
                disable_dht: false,
                disable_dht_persistence: true,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("rqbit session: {e}"))?;

        Ok(Self { session })
    }

    /// Fetch torrent file list using librqbit.
    /// Uses list_only mode — fetches metadata via BEP 9, no data downloaded.
    pub async fn fetch_torrent_files(
        &self,
        info_hash: &str,
        timeout: Duration,
    ) -> Result<Vec<TorrentFile>, String> {
        let hash_lower = info_hash.to_lowercase();
        let trackers = [
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://open.stealth.si:80/announce",
            "udp://tracker.openbittorrent.com:6969/announce",
            "udp://exodus.desync.com:6969/announce",
        ];
        let tracker_params: String = trackers
            .iter()
            .map(|t| format!("&tr={}", urlencoding::encode(t)))
            .collect();
        let magnet = format!("magnet:?xt=urn:btih:{hash_lower}{tracker_params}");

        let add_result = tokio::time::timeout(timeout, async {
            self.session
                .add_torrent(
                    librqbit::AddTorrent::from_url(&magnet),
                    Some(librqbit::AddTorrentOptions {
                        list_only: true,
                        output_folder: Some("/tmp/rqbit-dl".to_string()),
                        ..Default::default()
                    }),
                )
                .await
        })
        .await
        .map_err(|_| "rqbit: timed out".to_string())?
        .map_err(|e| format!("rqbit: {e}"))?;

        let info = match add_result {
            librqbit::AddTorrentResponse::ListOnly(resp) => resp.info,
            _ => return Err("rqbit: unexpected response type".into()),
        };

        let mut files = Vec::new();
        if let Ok(details) = info.iter_file_details() {
            for (idx, fd) in details.enumerate() {
                files.push(TorrentFile {
                    path: fd.filename.to_string().unwrap_or_default(),
                    size: fd.len,
                    index: idx as u32,
                });
            }
        }

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run with: cargo test rqbit_client::tests::test_rqbit_benchmark -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_rqbit_benchmark() {
        let cases = vec![
            ("dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c", "Big Buck Bunny (control)"),
            ("605b8f1c0032682c4826caf68db37b361199b2de", "Game of Thrones S01"),
            ("abdedc438f40419535b56d983e4d7c9294087613", "Umbrella Academy S01"),
            ("c78059ca630629fd65fa8fb789bb8719ae33b59e", "12 Monkeys S01"),
            ("6a32fa68e8d22685606e8e0e875a2259588a1d23", "Fallout S01"),
            ("9308486678f1dc29a41471eb29753aab43e4fdd9", "Love Death and Robots S01"),
            ("99774dd5d95b9e86f4f789f36af1730e14658c3e", "Arifureta S02"),
            ("d91d484a971cebdf5b4cadb74899989795d4368a", "Nagatoro S01"),
        ];

        println!("\n=== librqbit Benchmark (shared session) ===\n");

        let resolver = match RqbitResolver::new().await {
            Ok(r) => r,
            Err(e) => {
                println!("Failed to create rqbit session: {e}");
                return;
            }
        };

        // Give DHT a moment to bootstrap
        println!("  Waiting 5s for DHT bootstrap...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        let mut ok_count = 0u32;
        let mut total_ms = 0u128;

        for (hash, name) in &cases {
            print!("  {:<40} ... ", name);
            let start = std::time::Instant::now();
            match resolver.fetch_torrent_files(hash, Duration::from_secs(90)).await {
                Ok(files) => {
                    let elapsed = start.elapsed().as_millis();
                    let video_count = files.iter().filter(|f| {
                        let l = f.path.to_lowercase();
                        l.ends_with(".mkv") || l.ends_with(".mp4") || l.ends_with(".avi")
                    }).count();
                    println!("OK  {}ms  {} files ({} video)", elapsed, files.len(), video_count);
                    ok_count += 1;
                    total_ms += elapsed;
                }
                Err(e) => {
                    println!("FAIL  {}ms  {}", start.elapsed().as_millis(), e);
                }
            }
        }

        println!("\n=== Summary ===");
        println!("Resolved: {}/{}", ok_count, cases.len());
        if ok_count > 0 {
            println!("Avg time: {}ms", total_ms / ok_count as u128);
        }
    }
}
