use crate::dht::TorrentFile;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

/// qBittorrent Web API client for resolving torrent file lists.
/// This matches the approach used by knightcrawler: add magnet, wait for
/// metadata, read file list, then delete the torrent.
#[derive(Clone)]
pub struct QbitClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct QbitTorrentContent {
    /// File index within the torrent
    index: u32,
    /// File name/path
    name: String,
    /// File size in bytes
    size: u64,
}

#[derive(Debug, Deserialize)]
struct QbitTorrentInfo {
    /// State of the torrent: "metaDL" while fetching metadata, then transitions
    state: String,
}

#[derive(Debug)]
pub enum QbitError {
    ApiError(String),
    Timeout,
    MetadataFailed(String),
}

impl std::fmt::Display for QbitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QbitError::ApiError(s) => write!(f, "qBittorrent API error: {s}"),
            QbitError::Timeout => write!(f, "metadata resolution timed out"),
            QbitError::MetadataFailed(s) => write!(f, "metadata failed: {s}"),
        }
    }
}

impl QbitClient {
    pub fn new(base_url: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    /// Login to qBittorrent. Default credentials for linuxserver image.
    pub async fn login(&self) -> Result<(), QbitError> {
        // linuxserver/qbittorrent disables auth by default in newer versions
        // Try login with default creds, ignore failures (auth may be disabled)
        let _ = self
            .client
            .post(format!("{}/api/v2/auth/login", self.base_url))
            .form(&[("username", "admin"), ("password", "adminadmin")])
            .send()
            .await;
        Ok(())
    }

    /// Fetch the file list for a torrent by info hash.
    /// 1. Add magnet to qBittorrent
    /// 2. Poll until metadata is resolved (file list available)
    /// 3. Read file list
    /// 4. Delete the torrent (no data downloaded)
    pub async fn fetch_torrent_files(
        &self,
        info_hash: &str,
        timeout: Duration,
    ) -> Result<Vec<TorrentFile>, QbitError> {
        let hash_lower = info_hash.to_lowercase();
        // Include popular public trackers to speed up peer discovery
        let trackers = [
            "udp://tracker.opentrackr.org:1337/announce",
            "udp://open.stealth.si:80/announce",
            "udp://tracker.torrent.eu.org:451/announce",
            "udp://open.demonii.com:1337/announce",
            "udp://tracker.openbittorrent.com:6969/announce",
            "udp://exodus.desync.com:6969/announce",
        ];
        let tracker_params: String = trackers
            .iter()
            .map(|t| format!("&tr={}", urlencoding::encode(t)))
            .collect();
        let magnet = format!("magnet:?xt=urn:btih:{hash_lower}{tracker_params}");

        // Add the magnet link
        self.add_magnet(&magnet).await?;

        // Poll for metadata resolution
        let result = self.wait_for_metadata(&hash_lower, timeout).await;

        // Always clean up — delete the torrent whether we succeeded or not
        let files = match result {
            Ok(()) => self.get_torrent_files(&hash_lower).await,
            Err(e) => {
                let _ = self.delete_torrent(&hash_lower).await;
                return Err(e);
            }
        };

        let _ = self.delete_torrent(&hash_lower).await;
        files
    }

    async fn add_magnet(&self, magnet: &str) -> Result<(), QbitError> {
        // Must NOT use stopped=true — qBittorrent needs to connect to peers to fetch metadata.
        // Download limit is set to 1 byte/s to avoid actually downloading content.
        let resp = self
            .client
            .post(format!("{}/api/v2/torrents/add", self.base_url))
            .multipart(
                reqwest::multipart::Form::new()
                    .text("urls", magnet.to_string())
                    .text("dlLimit", "1")
                    .text("upLimit", "1"),
            )
            .send()
            .await
            .map_err(|e| QbitError::ApiError(format!("add magnet: {e}")))?;

        if !resp.status().is_success() {
            return Err(QbitError::ApiError(format!(
                "add magnet returned {}",
                resp.status()
            )));
        }
        Ok(())
    }

    async fn wait_for_metadata(
        &self,
        hash: &str,
        timeout: Duration,
    ) -> Result<(), QbitError> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(500);

        loop {
            if start.elapsed() > timeout {
                return Err(QbitError::Timeout);
            }

            // Check if torrent has metadata by trying to get its contents
            let url = format!(
                "{}/api/v2/torrents/files?hash={hash}",
                self.base_url
            );
            let resp = self
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| QbitError::ApiError(format!("get files: {e}")))?;

            if resp.status().is_success() {
                let text = resp
                    .text()
                    .await
                    .map_err(|e| QbitError::ApiError(format!("read body: {e}")))?;

                // qBittorrent returns empty array or error while metadata is pending
                if let Ok(files) = serde_json::from_str::<Vec<QbitTorrentContent>>(&text) {
                    if !files.is_empty() {
                        return Ok(());
                    }
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn get_torrent_files(&self, hash: &str) -> Result<Vec<TorrentFile>, QbitError> {
        let url = format!(
            "{}/api/v2/torrents/files?hash={hash}",
            self.base_url
        );

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| QbitError::ApiError(format!("get files: {e}")))?;

        if !resp.status().is_success() {
            return Err(QbitError::ApiError(format!(
                "get files returned {}",
                resp.status()
            )));
        }

        let contents: Vec<QbitTorrentContent> = resp
            .json()
            .await
            .map_err(|e| QbitError::ApiError(format!("parse files: {e}")))?;

        Ok(contents
            .into_iter()
            .map(|c| TorrentFile {
                path: c.name,
                size: c.size,
                index: c.index,
            })
            .collect())
    }

    async fn delete_torrent(&self, hash: &str) -> Result<(), QbitError> {
        self.client
            .post(format!("{}/api/v2/torrents/delete", self.base_url))
            .form(&[("hashes", hash), ("deleteFiles", "true")])
            .send()
            .await
            .map_err(|e| QbitError::ApiError(format!("delete: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magnet_format() {
        let hash = "DD8255ECDC7CA55FB0BBF81323D87062DB1F6D1C";
        let magnet = format!("magnet:?xt=urn:btih:{}", hash.to_lowercase());
        assert!(magnet.starts_with("magnet:?xt=urn:btih:dd8255"));
        assert_eq!(magnet.len(), 20 + 40); // "magnet:?xt=urn:btih:" + 40 hex chars
    }

    #[test]
    fn test_qbit_content_deserialization() {
        let json = r#"[
            {"index": 0, "name": "Season 1/S01E01.mkv", "size": 500000000, "priority": 1, "progress": 0, "is_seed": false, "piece_range": [0, 100], "availability": 1.0},
            {"index": 1, "name": "Season 1/S01E02.mkv", "size": 480000000, "priority": 1, "progress": 0, "is_seed": false, "piece_range": [101, 200], "availability": 1.0}
        ]"#;
        let contents: Vec<QbitTorrentContent> = serde_json::from_str(json).unwrap();
        assert_eq!(contents.len(), 2);
        assert_eq!(contents[0].index, 0);
        assert_eq!(contents[0].name, "Season 1/S01E01.mkv");
        assert_eq!(contents[0].size, 500000000);
        assert_eq!(contents[1].index, 1);
    }

    #[test]
    fn test_qbit_content_to_torrent_file() {
        let content = QbitTorrentContent {
            index: 3,
            name: "Show.S01E04.720p.mkv".into(),
            size: 750_000_000,
        };
        let tf = TorrentFile {
            path: content.name.clone(),
            size: content.size,
            index: content.index,
        };
        assert_eq!(tf.index, 3);
        assert_eq!(tf.path, "Show.S01E04.720p.mkv");
        assert_eq!(tf.size, 750_000_000);
    }

    #[test]
    fn test_qbit_error_display() {
        assert_eq!(
            QbitError::Timeout.to_string(),
            "metadata resolution timed out"
        );
        assert_eq!(
            QbitError::ApiError("test".into()).to_string(),
            "qBittorrent API error: test"
        );
    }
}
