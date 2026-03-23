use sha1::{Digest, Sha1};
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A file entry within a torrent.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentFile {
    /// Path/filename of this file within the torrent.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Index of this file in the torrent's file list (0-based).
    pub index: u32,
}

/// Errors that can occur during DHT/metadata operations.
#[derive(Debug)]
pub enum DhtError {
    NoPeersFound,
    MetadataFetchFailed(String),
    ParseFailed(String),
    Timeout,
}

impl std::fmt::Display for DhtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DhtError::NoPeersFound => write!(f, "no peers found via DHT"),
            DhtError::MetadataFetchFailed(s) => write!(f, "metadata fetch failed: {s}"),
            DhtError::ParseFailed(s) => write!(f, "metadata parse failed: {s}"),
            DhtError::Timeout => write!(f, "operation timed out"),
        }
    }
}

const PROTOCOL: &[u8] = b"\x13BitTorrent protocol";
const EXTENSION_BIT: u8 = 0x10; // Extension protocol (BEP 10)

/// Build the BitTorrent handshake message (68 bytes).
fn build_handshake(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> [u8; 68] {
    let mut buf = [0u8; 68];
    buf[..20].copy_from_slice(PROTOCOL);
    // Reserved bytes: set extension protocol bit
    buf[25] = EXTENSION_BIT;
    buf[28..48].copy_from_slice(info_hash);
    buf[48..68].copy_from_slice(peer_id);
    buf
}

/// Parse the info_hash from a 40-character hex string.
pub fn parse_info_hash(hex: &str) -> Result<[u8; 20], DhtError> {
    if hex.len() != 40 {
        return Err(DhtError::ParseFailed(format!(
            "info hash must be 40 hex chars, got {}",
            hex.len()
        )));
    }
    let mut hash = [0u8; 20];
    for i in 0..20 {
        hash[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| DhtError::ParseFailed("invalid hex in info hash".into()))?;
    }
    Ok(hash)
}

/// Bencode encode a simple dictionary for the extension handshake.
fn build_extension_handshake() -> Vec<u8> {
    // BEP 10 extension handshake: {m: {ut_metadata: 1}}
    let payload = b"d1:md11:ut_metadatai1eee";
    let mut msg = Vec::new();
    let len = (payload.len() + 2) as u32; // +2 for msg_id + ext_id
    msg.extend_from_slice(&len.to_be_bytes());
    msg.push(20); // Extended message ID
    msg.push(0); // Extension handshake ID
    msg.extend_from_slice(payload);
    msg
}

/// Build a BEP 9 metadata request message.
fn build_metadata_request(ut_metadata_id: u8, piece: u32) -> Vec<u8> {
    let payload = format!("d8:msg_typei0e5:piecei{piece}ee");
    let mut msg = Vec::new();
    let len = (payload.len() + 2) as u32;
    msg.extend_from_slice(&len.to_be_bytes());
    msg.push(20); // Extended message
    msg.push(ut_metadata_id);
    msg.extend_from_slice(payload.as_bytes());
    msg
}

/// Read a BitTorrent message from the stream (length-prefixed).
async fn read_message(stream: &mut TcpStream) -> Result<Vec<u8>, DhtError> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| DhtError::MetadataFetchFailed(format!("read length: {e}")))?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 {
        return Ok(vec![]);
    }
    if len > 1_048_576 {
        // 1MB max — metadata shouldn't be this big
        return Err(DhtError::MetadataFetchFailed("message too large".into()));
    }
    let mut buf = vec![0u8; len];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| DhtError::MetadataFetchFailed(format!("read body: {e}")))?;
    Ok(buf)
}

/// Extract ut_metadata ID and metadata_size from the extension handshake response.
fn parse_extension_handshake(data: &[u8]) -> Result<(u8, usize), DhtError> {
    // data starts after msg_id(20) and ext_id(0)
    // Find "ut_metadatai" followed by the number
    let s = std::str::from_utf8(data)
        .map_err(|_| DhtError::ParseFailed("extension handshake not utf8".into()))?;

    let ut_id = extract_bencode_int(s, "ut_metadata")
        .ok_or_else(|| DhtError::ParseFailed("no ut_metadata in extension handshake".into()))?
        as u8;

    let metadata_size = extract_bencode_int(s, "metadata_size")
        .ok_or_else(|| DhtError::ParseFailed("no metadata_size in extension handshake".into()))?
        as usize;

    Ok((ut_id, metadata_size))
}

/// Extract an integer value for a given key from a bencoded string.
/// Looks for pattern: `<key_len>:<key>i<number>e`
fn extract_bencode_int(data: &str, key: &str) -> Option<i64> {
    let pattern = format!("{}:{}", key.len(), key);
    let pos = data.find(&pattern)?;
    let after = &data[pos + pattern.len()..];
    if !after.starts_with('i') {
        return None;
    }
    let end = after.find('e')?;
    after[1..end].parse().ok()
}

/// Parse the torrent info dictionary to extract file entries.
/// Supports both single-file and multi-file torrents.
pub fn parse_torrent_files(info_bytes: &[u8]) -> Result<Vec<TorrentFile>, DhtError> {
    // Use serde_bencode to deserialize
    let info: serde_bencode::value::Value = serde_bencode::from_bytes(info_bytes)
        .map_err(|e| DhtError::ParseFailed(format!("bencode decode: {e}")))?;

    let dict = match info {
        serde_bencode::value::Value::Dict(d) => d,
        _ => return Err(DhtError::ParseFailed("info is not a dict".into())),
    };

    // Check for multi-file torrent (has "files" key)
    if let Some(files_val) = dict.get(b"files" as &[u8]) {
        parse_multi_file(files_val)
    } else {
        // Single-file torrent
        parse_single_file(&dict)
    }
}

fn parse_single_file(
    dict: &std::collections::HashMap<Vec<u8>, serde_bencode::value::Value>,
) -> Result<Vec<TorrentFile>, DhtError> {
    let name = dict
        .get(b"name" as &[u8])
        .and_then(|v| match v {
            serde_bencode::value::Value::Bytes(b) => String::from_utf8(b.clone()).ok(),
            _ => None,
        })
        .ok_or_else(|| DhtError::ParseFailed("missing 'name' in info dict".into()))?;

    let size = dict
        .get(b"length" as &[u8])
        .and_then(|v| match v {
            serde_bencode::value::Value::Int(n) => Some(*n as u64),
            _ => None,
        })
        .ok_or_else(|| DhtError::ParseFailed("missing 'length' in single-file torrent".into()))?;

    Ok(vec![TorrentFile {
        path: name,
        size,
        index: 0,
    }])
}

fn parse_multi_file(files_val: &serde_bencode::value::Value) -> Result<Vec<TorrentFile>, DhtError> {
    let files_list = match files_val {
        serde_bencode::value::Value::List(l) => l,
        _ => return Err(DhtError::ParseFailed("'files' is not a list".into())),
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

        let path_parts = file_dict
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

        if !path_parts.is_empty() {
            result.push(TorrentFile {
                path: path_parts,
                size: length,
                index: idx as u32,
            });
        }
    }

    Ok(result)
}

/// A shared, long-lived DHT client that maintains its routing table across lookups.
/// This is much more reliable than creating a new DHT client per request.
#[derive(Clone)]
pub struct SharedDht {
    dht: mainline::Dht,
}

impl SharedDht {
    /// Create and bootstrap a shared DHT client. Call once at startup.
    pub async fn new() -> Result<Self, DhtError> {
        let dht = mainline::Dht::client().map_err(|e| {
            DhtError::MetadataFetchFailed(format!("failed to create DHT client: {e}"))
        })?;

        let async_dht = dht.clone().as_async();
        let bootstrapped = tokio::time::timeout(Duration::from_secs(30), async {
            async_dht.bootstrapped().await
        })
        .await
        .map_err(|_| DhtError::Timeout)?;

        if !bootstrapped {
            return Err(DhtError::MetadataFetchFailed(
                "DHT bootstrap failed".into(),
            ));
        }

        Ok(Self { dht })
    }

    /// Fetch torrent metadata using this shared DHT client.
    pub async fn fetch_torrent_files(
        &self,
        info_hash_hex: &str,
        timeout: Duration,
    ) -> Result<Vec<TorrentFile>, DhtError> {
        let info_hash = parse_info_hash(info_hash_hex)?;
        let info_hash_id = mainline::Id::from_bytes(info_hash)
            .map_err(|e| DhtError::ParseFailed(format!("invalid info hash for DHT: {e}")))?;

        let async_dht = self.dht.clone().as_async();

        // Query peers using the long-lived routing table
        let mut peers: Vec<SocketAddrV4> = Vec::new();
        let mut get_peers = async_dht.get_peers(info_hash_id);

        let _ = tokio::time::timeout(Duration::from_secs(15), async {
            use futures_util::StreamExt;
            while let Some(batch) = get_peers.next().await {
                peers.extend(batch);
                if peers.len() >= 20 {
                    break;
                }
            }
        })
        .await;

        if peers.is_empty() {
            return Err(DhtError::NoPeersFound);
        }

        // Try connecting to peers and fetching metadata
        let peer_id = generate_peer_id();

        for peer_addr in peers.iter().take(10) {
            let addr = std::net::SocketAddr::V4(*peer_addr);
            match tokio::time::timeout(
                timeout,
                fetch_metadata_from_peer(addr, &info_hash, &peer_id),
            )
            .await
            {
                Ok(Ok(info_bytes)) => {
                    let mut hasher = Sha1::new();
                    hasher.update(&info_bytes);
                    let computed: [u8; 20] = hasher.finalize().into();
                    if computed != info_hash {
                        continue;
                    }
                    return parse_torrent_files(&info_bytes);
                }
                _ => continue,
            }
        }

        Err(DhtError::MetadataFetchFailed(
            "all peer connections failed".into(),
        ))
    }
}

/// Generate a random peer ID (-DM0100-<random>).
fn generate_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(b"-DM0100-");
    // Fill rest with pseudo-random bytes
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for i in 8..20 {
        id[i] = ((now >> ((i - 8) * 4)) & 0xFF) as u8;
    }
    id
}

/// Connect to a single peer and fetch the torrent metadata via BEP 9.
async fn fetch_metadata_from_peer(
    addr: std::net::SocketAddr,
    info_hash: &[u8; 20],
    peer_id: &[u8; 20],
) -> Result<Vec<u8>, DhtError> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|e| DhtError::MetadataFetchFailed(format!("connect: {e}")))?;

    // Send handshake
    let handshake = build_handshake(info_hash, peer_id);
    stream
        .write_all(&handshake)
        .await
        .map_err(|e| DhtError::MetadataFetchFailed(format!("write handshake: {e}")))?;

    // Read peer handshake
    let mut peer_handshake = [0u8; 68];
    stream
        .read_exact(&mut peer_handshake)
        .await
        .map_err(|e| DhtError::MetadataFetchFailed(format!("read handshake: {e}")))?;

    // Verify protocol
    if &peer_handshake[..20] != PROTOCOL {
        return Err(DhtError::MetadataFetchFailed("wrong protocol".into()));
    }

    // Check extension support
    if peer_handshake[25] & EXTENSION_BIT == 0 {
        return Err(DhtError::MetadataFetchFailed(
            "peer doesn't support extensions".into(),
        ));
    }

    // Send extension handshake
    let ext_handshake = build_extension_handshake();
    stream
        .write_all(&ext_handshake)
        .await
        .map_err(|e| DhtError::MetadataFetchFailed(format!("write ext handshake: {e}")))?;

    // Read messages until we get the extension handshake response
    let (ut_metadata_id, metadata_size) = loop {
        let msg = read_message(&mut stream).await?;
        if msg.is_empty() {
            continue; // Keep-alive
        }
        if msg[0] == 20 && msg.len() > 2 && msg[1] == 0 {
            // Extension handshake response
            break parse_extension_handshake(&msg[2..])?;
        }
    };

    if metadata_size == 0 || metadata_size > 10_485_760 {
        return Err(DhtError::MetadataFetchFailed(
            "invalid metadata_size".into(),
        ));
    }

    // Request all metadata pieces (16KB each)
    let piece_size = 16384;
    let num_pieces = (metadata_size + piece_size - 1) / piece_size;
    let mut metadata = vec![0u8; metadata_size];

    for piece in 0..num_pieces as u32 {
        let req = build_metadata_request(ut_metadata_id, piece);
        stream
            .write_all(&req)
            .await
            .map_err(|e| DhtError::MetadataFetchFailed(format!("write metadata req: {e}")))?;
    }

    // Collect metadata pieces
    let mut pieces_received = 0;
    while pieces_received < num_pieces {
        let msg = read_message(&mut stream).await?;
        if msg.is_empty() || msg[0] != 20 {
            continue;
        }
        if msg.len() < 3 || msg[1] != ut_metadata_id {
            continue;
        }

        // Parse the bencode header to find where the data starts
        let payload = &msg[2..];
        let payload_str = String::from_utf8_lossy(payload);

        // Look for msg_type = 1 (data response)
        if !payload_str.contains("8:msg_typei1e") {
            continue; // Reject or error, skip
        }

        // Extract piece number
        let piece_num = extract_bencode_int(&payload_str, "piece")
            .ok_or_else(|| DhtError::ParseFailed("no piece number in response".into()))?
            as usize;

        // Find the end of the bencode dict ("ee" marks the end)
        // The raw data follows immediately after
        let dict_end = find_bencode_dict_end(payload)
            .ok_or_else(|| DhtError::ParseFailed("can't find dict end in metadata response".into()))?;

        let piece_data = &payload[dict_end..];
        let offset = piece_num * piece_size;
        let end = (offset + piece_data.len()).min(metadata_size);
        let copy_len = end - offset;
        metadata[offset..offset + copy_len].copy_from_slice(&piece_data[..copy_len]);

        pieces_received += 1;
    }

    Ok(metadata)
}

/// Find the end of a bencoded dictionary in the given bytes.
/// Returns the index right after the closing 'e'.
fn find_bencode_dict_end(data: &[u8]) -> Option<usize> {
    if data.is_empty() || data[0] != b'd' {
        return None;
    }
    let mut depth = 0i32;
    let mut i = 0;
    while i < data.len() {
        match data[i] {
            b'd' | b'l' => {
                depth += 1;
                i += 1;
            }
            b'e' => {
                depth -= 1;
                i += 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            b'i' => {
                // Integer: skip to 'e'
                i += 1;
                while i < data.len() && data[i] != b'e' {
                    i += 1;
                }
                i += 1; // skip 'e'
            }
            b'0'..=b'9' => {
                // String: parse length, skip
                let start = i;
                while i < data.len() && data[i] != b':' {
                    i += 1;
                }
                let len_str = std::str::from_utf8(&data[start..i]).ok()?;
                let len: usize = len_str.parse().ok()?;
                i += 1; // skip ':'
                i += len; // skip string content
            }
            _ => {
                i += 1;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // =====================================================================
    // Info hash parsing
    // =====================================================================

    #[test]
    fn test_parse_info_hash_valid() {
        let hash = parse_info_hash("dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c").unwrap();
        assert_eq!(hash[0], 0xdd);
        assert_eq!(hash[1], 0x82);
        assert_eq!(hash[19], 0x1c);
    }

    #[test]
    fn test_parse_info_hash_uppercase() {
        let hash = parse_info_hash("DD8255ECDC7CA55FB0BBF81323D87062DB1F6D1C").unwrap();
        assert_eq!(hash[0], 0xdd);
    }

    #[test]
    fn test_parse_info_hash_wrong_length() {
        assert!(parse_info_hash("abc123").is_err());
    }

    #[test]
    fn test_parse_info_hash_invalid_hex() {
        assert!(parse_info_hash("zz8255ecdc7ca55fb0bbf81323d87062db1f6d1c").is_err());
    }

    // =====================================================================
    // Handshake building
    // =====================================================================

    #[test]
    fn test_build_handshake_length() {
        let info_hash = [0u8; 20];
        let peer_id = [1u8; 20];
        let hs = build_handshake(&info_hash, &peer_id);
        assert_eq!(hs.len(), 68);
    }

    #[test]
    fn test_build_handshake_protocol() {
        let hs = build_handshake(&[0; 20], &[0; 20]);
        assert_eq!(&hs[..20], PROTOCOL);
    }

    #[test]
    fn test_build_handshake_extension_bit() {
        let hs = build_handshake(&[0; 20], &[0; 20]);
        assert_eq!(hs[25] & EXTENSION_BIT, EXTENSION_BIT);
    }

    #[test]
    fn test_build_handshake_info_hash() {
        let info_hash = [0xAB; 20];
        let hs = build_handshake(&info_hash, &[0; 20]);
        assert_eq!(&hs[28..48], &info_hash);
    }

    #[test]
    fn test_build_handshake_peer_id() {
        let peer_id = [0xCD; 20];
        let hs = build_handshake(&[0; 20], &peer_id);
        assert_eq!(&hs[48..68], &peer_id);
    }

    // =====================================================================
    // Bencode parsing helpers
    // =====================================================================

    #[test]
    fn test_extract_bencode_int_ut_metadata() {
        let data = "d1:md11:ut_metadatai2ee13:metadata_sizei31235ee";
        assert_eq!(extract_bencode_int(data, "ut_metadata"), Some(2));
    }

    #[test]
    fn test_extract_bencode_int_metadata_size() {
        let data = "d1:md11:ut_metadatai2ee13:metadata_sizei31235ee";
        assert_eq!(extract_bencode_int(data, "metadata_size"), Some(31235));
    }

    #[test]
    fn test_extract_bencode_int_missing_key() {
        let data = "d1:md11:ut_metadatai2eee";
        assert_eq!(extract_bencode_int(data, "metadata_size"), None);
    }

    #[test]
    fn test_extract_bencode_int_msg_type() {
        let data = "d8:msg_typei1e5:piecei0ee";
        assert_eq!(extract_bencode_int(data, "msg_type"), Some(1));
        assert_eq!(extract_bencode_int(data, "piece"), Some(0));
    }

    // =====================================================================
    // Bencode dict end finding
    // =====================================================================

    #[test]
    fn test_find_bencode_dict_end_simple() {
        let data = b"d3:fooi1ee";
        assert_eq!(find_bencode_dict_end(data), Some(10));
    }

    #[test]
    fn test_find_bencode_dict_end_nested() {
        let data = b"d1:md11:ut_metadatai2eee";
        assert_eq!(find_bencode_dict_end(data), Some(data.len()));
    }

    #[test]
    fn test_find_bencode_dict_end_with_trailing_data() {
        let data = b"d8:msg_typei1e5:piecei0eeBINARYDATA";
        let end = find_bencode_dict_end(data).unwrap();
        assert_eq!(&data[end..], b"BINARYDATA");
    }

    #[test]
    fn test_find_bencode_dict_end_not_dict() {
        assert_eq!(find_bencode_dict_end(b"l3:fooe"), None);
    }

    #[test]
    fn test_find_bencode_dict_end_empty() {
        assert_eq!(find_bencode_dict_end(b""), None);
    }

    // =====================================================================
    // Torrent file parsing — single file
    // =====================================================================

    #[test]
    fn test_parse_single_file_torrent() {
        // Bencode: d4:name9:movie.mkv6:lengthi1073741824ee
        let info = b"d4:name9:movie.mkv6:lengthi1073741824ee";
        let files = parse_torrent_files(info).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "movie.mkv");
        assert_eq!(files[0].size, 1_073_741_824);
        assert_eq!(files[0].index, 0);
    }

    #[test]
    fn test_parse_single_file_with_piece_length() {
        // Include piece_length (should be ignored by our parser)
        let info = b"d4:name8:test.mp412:piece_lengthi262144e6:lengthi5000ee";
        let files = parse_torrent_files(info).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "test.mp4");
        assert_eq!(files[0].size, 5000);
    }

    // =====================================================================
    // Torrent file parsing — multi file
    // =====================================================================

    #[test]
    fn test_parse_multi_file_torrent() {
        let info = b"d5:filesld6:lengthi100e4:pathl12:Episode1.mkveed6:lengthi200e4:pathl12:Episode2.mkveed6:lengthi300e4:pathl12:Episode3.mkveee4:name11:My Season 1e";
        let files = parse_torrent_files(info).unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].path, "Episode1.mkv");
        assert_eq!(files[0].size, 100);
        assert_eq!(files[0].index, 0);
        assert_eq!(files[1].path, "Episode2.mkv");
        assert_eq!(files[1].size, 200);
        assert_eq!(files[1].index, 1);
        assert_eq!(files[2].path, "Episode3.mkv");
        assert_eq!(files[2].size, 300);
        assert_eq!(files[2].index, 2);
    }

    #[test]
    fn test_parse_multi_file_nested_path() {
        let info = b"d5:filesld6:lengthi500e4:pathl8:Season 110:S01E01.mkveee4:name7:My Showe";
        let files = parse_torrent_files(info).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "Season 1/S01E01.mkv");
    }

    #[test]
    fn test_parse_multi_file_with_sample_and_nfo() {
        let info = b"d5:filesld6:lengthi1000e4:pathl10:S01E01.mkveed6:lengthi50e4:pathl10:sample.mkveed6:lengthi1e4:pathl8:info.nfoeed6:lengthi2000e4:pathl10:S01E02.mkveee4:name4:Packe";
        let files = parse_torrent_files(info).unwrap();
        assert_eq!(files.len(), 4);
        assert_eq!(files[0].path, "S01E01.mkv");
        assert_eq!(files[0].index, 0);
        assert_eq!(files[1].path, "sample.mkv");
        assert_eq!(files[1].index, 1);
        assert_eq!(files[2].path, "info.nfo");
        assert_eq!(files[2].index, 2);
        assert_eq!(files[3].path, "S01E02.mkv");
        assert_eq!(files[3].index, 3);
    }

    // =====================================================================
    // Extension handshake parsing
    // =====================================================================

    #[test]
    fn test_parse_extension_handshake_valid() {
        let data = b"d1:md11:ut_metadatai3ee13:metadata_sizei45678ee";
        let (ut_id, size) = parse_extension_handshake(data).unwrap();
        assert_eq!(ut_id, 3);
        assert_eq!(size, 45678);
    }

    #[test]
    fn test_parse_extension_handshake_missing_ut() {
        let data = b"d1:md11:ut_whateveri3ee13:metadata_sizei45678ee";
        assert!(parse_extension_handshake(data).is_err());
    }

    #[test]
    fn test_parse_extension_handshake_missing_size() {
        let data = b"d1:md11:ut_metadatai3eee";
        assert!(parse_extension_handshake(data).is_err());
    }

    // =====================================================================
    // Metadata request building
    // =====================================================================

    #[test]
    fn test_build_metadata_request_piece_0() {
        let msg = build_metadata_request(2, 0);
        // Should be: [len(4)] [20] [2] [bencode]
        assert_eq!(msg[4], 20); // Extended message
        assert_eq!(msg[5], 2); // ut_metadata ID
        let payload = std::str::from_utf8(&msg[6..]).unwrap();
        assert!(payload.contains("8:msg_typei0e"));
        assert!(payload.contains("5:piecei0e"));
    }

    #[test]
    fn test_build_metadata_request_piece_5() {
        let msg = build_metadata_request(1, 5);
        let payload = std::str::from_utf8(&msg[6..]).unwrap();
        assert!(payload.contains("5:piecei5e"));
    }

    // =====================================================================
    // Extension handshake building
    // =====================================================================

    #[test]
    fn test_build_extension_handshake_format() {
        let msg = build_extension_handshake();
        assert_eq!(msg[4], 20); // Extended message
        assert_eq!(msg[5], 0); // Handshake ID
        let payload = std::str::from_utf8(&msg[6..]).unwrap();
        assert!(payload.contains("ut_metadata"));
    }

    // =====================================================================
    // Peer ID generation
    // =====================================================================

    #[test]
    fn test_generate_peer_id_prefix() {
        let id = generate_peer_id();
        assert_eq!(&id[..8], b"-DM0100-");
        assert_eq!(id.len(), 20);
    }

    #[test]
    fn test_generate_peer_id_unique() {
        let id1 = generate_peer_id();
        // Sleep briefly to ensure different timestamp
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = generate_peer_id();
        // They might be the same within the same nanosecond, but
        // the important thing is they're 20 bytes with the right prefix
        assert_eq!(&id1[..8], &id2[..8]);
    }

    // =====================================================================
    // Error display
    // =====================================================================

    #[test]
    fn test_dht_error_display() {
        assert_eq!(DhtError::NoPeersFound.to_string(), "no peers found via DHT");
        assert_eq!(DhtError::Timeout.to_string(), "operation timed out");
        assert_eq!(
            DhtError::MetadataFetchFailed("test".into()).to_string(),
            "metadata fetch failed: test"
        );
    }

    // =====================================================================
    // Edge cases for torrent parsing
    // =====================================================================

    #[test]
    fn test_parse_torrent_files_not_dict() {
        assert!(parse_torrent_files(b"l3:fooe").is_err());
    }

    #[test]
    fn test_parse_torrent_files_missing_name() {
        assert!(parse_torrent_files(b"d6:lengthi100ee").is_err());
    }

    #[test]
    fn test_parse_torrent_files_empty_multi() {
        let info = b"d5:filesle4:name4:teste";
        let files = parse_torrent_files(info).unwrap();
        assert!(files.is_empty());
    }
}
