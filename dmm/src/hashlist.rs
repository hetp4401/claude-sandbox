use serde::{Deserialize, Serialize};

/// A single torrent entry in a hashlist.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HashlistEntry {
    pub filename: String,
    pub hash: String,
    pub size: u64,
}

/// A decoded hashlist containing an optional title and a list of torrent entries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Hashlist {
    #[serde(default)]
    pub title: Option<String>,
    pub list: Vec<HashlistEntry>,
}

/// Errors that can occur when parsing a hashlist.
#[derive(Debug)]
pub enum ParseError {
    DecompressionFailed,
    InvalidUtf16,
    InvalidJson(serde_json::Error),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::DecompressionFailed => write!(f, "LZ-string decompression failed"),
            ParseError::InvalidUtf16 => write!(f, "Decompressed data is not valid UTF-16"),
            ParseError::InvalidJson(e) => write!(f, "Invalid JSON: {e}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Extract the encoded data fragment from an HTML hashlist file.
///
/// Hashlist HTML files contain an iframe with src="...#<encoded_data>".
/// This extracts the encoded data after the `#`.
pub fn extract_encoded_from_html(html: &str) -> Option<&str> {
    // Look for the iframe src pattern: src="https://debridmediamanager.com/hashlist#..."
    let marker = "debridmediamanager.com/hashlist#";
    let start = html.find(marker)? + marker.len();
    let remaining = &html[start..];
    // The encoded data ends at the closing quote
    let end = remaining.find('"')?;
    Some(&remaining[..end])
}

/// Decode an LZ-string URI-encoded compressed string into a `Hashlist`.
pub fn decode_hashlist(encoded: &str) -> Result<Hashlist, ParseError> {
    let decompressed_utf16 = lz_str::decompress_from_encoded_uri_component(encoded)
        .ok_or(ParseError::DecompressionFailed)?;
    let json_str =
        String::from_utf16(&decompressed_utf16).map_err(|_| ParseError::InvalidUtf16)?;
    serde_json::from_str(&json_str).map_err(ParseError::InvalidJson)
}

/// Parse a full HTML hashlist file into a `Hashlist`.
pub fn parse_hashlist_html(html: &str) -> Result<Hashlist, ParseError> {
    let encoded = extract_encoded_from_html(html).ok_or(ParseError::DecompressionFailed)?;
    decode_hashlist(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    // === Fixtures generated with JS lz-string.compressToEncodedURIComponent ===

    const SINGLE_ENCODED: &str = "N4IgNglgzgLiBcBtUAzCYCmA7AhgWwwRACEIBzAOmIFcBjAayuqywE8KAmABi4A4KAjHy4AHKmGoAlHOwAeHAGwAWEABoQACxxQNRACZ7eHAKzGMtPbQDstHKZQAjLg4cpeAgMwcPhq1wUceg4CKAp6ArRqIFAQAF6E8AJKCrwKSQrGCgC+ALpZQA";

    const MULTI_ENCODED: &str = "N4IgLglmA2CmIC4QBVYGcwAIDCB7acAxpLgHYgA0I0EGiA2qAGYRykCGAtvEgMoSkwsaADoATAAYAjBJEAWANIiAqgAkAIiIBC0AK4AldgE8RADzEA2AKwBaVQFEAatkogAFuzRvEIC+wCcAOxW-gBGTEwAJlaEEuxM1gDMUv5B-kyhgQAciWJSWelyTImEiVaRrmgQAF48cmL+cv4WgQ0WAL4UzKywHNw+qOwATmgiuEwivELC4tJiIjJZEgAOIgDq9lo26gAyIgCC+9jzsqqWcq4eXj7sWVlR7CFWcrAFUtmlrYFSfn7ZoVl8hJYJE5JF2JFEpUajxAok5BIJLlEZ1umwuDwQPY4MsPIJRuohrAuLMJBYRK0Vto9IYTOpkLwzOdLp5vEhQokmFJYIlQYQrOwLJkCnN4VYWgV2KFCJFYExRXJxYFobVEHk5IE5DkLFr2gBddpAA";

    const EMPTY_ENCODED: &str = "N4IgNglgzgLiBcBtAugXyA";

    const MINIMAL_ENCODED: &str = "N4IgNglgzgLiBcBtUAzCYCmA7AhgWwwRBg1gDo8BrANxABoQALHKRonAIwGMATDFAIwAmAMwAWAKwA2AOwAOAJwAGTr37Dx0+ctV9BQ+iCgQAXoXgClSgL4Bda0A";

    // === HTML extraction tests ===

    #[test]
    fn test_extract_encoded_from_html() {
        let html = r#"<!doctype html>
<html><head></head><body>
<iframe src="https://debridmediamanager.com/hashlist#SomeEncodedData123"></iframe>
</body></html>"#;
        assert_eq!(
            extract_encoded_from_html(html),
            Some("SomeEncodedData123")
        );
    }

    #[test]
    fn test_extract_encoded_from_real_html_structure() {
        let html = r#"<!doctype html>
<html>
<head>
<meta charset=UTF-8>
<title>Debrid Media Manager Hash List</title>
<style>iframe{border:none;position:absolute;top:0;left:0;width:100%;height:100%}</style>
</head>
<body>
<iframe src="https://debridmediamanager.com/hashlist#N4IgNglgzgLiBcBtAugXyA"></iframe>
</body>
</html>"#;
        let encoded = extract_encoded_from_html(html).unwrap();
        assert_eq!(encoded, "N4IgNglgzgLiBcBtAugXyA");
    }

    #[test]
    fn test_extract_encoded_no_iframe() {
        let html = "<html><body>No iframe here</body></html>";
        assert!(extract_encoded_from_html(html).is_none());
    }

    #[test]
    fn test_extract_encoded_empty_fragment() {
        let html =
            r#"<iframe src="https://debridmediamanager.com/hashlist#"></iframe>"#;
        assert_eq!(extract_encoded_from_html(html), Some(""));
    }

    // === Decompression + JSON parsing tests ===

    #[test]
    fn test_decode_single_entry() {
        let hashlist = decode_hashlist(SINGLE_ENCODED).unwrap();
        assert!(hashlist.title.is_none());
        assert_eq!(hashlist.list.len(), 1);

        let entry = &hashlist.list[0];
        assert_eq!(entry.filename, "Big.Buck.Bunny.2008.1080p.BluRay.x264");
        assert_eq!(entry.hash, "dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c");
        assert_eq!(entry.size, 1_468_614_656);
    }

    #[test]
    fn test_decode_multi_entry_with_title() {
        let hashlist = decode_hashlist(MULTI_ENCODED).unwrap();
        assert_eq!(hashlist.title.as_deref(), Some("Test Collection"));
        assert_eq!(hashlist.list.len(), 3);

        assert_eq!(
            hashlist.list[0].filename,
            "Sintel.2010.4K.UHD.BluRay.x265-HEVC"
        );
        assert_eq!(
            hashlist.list[0].hash,
            "6a9759bffd5c0af65319979fb7832189f4f3c35d"
        );
        assert_eq!(hashlist.list[0].size, 4_294_967_296);

        assert_eq!(
            hashlist.list[1].filename,
            "Tears.of.Steel.2012.1080p.WEB-DL.AAC2.0.H264"
        );
        assert_eq!(
            hashlist.list[1].hash,
            "a88fda5954e89178c372716a6a78b8180ed4dad3"
        );
        assert_eq!(hashlist.list[1].size, 734_003_200);

        assert_eq!(
            hashlist.list[2].filename,
            "Elephants.Dream.2006.720p.BluRay.DTS.x264"
        );
        assert_eq!(
            hashlist.list[2].hash,
            "b3f1e3d4c5a6b7890123456789abcdef01234567"
        );
        assert_eq!(hashlist.list[2].size, 2_147_483_648);
    }

    #[test]
    fn test_decode_empty_list() {
        let hashlist = decode_hashlist(EMPTY_ENCODED).unwrap();
        assert!(hashlist.title.is_none());
        assert!(hashlist.list.is_empty());
    }

    #[test]
    fn test_decode_minimal_entry() {
        let hashlist = decode_hashlist(MINIMAL_ENCODED).unwrap();
        assert_eq!(hashlist.list.len(), 1);
        assert_eq!(hashlist.list[0].filename, "test.mkv");
        assert_eq!(
            hashlist.list[0].hash,
            "abcdef1234567890abcdef1234567890abcdef12"
        );
        assert_eq!(hashlist.list[0].size, 100);
    }

    #[test]
    fn test_decode_invalid_encoded_data() {
        let result = decode_hashlist("not-valid-lz-data!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_empty_string() {
        let result = decode_hashlist("");
        assert!(result.is_err());
    }

    // === Full HTML -> Hashlist pipeline tests ===

    #[test]
    fn test_parse_hashlist_html_full_pipeline() {
        let html = format!(
            r#"<!doctype html>
<html><head><meta charset=UTF-8><title>Debrid Media Manager Hash List</title>
<style>iframe{{border:none}}</style></head><body>
<iframe src="https://debridmediamanager.com/hashlist#{}"></iframe>
</body></html>"#,
            SINGLE_ENCODED
        );
        let hashlist = parse_hashlist_html(&html).unwrap();
        assert_eq!(hashlist.list.len(), 1);
        assert_eq!(
            hashlist.list[0].filename,
            "Big.Buck.Bunny.2008.1080p.BluRay.x264"
        );
    }

    #[test]
    fn test_parse_hashlist_html_no_iframe_errors() {
        let html = "<html><body>nothing</body></html>";
        assert!(parse_hashlist_html(html).is_err());
    }

    // === Roundtrip / serialization tests ===

    #[test]
    fn test_hashlist_entry_serialization_roundtrip() {
        let entry = HashlistEntry {
            filename: "Movie.2024.1080p.mkv".to_string(),
            hash: "aa".repeat(20),
            size: 999_999,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: HashlistEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_hashlist_without_title_deserializes() {
        let json = r#"{"list":[{"filename":"f.mkv","hash":"abc","size":1}]}"#;
        let hashlist: Hashlist = serde_json::from_str(json).unwrap();
        assert!(hashlist.title.is_none());
        assert_eq!(hashlist.list.len(), 1);
    }

    #[test]
    fn test_hashlist_with_title_deserializes() {
        let json =
            r#"{"title":"My List","list":[{"filename":"f.mkv","hash":"abc","size":1}]}"#;
        let hashlist: Hashlist = serde_json::from_str(json).unwrap();
        assert_eq!(hashlist.title.as_deref(), Some("My List"));
    }

    #[test]
    fn test_hashlist_missing_list_field_errors() {
        let json = r#"{"title":"oops"}"#;
        assert!(serde_json::from_str::<Hashlist>(json).is_err());
    }

    #[test]
    fn test_hashlist_entry_missing_field_errors() {
        let json = r#"{"filename":"f.mkv","hash":"abc"}"#;
        assert!(serde_json::from_str::<HashlistEntry>(json).is_err());
    }

    // === Hash format validation tests ===

    #[test]
    fn test_hash_is_40_char_hex_in_real_data() {
        let hashlist = decode_hashlist(SINGLE_ENCODED).unwrap();
        let hash = &hashlist.list[0].hash;
        assert_eq!(hash.len(), 40);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_all_hashes_are_40_char_hex() {
        let hashlist = decode_hashlist(MULTI_ENCODED).unwrap();
        for entry in &hashlist.list {
            assert_eq!(entry.hash.len(), 40, "hash '{}' is not 40 chars", entry.hash);
            assert!(
                entry.hash.chars().all(|c| c.is_ascii_hexdigit()),
                "hash '{}' contains non-hex chars",
                entry.hash
            );
        }
    }
}
