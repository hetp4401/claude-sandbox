use crate::ingestor::{HashlistStatus, MagnetRecord};
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};

/// Thread-safe SQLite database handle.
/// Uses Mutex (not RwLock) because rusqlite Connection is !Sync.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

#[allow(dead_code)]
impl Db {
    /// Open (or create) the database at the given path.
    pub fn open(path: &str) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.init_schema()?;
        Ok(db)
    }

    /// Open an in-memory database (for tests).
    pub fn open_in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();

        conn.execute_batch(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA busy_timeout=5000;

            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                hash TEXT NOT NULL,
                magnet_uri TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                source_hashlist TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                content_type TEXT,
                title TEXT,
                season INTEGER,
                episode INTEGER,
                file_index INTEGER,
                imdb_tag TEXT
            );

            CREATE TABLE IF NOT EXISTS hashlists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                processed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS processed_names (
                name TEXT PRIMARY KEY
            );

            CREATE INDEX IF NOT EXISTS idx_records_content_type ON records(content_type);
            CREATE INDEX IF NOT EXISTS idx_records_imdb_tag ON records(imdb_tag);
            CREATE INDEX IF NOT EXISTS idx_records_hash ON records(hash);
            CREATE INDEX IF NOT EXISTS idx_records_processed_at ON records(processed_at);
            ",
        )?;

        Ok(())
    }

    // =========================================================================
    // Records
    // =========================================================================

    pub fn insert_record(&self, r: &MagnetRecord) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO records (filename, hash, magnet_uri, size_bytes, source_hashlist,
             processed_at, content_type, title, season, episode, file_index, imdb_tag)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                r.filename,
                r.hash,
                r.magnet_uri,
                r.size_bytes as i64,
                r.source_hashlist,
                r.processed_at,
                r.content_type,
                r.title,
                r.season.map(|v| v as i64),
                r.episode.map(|v| v as i64),
                r.file_index.map(|v| v as i64),
                r.imdb_tag,
            ],
        )?;
        Ok(())
    }

    pub fn insert_records(&self, records: &[MagnetRecord]) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "INSERT INTO records (filename, hash, magnet_uri, size_bytes, source_hashlist,
             processed_at, content_type, title, season, episode, file_index, imdb_tag)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        )?;
        for r in records {
            stmt.execute(params![
                r.filename,
                r.hash,
                r.magnet_uri,
                r.size_bytes as i64,
                r.source_hashlist,
                r.processed_at,
                r.content_type,
                r.title,
                r.season.map(|v| v as i64),
                r.episode.map(|v| v as i64),
                r.file_index.map(|v| v as i64),
                r.imdb_tag,
            ])?;
        }
        Ok(())
    }

    pub fn get_all_records(&self) -> Result<Vec<MagnetRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT filename, hash, magnet_uri, size_bytes, source_hashlist,
             processed_at, content_type, title, season, episode, file_index, imdb_tag
             FROM records ORDER BY processed_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(MagnetRecord {
                filename: row.get(0)?,
                hash: row.get(1)?,
                magnet_uri: row.get(2)?,
                size_bytes: row.get::<_, i64>(3)? as u64,
                source_hashlist: row.get(4)?,
                processed_at: row.get(5)?,
                content_type: row.get(6)?,
                title: row.get(7)?,
                season: row.get::<_, Option<i64>>(8)?.map(|v| v as u32),
                episode: row.get::<_, Option<i64>>(9)?.map(|v| v as u32),
                file_index: row.get::<_, Option<i64>>(10)?.map(|v| v as u32),
                imdb_tag: row.get(11)?,
            })
        })?;
        rows.collect()
    }

    pub fn get_unprocessed_season_packs(&self) -> Result<Vec<MagnetRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT filename, hash, magnet_uri, size_bytes, source_hashlist,
             processed_at, content_type, title, season, episode, file_index, imdb_tag
             FROM records
             WHERE imdb_tag IS NULL AND content_type = 'season'
               AND title IS NOT NULL AND season IS NOT NULL",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(MagnetRecord {
                filename: row.get(0)?,
                hash: row.get(1)?,
                magnet_uri: row.get(2)?,
                size_bytes: row.get::<_, i64>(3)? as u64,
                source_hashlist: row.get(4)?,
                processed_at: row.get(5)?,
                content_type: row.get(6)?,
                title: row.get(7)?,
                season: row.get::<_, Option<i64>>(8)?.map(|v| v as u32),
                episode: row.get::<_, Option<i64>>(9)?.map(|v| v as u32),
                file_index: row.get::<_, Option<i64>>(10)?.map(|v| v as u32),
                imdb_tag: row.get(11)?,
            })
        })?;
        rows.collect()
    }

    pub fn mark_season_pack_processed(&self, hash: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE records SET imdb_tag = 'processed'
             WHERE hash = ?1 AND content_type = 'season' AND imdb_tag IS NULL",
            params![hash],
        )?;
        Ok(())
    }

    pub fn count_records(&self) -> Result<RecordCounts, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM records", [], |r| r.get(0))?;
        let movies: i64 = conn.query_row(
            "SELECT COUNT(*) FROM records WHERE content_type = 'movie'",
            [],
            |r| r.get(0),
        )?;
        let episodes: i64 = conn.query_row(
            "SELECT COUNT(*) FROM records WHERE content_type = 'episode'",
            [],
            |r| r.get(0),
        )?;
        let seasons: i64 = conn.query_row(
            "SELECT COUNT(*) FROM records WHERE content_type = 'season'",
            [],
            |r| r.get(0),
        )?;
        Ok(RecordCounts {
            total: total as usize,
            movies: movies as usize,
            episodes: episodes as usize,
            seasons: seasons as usize,
        })
    }

    // =========================================================================
    // Hashlists
    // =========================================================================

    pub fn insert_hashlist(&self, h: &HashlistStatus) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO hashlists (name, record_count, status, processed_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![h.name, h.record_count as i64, h.status, h.processed_at],
        )?;
        Ok(())
    }

    pub fn get_all_hashlists(&self) -> Result<Vec<HashlistStatus>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT name, record_count, status, processed_at
             FROM hashlists ORDER BY processed_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(HashlistStatus {
                name: row.get(0)?,
                record_count: row.get::<_, i64>(1)? as usize,
                status: row.get(2)?,
                processed_at: row.get(3)?,
            })
        })?;
        rows.collect()
    }

    pub fn count_hashlists_done(&self) -> Result<usize, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM hashlists WHERE status = 'done'",
            [],
            |r| r.get(0),
        )?;
        Ok(count as usize)
    }

    pub fn count_hashlists(&self) -> Result<usize, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM hashlists", [], |r| r.get(0))?;
        Ok(count as usize)
    }

    // =========================================================================
    // Processed names
    // =========================================================================

    pub fn is_processed(&self, name: &str) -> Result<bool, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM processed_names WHERE name = ?1",
            params![name],
            |r| r.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn mark_processed(&self, name: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO processed_names (name) VALUES (?1)",
            params![name],
        )?;
        Ok(())
    }
}

pub struct RecordCounts {
    pub total: usize,
    pub movies: usize,
    pub episodes: usize,
    pub seasons: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Db {
        Db::open_in_memory().unwrap()
    }

    fn make_record(filename: &str, hash: &str, content_type: Option<&str>) -> MagnetRecord {
        MagnetRecord {
            filename: filename.into(),
            hash: hash.into(),
            magnet_uri: format!("magnet:?xt=urn:btih:{hash}"),
            size_bytes: 1000,
            source_hashlist: "test.html".into(),
            processed_at: "2024-01-01T00:00:00Z".into(),
            content_type: content_type.map(|s| s.into()),
            title: Some("Test".into()),
            season: Some(1),
            episode: None,
            file_index: None,
            imdb_tag: None,
        }
    }

    // =========================================================================
    // Schema creation
    // =========================================================================

    #[test]
    fn test_open_in_memory() {
        let db = test_db();
        assert_eq!(db.count_records().unwrap().total, 0);
    }

    #[test]
    fn test_schema_idempotent() {
        let db = test_db();
        // Calling init_schema again should not fail
        db.init_schema().unwrap();
        assert_eq!(db.count_records().unwrap().total, 0);
    }

    // =========================================================================
    // Records CRUD
    // =========================================================================

    #[test]
    fn test_insert_and_get_record() {
        let db = test_db();
        let r = make_record("movie.mkv", "aaa", Some("movie"));
        db.insert_record(&r).unwrap();

        let records = db.get_all_records().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].filename, "movie.mkv");
        assert_eq!(records[0].hash, "aaa");
        assert_eq!(records[0].content_type.as_deref(), Some("movie"));
    }

    #[test]
    fn test_insert_records_batch() {
        let db = test_db();
        let records = vec![
            make_record("a.mkv", "aaa", Some("movie")),
            make_record("b.mkv", "bbb", Some("episode")),
            make_record("c.mkv", "ccc", Some("season")),
        ];
        db.insert_records(&records).unwrap();
        assert_eq!(db.count_records().unwrap().total, 3);
    }

    #[test]
    fn test_record_counts() {
        let db = test_db();
        let records = vec![
            make_record("a.mkv", "aaa", Some("movie")),
            make_record("b.mkv", "bbb", Some("movie")),
            make_record("c.mkv", "ccc", Some("episode")),
            make_record("d.mkv", "ddd", Some("season")),
        ];
        db.insert_records(&records).unwrap();

        let counts = db.count_records().unwrap();
        assert_eq!(counts.total, 4);
        assert_eq!(counts.movies, 2);
        assert_eq!(counts.episodes, 1);
        assert_eq!(counts.seasons, 1);
    }

    #[test]
    fn test_records_ordered_by_processed_at_desc() {
        let db = test_db();
        let mut r1 = make_record("old.mkv", "aaa", Some("movie"));
        r1.processed_at = "2024-01-01T00:00:00Z".into();
        let mut r2 = make_record("new.mkv", "bbb", Some("movie"));
        r2.processed_at = "2024-06-01T00:00:00Z".into();

        db.insert_record(&r1).unwrap();
        db.insert_record(&r2).unwrap();

        let records = db.get_all_records().unwrap();
        assert_eq!(records[0].filename, "new.mkv");
        assert_eq!(records[1].filename, "old.mkv");
    }

    #[test]
    fn test_record_nullable_fields() {
        let db = test_db();
        let r = MagnetRecord {
            filename: "test.mkv".into(),
            hash: "xxx".into(),
            magnet_uri: "magnet:xxx".into(),
            size_bytes: 500,
            source_hashlist: "t.html".into(),
            processed_at: "2024-01-01T00:00:00Z".into(),
            content_type: None,
            title: None,
            season: None,
            episode: None,
            file_index: None,
            imdb_tag: None,
        };
        db.insert_record(&r).unwrap();

        let records = db.get_all_records().unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].content_type.is_none());
        assert!(records[0].title.is_none());
        assert!(records[0].season.is_none());
        assert!(records[0].episode.is_none());
        assert!(records[0].file_index.is_none());
        assert!(records[0].imdb_tag.is_none());
    }

    // =========================================================================
    // Season pack queries
    // =========================================================================

    #[test]
    fn test_get_unprocessed_season_packs() {
        let db = test_db();
        let records = vec![
            make_record("movie.mkv", "aaa", Some("movie")),
            make_record("season.mkv", "bbb", Some("season")),
            make_record("episode.mkv", "ccc", Some("episode")),
        ];
        db.insert_records(&records).unwrap();

        let packs = db.get_unprocessed_season_packs().unwrap();
        assert_eq!(packs.len(), 1);
        assert_eq!(packs[0].hash, "bbb");
    }

    #[test]
    fn test_get_unprocessed_season_packs_skips_processed() {
        let db = test_db();
        let mut r = make_record("season.mkv", "bbb", Some("season"));
        r.imdb_tag = Some("processed".into());
        db.insert_record(&r).unwrap();

        let packs = db.get_unprocessed_season_packs().unwrap();
        assert!(packs.is_empty());
    }

    #[test]
    fn test_mark_season_pack_processed() {
        let db = test_db();
        let r = make_record("season.mkv", "bbb", Some("season"));
        db.insert_record(&r).unwrap();

        assert_eq!(db.get_unprocessed_season_packs().unwrap().len(), 1);

        db.mark_season_pack_processed("bbb").unwrap();

        assert!(db.get_unprocessed_season_packs().unwrap().is_empty());

        let records = db.get_all_records().unwrap();
        assert_eq!(records[0].imdb_tag.as_deref(), Some("processed"));
    }

    #[test]
    fn test_mark_season_pack_only_affects_matching_hash() {
        let db = test_db();
        let r1 = make_record("s1.mkv", "aaa", Some("season"));
        let r2 = make_record("s2.mkv", "bbb", Some("season"));
        db.insert_record(&r1).unwrap();
        db.insert_record(&r2).unwrap();

        db.mark_season_pack_processed("aaa").unwrap();

        let packs = db.get_unprocessed_season_packs().unwrap();
        assert_eq!(packs.len(), 1);
        assert_eq!(packs[0].hash, "bbb");
    }

    // =========================================================================
    // Hashlists
    // =========================================================================

    #[test]
    fn test_insert_and_get_hashlist() {
        let db = test_db();
        let h = HashlistStatus {
            name: "test.html".into(),
            record_count: 5,
            status: "done".into(),
            processed_at: "2024-01-01T00:00:00Z".into(),
        };
        db.insert_hashlist(&h).unwrap();

        let hashlists = db.get_all_hashlists().unwrap();
        assert_eq!(hashlists.len(), 1);
        assert_eq!(hashlists[0].name, "test.html");
        assert_eq!(hashlists[0].record_count, 5);
        assert_eq!(hashlists[0].status, "done");
    }

    #[test]
    fn test_count_hashlists_done() {
        let db = test_db();
        db.insert_hashlist(&HashlistStatus {
            name: "a.html".into(),
            record_count: 3,
            status: "done".into(),
            processed_at: "2024-01-01T00:00:00Z".into(),
        })
        .unwrap();
        db.insert_hashlist(&HashlistStatus {
            name: "b.html".into(),
            record_count: 0,
            status: "error: failed".into(),
            processed_at: "2024-01-01T00:00:00Z".into(),
        })
        .unwrap();

        assert_eq!(db.count_hashlists().unwrap(), 2);
        assert_eq!(db.count_hashlists_done().unwrap(), 1);
    }

    // =========================================================================
    // Processed names
    // =========================================================================

    #[test]
    fn test_processed_names() {
        let db = test_db();
        assert!(!db.is_processed("test.html").unwrap());

        db.mark_processed("test.html").unwrap();
        assert!(db.is_processed("test.html").unwrap());
        assert!(!db.is_processed("other.html").unwrap());
    }

    #[test]
    fn test_mark_processed_idempotent() {
        let db = test_db();
        db.mark_processed("test.html").unwrap();
        db.mark_processed("test.html").unwrap(); // Should not error
        assert!(db.is_processed("test.html").unwrap());
    }

    // =========================================================================
    // Persistence simulation
    // =========================================================================

    #[test]
    fn test_data_survives_reopen() {
        let dir = std::env::temp_dir().join("dmm_test_persist");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test.db");
        let path_str = path.to_str().unwrap();

        // Clean up from previous runs
        let _ = std::fs::remove_file(&path);

        // First connection: insert data
        {
            let db = Db::open(path_str).unwrap();
            db.insert_record(&make_record("movie.mkv", "aaa", Some("movie")))
                .unwrap();
            db.mark_processed("test.html").unwrap();
            db.insert_hashlist(&HashlistStatus {
                name: "test.html".into(),
                record_count: 1,
                status: "done".into(),
                processed_at: "2024-01-01T00:00:00Z".into(),
            })
            .unwrap();
        }

        // Second connection: data should still be there
        {
            let db = Db::open(path_str).unwrap();
            assert_eq!(db.get_all_records().unwrap().len(), 1);
            assert!(db.is_processed("test.html").unwrap());
            assert_eq!(db.get_all_hashlists().unwrap().len(), 1);
        }

        // Clean up
        let _ = std::fs::remove_file(&path);
    }
}
